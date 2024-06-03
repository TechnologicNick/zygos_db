use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::config::{Column, Config, Dataset};
use crate::tsv_reader::{CellValue, TabSeparatedFileReader};

const HEADER_MAGIC: &[u8] = b"ZygosDB";
const HEADER_VERSION: u8 = 1;

#[derive(Debug)]
pub struct Database {
    path: std::path::PathBuf,
    config: Config,
}

pub struct Table {
    #[allow(dead_code)]
    chromosome: u8,
    rows: Vec<Row>,
}

pub type Row = Vec<CellValue>;

pub type IndicesList = Vec<(usize, usize)>;

impl Database {
    pub fn new(path: std::path::PathBuf, config: Config) -> Self {
        Self {
            path,
            config,
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        self.clear_if_database(&self.path)?;

        let mut file = std::fs::File::create(&self.path)?;

        let mut bytes: Vec<u8> = Vec::new();
        self.serialize_database_header(&mut bytes);
        

        let loaded_datasets = match self.load_datasets() {
            Ok(res) => res,
            Err(e) => {
                eprintln!("Failed to load datasets:\n\t{}", e);
                std::process::exit(1);
            }
        };

        let _dataset_indices = self.serialize_datasets(&mut bytes, loaded_datasets);

        file.write_all(&bytes)?;

        Ok(())
    }

    pub fn clear_if_database(&self, path: &PathBuf) -> std::io::Result<()> {
        let mut file = match OpenOptions::new().read(true).write(true).create(false).open(path) {
            Ok(file) => file,
            Err(_) => return Ok(()), // The file does not exist
        };

        let mut magic_bytes = [0; HEADER_MAGIC.len()];
        match file.read_exact(&mut magic_bytes) {
            Ok(_) => {
                if magic_bytes == HEADER_MAGIC {
                    file.set_len(0)
                } else {
                    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Not a ZygosDB database"))
                }
            },
            Err(_) => Ok(()), // The file is empty
        }
    }

    pub fn serialize_database_header(&self, bytes: &mut Vec<u8>) -> () {
        assert!(self.config.datasets.len() < 256);

        bytes.extend_from_slice(&HEADER_MAGIC);
        bytes.push(HEADER_VERSION);

        bytes.push(self.config.datasets.len() as u8);

        for dataset in self.config.datasets.values() {
            self.serialize_dataset_header(bytes, dataset);
        }
    }

    fn serialize_dataset_header(&self, bytes: &mut Vec<u8>, dataset: &Dataset) -> () {
        let dataset_name = &dataset.metadata.as_ref().unwrap().name;
        assert!(dataset_name.len() < 256);

        bytes.push(dataset_name.len() as u8);
        bytes.extend_from_slice(dataset_name.as_bytes());

        bytes.push(dataset.columns.len() as u8);

        for column in dataset.columns.iter() {
            self.serialize_column_header(bytes, &column);
        }
    }

    fn serialize_column_header(&self, bytes: &mut Vec<u8>, column: &Column) -> () {
        let column_name = &column.name;
        assert!(column_name.len() < 256);

        bytes.push(column.type_ as u8);
        bytes.push(column_name.len() as u8);
        bytes.extend_from_slice(column_name.as_bytes());
    }

    pub fn load_datasets(&self) -> Result<Vec<(&Dataset, Vec<Table>)>, String> {
        let loaded_datasets = self.config.datasets.values().map(|dataset| {
            match self.load_dataset(dataset) {
                Ok(res) => Ok((dataset, res)),
                Err(e) => Err(format!("Failed to load dataset '{}':\n\t{}", dataset.metadata.as_ref().unwrap().name, e)),
            }
        }).collect::<Result<Vec<_>, _>>()?;

        Ok(loaded_datasets)
    }

    fn load_dataset(&self, dataset: &Dataset) -> Result<Vec<Table>, String> {
        let config_path = &self.config.metadata.as_ref().expect("metadata must be present").config_path;
        
        let par_iter = dataset.get_paths(config_path).into_par_iter().map(|(chromosome, path)| {
            match self.load_dataset_file(&dataset, &path) {
                Ok(rows) => Ok(Table { chromosome, rows }),
                Err(e) => Err(format!("Failed to load file of chromosome {} '{}':\n\t{}", chromosome, path.display(), e)),
            }
        });

        let mut result = Vec::new();
        par_iter.collect_into_vec(&mut result);

        result.into_iter().collect()
    }

    fn load_dataset_file(&self, dataset: &Dataset, path: &PathBuf) -> Result<Vec<Row>, String> {
        let mut reader = TabSeparatedFileReader::new(std::fs::File::open(path).unwrap());

        let column_names = dataset.columns.iter().map(|column| column.name.to_owned()).collect();
        let column_indices: Vec<(String, usize)> = reader.find_column_indices(&column_names)?;

        let mut wide_index_to_config_column: Vec<(usize, &Column)> = Vec::new();
        for (column_name, index) in column_indices {
            match dataset.columns.iter().find(|column| column.name == column_name) {
                Some(column) => wide_index_to_config_column.push((index, column)),
                None => return Err(format!("Column '{}' not found in config", column_name)),
            };
        }

        let all_data: Vec<Row> = reader.read_all(&wide_index_to_config_column)?;
        let all_data: Vec<Row> = reader.convert_read_data(&dataset.columns, all_data)?;

        Ok(all_data)
    }

    pub fn serialize_datasets(&self, bytes: &mut Vec<u8>, datasets: Vec<(&Dataset, Vec<Table>)>) -> Result<Vec<Vec<IndicesList>>, String> {
        let mut dataset_indices = Vec::new();

        for (dataset, all_data) in datasets {
            let position_indices_list = self.serialize_dataset(bytes, dataset, all_data)?;
            dataset_indices.push(position_indices_list);
        }

        Ok(dataset_indices)
    }

    pub fn serialize_dataset(&self, bytes: &mut Vec<u8>, dataset: &Dataset, tables: Vec<Table>) -> Result<Vec<IndicesList>, String> {
        let mut indices = Vec::new();
        
        for table in tables {
            let position_indices = self.serialize_dataset_file(bytes, dataset, table.rows)?;
            indices.push(position_indices);
        }

        Ok(indices)
    }

    fn serialize_dataset_file(&self, bytes: &mut Vec<u8>, dataset: &Dataset, rows: Vec<Row>) -> Result<IndicesList, String> {
        // Map of position (first column) to offset in the file
        let mut position_indices: Vec<(usize, usize)> = Vec::new();

        for (i_row, row) in rows.iter().enumerate() {
            for (i_col, cell) in row.iter().enumerate() {
                match cell {
                    CellValue::Integer(i) => {
                        if i_col == 0 {
                            if *i < 0 {
                                return Err(format!("Position must be a positive integer (column {:?}, row {})", dataset.columns[i_col].name, i_row));
                            }
                            position_indices.push((*i as usize, bytes.len()));
                        }
                        bytes.extend_from_slice(&i.to_be_bytes());
                    },
                    CellValue::Float(f) => {
                        bytes.extend_from_slice(&f.to_be_bytes());
                    },
                    CellValue::String(s) => {
                        let s_bytes = s.as_bytes();
                        let s_len = s_bytes.len();

                        if s_len > 255 {
                            return Err(format!("Strings longer than 255 bytes are currently not supported (column {:?}, row {})", dataset.columns[i_col].name, i_row));
                        }

                        bytes.push(s_len as u8);
                        bytes.extend_from_slice(s_bytes);
                    },
                }
            }
        }

        Ok(position_indices)
    }
}
