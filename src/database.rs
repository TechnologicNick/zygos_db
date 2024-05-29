use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::config::{Column, Config, Dataset};

const HEADER_MAGIC: &[u8] = b"ZygosDB";
const HEADER_VERSION: u8 = 1;

#[derive(Debug)]
pub struct Database {
    path: std::path::PathBuf,
    config: Config,
}

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
}
