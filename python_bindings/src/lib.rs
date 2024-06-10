#![feature(btree_cursors)]

use std::{fs::{File, OpenOptions}, io::{BufReader, Error, ErrorKind, Read, Seek}, path::PathBuf};

use pyo3::prelude::*;
use zygos_db::ColumnType;

#[pyclass]
#[derive(Clone, Debug)]
pub struct DatabaseHeader {
    #[pyo3(get)]
    pub version: u8,
    #[pyo3(get)]
    pub datasets: Vec<DatasetHeader>,
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct DatasetHeader {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub columns: Vec<ColumnHeader>,
    #[pyo3(get)]
    pub tables: Vec<TableHeader>,
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct ColumnHeader {
    pub type_: ColumnType,
    #[pyo3(get)]
    pub name: String,
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct TableHeader {
    #[pyo3(get)]
    pub chromosome: u8,
    #[pyo3(get)]
    pub offset: u64,
}

impl From<zygos_db::query::DatabaseHeader> for DatabaseHeader {
    fn from(header: zygos_db::query::DatabaseHeader) -> Self {
        Self {
            version: header.version,
            datasets: header.datasets.into_iter().map(DatasetHeader::from).collect(),
        }
    }
}

impl From<zygos_db::query::DatasetHeader> for DatasetHeader {
    fn from(header: zygos_db::query::DatasetHeader) -> Self {
        Self {
            name: header.name,
            columns: header.columns.into_iter().map(ColumnHeader::from).collect(),
            tables: header.tables.into_iter().map(TableHeader::from).collect(),
        }
    }
}

impl From<zygos_db::query::ColumnHeader> for ColumnHeader {
    fn from(header: zygos_db::query::ColumnHeader) -> Self {
        Self {
            type_: header.type_,
            name: header.name,
        }
    }
}

impl From<zygos_db::query::TableHeader> for TableHeader {
    fn from(header: zygos_db::query::TableHeader) -> Self {
        Self {
            chromosome: header.chromosome,
            offset: header.offset,
        }
    }
}

#[pymethods]
impl DatabaseHeader {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

#[pymethods]
impl DatasetHeader {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

#[pymethods]
impl ColumnHeader {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

#[pymethods]
impl TableHeader {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

#[pyclass]
struct DatabaseQueryClient {
    inner: zygos_db::query::DatabaseQueryClient<std::fs::File>,
    #[pyo3(get)]
    path: PathBuf,
    #[pyo3(get)]
    header: DatabaseHeader,
}

#[pymethods]
impl DatabaseQueryClient {
    #[new]
    fn new(path: PathBuf) -> PyResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        let mut inner = zygos_db::query::DatabaseQueryClient::new(file);

        let header = inner.read_database_header()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        Ok(Self {
            inner,
            path,
            header: header.into(),
        })
    }

    fn read_table_index(&mut self, dataset_name: &str, chromosome: u8) -> PyResult<TableIndex> {
        let dataset = self.header.datasets.iter()
            .find(|dataset| dataset.name == dataset_name)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Dataset not found: {}", dataset_name)))?;

        let table = dataset.tables.iter()
            .find(|table| table.chromosome == chromosome)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Table not found: {}", chromosome)))?;

        let index = self.inner.read_table_index(table.offset)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        Ok(TableIndex {
            inner: index,
            dataset_name: dataset_name.to_string(),
            chromosome,
            columns: dataset.columns.clone(),
            path: self.path.clone(),
        })
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.header))
    }
}

#[pyclass]
#[derive(Clone)]
struct TableIndex {
    inner: zygos_db::query::TableIndex,
    #[pyo3(get)]
    dataset_name: String,
    #[pyo3(get)]
    chromosome: u8,
    columns: Vec<ColumnHeader>,
    path: PathBuf,
}

impl std::fmt::Debug for TableIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableIndex")
            .field("dataset_name", &self.dataset_name)
            .field("chromosome", &self.chromosome)
            .finish()
    }
}

#[pymethods]
impl TableIndex {
    fn get_all(&self) -> PyResult<Vec<(u64, u64)>> {
        Ok(self.inner.get_all())
    }

    fn get_range(&self, start: u64, end: u64) -> PyResult<Vec<(u64, u64)>> {
        Ok(self.inner.get_range(start, end))
    }

    #[getter]
    fn min_position(&self) -> u64 {
        self.inner.inner.keys().next().copied().unwrap_or(0)
    }

    #[getter]
    fn max_position(&self) -> u64 {
        self.inner.max_position
    }

    #[getter]
    fn index_start_offset(&self) -> u64 {
        self.inner.index_start_offset
    }

    #[getter]
    fn index_end_offset(&self) -> u64 {
        self.inner.index_end_offset
    }

    fn create_query(&self) -> PyResult<RowReader> {
        Ok(RowReader::new(
            self.path.clone(),
            self.clone(),
        )?)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

#[pyclass]
struct RowReader {
    reader: BufReader<File>,
    index: TableIndex,
}

impl RowReader {
    fn new(path: PathBuf, index: TableIndex) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            index,
        })
    }

    fn read_u64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0; size_of::<u64>()];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    fn read_i64(&mut self) -> std::io::Result<i64> {
        let mut buf = [0; size_of::<i64>()];
        self.reader.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    fn read_zigzag_i64(&mut self) -> std::io::Result<(i64, usize)> {
        let mut buf = [0u8; 9];
        self.reader.read_exact(&mut buf[0..1])?;
        let len = vint64::decoded_len(buf[0]);

        self.reader.read_exact(&mut buf[1..len])?;
        let mut slice = &buf[..len];

        Ok((vint64::signed::decode(&mut slice).unwrap(), len))
    }

    fn read_u8(&mut self) -> std::io::Result<u8> {
        let mut buf = [0; size_of::<u8>()];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_f64(&mut self) -> std::io::Result<f64> {
        let mut buf = [0; size_of::<f64>()];
        self.reader.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }

    fn read_string_u8(&mut self) -> std::io::Result<String> {
        let len: usize = self.read_u8()? as usize;
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf)?;
        Ok(String::from_utf8(buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
    }
}

#[pymethods]
impl RowReader {
    /// Query a range of rows from the database
    /// 
    /// # Arguments
    /// 
    /// * `position_value_start` - The start of the range (inclusive)
    /// * `position_value_end` - The end of the range (exclusive)
    /// 
    /// # Returns
    /// 
    /// A vector of rows
    fn query_range(&mut self, position_value_start: u64, position_value_end: u64) -> Result<Vec<Row>, std::io::Error> {
        let range: Vec<(u64, u64)> = self.index.get_range(position_value_start, position_value_end)?;

        let start_offset = match range.first() {
            Some((_position, offset)) => *offset,
            None => return Ok(Vec::new()),
        };

        let rows = self.deserialize_range(
            // Start at the first row
            start_offset,
            // Stop before we reach the start of the index, which is where
            // the table data has ended (probably will not get there)
            self.index.inner.index_start_offset,
            // Stop at the end of the range
            position_value_end,
        )?;

        Ok(rows)
    }

    /// Deserialize a range of bytes from the reader using raw offsets. Unless you know what you're doing, use `query_range` instead.
    /// 
    /// # Arguments
    /// 
    /// * `offset_start` - The offset in the database to the start of the range (inclusive). This must be at the start of a row
    /// * `offset_end` - The offset in the database to the end of the range (exclusive)
    /// * `position_value_end` - Stop if the position value is greater than this value
    /// 
    /// # Returns
    /// 
    /// A vector of bytes
    pub fn deserialize_range(&mut self, offset_start: u64, offset_end: u64, position_value_end: u64) -> Result<Vec<Row>, std::io::Error> {
        println!("Deserializing range: {} - {}, or until position value is greater than {}", offset_start, offset_end, position_value_end);

        self.reader.seek(std::io::SeekFrom::Start(offset_start))?;
        
        let mut rows = Vec::new();

        let lambdas: Vec<_> = self.index.columns.iter().map(|column| {
            match column.type_ {
                ColumnType::Integer => {
                    |reader: &mut RowReader| {
                        let (value, len) = reader.read_zigzag_i64().unwrap();
                        (CellValue::I64(value), len)
                    }
                },
                ColumnType::Float => {
                    |reader: &mut RowReader| (CellValue::F64(reader.read_f64().unwrap()), 8)
                },
                ColumnType::VolatileString => {
                    |reader: &mut RowReader| {
                        let string = match reader.read_string_u8() {
                            Ok(string) => string,
                            Err(e) => panic!("Reading string failed at position {:?}: {:?}",
                                reader.reader.seek(std::io::SeekFrom::Current(0)).unwrap(), e),
                        };
                        let bytes_read = string.len() as usize + 1;
                        (CellValue::String(string), bytes_read)
                    }
                },
                ColumnType::HashtableString => {
                    todo!("HashtableString has not been implemented yet!");
                },
            }
        }).collect();

        let mut current_pos = offset_start;
        'row_loop: loop {
            if current_pos >= offset_end {
                break;
            }

            let mut cells = Vec::new();
            let mut i = 0;
            for lambda in &lambdas {
                let (value, bytes_read) = lambda(self);

                if i == 0 {
                    match value {
                        CellValue::I64(i) => {
                            if i > position_value_end as i64 {
                                break 'row_loop;
                            }
                        },
                        _ => panic!("First column must be an integer"),
                    }
                }
                i += 1;

                cells.push(value);
                current_pos += bytes_read as u64;
            }
            rows.push(Row { cells });
        }

        Ok(rows)
    }
}

#[derive(Clone, Debug)]
enum CellValue {
    I64(i64),
    F64(f64),
    String(String),
}

impl IntoPy<PyObject> for CellValue {
    fn into_py(self, py: Python) -> PyObject {
        match self {
            CellValue::I64(i) => i.into_py(py),
            CellValue::F64(f) => f.into_py(py),
            CellValue::String(s) => s.into_py(py),
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct Row {
    cells: Vec<CellValue>,
}

#[pymethods]
impl Row {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.cells))
    }

    fn get(&self, index: usize) -> PyResult<CellValue> {
        self.cells.get(index)
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyIndexError, _>(format!("Index out of bounds: {}", index)))
            .cloned()
    }

    fn __getitem__(&self, index: usize) -> PyResult<CellValue> {
        self.get(index)
    }

    fn len(&self) -> usize {
        self.cells.len()
    }
}

/// A Python module to read ZygosDB files.
#[pymodule]
#[pyo3(name = "zygos_db")]
fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatabaseQueryClient>()?;
    Ok(())
}
