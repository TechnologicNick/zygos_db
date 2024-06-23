#![feature(btree_cursors)]
mod pyo3_utils;

use std::{cmp::max, fs::{File, OpenOptions}, io::{BufReader, Cursor, Error, ErrorKind, Read, Seek}, path::PathBuf};

use pyo3::{prelude::*, types::PyList};
use pyo3_utils::new_from_iter;
use zygos_db::{compression::{CompressionAlgorithm, RowDecompressor}, deserialize, ColumnType};
use rhexdump::prelude::*;
use rayon::prelude::*;

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
    pub compression_algorithm: CompressionAlgorithm,
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
            compression_algorithm: header.compression_algorithm,
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

    #[getter]
    fn compression_algorithm(&self) -> String {
        format!("{:?}", self.compression_algorithm)
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
            compression_algorithm: dataset.compression_algorithm,
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
    compression_algorithm: CompressionAlgorithm,
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

    fn create_query_parallel(&self, num_threads: Option<usize>) -> PyResult<ParallelRowReader> {
        let row_readers = (0..num_threads.unwrap_or_else(rayon::current_num_threads))
            .map(|_| RowReader::new(
                self.path.clone(),
                self.clone(),
            ));

        Ok(ParallelRowReader {
            index: self.clone(),
            row_readers: row_readers.collect::<Result<Vec<_>, _>>()?,
        })
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

    /// Deserialize a range of bytes from the reader using raw offsets. Unless you know what you're doing, use `query_range` instead.
    /// 
    /// # Arguments
    /// 
    /// * `bytes` - The bytes to deserialize
    /// * `position_value_start` - Skip rows until the position value is greater than or equal to this value
    /// * `position_value_end` - Stop if the position value is greater than this value
    /// 
    /// # Returns
    /// 
    /// A vector of bytes
    pub fn deserialize_range(
        &self,
        bytes: &[u8],
        position_value_start: u64,
        position_value_end: u64,
        out_rows: &mut Vec<Row>,
    ) -> std::io::Result<()> {
        // println!("Deserializing range: {}:{}-{}", self.index.chromosome, position_value_start, position_value_end);

        let offset_start: u64 = 0;
        let offset_end = bytes.len() as u64;

        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);

        let skip_lambdas: Vec<_> = self.index.columns.iter()
            .skip(1) // Skip the first position column, as we always want to read it
            .map(|column| {
                match column.type_ {
                    ColumnType::Integer => {
                        |cursor: &mut Cursor<&[u8]>| {
                            deserialize::skip_zigzag_i64(cursor).unwrap()
                        }
                    },
                    ColumnType::Float => {
                        |cursor: &mut Cursor<&[u8]>| {
                            deserialize::skip_f64(cursor).unwrap()
                        }
                    },
                    ColumnType::VolatileString => {
                        |cursor: &mut Cursor<&[u8]>| {
                            deserialize::skip_string_u8(cursor).unwrap()
                        }
                    },
                    ColumnType::HashtableString => {
                        todo!("HashtableString has not been implemented yet!");
                    },
                }
            }).collect();

        let read_lambdas: Vec<_> = self.index.columns.iter().map(|column| {
            match column.type_ {
                ColumnType::Integer => {
                    |cursor: &mut Cursor<&[u8]>| {
                        let (value, len) = deserialize::read_zigzag_i64(cursor)?;
                        Ok((CellValue::I64(value), len))
                    }
                },
                ColumnType::Float => {
                    |cursor: &mut Cursor<&[u8]>| Ok((CellValue::F64(deserialize::read_f64(cursor)?), 8))
                },
                ColumnType::VolatileString => {
                    |cursor: &mut Cursor<&[u8]>| {
                        let string = match deserialize::read_string_u8(cursor) {
                            Ok(string) => string,
                            Err(e) => return Err(Error::new(ErrorKind::InvalidData, format!(
                                "Reading string failed: {:?}", e
                            ))),
                        };
                        let bytes_read = string.len() as usize + 1;
                        Ok((CellValue::String(string), bytes_read))
                    }
                },
                ColumnType::HashtableString => {
                    todo!("HashtableString has not been implemented yet!");
                },
            }
        }).collect();

        let mut offset_in_block = offset_start;
        'row_loop: loop {
            if offset_in_block >= offset_end {
                break;
            }

            let mut cells = Vec::new();
            let mut i = 0;
            for lambda in &read_lambdas {
                let (value, bytes_read) = lambda(&mut cursor).map_err(|e| Error::new(ErrorKind::InvalidData, format!(
                    "Failed to read column {} of after successfully reading row at position {:?} of chromosome {:?}, before stopping at {:?}: {:?}",
                    i, offset_in_block, self.index.chromosome, offset_end, e,
                )))?;

                offset_in_block += bytes_read as u64;

                if i == 0 {
                    match value {
                        CellValue::I64(i) => {
                            if i > position_value_end as i64 {
                                break 'row_loop;
                            } else if i < position_value_start as i64 {
                                // Skip this row
                                for lambda in &skip_lambdas {
                                    let bytes_skipped = lambda(&mut cursor);
                                    offset_in_block += bytes_skipped as u64;
                                }
                                continue 'row_loop;
                            }
                        },
                        _ => panic!("First column must be an integer"),
                    }
                }
                i += 1;

                cells.push(value);
            }
            out_rows.push(Row { cells });
        }

        Ok(())
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
    fn query_range(&mut self, position_value_start: u64, position_value_end: u64) -> std::io::Result<Vec<Row>> {
        let mut range: Vec<(u64, u64)> = self.index.get_range(position_value_start, position_value_end)?;

        let start_offset = match range.first() {
            Some((_position, offset)) => *offset,
            None => return Ok(Vec::new()),
        };
        self.reader.seek(std::io::SeekFrom::Start(start_offset))?;

        // Append the end of the index to the range
        range.push((position_value_end, self.index.inner.index_start_offset));

        let blocks = range.windows(2).map(|window| {
            let [start, end] = window else { unreachable!() };
            (start, end)
        });

        let mut compressed: Vec<u8> = Vec::new();
        let mut decompressed: Vec<u8> = Vec::new();
        let decompressor = RowDecompressor::new(self.index.compression_algorithm);

        let mut rows = Vec::new();
        for (start, end) in blocks {
            compressed.clear();
            self.reader.by_ref().take(end.1 - start.1).read_to_end(&mut compressed)?;

            let slice = match decompressor.decompress(&compressed, &mut decompressed) {
                Ok(res) => res,
                Err(e) => {
                    eprintln!("Decompression failed: {:?}", e);
                    rhexdump!(&compressed[..], start.1);
                    return Err(e);
                },
            };

            self.deserialize_range(
                &slice,
                max(start.0, position_value_start),
                end.0,
                &mut rows,
            )?;
        }

        Ok(rows)
    }
}

fn divide_into_parts<I, T>(mut iter: I, num_parts: usize, len: usize) -> Vec<Vec<T>>
where
    I: Iterator<Item = T>,
{
    let mut result = Vec::with_capacity(num_parts);
    let base_size = len / num_parts;
    let remainder = len % num_parts;
    let mut remaining_items = len;

    for i in 0..num_parts {
        let mut current_part_size = base_size;
        if i < remainder {
            current_part_size += 1;
        }

        let mut part = Vec::with_capacity(current_part_size);
        for _ in 0..current_part_size {
            if let Some(item) = iter.next() {
                part.push(item);
                remaining_items -= 1;
            } else {
                break;
            }
        }

        result.push(part);
    }

    // If there are remaining items due to rounding in division, distribute them
    // to the earlier parts.
    for i in 0..remainder {
        if let Some(item) = iter.next() {
            result[i].push(item);
            remaining_items -= 1;
        } else {
            break;
        }
    }

    assert_eq!(remaining_items, 0, "Iterator did not yield expected number of items");

    result
}

#[pyclass]
struct ParallelRowReader {
    #[allow(dead_code)]
    index: TableIndex,
    row_readers: Vec<RowReader>,
}

#[pymethods]
impl ParallelRowReader {
    fn query_range(&mut self, py: Python<'_>, position_value_start: u64, position_value_end: u64) -> std::io::Result<PyObject> {
        let mut range: Vec<(u64, u64)> = self.index.get_range(position_value_start, position_value_end)?;
        if range.is_empty() {
            return Ok(PyList::empty_bound(py).into());
        }

        let range_len = range.len();

        // Append the end of the index to the range
        range.push((position_value_end, self.index.inner.index_start_offset));

        let blocks = range.windows(2).map(|window| {
            let [start, end] = window else { unreachable!() };
            (start, end)
        });

        let block_jobs = divide_into_parts(blocks, self.row_readers.len(), range_len);
        let num_non_empty_blocks = block_jobs.iter().filter(|blocks| !blocks.is_empty()).count();

        let res = self.row_readers[..num_non_empty_blocks].par_iter_mut().enumerate().map(|(i, reader)| {
            let blocks = &block_jobs[i];
            if blocks.is_empty() {
                return Ok(Vec::new());
            }

            let (position_value_start, _) = blocks.first().unwrap().0;
            let (position_value_end, _) = blocks.last().unwrap().1;
            reader.query_range(*position_value_start, *position_value_end)
        }).collect::<Result<Vec<_>, _>>()?;

        let len = res.iter().map(Vec::len).sum();
        let flattened = res
            .into_iter()
            .flat_map(|inner| inner)
            .map(|row| row.into_py(py));
        Ok(new_from_iter(py, len, &mut flattened.into_iter()).into())
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
