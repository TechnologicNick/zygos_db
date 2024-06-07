use std::{collections::BTreeMap, io::{Error, ErrorKind, Read, Seek, SeekFrom}, mem::size_of};
use serde::Deserialize;

use crate::{database::{HEADER_MAGIC, INDEX_MAGIC}, tsv_reader::ColumnType};

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseHeader {
    pub version: u8,
    pub datasets: Vec<DatasetHeader>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatasetHeader {
    pub name: String,
    pub columns: Vec<ColumnHeader>,
    pub tables: Vec<TableHeader>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ColumnHeader {
    #[serde(rename = "type")]
    pub type_: ColumnType,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TableHeader {
    pub chromosome: u8,
    pub offset: u64,
}

pub struct DatabaseQueryClient<R: Read + Seek> {
    reader: R,
}

impl<R: Read + Seek> DatabaseQueryClient<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
        }
    }

    pub fn read_u64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0; size_of::<u64>()];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    pub fn read_u8(&mut self) -> std::io::Result<u8> {
        let mut buf = [0; size_of::<u8>()];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_vint64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0u8; 9];
        self.reader.read_exact(&mut buf[0..1])?;
        let len = vint64::decoded_len(buf[0]);

        self.reader.read_exact(&mut buf[1..len])?;
        let mut slice = &buf[..len];

        Ok(vint64::decode(&mut slice).unwrap())
    }

    pub fn read_string_u8(&mut self) -> std::io::Result<String> {
        let len = self.read_u8()? as usize;
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf)?;
        Ok(String::from_utf8(buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))?)
    }

    pub fn read_database_header(&mut self) -> std::io::Result<DatabaseHeader> {
        self.reader.seek(SeekFrom::Start(0))?;

        {
            let mut buf_magic = [0; HEADER_MAGIC.len()];
            self.reader.read_exact(&mut buf_magic)?;
            if buf_magic != HEADER_MAGIC {
                let err_msg = format!(
                    "Invalid database magic: expected {:?}, got {:?}",
                    HEADER_MAGIC, buf_magic
                );
                return Err(Error::new(ErrorKind::InvalidData, err_msg));
            }
        }

        let version = self.read_u8()?;
        let num_datasets = self.read_u8()? as usize;

        let mut datasets = Vec::with_capacity(num_datasets);

        for _ in 0..num_datasets {
            let name = self.read_string_u8()?;
            let num_columns = self.read_u8()? as usize;

            let mut columns = Vec::with_capacity(num_columns);

            for _ in 0..num_columns {
                let type_id = self.read_u8()?;
                let type_ = ColumnType::try_from(type_id)
                    .map_err(|_| Error::new(ErrorKind::InvalidData, format!("Unknown column type with id {}", type_id)))?;
                let name = self.read_string_u8()?;

                columns.push(ColumnHeader{ type_, name });
            }

            let num_tables = self.read_u8()? as usize;

            let mut tables = Vec::with_capacity(num_tables);

            for _ in 0..num_tables {
                let chromosome = self.read_u8()?;
                let offset = self.read_u64()?;

                tables.push(TableHeader{ chromosome, offset });
            }

            datasets.push(DatasetHeader{ name, columns, tables });
        }

        Ok(DatabaseHeader{ version, datasets })
    }

    pub fn read_table_index(&mut self, offset: u64) -> std::io::Result<TableIndex> {
        self.reader.seek(SeekFrom::Start(offset))?;

        {
            let mut buf_magic = [0; INDEX_MAGIC.len()];
            self.reader.read_exact(&mut buf_magic)?;
            if buf_magic != INDEX_MAGIC {
                let err_msg = format!(
                    "Invalid table index magic at offset {}: expected {:?}, got {:?}",
                    offset, INDEX_MAGIC, buf_magic
                );
                return Err(Error::new(ErrorKind::InvalidData, err_msg));
            }
        }

        let end_offset = self.read_u64()?;
        let num_indices = self.read_u64()?;

        let mut res = BTreeMap::new();

        for _ in 0..num_indices {
            let position = self.read_vint64()?;
            let offset = self.read_vint64()?;

            res.insert(position, offset);
        }

        Ok(TableIndex{ inner: res, end_offset })
    }
}

pub struct TableIndex {
    pub inner: BTreeMap<u64, u64>,
    pub end_offset: u64,
}

impl TableIndex {
    pub fn get_all(&self) -> Vec<(u64, u64)> {
        self.inner.iter().map(|(k, v)| (*k, *v)).collect()
    }

    /// Get all indices in the range
    /// 
    /// # Arguments
    /// 
    /// * `start` - The start of the range (inclusive)
    /// * `end` - The end of the range (exclusive)
    /// 
    /// # Returns
    /// 
    /// A vector of tuples, where the first element is the position and the second element is the offset
    pub fn get_range(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        // We use Bound::Included and then cursor.prev() to get the index closest to the start, but not greater than it
        let mut cursor = self.inner.upper_bound(std::ops::Bound::Included(&start));
        cursor.prev();

        let mut indices = Vec::new();
        
        loop {
            match cursor.next() {
                Some((k, v)) => {
                    if *k >= end {
                        break;
                    }
                    indices.push((*k, *v));
                },
                None => break,
            }
        }

        indices
    }
}