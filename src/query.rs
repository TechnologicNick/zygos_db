use std::{collections::BTreeMap, io::{Read, Seek}, mem::size_of};
use crate::database::INDEX_MAGIC;



pub struct DatabaseQueryClient<R: Read + Seek> {
    reader: R,
}

impl<R: Read + Seek> DatabaseQueryClient<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
        }
    }

    pub fn read_table_index(&mut self, offset: u64) -> std::io::Result<BTreeMap<u64, u64>> {
        self.reader.seek(std::io::SeekFrom::Start(offset))?;

        {
            let mut buf_magic = [0; INDEX_MAGIC.len()];
            self.reader.read_exact(&mut buf_magic)?;
            if buf_magic != INDEX_MAGIC {
                let err_msg = format!(
                    "Invalid table index magic at offset {}: expected {:?}, got {:?}",
                    offset, INDEX_MAGIC, buf_magic
                );
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, err_msg));
            }
        }

        let mut buf_u64 = [0; size_of::<u64>()];

        self.reader.read_exact(&mut buf_u64)?;
        let num_indices = u64::from_be_bytes(buf_u64);

        let mut res = BTreeMap::new();

        for _ in 0..num_indices {
            self.reader.read_exact(&mut buf_u64)?;
            let position = u64::from_be_bytes(buf_u64);

            self.reader.read_exact(&mut buf_u64)?;
            let offset = u64::from_be_bytes(buf_u64);

            res.insert(position, offset);
        }

        Ok(res)
    }
}

