use std::io::{Read, Write};

use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompressionAlgorithm {
    None = 0,
    Gzip = 1,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        CompressionAlgorithm::None
    }
}

impl TryFrom<u8> for CompressionAlgorithm {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(CompressionAlgorithm::None),
            1 => Ok(CompressionAlgorithm::Gzip),
            _ => Err(()),
        }
    }
}

pub struct RowCompressor {
    pub(crate) buffer: Vec<u8>,
}

impl RowCompressor {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
        }
    }

    pub fn compress(&mut self, algorithm: CompressionAlgorithm, bytes: &mut impl Write) -> std::io::Result<usize> {
        match algorithm {
            CompressionAlgorithm::None => {
                bytes.write(&self.buffer)
            }
            CompressionAlgorithm::Gzip => {
                let mut encoder = flate2::write::GzEncoder::new(bytes, flate2::Compression::best());
                encoder.write_all(&self.buffer)?;
                encoder.try_finish()?;
                Ok(self.buffer.len())
            }
        }
    }
}

pub struct RowDecompressor {
    algorithm: CompressionAlgorithm,
}

#[allow(dead_code)]
impl RowDecompressor {
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self {
            algorithm,
        }
    }

    pub fn decompress<'a>(&self, bytes: &'a [u8], buffer: &'a mut Vec<u8>) -> std::io::Result<&'a [u8]> {
        match self.algorithm {
            CompressionAlgorithm::None => {
                Ok(bytes)
            }
            CompressionAlgorithm::Gzip => {
                let mut decoder = flate2::read::GzDecoder::new(bytes);
                buffer.clear();
                decoder.read_to_end(buffer)?;
                Ok(buffer.as_slice())
            }
        }
    }
}