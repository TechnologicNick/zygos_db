use std::io::{Read, Write};


pub enum CompressionAlgorithm {
    None = 0,
    Gzip = 1,
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

    pub fn decompress(&self, bytes: &[u8], buffer: &mut Vec<u8>) -> std::io::Result<usize> {
        match self.algorithm {
            CompressionAlgorithm::None => {
                buffer.clear();
                buffer.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            CompressionAlgorithm::Gzip => {
                let mut decoder = flate2::read::GzDecoder::new(bytes);
                buffer.clear();
                decoder.read_to_end(buffer)?;
                Ok(buffer.len())
            }
        }
    }
}