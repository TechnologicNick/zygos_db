use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use clap::ValueEnum;

use flate2::read::GzDecoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    /// Column contains only integers.
    Integer,
    /// Column contains only floats.
    Float,
    /// Column contains a lot of different strings, but there can be duplicates.
    VolatileString,
    /// Column contains strings that are repeated many times.
    HashtableString,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum)]
pub enum MissingValuePolicy {
    /// Omit the row if there is a missing value in it.
    OmitRow,
    /// Panic if there is a missing value in the row.
    Panic,
    /// Replace the missing value with an empty string.
    ReplaceWithEmptyString,
}

#[derive(Debug)]
pub struct NotEnoughLinesError;

impl std::fmt::Display for NotEnoughLinesError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Not enough lines to guess column types.")
    }
}

pub enum FileReader {
    Regular(File),
    Gzipped(GzDecoder<File>),
}

impl FileReader {
    pub fn new(file: File) -> Self {
        let mut magic_bytes = [0; 2];

        BufReader::new(file.try_clone().unwrap()).read_exact(magic_bytes.as_mut()).unwrap();

        file.try_clone().unwrap().seek(SeekFrom::Start(0)).unwrap();

        if magic_bytes == [0x1f, 0x8b] {
            return Self::Gzipped(GzDecoder::new(file));
        } else {
            return Self::Regular(file);
        }
    }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Regular(file) => file.read(buf),
            Self::Gzipped(gzipped_file) => gzipped_file.read(buf),
        }
    }
}

pub struct TabSeparatedFileReader {
    reader: BufReader<FileReader>,
}

impl TabSeparatedFileReader {
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(FileReader::new(file)),
        }
    }

    /// Reads a line from the file and splits it by tabs.
    pub fn read_line_and_split<'a>(&'a mut self, line_buf: &'a mut String) -> Option<std::str::Split<'_, char>> {
        line_buf.clear();
        self.reader.read_line(line_buf).unwrap();

        if line_buf.is_empty() {
            return None;
        }

        Some(line_buf.split('\t'))
    }

    /// Reads all lines of the file and guesses the column types based on the contents of the columns.
    /// The contents of the read lines are discarded.
    /// 
    /// # Arguments
    /// 
    /// * `column_indices` - The indices of the columns to guess the types of.
    /// * `volatile_threshold_fraction` - The fraction between 0 and 1 of the number of distinct values in a column that determines if the column is considered a volatile string column.
    /// * `min_sample_size` - The minimum number of lines to read to guess the column types.
    /// 
    /// # Returns
    /// 
    /// * A dictionary where the keys are the column indices and the values are the column types.
    pub fn guess_column_types_but_better(
        &mut self,
        columns: std::collections::HashMap<usize, MissingValuePolicy>,
        volatile_threshold_fraction: f32,
        min_sample_size: usize
    ) -> Result<std::collections::HashMap<usize, ColumnType>, NotEnoughLinesError> {
        let mut sorted_column_indices: Vec<usize> = columns.keys().copied().collect();
        sorted_column_indices.sort();

        let mut column_possibly_float: Vec<bool> = sorted_column_indices.iter().map(|_| true).collect();
        let mut column_possibly_integer: Vec<bool> = sorted_column_indices.iter().map(|_| true).collect();
        let mut column_possibly_hashtable_string: Vec<bool> = sorted_column_indices.iter().map(|_| true).collect();

        // We only keep track of the hashes of the values to save memory, as we don't need to store the actual values.
        let mut column_value_hashes: std::collections::HashMap<usize, std::collections::HashSet<u64>> = std::collections::HashMap::new();

        let mut loop_counter: usize = 0;

        let mut line_buf = String::new();
        
        'row_loop: loop {
            loop_counter += 1;
            let mut cell_bufs: Vec<&str> = sorted_column_indices.iter().map(|_| "").collect();

            let row = match self.read_line_and_split(&mut line_buf) {
                Some(row) => row,
                None => break,
            };

            let mut current_cell_buf_index = 0;
            for (wide_index, value) in row.enumerate() {
                if !columns.contains_key(&wide_index) {
                    continue;
                }

                cell_bufs[current_cell_buf_index] = value;
                current_cell_buf_index += 1;

                if value.is_empty() {
                    match columns[&wide_index] {
                        MissingValuePolicy::OmitRow => continue 'row_loop,
                        MissingValuePolicy::Panic => panic!("Missing value in column {} in line {}.", wide_index, loop_counter),
                        MissingValuePolicy::ReplaceWithEmptyString => {}, // Do nothing, as the value is already an empty string.
                    }
                }
            }
            
            for (narrow_index, value) in cell_bufs.iter().enumerate() {

                if column_possibly_integer[narrow_index] {
                    if value.parse::<i64>().is_err() {
                        println!("Failed to parse value {:?} as integer in column {}.", value, sorted_column_indices[narrow_index]);
                        column_possibly_integer.insert(narrow_index, false);
                    }
                }

                if column_possibly_float[narrow_index] {
                    if value.parse::<f64>().is_err() {
                        println!("Failed to parse value {:?} as float in column {}.", value, sorted_column_indices[narrow_index]);
                        column_possibly_float.insert(narrow_index, false);
                    }
                }

                if column_possibly_hashtable_string[narrow_index] {
                    let mut hasher = DefaultHasher::new();
                    value.hash(&mut hasher);
                    let value_hash = hasher.finish();
    
                    let hashes = column_value_hashes.entry(narrow_index).or_insert_with(std::collections::HashSet::new);
                    hashes.insert(value_hash);

                    if loop_counter >= min_sample_size && hashes.len() > (loop_counter as f32 * volatile_threshold_fraction) as usize {
                        println!("Determined column {} to be volatile after {} iterations.", sorted_column_indices[narrow_index], loop_counter);
                        column_possibly_hashtable_string.insert(narrow_index, false);
                        column_value_hashes.remove(&narrow_index);
                    }
                }
            }
        }

        if loop_counter < min_sample_size {
            return Err(NotEnoughLinesError);
        }

        let mut column_types = std::collections::HashMap::new();

        for (narrow_index, wide_index) in sorted_column_indices.iter().enumerate() {
            if column_possibly_integer[narrow_index] {
                column_types.insert(*wide_index, ColumnType::Integer);
                continue;
            }

            if column_possibly_float[narrow_index] {
                column_types.insert(*wide_index, ColumnType::Float);
                continue;
            }

            if column_possibly_hashtable_string[narrow_index] {
                column_types.insert(*wide_index, ColumnType::HashtableString);
            } else {
                column_types.insert(*wide_index, ColumnType::VolatileString);
            }
        }

        Ok(column_types)
    }
}
