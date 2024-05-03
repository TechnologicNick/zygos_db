use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};

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

#[derive(Debug)]
pub struct NotEnoughLinesError;

impl std::fmt::Display for NotEnoughLinesError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Not enough lines to guess column types.")
    }
}

pub struct TabSeparatedFileReader {
    reader: BufReader<File>,
}

impl TabSeparatedFileReader {
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
        }
    }

    /// Reads a line from the file and splits it by tabs.
    pub fn read_line_and_split(&mut self) -> std::io::Result<Vec<String>> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;

        if line.is_empty() {
            return Ok(vec![]);
        }

        Ok(line.trim().split('\t').map(|s| s.to_string()).collect())
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
    pub fn guess_column_types_but_better(&mut self, column_indices: Vec<usize>, volatile_threshold_fraction: f32, min_sample_size: usize) -> Result<std::collections::HashMap<usize, ColumnType>, NotEnoughLinesError> {
        let mut column_possibly_float: HashMap<usize, bool> = column_indices.iter().map(|&i| (i, true)).collect();
        let mut column_possibly_integer: HashMap<usize, bool> = column_indices.iter().map(|&i| (i, true)).collect();
        let mut column_possibly_hashtable_string: HashMap<usize, bool> = column_indices.iter().map(|&i| (i, true)).collect();

        // We only keep track of the hashes of the values to save memory, as we don't need to store the actual values.
        let mut column_value_hashes: std::collections::HashMap<usize, std::collections::HashSet<u64>> = std::collections::HashMap::new();

        let mut loop_counter: usize = 0;

        loop {
            loop_counter += 1;

            let row = self.read_line_and_split().unwrap();
            if row.is_empty() {
                break;
            }

            for (i, value) in row.iter().enumerate() {
                if !column_indices.contains(&i) {
                    continue;
                }

                if column_possibly_float[&i] {
                    if value.parse::<f64>().is_err() {
                        column_possibly_float.insert(i, false);
                    }
                } else if column_possibly_integer[&i] {
                    if value.parse::<i64>().is_err() {
                        column_possibly_integer.insert(i, false);
                    }
                }

                if column_possibly_hashtable_string[&i] {
                    let mut hasher = DefaultHasher::new();
                    value.hash(&mut hasher);
                    let value_hash = hasher.finish();
    
                    let hashes = column_value_hashes.entry(i).or_insert_with(std::collections::HashSet::new);
                    hashes.insert(value_hash);

                    if loop_counter >= min_sample_size && hashes.len() > (loop_counter as f32 * volatile_threshold_fraction) as usize {
                        println!("Determined column {} to be volatile after {} iterations.", i, loop_counter);
                        column_possibly_hashtable_string.insert(i, false);
                        column_value_hashes.remove(&i);
                    }
                }
            }
        }

        if loop_counter < min_sample_size {
            return Err(NotEnoughLinesError);
        }

        let mut column_types = std::collections::HashMap::new();

        for i in column_indices.iter() {
            if column_possibly_float[i] {
                column_types.insert(*i, ColumnType::Float);
                continue;
            }

            if column_possibly_integer[i] {
                column_types.insert(*i, ColumnType::Integer);
                continue;
            }

            if column_possibly_hashtable_string[i] {
                column_types.insert(*i, ColumnType::HashtableString);
            } else {
                column_types.insert(*i, ColumnType::VolatileString);
            }
        }

        Ok(column_types)
    }
}
