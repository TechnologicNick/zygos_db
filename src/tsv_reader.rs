use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use clap::ValueEnum;

use flate2::read::MultiGzDecoder;
use serde::Deserialize;

use crate::config::{Column, ColumnRole};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ColumnType {
    /// Column contains only integers.
    Integer = 0,
    /// Column contains only floats.
    Float = 1,
    /// Column contains a lot of different strings, but there can be duplicates.
    VolatileString = 2,
    /// Column contains strings that are repeated many times.
    HashtableString = 3,
}

impl ColumnType {
    fn get_cell_value(&self, value: &str) -> Result<CellValue, String> {
        match self {
            Self::Integer => {
                match value.parse() {
                    Ok(value) => Ok(CellValue::Integer(value)),
                    Err(_) => Err(format!("Failed to parse value '{:?}' as integer.", value)),
                }
            },
            Self::Float => {
                match value.parse() {
                    Ok(value) => Ok(CellValue::Float(value)),
                    Err(_) => Err(format!("Failed to parse value '{:?}' as float.", value)),
                }
            },
            Self::VolatileString => Ok(CellValue::String(value.to_owned())),
            Self::HashtableString => Ok(CellValue::String(value.to_owned())),
        }
    }
}

impl TryFrom<u8> for ColumnType {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Integer),
            1 => Ok(Self::Float),
            2 => Ok(Self::VolatileString),
            3 => Ok(Self::HashtableString),
            _ => Err(()),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ValueEnum, Deserialize)]
pub enum MissingValuePolicy {
    /// Omit the row if there is a missing value in it.
    OmitRow,
    /// Panic if there is a missing value in the row.
    Throw,
    /// Replace the missing value with an empty string.
    ReplaceWithEmptyString,
}

impl Default for MissingValuePolicy {
    fn default() -> Self {
        Self::Throw
    }
}

#[derive(Debug)]
pub enum CellValue {
    Integer(i64),
    Float(f64),
    String(String),
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
    Gzipped(MultiGzDecoder<File>),
}

impl FileReader {
    pub fn new(file: File) -> Self {
        let mut magic_bytes = [0; 2];

        file.try_clone().unwrap().read_exact(magic_bytes.as_mut()).unwrap();

        file.try_clone().unwrap().seek(SeekFrom::Start(0)).unwrap();

        if magic_bytes == [0x1f, 0x8b] {
            return Self::Gzipped(MultiGzDecoder::new(file));
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

/// A fast iterator that splits a string by a character, but ignores the character if it is inside a string.
pub struct FastSplit<'a> {
    buf: &'a str,
    split_on: char,
    start: usize,
    end: usize,
    is_in_string: bool,
}

impl<'a> FastSplit<'a> {
    fn new(buf: &'a str, split_on: char) -> Self {
        Self {
            buf,
            split_on,
            start: 0,
            end: 0,
            is_in_string: false,
        }
    }
}

impl<'a> Iterator for FastSplit<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end == self.buf.len() {
            return None;
        }

        let mut in_string = self.is_in_string;
        let start = self.start;
        let mut end = self.end;

        for (i, c) in self.buf[self.end..].char_indices() {
            end = self.end + i;

            if c == '"' {
                in_string = !in_string;
            }

            if c == self.split_on && !in_string {
                self.start = end + 1;
                self.end = end + 1;
                self.is_in_string = in_string;

                return Some(&self.buf[start..end]);
            }
        }

        self.start = end + 1;
        self.end = end + 1;
        self.is_in_string = in_string;

        Some(&self.buf[start..end + 1])
    }
}

pub struct TabSeparatedFileReader {
    reader: BufReader<FileReader>,
    split_on: char,
}

impl TabSeparatedFileReader {
    pub fn new(file: File) -> Self {
        Self::with_capacity(0x8000, file)
    }

    pub fn with_capacity(capacity: usize, file: File) -> Self {
        Self {
            reader: BufReader::with_capacity(capacity, FileReader::new(file)),
            split_on: '\t',
        }
    }

    /// Reads a line from the file and splits it by tabs.
    pub fn read_line_and_split<'a>(&'a mut self, line_buf: &'a mut String) -> Option<FastSplit> {
        line_buf.clear();
        self.reader.read_line(line_buf).unwrap();

        if line_buf.is_empty() {
            return None;
        }

        Some(FastSplit::new(line_buf.trim_end(), self.split_on))
    }

    /// Skips a number of lines in the file.
    pub fn skip_lines(&mut self, n: usize) -> std::io::Result<()>{
        for _ in 0..n {
            self.reader.read_line(&mut String::new())?;
        }

        Ok(())
    }

    /// Reads the header of the file.
    pub fn read_header(&mut self) -> Result<Vec<String>, String> {
        let mut line_buf = String::new();

        let split_tabs: Vec<_> = match self.read_line_and_split(&mut line_buf) {
            Some(header) => header.map(|s| s.to_owned()).collect(),
            None => return Err("Empty file.".to_string()),
        };

        if split_tabs.len() > 1 {
            return Ok(split_tabs);
        }


        let split_commas: Vec<_> = FastSplit::new(&line_buf.trim_end(), ',').map(|s| s.to_owned()).collect();

        if split_commas.len() > 1 {
            self.split_on = ',';
            return Ok(split_commas);
        }


        Err("Unable to determine the delimiter.".to_string())
    }

    /// Finds the indices of the columns with the given names in the header.
    pub fn find_column_indices(&mut self, column_names: &Vec<String>) -> Result<Vec<(String, usize)>, String> {
        let header = self.read_header()?;

        let mut column_indices = Vec::new();

        for column_name in column_names {
            match header.iter().position(|s| s == column_name) {
                Some(i) => column_indices.push((column_name.to_owned(), i)),
                None => return Err(format!("Column '{}' not found in header.", column_name)),
            }
        }

        Ok(column_indices)
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
        columns: HashMap<usize, MissingValuePolicy>,
        volatile_threshold_fraction: f32,
        min_sample_size: usize
    ) -> Result<HashMap<usize, ColumnType>, NotEnoughLinesError> {
        let mut sorted_column_indices: Vec<usize> = columns.keys().copied().collect();
        sorted_column_indices.sort();

        let mut column_possibly_float: Vec<bool> = sorted_column_indices.iter().map(|_| true).collect();
        let mut column_possibly_integer: Vec<bool> = sorted_column_indices.iter().map(|_| true).collect();
        let mut column_possibly_hashtable_string: Vec<bool> = sorted_column_indices.iter().map(|_| true).collect();

        // We only keep track of the hashes of the values to save memory, as we don't need to store the actual values.
        let mut column_value_hashes: HashMap<usize, HashSet<u64>> = HashMap::new();

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
                        MissingValuePolicy::Throw => panic!("Missing value in column {} in row {}.", wide_index, loop_counter),
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
    
                    let hashes = column_value_hashes.entry(narrow_index).or_insert_with(HashSet::new);
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

        let mut column_types = HashMap::new();

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

    pub fn read_all(&mut self, columns: &Vec<(usize, &Column)>) -> Result<Vec<Vec<CellValue>>, String> {
        let mut line_buf = String::new();
        let mut loop_counter: usize = 0;

        let mut rows: Vec<Vec<CellValue>> = Vec::new();

        'row_loop: loop {
            loop_counter += 1;

            let row: Vec<&str> = match self.read_line_and_split(&mut line_buf) {
                Some(row) => row.collect(),
                None => break,
            };

            for (wide_index, column) in columns.iter() {
                match row.get(*wide_index) {
                    Some(_) => {},
                    None => {
                        match column.missing_value_policy {
                            MissingValuePolicy::OmitRow => continue 'row_loop,
                            MissingValuePolicy::Throw => return Err(format!("Missing value in column {} in row {}.", wide_index, loop_counter)),
                            MissingValuePolicy::ReplaceWithEmptyString => {}, // Do nothing, as the value is already an empty string.
                        }
                    }
                };
            }

            let parsed = columns.iter().map(|(wide_index, column)| {
                let value = row.get(*wide_index).expect("Column index out of bounds");

                column.type_.get_cell_value(value)
            }).collect::<Result<Vec<CellValue>, String>>();

            match parsed {
                Ok(parsed) => rows.push(parsed),
                Err(e) => return Err(e),
            }
        }

        Ok(rows)
    }

    pub fn convert_read_data(&mut self, columns: &Vec<Column>, mut rows: Vec<Vec<CellValue>>) -> Result<Vec<Vec<CellValue>>, String> {
        assert!(columns[0].role == ColumnRole::Position || columns[0].role == ColumnRole::PositionStart, "First column must be a position.");

        println!("First row: {:?}", rows[0]);

        rows.sort_by(|a, b| {
            match (&a[0], &b[0]) {
                (CellValue::Integer(a), CellValue::Integer(b)) => a.cmp(b),
                _ => panic!("Values in first column must be integers. Found '{:?}' and '{:?}'.", a[0], b[0]),
            }
        });

        Ok(rows)
    }
}
