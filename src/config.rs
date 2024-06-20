use std::{collections::HashMap, path::PathBuf};

use serde::Deserialize;
use crate::{compression::CompressionAlgorithm, tsv_reader::{ColumnType, MissingValuePolicy}};

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub metadata: Option<ConfigMetadata>,
    pub datasets: HashMap<String, Dataset>,
}

#[derive(Debug)]
pub struct ConfigMetadata {
    pub config_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Dataset {
    #[serde(skip)]
    pub metadata: Option<DatasetMetadata>,
    pub file_per_chromosome: bool,
    pub chromosomes: Option<Vec<u8>>,
    pub path: String,
    pub columns: Vec<Column>,
    pub rows_per_index: usize,
    pub compression_algorithm: CompressionAlgorithm,
}

#[derive(Debug)]
pub struct DatasetMetadata {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ColumnType,
    #[serde(default)]
    pub role: ColumnRole,
    #[serde(default)]
    pub missing_value_policy: MissingValuePolicy,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Clone, Copy, Hash)]
#[serde(rename_all = "kebab-case")]
#[repr(u8)]
pub enum ColumnRole {
    Position,
    PositionStart,
    PositionEnd,
    Data = u8::MAX,
}

impl Default for ColumnRole {
    fn default() -> Self {
        ColumnRole::Data
    }
}

impl Config {
    /// Load a config file from a path. Panics if the file cannot be read.
    pub fn from_file(path: &str) -> Result<Self, toml::de::Error> {
        let config_str = std::fs::read_to_string(path).expect("Could not read config file");
        let mut res: Config = toml::from_str(&config_str)?;

        res.metadata = Some(ConfigMetadata {
            config_path: PathBuf::from(path),
        });

        for (name, dataset) in &mut res.datasets {
            dataset.metadata = Some(DatasetMetadata {
                name: name.to_owned(),
            });
        }

        Ok(res)
    }

    /// Validate the config file. Returns an error message if the config is invalid.
    pub fn validate(&self) -> Result<(), String> {
        for (name, dataset) in &self.datasets {
            self.validate_dataset(dataset).map_err(|e| format!("Dataset '{}': {}", name, e))?;
        }

        Ok(())
    }

    fn validate_dataset(&self, dataset: &Dataset) -> Result<(), String> {
        self.validate_path(dataset)?;
        self.validate_columns(dataset)?;

        match dataset.metadata.as_ref() {
            Some(metadata) => {
                if metadata.name.len() > 255 {
                    return Err(format!("Dataset name '{}' is too long (max 255 characters)", metadata.name));
                }
            },
            None => panic!("metadata must be present")
        }

        if dataset.rows_per_index == 0 {
            return Err("'rows_per_index' must be greater than 0".to_string());
        }

        Ok(())
    }

    fn validate_path(&self, dataset: &Dataset) -> Result<(), String> {
        if dataset.file_per_chromosome {
            match &dataset.chromosomes {
                Some(chromosomes) => {
                    if chromosomes.is_empty() {
                        return Err("'chromosomes' cannot be empty when 'file_per_chromosome' is true".to_string());
                    }
                },
                None => return Err("'chromosomes' must be specified when 'file_per_chromosome' is true".to_string()),
            }
        } else {
            return Err("Datasets with 'file_per_chromosome' set to false are currently not supported".to_string());
        }

        if !dataset.path.contains("{chromosome}") {
            return Err("'path' must contain '{chromosome}' when 'file_per_chromosome' is true".to_string());
        }

        for path in dataset.get_paths(&self.metadata.as_ref().unwrap().config_path).iter().map(|(_, path)| path) {
            if !path.is_file() {
                return Err(format!("File '{}' does not exist", path.display()));
            }
        }

        Ok(())
    }

    fn validate_columns(&self, dataset: &Dataset) -> Result<(), String> {
        let mut column_role_counts = HashMap::new();
        for column in &dataset.columns {
            let count = column_role_counts.entry(column.role).or_insert(0);
            *count += 1;
        }

        match (
            column_role_counts.get(&ColumnRole::Position),
            column_role_counts.get(&ColumnRole::PositionStart),
            column_role_counts.get(&ColumnRole::PositionEnd),
        ) {
            (None, None, None) => return Err("No columns have the role 'position' or 'position-start' or 'position-end'".to_string()),
            (Some(1), None, None) => {},
            (Some(_), None, None) => return Err("Only one column may have the role 'position'".to_string()),
            (None, Some(1), Some(1)) => {},
            (None, Some(_), Some(_)) => return Err("Only one column may have the role 'position-start' and only one column may have the role 'position-end'".to_string()),
            (Some(_), _, _) => return Err("If a column has the role 'position', no columns may have roles 'position-start' or 'position-end'".to_string()),
            (None, None, Some(_)) => return Err("If a column has the role 'position-end', a column with the role 'position-start' must be present".to_string()),
            (None, Some(_), None) => return Err("If a column has the role 'position-start', a column with the role 'position-end' must be present".to_string()),
        };

        for column in &dataset.columns {
            if column.role == ColumnRole::Position && column.type_ != ColumnType::Integer {
                return Err(format!("Column '{}' with the role 'position' must have the type 'integer'", column.name).to_string());
            } else if column.role == ColumnRole::PositionStart && column.type_ != ColumnType::Integer {
                return Err(format!("Column '{}' with the role 'position-start' must have the type 'integer'", column.name).to_string());
            } else if column.role == ColumnRole::PositionEnd && column.type_ != ColumnType::Integer {
                return Err(format!("Column '{}' with the role 'position-end' must have the type 'integer'", column.name).to_string());
            }
        }

        for (i, column) in dataset.columns.iter().enumerate() {
            if column.name.len() > 255 {
                return Err(format!("Column name '{}' is too long (max 255 characters)", column.name));
            }

            if i == 0 && column_role_counts.get(&ColumnRole::Position).is_some() && column.role != ColumnRole::Position {
                return Err("The column with role 'position' must be the first column".to_string());
            } else if i == 0 && column_role_counts.get(&ColumnRole::PositionStart).is_some() && column.role != ColumnRole::PositionStart {
                return Err("The column with role 'position-start' must be the first column".to_string());
            } else if i == 1 && column_role_counts.get(&ColumnRole::PositionEnd).is_some() && column.role != ColumnRole::PositionEnd {
                return Err("The column with role 'position-end' must be the second column".to_string());
            }
        }

        Ok(())
    }
}

impl Dataset {
    /// Get the paths to the dataset files.
    pub fn get_paths(&self, config_path: &PathBuf) -> Vec<(u8, PathBuf)> {
        let config_dir = config_path.parent().unwrap();

        if self.file_per_chromosome {
            let mut sorted = self.chromosomes.as_ref().unwrap().to_owned();
            sorted.sort();
            sorted.iter().map(|&chromosome| {
                (chromosome, config_dir.join(self.path.replace("{chromosome}", &chromosome.to_string())))
            }).collect()
        } else {
            let mut paths = Vec::new();
            paths.push((0, config_dir.join(self.path.to_owned())));
            paths
        }
    }
}
