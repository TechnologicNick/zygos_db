use std::{collections::HashMap, path::PathBuf};

use serde::Deserialize;
use crate::tsv_reader::ColumnType;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(skip)]
    metadata: Option<ConfigMetadata>,
    pub datasets: HashMap<String, Dataset>,
}

#[derive(Debug)]
pub struct ConfigMetadata {
    pub config_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Dataset {
    pub file_per_chromosome: bool,
    pub chromosomes: Option<Vec<u8>>,
    pub path: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: ColumnType,
    #[serde(default)]
    pub role: ColumnRole,
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
        }

        if !dataset.path.contains("{chromosome}") {
            return Err("'path' must contain '{chromosome}' when 'file_per_chromosome' is true".to_string());
        }

        for path in dataset.get_paths(&self.metadata.as_ref().unwrap().config_path).values() {
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

        Ok(())
    }
}

impl Dataset {
    /// Get the paths to the dataset files.
    pub fn get_paths(&self, config_path: &PathBuf) -> HashMap<u8, PathBuf> {
        let config_dir = config_path.parent().unwrap();

        if self.file_per_chromosome {
            let mut sorted = self.chromosomes.as_ref().unwrap().to_owned();
            sorted.sort();
            sorted.iter().map(|&chromosome| {
                (chromosome, config_dir.join(self.path.replace("{chromosome}", &chromosome.to_string())))
            }).collect()
        } else {
            let mut paths = HashMap::new();
            paths.insert(0, config_dir.join(self.path.to_owned()));
            paths
        }
    }
}