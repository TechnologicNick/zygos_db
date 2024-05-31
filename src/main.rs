mod tsv_reader;
mod config;
mod database;

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use ascii_table::AsciiTable;
use crossterm::tty::IsTty;

/// ZygosDB: A database for storing and querying genetic data.
#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Read a TSV file and guess the column types.
    GuessColumnTypes(GuessColumnTypesArgs),
    /// Parse the TSV header and the first few rows and prints a formatted table.
    Sample(SampleArgs),
    /// Build the database from a config file.
    Build(BuildArgs),
}

#[derive(Args)]
struct GuessColumnTypesArgs {
    /// The TSV file to read.
    file: String,
    /// Column names to guess the types of.
    #[arg(short, long)]
    column_names: Vec<String>,
    /// The fraction between 0 and 1 of the number of distinct values in a column that determines if the column is considered a volatile string column.
    #[arg(short, long, default_value_t = 0.2)]
    volatile_threshold_fraction: f32,
    /// The minimum number of lines to read to guess the column types.
    #[arg(short, long, default_value_t = 1000)]
    min_sample_size: usize,
    /// The policy to use for missing values.
    #[arg(value_enum, short = 'p', long, default_value_t = tsv_reader::MissingValuePolicy::ReplaceWithEmptyString)]
    missing_value_policy: tsv_reader::MissingValuePolicy,
}

#[derive(Args)]
struct SampleArgs {
    /// The TSV file to read.
    file: String,
    /// The number of rows to read.
    #[arg(short = 'n', long, default_value_t = 10)]
    rows: usize,
    /// The number of rows to skip.
    #[arg(short = 's', long, default_value_t = 0)]
    skip: usize,
    /// The maximum width of the table. If not specified, the width of the terminal is used.
    #[arg(short = 'w', long)]
    max_width: Option<usize>,
}

#[derive(Args)]
struct BuildArgs {
    /// The path to the configuration file.
    config: String,
    /// The path to the built database.
    /// If not specified, the database is built in the directory of the configuration file with the extension `.zygosdb`.
    /// If the database already exists, it is overwritten.
    #[arg(short, long)]
    output: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::GuessColumnTypes(args) => guess_column_types(args),
        Commands::Sample(args) => sample(args),
        Commands::Build(args) => build(args),
    }
}

fn guess_column_types(args: GuessColumnTypesArgs) {
    let file = std::fs::File::open(args.file).unwrap();
    let mut reader: tsv_reader::TabSeparatedFileReader = tsv_reader::TabSeparatedFileReader::new(file);

    let mut line_buf = String::new();
    let found_column_names: Vec<String> = reader.read_line_and_split(&mut line_buf).unwrap().map(|s| s.to_owned()).collect();

    // Verify all column names are present
    for column_name in args.column_names.iter() {
        if !found_column_names.contains(column_name) {
            eprintln!("Column name '{}' not found in file.", column_name);
            std::process::exit(1);
        }
    }

    let interesting_column_indices: std::collections::HashMap<usize, tsv_reader::MissingValuePolicy> = found_column_names.iter().enumerate().filter_map(|(i, header)| {
        // If the column name is in the list of column names to guess, or if the list is empty, include the column
        if args.column_names.contains(&header) || args.column_names.is_empty() {
            Some(i)
        } else {
            None
        }
    }).map(|i| (i, args.missing_value_policy)).collect();

    println!("Interesting column indices: {:?}", interesting_column_indices);
    
    let column_types = reader.guess_column_types_but_better(
        interesting_column_indices,
        args.volatile_threshold_fraction,
        args.min_sample_size
    ).unwrap();

    let named_column_types: std::collections::HashMap<String, &tsv_reader::ColumnType> = column_types.iter().map(|(&i, t)| {
        (found_column_names[i].to_owned(), t)
    }).collect();

    println!("Column types: {:?}", named_column_types);
}

fn sample(args: SampleArgs) {
    let file = std::fs::File::open(args.file).unwrap();
    let mut reader: tsv_reader::TabSeparatedFileReader = tsv_reader::TabSeparatedFileReader::new(file);

    let mut line_buf = String::new();
    
    let mut ascii_table = AsciiTable::default();

    if args.max_width.is_some() {
        ascii_table.set_max_width(args.max_width.unwrap());
    } else if std::io::stdout().is_tty() {
        match crossterm::terminal::size() {
            Ok((width, _)) => ascii_table.set_max_width(width as usize),
            Err(_) => ascii_table.set_max_width(usize::MAX),
        };
    } else {
        ascii_table.set_max_width(usize::MAX);
    }


    let mut data: Vec<Vec<String>> = vec![];

    // Read the column names
    for (i, column_name) in reader.read_line_and_split(&mut line_buf).expect("Empty file").enumerate() {
        ascii_table.column(i).set_header(format!("{:?}", column_name));
    }

    // Skip rows
    if args.skip > 0 {
        reader.skip_lines(args.skip).unwrap();
    }

    for _ in 0..args.rows {
        let line: Vec<String> = match reader.read_line_and_split(&mut line_buf) {
            Some(line) => line.into_iter().map(|s| format!("{:?}", s)).collect(),
            None => break,
        };
        data.push(line);
    }

    ascii_table.print(data);
}

fn build(args: BuildArgs) {
    println!("Building database from config file: {}", args.config);

    let config = match config::Config::from_file(&args.config) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Failed to parse config file: {}", e);
            std::process::exit(1);
        }
    };

    match config.validate() {
        Ok(_) => {},
        Err(e) => {
            eprintln!("Config validation failed:\n\t{}", e);
            std::process::exit(1);
        }
    }

    let output = match args.output {
        Some(output) => PathBuf::from(output),
        None => {
            let mut output = PathBuf::from(&args.config);
            output.set_extension("zygosdb");
            output
        }
    };

    let database = database::Database::new(output, config);
    match database.save() {
        Ok(_) => {},
        Err(e) => {
            eprintln!("Failed to save database: {}", e);
            std::process::exit(1);
        }
    }

    println!("Database: {:?}", database);
}
