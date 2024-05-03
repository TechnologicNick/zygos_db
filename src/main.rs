mod tsv_reader;

fn main() {
    println!("Hello, world!");

    let file = std::fs::File::open("gwas_catalog_v1.0.2-associations_e111_r2024-04-22.tsv").unwrap();
    let mut reader = tsv_reader::TabSeparatedFileReader::new(file);

    let column_names = reader.read_line_and_split().unwrap();
    println!("Column names: {:?}", column_names);

    let interesting_column_names = vec!["STUDY", "SNPS", "P-VALUE", "CHR_POS"];

    let interesting_column_indices: Vec<usize> = column_names.iter().enumerate().filter_map(|(i, header)| {
        if interesting_column_names.contains(&header.as_str()) {
            Some(i)
        } else {
            None
        }
    }).collect();

    let column_types = reader.guess_column_types_but_better(
        interesting_column_indices,
        0.2,
        1000
    ).unwrap();

    let named_column_types: std::collections::HashMap<String, &tsv_reader::ColumnType> = column_types.iter().map(|(&i, &ref t)| {
        (column_names[i].clone(), t)
    }).collect();

    println!("Column types: {:?}", named_column_types);
}
