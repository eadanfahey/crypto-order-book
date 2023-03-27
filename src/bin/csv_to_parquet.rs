use std::{process::Command, path::{PathBuf, Path}, fs::{self, DirEntry}};

fn main() {
    let data_dirs = vec![
        Path::new("data/binance/spot/trades").to_path_buf(),
        Path::new("data/binance/futures/book_ticks").to_path_buf(),
    ];

    let schema_filepaths = vec![
        Path::new("parquet_schemas/binance_spot_trades_schema.json").to_path_buf(),
        Path::new("parquet_schemas/binance_futures_book_ticks_schema.json").to_path_buf(),
    ];

    for (data_dir, schema_filepath) in data_dirs.into_iter().zip(schema_filepaths.into_iter()) {
        convert_csv_files_to_parquet(data_dir, schema_filepath);
    }
}

fn convert_csv_files_to_parquet(data_dir: PathBuf, schema_filepath: PathBuf) {
    for dir in list_dirs(data_dir).iter() {
        for csv_file in find_csv_files(dir.path()) {
            csv_to_parquet(csv_file.path(), schema_filepath.clone())
        }
    }
}

fn csv_to_parquet(csv_filepath: PathBuf, schema_filepath: PathBuf) {
    let dir = csv_filepath.parent().unwrap();
    let filename = csv_filepath.file_name().unwrap().to_str().unwrap();
    let (name, _) = get_file_extension(filename).unwrap();
    let parquet_filepath = dir.join(format!("{}.parquet", name));

    Command::new("csv2parquet")
        .args([
            "--header", "true",
            "--schema-file", &schema_filepath.to_str().unwrap(),
            "--compression", "zstd",
            csv_filepath.to_str().unwrap(),
            parquet_filepath.to_str().unwrap(),
        ])
        .output()
        .expect("failed");

    println!("Created {}", parquet_filepath.to_string_lossy());
}

fn list_dirs(dir: PathBuf) -> Vec<DirEntry> {
    let mut dirs = vec![];
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            dirs.push(entry);
        }
    }
    dirs
}

fn find_csv_files(dir: PathBuf) -> Vec<DirEntry> {
    let mut csv_files = vec![];
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let filename = entry.file_name().to_str().unwrap().to_string();
        let Some((_, file_ext)) = get_file_extension(&filename) else {
            continue;
        };
        if file_ext != "csv" {
            continue;
        }
        csv_files.push(entry);
    }
    csv_files
}

fn get_file_extension(name: &str) -> Option<(&str, &str)> {
    match name.split_once(".") {
        Some((name, ext)) => Some((name, ext)),
        _ => None
    }
}
