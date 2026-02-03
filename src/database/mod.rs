/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

mod measurements;
mod spectra;
mod spectrometers;
mod table;

use measurements::*;
use spectrometers::*;
use std::fs;
use std::path::PathBuf;
use table::*;

struct Database {
    path: PathBuf,
    spectrometers: Spectrometers,
    measurements: Measurements,
}

impl Database {
    pub fn new(filename: &str) -> Result<Database, std::io::Error> {
        fs::create_dir_all(filename)?;
        let path = std::env::current_dir()?.join(filename).canonicalize()?;
        let spectrometers = Spectrometers::new(&path)?;
        let measurements = Measurements::new(&path)?;
        let db = Database {
            path,
            spectrometers,
            measurements,
        };
        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    use std::path::Path;

    #[test]
    fn database_creation() {
        let db = Database::new("test").unwrap();
        assert!(db.path.exists());
    }

    fn inspect_parquet_schema(path: &Path) {
        let file = fs::File::open(path).unwrap();
        let mut reader = ParquetReader::new(file);
        let schema = reader.schema().unwrap();
        println!("{:?}", schema);
    }
}
