/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

#![feature(iter_collect_into)]

mod error;
mod intensities;
mod measurements;
mod wavelengths;
mod writer;

use std::fs::DirBuilder;
use std::path::PathBuf;

pub use self::error::Error;
use self::intensities::Intensities;
use self::measurements::Measurements;
use self::wavelengths::Wavelengths;
use self::writer::Writer;

pub struct Database {
    pub path: PathBuf,
    pub wavelengths: Wavelengths,
    pub measurements: Measurements,
    pub intensities: Intensities,
}

impl Database {
    pub fn new(filepath: &str) -> Result<Database, Error> {
        DirBuilder::new().recursive(true).create(&filepath)?;
        let path = PathBuf::from(filepath).canonicalize()?;
        let db = Database {
            wavelengths: Wavelengths::new(&path)?,
            measurements: Measurements::new(&path)?,
            intensities: Intensities::new(&path)?,
            path,
        };
        Ok(db)
    }
}

/* ---------------------------------------------------------------------------------- Unit Tests */

#[cfg(test)]
mod tests {
    use std::fs::{File, remove_dir_all};

    use arrow::array::AsArray;
    use arrow::datatypes::UInt32Type;
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::FileWriter;

    use super::*;
    #[test]
    fn database_creation() {
        const PATH: &str = "test-creation";
        let db = Database::new(PATH).unwrap();
        assert!(db.path.exists());
        remove_dir_all(PATH).unwrap();
    }

    #[test]
    fn wavelengths_schema() {
        const PATH: &str = "test-wavelengths-schema";
        let db = Database::new(PATH).unwrap();
        let file = File::open(db.wavelengths.path).unwrap();
        let reader = StreamReader::try_new(file, None).unwrap();
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);
        remove_dir_all(PATH).unwrap();
    }

    #[test]
    fn push_wavelengths() {
        const PATH: &str = "test-push-wavelengths";
        let mut db = Database::new(PATH).unwrap();
        let ids = db.wavelengths.push(vec![1E-9, 1E-3, 1E3, 1E9]).unwrap();
        assert_eq!(ids, vec![0, 1, 2, 3]);
        remove_dir_all(PATH).unwrap();
    }

    #[test]
    fn commit_and_read_wavelengths() {
        const PATH: &str = "test-commit-and-read";
        let mut db = Database::new(PATH).unwrap();

        // 1. Write wavelength data to disk
        let ids = db
            .wavelengths
            .push(vec![1E-9, 1E-3, 1E3, 1E9])
            .expect("Failed to push wavelengths");
        db.wavelengths
            .commit()
            .expect("Failed to commit wavelengths");

        // 2. Read back wavelength data from disk
        let file = File::open(&db.wavelengths.path).expect("Failed to open wavelengths file");
        let reader = StreamReader::try_new(file, None).expect("Failed to create StreamReader");
        let data: Vec<u32> =
            reader
                .into_iter()
                .filter_map(Result::ok)
                .fold(Vec::new(), |mut ids, batch| {
                    batch
                        .column_by_name("id")
                        .expect("Unable to read 'id' column")
                        .as_primitive::<UInt32Type>()
                        .values()
                        .iter()
                        .collect_into(&mut ids)
                        .to_owned()
                });

        // 3. Check that read data matches written data
        assert_eq!(ids, data);
        remove_dir_all(PATH).unwrap();
    }

    #[test]
    fn finalise() {
        const PATH: &str = "test-finalise";
        let db = Database::new(PATH).unwrap();
        let input = File::open(db.wavelengths.path).unwrap();
        let reader = StreamReader::try_new(input, None).unwrap();
        let schema = reader.schema();
        let output = File::create(db.path.join("wavelengths-finalised.arrow")).unwrap();
        let mut writer = FileWriter::try_new(output, &schema).unwrap();
        reader
            .into_iter()
            .filter_map(Result::ok)
            .for_each(|batch| writer.write(&batch).unwrap());
        writer.finish().unwrap(); // Write the file footer
        remove_dir_all(PATH).unwrap();
    }
}
