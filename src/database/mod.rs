/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod intensities;
mod measurements;
mod wavelengths;
mod writer;

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs;
use std::path::PathBuf;

// use pyo3::prelude::*;
use self::intensities::*;
use self::measurements::*;
use self::wavelengths::*;
use self::writer::*;
use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

// #[pyclass]
pub struct Database {
    pub path: PathBuf,
    pub wavelengths: WavelengthWriter,
    pub measurements: MeasurementWriter,
    pub intensities: IntensityWriter,
}

// #[pymethods]
impl Database {
    // #[new]
    // #[pyo3(signature = (filepath))]
    pub fn new(filepath: &str) -> Result<Database, Error> {
        fs::DirBuilder::new().recursive(true).create(&filepath)?;
        let buf = PathBuf::from(filepath).canonicalize()?;
        let db = Database {
            wavelengths: buf.as_path().try_into()?,
            measurements: buf.as_path().try_into()?,
            intensities: buf.as_path().try_into()?,
            path: buf,
        };
        Ok(db)
    }
}

/* ---------------------------------------------------------------------------------- Unit Tests */

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn database_creation() {
        let mut db = Database::new("test").unwrap();
        assert!(db.path.exists());
        let a = db.push_wavelengths(vec![0.0, 0.1, 0.2, 0.3]).unwrap();
        println!("Pushed wavelengths: {:?}", a);
        db.wavelengths.commit();
        println!("Committed wavelengths");

        // Read the file as an IPC stream and print the schema
        let wavelengths_path = db.path.join("wavelengths").with_extension("arrow");
        let file = fs::File::open(wavelengths_path).unwrap();
        let reader = StreamReader::try_new(file, None).unwrap();
        println!("Schema: {:?}", reader.schema());
    }

    #[test]
    fn rationalise() {
        // Open the input stream file
        let input =
            fs::File::open("/Users/amelia/Programming/Rust/optic/test/wavelengths.arrow").unwrap();

        // StreamReader reads the schema from the first message in the stream
        let mut stream_reader = StreamReader::try_new_buffered(input, None).unwrap();
        let schema = stream_reader.schema();

        // Create the output IPC file writer (this format includes a footer on finish)
        let output = std::fs::File::create(
            "/Users/amelia/Programming/Rust/optic/test/wavelengths_rationalised.arrow",
        )
        .unwrap();
        let mut file_writer = arrow::ipc::writer::FileWriter::try_new(output, &schema).unwrap();

        // Copy all record batches from stream -> file
        for maybe_batch in stream_reader {
            let batch = maybe_batch.unwrap();
            file_writer.write(&batch).unwrap();
        }

        // IMPORTANT: writes the footer + finalizes the file format
        file_writer.finish().unwrap();
    }
}
