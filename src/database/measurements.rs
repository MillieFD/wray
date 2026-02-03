/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use super::Table;
use DataType::{Float64, UInt8, UInt32};
use polars::prelude::*;
use std::fs::File;
use std::io::Error;
use std::path::{Path, PathBuf};
use uom::si::f64::{Length, Time};

pub(super) struct Measurements {
    path: PathBuf,
    dataframe: DataFrame,
}

impl Measurements {
    pub(super) fn new<P>(path: P) -> Result<Measurements, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().join("measurements.parquet");
        let mut file = File::create(&path)?;
        let mut dataframe = Self::empty();
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut dataframe)
            .unwrap();
        let m = Measurements { path, dataframe };
        Ok(m)
    }

    pub(super) fn add(
        &mut self,
        x: Length,
        y: Length,
        z: Length,
        interfibre: Length,
        integration: Time,
        spectrometer: u8,
    ) -> Result<u32, Error> {
        let next = self.dataframe.height();
        let timestamp = std::time::SystemTime::now();
        unimplemented!()
    }
}

impl Table for Measurements {
    fn schema() -> Schema {
        Schema::from_iter([
            Field::new("id".into(), UInt32),
            Field::new("timestamp".into(), Float64),
            Field::new("x".into(), Float64),
            Field::new("y".into(), Float64),
            Field::new("z".into(), Float64),
            Field::new("interfibre".into(), Float64),
            Field::new("integration".into(), Float64),
            Field::new("spectrometer_id".into(), UInt8),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn inspect_measurements_parquet_schema() {
        let temp = std::env::temp_dir();
        let m = Measurements::new(&temp).unwrap();
        let file = File::open(&m.path).unwrap();
        let schema = ParquetReader::new(file).schema().unwrap();
        println!("{:?}", schema);
        fs::remove_file(m.path).unwrap();
    }
}
