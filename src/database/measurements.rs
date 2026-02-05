/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fs::File;
use std::io::Error;
use std::path::{Path, PathBuf};

use DataType::{Float64, UInt8, UInt32};
use polars::prelude::*;
use uom::si::f64::{Length, Time};

use super::Table;

pub(super) struct Measurements {
    path: PathBuf,
    height: usize,
}

impl Measurements {
    pub(super) fn new<P>(path: P) -> Result<Measurements, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().join("measurements.ipc");
        let mut file = File::create(&path)?;
        let mut dataframe = Self::empty();
        IpcStreamWriter::new(&mut file)
            .finish(&mut dataframe)
            .unwrap();
        let m = Measurements { path, height: 0 };
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
        let id = self.height as u32;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let mut new_row = df!(
            "id" => [id],
            "timestamp" => [timestamp],
            "x" => [x.value],
            "y" => [y.value],
            "z" => [z.value],
            "interfibre" => [interfibre.value],
            "integration" => [integration.value],
            "spectrometer_id" => [spectrometer],
        )
        .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

        let mut file = std::fs::OpenOptions::new().append(true).open(&self.path)?;

        IpcStreamWriter::new(&mut file)
            .finish(&mut new_row)
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e))?;

        self.height += 1;
        Ok(id)
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
    use std::fs;

    use super::*;

    #[test]
    fn test_add_measurements() {
        let temp = std::env::temp_dir();
        let mut m = Measurements::new(&temp).unwrap();

        use uom::si::length::meter;
        use uom::si::time::second;

        m.add(
            Length::new::<meter>(1.0),
            Length::new::<meter>(2.0),
            Length::new::<meter>(3.0),
            Length::new::<meter>(0.1),
            Time::new::<second>(0.5),
            1,
        )
        .unwrap();

        m.add(
            Length::new::<meter>(4.0),
            Length::new::<meter>(5.0),
            Length::new::<meter>(6.0),
            Length::new::<meter>(0.2),
            Time::new::<second>(1.0),
            2,
        )
        .unwrap();

        let mut file = File::open(&m.path).unwrap();
        let df0 = IpcStreamReader::new(&mut file).finish().unwrap();
        let df1 = IpcStreamReader::new(&mut file).finish().unwrap();
        let df2 = IpcStreamReader::new(&mut file).finish().unwrap();

        assert_eq!(df0.height(), 0);
        assert_eq!(df1.height(), 1);
        assert_eq!(df2.height(), 1);
        assert_eq!(m.height, 2);

        fs::remove_file(m.path).unwrap();
    }
}
