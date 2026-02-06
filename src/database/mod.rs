/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod measurements;
mod spectra;
mod spectrometers;
mod table;
mod writer;

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs;
use std::path::PathBuf;

use measurements::*;
use spectra::*;
use spectrometers::*;
use writer::*;

use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

pub struct Database {
    path: PathBuf,
    wavelengths: WavelengthWriter,
    measurements: MeasurementWriter,
    intensities: IntensityWriter,
}

impl Database {
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
        let db = Database::new("test").unwrap();
        assert!(db.path.exists());
    }
}
