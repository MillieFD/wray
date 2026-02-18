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
