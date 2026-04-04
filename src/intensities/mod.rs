/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

mod builder;
pub(crate) mod record;

/* ----------------------------------------------------------------------------- Private Imports */

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::DataType::{Float64, UInt16, UInt32};
use arrow::datatypes::{Field, Schema};

use self::builder::Builder;
use self::record::Record;
use crate::format::Segment;
use crate::table::{self, Ipc, Sink};
use crate::{Error, Manifest};

/* ------------------------------------------------------------------------------ Public Exports */

/// Abstraction over the intensity table.
///
/// Each [`Record`] maps a `Measurement ID: u32` and `wavelength ID: u16` pair to an `Intensity:
/// f64` value. See `FORMAT.md` for more details.
///
/// New data accumulates in memory with [`Self::push`] and is automatically written to disk on
/// [`Drop`] or when [`Builder::is_full`] returns `true`
pub struct Intensities {
    /// IPC stream writer for appending new intensity measurements.
    ipc: Ipc<Builder>,
    /// Path to the dataset file.
    path: PathBuf,
    /// Location descriptors for written intensity segments.
    segments: Vec<Segment>,
}

impl Intensities {
    /// TODO add doc comment
    pub(crate) fn new(manifest: &Manifest) -> Result<Self, Error> {
        Ok(Self {
            ipc: Some(Ipc::new(
                Self::new_stream()?,
                Self::schema(),
                Builder::default(),
            )),
            path: manifest.path.clone(),
            segments: Vec::new(), // TODO add fn to extract intensity Segments from manifest.toml
        })
    }

    /// Record intensity values for a single measurement.
    ///
    /// `wavelengths` and `intensities` must have the same length — each
    /// element pair becomes one row in the table.
    pub fn push(
        &mut self,
        measurement: u32,
        wavelengths: &[u16],
        intensities: &[f64],
    ) -> Result<(), Error> {
        let ipc = self.ipc.as_mut().expect("dataset open for writing");
        ipc.builder.push(measurement, wavelengths, intensities);
        self.check()
    }

    /// Read all intensity records from the dataset.
    ///
    /// Automatically selects stream-based or memory-mapped reading based on
    /// whether the dataset is writable.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file cannot be read or the IPC data is invalid.
    pub fn read(&self) -> Result<Vec<Record>, Error> {
        match self.ipc.is_some() {
            true => table::read_stream(&self.path, &self.segments),
            false => table::read_mmap(&self.path, &self.segments),
        }
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Sink for Intensities {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("measurement", UInt32, false),
            Field::new("wavelength", UInt16, false),
            Field::new("intensity", Float64, false),
        ]))
    });

    fn write(&mut self) -> Result<(), Error> {
        self.ipc.as_mut().expect("dataset open for writing").flush()
    }

    fn check(&mut self) -> Result<(), Error> {
        self.ipc
            .as_mut()
            .expect("dataset open for writing")
            .try_flush()
    }

    fn reset(&mut self, segments: Vec<Segment>) -> Result<(), Error> {
        self.ipc
            .as_mut()
            .expect("dataset open for writing")
            .reset(Self::new_stream()?);
        self.segments = segments;
        Ok(())
    }

    fn take_bytes(&mut self) -> Result<Vec<u8>, Error> {
        match self.ipc.as_mut() {
            Some(ipc) => ipc.take_bytes(),
            None => Ok(Vec::new()),
        }
    }

    fn finish(&self) -> Result<Vec<u8>, Error> {
        table::consolidate(&self.path, &self.segments, &Self::SCHEMA)
    }
}
