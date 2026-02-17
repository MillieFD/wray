/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod accumulator;

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::File;
use std::path::Path;
use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float64, UInt32};
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use pyo3::prelude::*;

use self::accumulator::*;
use super::Writer;
use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

#[pyclass]
pub struct Intensities {
    writer: StreamWriter<File>,
    acc: Accumulator,
}

impl Intensities {
    pub(super) fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().join("intensities").with_extension("arrow");
        let writer = File::create(path)?.try_into()?;
        Ok(writer)
    }
}

#[pymethods]
impl Intensities {
    pub fn push(&mut self, measurement: u32, wavelengths: Vec<u32>, intensities: Vec<f64>) {
        self.acc.append(measurement, wavelengths, intensities);
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        let columns = self.acc.columns();
        let batch = RecordBatch::try_new(Self::schema(), columns)?;
        self.writer.write(&batch).map_err(Error::from)
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for Intensities {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        let fields = [
            Field::new("measurement", UInt32, false).into(),
            Field::new("wavelength", UInt32, false).into(),
            Field::new("intensity", Float64, false).into(),
        ];
        Schema::new(fields).into()
    });
}

impl TryFrom<File> for Intensities {
    type Error = ArrowError;

    fn try_from(file: File) -> Result<Self, Self::Error> {
        let writer = StreamWriter::try_new(file, &Self::SCHEMA)?.into();
        Ok(writer)
    }
}

impl From<StreamWriter<File>> for Intensities {
    fn from(writer: StreamWriter<File>) -> Self {
        let acc = Accumulator::new();
        Self { writer, acc }
    }
}

impl TryFrom<&Path> for Intensities {
    type Error = Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        Self::new(path)
    }
}
