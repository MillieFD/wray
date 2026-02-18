/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod builder;

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float64, UInt32};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;

use self::builder::Builder;
use crate::{Error, Writer};

/* ------------------------------------------------------------------------------ Public Exports */

pub struct Intensities {
    stream: StreamWriter<File>,
    builder: Builder,
}

impl Intensities {
    pub(super) fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        path.as_ref()
            .join("intensities")
            .with_extension("arrow")
            .try_into()
    }

    pub fn push(&mut self, measurement: u32, wavelengths: Vec<u32>, intensities: Vec<f64>) {
        self.builder.append(measurement, wavelengths, intensities);
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        let columns = self.builder.columns();
        let batch = RecordBatch::try_new(Self::schema(), columns)?;
        self.stream.write(&batch).map_err(Error::from)
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

impl TryFrom<PathBuf> for Intensities {
    type Error = Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?
            .try_into()
    }
}

impl TryFrom<File> for Intensities {
    type Error = Error;

    fn try_from(file: File) -> Result<Self, Self::Error> {
        Ok(Self {
            stream: StreamWriter::try_new(file, &Self::SCHEMA)?,
            builder: Builder::new(),
        })
    }
}
