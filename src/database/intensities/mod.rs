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

use accumulator::*;
use arrow::array::RecordBatch;
use arrow::datatypes::DataType::UInt32;
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;

use super::Writer;
use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct IntensityWriter {
    writer: StreamWriter<File>,
    acc: Accumulator,
}

impl IntensityWriter {
    pub(super) fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().join("intensities").with_extension("arrow");
        let writer = File::create(path)?.try_into()?;
        Ok(writer)
    }

    pub fn commit(&mut self) {
        let columns = self.acc.columns();
        let batch = RecordBatch::try_new(Self::schema(), columns).unwrap();
        self.writer.write(&batch).expect("Failed to write batch");
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for IntensityWriter {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        let fields = [
            Field::new("measurement", UInt32, false).into(),
            Field::new("wavelength", UInt32, false).into(),
            Field::new("intensity", Float64, false).into(),
        ];
        Schema::new(fields).into()
    });
}

impl TryFrom<File> for IntensityWriter {
    type Error = ArrowError;

    fn try_from(file: File) -> Result<Self, Self::Error> {
        let writer = StreamWriter::try_new(file, &Self::SCHEMA)?.into();
        Ok(writer)
    }
}

impl From<StreamWriter<File>> for IntensityWriter {
    fn from(writer: StreamWriter<File>) -> Self {
        let acc = Accumulator::new();
        Self { writer, acc }
    }
}

impl TryFrom<&Path> for IntensityWriter {
    type Error = Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        Self::new(path)
    }
}

impl Writer for IntensityWriter {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        let fields = [Field::new("measurement_id", UInt32, false).into()];
        Schema::new(fields).into()
    });
}
