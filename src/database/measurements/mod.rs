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
use arrow::datatypes::DataType::{Int32, Timestamp, UInt32};
use arrow::datatypes::TimeUnit::Microsecond;
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use super::Writer;
use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct MeasurementWriter {
    writer: StreamWriter<File>,
    acc: Accumulator,
}

impl MeasurementWriter {
    pub(super) fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().join("measurements.ipc");
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

impl TryFrom<File> for MeasurementWriter {
    type Error = ArrowError;

    fn try_from(file: File) -> Result<Self, Self::Error> {
        let writer = StreamWriter::try_new(file, &Self::SCHEMA)?.into();
        Ok(writer)
    }
}

impl From<StreamWriter<File>> for MeasurementWriter {
    fn from(writer: StreamWriter<File>) -> Self {
        let acc = Accumulator::new();
        Self { writer, acc }
    }
}

impl TryFrom<&Path> for MeasurementWriter {
    type Error = Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        Self::new(path)
    }
}

impl Writer for MeasurementWriter {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        let fields = [
            Field::new("measurement_id", UInt32, false).into(),
            Field::new("timestamp", Timestamp(Microsecond, None), false).into(),
            Field::new("x_coordinate", Int32, false).into(),
            Field::new("y_coordinate", Int32, false).into(),
            Field::new("z_coordinate", Int32, false).into(),
            Field::new("a_interfibre", Int32, false).into(),
            Field::new("integration_duration", UInt32, false).into(),
            Field::new("spectrometer_id", UInt32, false).into(),
        ];
        Schema::new(fields).into()
    });
}
