/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod builder;
pub(crate) mod record;

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float64, UInt16, UInt32};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;

use self::builder::Builder;
use crate::Error;
use crate::writer::Writer;

/* -------------------------------------------------------------------------------- Constants */

/// Flush threshold — rows per batch.
const SIZE: usize = 32_768;

/* ------------------------------------------------------------------------------ Public Exports */

/// Writer for the intensities table.
///
/// Each row maps a `(measurement, wavelength)` pair to an intensity value.
/// Rows are auto-flushed to the in-memory IPC stream every [`SIZE`] rows.
pub(crate) struct Intensities {
    stream: StreamWriter<Buf>,
    buf: Buf,
    builder: Builder,
}

impl Intensities {
    /// Create a new, empty intensities table.
    pub fn new() -> Result<Self, Error> {
        let buf = Buf::new();
        let stream = Self::new_stream_writer(buf.clone())?;
        Ok(Self {
            stream,
            buf,
            builder: Builder::new(),
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
        self.builder.push(measurement, wavelengths, intensities);
        if self.builder.len() >= SIZE {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush pending rows from the builder into the IPC stream.
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.builder.len() == 0 {
            return Ok(());
        }
        let columns = self.builder.columns();
        let batch = RecordBatch::try_new(Self::schema(), columns)?;
        self.stream.write(&batch)?;
        Ok(())
    }

    /// Write the Arrow IPC EOS sentinel.
    pub fn finish(&mut self) -> Result<(), Error> {
        self.flush()?;
        self.stream.finish()?;
        Ok(())
    }

    /// Snapshot the current IPC bytes (without EOS).
    pub fn bytes(&self) -> Vec<u8> {
        self.buf.bytes()
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for Intensities {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("measurement", UInt32, false),
            Field::new("wavelength", UInt16, false),
            Field::new("intensity", Float64, false),
        ]))
    });
}
