/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod builder;
mod record;

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float64, UInt16};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;
use uom::si::f32::Length;
use uom::si::length::nanometer;

use self::builder::Builder;
use self::record::Record;
use crate::Error;
use crate::format::Buf;
use crate::writer::Writer;

/* -------------------------------------------------------------------------------- Constants */

/// Flush threshold — rows per batch.
const SIZE: usize = 16_384;

/* ------------------------------------------------------------------------------ Public Exports */

/// Writer for the wavelengths table.
///
/// Maintains an in-memory cache of all wavelength records for deduplication
/// without disk reads. New wavelengths are accumulated in an Arrow builder
/// and auto-flushed to the in-memory IPC stream every [`SIZE`] rows.
pub(crate) struct Wavelengths {
    stream: StreamWriter<Buf>,
    buf: Buf,
    builder: Builder,
    records: Vec<Record>,
}

impl Wavelengths {
    /// Create a new, empty wavelengths table.
    pub fn new() -> Result<Self, Error> {
        let buf = Buf::new();
        let stream = Self::new_stream_writer(buf.clone())?;
        Ok(Self {
            stream,
            buf,
            builder: Builder::new(),
            records: Vec::new(),
        })
    }

    /// Insert wavelengths (in nanometres), returning their `u16` IDs.
    ///
    /// Duplicate wavelengths (within `TOLERANCE` nm) reuse existing IDs.
    /// New wavelengths are assigned sequential IDs starting after the
    /// current maximum.
    pub fn push(&mut self, wavelengths: &[f32]) -> Result<Vec<u16>, Error> {
        const TOLERANCE: f32 = 1E-12;
        let mut next = self
            .records
            .iter()
            .map(|r| r.id)
            .max()
            .map_or(0, |id| id + 1);
        let mut ids = Vec::with_capacity(wavelengths.len());
        for &wl in wavelengths {
            let nm = Length::new::<nanometer>(wl);
            let existing = self
                .records
                .iter()
                .find(|r| (r.nm - nm).abs().get::<nanometer>() < TOLERANCE)
                .map(|r| r.id);
            if let Some(id) = existing {
                ids.push(id);
            } else {
                let id = next;
                next += 1;
                self.records.push(Record::new(id, nm));
                self.builder.push(id, nm);
                ids.push(id);
            }
        }
        if self.builder.len() >= SIZE {
            self.flush()?;
        }
        Ok(ids)
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

impl Writer for Wavelengths {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("id", UInt16, false),
            Field::new("nm", Float64, false),
        ]))
    });
}
