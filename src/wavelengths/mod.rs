/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ------------------------------------------------------------------------------------- Modules */

mod builder;
pub(super) mod record;

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float32, UInt16};
use arrow::datatypes::{Field, Float32Type, Schema, UInt16Type};

use self::builder::Builder;
use self::record::Record;
use crate::Error;
use crate::util::col;
use crate::writer::{Ipc, Writer};

/* ------------------------------------------------------------------------------ Public Exports */

/// Writer for the wavelengths table.
///
/// Maintains an in-memory cache of all wavelength records for deduplication.
/// New wavelengths are accumulated in an Arrow builder and auto-flushed to
/// the in-memory IPC stream when the builder is full.
pub struct Wavelengths {
    /// Shared IPC stream + builder.
    ipc: Ipc<Builder>,
    /// Next auto-increment wavelength ID.
    next: AtomicU16,
    /// In-memory dedup cache.
    records: Vec<Record>,
}

impl Wavelengths {
    /// Create a new wavelengths table, optionally pre-populated with existing records.
    pub fn new(records: Vec<Record>) -> Result<Self, Error> {
        let next = records.iter().map(|r| r.id).max().map_or(0, |id| id + 1);
        Ok(Self {
            ipc: Ipc::new(Self::new_stream()?, Self::schema(), Builder::new()),
            next: AtomicU16::new(next),
            records,
        })
    }

    /// Insert wavelengths (in nanometres), returning their `u16` IDs.
    ///
    /// Duplicate wavelengths (within tolerance) reuse existing IDs.
    pub fn push(&mut self, wavelengths: &[f32]) -> Result<Vec<u16>, Error> {
        let ids = wavelengths
            .iter()
            .map(|&nm| match self.find(nm) {
                Some(r) => r.id,
                None => {
                    let id = self.next.fetch_add(1, Ordering::SeqCst);
                    self.records.push(Record::new(id, nm));
                    self.ipc.builder.push(id, nm);
                    id
                }
            })
            .collect();
        self.ipc.try_flush()?;
        Ok(ids)
    }

    /// Searches for an existing [`Record`] within a constant wavelength tolerance.
    ///
    /// Returns [`Some`] if a matching record is found, else returns [`None`].
    ///
    /// # Example
    ///
    /// ```rust
    /// match wavelengths.find(50f32) {
    ///     Some(record) => println!("Matching record found: {:?}", record),
    ///     None => println!("No matching record found."),
    /// }
    /// ```
    fn find(&self, nm: f32) -> Option<&Record> {
        const TOLERANCE: f32 = 1E-10; // 100 picometers
        self.records.iter().find(|r| (r.nm - nm).abs() < TOLERANCE)
    }

    /// Flush pending rows from the [`Builder`] into the [`IPC stream`][1] if the builder exceeds a
    /// constant threshold size, else no-op.
    ///
    /// [1]: StreamWriter
    pub fn try_flush(&mut self) -> Result<(), Error> {
        // TODO Would it be more performant to use an arrow buffered stream writer?
        match self.builder.is_full() {
            true => self.flush(),
            false => Ok(()),
        }
    }

    /// Flush pending rows from the [`Builder`] into the [`IPC stream`][1]
    ///
    /// [1]: StreamWriter
    pub fn flush(&mut self) -> Result<(), Error> {
        let columns = self.builder.columns();
        let batch = RecordBatch::try_new(Self::schema(), columns)?;
        self.stream.write(&batch)?;
        Ok(())
    }

    /// Search for an existing [`Record`] within a constant wavelength tolerance.
    fn find(&self, nm: f32) -> Option<&Record> {
        const TOLERANCE: f32 = 1E-10;
        self.records.iter().find(|r| (r.nm - nm).abs() < TOLERANCE)
    }
}

/* ---------------------------------------------------------------------------- Read Functions */

/// Extract [`Record`]s from pre-decoded [`RecordBatch`]es.
pub(crate) fn decode(batches: &[RecordBatch]) -> Result<Vec<Record>, Error> {
    batches.iter().try_fold(Vec::new(), |mut out, batch| {
        let ids = col::<UInt16Type>(batch, "id")?;
        let nms = col::<Float32Type>(batch, "nm")?;
        (0..batch.num_rows()).for_each(|i| out.push(Record::new(ids.value(i), nms.value(i))));
        Ok(out)
    })
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for Wavelengths {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("id", UInt16, false),
            Field::new("nm", Float32, false),
        ]))
    });
}
