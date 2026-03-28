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

    /// Append wavelengths to the end of the dataset. Returns a list of unique `u16` IDs.
    ///
    /// ### Deduplication
    ///
    /// Existing wavelengths are identified using [`find`]. New wavelengths are assigned the next
    /// sequentially available ID.
    pub fn push(&mut self, nms: &[f32]) -> Result<Vec<u16>, Error> {
        let disk = read_records(&self.path, &self.segments)?;
        let ids: Vec<u16> = nms
            .iter()
            .map(|&nm| match find(nm, &self.pending, &disk) {
                Some(id) => id,
                None => self.insert(nm),
            })
            .collect();
        self.ipc.try_flush()?;
        Ok(ids)
    }

    /// Flush builder, finish stream, and extract the serialised bytes.
    pub fn take_bytes(&mut self) -> Result<Vec<u8>, Error> {
        self.ipc.take_bytes()
    }

    /// Discard the current stream and start a fresh one.
    pub fn reset(&mut self) -> Result<(), Error> {
        self.ipc.reset(Self::new_stream()?);
        Ok(())
    }

    /// Search for an existing [`Record`] within a constant wavelength tolerance.
    ///
    /// Returns [`Some`] if a matching record is found, else returns [`None`].
    ///
    /// # Example
    ///
    /// ```rust
    /// match wavelengths.find(123.45) {
    ///     Some(record) => println!("Matching record found: {:?}", record),
    ///     None => println!("No matching record found."),
    /// }
    /// ```
    fn find(&self, nm: f32) -> Option<&Record> {
        const TOLERANCE: f32 = 1E-10; // 100 picometers
        self.records.iter().find(|r| (r.nm - nm).abs() < TOLERANCE)
    }
}

/* ---------------------------------------------------------------------------- Read Functions */

/// Return the `id` of a wavelength matching `nm` within 100 picometre tolerance, or [`None`] if no
/// matching wavelength is found.
///
/// Checks `pending` (in memory) before `written` (on disk).
fn find(nm: f32, pending: &[Record], written: &[Record]) -> Option<u16> {
    pending
        .iter()
        .chain(written) // Checks pending (in memory) before written (on disk)
        .find(|r| (r.nm - nm).abs() < 1E-10) // 100 picometre tolerance
        .map(|r| r.id)
}

/// Eagerly decode [`Record`]s from the given wavelength [`Segment`]s on disk.
pub(super) fn read<P, S>(path: P, mut segments: S) -> Result<Vec<Record>, Error>
where
    P: AsRef<Path>,
    S: Iterator<Item = Segment>,
{
    let mut records = Vec::new();
    let mut file = File::open(path)?;
    'outer: while let Some(segment) = segments.next() {
        let mut stream = segment.stream(&mut file)?;
        'inner: while let Some(batch) = stream.next().transpose()? {
            for row in 0..batch.num_rows() {
                let record = Record::read(&batch, row);
                records.push(record);
            }
        }
    };
    Ok(records)
}

/// Extract [`Record`]s from [`RecordBatch`]es.
pub(crate) fn decode(batches: &[RecordBatch]) -> Vec<Record> {
    batches
        .iter()
        .flat_map(|batch| {
            let ids = col::<UInt16Type>(batch, "id")
                .expect("Batch does not contain 'id' column")
                .values(); // SAFETY: ID values are guaranteed non-null
            let nms = col::<Float32Type>(batch, "nm")
                .expect("Batch does not contain 'nm' column")
                .values(); // SAFETY: Wavelength values are guaranteed non-null
            ids.iter().zip(nms).map(|(id, nm)| Record::new(*id, *nm))
        })
        .collect()
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
