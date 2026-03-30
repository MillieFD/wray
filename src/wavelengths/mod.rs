/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ------------------------------------------------------------------------------------- Modules */

mod builder;
pub(crate) mod record;

/* ----------------------------------------------------------------------------- Private Imports */

use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, LazyLock};

use arrow::datatypes::DataType::{Float32, UInt16};
use arrow::datatypes::{Field, Schema};

use self::builder::Builder;
use self::record::Record;
use crate::format::Segment;
use crate::table::{self, Ipc, Sink};
use crate::{Error, Manifest};

/* ------------------------------------------------------------------------------ Public Exports */

/// Abstraction over the wavelengths table.
///
/// Deduplication is performed against both the current write cycle's [`pending`]
/// records and the previously written on-disk segments, which are read on each
/// [`push`](Wavelengths::push) call. Only the small set of wavelengths added
/// since the last flush is held in memory at any time.
pub struct Wavelengths {
    /// IPC stream writer (`None` when read-only).
    ipc: Option<Ipc<Builder>>,
    /// Next available wavelength ID.
    next: AtomicU16,
    /// Path to the dataset file.
    path: PathBuf,
    /// Location descriptors for written wavelength segments.
    segments: Vec<Segment>,
    /// Wavelengths added in the current write cycle.
    pending: Vec<Record>,
}

impl Wavelengths {
    /// Create or open a wavelengths table for the dataset at `path`.
    pub(crate) fn new(manifest: &Manifest) -> Result<Self, Error> {
        Ok(Self {
            ipc: Some(Ipc::new(
                Self::new_stream()?,
                Self::schema(),
                Builder::new(),
            )),
            next: table::read_stream(&manifest.path, &manifest.wavelengths)?
                .iter()
                .map(|r: &Record| r.id)
                .max()
                .map_or(0, |id| id + 1)
                .into(),
            path: manifest.path.clone(),
            segments: manifest.wavelengths.clone(),
            pending: Vec::new(),
        })
    }

    /// Append wavelengths to the end of the dataset. Returns unique `u16` IDs.
    ///
    /// ### Deduplication
    ///
    /// Existing wavelengths are identified using [`find`](Self::find). New
    /// wavelengths are assigned the next sequentially available ID.
    pub fn push(&mut self, nms: &[f32]) -> Result<Vec<u16>, Error> {
        let disk: Vec<Record> = table::read_stream(&self.path, &self.segments)?;
        let ids: Vec<u16> = nms
            .iter()
            .map(|&nm| match self.find(nm, &disk) {
                Some(id) => id,
                None => self.insert(nm),
            })
            .collect();
        self.check()?;
        Ok(ids)
    }

    fn insert(&mut self, nm: f32) -> u16 {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        self.pending.push(Record::new(id, nm));
        let ipc = self.ipc.as_mut().expect("dataset open for writing");
        ipc.builder.push(id, nm);
        id
    }

    /// Read all wavelength records from the dataset.
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

    /// Return the `id` of a wavelength matching `nm` within 100 pm tolerance.
    fn find(&self, nm: f32, written: &[Record]) -> Option<u16> {
        self.pending
            .iter()
            .chain(written)
            .find(|r| (r.nm - nm).abs() < 1E-10)
            .map(|r| r.id)
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Sink for Wavelengths {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("id", UInt16, false),
            Field::new("nm", Float32, false),
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
        self.pending.clear();
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
