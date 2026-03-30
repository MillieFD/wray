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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow::datatypes::DataType::{Float32, UInt32, UInt64};
use arrow::datatypes::{Field, Schema};

use self::builder::Builder;
use self::record::Record;
use crate::Error;
use crate::format::Segment;
use crate::table::{self, Ipc, Sink};

/* ------------------------------------------------------------------------------ Public Exports */

/// Abstraction over the measurements table.
///
/// Each measurement is assigned a sequential `u32` ID. Timestamps are stored
/// as `UInt64` microsecond offsets from the manifest epoch. Coordinate columns
/// are nullable `Float32`.
pub struct Measurements {
    /// IPC stream writer (`None` when read-only).
    ipc: Option<Ipc<Builder>>,
    /// Next auto-increment measurement ID.
    next: AtomicU32,
    /// Manifest epoch in microseconds since UNIX epoch.
    epoch: u64,
    /// Path to the dataset file.
    path: PathBuf,
    /// Location descriptors for written measurement segments.
    segments: Vec<Segment>,
}

impl Measurements {
    /// Create or open a measurements table for the dataset at `path`.
    ///
    /// When `writable` is `true`, an IPC stream writer is initialised.
    pub(crate) fn new(
        path: impl AsRef<Path>,
        segments: Vec<Segment>,
        writable: bool,
        epoch: u64,
        next_id: u32,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        let ipc = match writable {
            true => Some(Ipc::new(Self::new_stream()?, Self::schema(), Builder::default())),
            false => None,
        };
        Ok(Self {
            ipc,
            next: AtomicU32::new(next_id),
            epoch,
            path,
            segments,
        })
    }

    /// Record a new measurement. Returns the assigned measurement ID.
    ///
    /// ### Cargo Features
    ///
    /// All optional coordinate fields are feature-gated. Unneeded fields can be
    /// disabled in `cargo.toml` for improved ergonomics. This does not change
    /// the underlying `schema`.
    #[allow(clippy::too_many_arguments, reason = "User may require all fields")]
    pub fn push(
        &mut self,
        #[cfg(feature = "x")] x: Option<f32>,
        #[cfg(feature = "y")] y: Option<f32>,
        #[cfg(feature = "z")] z: Option<f32>,
        #[cfg(feature = "a")] a: Option<f32>,
        #[cfg(feature = "b")] b: Option<f32>,
        #[cfg(feature = "c")] c: Option<f32>,
        integration: u32,
    ) -> Result<u32, Error> {
        let now: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_micros()
            .try_into()
            .expect("microsecond timestamp exceeds u64");
        let timestamp = now.saturating_sub(self.epoch);
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let ipc = self.ipc.as_mut().expect("dataset open for writing");
        ipc.builder.push(id, timestamp, x, y, z, a, b, c, integration);
        self.check()?;
        Ok(id)
    }
    /// Read all measurement records from the dataset.
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

impl Sink for Measurements {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("id", UInt32, false),
            Field::new("timestamp", UInt64, false),
            Field::new("x", Float32, true),
            Field::new("y", Float32, true),
            Field::new("z", Float32, true),
            Field::new("a", Float32, true),
            Field::new("b", Float32, true),
            Field::new("c", Float32, true),
            Field::new("integration", UInt32, false),
        ]))
    });

    fn write(&mut self) -> Result<(), Error> {
        self.ipc.as_mut().expect("dataset open for writing").flush()
    }

    fn check(&mut self) -> Result<(), Error> {
        self.ipc.as_mut().expect("dataset open for writing").try_flush()
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

