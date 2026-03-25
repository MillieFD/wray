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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float32, UInt32, UInt64};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;

use self::builder::Builder;
use crate::writer::Writer;
use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

/// Writer for the measurements table.
///
/// Each measurement is assigned a sequential `u32` ID. Timestamps are stored
/// as `UInt64` microsecond offsets from the manifest epoch. Coordinate columns
/// are nullable `Float32`.
pub struct Measurements {
    /// Shared IPC stream + builder.
    ipc: Ipc<Builder>,
    /// Next auto-increment measurement ID.
    next: AtomicU32,
    /// Manifest epoch in microseconds since UNIX epoch.
    epoch: u64,
}

impl Measurements {
    /// Create a new measurements table with the given epoch and starting ID.
    pub fn new(epoch: u64, next_id: u32) -> Result<Self, Error> {
        Ok(Self {
            ipc: Ipc::new(Self::new_stream()?, Self::schema(), Builder::default()),
            next: AtomicU32::new(next_id),
            epoch,
        })
    }

    /// Record a new measurement. Returns the assigned measurement ID.
    ///
    /// All optional coordinate fields are feature-gated.
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
        let ts = now.saturating_sub(self.epoch);
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        self.ipc.builder.push(id, ts, x, y, z, a, b, c, integration);
        self.ipc.try_flush()?;
        Ok(id)
    }

    /// Flush pending rows from the [`Builder`] into the [`IPC stream`][1] if the builder exceeds a
    /// constant threshold size, else no-op.
    ///
    /// [1]: StreamWriter
    pub fn try_flush(&mut self) -> Result<(), Error> {
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

    /// Discard the current stream and start a fresh one.
    pub fn reset(&mut self) -> Result<(), Error> {
        self.ipc.reset(Self::new_stream()?);
        Ok(())
    }
}

/* ---------------------------------------------------------------------------- Read Functions */

fn convert_length(
    val: Option<Length>,
    unit: Option<Units>,
    name: &str,
) -> Result<Option<f32>, Error> {
    match (val, unit) {
        (Some(v), Some(u)) => Ok(Some(u.length_to_f32(v))),
        (None, _) => Ok(None),
        (Some(_), None) => Err(Error::InvalidFormat(format!(
            "{name} value provided but unit not configured"
        ))),
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for Measurements {
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
            Field::new("integration", UInt64, false),
        ]))
    });
}
