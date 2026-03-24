/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod builder;

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float32, UInt32, UInt64};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;
use uom::si::f32::{Angle, Length, Time};
use uom::si::time::microsecond;

use self::builder::Builder;
use crate::format::{Buf, Units};
use crate::writer::Writer;
use crate::{Config, Error};

/* -------------------------------------------------------------------------------- Constants */

/// Flush threshold — rows per batch.
const SIZE: usize = 8_192;

/* ------------------------------------------------------------------------------ Public Exports */

/// Writer for the measurements table.
///
/// Each measurement is assigned a sequential `u32` ID. Timestamps are stored
/// as `UInt64` microsecond offsets from the manifest epoch. Coordinate columns
/// (`x`, `y`, `z`, `a`) are nullable `Float32`.
pub(crate) struct Measurements {
    // TODO add one-line doc comment for each field
    stream: StreamWriter<Buf>,
    buf: Buf,
    builder: Builder,
    /// Next auto-increment measurement ID.
    next: AtomicU32,
    /// Manifest epoch in microseconds since UNIX epoch.
    epoch: u64,
}

impl Measurements {
    /// Create a new, empty measurements table.
    pub fn new(epoch: i64, cfg: &Config) -> Result<Self, Error> {
        let buf = Buf::new();
        let stream = Self::new_stream_writer(buf.clone())?; // TODO Can we avoid this clone or make it cheaper by using an Arc inside `Buf`?
        Ok(Self {
            stream,
            buf,
            builder: Builder::new(),
            next_id: 0,
            epoch,
            cfg: cfg.clone(), // Inavoidable deep clone not in hot-path.
        })
    }

    /// Record a new measurement. Returns the assigned measurement ID.
    ///
    /// All optional coordinate fields are feature-gated. Unneeded fields can be disabled in
    /// `cargo.toml` for improved ergonomics. This does not change the underlying `schema`.
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
            .expect("Great scott! System clock is before the unix epoch")
            .as_micros()
            .try_into()
            .expect("Microsecond timestamp exceeds u64 range");
        let ts = now.saturating_sub(self.epoch);
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        self.builder.push(id, ts, x, y, z, a, b, c, integration);
        self.try_flush()?;
        Ok(id)
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

/* -------------------------------------------------------------------------- Helper Functions */

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
