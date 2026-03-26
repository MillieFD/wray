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
use arrow::datatypes::{Field, Float32Type, Schema, UInt32Type, UInt64Type};

use self::builder::Builder;
use self::record::Record;
use crate::Error;
use crate::util::{col, nullable};
use crate::writer::{Ipc, Writer};

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
    /// ### Cargo Features
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
        let timestamp = now.saturating_sub(self.epoch);
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        self.ipc.builder.push(id, timestamp, x, y, z, a, b, c, integration);
        self.ipc.try_flush()?;
        Ok(id)
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
}

/* ---------------------------------------------------------------------------- Read Functions */

/// Extract [`Record`]s from pre-decoded [`RecordBatch`]es.
pub(crate) fn decode(batches: &[RecordBatch]) -> Result<Vec<Record>, Error> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = col::<UInt32Type>(batch, "id")?;
        let ts = col::<UInt64Type>(batch, "timestamp")?;
        let xs = col::<Float32Type>(batch, "x")?;
        let ys = col::<Float32Type>(batch, "y")?;
        let zs = col::<Float32Type>(batch, "z")?;
        let als = col::<Float32Type>(batch, "a")?;
        let bs = col::<Float32Type>(batch, "b")?;
        let cs = col::<Float32Type>(batch, "c")?;
        let integ = col::<UInt32Type>(batch, "integration")?;
        (0..batch.num_rows()).for_each(|i| {
            out.push(Record {
                id: ids.value(i),
                timestamp: ts.value(i),
                x: nullable(xs, i),
                y: nullable(ys, i),
                z: nullable(zs, i),
                a: nullable(als, i),
                b: nullable(bs, i),
                c: nullable(cs, i),
                integration: integ.value(i),
            });
        });
    }
    Ok(out)
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
            Field::new("integration", UInt32, false),
        ]))
    });
}
