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

use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float32, UInt32, UInt64};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;
use uom::si::f64::{Angle, Length, Time};
use uom::si::time::microsecond;

use self::builder::Builder;
use crate::Error;
use crate::format::{Buf, Units};
use crate::writer::Writer;

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
    stream: StreamWriter<Buf>,
    buf: Buf,
    builder: Builder,
    next_id: u32,
    init_ts: i64,
    x_unit: Option<Units>,
    y_unit: Option<Units>,
    z_unit: Option<Units>,
    a_unit: Option<Units>,
}

impl Measurements {
    /// Create a new, empty measurements table.
    pub fn new(
        init_ts: i64,
        x_unit: Option<Units>,
        y_unit: Option<Units>,
        z_unit: Option<Units>,
        a_unit: Option<Units>,
    ) -> Result<Self, Error> {
        let buf = Buf::new();
        let stream = Self::new_stream_writer(buf.clone())?;
        Ok(Self {
            stream,
            buf,
            builder: Builder::new(),
            next_id: 0,
            init_ts,
            x_unit,
            y_unit,
            z_unit,
            a_unit,
        })
    }

    /// Record a new measurement. Returns the assigned measurement ID.
    ///
    /// The timestamp is captured automatically as a microsecond offset from the
    /// manifest epoch. Coordinates are converted to `f32` in the configured
    /// storage unit. Passing a coordinate value when the corresponding axis
    /// was not configured is an error.
    pub fn push(
        &mut self,
        x: Option<Length>,
        y: Option<Length>,
        z: Option<Length>,
        a: Option<Angle>,
        integration: Time,
    ) -> Result<u32, Error> {
        let ts = SystemTime::UNIX_EPOCH
            .elapsed()
            .expect("system clock after epoch")
            .as_micros() as i64
            - self.init_ts;
        let ts = ts as u64;
        let id = self.next_id;
        self.next_id += 1;
        let x = convert_length(x, self.x_unit, "x")?;
        let y = convert_length(y, self.y_unit, "y")?;
        let z = convert_length(z, self.z_unit, "z")?;
        let a = convert_angle(a, self.a_unit, "a")?;
        let integration = integration.get::<microsecond>() as u64;
        self.builder.push(id, ts, x, y, z, a, integration);
        if self.builder.len() >= SIZE {
            self.flush()?;
        }
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

fn convert_angle(
    val: Option<Angle>,
    unit: Option<Units>,
    name: &str,
) -> Result<Option<f32>, Error> {
    match (val, unit) {
        (Some(v), Some(u)) => Ok(Some(u.angle_to_f32(v))),
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
            Field::new("integration", UInt64, false),
        ]))
    });
}
