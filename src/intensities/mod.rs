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
use arrow::datatypes::{Field, Float64Type, Schema, UInt16Type, UInt32Type};

use self::builder::Builder;
use self::record::Record;
use crate::Error;
use crate::util::col;
use crate::writer::{Ipc, Writer};

/* ------------------------------------------------------------------------------ Public Exports */

/// Writer for the intensities table.
///
/// Each row maps a `(measurement, wavelength)` pair to an intensity value.
/// Rows are auto-flushed to the in-memory IPC stream every 32 768 rows.
pub struct Intensities {
    /// Shared IPC stream + builder.
    ipc: Ipc<Builder>,
}

impl Intensities {
    /// Create a new, empty intensities table.
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            ipc: Ipc::new(Self::new_stream()?, Self::schema(), Builder::default()),
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
        self.ipc.builder.push(measurement, wavelengths, intensities);
        self.ipc.try_flush()
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
        let ms = col::<UInt32Type>(batch, "measurement")?;
        let wls = col::<UInt16Type>(batch, "wavelength")?;
        let vals = col::<Float64Type>(batch, "intensity")?;
        (0..batch.num_rows()).for_each(|i| {
            out.push(Record {
                measurement: ms.value(i),
                wavelength: wls.value(i),
                intensity: vals.value(i),
            });
        });
    }
    Ok(out)
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
