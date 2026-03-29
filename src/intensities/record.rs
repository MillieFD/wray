/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fmt::{Display, Formatter};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Float64Type, UInt16Type, UInt32Type};

use crate::table;

/* ------------------------------------------------------------------------------ Public Exports */

/// A single intensity entry returned by read queries.
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct Record {
    /// Measurement ID. Foreign key to the measurements table.
    pub measurement: u32,
    /// Wavelength ID. Foreign key to the wavelengths table.
    pub wavelength: u16,
    /// Measured spectral intensity.
    pub intensity: f64,
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl table::Record for Record {
    fn read(batch: &RecordBatch, row: usize) -> Self {
        Self {
            measurement: batch
                .column_by_name("measurement")
                .expect("missing 'measurement' column")
                .as_primitive::<UInt32Type>()
                .value(row),
            wavelength: batch
                .column_by_name("wavelength")
                .expect("missing 'wavelength' column")
                .as_primitive::<UInt16Type>()
                .value(row),
            intensity: batch
                .column_by_name("intensity")
                .expect("missing 'intensity' column")
                .as_primitive::<Float64Type>()
                .value(row),
        }
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.intensity.fmt(f)
    }
}
