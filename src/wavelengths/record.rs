/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fmt::{Display, Formatter};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{Float32Type, UInt16Type};

use crate::table;

/* ------------------------------------------------------------------------------ Public Exports */

/// A single wavelength entry.
///
/// Used both internally for deduplication and as the public read-query result.
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct Record {
    /// Unique wavelength identifier.
    pub id: u16,
    /// Wavelength in nanometres.
    pub nm: f32,
}

impl Record {
    pub(super) const fn new(id: u16, nm: f32) -> Self {
        Self { id, nm }
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl table::Record for Record {
    /// Extract a single [`Record`] from `batch` at the specified `row` index.
    ///
    /// ### Panics
    ///
    /// - If the batch does not contain the required columns
    /// - If the row index is out of bounds
    fn read(batch: &RecordBatch, row: usize) -> Self {
        Self {
            id: batch
                .column_by_name("id")
                .expect("missing 'id' column")
                .as_primitive::<UInt16Type>()
                .value(row),
            nm: batch
                .column_by_name("nm")
                .expect("missing 'nm' column")
                .as_primitive::<Float32Type>()
                .value(row),
        }
    }
}

impl Display for Record {
    /// Formats the value using the given formatter.
    ///
    /// # Display or Debug?
    ///
    /// Use [`Display`] to show the wavelength value with units (nm). Use [`Debug`][1] to show the
    /// full record including wavelength ID. See trait-level documentation for more information.
    ///
    /// [1]: std::fmt::Debug
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} nm", self.nm)
    }
}

impl From<(u16, f32)> for Record {
    fn from((id, nm): (u16, f32)) -> Self {
        Self::new(id, nm)
    }
}
