/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fmt::{Display, Formatter};

use arrow::array::RecordBatch;
use arrow::datatypes::{Float32Type, UInt32Type, UInt64Type};

use crate::table;
use crate::util::{col, nullable};

/* ------------------------------------------------------------------------------ Public Exports */

/// A single measurement entry returned by read queries.
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct Record {
    /// Unique measurement identifier.
    pub id: u32,
    /// Microsecond offset from the manifest epoch.
    pub timestamp: u64,
    /// X coordinate in SI base units, or `None` if unused.
    pub x: Option<f32>,
    /// Y coordinate in SI base units, or `None` if unused.
    pub y: Option<f32>,
    /// Z coordinate in SI base units, or `None` if unused.
    pub z: Option<f32>,
    /// A coordinate in SI base units, or `None` if unused.
    pub a: Option<f32>,
    /// B coordinate in SI base units, or `None` if unused.
    pub b: Option<f32>,
    /// C coordinate in SI base units, or `None` if unused.
    pub c: Option<f32>,
    /// Integration time in microseconds.
    pub integration: u32,
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl table::Record for Record {
    fn read(batch: &RecordBatch, row: usize) -> Self {
        let ids = col::<UInt32Type>(batch, "id").expect("missing 'id' column");
        let ts = col::<UInt64Type>(batch, "timestamp").expect("missing 'timestamp' column");
        let xs = col::<Float32Type>(batch, "x").expect("missing 'x' column");
        let ys = col::<Float32Type>(batch, "y").expect("missing 'y' column");
        let zs = col::<Float32Type>(batch, "z").expect("missing 'z' column");
        let als = col::<Float32Type>(batch, "a").expect("missing 'a' column");
        let bs = col::<Float32Type>(batch, "b").expect("missing 'b' column");
        let cs = col::<Float32Type>(batch, "c").expect("missing 'c' column");
        let integ = col::<UInt32Type>(batch, "integration").expect("missing 'integration' column");
        Self {
            id: ids.value(row),
            timestamp: ts.value(row),
            x: nullable(xs, row),
            y: nullable(ys, row),
            z: nullable(zs, row),
            a: nullable(als, row),
            b: nullable(bs, row),
            c: nullable(cs, row),
            integration: integ.value(row),
        }
    }
}

impl Display for Record {
    /// Formats the value using the given formatter.
    ///
    /// # Display or Debug?
    ///
    /// Use [`Display`] to show measurement ID and timestamp; the minimum information required
    /// to uniquely identify a record. Use [`Debug`][1] to show the full record, including
    /// coordinates and integration time. See trait-level documentation for more information.
    ///
    /// [1]: std::fmt::Debug
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ id: {}, timestamp: {} µs }}", self.id, self.timestamp)
    }
}
