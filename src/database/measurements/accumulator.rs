/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::Arc;
use std::time::SystemTime;

use arrow::array::{Array, Int32Builder, UInt8Builder, UInt32Builder, UInt64Builder};
use uom::si::i32::Length;
use uom::si::length::micrometer;
use uom::si::time::microsecond;
use uom::si::u32::Time;

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct Accumulator {
    rows: u32,
    id: UInt32Builder,
    timestamp: UInt64Builder,
    x: Int32Builder,
    y: Int32Builder,
    z: Int32Builder,
    a: Int32Builder,
    // r: Int32Builder,
    duration: UInt32Builder,
    spectrometer: UInt8Builder,
}

impl Accumulator {
    pub(super) fn new() -> Self {
        Self {
            rows: 0,
            id: Default::default(),
            timestamp: Default::default(),
            x: Default::default(),
            y: Default::default(),
            z: Default::default(),
            a: Default::default(),
            // r: Default::default(),
            duration: Default::default(),
            spectrometer: Default::default(),
        }
    }

    pub fn push(&mut self, x: Length, y: Length, z: Length, a: Length, i: Time, spectrometer: u8) {
        let timestamp = SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap_or_default()
            .as_millis() as u64;
        self.id.append_value(self.rows);
        self.timestamp.append_value(timestamp);
        self.x.append_value(x.get::<micrometer>());
        self.y.append_value(y.get::<micrometer>());
        self.z.append_value(z.get::<micrometer>());
        self.a.append_value(a.get::<micrometer>());
        // self.r.append_value(r.get::<radian>());
        self.duration.append_value(i.get::<microsecond>());
        self.spectrometer.append_value(spectrometer);
        self.rows += 1;
    }

    pub(super) fn columns(&mut self) -> Vec<Arc<dyn Array>> {
        vec![
            Arc::new(self.id.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.x.finish()),
            Arc::new(self.y.finish()),
            Arc::new(self.z.finish()),
            Arc::new(self.a.finish()),
            Arc::new(self.duration.finish()),
            Arc::new(self.spectrometer.finish()),
        ]
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */
