/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::SystemTime;

use arrow::array::{
    ArrayRef,
    DurationMicrosecondBuilder,
    Int32Builder,
    TimestampMillisecondBuilder,
    UInt32Builder,
};
use uom::si::f64::{Length, Time};
use uom::si::length::micrometer;
use uom::si::time::microsecond;

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct Accumulator {
    next: AtomicU32,
    id: UInt32Builder,
    timestamp: TimestampMillisecondBuilder,
    x: Int32Builder,
    y: Int32Builder,
    z: Int32Builder,
    a: UInt32Builder,
    integration: DurationMicrosecondBuilder,
}

impl Accumulator {
    pub(super) fn new() -> Self {
        Self {
            next: Default::default(), // TODO: next read from existing or 0,
            id: Default::default(),
            timestamp: Default::default(),
            x: Default::default(),
            y: Default::default(),
            z: Default::default(),
            a: Default::default(),
            integration: Default::default(),
        }
    }

    pub fn read<P>(path: &P) -> u32
    where
        P: AsRef<Path> + ?Sized,
    {
        let file = File::open(path).expect("Unable to open 'measurements' file");
        StreamReader::try_new(file, None)
            .expect("Unable to read 'measurements' file")
            .filter_map(Result::ok)
            .map(|batch| {
                batch
                    .column_by_name("id")
                    .expect("Unable to read 'id' column")
                    .as_primitive::<UInt32Type>()
                    .values()
            })
            .flatten()
            .last()
            .map(|id| id + 1)
            .unwrap_or_default()
    }

    pub fn append(&mut self, x: Length, y: Length, z: Length, a: Length, i: Time) -> u32 {
        // Calculate values
        let timestamp = SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap_or_default()
            .as_millis() as i64;
        let id: u32 = self.next.fetch_add(1, Ordering::Relaxed);
        // Append values to IPC Array Builders
        self.id.append_value(id);
        self.timestamp.append_value(timestamp);
        self.x.append_value(x.get::<micrometer>() as i32);
        self.y.append_value(y.get::<micrometer>() as i32);
        self.z.append_value(z.get::<micrometer>() as i32);
        self.a.append_value(a.get::<micrometer>() as u32);
        self.integration.append_value(i.get::<microsecond>() as i64);
        // Return the measurement ID
        id
    }

    pub(super) fn columns(&mut self) -> Vec<ArrayRef> {
        vec![
            self.id.finish().into(),
            self.timestamp.finish().into(),
            self.x.finish().into(),
            self.y.finish().into(),
            self.z.finish().into(),
            self.a.finish().into(),
            self.integration.finish().into(),
        ]
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */
