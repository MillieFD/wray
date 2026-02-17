/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::SystemTime;

use arrow::array::{
    ArrayRef,
    AsArray,
    DurationMicrosecondBuilder,
    Float64Builder,
    TimestampMillisecondBuilder,
    UInt32Builder,
};
use arrow::datatypes::UInt32Type;
use arrow::ipc::reader::StreamReader;
use uom::si::f64::{Length, Time};
use uom::si::length::micrometer;
use uom::si::time::microsecond;

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct Builder {
    next: AtomicU32,
    id: UInt32Builder,
    timestamp: TimestampMillisecondBuilder,
    #[cfg(feature = "x")]
    x: Float64Builder,
    #[cfg(feature = "y")]
    y: Float64Builder,
    #[cfg(feature = "z")]
    z: Float64Builder,
    #[cfg(feature = "a")]
    a: Float64Builder,
    integration: DurationMicrosecondBuilder,
}

impl Builder {
    pub(super) fn new<P>(path: &P) -> Self
    where
        P: AsRef<Path> + ?Sized,
    {
        Self {
            next: Self::read(path),
            id: Default::default(),
            timestamp: Default::default(),
            #[cfg(feature = "x")]
            x: Default::default(),
            #[cfg(feature = "y")]
            y: Default::default(),
            #[cfg(feature = "z")]
            z: Default::default(),
            #[cfg(feature = "a")]
            a: Default::default(),
            integration: Default::default(),
        }
    }

    pub fn read<P>(path: &P) -> AtomicU32
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
            .into()
    }

    pub fn append(&mut self, x: Length, y: Length, z: Length, a: Length, i: Time) -> u32 {
        let timestamp = SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap_or_default()
            .as_millis() as i64;
        let id: u32 = self.next.fetch_add(1, Ordering::Relaxed);
        self.id.append_value(id);
        self.timestamp.append_value(timestamp);
        self.x.append_value(x.get::<micrometer>());
        self.y.append_value(y.get::<micrometer>());
        self.z.append_value(z.get::<micrometer>());
        self.a.append_value(a.get::<micrometer>());
        self.integration.append_value(i.get::<microsecond>() as i64);
        id // Return the inserted measurement ID
    }

    pub(super) fn columns(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.id.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.x.finish()),
            Arc::new(self.y.finish()),
            Arc::new(self.z.finish()),
            Arc::new(self.a.finish()),
            Arc::new(self.integration.finish()),
        ]
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */
