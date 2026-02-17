/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod accumulator;
mod record;

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::{File, OpenOptions};
use std::ops::Sub;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, AsArray, Datum, RecordBatch};
use arrow::datatypes::DataType::{Float64, UInt32};
use arrow::datatypes::{Field, Float64Type, Schema, UInt32Type};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use pyo3::prelude::*;
use uom::si::f64::Length;
use uom::si::length::nanometer;

use self::accumulator::*;
use self::record::*;
use super::Writer;
use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct WavelengthWriter {
    writer: StreamWriter<File>,
    acc: Accumulator,
    path: PathBuf,
}

impl WavelengthWriter {
    pub(super) fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().join("wavelengths").with_extension("arrow");
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let writer = Self {
            writer: StreamWriter::try_new(file, &Self::SCHEMA)?,
            acc: Accumulator::new(),
            path: path.to_path_buf(),
        };
        Ok(writer)
    }

    fn read(&self) -> Vec<Record> {
        let file = File::open(&self.path).expect("Unable to open 'wavelengths' file");
        StreamReader::try_new(file, None)
            .expect("Unable to read 'wavelengths' file")
            .filter_map(Result::ok)
            .fold(Vec::new(), |mut records, batch| {
                let nms = batch
                    .column_by_name("nm")
                    .expect("Unable to read 'nm' column")
                    .as_primitive::<Float64Type>()
                    .values()
                    .iter()
                    .copied()
                    .map(Length::new::<nanometer>);
                let ids = batch
                    .column_by_name("id")
                    .expect("Unable to read 'id' column")
                    .as_primitive::<UInt32Type>()
                    .values()
                    .iter()
                    .copied();
                ids.zip(nms)
                    .map(Record::from)
                    .collect_into(&mut records)
                    .to_owned()
            })
    }

    pub(super) fn push(&mut self, wavelengths: Vec<f64>) -> Result<Vec<u32>, ArrowError> {
        const TOLERANCE: f64 = 1E-12;
        let mut records = self.read();
        records.sort_unstable(); // In-place sort does not allocate
        let next = AtomicU32::new(records.last().map_or(0, |record| record.id + 1));
        let ids = wavelengths
            .iter()
            .map(|wl| Length::new::<nanometer>(*wl))
            .scan(records.iter(), |mut iter, wl| {
                loop {
                    match iter.next() {
                        Some(record) if record.nm.sub(wl).abs().value < TOLERANCE => {
                            break Some(record.id);
                        }
                        Some(record) if record.nm.sub(wl).value > TOLERANCE => continue,
                        _ => {
                            let id = next.fetch_add(1, Ordering::Relaxed);
                            self.acc.push(id, wl);
                            break Some(id);
                        }
                    }
                }
            })
            .collect();
        Ok(ids)
    }

    pub fn commit(&mut self) {
        let columns = self.acc.columns();
        let batch = RecordBatch::try_new(Self::schema(), columns).unwrap();
        self.writer.write(&batch).expect("Failed to write batch");
        // TODO: Handle errors gracefully
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for WavelengthWriter {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        let fields = [
            Field::new("id", UInt32, false).into(),
            Field::new("nm", Float64, false).into(),
        ];
        Schema::new(fields).into()
    });
}

impl TryFrom<&Path> for WavelengthWriter {
    type Error = Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        Self::new(path)
    }
}
