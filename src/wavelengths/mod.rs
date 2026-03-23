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

use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Float64, UInt16};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;

use self::builder::Builder;
use self::record::Record;
use crate::Error;
use crate::format::Buf;
use crate::writer::Writer;

/* ------------------------------------------------------------------------------ Public Exports */

/// Writer for the wavelengths table.
///
/// Maintains an in-memory cache of all wavelength records for deduplication
/// without disk reads. New wavelengths are accumulated in an Arrow builder
/// and auto-flushed to the in-memory IPC stream every [`SIZE`] rows.
pub(crate) struct Wavelengths {
    stream: StreamWriter<Buf>,
    buf: Buf,
    builder: Builder,
    records: Vec<Record>,
}

impl Wavelengths {
    /// Create a new, empty wavelengths table.
    pub fn new(records: Vec<Record>) -> Result<Self, Error> {
        Ok(Self {
            stream: Self::new_stream_writer()?,
            builder: Builder::new(),
            next: records
                .iter()
                .map(|r| r.id)
                .max()
                .map_or(0, |id| id + 1)
                .into(),
            records,
        })
    }

    /// Insert wavelengths (in nanometres), returning their `u16` IDs.
    ///
    /// Duplicate wavelengths (within `TOLERANCE` nm) reuse existing IDs.
    /// New wavelengths are assigned sequential IDs starting after the
    /// current maximum.
    pub fn push(&mut self, wavelengths: &[f32]) -> Result<Vec<u16>, Error> {
        let ids = wavelengths
            .iter()
            .map(|&nm| match self.find(nm) {
                Some(record) => record.id,
                None => {
                    let id = self.next.fetch_add(1, Ordering::SeqCst);
                    let record = Record::new(id, nm);
                    self.records.push(record);
                    self.builder.push(id, nm);
                    id
                }
            })
            .collect();
        self.try_flush()?;
        Ok(ids)
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

/// Extract [`Record`]s from pre-decoded [`RecordBatch`]es.
pub(crate) fn decode(batches: &[RecordBatch]) -> Result<Vec<Record>, Error> {
    batches.iter().try_fold(Vec::new(), |mut records, batch| {
        let ids = col::<UInt16Type>(batch, "id")?;
        let nms = col::<Float32Type>(batch, "nm")?;
        for i in 0..batch.num_rows() {
            records.push(Record {
                // SAFETY: Index cannot be out of bounds. Columns are guaranteed non-null.
                id: ids.value(i),
                nm: nms.value(i),
            });
        }
        Ok(records)
    })
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for Wavelengths {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new("id", UInt16, false),
            Field::new("nm", Float64, false),
        ]))
    });
}
