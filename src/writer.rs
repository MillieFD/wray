/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::io::{BufWriter, Cursor};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::writer::{FileWriter, StreamWriter};

use crate::Error;

/* -------------------------------------------------------------------------------- Type Alias */

/// Arrow IPC stream writer backed by an in-memory buffer.
pub(crate) type Stream = StreamWriter<BufWriter<Vec<u8>>>;

/* ------------------------------------------------------------------------------ Build Trait */

/// Trait for Arrow column builders shared across all tables.
pub(crate) trait Build {
    /// Number of pending rows.
    fn len(&self) -> usize;
    /// Whether the builder has reached its flush threshold.
    fn is_full(&self) -> bool;
    /// Finish current arrays and return columns. Resets the builder.
    fn columns(&mut self) -> Vec<ArrayRef>;
}

/* ------------------------------------------------------------------------------ Ipc Writer */

/// Generic Arrow IPC stream writer paired with a column [`Build`]er.
///
/// Encapsulates the flush → batch → write → reset lifecycle shared by all
/// three table writers ([`Wavelengths`], [`Measurements`], [`Intensities`]).
pub(crate) struct Ipc<B: Build> {
    /// Arrow IPC stream writer (taken on [`take_bytes`](Self::take_bytes)).
    stream: Option<Stream>,
    /// Arrow schema for [`RecordBatch`] construction.
    schema: Arc<Schema>,
    /// Column builder for pending rows.
    pub builder: B,
}

impl<B: Build> Ipc<B> {
    /// Create a new [`Ipc`] writer.
    // SAFETY: Arc::new and Some() are const-stable from Rust 1.x; clippy
    // suggests const but Arc<T> does not yet have a const constructor.
    #[allow(clippy::missing_const_for_fn, reason = "Arc<Schema> lacks const constructor")]
    pub fn new(stream: Stream, schema: Arc<Schema>, builder: B) -> Self {
        Self { stream: Some(stream), schema, builder }
    }

    /// Flush pending rows from the builder into the IPC stream.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the Arrow [`RecordBatch`] cannot be constructed or
    /// written to the IPC stream.
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.builder.len() == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(self.schema.clone(), self.builder.columns())?;
        self.stream.as_mut().expect("stream alive").write(&batch)?;
        Ok(())
    }

    /// Flush if the builder has reached its capacity threshold.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if [`flush`](Self::flush) fails.
    pub fn try_flush(&mut self) -> Result<(), Error> {
        match self.builder.is_full() {
            true => self.flush(),
            false => Ok(()),
        }
    }

    /// Flush, finish the stream, and extract the serialised IPC bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if flushing, finishing the stream, or unwrapping the
    /// inner buffer fails.
    pub fn take_bytes(&mut self) -> Result<Vec<u8>, Error> {
        self.flush()?;
        let mut s = self.stream.take().expect("stream alive");
        s.finish()?;
        let buf = s.into_inner()?;
        buf.into_inner().map_err(|e| Error::Io(e.into_error()))
    }

    /// Replace the consumed stream with a fresh one.
    pub fn reset(&mut self, stream: Stream) {
        self.stream = Some(stream);
    }
}

/* ----------------------------------------------------------------------------- Writer Trait */

/// Schema provider and factory for Arrow IPC table writers.
pub(crate) trait Writer {
    /// Lazily initialised Arrow schema for this table.
    const SCHEMA: LazyLock<Arc<Schema>>;

    /// Clone the shared schema [`Arc`].
    fn schema() -> Arc<Schema> {
        Self::SCHEMA.clone()
    }

    /// Create a new [`StreamWriter`] backed by an in-memory [`Vec<u8>`].
    ///
    /// # Errors
    ///
    /// Returns [`ArrowError`] if the writer cannot be initialised.
    fn new_stream() -> Result<Stream, ArrowError> {
        StreamWriter::try_new_buffered(Vec::new(), &Self::SCHEMA)
    }

    /// Create a new [`FileWriter`] backed by a seekable [`Cursor`].
    ///
    /// # Errors
    ///
    /// Returns [`ArrowError`] if the writer cannot be initialised.
    fn new_file_writer() -> Result<FileWriter<BufWriter<Cursor<Vec<u8>>>>, ArrowError> {
        FileWriter::try_new_buffered(Cursor::new(Vec::new()), &Self::SCHEMA)
    }
}
