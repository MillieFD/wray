/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fmt::{Debug, Display};
use std::fs::File;
use std::io::{BufWriter, Cursor};
use std::path::Path;
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::{FileWriter, StreamWriter};

use crate::Error;
use crate::format::Segment;

/* ---------------------------------------------------------------------------------- Type Alias */

/// Arrow IPC stream writer backed by an in-memory buffer.
pub(crate) type Stream = StreamWriter<BufWriter<Vec<u8>>>;

/* --------------------------------------------------------------------------------- Build Trait */

/// Trait for Arrow column builders shared across all tables.
pub(crate) trait Build {
    /// Number of pending rows.
    fn len(&self) -> usize;
    /// Whether the builder has reached its flush threshold.
    fn is_full(&self) -> bool;
    /// Finish current arrays and return columns. Resets the builder.
    fn columns(&mut self) -> Vec<ArrayRef>;
}

/* ---------------------------------------------------------------------------------- Ipc Helper */

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
    pub const fn new(stream: Stream, schema: Arc<Schema>, builder: B) -> Self {
        Self {
            stream: Some(stream),
            schema,
            builder,
        }
    }

    /// Flush pending rows from the builder into the IPC stream.
    ///
    /// ### Errors
    ///
    /// Returns [`ArrowError::IpcError`] if the [`StreamWriter`] is closed.
    pub fn flush(&mut self) -> Result<(), Error> {
        // TODO Change return type to use `ArrowError`
        if self.builder.len() == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(self.schema.clone(), self.builder.columns())?;
        self.stream
            .as_mut()
            .expect("Stream is None")
            .write(&batch)?;
        // TODO Don't store `stream` as `Option` → Remove need for `expect`
        Ok(())
    }

    /// Flush if the builder has reached its capacity threshold.
    pub fn try_flush(&mut self) -> Result<(), Error> {
        match self.builder.is_full() {
            true => self.flush(),
            false => Ok(()),
        }
    }

    /// Flush, finish the stream, and extract the serialised IPC bytes.
    ///
    /// Returns an empty [`Vec`] when the builder has no pending rows and no
    /// batches have been written to the stream.
    pub fn take_bytes(&mut self) -> Result<Vec<u8>, Error> {
        if self.builder.len() == 0 && self.stream.is_none() {
            return Ok(Vec::new());
        }
        self.flush()?;
        self.stream
            .take()
            .expect("stream alive")
            .into_inner()?
            .into_inner()
            .map_err(|e| Error::Io(e.into_error()))
    }

    /// Replace the consumed stream with a fresh one.
    pub fn reset(&mut self, stream: Stream) {
        self.stream = Some(stream);
    }
}

/* ---------------------------------------------------------------------------------- Sink Trait */

/// All functions necessary to store data in a table.
///
/// Each table struct implements this trait. The [`push`] method is **not** part
/// of the trait because each table has a unique signature.
///
/// [`push`]: crate::wavelengths::Wavelengths::push
pub(crate) trait Sink {
    /// Lazily initialised Arrow schema for this table.
    const SCHEMA: LazyLock<Arc<Schema>>;

    /// Flush pending builder data to the in-memory IPC stream.
    #[allow(dead_code, reason = "public API for explicit flush")]
    fn write(&mut self) -> Result<(), Error>;

    /// If pending data exceeds ~256 KB, call [`write`](Self::write).
    fn check(&mut self) -> Result<(), Error>;

    /// Reset the IPC stream for the next write cycle.
    fn reset(&mut self, segments: Vec<Segment>) -> Result<(), Error>;

    /// Flush, finish the IPC stream, and extract serialised bytes.
    fn take_bytes(&mut self) -> Result<Vec<u8>, Error>;

    /// Consolidate all on-disk segments into Arrow IPC **file** format bytes.
    ///
    /// Pending data must be flushed to disk before calling. Uses
    /// [`Self::SCHEMA`] to construct a [`FileWriter`] internally.
    fn finish(&self) -> Result<Vec<u8>, Error>;

    /// Clone the shared schema [`Arc`].
    fn schema() -> Arc<Schema> {
        Self::SCHEMA.clone()
    }

    /// Create a new [`StreamWriter`] backed by an in-memory [`Vec<u8>`].
    ///
    /// ### Errors
    ///
    /// Returns [`ArrowError`] if the writer cannot be initialised.
    // TODO Where is this fn used? Replace `Vec` with `Take` for zero-copy window into `File`.
    fn new_stream() -> Result<Stream, ArrowError> {
        StreamWriter::try_new_buffered(Vec::new(), &Self::SCHEMA)
    }
}

/* -------------------------------------------------------------------------------- Record Trait */

/// Consistent interface for table record types.
///
/// All record structs (`Wavelength`, `Measurement`, `Intensity`) implement this
/// trait, ensuring each can be extracted row-by-row from a [`RecordBatch`].
pub trait Record: Copy + Clone + Debug + Default + PartialEq + PartialOrd + Display {
    /// Extract a single record from `batch` at the given `row` index.
    fn read(batch: &RecordBatch, row: usize) -> Self;
}

/* ------------------------------------------------------------------------- Shared Read Helpers */

/// Read records from Arrow IPC **stream** segments using zero-copy
/// [`Take`](std::io::Take) windows.
///
/// Opens the file at `path`, iterates `segments`, and for each one creates a
/// [`StreamReader`](arrow::ipc::reader::StreamReader) via
/// [`Segment::stream`](Segment::stream). Records are extracted
/// row-by-row via [`Record::read`].
pub(crate) fn read_stream<P, R>(path: P, segments: &[Segment]) -> Result<Vec<R>, Error>
where
    P: AsRef<Path>,
    R: Record,
{
    // TODO Change `signature` in fn signature to an iterator with <Item = Segment>
    if segments.is_empty() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path)?;
    let mut records = Vec::new();
    'outer: for segment in segments {
        let mut stream = segment.stream(&mut file)?;
        'inter: while let Some(batch) = stream.next().transpose()? {
            'inner: for row in 0..batch.num_rows() {
                let record = R::read(&batch, row);
                records.push(record);
            }
        }
    }
    Ok(records)
}

/// Read records from Arrow IPC **file** segments via memory-mapped I/O.
///
/// Opens the file at `path` in read-only mode and creates a [`memmap2::Mmap`].
/// For each segment, slices the mapped region and wraps it in a
/// [`Cursor`] for [`FileReader`] consumption.
///
/// ### Safety
///
/// The file is opened read-only. Finished datasets are sealed and immutable, so
/// no concurrent mutation can occur while the mapping is live.
pub(crate) fn read_mmap<R: Record>(path: &Path, segments: &[Segment]) -> Result<Vec<R>, Error> {
    let file = File::open(path)?;
    // SAFETY: file is opened read-only; finished datasets are sealed and immutable.
    let map = unsafe { memmap2::Mmap::map(&file)? };
    let mut records = Vec::new();
    for seg in segments {
        let (start, len) = seg.byte_range();
        let reader = FileReader::try_new(Cursor::new(&map[start..start + len]), None)?;
        for batch in reader {
            let batch = batch?;
            (0..batch.num_rows()).for_each(|i| records.push(R::read(&batch, i)));
        }
    }
    Ok(records)
}

/* --------------------------------------------------------------- Segment Consolidation */

/// Consolidate all segments into Arrow IPC **file** format bytes.
///
/// Reads stream-format segments from disk, decodes them into
/// [`RecordBatch`]es, and re-encodes as a single Arrow IPC file using the
/// provided `schema`.
pub(crate) fn consolidate(
    path: &Path,
    segments: &[Segment],
    schema: &Arc<Schema>,
) -> Result<Vec<u8>, Error> {
    let mut all = Vec::new();
    for seg in segments {
        let mut file = File::open(path)?;
        let mut stream = seg.stream(&mut file)?;
        while let Some(batch) = stream.next().transpose()? {
            all.push(batch);
        }
    }
    if all.is_empty() {
        return Ok(Vec::new());
    }
    let mut w = FileWriter::try_new_buffered(Cursor::new(Vec::new()), schema)?;
    for batch in &all {
        w.write(batch)?;
    }
    w.finish()?;
    let buf = w.into_inner()?;
    let cursor = buf.into_inner().map_err(|e| Error::Io(e.into_error()))?;
    Ok(cursor.into_inner())
}
