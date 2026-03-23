/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use arrow::array::{Array, ArrowPrimitiveType, AsArray, PrimitiveArray, RecordBatch};
use arrow::datatypes::{Float32Type, Float64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow::ipc::reader::StreamReader;
use uom::si::f32::{Angle, Length, Time};

use crate::format::{Config, EOS, HEADER, Header};
use crate::intensities::Intensities;
use crate::measurements::Measurements;
use crate::wavelengths::Wavelengths;
use crate::{
    Error,
    IntensityRecord,
    Manifest,
    MeasurementRecord,
    WavelengthRecord,
    intensities,
    measurements,
    wavelengths,
};

/// A `.wr` dataset for storing spatially located optical spectroscopy data.
///
/// Create a new dataset with [`Dataset::new`], push data with the various
/// `push_*` methods, and finalise with [`Dataset::close`] or
/// [`Dataset::finish`]. The dataset is also written on [`Drop`].
///
/// Open an existing file for reading with [`Dataset::open`].
pub struct Dataset {
    path: PathBuf,
    manifest: Manifest,
    // TODO wavelengths, measurements, and intensities tables are not optional in the schema. Explain why they are options here.
    wavelengths: Option<Wavelengths>,
    measurements: Option<Measurements>,
    intensities: Option<Intensities>,
    /// Raw IPC bytes for each section (populated by [`Dataset::open`] or after close).
    wl_bytes: Vec<u8>,
    ms_bytes: Vec<u8>,
    it_bytes: Vec<u8>,
    closed: bool,
}

impl Dataset {
    /// Create a new `.wr` dataset at `path`.
    ///
    /// The file is not written to disk until [`close`](Self::close),
    /// [`finish`](Self::finish), [`commit`](Self::commit), or [`Drop`].
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the Arrow IPC stream writers cannot be initialised.
    ///
    /// # Panics
    ///
    /// Panics if the system clock is set before the UNIX epoch.
    pub fn new(path: impl AsRef<Path>, cfg: &Config) -> Result<Self, Error> {
        let timestamp: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Great scott! System clock is before the unix epoch")
            .as_micros()
            .try_into()
            .expect("Microsecond timestamp exceeds u64 range");
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            manifest: Manifest::new(timestamp, cfg),
            wavelengths: Some(Wavelengths::new()?),
            measurements: Some(Measurements::new(timestamp, cfg)?),
            intensities: Some(Intensities::new()?),
            wl_bytes: Vec::new(),
            ms_bytes: Vec::new(),
            it_bytes: Vec::new(),
            closed: false,
        })
    }

    /// Open an existing `.wr` file for reading.
    ///
    /// Validates the magic bytes and format version, parses the manifest,
    /// and stores the raw Arrow IPC sections for query access.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file cannot be read, the header is invalid,
    /// or the manifest TOML is malformed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let data = std::fs::read(path.as_ref())?;
        let header = Header::read(&mut &data[..])?;
        let m_start = HEADER;
        let m_end = m_start + header.manifest_len as usize;
        let manifest: Manifest = toml::from_str(std::str::from_utf8(&data[m_start..m_end])?)?;
        let wl_start = m_end;
        let wl_end = wl_start + header.wavelengths_len as usize;
        let ms_start = wl_end;
        let ms_end = ms_start + header.measurements_len as usize;
        let it_start = ms_end;
        let it_end = it_start + header.intensities_len as usize;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            manifest,
            wavelengths: None,
            measurements: None,
            intensities: None,
            wl_bytes: data[wl_start..wl_end].to_vec(),
            ms_bytes: data[ms_start..ms_end].to_vec(),
            it_bytes: data[it_start..it_end].to_vec(),
            closed: true,
        })
    }

    /* -------------------------------------------------------------------------------- Push API */

    /// Insert wavelengths (in nanometres) and return their `u16` IDs.
    ///
    /// Duplicate wavelengths are deduplicated with 1 × 10⁻¹² nm tolerance.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the Arrow builder cannot flush to the IPC stream.
    ///
    /// # Panics
    ///
    /// Panics if the dataset was opened read-only (via [`open`](Self::open)).
    // TODO Move all push functions into respective modules e.g. self.wavelengths.push
    pub fn push_wavelengths(&mut self, wavelengths: &[f32]) -> Result<Vec<u16>, Error> {
        self.wavelengths
            .as_mut()
            .expect("dataset open for writing")
            .push(wavelengths)
    }

    /// Record a new measurement. Returns the assigned `u32` ID.
    ///
    /// The timestamp is captured automatically. Pass `None` for unused
    /// coordinate axes.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the Arrow builder cannot flush to the IPC stream.
    ///
    /// # Panics
    ///
    /// Panics if the dataset was opened read-only (via [`open`](Self::open)).
    pub fn push(
        &mut self,
        x: Option<Length>,
        y: Option<Length>,
        z: Option<Length>,
        a: Option<Angle>,
        integration: Time,
    ) -> Result<u32, Error> {
        self.measurements
            .as_mut()
            .expect("dataset open for writing")
            .push(x, y, z, a, integration)
    }

    /// Record intensity values for a single measurement.
    ///
    /// `wavelengths` and `intensities` must have the same length.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the Arrow builder cannot flush to the IPC stream.
    ///
    /// # Panics
    ///
    /// Panics if the dataset was opened read-only (via [`open`](Self::open)).
    pub fn push_intensities(
        &mut self,
        measurement: u32,
        wavelengths: &[u16],
        intensities: &[f64],
    ) -> Result<(), Error> {
        self.intensities
            .as_mut()
            .expect("dataset open for writing")
            .push(measurement, wavelengths, intensities)
    }

    /* ----------------------------------------------------------------- Calibrations */

    /// Mark a measurement ID as a calibration measurement.
    pub fn calibration(&mut self, id: u32) {
        self.manifest.calibrations.push(id);
    }

    /* ----------------------------------------------------------------- Lifecycle */

    /// Flush all pending data and write the `.wr` file to disk.
    ///
    /// Consumes `self`. Use this when you need to handle write errors.
    /// The manifest `finished` flag remains `false`, allowing the file
    /// to be reopened for appending.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the IPC streams cannot be finalised or the file cannot be written.
    pub fn close(mut self) -> Result<(), Error> {
        self.write_to_disk()?;
        self.closed = true;
        Ok(())
    }

    /// Finalise and write the `.wray` file to disk.
    ///
    /// Like [`close`](Self::close) but sets `manifest.finished = true`,
    /// signalling that no more data will be appended.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the IPC streams cannot be finalised or the file cannot be written.
    pub fn finish(mut self) -> Result<(), Error> {
        self.manifest.finished = true;
        self.write_to_disk()?;
        self.closed = true;
        Ok(())
    }

    /// Write a snapshot of the current data to disk without consuming `self`.
    ///
    /// Useful for long-running experiments that need periodic durability.
    /// The in-memory streams continue accumulating data after this call.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if any IPC stream cannot be flushed or the file cannot be written.
    pub fn commit(&mut self) -> Result<(), Error> {
        // Flush builders into streams (but do not write EOS)
        if let Some(ref mut wl) = self.wavelengths {
            wl.flush()?;
        }
        if let Some(ref mut ms) = self.measurements {
            ms.flush()?;
        }
        if let Some(ref mut it) = self.intensities {
            it.flush()?;
        }
        // Snapshot bytes (no EOS) and write with EOS appended
        let wl = self.wl_snapshot();
        let ms = self.ms_snapshot();
        let it = self.it_snapshot();
        self.write_file(&wl, &ms, &it)
    }

    /* ----------------------------------------------------------------- Read API */

    /// Borrow the manifest metadata.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Read all wavelength records from the dataset.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the IPC stream is malformed or a column is missing.
    pub fn read_wavelengths(&self) -> Result<Vec<WavelengthRecord>, Error> {
        let bytes = self.wl_section();
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch?;
            let ids = batch
                .column_by_name("id")
                .ok_or_else(|| Error::MissingColumn("id".into()))?
                .as_primitive::<UInt16Type>();
            let nms = batch
                .column_by_name("nm")
                .ok_or_else(|| Error::MissingColumn("nm".into()))?
                .as_primitive::<Float32Type>();
            for i in 0..batch.num_rows() {
                out.push(WavelengthRecord {
                    id: ids.value(i),
                    nm: nms.value(i),
                });
            }
        }
        Ok(out)
    }

    /// Read all measurement records from the dataset.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the IPC stream is malformed or a column is missing.
    // TODO Move all read functions into respective modules e.g. self.wavelengths.read
    pub fn read_measurements(&self) -> Result<Vec<MeasurementRecord>, Error> {
        let bytes = self.ms_section();
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch?;
            let ids = col_primitive::<UInt32Type>(&batch, "id")?;
            let ts = col_primitive::<UInt64Type>(&batch, "timestamp")?;
            let xs = col_primitive::<Float32Type>(&batch, "x")?;
            let ys = col_primitive::<Float32Type>(&batch, "y")?;
            let zs = col_primitive::<Float32Type>(&batch, "z")?;
            let als = col_primitive::<Float32Type>(&batch, "a")?;
            let integ = col_primitive::<UInt64Type>(&batch, "integration")?;
            for i in 0..batch.num_rows() {
                out.push(MeasurementRecord {
                    id: ids.value(i),
                    timestamp: ts.value(i),
                    x: nullable(xs, i),
                    y: nullable(ys, i),
                    z: nullable(zs, i),
                    a: nullable(als, i),
                    integration: integ.value(i),
                });
            }
        }
        Ok(out)
    }

    /// Read all intensity records from the dataset.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the IPC stream is malformed or a column is missing.
    pub fn read_intensities(&self) -> Result<Vec<IntensityRecord>, Error> {
        let bytes = self.it_section();
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None)?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch?;
            let ms = col_primitive::<UInt32Type>(&batch, "measurement")?;
            let wls = col_primitive::<UInt16Type>(&batch, "wavelength")?;
            let vals = col_primitive::<Float64Type>(&batch, "intensity")?;
            for i in 0..batch.num_rows() {
                out.push(IntensityRecord {
                    measurement: ms.value(i),
                    wavelength: wls.value(i),
                    intensity: vals.value(i),
                });
            }
        }
        Ok(out)
    }

    /* ------------------------------------------------------------------------------- Internals */

    fn write_to_disk(&mut self) -> Result<(), Error> {
        if self.closed {
            return Ok(());
        }
        // Finish all streams (writes EOS into the Buf)
        if let Some(ref mut wl) = self.wavelengths {
            wl.finish()?;
        }
        if let Some(ref mut ms) = self.measurements {
            ms.finish()?;
        }
        if let Some(ref mut it) = self.intensities {
            it.finish()?;
        }
        let wl = self
            .wavelengths
            .as_ref()
            .map_or_else(Vec::new, wavelengths::Wavelengths::bytes);
        let ms = self
            .measurements
            .as_ref()
            .map_or_else(Vec::new, measurements::Measurements::bytes);
        let it = self
            .intensities
            .as_ref()
            .map_or_else(Vec::new, intensities::Intensities::bytes);
        self.write_file(&wl, &ms, &it)
    }

    fn wl_snapshot(&self) -> Vec<u8> {
        match &self.wavelengths {
            Some(wl) => eos(wl.bytes()),
            None => self.wl_bytes.clone(),
        }
    }

    fn ms_snapshot(&self) -> Vec<u8> {
        match &self.measurements {
            Some(ms) => eos(ms.bytes()),
            None => self.ms_bytes.clone(),
        }
    }

    fn it_snapshot(&self) -> Vec<u8> {
        match &self.intensities {
            Some(it) => eos(it.bytes()),
            None => self.it_bytes.clone(),
        }
    }

    /// Get the wavelengths IPC section bytes (with EOS, ready for [`StreamReader`]).
    fn wl_section(&self) -> Vec<u8> {
        match &self.wavelengths {
            Some(wl) => eos(wl.bytes()),
            None => self.wl_bytes.clone(),
        }
    }

    fn ms_section(&self) -> Vec<u8> {
        match &self.measurements {
            Some(ms) => eos(ms.bytes()),
            None => self.ms_bytes.clone(),
        }
    }

    fn it_section(&self) -> Vec<u8> {
        match &self.intensities {
            Some(it) => eos(it.bytes()),
            None => self.it_bytes.clone(),
        }
    }

    fn write_file(&self, wl: &[u8], ms: &[u8], it: &[u8]) -> Result<(), Error> {
        let manifest_str = toml::to_string(&self.manifest)?;
        let manifest_bytes = manifest_str.as_bytes();
        let header = Header {
            manifest_len: manifest_bytes.len() as u64,
            wavelengths_len: wl.len() as u64,
            measurements_len: ms.len() as u64,
            intensities_len: it.len() as u64,
        };
        let mut file = std::fs::File::create(&self.path)?;
        header.write(&mut file)?;
        std::io::Write::write_all(&mut file, manifest_bytes)?;
        std::io::Write::write_all(&mut file, wl)?;
        std::io::Write::write_all(&mut file, ms)?;
        std::io::Write::write_all(&mut file, it)?;
        Ok(())
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Drop for Dataset {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.write_to_disk();
        }
    }
}

/* ---------------------------------------------------------------------------- Helper Functions */

/// Append the 8-byte Arrow IPC EOS sentinel to a byte vector.
fn eos(mut bytes: Vec<u8>) -> Vec<u8> {
    // TODO // TODO Remove EOS constant. Bytes written automatically on arrow::ipc::writer::StreamWriter::finish
    bytes.extend_from_slice(&EOS);
    bytes
}

/// Extract a typed primitive column from a [`RecordBatch`] by name.
fn col_primitive<'a, T>(batch: &'a RecordBatch, name: &str) -> Result<&'a PrimitiveArray<T>, Error>
where
    T: ArrowPrimitiveType,
{
    batch
        .column_by_name(name)
        .ok_or_else(|| Error::MissingColumn(name.into()))
        .map(AsArray::as_primitive::<T>)
}

/// Read a nullable value at row `i`.
fn nullable<T>(arr: &PrimitiveArray<T>, i: usize) -> Option<T::Native>
where
    T: ArrowPrimitiveType,
{
    match arr.is_null(i) {
        true => None,
        false => Some(arr.value(i)),
    }
}
