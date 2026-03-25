/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::format::{Config, HEADER, Header, Manifest, Segment, Table};
use crate::intensities::Intensities;
use crate::measurements::Measurements;
use crate::util::batches;
use crate::wavelengths::Wavelengths;
use crate::writer::Writer;
use crate::{Error, Intensity, Measurement, Wavelength, intensities, measurements, wavelengths};

/* ------------------------------------------------------------------------------ Public Exports */

/// A `.wr` dataset for storing optical spectroscopy data with optional metadata.
///
/// Use [`Dataset::new`] to create or open a dataset, push data via the table
/// accessor methods, and finalise with [`Dataset::close`] or [`Dataset::finish`].
/// The dataset is also written on [`Drop`].
pub struct Dataset {
    /// File path.
    path: PathBuf,
    /// Experiment metadata.
    manifest: Manifest,
    /// Wavelengths writer (`None` when read-only or closed).
    wavelengths: Option<Wavelengths>,
    /// Measurements writer (`None` when read-only or closed).
    measurements: Option<Measurements>,
    /// Intensities writer (`None` when read-only or closed).
    intensities: Option<Intensities>,
    /// Complete file data for segment reads.
    file_data: Vec<u8>,
    /// Whether the file uses finished (Arrow file) format.
    finished: bool,
    /// Whether the dataset has been written and closed.
    closed: bool,
}

impl Dataset {
    /// Create or open a dataset at `path`.
    ///
    /// If the file exists, it is opened (writable if unfinished, read-only if
    /// finished). Otherwise a new file is created with the given [`Config`].
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file cannot be read or written, or if the
    /// Arrow IPC stream writers cannot be initialised.
    pub fn new(path: impl AsRef<Path>, cfg: &Config) -> Result<Self, Error> {
        match path.as_ref().exists() {
            true => Self::open(path),
            false => Self::create(path, cfg),
        }
    }

    /// Open an existing `.wr` file.
    ///
    /// Unfinished files are opened for appending (wavelength dedup cache and
    /// measurement ID sequence are restored). Finished files are read-only.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file cannot be read, the header is invalid,
    /// or the manifest TOML is malformed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let data = std::fs::read(path.as_ref())?;
        let header = Header::read(&mut &data[..])?;
        let m_start = header.manifest_offset as usize;
        let m_end = m_start + header.manifest_len as usize;
        let manifest: Manifest = toml::from_str(std::str::from_utf8(&data[m_start..m_end])?)?;

        if header.finished {
            return Ok(Self {
                path: path.as_ref().to_path_buf(),
                manifest,
                wavelengths: None,
                measurements: None,
                intensities: None,
                file_data: data,
                finished: true,
                closed: true,
            });
        }

        // Restore wavelength dedup cache from existing segments.
        let mut wl_batches = Vec::new();
        for stream in segment_streams(&data, &manifest.segments, Table::Wavelengths) {
            wl_batches.extend(batches(&stream)?);
        }
        let existing_wl = wavelengths::decode(&wl_batches)?;

        // Restore next measurement ID from existing segments.
        let mut ms_batches = Vec::new();
        for stream in segment_streams(&data, &manifest.segments, Table::Measurements) {
            ms_batches.extend(batches(&stream)?);
        }
        let existing_ms = measurements::decode(&ms_batches)?;
        let next_ms = existing_ms.iter().map(|r| r.id).max().map_or(0, |id| id + 1);

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            wavelengths: Some(Wavelengths::new(existing_wl)?),
            measurements: Some(Measurements::new(manifest.timestamp, next_ms)?),
            intensities: Some(Intensities::new()?),
            manifest,
            file_data: data,
            finished: false,
            closed: false,
        })
    }

    /* ----------------------------------------------------------------------- Table Accessors */

    /// Mutable access to the wavelengths table writer.
    ///
    /// # Panics
    ///
    /// Panics if the dataset is not open for writing.
    pub fn wavelengths(&mut self) -> &mut Wavelengths {
        self.wavelengths.as_mut().expect("dataset open for writing")
    }

    /// Mutable access to the measurements table writer.
    ///
    /// # Panics
    ///
    /// Panics if the dataset is not open for writing.
    pub fn measurements(&mut self) -> &mut Measurements {
        self.measurements.as_mut().expect("dataset open for writing")
    }

    /// Mutable access to the intensities table writer.
    ///
    /// # Panics
    ///
    /// Panics if the dataset is not open for writing.
    pub fn intensities(&mut self) -> &mut Intensities {
        self.intensities.as_mut().expect("dataset open for writing")
    }

    /* --------------------------------------------------------------------------- Metadata */

    /// Mark a measurement ID as a calibration measurement.
    pub fn calibration(&mut self, id: u32) {
        self.manifest.calibrations.push(id);
    }

    /// Borrow the manifest metadata.
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Whether the dataset has been finalised via [`finish`](Self::finish).
    pub const fn is_finished(&self) -> bool {
        self.finished
    }

    /* ------------------------------------------------------------------------------ Read */

    /// Read all wavelength records from the dataset.
    pub fn read_wavelengths(&self) -> Result<Vec<Wavelength>, Error> {
        wavelengths::decode(&self.read_batches(Table::Wavelengths)?)
    }

    /// Read all measurement records from the dataset.
    pub fn read_measurements(&self) -> Result<Vec<Measurement>, Error> {
        measurements::decode(&self.read_batches(Table::Measurements)?)
    }

    /// Read all intensity records from the dataset.
    pub fn read_intensities(&self) -> Result<Vec<Intensity>, Error> {
        intensities::decode(&self.read_batches(Table::Intensities)?)
    }

    /* ----------------------------------------------------------------------------- Write */

    /// Flush and write the `.wr` file to disk. Sets `closed = true`.
    pub fn close(mut self) -> Result<(), Error> {
        self.write_to_disk()?;
        self.closed = true;
        Ok(())
    }

    /// Consolidate into Arrow IPC **file** format, seal, and write to disk.
    pub fn finish(mut self) -> Result<(), Error> {
        self.write_segmented()?;
        self.write_finished(&self.path.clone())?;
        self.closed = true;
        Ok(())
    }

    /// Finalise to a **new** file at `path`, leaving the original unchanged.
    ///
    /// The original file remains unfinished and appendable. The new file
    /// contains all data consolidated into Arrow IPC file format.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the IPC streams cannot be finalised or the file
    /// cannot be written.
    pub fn finish_to(&mut self, path: impl AsRef<Path>) -> Result<(), Error> {
        self.write_finished(path.as_ref(), false)
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

    /* ---------------------------------------------------------------------------- Private */

    /// Create a brand-new `.wr` dataset.
    fn create(path: impl AsRef<Path>, cfg: &Config) -> Result<Self, Error> {
        let timestamp: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_micros()
            .try_into()
            .expect("microsecond timestamp exceeds u64");
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            manifest: Manifest::new(timestamp, cfg),
            wavelengths: Some(Wavelengths::new(Vec::new())?),
            measurements: Some(Measurements::new(timestamp, 0)?),
            intensities: Some(Intensities::new()?),
            file_data: Vec::new(),
            finished: false,
            closed: false,
        })
    }

    /// Decode all [`RecordBatch`]es for a table from disk segments.
    fn read_batches(&self, table: Table) -> Result<Vec<arrow::array::RecordBatch>, Error> {
        let mut all = Vec::new();
        for bytes in segment_streams(&self.file_data, &self.manifest.segments, table) {
            let decoded = match self.finished {
                true => crate::util::file_batches(&bytes)?,
                false => batches(&bytes)?,
            };
            all.extend(decoded);
        }
        Ok(all)
    }

    /// Flush pending data and write the segmented file to disk.
    fn write_to_disk(&mut self) -> Result<(), Error> {
        match self.closed {
            true => Ok(()),
            false => self.write_segmented(),
        }
    }

    /// Extract pending bytes via [`take_bytes`], append as new segments, rewrite file.
    fn write_segmented(&mut self) -> Result<(), Error> {
        let new_wl = self.wavelengths.as_mut().map(|w| w.take_bytes()).transpose()?.unwrap_or_default();
        let new_ms = self.measurements.as_mut().map(|m| m.take_bytes()).transpose()?.unwrap_or_default();
        let new_it = self.intensities.as_mut().map(|i| i.take_bytes()).transpose()?.unwrap_or_default();

        let existing_end = self
            .manifest
            .segments
            .iter()
            .map(|s| s.offset + s.length)
            .max()
            .unwrap_or(HEADER as u64);

        let mut segments = self.manifest.segments.clone();
        let mut offset = existing_end;

        for (table, bytes) in [
            (Table::Wavelengths, &new_wl),
            (Table::Measurements, &new_ms),
            (Table::Intensities, &new_it),
        ] {
            if !bytes.is_empty() {
                segments.push(Segment { table, offset, length: bytes.len() as u64 });
                offset += bytes.len() as u64;
            }
        }

        self.manifest.segments = segments;
        let manifest_str = toml::to_string(&self.manifest)?;
        let manifest_bytes = manifest_str.as_bytes();

        let header = Header {
            manifest_offset: offset,
            manifest_len: manifest_bytes.len() as u64,
            finished: false,
        };

        let end = existing_end as usize;
        let existing = match end > HEADER && self.file_data.len() >= end {
            true => &self.file_data[HEADER..end],
            false => &[] as &[u8],
        };

        let mut file = std::fs::File::create(&self.path)?;
        header.write(&mut file)?;
        std::io::Write::write_all(&mut file, existing)?;
        std::io::Write::write_all(&mut file, &new_wl)?;
        std::io::Write::write_all(&mut file, &new_ms)?;
        std::io::Write::write_all(&mut file, &new_it)?;
        std::io::Write::write_all(&mut file, manifest_bytes)?;

        self.file_data = std::fs::read(&self.path)?;

        if let Some(ref mut wl) = self.wavelengths { wl.reset()?; }
        if let Some(ref mut ms) = self.measurements { ms.reset()?; }
        if let Some(ref mut it) = self.intensities { it.reset()?; }

        Ok(())
    }

    /// Consolidate all segments into Arrow IPC file format and write to `path`.
    fn write_finished(&self, path: &Path) -> Result<(), Error> {
        let wl = consolidate::<Wavelengths>(&self.file_data, &self.manifest.segments, Table::Wavelengths)?;
        let ms = consolidate::<Measurements>(&self.file_data, &self.manifest.segments, Table::Measurements)?;
        let it = consolidate::<Intensities>(&self.file_data, &self.manifest.segments, Table::Intensities)?;

        let mut segments = Vec::with_capacity(3);
        let mut offset = HEADER as u64;

        for (table, bytes) in [
            (Table::Wavelengths, &wl),
            (Table::Measurements, &ms),
            (Table::Intensities, &it),
        ] {
            if !bytes.is_empty() {
                segments.push(Segment { table, offset, length: bytes.len() as u64 });
                offset += bytes.len() as u64;
            }
        }

        let mut manifest = self.manifest.clone();
        manifest.segments = segments;
        let manifest_str = toml::to_string(&manifest)?;
        let manifest_bytes = manifest_str.as_bytes();

        let header = Header {
            manifest_offset: offset,
            manifest_len: manifest_bytes.len() as u64,
            finished: true,
        };

        let mut file = std::fs::File::create(path)?;
        header.write(&mut file)?;
        std::io::Write::write_all(&mut file, &wl)?;
        std::io::Write::write_all(&mut file, &ms)?;
        std::io::Write::write_all(&mut file, &it)?;
        std::io::Write::write_all(&mut file, manifest_bytes)?;

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

/// Collect each segment's bytes as a separate IPC stream for a given table.
fn segment_streams(data: &[u8], segments: &[Segment], table: Table) -> Vec<Vec<u8>> {
    segments
        .iter()
        .filter(|s| s.table == table)
        .filter_map(|seg| {
            let start = seg.offset as usize;
            let end = start + seg.length as usize;
            match end <= data.len() {
                true => Some(data[start..end].to_vec()),
                false => None,
            }
        })
        .collect()
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
    if all.is_empty() {
        return Ok(Vec::new());
    }
    let mut writer = W::new_file_writer()?;
    for batch in &all {
        writer.write(batch)?;
    }
    writer.finish()?;
    let buf = writer.into_inner()?;
    let cursor = buf.into_inner().map_err(|e| Error::Io(e.into_error()))?;
    Ok(cursor.into_inner())
}
