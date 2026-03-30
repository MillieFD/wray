/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::SystemTime;

use crate::Error;
use crate::format::{Config, Format, HEADER, Header, Manifest, Segment};
use crate::intensities::Intensities;
use crate::measurements::Measurements;
use crate::table::Sink;
use crate::wavelengths::Wavelengths;

/* ------------------------------------------------------------------------------ Public Exports */

/// A writable `.wr` dataset backed by Arrow IPC stream segments.
///
/// # Lifecycle
///
/// 1. **Create** a new file with [`new`](Self::new).
/// 2. **Push** wavelengths, measurements, and intensities via the public table fields.
/// 3. [`close`](Self::close) the file once all data has been written.
/// 4. Reopen with [`new`](Self::new) to append additional data.
/// 5. Seal with [`finish`](Self::finish) for read-only access, or create a snapshot with
///    [`snapshot`](Self::snapshot).
pub struct Dataset {
    /// Experiment metadata.
    pub manifest: Manifest,
    /// Wavelengths table.
    pub wavelengths: Wavelengths,
    /// Measurements table.
    pub measurements: Measurements,
    /// Intensities table.
    pub intensities: Intensities,
}

impl Dataset {
    /// Create or open a dataset at `path`.
    ///
    /// If the file exists and is unfinished, it is reopened for appending.
    /// If it does not exist, a new file is created with the given [`Config`].
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file is finished (use
    /// [`Dataset::open`](super::Dataset::open) instead), cannot be read, or the
    /// Arrow IPC writers fail to initialise.
    pub fn new(path: impl AsRef<Path>, cfg: &Config) -> Result<Self, Error> {
        let path = path.as_ref();
        match path.exists() {
            true => {
                let header = Header::new(path)?;
                if header.format != Format::Unfinished {
                    return Err(Error::InvalidFormat("cannot append to finished dataset"));
                }
                header.manifest()?.try_into()
            }
            false => Self::create(path, cfg),
        }
    }

    /// Mark a measurement ID as a calibration measurement.
    pub fn calibration(&mut self, id: u32) {
        self.manifest.calibrations.push(id);
    }

    /* ----------------------------------------------------------------------------- Write */

    /// Flush pending data and write the `.wr` file to disk.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file cannot be written.
    pub fn close(mut self) -> Result<(), Error> {
        self.write_segmented()?;
        std::mem::forget(self);
        Ok(())
    }

    /// Consolidate into Arrow IPC **file** format, seal, and return a
    /// [`finished::Dataset`](super::finished::Dataset) handle.
    ///
    /// The original file is overwritten in place.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file cannot be written or consolidated.
    pub fn finish(mut self) -> Result<super::finished::Dataset, Error> {
        self.write_segmented()?;
        let path = self.manifest.path.clone();
        let manifest = self.write_finished(&path)?;
        std::mem::forget(self);
        super::finished::Dataset::new(manifest)
    }

    /// Consolidate to a **new** file at `path`, leaving the original appendable.
    ///
    /// Returns a [`finished::Dataset`](super::finished::Dataset) for the sealed
    /// copy.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if either file cannot be written or consolidated.
    pub fn snapshot(&mut self, path: impl AsRef<Path>) -> Result<super::finished::Dataset, Error> {
        self.write_segmented()?;
        let manifest = self.write_finished(path.as_ref())?;
        super::finished::Dataset::new(manifest)
    }

    /* ---------------------------------------------------------------------------- Private */

    /// Create a brand-new dataset.
    fn create(path: &Path, cfg: &Config) -> Result<Self, Error> {
        let timestamp: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_micros()
            .try_into()
            .expect("microsecond timestamp exceeds u64");
        let manifest = Manifest::new(path, timestamp, cfg);
        let wavelengths = Wavelengths::new(&manifest)?;
        let measurements = Measurements::new(&manifest)?;
        let intensities = Intensities::new(&manifest)?;
        Ok(Self {
            wavelengths,
            measurements,
            intensities,
            manifest,
        })
    }

    /// Extract pending bytes, append as new segments, rewrite file.
    fn write_segmented(&mut self) -> Result<(), Error> {
        let new_wl = self.wavelengths.take_bytes()?;
        let new_ms = self.measurements.take_bytes()?;
        let new_it = self.intensities.take_bytes()?;

        let existing_end = self
            .manifest
            .wavelengths
            .iter()
            .chain(&self.manifest.measurements)
            .chain(&self.manifest.intensities)
            .map(Segment::end)
            .max()
            .unwrap_or(HEADER as u64);

        let mut wl_segs = self.manifest.wavelengths.clone();
        let mut ms_segs = self.manifest.measurements.clone();
        let mut it_segs = self.manifest.intensities.clone();
        let mut offset = existing_end;

        if !new_wl.is_empty() {
            wl_segs.push(Segment {
                offset: SeekFrom::Start(offset),
                length: new_wl.len() as u64,
            });
            offset += new_wl.len() as u64;
        }
        if !new_ms.is_empty() {
            ms_segs.push(Segment {
                offset: SeekFrom::Start(offset),
                length: new_ms.len() as u64,
            });
            offset += new_ms.len() as u64;
        }
        if !new_it.is_empty() {
            it_segs.push(Segment {
                offset: SeekFrom::Start(offset),
                length: new_it.len() as u64,
            });
            offset += new_it.len() as u64;
        }

        self.manifest.wavelengths = wl_segs;
        self.manifest.measurements = ms_segs;
        self.manifest.intensities = it_segs;
        let manifest_str = toml::to_string(&self.manifest)?;
        let manifest_bytes = manifest_str.as_bytes();

        let header = Header {
            path: self.manifest.path.clone(),
            manifest: Segment {
                offset: SeekFrom::Start(offset),
                length: manifest_bytes.len() as u64,
            },
            format: Format::Unfinished,
        };

        let existing = match existing_end > HEADER as u64 {
            true => {
                let mut src = std::fs::File::open(&self.manifest.path)?;
                src.seek(SeekFrom::Start(HEADER as u64))?;
                let len = (existing_end - HEADER as u64) as usize;
                let mut buf = vec![0u8; len];
                src.read_exact(&mut buf)?;
                buf
            }
            false => Vec::new(),
        };

        let mut file = std::fs::File::create(&self.manifest.path)?;
        header.write(&mut file)?;
        std::io::Write::write_all(&mut file, &existing)?;
        std::io::Write::write_all(&mut file, &new_wl)?;
        std::io::Write::write_all(&mut file, &new_ms)?;
        std::io::Write::write_all(&mut file, &new_it)?;
        std::io::Write::write_all(&mut file, manifest_bytes)?;

        self.wavelengths.reset(self.manifest.wavelengths.clone())?;
        self.measurements
            .reset(self.manifest.measurements.clone())?;
        self.intensities.reset(self.manifest.intensities.clone())?;

        Ok(())
    }

    /// Consolidate all segments into Arrow IPC file format and write to `path`.
    ///
    /// Returns the [`Manifest`] for the finished file, with its
    /// [`path`](Manifest::path) field set to `path`.
    fn write_finished(&self, path: &Path) -> Result<Manifest, Error> {
        let wl = self.wavelengths.finish()?;
        let ms = self.measurements.finish()?;
        let it = self.intensities.finish()?;

        let mut manifest = self.manifest.clone();
        let mut offset = HEADER as u64;
        manifest.wavelengths = Vec::new();
        manifest.measurements = Vec::new();
        manifest.intensities = Vec::new();

        if !wl.is_empty() {
            manifest.wavelengths.push(Segment {
                offset: SeekFrom::Start(offset),
                length: wl.len() as u64,
            });
            offset += wl.len() as u64;
        }
        if !ms.is_empty() {
            manifest.measurements.push(Segment {
                offset: SeekFrom::Start(offset),
                length: ms.len() as u64,
            });
            offset += ms.len() as u64;
        }
        if !it.is_empty() {
            manifest.intensities.push(Segment {
                offset: SeekFrom::Start(offset),
                length: it.len() as u64,
            });
            offset += it.len() as u64;
        }

        let manifest_str = toml::to_string(&manifest)?;
        let manifest_bytes = manifest_str.as_bytes();

        let header = Header {
            path: path.to_path_buf(),
            manifest: Segment {
                offset: SeekFrom::Start(offset),
                length: manifest_bytes.len() as u64,
            },
            format: Format::Finished,
        };

        let mut file = std::fs::File::create(path)?;
        header.write(&mut file)?;
        std::io::Write::write_all(&mut file, &wl)?;
        std::io::Write::write_all(&mut file, &ms)?;
        std::io::Write::write_all(&mut file, &it)?;
        std::io::Write::write_all(&mut file, manifest_bytes)?;

        manifest.path = path.to_path_buf();
        Ok(manifest)
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl TryFrom<Manifest> for Dataset {
    type Error = Error;

    fn try_from(manifest: Manifest) -> Result<Self, Self::Error> {
        Ok(Self {
            wavelengths: Wavelengths::new(&manifest)?,
            measurements: Measurements::new(&manifest)?,
            intensities: Intensities::new(&manifest)?,
            manifest,
        })
    }
}

impl Drop for Dataset {
    fn drop(&mut self) {
        let _ = self.write_segmented();
    }
}
