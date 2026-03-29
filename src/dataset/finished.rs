/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::path::Path;

use crate::Error;
use crate::format::Manifest;
use crate::intensities::Intensities;
use crate::measurements::Measurements;
use crate::wavelengths::Wavelengths;

/* ------------------------------------------------------------------------------ Public Exports */

/// A sealed, read-only `.wr` dataset backed by Arrow IPC file segments.
///
/// Finished datasets are immutable and use memory-mapped I/O for zero-copy
/// random-access reads. Construct via [`Dataset::open`](super::Dataset::open)
/// or [`unfinished::Dataset::finish`](super::unfinished::Dataset::finish).
pub struct Dataset {
    /// Experiment metadata.
    manifest: Manifest,
    /// Wavelengths table (read-only).
    pub wavelengths: Wavelengths,
    /// Measurements table (read-only).
    pub measurements: Measurements,
    /// Intensities table (read-only).
    pub intensities: Intensities,
}

impl Dataset {
    /// Construct a finished dataset from a path and pre-read [`Manifest`].
    pub(crate) fn new(path: impl AsRef<Path>, manifest: Manifest) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        Ok(Self {
            wavelengths: Wavelengths::new(path.clone(), manifest.wavelengths.clone(), false)?,
            measurements: Measurements::new(
                path.clone(),
                manifest.measurements.clone(),
                false,
                manifest.timestamp,
                0,
            )?,
            intensities: Intensities::new(path, manifest.intensities.clone(), false)?,
            manifest,
        })
    }

    /// Borrow the experiment metadata.
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}
