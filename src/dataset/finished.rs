/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

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
    /// Borrow the experiment metadata.
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl TryFrom<Manifest> for Dataset {
    type Error = Error;

    /// Construct a finished dataset from a [`Manifest`].
    ///
    /// The manifest's [`path`](Manifest::path) field must be set before calling;
    /// [`read_header`](super::read_header) sets it automatically.
    fn try_from(manifest: Manifest) -> Result<Self, Self::Error> {
        let wavelengths = Wavelengths::new(&manifest.path, manifest.wavelengths.clone(), false)?;
        let measurements = Measurements::new(
            &manifest.path,
            manifest.measurements.clone(),
            false,
            manifest.timestamp,
            0,
        )?;
        let intensities = Intensities::new(&manifest.path, manifest.intensities.clone(), false)?;
        Ok(Self {
            wavelengths,
            measurements,
            intensities,
            manifest,
        })
    }
}
