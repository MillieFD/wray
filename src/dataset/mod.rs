/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ------------------------------------------------------------------------------------- Modules */

pub mod finished;
pub mod unfinished;

/* ----------------------------------------------------------------------------- Private Imports */

use std::path::Path;

use crate::Error;
use crate::format::{Format, Header, Manifest};

/* ------------------------------------------------------------------------------ Public Exports */

/// A `.wr` dataset for storing optical spectroscopy data.
///
/// Provides type-safe separation between [`Unfinished`] and [`Finished`] file states.
pub enum Dataset {
    /// Writable dataset backed by Arrow IPC stream segments.
    Unfinished(unfinished::Dataset),
    /// Read-only dataset backed by Arrow IPC file segments.
    Finished(finished::Dataset),
}

impl Dataset {
    /// Open an existing `.wr` file.
    ///
    /// Returns [`Unfinished`](Self::Unfinished) for appendable files and
    /// [`Finished`](Self::Finished) for sealed read-only files.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if the file cannot be read, the header is invalid,
    /// or the manifest TOML is malformed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let header = Header::new(path)?;
        let manifest = header.manifest()?;
        match header.format {
            Format::Finished => {
                finished::Dataset::try_from(manifest).map(Self::Finished)
            }
            Format::Unfinished => {
                unfinished::Dataset::try_from(manifest).map(Self::Unfinished)
            }
        }
    }

    /// Borrow the experiment metadata.
    pub fn manifest(&self) -> &Manifest {
        match self {
            Self::Unfinished(ds) => ds.manifest(),
            Self::Finished(ds) => ds.manifest(),
        }
    }

    /// Whether the dataset has been sealed via [`finish`](unfinished::Dataset::finish).
    pub const fn is_finished(&self) -> bool {
        match self {
            Self::Finished(_) => true,
            Self::Unfinished(_) => false,
        }
    }
}
