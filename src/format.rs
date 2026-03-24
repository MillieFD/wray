/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::Error;

/* -------------------------------------------------------------------------------- Constants */

/// Magic bytes at the start of every `.wr` file.
pub(crate) const MAGIC: &[u8; 4] = b"WRAY";

/// Current format version (major, minor, patch).
pub(crate) const VERSION: [u8; 3] = [0, 2, 0];

/// Length (in bytes) of the fixed-size file header.
///
/// Layout: `MAGIC(4) + VERSION(3) + FINISHED(1) + manifest_offset(8) + manifest_len(8) = 24`.
pub(crate) const HEADER: usize = 24;

/* ------------------------------------------------------------------------------ Public Exports */

/// Physical dimension for a coordinate axis.
///
/// All values are stored as raw `f32` in SI base units:
/// metres for [`Length`](Units::Length), radians for [`Angle`](Units::Angle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Units {
    /// Length in metres.
    Length,
    /// Angle in radians.
    Angle,
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl core::fmt::Display for Units {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::Length => f.write_str("m"),
            Self::Angle => f.write_str("rad"),
        }
    }
}

/* ------------------------------------------------------------------------------ Configuration */

/// Per-axis dataset configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Dimension for the `x` coordinate axis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x: Option<Units>,
    /// Dimension for the `y` coordinate axis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub y: Option<Units>,
    /// Dimension for the `z` coordinate axis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub z: Option<Units>,
    /// Dimension for the `a` coordinate axis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a: Option<Units>,
    /// Dimension for the `b` coordinate axis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub b: Option<Units>,
    /// Dimension for the `c` coordinate axis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<Units>,
}

/* ------------------------------------------------------------------------------ Format Enum */

/// File format encoding stored in the binary header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Arrow IPC stream format — supports reading and appending.
    Unfinished,
    /// Arrow IPC file format — compression and random-access reads.
    Finished,
}

/* ------------------------------------------------------------------------------------ Manifest */

/// Identifies which Arrow table a [`Segment`] belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Table {
    /// The wavelengths table.
    Wavelengths,
    /// The measurements table.
    Measurements,
    /// The intensities table.
    Intensities,
}

/// A contiguous byte range of Arrow IPC data within the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    /// Which table this segment belongs to.
    pub table: Table,
    /// Byte offset from the start of the file.
    pub offset: u64,
    /// Length in bytes.
    pub length: u64,
}

/// Experiment-level metadata stored in every `.wray` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Absolute UNIX epoch timestamp in microseconds when the dataset was created.
    pub timestamp: u64,
    /// Measurement IDs that are calibration measurements.
    pub calibrations: Vec<u32>,
    /// Per-axis dimension configuration.
    pub axes: Config,
    /// Segment index — byte ranges for each table's IPC data.
    pub segments: Vec<Segment>,
}

impl Manifest {
    /// Create a new [`Manifest`] for the given creation timestamp and [`Config`].
    pub(crate) fn new(timestamp: u64, cfg: &Config) -> Self {
        Self {
            timestamp,
            calibrations: Vec::with_capacity(8),
            axes: cfg.clone(),
            segments: Vec::new(),
        }
    }
}

/* ---------------------------------------------------------------------------------- Header */

/// The 24-byte header at the start of every `.wr` file.
///
/// Layout: `MAGIC(4) + VERSION(3) + FINISHED(1) + manifest_offset(8) + manifest_len(8)`.
pub(crate) struct Header {
    /// Byte offset of the TOML manifest from the start of the file.
    pub manifest_offset: u64,
    /// Length of the TOML manifest in bytes.
    pub manifest_len: u64,
    /// Whether the file uses the finished (Arrow file) format.
    pub finished: bool,
}

impl Header {
    /// Write the header to `w`.
    pub fn write<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(MAGIC)?;
        w.write_all(&VERSION)?;
        w.write_all(&[u8::from(self.finished)])?;
        w.write_all(&self.manifest_offset.to_le_bytes())?;
        w.write_all(&self.manifest_len.to_le_bytes())?;
        Ok(())
    }

    /// Read and validate the header from `r`.
    pub fn read<R: Read>(r: &mut R) -> Result<Self, Error> {
        let mut buf = [0u8; HEADER];
        r.read_exact(&mut buf)?;
        if &buf[0..4] != MAGIC {
            return Err(Error::InvalidFormat("invalid magic bytes".into()));
        }
        if buf[4] != VERSION[0] {
            return Err(Error::InvalidFormat(format!(
                "unsupported version: {}.{}.{}",
                buf[4], buf[5], buf[6]
            )));
        }
        Ok(Self {
            finished: buf[7] != 0,
            manifest_offset: u64::from_le_bytes(buf[8..16].try_into().expect("8 bytes")),
            manifest_len: u64::from_le_bytes(buf[16..24].try_into().expect("8 bytes")),
        })
    }
}
