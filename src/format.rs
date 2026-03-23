/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::io::{self, Read, Write};
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use crate::Error;

/* -------------------------------------------------------------------------------- Constants */

/// Magic bytes at the start of every `.wr` file.
pub(crate) const MAGIC: &[u8; 4] = b"WRAY";

/// Current format version.
pub(crate) const VERSION: u32 = 1;

/// Length (in bytes) of the fixed size file header.
// TODO How is semantic versioning stored in a single `u32`? Use three `u8` instead e.g. for 1.0.0?
// TODO Can HEADER size be determined at compiletime by defining what is in the header?
pub(crate) const HEADER: usize = MAGIC.len() + size_of::<u32>() + 4 * size_of::<u64>();

/// Arrow IPC end-of-stream sentinel (continuation marker + zero metadata size).
// TODO Remove EOS constant. Bytes written automatically on arrow::ipc::writer::StreamWriter::finish
pub(crate) const EOS: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

/* ------------------------------------------------------------------------------ Public Exports */

/// Physical unit for a coordinate axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Units {
    /// Length in metres.
    Length,
    /// Angle using radians.
    Angle,
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Display for Units {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

/* --------------------------------------------------------------------------------- Manifest */

/// Experiment-level metadata stored in every `.wr` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Wray format version.
    pub version: u32,
    /// Absolute UNIX epoch timestamp in microseconds when the dataset was created.
    pub timestamp: u64,
    /// Measurement IDs that are calibration measurements.
    pub calibrations: Vec<u32>,
    /// Whether the dataset has been explicitly finalised.
    pub finished: bool,
    /// Per-axis storage units.
    // TODO Remove ManifestUnits struct. Store Config struct.
    pub units: ManifestUnits,
}

/// Per-axis unit declarations in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManifestUnits {
    /// Unit for x coordinate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x: Option<Units>,
    /// Unit for y coordinate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub y: Option<Units>,
    /// Unit for z coordinate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub z: Option<Units>,
    /// Unit for angle coordinate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a: Option<Units>,
}

impl Manifest {
    /// Create a new manifest for the given creation timestamp and configuration.
    pub(crate) fn new(timestamp: u64, cfg: &Config) -> Self {
        Self {
            version: VERSION,
            timestamp,
            calibrations: Vec::with_capacity(8),
            finished: false,
            units: ManifestUnits {
                // TODO Remove ManifestUnits struct and store Config struct directly?
                x: cfg.x,
                y: cfg.y,
                z: cfg.z,
                a: cfg.a,
            },
        }
    }
}

/* ---------------------------------------------------------------------------------- Header */

/// The 40-byte header at the start of every `.wray` file.
pub(crate) struct Header {
    pub manifest_len: u64,
    pub wavelengths_len: u64,
    pub measurements_len: u64,
    pub intensities_len: u64,
}

impl Header {
    /// Write the header to `w`.
    pub fn write<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(MAGIC)?;
        w.write_all(&VERSION.to_le_bytes())?;
        w.write_all(&self.manifest_len.to_le_bytes())?;
        w.write_all(&self.wavelengths_len.to_le_bytes())?;
        w.write_all(&self.measurements_len.to_le_bytes())?;
        w.write_all(&self.intensities_len.to_le_bytes())?;
        Ok(())
    }

    /// Read and validate the header from `r`.
    pub fn read<R: Read>(r: &mut R) -> Result<Self, Error> {
        let mut buf = [0u8; HEADER];
        r.read_exact(&mut buf)?;
        if &buf[0..4] != MAGIC {
            return Err(Error::InvalidFormat("Invalid magic bytes".into()));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        if version != VERSION {
            return Err(Error::InvalidFormat(format!(
                "unsupported version: {version}"
            )));
        }
        Ok(Self {
            manifest_len: u64::from_le_bytes(buf[8..16].try_into().expect("8 bytes")),
            wavelengths_len: u64::from_le_bytes(buf[16..24].try_into().expect("8 bytes")),
            measurements_len: u64::from_le_bytes(buf[24..32].try_into().expect("8 bytes")),
            intensities_len: u64::from_le_bytes(buf[32..40].try_into().expect("8 bytes")),
        })
    }
}
