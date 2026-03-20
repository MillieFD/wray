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
use uom::si::angle::{degree, radian};
use uom::si::f64::{Angle, Length};
use uom::si::length::{meter, micrometer, millimeter, nanometer};

use crate::Error;

/* -------------------------------------------------------------------------------- Constants */

/// Magic bytes at the start of every `.wray` file.
pub(crate) const MAGIC: &[u8; 4] = b"WRAY";

/// Current format version.
pub(crate) const VERSION: u32 = 1;

/// Fixed header size in bytes.
pub(crate) const HEADER_SIZE: usize = 40;

/// Arrow IPC end-of-stream sentinel (continuation marker + zero metadata size).
pub(crate) const EOS: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

/* ------------------------------------------------------------------------------ Public Exports */

/// Physical unit for a coordinate axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Units {
    /// Nanometres.
    Nm,
    /// Micrometres.
    Um,
    /// Millimetres.
    Mm,
    /// Metres.
    M,
    /// Degrees.
    Deg,
    /// Radians.
    Rad,
}

impl Units {
    /// Returns `true` for length units (nm, µm, mm, m).
    pub fn is_length(self) -> bool {
        matches!(self, Self::Nm | Self::Um | Self::Mm | Self::M)
    }

    /// Returns `true` for angle units (deg, rad).
    pub fn is_angle(self) -> bool {
        matches!(self, Self::Deg | Self::Rad)
    }

    /// Convert a [`Length`] to `f32` in this unit.
    ///
    /// # Panics
    ///
    /// Panics if `self` is an angle unit ([`Units::Deg`] or [`Units::Rad`]).
    pub fn length_to_f32(self, v: Length) -> f32 {
        match self {
            Self::Nm => v.get::<nanometer>() as f32,
            Self::Um => v.get::<micrometer>() as f32,
            Self::Mm => v.get::<millimeter>() as f32,
            Self::M => v.get::<meter>() as f32,
            _ => panic!("not a length unit"),
        }
    }

    /// Convert an [`Angle`] to `f32` in this unit.
    ///
    /// # Panics
    ///
    /// Panics if `self` is a length unit ([`Units::Nm`], [`Units::Um`], [`Units::Mm`], or [`Units::M`]).
    pub fn angle_to_f32(self, v: Angle) -> f32 {
        match self {
            Self::Deg => v.get::<degree>() as f32,
            Self::Rad => v.get::<radian>() as f32,
            _ => panic!("not an angle unit"),
        }
    }

    /// Convert a raw `f32` stored in `self` units to the requested `target` units.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `target` are not in the same physical dimension
    /// (e.g. converting a length unit to an angle unit).
    pub fn convert(self, value: f32, target: Self) -> f32 {
        match (self.is_length(), target.is_length()) {
            (true, true) => {
                let base = f64::from(value) * self.length_scale();
                (base / target.length_scale()) as f32
            }
            (false, false) => {
                let base = f64::from(value) * self.angle_scale();
                (base / target.angle_scale()) as f32
            }
            _ => panic!("cannot convert between length and angle"),
        }
    }

    fn length_scale(self) -> f64 {
        match self {
            Self::Nm => 1e-9,
            Self::Um => 1e-6,
            Self::Mm => 1e-3,
            Self::M => 1.0,
            _ => unreachable!(),
        }
    }

    fn angle_scale(self) -> f64 {
        match self {
            Self::Deg => std::f64::consts::PI / 180.0,
            Self::Rad => 1.0,
            _ => unreachable!(),
        }
    }
}

impl Display for Units {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Nm => f.write_str("nm"),
            Self::Um => f.write_str("um"),
            Self::Mm => f.write_str("mm"),
            Self::M => f.write_str("m"),
            Self::Deg => f.write_str("deg"),
            Self::Rad => f.write_str("rad"),
        }
    }
}

/* ------------------------------------------------------------------------------ Configuration */

/// Options for creating a new [`Dataset`](crate::Dataset).
///
/// Specify which coordinate axes are active and their storage units.
/// Omit an axis to leave it unused (nullable column, all nulls).
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Unit for the x coordinate axis.
    pub x: Option<Units>,
    /// Unit for the y coordinate axis.
    pub y: Option<Units>,
    /// Unit for the z coordinate axis.
    pub z: Option<Units>,
    /// Unit for the angle coordinate axis.
    pub a: Option<Units>,
}

/* --------------------------------------------------------------------------------- Manifest */

/// Experiment-level metadata stored in every `.wray` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version (currently `1.0`).
    pub version: f64,
    /// Absolute UNIX epoch timestamp in microseconds when the dataset was created.
    pub timestamp: i64,
    /// Measurement IDs that are calibration measurements.
    pub calibrations: Vec<u32>,
    /// Whether the dataset has been explicitly finalised.
    pub finished: bool,
    /// Per-axis storage units.
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
    pub(crate) fn new(timestamp: i64, cfg: &Config) -> Self {
        Self {
            version: 1.0,
            timestamp,
            calibrations: Vec::new(),
            finished: false,
            units: ManifestUnits {
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
        let mut buf = [0u8; HEADER_SIZE];
        r.read_exact(&mut buf)?;
        if &buf[0..4] != MAGIC {
            return Err(Error::InvalidFormat("invalid magic bytes".into()));
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

/* --------------------------------------------------------------------------- Shared Buffer */

/// A clonable write-only buffer backed by `Rc<RefCell<Vec<u8>>>`.
///
/// [`StreamWriter`](arrow::ipc::writer::StreamWriter) takes ownership of its
/// inner writer. `Buf` allows a second handle to read accumulated bytes at any
/// time (e.g. for [`commit`](crate::Dataset::commit) snapshots).
#[derive(Clone)]
pub(crate) struct Buf(Rc<RefCell<Vec<u8>>>);

impl Buf {
    /// Create an empty buffer.
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(Vec::new())))
    }

    /// Clone the accumulated bytes.
    pub fn bytes(&self) -> Vec<u8> {
        self.0.borrow().clone()
    }
}

impl Write for Buf {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
