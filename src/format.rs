/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Take, Write};
use std::mem::size_of;

use arrow::ipc::reader::StreamReader;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/* ----------------------------------------------------------------------------------- Constants */

/// Magic bytes to identify the start of every `.wr` file.
pub(super) const MAGIC: &[u8; 4] = b"WRAY";

/// Format version number.
pub(super) const VERSION: u8 = 1;

/// Length (in bytes) of the fixed-size file header.
///
/// Derived at compile time from the sizes of its constituent fields:
/// `MAGIC(4) + manifest_offset(8) + manifest_len(8) + VERSION(1) + file_type(1)`.
pub(super) const HEADER: usize = size_of::<[u8; 4]>()  // MAGIC
    + size_of::<u64>()  // manifest_offset
    + size_of::<u64>()  // manifest_len
    + size_of::<u8>()   // VERSION
    + size_of::<u8>();  // file_type

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

/* ------------------------------------------------------------------------------- Configuration */

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

/* --------------------------------------------------------------------------------- Format Enum */

/// File type stored in the binary header.
///
/// Encoded as a single byte (`u8`) in the file header.
/// `0` = [`Unfinished`](Format::Unfinished), `1` = [`Finished`](Format::Finished).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Format {
    /// Arrow IPC stream format — supports reading and appending.
    Unfinished,
    /// Arrow IPC file format — compression and random-access reads.
    Finished,
}

impl From<Format> for u8 {
    fn from(f: Format) -> Self {
        match f {
            Format::Unfinished => 0,
            Format::Finished => 1,
        }
    }
}

impl TryFrom<u8> for Format {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unfinished),
            1 => Ok(Self::Finished),
            _ => Err(crate::Error::InvalidFormat("unknown file type")),
        }
    }
}

/* ------------------------------------------------------------------------------------ Manifest */

/// A contiguous byte range of Arrow IPC data within the file.
#[derive(Debug, Clone)]
pub struct Segment {
    /// Byte offset from the start of the file.
    pub offset: SeekFrom,
    /// Length in bytes.
    pub length: u64,
}

pub(crate) type Stream<'a> = StreamReader<BufReader<Take<&'a mut File>>>;

impl Segment {
    /// Create a zero-copy [`StreamReader`] window into `file` for this segment.
    pub(crate) fn stream<'a>(&self, file: &'a mut File) -> Result<Stream<'a>, Error> {
        file.seek(self.offset)?;
        let view = file.take(self.length); // zero-copy window into the file
        Ok(StreamReader::try_new_buffered(view, None)?) // buffer reduces syscall overhead
    }

    /// `(start, len)` as `usize` for slicing memory-mapped buffers.
    pub(crate) fn byte_range(&self) -> (usize, usize) {
        let SeekFrom::Start(off) = self.offset else { panic!("Offset is not SeekFrom::Start") };
        (off as usize, self.length as usize)
    }

    /// Byte offset past the last byte of this segment.
    pub fn end(&self) -> u64 {
        let SeekFrom::Start(off) = self.offset else { panic!("Offset is not SeekFrom::Start") };
        off + self.length
    }
}

impl Serialize for Segment {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let SeekFrom::Start(offset) = self.offset else {
            use serde::ser::Error;
            return Err(Error::custom("Segment offset is not SeekFrom::Start"));
        };
        (offset, self.length).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Segment {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (offset, length) = <(u64, u64)>::deserialize(deserializer)?;
        Ok(Self {
            offset: SeekFrom::Start(offset),
            length,
        })
    }
}

/// Experiment-level metadata stored in every `.wr` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Absolute dataset initialisation timestamp in microseconds relative to the UNIX epoch.
    pub timestamp: u64,
    /// Calibration measurement IDs.
    pub calibrations: Vec<u32>,
    /// Dataset configuration.
    pub cfg: Config,
    /// File segments containing intensity data
    pub intensities: Vec<Segment>,
    /// File segments containing measurements data.
    pub measurements: Vec<Segment>,
    /// File segments containing wavelength data.
    pub wavelengths: Vec<Segment>,
}

impl Manifest {
    /// Create a new [`Manifest`] for the given creation timestamp and [`Config`].
    pub(crate) fn new(timestamp: u64, cfg: &Config) -> Self {
        Self {
            timestamp,
            calibrations: Vec::with_capacity(8),
            cfg: cfg.clone(),
            intensities: Vec::new(),
            measurements: Vec::new(),
            wavelengths: Vec::new(),
        }
    }
}

/* -------------------------------------------------------------------------------------- Header */

/// The fixed-size header at the start of every `.wr` file.
///
/// Layout: `MAGIC(4) + manifest_offset(8) + manifest_len(8) + VERSION(1) + file_type(1)`.
pub(crate) struct Header {
    /// Location of the TOML manifest within the file.
    pub manifest: Segment,
    /// File type. See [`Format`].
    pub format: Format,
}

impl Header {
    /// Write the header to `w`.
    pub fn write<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        let SeekFrom::Start(offset) = self.manifest.offset else {
            return Err(Error::InvalidFormat("manifest position must be absolute from file start"));
        };
        w.write_all(MAGIC)?;
        w.write_all(&offset.to_le_bytes())?;
        w.write_all(&self.manifest.length.to_le_bytes())?;
        w.write_all(&[VERSION])?;
        w.write_all(&[u8::from(self.format)])?;
        Ok(())
    }

    /// Read and validate the header from `r`.
    pub fn read<R: Read>(r: &mut R) -> Result<Self, Error> {
        let mut buf = [0u8; HEADER];
        r.read_exact(&mut buf)?;
        if &buf[0..4] != MAGIC {
            return Err(Error::InvalidFormat("invalid magic bytes"));
        }
        let manifest_offset = u64::from_le_bytes(buf[4..12].try_into().expect("8 bytes"));
        let manifest_len = u64::from_le_bytes(buf[12..20].try_into().expect("8 bytes"));
        let version = buf[20];
        if version != VERSION {
            return Err(Error::InvalidFormat("unsupported version"));
        }
        let format = Format::try_from(buf[21])?;
        Ok(Self {
            manifest: Segment {
                offset: SeekFrom::Start(manifest_offset),
                length: manifest_len,
            },
            format,
        })
    }
}
