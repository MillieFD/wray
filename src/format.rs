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
use std::path::{Path, PathBuf};

use arrow::ipc::reader::StreamReader;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/* ----------------------------------------------------------------------------------- Constants */

/// Magic bytes to identify the start of every `.wr` file.
pub(super) const MAGIC: &[u8; 4] = b"WRAY";

/// Format version number.
pub(super) const VERSION: u8 = 1;

/// Length of the fixed-size file `header` in bytes.
///
/// ```
/// Field             Type    Size    Offset   Contents
/// ────────────────  ──────  ──────  ───────  ─────────────────────────────────────
/// Magic Bytes       UTF-8   4       0        Identifies the format with b"WRAY"
/// Format Version    u8      1       4        Specifies the schema version
/// File Type         u8      1       5        Describes the file variant
/// Manifest Offset   u64 LE  8       6        Byte offset of the TOML manifest
/// Manifest Length   u64 LE  8       14       Length of the TOML manifest in bytes
/// Data Segments     Arrow   …       22       One or more Apache Arrow IPC segments
/// Manifest          UTF-8   M       EOF      File metadata TOML key-value pairs
/// ```
///
/// The header is exactly **22 bytes**. The manifest is located at `manifest_offset`
/// from the start of the file and is `manifest_length` bytes long. All multibyte
/// integers are **little-endian**.
pub(super) const HEADER: usize = size_of::<[u8; 4]>() // Magic bytes
    + size_of::<u8>()   // Format version
    + size_of::<u8>()   // File type
    + size_of::<u64>()  // Manifest offset
    + size_of::<u64>(); // Manifest length

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

/// File type stored as a single byte (`u8`) in the binary header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Format {
    /// Arrow IPC stream format optimised for appending new data.
    Unfinished,
    /// Arrow IPC file format optimised for random-access reads.
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
    /// Byte offset to the start of the segment.
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
    /// Path to the `.wr` file on disk. Not serialised to TOML.
    #[serde(skip)]
    pub path: PathBuf,
}

impl Manifest {
    /// Create a new [`Manifest`] for the given file `path`, creation timestamp, and [`Config`].
    pub(crate) fn new(path: impl AsRef<Path>, timestamp: u64, cfg: &Config) -> Self {
        Self {
            timestamp,
            calibrations: Vec::with_capacity(8),
            cfg: cfg.clone(),
            intensities: Vec::new(),
            measurements: Vec::new(),
            wavelengths: Vec::new(),
            path: path.as_ref().to_path_buf(),
        }
    }
}

/* -------------------------------------------------------------------------------------- Header */

/// Fixed-size header at the start of every `.wr` file.
pub(crate) struct Header {
    /// Path to the dataset file.
    pub path: PathBuf,
    /// Location of the TOML manifest.
    pub manifest: Segment,
    /// File type. See [`Format`].
    pub format: Format,
}

impl Header {
    /// Open `path` and read and validate the file header.
    pub fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let mut file = File::open(&path)?;
        let mut header = Self::read(&mut file)?;
        header.path = path.as_ref().to_path_buf();
        Ok(header)
    }

    /// Read and parse the manifest TOML from this header's file.
    pub fn manifest(&self) -> Result<Manifest, Error> {
        let mut file = File::open(&self.path)?;
        file.seek(self.manifest.offset)?;
        let mut buf = vec![0u8; self.manifest.length as usize];
        file.read_exact(&mut buf)?;
        let mut manifest: Manifest = toml::from_str(std::str::from_utf8(&buf)?)?;
        manifest.path = self.path.clone();
        Ok(manifest)
    }

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
    fn read<R: Read>(r: &mut R) -> Result<Self, Error> {
        let mut buf = [0u8; HEADER];
        r.read_exact(&mut buf)?;
        let version = Self::version(&buf)?;
        let offset = buf[6..14].try_into().map(u64::from_le_bytes)?;
        Ok(Self {
            path: PathBuf::new(),
            manifest: Segment {
                offset: SeekFrom::Start(offset),
                length: buf[14..22].try_into().map(u64::from_le_bytes)?,
            },
            format: Format::try_from(buf[5])?,
        })
    }

    /// Extract the `WRAY` version number from the provided header bytes. Uses [`MAGIC`] bytes
    ///
    /// # Returns
    /// - [`Ok(u8)`](Ok) if the header is valid.
    /// - [`Err(Error)`](Err) if the bytes are not a valid `WRAY` header.
    ///
    /// # Panics
    /// - If the `header` slice contains fewer than five elements.
    fn version(header: &[u8]) -> Result<u8, Error> {
        match &header[..4] {
            bytes if bytes == MAGIC => Ok(header[4]),
            bytes => Err(format!("Invalid magic bytes: {bytes:?}").into()),
        }
    }
}
