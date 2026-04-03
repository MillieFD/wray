/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{Segment, Manifest};
use crate::{Error, Format};

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
pub(crate) const HEADER: usize = size_of::<[u8; 4]>() // Magic bytes
    + size_of::<u8>()   // Format version
    + size_of::<u8>()   // File type
    + size_of::<u64>()  // Manifest offset
    + size_of::<u64>(); // Manifest length

/* ----------------------------------------------------------------------------- Public Exports */

/// Fixed-size header at the start of every `.wr` file.
pub(crate) struct Header {
    /// Path to the dataset file.
    pub path: PathBuf,
    /// Location of the TOML manifest.
    pub manifest: Segment,
    /// File type. See [`Format`].
    // TODO Remove Format enum and unfinished::Dataset struct in favour of universal Arrow feather
    pub format: Format,
}

impl Header {
    /// Open `path` and read and validate the file header.
    pub fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let mut file = File::open(&path)?;
        let mut buf = [0u8; HEADER];
        file.read_exact(&mut buf)?;
        let version = Self::version(&buf)?;
        let offset = buf[6..14].try_into().map(u64::from_le_bytes)?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            manifest: Segment {
                offset: SeekFrom::Start(offset),
                length: buf[14..22].try_into().map(u64::from_le_bytes)?,
            },
            format: Format::try_from(buf[5])?,
        })
    }

    /// Read and parse the manifest TOML from this header's file.
    pub fn manifest(&self) -> Result<Manifest, Error> {
        let mut file = File::open(&self.path)?;
        let mut buf = vec![0u8; self.manifest.length as usize];
        file.seek(self.manifest.offset)?;
        file.read_exact(&mut buf)?;
        toml::from_slice(&buf).map_err(Error::from)
    }

    /// Write the file header to `w`.
    pub fn write<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        let offset = match self.manifest.offset {
            SeekFrom::Start(off) => off.to_le_bytes(),
            other => return Err(Error::new("Offset must be SeekFrom::Start")),
        };
        w.write_all(MAGIC)?;
        w.write_all(&offset)?;
        w.write_all(&self.manifest.length.to_le_bytes())?;
        w.write_all(&[VERSION])?;
        w.write_all(&[self.format.into()])?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic() {
        // Magic bytes identify the WRAY format and must remain stable across versions.
        assert_eq!(MAGIC, &[87, 82, 65, 89])
    }
}
