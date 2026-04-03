/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

mod header;
mod manifest;
mod segment;

/* ----------------------------------------------------------------------------- Private Imports */

use serde::{Deserialize, Serialize};

use self::manifest::Manifest;
use self::segment::Segment;
use crate::Error;

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
