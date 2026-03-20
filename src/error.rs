/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fmt::{Debug, Display, Formatter};

use arrow::error::ArrowError;

/* ------------------------------------------------------------------------------ Public Exports */

/// Errors produced by the `wray` library.
#[derive(Debug)]
pub enum Error {
    /// Apache Arrow operation failed.
    Arrow(ArrowError),
    /// File-system I/O failed.
    Io(std::io::Error),
    /// TOML serialisation or deserialisation failed.
    Toml(String),
    /// A required Arrow column was not found.
    MissingColumn(String),
    /// The `.wray` binary layout is invalid or unsupported.
    InvalidFormat(String),
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Arrow(e) => write!(f, "Arrow: {e}"),
            Self::Io(e) => write!(f, "IO: {e}"),
            Self::Toml(e) => write!(f, "TOML: {e}"),
            Self::MissingColumn(c) => write!(f, "missing column: {c}"),
            Self::InvalidFormat(msg) => write!(f, "invalid format: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Self::Arrow(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<toml::ser::Error> for Error {
    fn from(e: toml::ser::Error) -> Self {
        Self::Toml(e.to_string())
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Self {
        Self::Toml(e.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::InvalidFormat(e.to_string())
    }
}
