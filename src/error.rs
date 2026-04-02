/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::array::TryFromSliceError;
use std::fmt::{Debug, Display, Formatter};
use std::str::Utf8Error;

use arrow::error::ArrowError;
use serde::{Deserialize, Serialize};

/* ------------------------------------------------------------------------------ Public Exports */

/// Errors that can occur when using the `wray` format.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Error {
    msg: String,
}

impl Error {
    pub(super) fn new<S: Display>(msg: S) -> Self {
        Self {
            msg: msg.to_string(),
        }
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Wray Error : {}", self.msg)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Self::new(msg)
    }
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Self::new(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new(e)
    }
}

impl From<toml::ser::Error> for Error {
    fn from(e: toml::ser::Error) -> Self {
        Self::new(e)
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Self {
        Self::new(e)
    }
}

impl From<Utf8Error> for Error {
    fn from(e: Utf8Error) -> Self {
        Self::new(e)
    }
}

impl From<TryFromSliceError> for Error {
    fn from(e: TryFromSliceError) -> Self {
        Self::new(e)
    }
}
