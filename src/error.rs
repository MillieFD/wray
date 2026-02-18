/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fmt::{Debug, Display, Formatter};

use arrow::error::ArrowError;

/* ------------------------------------------------------------------------------ Public Exports */

#[derive(Debug)]
pub enum Error {
    ArrowError(ArrowError),
    IOError(std::io::Error),
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Error::ArrowError(e) => write!(f, "Arrow Error: {}", e),
            Error::IOError(e) => write!(f, "IO Error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Error::ArrowError(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IOError(value)
    }
}
