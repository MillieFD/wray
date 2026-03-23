/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ------------------------------------------------------------------------------ Public Exports */

use std::fmt::{Display, Formatter};

/// A single intensity entry returned by read queries.
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct Record {
    /// Measurement ID. Foreign key to the measurements table.
    pub measurement: u32,
    /// Wavelength ID. Foreign key to the wavelengths table.
    pub wavelength: u16,
    /// Measured spectral intensity.
    pub intensity: f64,
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Display for Record {
    /// Formats the value using the given formatter.
    ///
    /// # Display or Debug?
    ///
    /// Use [`Display`] to show the intensity value only e.g. when printing results to `stout`.
    /// Use [`Debug`] to show the full record including measurement and wavelength IDs.
    /// See trait-level documentation for more information.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.intensity.fmt(f)
    }
}
