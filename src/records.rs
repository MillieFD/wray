/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ------------------------------------------------------------------------------ Public Exports */

/// A single wavelength entry returned by read queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WavelengthRecord {
    /// Unique wavelength identifier.
    pub id: u16,
    /// Wavelength in nanometres.
    pub nm: f64,
}

/// A single measurement entry returned by read queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MeasurementRecord {
    /// Unique measurement identifier.
    pub id: u32,
    /// Microsecond offset from the manifest epoch.
    pub timestamp: u64,
    /// X coordinate in the configured storage unit (or `None`).
    pub x: Option<f32>,
    /// Y coordinate in the configured storage unit (or `None`).
    pub y: Option<f32>,
    /// Z coordinate in the configured storage unit (or `None`).
    pub z: Option<f32>,
    /// Angle coordinate in the configured storage unit (or `None`).
    pub a: Option<f32>,
    /// Integration time in microseconds.
    pub integration: u64,
}

/// A single intensity entry returned by read queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IntensityRecord {
    /// Foreign key to the measurement table.
    pub measurement: u32,
    /// Foreign key to the wavelength table.
    pub wavelength: u16,
    /// Spectral intensity value.
    pub intensity: f64,
}
