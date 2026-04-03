/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use serde::{Deserialize, Serialize};

use crate::Config;
use crate::format::segment::Segment;

/* ------------------------------------------------------------------------------ Public Exports */

/// Experiment-level metadata stored in every `.wr` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Dataset initialisation timestamp in microseconds after the UNIX epoch.
    pub timestamp: u64,
    /// Calibration measurement IDs.
    pub calibrations: Vec<u32>,
    /// Dataset configuration.
    pub cfg: Config,
    /// File segments containing intensity data
    pub intensities: Vec<Segment>,
    /// File segments containing measurement data.
    pub measurements: Vec<Segment>,
    /// File segments containing wavelength data.
    pub wavelengths: Vec<Segment>,
}

impl Manifest {
    /// Create a new [`crate::Manifest`] for the given creation timestamp and [`Config`].
    pub(in crate::format) fn new(timestamp: u64, cfg: Config) -> Self {
        Self {
            timestamp,
            calibrations: Vec::new(),
            cfg,
            intensities: Vec::new(),
            measurements: Vec::new(),
            wavelengths: Vec::new(),
        }
    }
}
