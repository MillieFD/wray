/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fmt::{Display, Formatter};

/* ------------------------------------------------------------------------------ Public Exports */

/// A single measurement entry returned by read queries.
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct Record {
    /// Unique measurement identifier.
    pub id: u32,
    /// Microsecond offset from the manifest epoch.
    pub timestamp: u64,
    /// X coordinate in SI base units, or `None` if unused.
    pub x: Option<f32>,
    /// Y coordinate in SI base units, or `None` if unused.
    pub y: Option<f32>,
    /// Z coordinate in SI base units, or `None` if unused.
    pub z: Option<f32>,
    /// A coordinate in SI base units, or `None` if unused.
    pub a: Option<f32>,
    /// B coordinate in SI base units, or `None` if unused.
    pub b: Option<f32>,
    /// C coordinate in SI base units, or `None` if unused.
    pub c: Option<f32>,
    /// Integration time in microseconds.
    pub integration: u32,
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Display for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record {{ id: {}, timestamp: {} }}", self.id, self.timestamp)
    }
}
