/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fmt::{Display, Formatter};

/* ------------------------------------------------------------------------------ Public Exports */

/// A single wavelength entry.
///
/// Used both internally for deduplication and as the public read-query result.
#[derive(Copy, Clone, Debug, Default, PartialEq, PartialOrd)]
pub struct Record {
    /// Unique wavelength identifier.
    pub id: u16,
    /// Wavelength in nanometres.
    pub nm: f32,
}

impl Record {
    pub(super) const fn new(id: u16, nm: f32) -> Self {
        Self { id, nm }
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Display for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} nm", self.nm)
    }
}

impl From<(u16, f32)> for Record {
    fn from((id, nm): (u16, f32)) -> Self {
        Self::new(id, nm)
    }
}
