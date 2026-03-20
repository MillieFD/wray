/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::cmp::Ordering;

use uom::si::f32::Length;

/* ------------------------------------------------------------------------------ Public Exports */

/// In-memory representation of a single wavelength entry.
#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Record {
    pub id: u16,
    pub nm: Length,
}

impl Record {
    pub(crate) fn new(id: u16, nm: Length) -> Self {
        Self { id, nm }
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl From<(u16, Length)> for Record {
    fn from((id, nm): (u16, Length)) -> Self {
        Self::new(id, nm)
    }
}

impl Eq for Record {}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.nm == other.nm
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.nm.partial_cmp(&other.nm).unwrap_or(Ordering::Equal)
    }
}
