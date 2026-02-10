/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::cmp::Ordering;

use uom::si::f64::Length;

/* ------------------------------------------------------------------------------ Public Exports */

#[derive(Copy, Clone, Debug, Default)]
pub(super) struct Record {
    pub id: u32,
    pub nm: Length,
}

impl Record {
    pub fn new(id: u32, nm: Length) -> Self {
        Self { id, nm }
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl From<(u32, Length)> for Record {
    fn from((id, wl): (u32, Length)) -> Self {
        Self::new(id, wl)
    }
}

impl Eq for Record {}

impl PartialEq<Self> for Record {
    fn eq(&self, other: &Self) -> bool {
        self.nm == other.nm
    }
}

impl PartialOrd<Self> for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.nm.partial_cmp(&other.nm)
    }
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.nm.partial_cmp(&other.nm).unwrap_or(Ordering::Equal)
    }
}
