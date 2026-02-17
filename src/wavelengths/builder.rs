/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, UInt32Builder};
use uom::si::f64::Length;
use uom::si::length::nanometer;

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct Builder {
    id: UInt32Builder,
    nm: Float64Builder,
}

impl Builder {
    pub(super) fn new() -> Self {
        Self {
            id: Default::default(),
            nm: Default::default(),
        }
    }

    pub(super) fn append(&mut self, id: u32, wavelength: Length) {
        self.id.append_value(id);
        self.nm.append_value(wavelength.get::<nanometer>());
    }

    pub(super) fn columns(&mut self) -> Vec<ArrayRef> {
        vec![Arc::new(self.id.finish()), Arc::new(self.nm.finish())]
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */
