/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Builder, UInt16Builder};
use uom::si::f32::Length;
use uom::si::length::nanometer;

/* ------------------------------------------------------------------------------ Public Exports */

/// Arrow record-batch builder for the wavelengths table.
pub(super) struct Builder {
    id: UInt16Builder,
    nm: Float32Builder,
    len: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            id: Default::default(),
            nm: Default::default(),
            len: 0,
        }
    }

    /// Append a single wavelength row.
    pub fn push(&mut self, id: u16, wavelength: Length) {
        self.id.append_value(id);
        self.nm.append_value(wavelength.get::<nanometer>());
        self.len += 1;
    }

    /// Number of rows pending in this builder since the last [`columns`] call.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Finish the current arrays and return them as columns. Resets the builder.
    pub fn columns(&mut self) -> Vec<ArrayRef> {
        self.len = 0;
        vec![Arc::new(self.id.finish()), Arc::new(self.nm.finish())]
    }
}
