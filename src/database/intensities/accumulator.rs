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

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct Accumulator {
    measurement: UInt32Builder,
    wavelength: UInt32Builder,
    intensity: Float64Builder,
}

impl Accumulator {
    pub(super) fn new() -> Self {
        Self {
            measurement: Default::default(),
            wavelength: Default::default(),
            intensity: Default::default(),
        }
    }

    pub fn push(&mut self, measurement: u32, wavelengths: Vec<u32>, intensities: Vec<f64>) {
        self.measurement.append_value(measurement);
        wavelengths.into_iter().zip(intensities).for_each(|(λ, i)| {
            self.wavelength.append_value(λ);
            self.intensity.append_value(i);
        })
    }

    pub(super) fn columns(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.measurement.finish()),
            Arc::new(self.wavelength.finish()),
            Arc::new(self.intensity.finish()),
        ]
    }
}
