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

pub(super) struct Builder {
    measurement: UInt32Builder,
    wavelength: UInt32Builder,
    intensity: Float64Builder,
}

impl Builder {
    pub(super) fn new() -> Self {
        Self {
            measurement: Default::default(),
            wavelength: Default::default(),
            intensity: Default::default(),
        }
    }

    pub(super) fn push(&mut self, measurement: u32, wavelengths: &Vec<u32>, intensities: Vec<f64>) {
        wavelengths
            .into_iter()
            .map(u32::clone)
            .zip(intensities)
            .for_each(|(λ, i)| {
                self.measurement.append_value(measurement);
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
