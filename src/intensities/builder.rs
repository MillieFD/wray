/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, UInt16Builder, UInt32Builder};

use crate::writer::Build;

/* ------------------------------------------------------------------------------ Public Exports */

/// Arrow record-batch builder for the intensities table.
#[derive(Debug, Default)]
pub(super) struct Builder {
    /// Measurement foreign key.
    measurement: UInt32Builder,
    /// Wavelength foreign key.
    wavelength: UInt16Builder,
    /// Spectral intensity value.
    intensity: Float64Builder,
    /// Number of pending rows.
    len: usize,
}

impl Builder {
    /// Expand one measurement's wavelength/intensity vectors into rows.
    pub fn push(&mut self, measurement: u32, wavelengths: &[u16], intensities: &[f64]) {
        wavelengths
            .iter()
            .copied()
            .zip(intensities.iter().copied())
            .for_each(|(λ, i)| {
                self.measurement.append_value(measurement);
                self.wavelength.append_value(λ);
                self.intensity.append_value(i);
                self.len += 1;
            });
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Build for Builder {
    fn len(&self) -> usize {
        self.len
    }

    fn is_full(&self) -> bool {
        const MAX: usize = 32_768;
        self.len >= MAX
    }

    fn columns(&mut self) -> Vec<ArrayRef> {
        self.len = 0;
        vec![
            Arc::new(self.measurement.finish()),
            Arc::new(self.wavelength.finish()),
            Arc::new(self.intensity.finish()),
        ]
    }
}
