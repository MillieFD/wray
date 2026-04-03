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

use crate::table::Build;

/* ------------------------------------------------------------------------------ Public Exports */

/// Arrow [`RecordBatch`][1] builder for the [`Intensities`][2] table.
///
/// [1]: arrow::array::RecordBatch
/// [2]: super::Intensities
#[derive(Debug, Default)]
pub(super) struct Builder {
    /// Measurement IDs. Foreign keys to [`measurements`](crate::measurements::Measurements).
    measurement: UInt32Builder,
    /// Wavelength IDs. Foreign keys to [`wavelengths`](crate::wavelengths::Wavelengths).
    wavelength: UInt16Builder,
    /// Measured spectral intensities.
    intensity: Float64Builder,
    /// Number of pending rows.
    len: usize,
}

impl Builder {
    /// Append a [`measurement`](super::Record) to the [`Builder`].
    pub fn push(&mut self, measurement: u32, wavelengths: &[u16], intensities: &[f64]) {
        wavelengths.iter().zip(intensities).for_each(|(λ, i)| {
            self.measurement.append_value(measurement);
            self.wavelength.append_value(*λ);
            self.intensity.append_value(*i);
        });
        self.len += intensities.len();
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Build for Builder {
    fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the estimated size of [`Self`] exceeds **128 MB**.
    ///
    /// ### Arrow Specification
    ///
    /// Each [`RecordBatch`][1] can contain an arbitrary number of rows. When implementing the
    /// [`arrow`] columnar format, users must decide how many rows to include in each batch.
    ///
    /// - Smaller batches transmit with lower latency but incur greater per-message overhead.
    /// - Larger batches amortise header costs and improve throughput, but increase memory footprint
    ///   and latency.
    ///
    /// Performance studies suggest batches in the `256 KB` to `1 MB` range. Consider adjusting
    /// batch size to fit CPU caches (L2/L3) and balance streaming latency.
    ///
    /// [1]: arrow::array::RecordBatch
    fn is_full(&self) -> bool {
        self.len >= 128 * 1024 * 1024 / size_of::<super::Record>()
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
