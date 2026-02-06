/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::Arc;

use arrow::array::{Array, Float64Builder, UInt32Builder};

/* ------------------------------------------------------------------------------ Public Exports */

pub(super) struct Accumulator {
    len: u32,
    data: Vec<Float64Builder>,
    id: UInt32Builder,
    measurement: UInt32Builder,
}

impl Accumulator {
    pub(super) fn new(n_wavelengths: usize) -> Self {
        Self {
            len: 0,
            data: (0..n_wavelengths).map(|i| Float64Builder::new()).collect(),
            id: Default::default(),
            measurement: Default::default(),
        }
    }

    pub fn push(&mut self, measurement: u32, data: Vec<f64>) {
        self.id.append_value(self.len);
        self.measurement.append_value(measurement);
        self.data.iter_mut().zip(data).for_each(|(builder, value)| {
            builder.append_value(value);
        });
        self.len += 1;
    }

    pub(super) fn columns(&mut self) -> Vec<Arc<dyn Array>> {
        self.data
            .iter_mut()
            .map(|builder| Arc::new(builder.finish()) as Arc<dyn Array>)
            .chain([
                Arc::new(self.id.finish()) as Arc<dyn Array>,
                Arc::new(self.measurement.finish()) as Arc<dyn Array>,
            ])
            .collect()
    }
}
