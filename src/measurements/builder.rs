/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Builder, UInt32Builder, UInt64Builder};

/* ------------------------------------------------------------------------------ Public Exports */

/// Arrow record-batch builder for the measurements table.
#[derive(Debug, Default)]
pub(super) struct Builder {
    id: UInt32Builder,
    timestamp: UInt64Builder,
    x: Float32Builder,
    y: Float32Builder,
    z: Float32Builder,
    a: Float32Builder,
    integration: UInt64Builder,
    b: Float32Builder,
    c: Float32Builder,
    integration: UInt32Builder,
    len: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one measurement row. Coordinates are raw `f32` values already
    /// converted to the correct storage unit by the caller.
    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        id: u32,
        timestamp: u64,
        x: Option<f32>,
        y: Option<f32>,
        z: Option<f32>,
        a: Option<f32>,
        integration: u64,
    ) {
        self.id.append_value(id);
        self.timestamp.append_value(timestamp);
        self.x.append_option(x);
        self.y.append_option(y);
        self.z.append_option(z);
        self.a.append_option(a);
        self.integration.append_value(integration);
        self.len += 1;
    }

    /// Number of rows pending since the last [`columns`] call.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Finish the current arrays and return them as columns. Resets the builder.
    pub fn columns(&mut self) -> Vec<ArrayRef> {
        self.len = 0;
        vec![
            Arc::new(self.id.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.x.finish()),
            Arc::new(self.y.finish()),
            Arc::new(self.z.finish()),
            Arc::new(self.a.finish()),
            Arc::new(self.integration.finish()),
        ]
    }
}
