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
    /// Unique measurement identifier.
    id: UInt32Builder,
    /// Microsecond offset from the manifest epoch.
    timestamp: UInt64Builder,
    /// Optional X coordinate in SI base units.
    x: Float32Builder,
    /// Optional Y coordinate in SI base units.
    y: Float32Builder,
    /// Optional Z coordinate in SI base units.
    z: Float32Builder,
    /// Optional A coordinate in SI base units.
    a: Float32Builder,
    /// Optional B coordinate in SI base units.
    b: Float32Builder,
    /// Optional C coordinate in SI base units.
    c: Float32Builder,
    /// Integration time in microseconds.
    integration: UInt32Builder,
    /// Number of measurements in the batch.
    len: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one measurement row.
    ///
    /// All optional coordinate fields are feature-gated. Unneeded fields can be disabled in
    /// `cargo.toml` for improved ergonomics. This does not change the underlying `schema`.
    #[allow(clippy::too_many_arguments, reason = "User may require all fields")]
    pub fn push(
        &mut self,
        id: u32,
        timestamp: u64,
        #[cfg(feature = "x")] x: Option<f32>,
        #[cfg(feature = "y")] y: Option<f32>,
        #[cfg(feature = "z")] z: Option<f32>,
        #[cfg(feature = "a")] a: Option<f32>,
        #[cfg(feature = "b")] b: Option<f32>,
        #[cfg(feature = "c")] c: Option<f32>,
        integration: u32,
    ) {
        self.id.append_value(id);
        self.timestamp.append_value(timestamp);
        self.x.append_option(x);
        self.y.append_option(y);
        self.z.append_option(z);
        self.a.append_option(a);
        self.b.append_option(b);
        self.c.append_option(c);
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
