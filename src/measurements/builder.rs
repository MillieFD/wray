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

use crate::table::Build;

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
    /// Number of pending rows.
    len: usize,
}

impl Builder {
    /// Append one measurement row.
    #[allow(clippy::too_many_arguments, reason = "Schema requires all fields")]
    pub fn push(
        &mut self,
        id: u32,
        timestamp: u64,
        x: Option<f32>,
        y: Option<f32>,
        z: Option<f32>,
        a: Option<f32>,
        b: Option<f32>,
        c: Option<f32>,
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
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Build for Builder {
    fn len(&self) -> usize {
        self.len
    }

    /// ~256 KB batches at 40 bytes/row ≈ 6500 rows.
    fn is_full(&self) -> bool {
        const MAX: usize = 6500;
        self.len >= MAX
    }

    fn columns(&mut self) -> Vec<ArrayRef> {
        self.len = 0;
        vec![
            Arc::new(self.id.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.x.finish()),
            Arc::new(self.y.finish()),
            Arc::new(self.z.finish()),
            Arc::new(self.a.finish()),
            Arc::new(self.b.finish()),
            Arc::new(self.c.finish()),
            Arc::new(self.integration.finish()),
        ]
    }
}
