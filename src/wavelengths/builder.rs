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

/* ------------------------------------------------------------------------------ Public Exports */

/// Arrow record-batch builder for the wavelengths table.
pub(super) struct Builder {
    ids: UInt16Builder,
    nms: Float32Builder,
    pub(super) len: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            ids: Default::default(),
            nms: Default::default(),
            len: Default::default(),
        }
    }

    /// Append a single wavelength row.
    pub fn push(&mut self, id: u16, nm: f32) {
        self.ids.append_value(id);
        self.nms.append_value(nm);
        self.len += 1;
    }

    /// Returns true if the builder is ready to be flushed.
    ///
    /// # Apache Arrow Chunking
    ///
    /// Each record batch can contain an arbitrary number of rows. When implementing the arrow
    /// columnar format, users must decide how many rows to include in each record batch. Smaller
    /// batches transmit with lower latency but incur more per-message overhead. Larger batches
    /// amortise header costs and improve throughput but increase memory footprint and latency.
    /// Arrow performance studies suggest batches in the `256 KB` to `1 MB` range. Consider
    /// adjusting batch size to fit CPU caches (L2/L3) and balance streaming latency.
    ///
    /// | **Batch Size** | **Throughput (GB/s)** |
    /// | -------------: | --------------------: |
    /// | 16 KB | ~1.0 |
    /// | 64 KB | ~2.0 |
    /// | 256 KB | ~5.0 |
    /// | 1 MB | ~7.7 |
    /// | 16 MB | ~6.8 |
    pub fn is_full(&self) -> bool {
        const SIZE: usize = 256 * 1024 / size_of::<u16>() + size_of::<f32>();
        self.len >= SIZE
    }

    /// Finish the current arrays and return them as columns. Resets the builder.
    pub fn columns(&mut self) -> Vec<ArrayRef> {
        self.len = 0;
        vec![Arc::new(self.ids.finish()), Arc::new(self.nms.finish())]
    }
}
