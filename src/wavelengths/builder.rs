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

    /// Finish the current arrays and return them as columns. Resets the builder.
    pub fn columns(&mut self) -> Vec<ArrayRef> {
        self.len = 0;
        vec![Arc::new(self.ids.finish()), Arc::new(self.nms.finish())]
    }
}
