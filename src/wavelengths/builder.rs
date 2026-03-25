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

use crate::writer::Build;

/* ------------------------------------------------------------------------------ Public Exports */

/// Arrow record-batch builder for the wavelengths table.
pub(super) struct Builder {
    /// Wavelength identifiers.
    ids: UInt16Builder,
    /// Wavelength values in nanometres.
    nms: Float32Builder,
    /// Number of pending rows.
    len: usize,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            ids: Default::default(),
            nms: Default::default(),
            len: 0,
        }
    }

    /// Append a single wavelength row.
    pub fn push(&mut self, id: u16, nm: f32) {
        self.ids.append_value(id);
        self.nms.append_value(nm);
        self.len += 1;
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Build for Builder {
    fn len(&self) -> usize {
        self.len
    }

    fn is_full(&self) -> bool {
        const MAX: usize = 256 * 1024 / (size_of::<u16>() + size_of::<f32>());
        self.len >= MAX
    }

    fn columns(&mut self) -> Vec<ArrayRef> {
        self.len = 0;
        vec![Arc::new(self.ids.finish()), Arc::new(self.nms.finish())]
    }
}
