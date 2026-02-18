/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::{Arc, LazyLock};

use arrow::datatypes::Schema;

/* ------------------------------------------------------------------------------- Pubic Exports */

pub(super) trait Writer {
    const SCHEMA: LazyLock<Arc<Schema>>;
    fn schema() -> Arc<Schema> {
        Self::SCHEMA.clone() // Inexpensive Arc Clone
    }
}
