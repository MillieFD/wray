/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use polars::prelude::{DataFrame, Schema};

pub trait Table {
    fn schema() -> Schema;
    fn empty() -> DataFrame {
        let schema = Self::schema();
        DataFrame::empty_with_schema(&schema)
    }
}
