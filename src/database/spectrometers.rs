/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use super::Table;
use DataType::*;
use polars::prelude::*;
use std::fs;
use std::io::Error;
use std::path::Path;

pub(super) struct Spectrometers;

impl Spectrometers {
    pub(super) fn new(path: &Path) -> Result<Spectrometers, Error> {
        let mut file = fs::File::create(&path.join("spectrometers.parquet"))?;
        let mut df = Self::empty();
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)
            .unwrap();
        Ok(Spectrometers {})
    }
}

impl Table for Spectrometers {
    fn schema() -> Schema {
        Schema::from_iter([
            Field::new("id".into(), UInt8),
            Field::new("wavelengths".into(), List(Box::new(Float64))),
        ])
    }
}
