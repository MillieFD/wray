/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

use std::fs;
use std::io::Error;
use std::path::{Path, PathBuf};

use DataType::*;
use polars::prelude::*;

use super::Table;

pub(super) struct Spectrometers {
    path: PathBuf,
}

impl Spectrometers {
    pub(super) fn new(path: &Path) -> Result<Spectrometers, Error> {
        let path = path.join("spectrometers.ipc");
        let mut file = fs::File::create(&path)?;
        let mut df = Self::empty();
        IpcStreamWriter::new(&mut file).finish(&mut df).unwrap();
        Ok(Spectrometers { path })
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
