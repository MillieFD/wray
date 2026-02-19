/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Modules */

mod builder;

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType::{Duration, Float64, Timestamp, UInt32};
use arrow::datatypes::TimeUnit::Microsecond;
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;
use uom::si::f64::{Length, Time};

use self::builder::*;
use crate::{Error, Writer};

/* ------------------------------------------------------------------------------ Public Exports */

pub struct Measurements {
    stream: StreamWriter<File>,
    builder: Builder,
}

impl Measurements {
    pub(super) fn new<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        path.as_ref()
            .join("measurements")
            .with_extension("arrow")
            .try_into()
    }

    pub fn push(
        &mut self,
        #[cfg(feature = "x")] x: Length,
        #[cfg(feature = "y")] y: Length,
        #[cfg(feature = "z")] z: Length,
        #[cfg(feature = "a")] a: Length,
        i: Time,
    ) -> u32 {
        self.builder.push(
            #[cfg(feature = "x")]
            x,
            #[cfg(feature = "y")]
            y,
            #[cfg(feature = "z")]
            z,
            #[cfg(feature = "a")]
            a,
            i,
        )
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        let columns = self.builder.columns();
        let batch = RecordBatch::try_new(Self::schema(), columns)?;
        self.stream.write(&batch).map_err(Error::from)
    }
}

/* ----------------------------------------------------------------------- Trait Implementations */

impl Writer for Measurements {
    const SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        let fields = [
            Field::new("id", UInt32, false).into(),
            Field::new("timestamp", Timestamp(Microsecond, None), false).into(),
            #[cfg(feature = "x")]
            Field::new("x", Float64, false).into(),
            #[cfg(feature = "y")]
            Field::new("y", Float64, false).into(),
            #[cfg(feature = "z")]
            Field::new("z", Float64, false).into(),
            #[cfg(feature = "a")]
            Field::new("a", Float64, false).into(),
            Field::new("integration", Duration(Microsecond), false).into(),
        ];
        Schema::new(fields).into()
    });
}

impl TryFrom<PathBuf> for Measurements {
    type Error = Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let stream = StreamWriter::try_new(file, &Self::SCHEMA)?;
        let builder = Builder::new(&path);
        let db = Self { stream, builder };
        Ok(db)
    }
}
