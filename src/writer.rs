/*
Project: Optic
GitHub: https://github.com/MillieFD/optic

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::File;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::CompressionType;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

/* ------------------------------------------------------------------------------- Pubic Exports */

pub(super) trait Writer {
    const SCHEMA: LazyLock<Arc<Schema>>;

    fn schema() -> Arc<Schema> {
        Self::SCHEMA.clone() // Inexpensive Arc Clone
    }

    fn ipc_write_options() -> IpcWriteOptions {
        let compression = Some(CompressionType::ZSTD);
        IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap()
    }

    fn new_stream_writer(file: File) -> Result<StreamWriter<File>, ArrowError> {
        let options = Self::ipc_write_options();
        let stream = StreamWriter::try_new_with_options(file, &Self::SCHEMA, options)?;
        Ok(stream)
    }
}
