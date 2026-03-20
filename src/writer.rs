/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::sync::{Arc, LazyLock};

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::CompressionType;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};

use crate::format::Buf;

/* ------------------------------------------------------------------------------ Public Exports */

/// Shared behaviour for all Arrow IPC table writers.
#[allow(clippy::declare_interior_mutable_const)]
pub(crate) trait Writer {
    /// Lazily initialised Arrow schema for this table.
    const SCHEMA: LazyLock<Arc<Schema>>;

    /// Clone the shared schema `Arc`.
    #[allow(clippy::borrow_interior_mutable_const)]
    fn schema() -> Arc<Schema> {
        Self::SCHEMA.clone()
    }

    /// IPC write options with ZSTD compression enabled.
    fn ipc_write_options() -> IpcWriteOptions {
        IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .expect("ZSTD compression supported")
    }

    /// Create a new [`StreamWriter`] backed by the given [`Buf`].
    #[allow(clippy::borrow_interior_mutable_const)]
    fn new_stream_writer(buf: Buf) -> Result<StreamWriter<Buf>, ArrowError> {
        StreamWriter::try_new_with_options(buf, &Self::SCHEMA, Self::ipc_write_options())
    }
}
