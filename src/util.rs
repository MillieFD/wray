/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::io::Cursor;

use arrow::array::{Array, ArrowPrimitiveType, AsArray, PrimitiveArray, RecordBatch};
use arrow::ipc::reader::{FileReader, StreamReader};

use crate::Error;

/* ------------------------------------------------------------------------------ Public Exports */

/// Extract a typed primitive column from a [`RecordBatch`] by name.
pub(crate) fn col<'a, T>(batch: &'a RecordBatch, name: &str) -> Result<&'a PrimitiveArray<T>, Error>
where
    T: ArrowPrimitiveType,
{
    batch
        .column_by_name(name)
        .ok_or_else(|| Error::MissingColumn(name.into()))
        .map(AsArray::as_primitive::<T>)
}

/// Decode all [`RecordBatch`]es from an IPC stream (must include EOS).
pub(crate) fn batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, Error> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    reader.into_iter().map(|b| Ok(b?)).collect()
}

/// Decode all [`RecordBatch`]es from Arrow IPC **file** format bytes.
pub(crate) fn file_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, Error> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = FileReader::try_new(Cursor::new(bytes), None)?;
    reader.into_iter().map(|b| Ok(b?)).collect()
}

/// Read a nullable value at row `i`.
pub(crate) fn nullable<T>(arr: &PrimitiveArray<T>, i: usize) -> Option<T::Native>
where
    T: ArrowPrimitiveType,
{
    match arr.is_null(i) {
        true => None,
        false => Some(arr.value(i)),
    }
}
