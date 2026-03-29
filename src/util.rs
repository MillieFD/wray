/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use arrow::array::{Array, ArrowPrimitiveType, AsArray, PrimitiveArray, RecordBatch};

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
