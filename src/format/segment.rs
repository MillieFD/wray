/*
Project: Wray
GitHub: https://github.com/MillieFD/wray

BSD 3-Clause License, Copyright (c) 2026, Amelia Fraser-Dale

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the conditions of the LICENSE are met.
*/

/* ----------------------------------------------------------------------------- Private Imports */

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Take};

use arrow::ipc::reader::StreamReader;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

/* ----------------------------------------------------------------------------- Public Exports */

/// A contiguous byte range of Arrow IPC data within the file.
#[derive(Debug, Clone)]
pub struct Segment {
    /// Byte offset to the start of the segment.
    pub offset: SeekFrom,
    /// Length in bytes.
    pub length: u64,
}

pub(crate) type Stream<'a> = StreamReader<BufReader<Take<&'a mut File>>>;

impl Segment {
    /// Create a zero-copy [`StreamReader`] window into `file` for this segment.
    pub(crate) fn stream<'a>(&self, file: &'a mut File) -> Result<Stream<'a>, Error> {
        file.seek(self.offset)?;
        let view = file.take(self.length); // zero-copy window into the file
        Ok(StreamReader::try_new_buffered(view, None)?) // buffer reduces syscall overhead
    }

    /// Byte offset past the last byte of this segment.
    pub fn end(&self) -> u64 {
        let SeekFrom::Start(off) = self.offset else {
            panic!("Offset is not SeekFrom::Start")
        };
        off + self.length
    }
}

impl Serialize for Segment {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let SeekFrom::Start(offset) = self.offset else {
            use serde::ser::Error;
            return Err(Error::custom("Segment offset is not SeekFrom::Start"));
        };
        (offset, self.length).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Segment {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (offset, length) = <(u64, u64)>::deserialize(deserializer)?;
        Ok(Self {
            offset: SeekFrom::Start(offset),
            length,
        })
    }
}
