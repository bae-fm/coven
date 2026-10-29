//! Local bounded reads of retained provider bytes during an apply transaction.

use super::{read_local, sql_offset, InputConnection, InputRead, RANGES};
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;

/// Independent cursor over one pinned input. Closing the database invalidates
/// every cursor; an absent cached range returns an I/O error, never a network read.
#[derive(Clone)]
pub struct RetainedBytes {
    reader: InputConnection,
    reading: Arc<InputRead>,
    position: u64,
    length: u64,
}

impl RetainedBytes {
    pub(super) fn new(reader: InputConnection, reading: Arc<InputRead>, length: u64) -> Self {
        Self {
            reader,
            reading,
            position: 0,
            length,
        }
    }

    /// The received object's declared length.
    pub fn len(&self) -> u64 {
        self.length
    }

    /// Whether this object has no bytes.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

impl Read for RetainedBytes {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let length = self
            .length
            .saturating_sub(self.position)
            .min(output.len() as u64)
            .min(coven_format::chunks::CHUNK_SIZE as u64);
        if length == 0 {
            return Ok(0);
        }
        let start = self.position;
        let end = start + length;
        let ranges = read_local(&self.reader, |db| {
            db.query(
                RANGES,
                (self.reading.id, sql_offset(start)?, sql_offset(end)?),
                |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, Vec<u8>>(1)?)),
            )
        })
        .map_err(io::Error::other)?;
        let mut copied = 0;
        for (offset, bytes) in ranges {
            if offset != start + copied as u64 {
                break;
            }
            output[copied..copied + bytes.len()].copy_from_slice(&bytes);
            copied += bytes.len();
        }
        if copied == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "provider bytes have not been received",
            ));
        }
        #[cfg(any(test, feature = "test-utils"))]
        coven_foundation::test_work::retained_read(copied);
        self.position += copied as u64;
        Ok(copied)
    }
}

impl Seek for RetainedBytes {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::End(offset) => self.length.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        }
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "input position is outside u64")
        })?;
        self.position = position;
        Ok(position)
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}
