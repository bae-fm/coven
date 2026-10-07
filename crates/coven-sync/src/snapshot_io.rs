//! Bounded disk and storage reads used by snapshot operations.

use crate::SyncError;
use coven_crypto::{DerivedKeys, StoreKeyring};
use coven_foundation::{
    files::{FileArea, FileName, FileReader, StoreDir, StoreReadLock},
    id_source::KeyId,
};
use coven_merge::Audience;
use coven_storage::{ByteRange, ObjectPath, Storage};
use std::io::{self, Read};

pub(super) const BUFFER: usize = coven_format::chunks::CHUNK_SIZE;

/// Retains the store while SQLite consumes a checked temporary file.
pub(crate) struct SnapshotInput {
    reader: FileReader,
    offset: u64,
    _lock: StoreReadLock,
}

impl SnapshotInput {
    pub(super) fn open(directory: &StoreDir, name: &str) -> Result<Self, SyncError> {
        let lock = directory.lock_read_only()?;
        let name =
            FileName::new(name.to_owned()).map_err(|_| coven_database::DbError::DamagedDatabase)?;
        let reader = directory.file(FileArea::AppProvided, &name).open_reader()?;
        Ok(Self {
            reader,
            offset: 0,
            _lock: lock,
        })
    }

    pub(super) fn size(&self) -> u64 {
        self.reader.size()
    }

    pub(super) fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, SyncError> {
        Ok(self.reader.read_at(offset, length)?)
    }
}

impl Read for SnapshotInput {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let length =
            (self.reader.size() - self.offset).min(bytes.len().min(BUFFER) as u64) as usize;
        if length == 0 {
            return Ok(0);
        }
        let read = self
            .reader
            .read_at(self.offset, length)
            .map_err(io::Error::other)?;
        bytes[..read.len()].copy_from_slice(&read);
        self.offset += read.len() as u64;
        Ok(read.len())
    }
}

pub(super) fn name(value: &str) -> Result<FileName, SyncError> {
    FileName::new(value.to_owned()).map_err(|_| coven_database::DbError::DamagedDatabase.into())
}

pub(super) fn remove(directory: &StoreDir, name: &str) -> Result<(), SyncError> {
    let _lock = directory.lock_read_only()?;
    let name =
        FileName::new(name.to_owned()).map_err(|_| coven_database::DbError::DamagedDatabase)?;
    Ok(directory.file(FileArea::AppProvided, &name).remove()?)
}

pub(super) fn key(
    ring: &StoreKeyring,
    audience: &Audience,
    id: KeyId,
) -> Result<DerivedKeys, SyncError> {
    crate::write_seal::derive(ring, audience, id)
}

pub(super) async fn range(
    storage: &dyn Storage,
    path: &ObjectPath,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, SyncError> {
    let end = offset
        .checked_add(length as u64)
        .ok_or(coven_format::Error::Truncated)?;
    let bytes = storage
        .read_range(path, ByteRange::new(offset, end)?)
        .await
        .map_err(|error| match error {
            coven_storage::StorageError::InvalidRange => {
                SyncError::Format(coven_format::Error::Truncated)
            }
            error => SyncError::Storage(error),
        })?;
    if bytes.len() != length {
        return Err(coven_format::Error::Truncated.into());
    }
    Ok(bytes)
}
