//! The exclusive OS lock held for the lifetime of a writable store.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use crate::files::FileError;
use crate::id_source::StoreId;

/// An exclusive store lock. Dropping it closes the OS handle and releases the
/// lock. Read-only opens take no lock, so they can coexist with a writer (§20.1).
#[derive(Debug)]
pub struct StoreLock {
    _file: File,
}

/// A store already open for writing is distinct from a filesystem failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreLockError {
    /// Another handle or process holds the store's exclusive lock.
    #[error("store {0} is already open for writing")]
    AlreadyOpen(StoreId),
    /// Opening or locking the lock file failed for another reason.
    #[error("store lock: {0}")]
    File(#[from] FileError),
}

pub(crate) fn acquire(path: &Path, id: StoreId) -> Result<StoreLock, StoreLockError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|source| FileError::at("open store lock", path, source))?;
    match file.try_lock() {
        Ok(()) => Ok(StoreLock { _file: file }),
        Err(TryLockError::WouldBlock) => Err(StoreLockError::AlreadyOpen(id)),
        Err(TryLockError::Error(source)) => Err(FileError::at("lock store", path, source).into()),
    }
}

#[cfg(test)]
#[path = "lock_tests.rs"]
mod tests;
