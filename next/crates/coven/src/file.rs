//! Adapt the database's local file capability to the application API.

use crate::{DbError, DeviceId, DiskError, FileLocation, StoreLockError};
use coven_database::{LocalFileError, LocalFileStream};
use std::path::PathBuf;

/// An open file with checked identity and range-reading state (§16.3).
/// Retaining a stream prevents store deletion, including after the store closes.
pub struct FileStream {
    local: LocalFileStream,
}

impl FileStream {
    pub(crate) fn new(local: LocalFileStream) -> Self {
        Self { local }
    }
    /// The file's whole size in bytes.
    pub fn plaintext_size(&self) -> u64 {
        self.local.plaintext_size()
    }
    /// Reads `len` bytes at `offset`. A range past the end is an error, never
    /// a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileReadError> {
        Ok(self.local.read_at(offset, len).await?)
    }
}

/// Reading a file failed with a cause the app can distinguish (§20.8).
#[derive(Debug, thiserror::Error)]
pub enum FileReadError {
    /// An uploaded file was read with no storage connected.
    #[error("no storage connected")]
    NoStorage,
    /// The file is only on another device, which the app can name.
    #[error("file {id} is on device {device:?}")]
    OnOtherDevice {
        /// The row's file id.
        id: String,
        /// The device keeping its bytes.
        device: DeviceId,
    },
    /// A user-provided file is gone from its recorded path.
    #[error("file {id} is missing at {}", path.display())]
    UserFileMissing {
        /// The row's file id.
        id: String,
        /// Its recorded original path.
        path: PathBuf,
    },
    /// A user-provided file's size or modification time no longer matches
    /// what coven recorded.
    #[error("file {id} changed at {}", path.display())]
    UserFileChanged {
        /// The row's file id.
        id: String,
        /// Its recorded original path.
        path: PathBuf,
    },
    /// A copy on this device failed its check.
    #[error("file {id} failed its content check")]
    Integrity {
        /// The row's file id.
        id: String,
    },
    /// The range lies outside the file.
    #[error("file {id} range {offset}..{end} exceeds {size}")]
    RangeOutOfBounds {
        /// The row's file id.
        id: String,
        /// The requested first byte.
        offset: u64,
        /// The requested exclusive end, saturated if addition overflows.
        end: u64,
        /// The file's whole size.
        size: u64,
    },
    /// The database failed, with its cause.
    #[error(transparent)]
    Database(#[from] DbError),
    /// The disk failed, with its cause.
    #[error(transparent)]
    Disk(#[from] DiskError),
    /// The store cannot be retained while opening its file.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
}

impl From<LocalFileError> for FileReadError {
    fn from(error: LocalFileError) -> Self {
        match error {
            LocalFileError::Unavailable {
                location: FileLocation::Uploaded,
                ..
            } => Self::NoStorage,
            LocalFileError::Unavailable {
                id,
                location: FileLocation::OnDevice(device),
            } => Self::OnOtherDevice { id, device },
            LocalFileError::UserFileMissing { id, path } => Self::UserFileMissing { id, path },
            LocalFileError::UserFileChanged { id, path } => Self::UserFileChanged { id, path },
            LocalFileError::Integrity { id } => Self::Integrity { id },
            LocalFileError::RangeOutOfBounds {
                id,
                offset,
                end,
                size,
            } => Self::RangeOutOfBounds {
                id,
                offset,
                end,
                size,
            },
            LocalFileError::Database(error) => Self::Database(error),
            LocalFileError::Disk(error) => Self::Disk(error),
            LocalFileError::Lock(error) => Self::Lock(error),
        }
    }
}

#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
