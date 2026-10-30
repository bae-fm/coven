//! Typed file read and upload failures shared with the application facade.

use coven_database::{DbError, LocalFileError};
use coven_foundation::{
    files::{FileError as DiskError, StoreLockError},
    id_source::{DeviceId, FileId},
};
use coven_storage::StorageError;
use std::path::PathBuf;

/// Reading a file failed with a cause the app can distinguish (E8).
#[derive(Debug, thiserror::Error)]
pub enum FileReadError {
    /// This file uses a newer coven format (Appendix C.2).
    #[error("an update is required to read this file")]
    UpdateRequired(crate::RequiredUpdate),
    /// No storage capability is connected.
    #[error("no storage connected")]
    NoStorage,
    /// Storage confirms nothing at the path a stored file's record names:
    /// the provider lost it (§17.5). Nothing brings it back on its own.
    #[error("file {id}, uploaded by device {device:?}, is missing from storage")]
    Missing {
        /// The file's id.
        id: String,
        /// The device that uploaded it.
        device: DeviceId,
    },
    /// The file is only on one device, another one or this one with no
    /// source to read: nothing here can read it, and nothing waits for it
    /// (§17.2).
    #[error("file {file} is only on device {device:?}")]
    DeviceOnly {
        /// The file.
        file: FileId,
        /// The device that has it.
        device: DeviceId,
    },
    /// A recorded original is absent.
    #[error("file {id} is missing at {}", path.display())]
    UserFileMissing {
        /// The file's id.
        id: String,
        /// Recorded original path.
        path: PathBuf,
    },
    /// An original changed after it was staged.
    #[error("file {id} changed at {}", path.display())]
    UserFileChanged {
        /// The file's id.
        id: String,
        /// Recorded original path.
        path: PathBuf,
    },
    /// Authentication, a chunk hash or the whole-file hash failed.
    #[error("file {id} failed its content check")]
    Integrity {
        /// The file's id.
        id: String,
        /// The authentication failure, when a chunk failed to open. A size,
        /// layout or hash disagreement has no cause beyond the comparison.
        #[source]
        cause: Option<std::sync::Arc<coven_crypto::CryptoError>>,
    },
    /// The requested range is outside the file.
    #[error("file {id} range {offset}..{end} exceeds {size}")]
    RangeOutOfBounds {
        /// The file's id.
        id: String,
        /// Requested start.
        offset: u64,
        /// Requested end, saturated on overflow.
        end: u64,
        /// Whole plaintext size.
        size: u64,
    },
    /// A database failure, retaining its cause.
    #[error(transparent)]
    Database(DbError),
    /// A disk failure, retaining its cause.
    #[error(transparent)]
    Disk(#[from] DiskError),
    /// The store cannot be retained while opening its file.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
    /// A provider failure, retaining its classification and native cause.
    #[error(transparent)]
    Storage(#[from] StorageError),
}
impl From<DbError> for FileReadError {
    fn from(error: DbError) -> Self {
        match error {
            DbError::Disk(error) => Self::Disk(error),
            DbError::Lock(error) => Self::Lock(error),
            error => Self::Database(error),
        }
    }
}
impl FileReadError {
    /// A chunk of file `id` that did not seal or open. An authentication
    /// failure keeps its crypto error. A chunk length outside the header's
    /// layout is the layout's disagreement, which the comparison states.
    pub(crate) fn chunk(id: String, error: coven_format::file::FileError) -> Self {
        Self::Integrity {
            id,
            cause: match error {
                coven_format::file::FileError::Crypto(error) => Some(std::sync::Arc::new(error)),
                coven_format::file::FileError::Format(_) => None,
            },
        }
    }
}
impl From<LocalFileError> for FileReadError {
    fn from(error: LocalFileError) -> Self {
        match error {
            LocalFileError::UserFileMissing { id, path } => Self::UserFileMissing { id, path },
            LocalFileError::UserFileChanged { id, path } => Self::UserFileChanged { id, path },
            LocalFileError::Integrity { id } => Self::Integrity { id, cause: None },
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
            LocalFileError::Database(error) => error.into(),
            LocalFileError::Disk(error) => Self::Disk(error),
            LocalFileError::Lock(error) => Self::Lock(error),
        }
    }
}

/// A file transfer retains its typed cause and exact refusal. The pending
/// record keeps the category before the app-facing error merges content checks.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferError {
    /// Content or immutable identity verification failed.
    #[error("file transfer was refused: {0:?}")]
    Refused(crate::Refusal),
    /// Reading, storing or checking bytes failed.
    #[error(transparent)]
    File(#[from] FileReadError),
}

impl From<StorageError> for TransferError {
    fn from(error: StorageError) -> Self {
        Self::File(error.into())
    }
}
impl From<DbError> for TransferError {
    fn from(error: DbError) -> Self {
        Self::File(error.into())
    }
}
impl From<DiskError> for TransferError {
    fn from(error: DiskError) -> Self {
        Self::File(error.into())
    }
}
impl From<LocalFileError> for TransferError {
    fn from(error: LocalFileError) -> Self {
        Self::File(error.into())
    }
}

#[cfg(test)]
#[path = "file_error_tests.rs"]
mod tests;
