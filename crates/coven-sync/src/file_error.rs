//! Typed file read and upload failures shared with the application facade.

use coven_database::{DbError, LocalFileError};
use coven_foundation::{
    files::{FileError as DiskError, StoreLockError},
    id_source::DeviceId,
};
use coven_storage::StorageError;
use std::path::PathBuf;

/// Reading a file failed with a cause the app can distinguish (E8).
#[derive(Debug, thiserror::Error)]
pub enum FileReadError {
    /// This file uses a newer coven format (§17.2).
    #[error("an update is required to read this file")]
    UpdateRequired(crate::RequiredUpdate),
    /// No storage capability is connected.
    #[error("no storage connected")]
    NoStorage,
    /// Storage confirms absence; the active uploader has not reported failure.
    #[error("file {id} is still uploading on device {device:?}")]
    Uploading {
        /// The file's fixed object id.
        id: String,
        /// The device responsible for uploading it.
        device: DeviceId,
    },
    /// Storage confirms absence and the uploader cannot supply it (§16.1).
    #[error("file {id} is missing: device {device:?} cannot supply it ({reason:?})")]
    Missing {
        /// The file's fixed object id.
        id: String,
        /// The device responsible for uploading it.
        device: DeviceId,
        /// Why it cannot.
        reason: crate::FileMissingReason,
    },
    /// A recorded original is absent.
    #[error("file {id} is missing at {}", path.display())]
    UserFileMissing {
        /// The file's fixed object id.
        id: String,
        /// Recorded original path.
        path: PathBuf,
    },
    /// An original changed after attachment.
    #[error("file {id} changed at {}", path.display())]
    UserFileChanged {
        /// The file's fixed object id.
        id: String,
        /// Recorded original path.
        path: PathBuf,
    },
    /// Authentication, a chunk hash or the whole-file hash failed.
    #[error("file {id} failed its content check")]
    Integrity {
        /// The file's fixed object id.
        id: String,
    },
    /// The requested range is outside the file.
    #[error("file {id} range {offset}..{end} exceeds {size}")]
    RangeOutOfBounds {
        /// The file's fixed object id.
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
impl From<LocalFileError> for FileReadError {
    fn from(error: LocalFileError) -> Self {
        match error {
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
