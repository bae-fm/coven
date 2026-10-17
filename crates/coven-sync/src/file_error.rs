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
    Database(#[from] DbError),
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
            LocalFileError::Database(error) => Self::Database(error),
            LocalFileError::Disk(error) => Self::Disk(error),
            LocalFileError::Lock(error) => Self::Lock(error),
        }
    }
}

/// An upload attempt failed. Its pending record keeps the category; the
/// native cause stays in this process's log.
#[derive(Debug, thiserror::Error)]
pub(crate) enum UploadFailure {
    /// The immutable destination contains different bytes.
    #[error("file upload was refused: {0:?}")]
    Refused(crate::Refusal),
    /// Reading or checking the source failed.
    #[error(transparent)]
    File(#[from] FileReadError),
    /// The provider failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl From<DbError> for UploadFailure {
    fn from(error: DbError) -> Self {
        Self::File(error.into())
    }
}
impl From<DiskError> for UploadFailure {
    fn from(error: DiskError) -> Self {
        Self::File(error.into())
    }
}
impl From<LocalFileError> for UploadFailure {
    fn from(error: LocalFileError) -> Self {
        Self::File(error.into())
    }
}

#[cfg(test)]
#[path = "file_error_tests.rs"]
mod tests;
