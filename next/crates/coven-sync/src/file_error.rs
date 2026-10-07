//! Typed file read and upload failures shared with the application facade.

use coven_database::{DbError, FileLocation, LocalFileError};
use coven_foundation::{
    files::{FileError as DiskError, StoreLockError},
    id_source::DeviceId,
};
use coven_storage::{StorageError, StorageFailure};
use std::{path::PathBuf, sync::Arc};

/// Reading a file failed with a cause the app can distinguish (§20.8).
#[derive(Debug, thiserror::Error)]
pub enum FileReadError {
    /// Required bytes are uncached and the network is unavailable.
    #[error("file {id} is unavailable offline")]
    Offline {
        /// The row's file id.
        id: String,
    },
    /// No storage capability is connected.
    #[error("no storage connected")]
    NoStorage,
    /// Only the named device holds the file.
    #[error("file {id} is on device {device:?}")]
    OnOtherDevice {
        /// The row's file id.
        id: String,
        /// The device holding the bytes.
        device: DeviceId,
    },
    /// A recorded original is absent.
    #[error("file {id} is missing at {}", path.display())]
    UserFileMissing {
        /// The row's file id.
        id: String,
        /// Recorded original path.
        path: PathBuf,
    },
    /// An original changed after attachment.
    #[error("file {id} changed at {}", path.display())]
    UserFileChanged {
        /// The row's file id.
        id: String,
        /// Recorded original path.
        path: PathBuf,
    },
    /// Authentication or the whole-file hash failed.
    #[error("file {id} failed its content check")]
    Integrity {
        /// The row's file id.
        id: String,
    },
    /// The requested range is outside the file.
    #[error("file {id} range {offset}..{end} exceeds {size}")]
    RangeOutOfBounds {
        /// The row's file id.
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
    /// A provider failure other than an unavailable network.
    #[error(transparent)]
    Storage(#[from] StorageError),
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

/// A durable failure category. OS and provider error objects remain available
/// in the live failure; after reopening, this records the actionable category.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RecordedUploadFailure {
    /// No storage was connected.
    NoStorage,
    /// Source bytes were absent or changed, or failed authentication.
    File,
    /// Local database or filesystem failed.
    Local,
    /// File-key creation failed.
    Crypto,
    /// The provider's classified failure.
    Storage(StorageFailure),
}

/// An upload attempt failed. Sharing the cause preserves native error objects
/// in both a drain result and the live queue without converting them to text.
#[derive(Debug, thiserror::Error)]
pub enum UploadFailure {
    /// Reading or checking the source failed.
    #[error(transparent)]
    File(#[from] FileReadError),
    /// The provider failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Creating a file key failed.
    #[error(transparent)]
    Crypto(#[from] coven_crypto::CryptoError),
    /// A previous process recorded this failure category.
    #[error("recorded upload failure: {0:?}")]
    Recorded(RecordedUploadFailure),
}
impl UploadFailure {
    pub(crate) fn recording(&self) -> RecordedUploadFailure {
        match self {
            Self::File(FileReadError::NoStorage) => RecordedUploadFailure::NoStorage,
            Self::File(
                FileReadError::Database(_) | FileReadError::Disk(_) | FileReadError::Lock(_),
            ) => RecordedUploadFailure::Local,
            Self::File(FileReadError::Storage(error)) | Self::Storage(error) => {
                RecordedUploadFailure::Storage(error.failure())
            }
            Self::File(_) => RecordedUploadFailure::File,
            Self::Crypto(_) => RecordedUploadFailure::Crypto,
            Self::Recorded(recorded) => recorded.clone(),
        }
    }
}
/// Failed files and their shared, typed causes from a drain.
pub type UploadFailures = Vec<(coven_database::FileRef, Arc<UploadFailure>)>;

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
