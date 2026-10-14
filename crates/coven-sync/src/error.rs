//! Failures of store-log publication, file uploads and sync steps (E5, E7).

use coven_crypto::{custody::KeyError, CryptoError, MaterialError};
use coven_database::{DbError, DropReason};
use coven_foundation::files::{FileError, ObservationError, StoreLockError};
use coven_foundation::id_source::{KeyId, StoreId};
use coven_storage::{StorageError, StorageFailure};
use std::sync::Arc;

/// A synchronization request failed; its durable queue, if any, remains retryable.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// This call requires work that cannot yet advance.
    #[error("pending: {0:?}")]
    Pending(Box<crate::PendingRecord>),
    /// A local invariant or resource failed, classified without erasing its
    /// native cause. One failure delivered to several callers shares one cause.
    #[error("{failure:?}: {source}")]
    Failed {
        /// Durable local category.
        failure: crate::LocalFailure,
        /// Original failure retained by the initiating call.
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    /// A later step, such as a rollback or closing another owner, failed
    /// too; neither failure is hidden.
    #[error("{operation}; then: {cleanup}")]
    Cleanup {
        /// The original failure.
        operation: Box<SyncError>,
        /// The later failure, such as restoring the original state.
        #[source]
        cleanup: Box<SyncError>,
    },
    /// This call requires connected storage; durable work remains queued.
    #[error("storage is not connected")]
    NoStorage,
    /// The account holding the store cannot be removed.
    #[error("the store owner cannot be removed")]
    StoreOwner,
    /// The request or invitation is absent, expired, or no longer matches.
    #[error("invitation or join request is no longer current")]
    InvitationChanged,
    /// The requested journal row is not failed app work available for retry or discard.
    #[error("operation {0:?} is not pending")]
    NotPending(crate::OperationId),
    /// Store-log and snapshot publication wait for the retained reload (§18).
    #[error("snapshot reload {0:?} must finish before publishing entries or snapshots")]
    ReloadPending(crate::OperationId),
    /// Decoding persisted operation data failed; its cause remains available.
    #[error("invalid operation data: {0}")]
    OperationData(#[from] serde_json::Error),
    /// Deleting an S3 key with admin access did not establish its absence;
    /// its pending deletion stays (§13).
    #[error(transparent)]
    AccessKeyNotDeleted(#[from] crate::access_keys::DeleteAccessKeyError),
    /// Storage refused or failed a request.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The local database failed.
    #[error(transparent)]
    Database(DbError),
    /// Unlocking or keeping keys failed.
    #[error(transparent)]
    SecureStorage(#[from] KeyError),
    /// Making encrypted or signed bytes failed.
    #[error(transparent)]
    Crypto(CryptoError),
    /// The proposed entry violates its byte format.
    #[error(transparent)]
    Format(coven_format::Error),
    /// A required key has not arrived or the local key material conflicts.
    #[error(transparent)]
    Key(#[from] MaterialError),
    /// A required key's sealed copy has not arrived.
    #[error("key {0} is not available yet")]
    KeyUnavailable(KeyId),
    /// An entry must not introduce a second key under an existing identity.
    #[error("key identity {0} has already been used")]
    KeyAlreadyUsed(KeyId),
    /// The proposed change is already invalid in the author's applied view.
    #[error("store-log change rejected: {0:?}")]
    Rejected(DropReason),
    /// A required object failed authentication or parsing.
    #[error(transparent)]
    Damaged(#[from] crate::DamagedObject),
    /// This device or its member was removed; syncing stops for good (§10).
    #[error("this device or its member was removed")]
    Removed,
    /// This call needs a newer app schema or coven format (§17).
    #[error("an update is required: {0:?}")]
    UpdateRequired(crate::RequiredUpdate),
    /// A create-store entry named a store other than this database's store.
    #[error("create-store entry names another store")]
    WrongStore {
        /// This database's store.
        expected: StoreId,
        /// The proposed creation's store.
        actual: StoreId,
    },
}

impl From<coven_database::CovenError> for SyncError {
    fn from(error: coven_database::CovenError) -> Self {
        match error {
            coven_database::CovenError::Database(error) => error.into(),
            error => Self::Failed {
                failure: crate::LocalFailure::Database,
                source: Arc::new(error),
            },
        }
    }
}

impl From<DbError> for SyncError {
    fn from(error: DbError) -> Self {
        match error {
            // The database refusing sync's own requests out of order or
            // against changed state is an internal invariant, not a database
            // failure an app can act on.
            error @ (DbError::InvalidStoreLogEntry { .. }
            | DbError::StoreLogEntriesChanged
            | DbError::StoreLogEntryChanged(_)
            | DbError::UploadNotOldest { .. }
            | DbError::UploadNotAttempted { .. }
            | DbError::KeyUploadChanged { .. }
            | DbError::OperationChanged(_)) => Self::Failed {
                failure: crate::LocalFailure::Database,
                source: Arc::new(error),
            },
            error => Self::Database(error),
        }
    }
}

impl SyncError {
    /// A local file, directory or store-lock failure, retaining its cause.
    pub(crate) fn disk(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Failed {
            failure: crate::LocalFailure::Disk,
            source: Arc::new(error),
        }
    }

    /// The local category recorded for a failure that names no other condition.
    pub(crate) fn local_failure(&self) -> crate::LocalFailure {
        match self {
            Self::Failed { failure, .. } => *failure,
            Self::Database(_) => crate::LocalFailure::Database,
            Self::SecureStorage(_) => crate::LocalFailure::KeyCustody,
            Self::Crypto(_) | Self::Key(_) => crate::LocalFailure::Crypto,
            Self::Cleanup { operation, .. } => operation.local_failure(),
            _ => crate::LocalFailure::Operation,
        }
    }

    /// Splits a failure for delivery to several receivers: its local category
    /// and the original as one shared cause, so each receives the same `Failed`.
    pub(crate) fn into_shared(
        self,
    ) -> (
        crate::LocalFailure,
        Arc<dyn std::error::Error + Send + Sync>,
    ) {
        match self {
            Self::Failed { failure, source } => (failure, source),
            error => (error.local_failure(), Arc::new(error)),
        }
    }
}

impl From<FileError> for SyncError {
    fn from(error: FileError) -> Self {
        Self::disk(error)
    }
}

impl From<ObservationError> for SyncError {
    fn from(error: ObservationError) -> Self {
        Self::disk(error)
    }
}

impl From<StoreLockError> for SyncError {
    fn from(error: StoreLockError) -> Self {
        Self::disk(error)
    }
}

/// A sync step stopped as a whole (E5).
#[derive(Clone, Debug, thiserror::Error)]
pub enum SyncFailure {
    /// A schema or stored object format is newer than this implementation.
    #[error("an update is required")]
    UpdateRequired(crate::RequiredUpdate),
    /// This device or its member has been removed.
    #[error("this device or its member was removed")]
    Removed,
    /// The provider refused or failed a request.
    #[error("storage failed: {0}")]
    Storage(#[source] Arc<StorageError>),
    /// Any other failure, retaining its typed cause.
    #[error("sync failed: {0}")]
    Other(#[source] Arc<dyn std::error::Error + Send + Sync>),
}

impl From<DropReason> for SyncError {
    fn from(reason: DropReason) -> Self {
        Self::Rejected(reason)
    }
}

impl From<StorageFailure> for SyncError {
    fn from(failure: StorageFailure) -> Self {
        Self::Storage(failure.into())
    }
}

impl From<SyncError> for SyncFailure {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::Removed => Self::Removed,
            SyncError::UpdateRequired(update) => Self::UpdateRequired(update),
            SyncError::Storage(error) => Self::Storage(Arc::new(error)),
            error => Self::Other(Arc::new(error)),
        }
    }
}

impl From<coven_format::Error> for SyncError {
    fn from(error: coven_format::Error) -> Self {
        match required_format(&error) {
            Some(update) => Self::UpdateRequired(update),
            None => Self::Format(error),
        }
    }
}

impl From<CryptoError> for SyncError {
    fn from(error: CryptoError) -> Self {
        Self::Crypto(error)
    }
}

/// An object whose checks failed (§19.1).
#[derive(Debug, thiserror::Error)]
#[error("damaged object at {path}: {failure}")]
pub struct DamagedObject {
    /// The exact path read from storage.
    pub path: String,
    /// The failed check and its cause.
    #[source]
    pub failure: crate::Refusal,
}

pub(crate) fn required_format(
    error: &(dyn std::error::Error + 'static),
) -> Option<crate::RequiredUpdate> {
    let version = match error.downcast_ref::<coven_format::Error>() {
        Some(coven_format::Error::UnsupportedVersion(version)) => Some(*version),
        _ => None,
    }?;
    (version > coven_format::FORMAT_VERSION)
        .then_some(crate::RequiredUpdate::CovenFormat { version })
}

/// Older unsupported formats are damaged inputs; only newer ones require an update.
pub(crate) fn newer_format(error: &(dyn std::error::Error + 'static)) -> bool {
    required_format(error).is_some()
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
