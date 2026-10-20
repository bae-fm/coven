//! Failures of sequence publication, file uploads and sync steps (E5, E7).

use crate::OperationRejection;
use coven_crypto::{custody::KeyError, CryptoError, MaterialError};
use coven_database::DbError;
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
    /// Administrative and snapshot publication wait for the retained reload (§11).
    #[error("snapshot reload {0:?} must finish before publishing entries or snapshots")]
    ReloadPending(crate::OperationId),
    /// Decoding persisted operation data failed; its cause remains available.
    #[error("invalid operation data: {0}")]
    OperationData(#[from] serde_json::Error),
    /// Deleting an S3 key with admin access did not establish its absence;
    /// its pending deletion stays (§14.3).
    #[error(transparent)]
    AccessKeyNotDeleted(#[from] crate::sequence_sync::access_keys::DeleteAccessKeyError),
    /// No greater positive store slot remains.
    #[error("the store sequence is exhausted")]
    SequenceExhausted,
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
    #[error("administrative change rejected: {0:?}")]
    Rejected(OperationRejection),
    /// A required object failed authentication or parsing.
    #[error(transparent)]
    Damaged(#[from] crate::DamagedObject),
    /// This device or its member was removed; syncing stops for good (§14.1).
    #[error("this device or its member was removed")]
    Removed,
    /// Another copy uses this device's id, as this evidence shows. This copy
    /// sends nothing more; nothing is recorded, and `reset_device` is the
    /// app's option (§19.3).
    #[error("another copy uses this device's id: {0:?}")]
    DeviceIdInUse(crate::DeviceIdEvidence),
    /// Held signing credentials are no longer current for this stable id, or
    /// an object was fixed under credentials that are no longer current.
    /// Sending stops; it never re-signs an old object with newer keys (§14.1).
    #[error("member {member:?} has newer keys; held credentials cannot send")]
    MemberKeysReplaced {
        /// The stable member whose keys were replaced.
        member: coven_foundation::id_source::MemberId,
    },
    /// This call needs a newer app schema or coven format (§18).
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
            error @ (DbError::OperationChanged(_) | DbError::Invariant { .. }) => Self::Failed {
                failure: crate::LocalFailure::Database,
                source: Arc::new(error),
            },
            error => Self::Database(error),
        }
    }
}

impl SyncError {
    /// The durable reason for a failed request, without losing its typed cause here.
    pub fn pending_reason(&self, provider: coven_storage::CloudProvider) -> crate::PendingReason {
        use crate::PendingReason;
        use coven_merge::Audience;
        let error = self;
        match error {
            SyncError::Pending(record) => record.reason.clone(),
            SyncError::Storage(error) => PendingReason::Storage {
                provider,
                failure: error.failure(),
                answered: error.answered(),
            },
            SyncError::Damaged(object) => PendingReason::Refused(object.failure.clone()),
            SyncError::UpdateRequired(update) => PendingReason::UpdateRequired(*update),
            SyncError::Removed => PendingReason::Removed,
            SyncError::MemberKeysReplaced { member } => {
                PendingReason::MemberKeysReplaced { member: *member }
            }
            SyncError::KeyUnavailable(key) => PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key: *key,
            },
            SyncError::Rejected(reason) => PendingReason::Rejected(reason.clone()),
            SyncError::Cleanup { operation, cleanup } => PendingReason::Cleanup {
                operation: Box::new(operation.pending_reason(provider)),
                cleanup: Box::new(cleanup.pending_reason(provider)),
            },
            _ => PendingReason::Failed(error.local_failure()),
        }
    }

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

    /// A local database, disk or custody failure that can clear by itself,
    /// such as a locked keychain or a failed disk write (E5). It fails only
    /// this attempt and says nothing about the slot being applied (§19.1).
    pub(crate) fn local_failure_clears_by_itself(&self) -> bool {
        matches!(
            self,
            Self::Database(_) | Self::SecureStorage(_) | Self::Failed { .. }
        ) && crate::PendingReason::Failed(self.local_failure()).clears()
            == Some(crate::pending::Clears::ByItself)
    }

    /// The local category and one shared cause, for a status and a waiting
    /// caller failed by the same error.
    pub fn into_shared(
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
    /// Another copy uses this device's id; this copy sends nothing more (§19.3).
    #[error("another copy uses this device's id: {0:?}")]
    DeviceIdInUse(crate::DeviceIdEvidence),
    /// The provider refused or failed a request.
    #[error("storage failed: {0}")]
    Storage(#[source] Arc<StorageError>),
    /// The pass finished, but this work of coven's own failed its last
    /// attempt; it retries under backoff (E5).
    #[error("{0:?}")]
    Operation(Box<crate::PendingRecord>),
    /// Any other failure, retaining its typed cause.
    #[error("sync failed: {0}")]
    Other(#[source] Arc<dyn std::error::Error + Send + Sync>),
}

impl From<OperationRejection> for SyncError {
    fn from(reason: OperationRejection) -> Self {
        match reason {
            OperationRejection::StoreOwner => Self::StoreOwner,
            reason => Self::Rejected(reason),
        }
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
            SyncError::DeviceIdInUse(evidence) => Self::DeviceIdInUse(evidence),
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
#[derive(Clone, Debug, thiserror::Error)]
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
