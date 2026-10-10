//! Failures of store-log publication, file uploads and sync steps (E5, E7).

use coven_crypto::{custody::KeyError, CryptoError, MaterialError};
use coven_database::{DbError, DropReason};
use coven_foundation::id_source::{KeyId, StoreId};
use coven_storage::{StorageError, StorageFailure};
use std::sync::Arc;

/// A synchronization request failed; its durable queue, if any, remains retryable.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// An explicit recovery or join requires an object from a stopped log.
    #[error("operation requires a stuck log: {0:?}")]
    StuckLog(coven_format::stuck::StuckRecord),
    /// Provider setup failed before replacing the active connection.
    #[error(transparent)]
    Setup(#[from] Box<coven_storage::StorageSetupError>),
    /// A credential update does not contain a valid restore code.
    #[error(transparent)]
    Code(#[from] crate::CodeError),
    /// A credential update names a different person.
    #[error("restore code belongs to {actual}, expected {expected}")]
    WrongMember {
        /// The current member.
        expected: coven_crypto::MemberId,
        /// The member named by the code.
        actual: coven_crypto::MemberId,
    },
    /// Rollback failed too; neither failure is hidden.
    #[error("{operation}; rollback failed: {cleanup}")]
    Cleanup {
        /// The original failure.
        operation: Box<SyncError>,
        /// Failure restoring the original state.
        #[source]
        cleanup: Box<SyncError>,
    },
    /// Reading a snapshot stream failed.
    #[error(transparent)]
    SnapshotIo(#[from] std::io::Error),
    /// Recorded snapshot bytes disappeared or changed on disk.
    #[error(transparent)]
    SnapshotFile(#[from] coven_foundation::files::ObservationError),
    /// Keeping a snapshot file failed.
    #[error(transparent)]
    Disk(#[from] coven_foundation::files::FileError),
    /// The store could not be retained during snapshot work.
    #[error(transparent)]
    Lock(#[from] coven_foundation::files::StoreLockError),
    /// This call requires connected storage; durable work remains queued.
    #[error("storage is not connected")]
    NoStorage,
    /// The caller has no authority for this change.
    #[error("permission denied")]
    PermissionDenied,
    /// The change would remove the final admin.
    #[error("the store must retain an admin")]
    LastAdmin,
    /// The account holding the store cannot be removed.
    #[error("the store owner cannot be removed")]
    StoreOwner,
    /// Circle membership is required by this operation.
    #[error("not a member of circle {0}")]
    CircleNotMember(coven_foundation::id_source::CircleId),
    /// A circle operation named a deleted or absent circle.
    #[error("circle {0} is deleted")]
    CircleDeleted(coven_foundation::id_source::CircleId),
    /// Only current members can be invited into a circle.
    #[error("{0} is not a store member")]
    NotStoreMember(coven_crypto::MemberId),
    /// The request or invitation is absent, expired, or no longer matches.
    #[error("invitation or join request is no longer current")]
    InvitationChanged,
    /// The requested journal row is not failed app work available for retry or discard.
    #[error("operation {0:?} is not pending")]
    NotPending(crate::OperationId),
    /// Store-log and snapshot publication wait for the retained reload (§18).
    #[error("snapshot reload {0:?} must finish before publishing entries or snapshots")]
    ReloadPending(crate::OperationId),
    /// The reset's shared reload failed and must finish before the reset call can.
    #[error("recovery operation {operation:?} failed: {failure}")]
    RecoveryPending {
        /// The failed reload operation.
        operation: crate::OperationId,
        /// Its retained cause.
        failure: String,
    },
    /// Decoding persisted operation data failed; its cause remains available.
    #[error("invalid operation data: {0}")]
    OperationData(#[from] serde_json::Error),

    /// Revocation left grants requiring the owner's action. The operation stays
    /// pending so an unattended removal cannot lose this result.
    #[error("storage access remains through grants: {0:?}")]
    AccessRemains(Vec<coven_storage::RetainedAccess>),
    /// Storage refused or failed a request.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The local database failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// Unlocking or keeping keys failed.
    #[error(transparent)]
    SecureStorage(#[from] KeyError),
    /// Making encrypted or signed bytes failed.
    #[error(transparent)]
    Crypto(#[from] CryptoError),
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
    /// This device must stop syncing with this location.
    #[error(transparent)]
    Stopped(#[from] SyncFailure),
    /// A create-store entry named a store other than this database's store.
    #[error("create-store entry names another store")]
    WrongStore {
        /// This database's store.
        expected: StoreId,
        /// The proposed creation's store.
        actual: StoreId,
    },
}

impl SyncError {
    /// Network interruptions and unavailable prerequisites remain runnable.
    pub(crate) fn blocks_operation(&self) -> bool {
        match self {
            Self::NoStorage
            | Self::KeyUnavailable(_)
            | Self::ReloadPending(_)
            | Self::Stopped(SyncFailure::UpdateRequired) => false,
            Self::Storage(error) => !error.retryable(),
            _ => true,
        }
    }
}

/// A sync step stopped as a whole (E5).
#[derive(Clone, Debug, thiserror::Error)]
pub enum SyncFailure {
    /// A schema or stored object format is newer than this implementation.
    #[error("an update is required")]
    UpdateRequired,
    /// This device or its member has been removed.
    #[error("this device or its member was removed")]
    Removed,
    /// Another store's creation precedes this store's creation at this location.
    #[error("another store took this storage location")]
    LocationTaken,
    /// The provider refused or failed a request.
    #[error("storage failed: {0}")]
    Storage(#[source] Arc<StorageError>),
    /// Any other failure, retaining its typed cause.
    #[error("sync failed: {0}")]
    Other(#[source] Arc<dyn std::error::Error + Send + Sync>),
}

impl From<StorageFailure> for SyncError {
    fn from(failure: StorageFailure) -> Self {
        Self::Storage(failure.into())
    }
}

impl From<SyncError> for SyncFailure {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::Stopped(failure) => failure,
            SyncError::Storage(error) => Self::Storage(Arc::new(error)),
            error => Self::Other(Arc::new(error)),
        }
    }
}

impl From<coven_format::Error> for SyncError {
    fn from(error: coven_format::Error) -> Self {
        match error {
            error if newer_format(&error) => Self::Stopped(SyncFailure::UpdateRequired),
            error => Self::Format(error),
        }
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
    pub failure: ObjectCheckFailure,
}

/// Authentication and parsing failures retain their original causes.
#[derive(Debug, thiserror::Error)]
pub enum ObjectCheckFailure {
    /// The authenticated write failed the merge or application schema's checks.
    #[error("invalid write: {0}")]
    InvalidWrite(#[source] Arc<DbError>),
    /// Opening the object or its authenticated path failed.
    #[error("decryption failed: {0}")]
    Decryption(#[source] CryptoError),
    /// The author's signature did not verify.
    #[error("signature failed: {0}")]
    Signature(#[source] CryptoError),
    /// The bytes or their causal metadata violate the format.
    #[error("parse failed: {0}")]
    Parse(#[source] Arc<dyn std::error::Error + Send + Sync>),
}

impl ObjectCheckFailure {
    pub(crate) fn category(&self) -> coven_format::stuck::StuckFailure {
        use coven_format::stuck::StuckFailure;
        match self {
            Self::Decryption(_) => StuckFailure::Decryption,
            Self::Signature(_) => StuckFailure::Signature,
            Self::Parse(_) => StuckFailure::Parse,
            Self::InvalidWrite(_) => StuckFailure::InvalidWrite,
        }
    }
}

/// Older unsupported formats are damaged inputs; only newer ones require an update.
pub(crate) fn newer_format(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<coven_format::Error>(),
        Some(coven_format::Error::UnsupportedVersion(version)) if *version > coven_format::FORMAT_VERSION
    ) || matches!(
        error.downcast_ref::<CryptoError>(),
        Some(CryptoError::UnsupportedVersion(version)) if *version > coven_format::FORMAT_VERSION
    )
}
