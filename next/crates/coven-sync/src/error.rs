//! Failures of store-log publication, file uploads and sync steps (§20.5, §20.7).

use coven_crypto::{custody::KeyError, CryptoError, MaterialError};
use coven_database::{DbError, DropReason};
use coven_foundation::id_source::{KeyId, StoreId};
use coven_storage::StorageError;
use std::sync::Arc;

/// A synchronization request failed; its durable queue, if any, remains retryable.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// A user-selected download path already has a filesystem entry.
    #[error("download destination already exists: {}", path.display())]
    DestinationExists {
        /// The path that was refused, never replaced.
        path: std::path::PathBuf,
    },
    /// Keeping a user-provided file requires its destination path.
    #[error("file {id} requires a download destination")]
    DestinationRequired {
        /// The file id used as the destinations map key.
        id: String,
    },
    /// Two requested files cannot both own one download destination.
    #[error("download destination is repeated: {}", path.display())]
    DestinationRepeated {
        /// The repeated destination.
        path: std::path::PathBuf,
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
    /// The requested journal row is not a blocked operation.
    #[error("operation {0:?} is not blocked")]
    NotBlocked(crate::OperationId),
    /// Store-log and snapshot publication wait for the retained reload (§18).
    #[error("snapshot reload {0:?} must finish before publishing entries or snapshots")]
    ReloadPending(crate::OperationId),
    /// The reset's shared reload failed and must finish before the reset call can.
    #[error("recovery operation {operation:?} failed: {failure}")]
    RecoveryBlocked {
        /// The failed reload operation.
        operation: crate::OperationId,
        /// Its retained cause, also available in the blocked-operation report.
        failure: String,
    },
    /// Decoding persisted operation data failed; its cause remains available.
    #[error("invalid operation data: {0}")]
    OperationData(#[from] serde_json::Error),

    /// Revocation left grants requiring the owner's action. The operation stays
    /// blocked so an unattended removal cannot lose this result.
    #[error("storage access remains through grants: {0:?}")]
    AccessRemains(Vec<coven_storage::RetainedAccess>),
    /// Storage refused or failed a request.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The local database failed.
    #[error(transparent)]
    Database(#[from] DbError),
    /// A file reference or source needed by the operation could not be used.
    #[error(transparent)]
    File(crate::FileReadError),
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
    /// This install has no member keys in custody.
    #[error("member keys are absent from custody")]
    MissingMemberKeys,
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

/// A sync step stopped as a whole (§20.5).
#[derive(Clone, Debug, thiserror::Error)]
pub enum SyncFailure {
    /// A stored format is newer than this implementation.
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
            coven_format::Error::UnsupportedVersion(version)
                if version > coven_format::FORMAT_VERSION =>
            {
                Self::Stopped(SyncFailure::UpdateRequired)
            }
            error => Self::Format(error),
        }
    }
}
impl From<crate::FileReadError> for SyncError {
    fn from(error: crate::FileReadError) -> Self {
        match error {
            crate::FileReadError::UpdateRequired => Self::Stopped(SyncFailure::UpdateRequired),
            error => Self::File(error),
        }
    }
}
