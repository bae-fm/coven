//! Failures of store-log publication and a sync step (§20.5).

use coven_crypto::{custody::KeyError, CryptoError, MaterialError};
use coven_database::{DbError, DropReason};
use coven_foundation::id_source::{KeyId, StoreId};
use coven_storage::StorageError;
use std::sync::Arc;

/// A local store-log operation failed; its durable queue, if any, remains retryable.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// This handle has no connected provider; operations wait for one.
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
    /// Unlocking or keeping keys failed.
    #[error(transparent)]
    SecureStorage(#[from] KeyError),
    /// Making encrypted or signed bytes failed.
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    /// The proposed entry violates its byte format.
    #[error(transparent)]
    Format(#[from] coven_format::Error),
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
