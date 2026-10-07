//! Failures of store-log publication and a sync step (§20.5).

use coven_crypto::{custody::KeyError, CryptoError, MaterialError};
use coven_database::{DbError, DropReason};
use coven_foundation::id_source::{KeyId, StoreId};
use coven_storage::StorageError;
use std::sync::Arc;

/// A local store-log operation failed; its durable queue, if any, remains retryable.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
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
