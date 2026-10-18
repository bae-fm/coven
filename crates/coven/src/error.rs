//! Failures composing the store's directory, storage and custody capabilities.

use crate::{
    CryptoError, FileError, KeyError, StorageError, StorageSetupError, StoreLockError, SyncError,
};

/// Creating a store failed, with publication and rollback made explicit (E1).
#[derive(Debug, thiserror::Error)]
pub enum StoreCreationError {
    /// Preparing, publishing or removing the hidden directory failed.
    #[error(transparent)]
    Directory(#[from] coven_foundation::files::BootstrapDirectoryError),
    /// Opening the database or custody failed.
    #[error(transparent)]
    Store(#[from] crate::CovenError),
    /// Creating or persisting the member identity failed.
    #[error(transparent)]
    Identity(#[from] crate::IdentityError),
    /// Closing the database or its workers failed.
    #[error(transparent)]
    Sync(#[from] SyncError),
    /// Closing or rollback also failed; both causes remain available.
    #[error("{operation}; cleanup failed: {cleanup}")]
    Cleanup {
        /// The original failure.
        operation: Box<StoreCreationError>,
        /// A cleanup failure.
        #[source]
        cleanup: Box<StoreCreationError>,
    },
    /// The complete store is visible and closed, but its directory sync failed.
    #[error("store {store} was published but its directory sync failed: {source}")]
    PublishedButNotSynced {
        /// The published store; reopen it with `CovenBuilder::open`.
        store: crate::StoreId,
        /// The directory-sync failure, retaining any close failure as `Cleanup`.
        #[source]
        source: Box<StoreCreationError>,
    },
}

impl StoreCreationError {
    pub(crate) fn with_cleanup(self, cleanup: Self) -> Self {
        Self::Cleanup {
            operation: Box::new(self),
            cleanup: Box::new(cleanup),
        }
    }
}

/// Deleting the local store stopped at a step the caller may retry (E1).
#[derive(Debug, thiserror::Error)]
pub enum StoreDeletionError {
    /// The store is open or its lock could not be taken.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
    /// Writes or files wait to upload; nothing was deleted.
    #[error("{} writes and {} files wait to upload", .0.writes, .0.files)]
    Unsent(coven_database::UnsentWork),
    /// Reading the queue or publishing this device's removal failed;
    /// nothing was deleted. Closing failed after the removal was published.
    #[error(transparent)]
    Sync(#[from] crate::SyncError),
    /// Removing a keychain entry failed.
    #[error(transparent)]
    Key(#[from] KeyError),
    /// Removing the store directory failed.
    #[error(transparent)]
    File(#[from] FileError),
}

/// Opening a read-only store or its saved file-storage connection failed.
#[derive(Debug, thiserror::Error)]
pub enum ReadOnlyOpenError {
    /// The database or store directory could not be opened.
    #[error(transparent)]
    Local(#[from] crate::CovenError),
    /// Saved settings, credential custody or provider construction failed.
    #[error(transparent)]
    Storage(#[from] crate::SyncError),
}

/// Opening this member's sealed store keys failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreKeyUnlockError {
    /// No stored provider configuration or credentials are available.
    #[error("no storage configured")]
    NoStorage,
    /// The provider refused or failed a request.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// An authenticated key could not be opened.
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    /// Custody refused to read or persist keys.
    #[error(transparent)]
    SecureStorage(#[from] KeyError),
    /// Sequence validation or local work failed, preserving its cause.
    #[error("opening store keys: {0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) fn setup_error(error: SyncError) -> StorageSetupError {
    match error {
        SyncError::SecureStorage(error) => StorageSetupError::SecureStorage(error),
        SyncError::Storage(error) => StorageSetupError::Storage(error),
        error => StorageSetupError::Internal(Box::new(error)),
    }
}

pub(crate) fn unlock_error(error: SyncError) -> StoreKeyUnlockError {
    match error {
        SyncError::NoStorage => StoreKeyUnlockError::NoStorage,
        SyncError::SecureStorage(error) => StoreKeyUnlockError::SecureStorage(error),
        SyncError::Crypto(error) => StoreKeyUnlockError::Crypto(error),
        SyncError::Storage(error) => StoreKeyUnlockError::Storage(error),
        error => StoreKeyUnlockError::Other(Box::new(error)),
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
