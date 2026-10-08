//! Failures composing the store's directory, storage and custody capabilities.

use crate::{
    CryptoError, FileError, KeyError, StorageError, StorageSetupError, StoreLockError, SyncError,
};

/// Creating a store failed, with publication and rollback made explicit (E1).
pub type StoreCreationError = coven_foundation::files::StoreCreationError<KeyError>;

/// Deleting the local store stopped at a step the caller may retry (E1).
#[derive(Debug, thiserror::Error)]
pub enum StoreDeletionError {
    /// The store is open or its lock could not be taken.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
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

/// Explicit damaged-database recovery failed. The source archive is retained
/// after replacement begins, and ordinary opens refuse an unfinished reload.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    /// Opening SQLite, local custody or the directory failed.
    #[error(transparent)]
    Local(#[from] crate::CovenError),
    /// The authenticated store log or snapshot could not be loaded.
    #[error(transparent)]
    Sync(#[from] crate::SyncError),
    /// Recovery requires a store key before moving the damaged database.
    #[error("recovery requires unlocked store keys")]
    NoStoreKeys,
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
    /// Store-log validation or local work failed, preserving its cause.
    #[error("opening store keys: {0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) fn setup_error(error: SyncError) -> StorageSetupError {
    match error {
        SyncError::Setup(error) => *error,
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
