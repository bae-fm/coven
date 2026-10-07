//! Failures composing the store's directory and custody capabilities.

use crate::{FileError, KeyError, StoreLockError};

/// Creating a store failed, with publication and rollback made explicit (§20.1).
pub type StoreCreationError = coven_foundation::files::StoreCreationError<KeyError>;

/// Deleting the local store stopped at a step the caller may retry (§20.1).
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
