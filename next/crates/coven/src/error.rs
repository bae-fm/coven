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
