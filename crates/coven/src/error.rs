//! Failures composing the store's directory, storage and custody capabilities.

use crate::{
    CryptoError, FileError, KeyError, StorageError, StorageSetupError, StoreLockError, SyncError,
};

/// Creating a store failed, with publication and rollback made explicit (E1).
#[derive(Debug, thiserror::Error)]
pub enum StoreCreationError {
    /// Preparing, publishing or removing the hidden directory failed.
    #[error(transparent)]
    Directory(coven_foundation::files::BootstrapDirectoryError),
    /// Opening the database or custody failed.
    #[error(transparent)]
    Store(crate::CovenError),
    /// The device is locked, and the protection class the app chose keeps
    /// the new store's files from being made until it unlocks (api.md E1).
    /// No store was made; the app waits for the unlock and creates it again.
    #[error("{}: the device is locked, and the store's files are protected until it unlocks", path.display())]
    DeviceLocked {
        /// The first protected file the lock refused.
        path: std::path::PathBuf,
    },
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
    /// Creation's runtime is shutting down and dropped its work (E15).
    /// Creation runs as its own task, so with `ShutdownOutcome::Unknown` it
    /// may have stopped anywhere, before or after publishing the store;
    /// listing the stores shows which. A step on the blocking pool that the
    /// runtime dropped is `NotRun`.
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// Creation panicked: a bug, which the caller reports (E15). As with
    /// `Shutdown`, listing the stores shows whether it published the store.
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<coven_foundation::files::BootstrapDirectoryError> for StoreCreationError {
    fn from(error: coven_foundation::files::BootstrapDirectoryError) -> Self {
        match error.device_locked() {
            Some(path) => Self::DeviceLocked { path: path.to_owned() },
            None => Self::Directory(error),
        }
    }
}

impl From<crate::CovenError> for StoreCreationError {
    fn from(error: crate::CovenError) -> Self {
        match error {
            crate::CovenError::DeviceLocked { path } => Self::DeviceLocked { path },
            error => Self::Store(error),
        }
    }
}

impl From<crate::task::TaskFailed> for StoreCreationError {
    fn from(failed: crate::task::TaskFailed) -> Self {
        failed.into_error()
    }
}

impl StoreCreationError {
    pub(crate) fn with_cleanup(self, cleanup: Self) -> Self {
        Self::Cleanup { operation: Box::new(self), cleanup: Box::new(cleanup) }
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
    /// nothing was deleted, and this device hasn't applied its removal.
    /// `delete_store` again finishes it.
    #[error(transparent)]
    Sync(#[from] crate::SyncError),
    /// This device's removal was published, or no storage was set up, and
    /// closing the handle failed; nothing was deleted.
    /// `Coven::discard_store` finishes the deletion.
    #[error("closing the store failed after this device left it: {0}")]
    Closing(#[source] crate::SyncError),
    /// Removing a keychain entry failed.
    #[error(transparent)]
    Key(#[from] KeyError),
    /// Removing the store directory failed.
    #[error(transparent)]
    File(#[from] FileError),
    /// The deletion's runtime is shutting down and dropped it before it
    /// started, so nothing was deleted (E15).
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// The deletion panicked: a bug, which the caller reports (E15).
    /// Retrying finishes a deletion that stopped partway.
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<crate::task::TaskFailed> for StoreDeletionError {
    fn from(failed: crate::task::TaskFailed) -> Self {
        failed.into_error()
    }
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
    /// This device or its member was removed from the store (§14.1).
    #[error("this device or its member was removed")]
    Removed,
    /// The store needs a newer app schema or coven format (§18).
    #[error("an update is required: {0:?}")]
    UpdateRequired(crate::RequiredUpdate),
    /// Sequence validation or local work failed, preserving its cause.
    #[error("opening store keys: {0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Unlocking's runtime is shutting down and dropped it partway, so
    /// whether it saved the keys it opened is unknown (E15).
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// Unlocking panicked: a bug, which the caller reports (E15). Whether
    /// it saved the keys it opened is unknown.
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<crate::task::TaskFailed> for StoreKeyUnlockError {
    fn from(failed: crate::task::TaskFailed) -> Self {
        failed.into_error()
    }
}

/// What setup and unlock report by name: a removal or a required update,
/// whether the pass returned it or stopped at a record of it.
enum Stopped {
    Removed,
    UpdateRequired(crate::RequiredUpdate),
}

fn stopped(error: &SyncError) -> Option<Stopped> {
    let reason = match error {
        SyncError::Removed => return Some(Stopped::Removed),
        SyncError::UpdateRequired(update) => return Some(Stopped::UpdateRequired(*update)),
        SyncError::Pending(record) => &record.reason,
        _ => return None,
    };
    match reason {
        crate::PendingReason::Removed => Some(Stopped::Removed),
        crate::PendingReason::UpdateRequired(update) => Some(Stopped::UpdateRequired(*update)),
        _ => None,
    }
}

pub(crate) fn setup_error(error: SyncError) -> StorageSetupError {
    match stopped(&error) {
        Some(Stopped::Removed) => return StorageSetupError::Removed,
        Some(Stopped::UpdateRequired(update)) => return StorageSetupError::UpdateRequired(update),
        None => {}
    }
    match error {
        SyncError::SecureStorage(error) => StorageSetupError::SecureStorage(error),
        SyncError::Storage(error) => StorageSetupError::Storage(error),
        SyncError::StoreOwner => StorageSetupError::StoreOwner,
        SyncError::Shutdown(shutdown) => StorageSetupError::Shutdown(shutdown),
        SyncError::Bug(bug) => StorageSetupError::Bug(bug),
        error => StorageSetupError::Internal(Box::new(error)),
    }
}

pub(crate) fn unlock_error(error: SyncError) -> StoreKeyUnlockError {
    match stopped(&error) {
        Some(Stopped::Removed) => return StoreKeyUnlockError::Removed,
        Some(Stopped::UpdateRequired(update)) => {
            return StoreKeyUnlockError::UpdateRequired(update)
        }
        None => {}
    }
    match error {
        SyncError::NoStorage => StoreKeyUnlockError::NoStorage,
        SyncError::SecureStorage(error) => StoreKeyUnlockError::SecureStorage(error),
        SyncError::Crypto(error) => StoreKeyUnlockError::Crypto(error),
        SyncError::Storage(error) => StoreKeyUnlockError::Storage(error),
        SyncError::Shutdown(shutdown) => StoreKeyUnlockError::Shutdown(shutdown),
        SyncError::Bug(bug) => StoreKeyUnlockError::Bug(bug),
        error => StoreKeyUnlockError::Other(Box::new(error)),
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
