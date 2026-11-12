//! Why opening a new installation failed, and how each cause it keeps
//! becomes one of its variants (§13, E10, E15).

use super::*;
use coven_foundation::files::BootstrapDirectoryError;

/// Opening a new installation failed. A join's unpublished work, and a
/// recorded restore's, remain for an explicit retry unless the call was
/// cancelled or the invite was settled; a restore before its record removes
/// its own.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// Cancellation removes unpublished local data and leaves final custody alone.
    #[error("opening cancelled")]
    Cancelled,
    /// Cancelling a join after its request may have been sent. The join stays
    /// unfinished and resumable; it ends only by approval, decline or expiry
    /// (§13.2).
    #[error("the join request may have been sent; the join stays unfinished")]
    JoinRequestSent,
    /// No join is under way for this store on this device: none was started
    /// here, or it has ended.
    #[error("no join is under way for store {0}")]
    NoUnfinishedJoin(StoreId),
    /// No unfinished first installation of this store is on this device, or
    /// a call holds it, so `discard_unfinished_installation` deleted nothing.
    #[error("no unfinished installation of store {0} to discard")]
    NoUnfinishedInstallation(StoreId),
    /// Cancelling a restore once its device may be registered. The restore
    /// stays unfinished and listed, and only finishes (§13.1).
    #[error("the device may be registered; the restore stays unfinished")]
    RestoreRegistered,
    /// No restore is under way for this store on this device: none recorded
    /// itself here, or it has ended.
    #[error("no restore is under way for store {0}")]
    NoUnfinishedRestore(StoreId),
    /// No replacement of this device's installation of the store is under
    /// way past its record: none recorded itself, or it has finished, or a
    /// connected reset's kept record is gone.
    #[error("no replacement is under way for store {0}")]
    NoUnfinishedReplacement(StoreId),
    /// The code cannot be used for this operation.
    #[error(transparent)]
    Code(#[from] CodeError),
    /// Preparing, publishing or removing the unpublished directory failed.
    #[error(transparent)]
    Directory(BootstrapDirectoryError),
    /// Opening the database failed.
    #[error(transparent)]
    Store(CovenError),
    /// The device is locked, and the protection class the app chose keeps
    /// the installation's files closed until it unlocks (api.md E1). A join's
    /// unfinished installation stays for the retry, and a recorded
    /// restore's; a restore before its record removes its own. The app waits
    /// for the unlock and calls again.
    #[error("{}: the device is locked, and the store's files are protected until it unlocks", path.display())]
    DeviceLocked {
        /// The first protected file the lock refused.
        path: std::path::PathBuf,
    },
    /// Provider admission, signed membership or snapshot loading failed.
    #[error(transparent)]
    Sync(SyncError),
    /// Provider sign-in failed.
    #[error(transparent)]
    OAuth(coven_storage::providers::OAuthError),
    /// Final custody could not be read or written.
    #[error(transparent)]
    SecureStorage(KeyError),
    /// Cleanup failed too; retry can inspect both failures.
    #[error("{operation}; cleanup failed: {cleanup}")]
    Cleanup {
        /// The initial failure.
        operation: Box<BootstrapError>,
        /// Its cleanup failure.
        #[source]
        cleanup: Box<BootstrapError>,
    },
    /// The loaded store is published; confirming durability or final cleanup
    /// failed. Custody remains committed and the handle retains session-only keys.
    #[error("store is published; final durability or cleanup failed: {source}")]
    Published {
        /// The open store, including its session-only custody.
        handle: CovenHandle,
        /// Failure after publication.
        #[source]
        source: Box<BootstrapError>,
    },
    /// The call's runtime is shutting down and dropped its keychain work
    /// before it started, so nothing changed (E15).
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// The call's work panicked: a bug, which the caller reports (E15).
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<BootstrapDirectoryError> for BootstrapError {
    fn from(error: BootstrapDirectoryError) -> Self {
        if let BootstrapDirectoryError::Bug(bug) = error {
            return Self::Bug(bug);
        }
        match error.device_locked() {
            Some(path) => Self::DeviceLocked { path: path.to_owned() },
            None => Self::Directory(error),
        }
    }
}

/// A code sync refuses is refused as the call's own code: a join's, as a
/// restore's.
impl From<SyncError> for BootstrapError {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::Code(error) => Self::Code(error),
            SyncError::Bug(bug) => Self::Bug(bug),
            error => Self::Sync(error),
        }
    }
}

impl From<CovenError> for BootstrapError {
    fn from(error: CovenError) -> Self {
        match error {
            CovenError::DeviceLocked { path } => Self::DeviceLocked { path },
            CovenError::Bug(bug) => Self::Bug(bug),
            error => Self::Store(error),
        }
    }
}

/// A database call's or opening's failure is the store's.
impl From<DbError> for BootstrapError {
    fn from(error: DbError) -> Self {
        CovenError::from(error).into()
    }
}

impl From<coven_database::OpenError> for BootstrapError {
    fn from(error: coven_database::OpenError) -> Self {
        CovenError::from(error).into()
    }
}

impl From<coven_storage::providers::OAuthError> for BootstrapError {
    fn from(error: coven_storage::providers::OAuthError) -> Self {
        match error {
            coven_storage::providers::OAuthError::Storage(StorageError::Bug(bug)) => Self::Bug(bug),
            error => Self::OAuth(error),
        }
    }
}

impl From<StoreLockError> for BootstrapError {
    fn from(error: StoreLockError) -> Self {
        BootstrapDirectoryError::from(error).into()
    }
}

impl From<coven_foundation::files::FileError> for BootstrapError {
    fn from(error: coven_foundation::files::FileError) -> Self {
        BootstrapDirectoryError::from(error).into()
    }
}

impl From<KeyError> for BootstrapError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::SecureStorage(error),
        }
    }
}

impl crate::task::CarriesBug for BootstrapError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Directory(error) => error.bug(),
            Self::Store(error) => error.bug(),
            Self::Sync(error) => error.bug(),
            Self::OAuth(error) => error.bug(),
            Self::SecureStorage(error) => error.bug(),
            Self::Cleanup { operation, cleanup } => operation.bug().or_else(|| cleanup.bug()),
            Self::Published { source, .. } => source.bug(),
            _ => None,
        }
    }
}

impl From<crate::task::TaskFailed> for BootstrapError {
    fn from(failed: crate::task::TaskFailed) -> Self {
        failed.into_error()
    }
}
