//! Failures composing the store's directory, storage and custody capabilities.

use crate::{
    CryptoError, FileError, KeyError, OAuthError, StorageCheckError, StorageError, StoreLockError,
    SyncError,
};

/// A failed setup commits no local storage settings or credentials.
#[derive(Debug, thiserror::Error)]
pub enum StorageSetupError {
    /// A provider or S3 settings check refused the location.
    #[error(transparent)]
    Check(StorageCheckError),
    /// Reconnecting cannot move an established store to another location.
    #[error("storage location change is unsupported")]
    LocationChangeUnsupported,
    /// A classified storage failure.
    #[error(transparent)]
    Storage(StorageError),
    /// Sign-in failed or was cancelled before setup could connect.
    #[error(transparent)]
    OAuth(#[from] OAuthError),
    /// The facade could not commit to key custody.
    #[error("secure storage: {0}")]
    SecureStorage(KeyError),
    /// This device or its member was removed from the store (§14.1).
    #[error("this device or its member was removed")]
    Removed,
    /// This member's account holds the store, and setup signed in with
    /// another account. Their access stays on the account that holds it:
    /// the person signs in with that one (§10.1, §20.9).
    #[error("the store's owner must sign in with the account that holds it")]
    StoreOwner,
    /// The store needs a newer app schema or coven format (§18).
    #[error("an update is required: {0:?}")]
    UpdateRequired(coven_format::RequiredUpdate),
    /// The app cancelled setup before its point of no return, and what it
    /// had written was undone (§11.6).
    #[error("storage setup was cancelled and undone")]
    Cancelled,
    /// A setup on this device is past its point of no return and still
    /// finishing, so another can't start; starting sync finishes it (§11.6).
    #[error("a storage setup is still finishing")]
    SetupRunning,
    /// A local setup step failed, retaining its cause.
    #[error("storage setup: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Setup's runtime is shutting down and dropped it partway, so how much
    /// of it was saved is unknown (E15).
    #[error(transparent)]
    Shutdown(#[from] coven_foundation::task::Shutdown),
    /// Setup panicked: a bug, which the caller reports (E15). How much of it
    /// was saved is unknown.
    #[error(transparent)]
    Bug(#[from] coven_foundation::task::Bug),
}

impl coven_foundation::task::CarriesBug for StorageSetupError {
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::SecureStorage(error) => error.bug(),
            Self::Storage(error) => error.bug(),
            Self::Check(error) => error.bug(),
            Self::OAuth(error) => error.bug(),
            _ => None,
        }
    }
}

impl From<StorageError> for StorageSetupError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Bug(bug) => Self::Bug(bug),
            error => Self::Storage(error),
        }
    }
}

/// A storage failure keeps its one spelling, `Storage`; the other refusals
/// are checks.
impl From<StorageCheckError> for StorageSetupError {
    fn from(error: StorageCheckError) -> Self {
        match error {
            StorageCheckError::Storage(error) => Self::from(error),
            error => Self::Check(error),
        }
    }
}

impl From<KeyError> for StorageSetupError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::SecureStorage(error),
        }
    }
}

impl From<coven_foundation::task::TaskFailed> for StorageSetupError {
    fn from(failed: coven_foundation::task::TaskFailed) -> Self {
        failed.into_error()
    }
}

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
    Identity(crate::IdentityError),
    /// Closing the database or its workers failed.
    #[error(transparent)]
    Sync(SyncError),
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
        if let coven_foundation::files::BootstrapDirectoryError::Bug(bug) = error {
            return Self::Bug(bug);
        }
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
            crate::CovenError::Bug(bug) => Self::Bug(bug),
            error => Self::Store(error),
        }
    }
}

/// Opening the database fails as the store's open does.
impl From<coven_database::OpenError> for StoreCreationError {
    fn from(error: coven_database::OpenError) -> Self {
        crate::CovenError::from(error).into()
    }
}

impl From<crate::IdentityError> for StoreCreationError {
    fn from(error: crate::IdentityError) -> Self {
        match error {
            crate::IdentityError::Bug(bug) => Self::Bug(bug),
            error => Self::Identity(error),
        }
    }
}

impl From<SyncError> for StoreCreationError {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::Bug(bug) => Self::Bug(bug),
            error => Self::Sync(error),
        }
    }
}

impl crate::task::CarriesBug for StoreCreationError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Directory(error) => error.bug(),
            Self::Store(error) => error.bug(),
            Self::Identity(error) => error.bug(),
            Self::Sync(error) => error.bug(),
            Self::Cleanup { operation, cleanup } => operation.bug().or_else(|| cleanup.bug()),
            Self::PublishedButNotSynced { source, .. } => source.bug(),
            _ => None,
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
    Lock(StoreLockError),
    /// Writes or files wait to upload; nothing was deleted.
    #[error("{} writes and {} files wait to upload", .0.writes, .0.files)]
    Unsent(coven_database::UnsentWork),
    /// Reading the queue, connecting, or journaling this device's removal
    /// failed before the removal was journaled: nothing was deleted, nothing
    /// in storage changed, and the store is as it was.
    #[error(transparent)]
    Sync(crate::SyncError),
    /// A location is recorded for the store and this device holds no
    /// credentials for it, so it can't publish its removal: the person
    /// signs in to `provider`, then deletes again. Before the removal was
    /// journaled nothing changed; after, the deletion stays recorded for
    /// `CovenBuilder::resume_deletion`.
    #[error("sign in to {0:?} to delete the store from this device")]
    SignInRequired(crate::CloudProvider),
    /// This device's removal is journaled, and the deletion failed before it
    /// applied, with `0`'s cause. The deletion only goes forward: the handle
    /// is closed, the store lists as being deleted and refuses opens, and
    /// `CovenBuilder::resume_deletion` finishes it.
    #[error("the store is being deleted from this device; finishing it failed: {0}")]
    Unfinished(#[source] crate::SyncError),
    /// This device's removal applied, or no storage was set up, and closing
    /// the handle failed; nothing was deleted. A recorded deletion is
    /// finished by `CovenBuilder::resume_deletion`, and either by
    /// `Coven::discard_store`.
    #[error("closing the store failed after this device left it: {0}")]
    Closing(#[source] crate::SyncError),
    /// Opening the store to finish its recorded deletion failed.
    #[error(transparent)]
    Open(crate::CovenError),
    /// `resume_deletion` found no deletion of the store recorded on this
    /// device.
    #[error("no deletion of store {0} is under way")]
    NoUnfinishedDeletion(crate::StoreId),
    /// Removing a keychain entry failed.
    #[error(transparent)]
    Key(KeyError),
    /// Removing the store directory failed.
    #[error(transparent)]
    File(FileError),
    /// The deletion's runtime is shutting down and dropped it before it
    /// started, so nothing was deleted (E15).
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// The deletion panicked: a bug, which the caller reports (E15).
    /// Retrying finishes a deletion that stopped partway.
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl crate::task::CarriesBug for StoreDeletionError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Lock(error) => error.bug(),
            Self::Sync(error) | Self::Closing(error) | Self::Unfinished(error) => error.bug(),
            Self::Open(error) => error.bug(),
            Self::Key(error) => error.bug(),
            Self::File(error) => error.bug(),
            _ => None,
        }
    }
}

impl From<crate::SyncError> for StoreDeletionError {
    fn from(error: crate::SyncError) -> Self {
        match error {
            crate::SyncError::Bug(bug) => Self::Bug(bug),
            error => Self::Sync(error),
        }
    }
}

impl From<KeyError> for StoreDeletionError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::Key(error),
        }
    }
}

impl From<FileError> for StoreDeletionError {
    fn from(error: FileError) -> Self {
        match error {
            FileError::Bug(bug) => Self::Bug(bug),
            error => Self::File(error),
        }
    }
}

impl From<StoreLockError> for StoreDeletionError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Bug(bug) => Self::Bug(bug),
            error => Self::Lock(error),
        }
    }
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
    Local(crate::CovenError),
    /// Saved settings, credential custody or provider construction failed.
    #[error(transparent)]
    Storage(crate::SyncError),
    /// Opening panicked: a bug, which the caller reports (E15).
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<crate::CovenError> for ReadOnlyOpenError {
    fn from(error: crate::CovenError) -> Self {
        match error {
            crate::CovenError::Bug(bug) => Self::Bug(bug),
            error => Self::Local(error),
        }
    }
}

/// A database call's or opening's failure is the store's.
impl From<crate::DbError> for ReadOnlyOpenError {
    fn from(error: crate::DbError) -> Self {
        crate::CovenError::from(error).into()
    }
}

impl From<coven_database::OpenError> for ReadOnlyOpenError {
    fn from(error: coven_database::OpenError) -> Self {
        crate::CovenError::from(error).into()
    }
}

impl From<crate::SyncError> for ReadOnlyOpenError {
    fn from(error: crate::SyncError) -> Self {
        match error {
            crate::SyncError::Bug(bug) => Self::Bug(bug),
            error => Self::Storage(error),
        }
    }
}

impl crate::task::CarriesBug for ReadOnlyOpenError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Local(error) => error.bug(),
            Self::Storage(error) => error.bug(),
        }
    }
}

/// Opening this member's sealed store keys failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreKeyUnlockError {
    /// No stored provider configuration or credentials are available.
    #[error("no storage configured")]
    NoStorage,
    /// The provider refused or failed a request.
    #[error(transparent)]
    Storage(StorageError),
    /// Unlocking connected while a storage setup past its point of no
    /// return was still finishing, so it finished that setup (§11.6), and
    /// making a key or sealing a key copy for an operation settled there
    /// failed: the device's random source failed, or the recipient's
    /// sealing key is of low order.
    #[error(transparent)]
    Crypto(CryptoError),
    /// Custody refused to read or persist keys.
    #[error(transparent)]
    SecureStorage(KeyError),
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

impl crate::task::CarriesBug for StoreKeyUnlockError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Storage(error) => error.bug(),
            Self::SecureStorage(error) => error.bug(),
            _ => None,
        }
    }
}

impl From<StorageError> for StoreKeyUnlockError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Bug(bug) => Self::Bug(bug),
            error => Self::Storage(error),
        }
    }
}

impl From<KeyError> for StoreKeyUnlockError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::SecureStorage(error),
        }
    }
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
        // A joined store whose admission was refused never became a member:
        // shown as a removal is (§13.2).
        SyncError::Removed(_) => return Some(Stopped::Removed),
        SyncError::UpdateRequired(update) => return Some(Stopped::UpdateRequired(*update)),
        SyncError::Pending(record) => &record.reason,
        _ => return None,
    };
    match reason {
        crate::PendingReason::Removed(_) => Some(Stopped::Removed),
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
        SyncError::SecureStorage(error) => error.into(),
        SyncError::Storage(error) => error.into(),
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
        SyncError::SecureStorage(error) => error.into(),
        SyncError::Crypto(error) => StoreKeyUnlockError::Crypto(error),
        SyncError::Storage(error) => error.into(),
        SyncError::Shutdown(shutdown) => StoreKeyUnlockError::Shutdown(shutdown),
        SyncError::Bug(bug) => StoreKeyUnlockError::Bug(bug),
        error => StoreKeyUnlockError::Other(Box::new(error)),
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
