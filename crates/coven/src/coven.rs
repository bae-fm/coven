//! This app's stores on this device: where they live, the keychain their
//! entries are kept in, and the device's network; and the calls that create,
//! list, locate, open and remove them (E1).

use crate::{
    BootstrapError, CovenBuilder, CovenError, DbError, FileError, SavedRestoreEntry,
    StoreDeletionError, StoreDir, StoreId, StoreLockError,
};
use coven_crypto::custody::{KeyError, Keychain, StoreKeychain};
use coven_foundation::files::{ReplacedBy, StoreDirAccess, StoreDirectories};
use coven_foundation::network_watch::NetworkSource;
use std::path::PathBuf;
use std::sync::Arc;

/// This app's stores on this device (E1): the app directory each has its own
/// directory under, the OS keychain service their keys and secrets are kept
/// under, and the device's network, which each open store watches. The app
/// builds one at startup and holds it.
#[derive(Clone)]
pub struct Coven {
    directories: StoreDirectories,
    keychain: Arc<Keychain>,
    network: NetworkSource,
}

/// A store on this device (E1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreInfo {
    /// The store's identity.
    pub id: StoreId,
    /// The current name from this device's committed database state. While
    /// a replacement is unfinished, the name its new installation loaded.
    pub name: String,
    /// The work that must finish before the store opens, if any.
    pub unfinished: Option<Unfinished>,
}

/// Work on a store that must finish before it opens (E1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unfinished {
    /// A replacement of this device's installation, which the call it names
    /// finishes, or `CovenBuilder::resume_replacement` (§19.3).
    Replacement(ReplacedBy),
    /// This device is deleting the store and leaving it, its removal
    /// journaled or about to be: `CovenBuilder::resume_deletion` finishes it
    /// (§11.6).
    Deletion,
}

/// A store on this device as the listing found it (E1).
#[derive(Debug)]
pub enum ListedStore {
    /// Its name and state read.
    Readable(StoreInfo),
    /// Its replacement record or name can't be read, for `reason`. The app
    /// shows it as damaged and can offer `discard_store` for it.
    Unreadable {
        /// The store whose record or name can't be read.
        id: StoreId,
        /// Why it can't be read.
        reason: CovenError,
    },
}

impl ListedStore {
    /// The store this entry lists.
    pub fn id(&self) -> StoreId {
        match self {
            Self::Readable(info) => info.id,
            Self::Unreadable { id, .. } => *id,
        }
    }
}

/// A failure listing the stores. A store that can't be read is not one: it
/// is listed as `ListedStore::Unreadable`.
#[derive(Debug, thiserror::Error)]
pub enum StoreListError {
    /// The directory listing or an identity settings read failed.
    #[error(transparent)]
    File(FileError),
    /// The runtime is shutting down and dropped the listing's work (E15).
    #[error(transparent)]
    Shutdown(#[from] crate::task::Shutdown),
    /// Listing the stores panicked on the blocking pool: a bug, which the
    /// caller reports (E15).
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
    /// Finishing the recorded deletion of `store` failed (E1). The store is
    /// not listed, and the next listing finishes the deletion.
    #[error("finishing the deletion of store {store} failed: {source}")]
    Deletion {
        /// The store being deleted.
        store: StoreId,
        /// Why its deletion stopped.
        #[source]
        source: StoreDeletionError,
    },
}

impl From<FileError> for StoreListError {
    fn from(error: FileError) -> Self {
        match error {
            FileError::Bug(bug) => Self::Bug(bug),
            FileError::Shutdown(shutdown) => Self::Shutdown(shutdown),
            error => Self::File(error),
        }
    }
}

impl crate::task::CarriesBug for StoreListError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::File(error) => error.bug(),
            Self::Deletion { source, .. } => source.bug(),
            Self::Shutdown(_) => None,
        }
    }
}

impl Coven {
    /// The stores under `app_dir`, their keys and secrets in the OS keychain
    /// under the service `keyring_service`, watching the platform's network.
    /// The app creates `app_dir`; coven creates directories only inside it
    /// and never opens a directory above it, which Android refuses. No key
    /// is read here. A bug in work nobody waits for, in any store, goes to
    /// `bugs`, the app's crash reporter (E15). An empty service name, or one
    /// containing NUL, fails with `KeyError::InvalidServiceName`; an OS
    /// keychain coven can't reach fails with its `KeyError`.
    pub fn new(
        app_dir: PathBuf,
        keyring_service: &str,
        bugs: crate::task::BugSinkRef,
    ) -> Result<Self, KeyError> {
        Ok(Self {
            directories: StoreDirectories::new(app_dir, bugs),
            keychain: Keychain::native(keyring_service)?,
            network: NetworkSource::Platform,
        })
    }

    /// Read the saved storage location without opening the store or its custody.
    /// None means storage has never been set up; once set up, the location stays.
    pub async fn storage_config(
        store_dir: &StoreDir,
    ) -> Result<Option<crate::StorageConfig>, crate::SyncError> {
        let store_dir = store_dir.clone();
        let bugs = store_dir.bugs().clone();
        crate::task::blocking_completing("read the storage settings", bugs, move || {
            coven_storage::recorded_config(&store_dir)
        })
        .await?
        .map_err(crate::SyncError::from)
    }

    /// Keep SQLite's temporary tables in `directory`, such as the app's cache
    /// folder, for every store this process opens. An Android app calls this
    /// before opening its first store: SQLite finds no folder there by itself,
    /// and temporary tables that outgrow a connection's cache fail without
    /// one. Elsewhere SQLite uses the system's.
    pub fn set_temporary_directory(
        directory: &std::path::Path,
    ) -> Result<(), crate::TemporaryDirectoryError> {
        coven_database::set_temporary_directory(directory)
    }

    /// Stores on this device, ordered by id, with their committed names.
    /// A store whose replacement record or name can't be read is listed as
    /// `Unreadable` with why; a listing never substitutes an older name.
    /// A store whose replacement or deletion is unfinished lists with the
    /// work that must finish, so the app can offer to finish it; an
    /// unfinished first installation is not listed. First it finishes every
    /// deletion from the device that a cut call left under the directory's
    /// deletion name, which no call holds (§19.3, E1); a failure there fails
    /// the listing with `StoreListError::Deletion`.
    pub async fn stores(&self) -> Result<Vec<ListedStore>, StoreListError> {
        for store in self.directories.recorded_deletions().await? {
            let directory = self.store_dir(&store);
            let keys = StoreKeychain::new(self.keychain.clone(), store);
            match finish_deletion(&directory, keys).await {
                Ok(()) => tracing::info!(%store, "finished a recorded deletion"),
                // A call is deleting it now, and finishes it.
                Err(StoreDeletionError::Lock(StoreLockError::AlreadyOpen(_))) => {
                    tracing::debug!(%store, "a call holds a recorded deletion")
                }
                Err(StoreDeletionError::Bug(bug)) => return Err(StoreListError::Bug(bug)),
                Err(StoreDeletionError::Shutdown(shutdown)) => {
                    return Err(StoreListError::Shutdown(shutdown))
                }
                Err(source) => return Err(StoreListError::Deletion { store, source }),
            }
        }
        let mut stores = Vec::new();
        for id in self.directories.store_ids().await? {
            stores.push(match self.read_listed(id).await {
                Ok(info) => ListedStore::Readable(info),
                Err(error) => {
                    if let Some(bug) = crate::task::CarriesBug::bug(&error) {
                        return Err(StoreListError::Bug(bug.clone()));
                    }
                    match error {
                        CovenError::Shutdown(shutdown)
                        | CovenError::Database(DbError::Shutdown(shutdown)) => {
                            return Err(StoreListError::Shutdown(shutdown));
                        }
                        reason => ListedStore::Unreadable { id, reason },
                    }
                }
            });
        }
        Ok(stores)
    }

    async fn read_listed(&self, id: StoreId) -> Result<StoreInfo, CovenError> {
        let directory = self.store_dir(&id);
        let (replacement, deletion) = {
            let directory = directory.clone();
            let bugs = directory.bugs().clone();
            crate::task::blocking_completing("read the store's records", bugs, move || {
                Ok::<_, CovenError>((
                    directory.unfinished_replacement()?,
                    directory.unfinished_deletion().map_err(DbError::from)?,
                ))
            })
            .await??
        };
        Ok(match replacement {
            Some(record) => StoreInfo {
                id,
                name: record.name,
                unfinished: Some(Unfinished::Replacement(record.by)),
            },
            None => StoreInfo {
                id,
                name: coven_database::read_store_name(directory).await?,
                unfinished: deletion.map(|_| Unfinished::Deletion),
            },
        })
    }

    /// Address a store without reading settings or acquiring the writer lock.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir {
        self.directories.store_dir(id)
    }

    /// Configure creating, opening, restoring or joining a store.
    pub fn builder(&self) -> CovenBuilder {
        CovenBuilder::new(self.directories.clone(), self.keychain.clone(), self.network.clone())
    }

    /// Deletes a closed store from this device without opening it: this
    /// device's own keychain entries for it, including every saved host
    /// secret, then its directory. The person's saved restore code and S3 key
    /// stay for their next devices; they go only when the
    /// person leaves the store (§13.1). Unsent writes and files are lost, and
    /// this device stays registered with the store until it is removed; prefer
    /// `CovenHandle::delete_store`. Use this for a store that cannot open, or
    /// to finish a handle's deletion that failed after closing. A store whose
    /// `reset_device` or `restore_device` stopped past its record is deleted
    /// whole: both installations, the replacement's directory and record,
    /// and this device's entries, which by then may name either device.
    /// Refused while a writer, read-only handle, file stream or a running
    /// replacement remains; storage is untouched. A handle of it in this
    /// process that is closing, or whose last clone was dropped, is waited
    /// for. Its first step renames the store's directory, which hides the
    /// store and records the deletion: one that stops part way, a crash
    /// included, is finished by retrying, by the next `stores()`, or by a
    /// restore or join of the store (§19.3).
    pub async fn discard_store(&self, store_dir: &StoreDir) -> Result<(), StoreDeletionError> {
        let keys = StoreKeychain::new(self.keychain.clone(), store_dir.id());
        finish_deletion(store_dir, keys).await
    }

    /// The saved entries of this app's keychain service, in iCloud Keychain on
    /// Apple and in this phone's Block Store on Android, one per store,
    /// ordered by store id; empty when none are saved. An entry coven can't
    /// restore from is listed as `Unreadable`, never omitted, so the person
    /// can forget it. A keychain read failure (the search, an entry's bytes
    /// or an S3 key's presence) fails the whole call, as does a restore-code
    /// account with no store id, `KeyError::RestoreCodeStoreId`. A keychain
    /// with neither iCloud Keychain nor Block Store fails with
    /// `BootstrapError::SecureStorage(KeyError::Unsupported)`. Pass a readable
    /// entry to `restore_from_saved_code`, which reads its current code and key.
    pub async fn saved_restore_codes(&self) -> Result<Vec<SavedRestoreEntry>, BootstrapError> {
        let keychain = self.keychain.clone();
        let bugs = self.directories.bugs().clone();
        crate::task::blocking_completing("list saved restore codes", bugs, move || {
            crate::saved_restore::list(keychain)
        })
        .await?
    }

    /// Delete the store's saved S3 key, then its restore code: from iCloud
    /// Keychain, and so from every one of the person's Apple devices, or from
    /// this phone's Block Store, leaving other phones' own (§13.1). Readable
    /// and unreadable entries alike; every keychain group's copy goes. The
    /// store, its storage and every device's copy stay; no store need be open
    /// and no provider connected. Absent entries succeed; after a failure
    /// part way the store is still listed, and calling again finishes. A
    /// later join, restore, reset or member-key replacement saves the entry
    /// again. A keychain with neither iCloud Keychain nor Block Store fails
    /// with `BootstrapError::SecureStorage(KeyError::Unsupported)`.
    pub async fn forget_saved_restore_code(&self, store: StoreId) -> Result<(), BootstrapError> {
        let keychain = self.keychain.clone();
        let bugs = self.directories.bugs().clone();
        crate::task::blocking_completing("forget the saved restore code", bugs, move || {
            crate::saved_restore::forget(keychain, store)
        })
        .await?
    }
}

/// Delete the store at `directory` from this device, or finish a deletion a
/// cut call left recorded, once no instance of it in this process is
/// closing.
pub(crate) async fn finish_deletion<E>(directory: &StoreDir, keys: StoreKeychain) -> Result<(), E>
where
    E: From<StoreLockError>
        + From<FileError>
        + From<KeyError>
        + From<crate::task::TaskFailed>
        + crate::task::CarriesBug
        + Send
        + 'static,
{
    directory.closed().await?;
    let directory = directory.clone();
    let bugs = directory.bugs().clone();
    crate::task::blocking_completing("delete the store", bugs, move || delete(&directory, &keys))
        .await?
}

/// Delete the store at `directory` from this device: rename its directory to
/// its deletion name, which hides the store and records the deletion, then
/// delete this device's entries for it, then the directory and its lock
/// files (§19.3, E1). Each step finds the last one's work done, so a
/// deletion stopped anywhere is finished by deleting again.
pub(crate) fn delete<E>(directory: &StoreDir, keys: &StoreKeychain) -> Result<(), E>
where
    E: From<StoreLockError> + From<FileError> + From<KeyError>,
{
    let mut lock = directory.lock_for_deletion()?;
    if let Some(lock) = &mut lock {
        lock.unpublish()?;
    }
    keys.delete_device_entries()?;
    if let Some(lock) = lock {
        lock.remove_directory()?;
    }
    Ok(())
}

#[cfg(any(test, feature = "test-utils"))]
#[path = "test_utils.rs"]
mod test_utils;

#[cfg(test)]
#[path = "coven_tests.rs"]
mod tests;
