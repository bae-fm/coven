//! Hidden stores retain interrupted bootstrap work until explicit retry or
//! cancellation, or until a listing reclaims one nothing refers to.

use super::{
    atomic_file::exists, creation, device_reset, lock, protection::Protection,
    DirectoryCreationError, FileError, StoreDir, StoreLockError,
};
use crate::files::StoreDirAccess;
use crate::id_source::{IdSource, StoreId};
use crate::locks::lock_unpoisoned;
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Filesystem failures while reserving, publishing or cancelling a new device.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapDirectoryError {
    /// A store already exists or preparing its directory failed.
    #[error(transparent)]
    Create(#[from] DirectoryCreationError),
    /// Another bootstrap or an open database prevents this operation.
    #[error(transparent)]
    Lock(StoreLockError),
    /// A filesystem operation failed.
    #[error(transparent)]
    File(FileError),
    /// The work panicked on the blocking pool: a bug, which the caller
    /// reports (E15).
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<StoreLockError> for BootstrapDirectoryError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Bug(bug) => Self::Bug(bug),
            error => Self::Lock(error),
        }
    }
}

impl From<FileError> for BootstrapDirectoryError {
    fn from(error: FileError) -> Self {
        match error {
            FileError::Bug(bug) => Self::Bug(bug),
            error => Self::File(error),
        }
    }
}

impl crate::task::CarriesBug for BootstrapDirectoryError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::File(error) => error.bug(),
            Self::Create(creation) => creation.bug(),
            Self::Lock(error) => error.bug(),
        }
    }
}

impl BootstrapDirectoryError {
    /// The protected file the lock refused, when the device is locked and
    /// the store's chosen class keeps its files closed (api.md E1).
    pub fn device_locked(&self) -> Option<&std::path::Path> {
        let file = match self {
            Self::File(file) => file,
            Self::Create(creation) => creation.file()?,
            Self::Lock(_) | Self::Bug(_) => return None,
        };
        match file {
            FileError::DeviceLocked { path } => Some(path),
            _ => None,
        }
    }

    /// The store is already visible, and only syncing its parent directory
    /// failed.
    pub fn published_unsynced(&self) -> bool {
        matches!(self, Self::Create(DirectoryCreationError::Published { .. }))
    }
}

/// An unpublished store at its permanent path, protected by a bootstrap lease.
/// Dropping preserves resumable work; `cancel` removes it after its database
/// closes. Publication removes the marker without moving any open files.
pub struct BootstrapStore {
    // Drops before the lease: the store's locks are closing before the
    // bootstrap lock is released, so a call resuming the store waits for a
    // writer the bootstrap's database may still hold.
    _closing: lock::StoreClosingGuard,
    path: PathBuf,
    id: StoreId,
    lease: BootstrapLease,
    protection: Protection,
}

/// The bootstrap lock, which the bootstrap or replacement that took it holds
/// alone until it publishes, cancels or is dropped. Its directories name the
/// lock's instance and open the unpublished store only while it holds it.
#[derive(Debug)]
pub(super) struct BootstrapLease {
    held: Mutex<Option<lock::RegisteredLock>>,
    instance: lock::LockInstance,
}

impl BootstrapLease {
    pub(super) fn new(lock: lock::RegisteredLock) -> Self {
        Self { instance: lock.instance(), held: Mutex::new(Some(lock)) }
    }

    pub(super) fn instance(&self) -> lock::LockInstance {
        self.instance
    }

    pub(super) fn release(&self) {
        lock_unpoisoned(&self.held).take();
    }
}

impl BootstrapStore {
    /// The unpublished directory, which opens the store while this bootstrap
    /// holds its lock.
    pub fn directory(&self) -> StoreDir {
        StoreDir::bootstrapping(self.path.clone(), self.id, self.lease.instance(), self.protection)
    }

    /// Publish the loaded, open store by removing its durable marker.
    /// After a `Create(Published { .. })` error it is already visible.
    pub fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        let _reader = self.directory().lock_read_only()?;
        let published = |source: io::Error| -> BootstrapDirectoryError {
            DirectoryCreationError::Published { id: self.id, source }.into()
        };
        let result = remove_marker(&self.path, published);
        self.lease.release();
        result
    }

    /// Whether the store is published: its marker is gone. After a panic
    /// in a publication, this says how far it got.
    pub fn is_published(&self) -> Result<bool, FileError> {
        Ok(!is_pending(&self.path)?)
    }

    /// Remove unpublished data and locks after all database work has closed.
    /// An absent directory succeeds; a published store cannot be cancelled.
    pub fn cancel(&self) -> Result<(), BootstrapDirectoryError> {
        let mut held = lock_unpoisoned(&self.lease.held);
        let guard = self.lock_for_deletion(&mut held)?;
        if exists("inspect bootstrap path", &self.path)? && !is_pending(&self.path)? {
            return Err(DirectoryCreationError::AlreadyExists(self.id).into());
        }
        if let Some(guard) = guard {
            guard.remove_directory()?;
        }
        held.take();
        Ok(())
    }

    /// Lock the store for deletion, handing the deletion the bootstrap lock
    /// `held` while this bootstrap holds it. Once it has published, the
    /// deletion takes the lock itself, as any deletion does.
    fn lock_for_deletion(
        &self,
        held: &mut Option<lock::RegisteredLock>,
    ) -> Result<Option<super::StoreDeletionLock>, StoreLockError> {
        if held.is_some() {
            lock::for_deletion(&self.path, self.id, Some(held))
        } else {
            self.directory().lock_for_deletion()
        }
    }
}

/// The file whose presence keeps a store unpublished.
pub(super) const MARKER: &str = ".coven-bootstrap";

pub(super) fn marker(directory: &Path) -> PathBuf {
    directory.join(MARKER)
}

/// Remove the marker; only a failure syncing its directory leaves the store
/// visible, which `published` reports.
pub(super) fn remove_marker(
    directory: &Path,
    published: impl Fn(io::Error) -> BootstrapDirectoryError,
) -> Result<(), BootstrapDirectoryError> {
    match super::atomic_file::remove(&marker(directory)) {
        // Directory removal retains the IO error from syncing its parent.
        Ok(()) => Ok(()),
        Err(FileError::AfterRemove { source, .. }) => Err(published(
            *source.downcast::<io::Error>().expect("directory removal retains its IO cause"),
        )),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn is_pending(directory: &Path) -> Result<bool, FileError> {
    exists("inspect bootstrap path", &marker(directory))
}

pub(super) async fn reserve_bootstrap_directory(
    root: &Path,
    id: StoreId,
    ids: &dyn IdSource,
    protection: Protection,
) -> Result<BootstrapStore, BootstrapDirectoryError> {
    creation::create_directory(root)
        .map_err(|source| FileError::at("create bootstrap layout", root, source))?;
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve bootstrap layout", root, source))?;
    let path = root.join(id.to_string());
    let lock = lock::acquire_bootstrap(&path, id).await?;
    let closing = lock.closing_guard();
    let lease = BootstrapLease::new(lock);
    if !exists("inspect bootstrap path", &path)? {
        creation::create(&root, id, ids, true, protection)?;
    }
    let metadata = fs::symlink_metadata(&path)
        .map_err(|source| FileError::at("inspect unpublished store", &path, source))?;
    if !metadata.is_dir() {
        return Err(FileError::at(
            "inspect unpublished store",
            &path,
            io::Error::new(io::ErrorKind::InvalidInput, "unpublished store is not a directory"),
        )
        .into());
    }
    // A published or interrupted installation may still have a live writer,
    // including one in another process. Refuse it before inspecting its data.
    let _writer = lock::acquire_for_open(&path, id).await?;
    // A store in the middle of a swap is finished by the call that began it.
    if let Some(record) =
        device_reset::unfinished(&path, id).map_err(DirectoryCreationError::Settings)?
    {
        return Err(StoreLockError::ReplacementUnfinished { store: id, by: record.by }.into());
    }
    if !is_pending(&path)? {
        return Err(DirectoryCreationError::AlreadyExists(id).into());
    }
    // A resumed installation may have been made with another class (E1).
    protection.apply(&path)?;
    let store = BootstrapStore { _closing: closing, path, id, lease, protection };
    store.directory().settings().map_err(DirectoryCreationError::Settings)?;
    Ok(store)
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
