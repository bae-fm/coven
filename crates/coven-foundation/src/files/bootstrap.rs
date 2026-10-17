//! Hidden stores retain interrupted bootstrap work until explicit retry or cancellation.

use super::{
    atomic_file::exists, creation, device_reset, lock, DeviceReset, DirectoryCreationError,
    FileError, StoreDir, StoreLockError,
};
use crate::id_source::{IdSource, StoreId};
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// Filesystem failures while reserving, publishing or cancelling a new device.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapDirectoryError {
    /// A store already exists or preparing its directory failed.
    #[error(transparent)]
    Create(#[from] DirectoryCreationError),
    /// Another bootstrap or an open database prevents this operation.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
    /// A filesystem operation failed.
    #[error(transparent)]
    File(#[from] FileError),
}

/// An unpublished store at its permanent path, protected by a bootstrap lease.
/// Dropping preserves resumable work; `cancel` removes it after its database
/// closes. Publication removes the marker without moving any open files.
pub struct BootstrapStore {
    // Drops before the lease so surviving directory clones are closing.
    _closing: lock::StoreClosingGuard,
    path: PathBuf,
    id: StoreId,
    lease: Arc<BootstrapLease>,
}

/// Unpublished directory clones keep the lease until publication or cancellation.
#[derive(Debug)]
pub(super) struct BootstrapLease(Mutex<Option<lock::RegisteredLock>>);

impl BootstrapLease {
    pub(super) fn is_held(&self) -> bool {
        self.0
            .lock()
            .expect("bootstrap lease lock poisoned")
            .is_some()
    }

    fn release(&self) {
        self.0.lock().expect("bootstrap lease lock poisoned").take();
    }
}

impl BootstrapStore {
    /// The unpublished directory; its capability retains the bootstrap lease.
    pub fn directory(&self) -> StoreDir {
        StoreDir::bootstrapping(self.path.clone(), self.id, self.lease.clone())
    }

    /// The replacement this bootstrap completes, if it resets an installation.
    pub fn device_reset(&self) -> Result<Option<DeviceReset>, FileError> {
        device_reset::read(&self.path)
    }

    /// The discarded installation's database, which beginning this reset moved
    /// aside unchanged (§10). Only its operation journal is read, in place.
    pub fn discarded_database_path(&self, reset: &DeviceReset) -> std::path::PathBuf {
        device_reset::discarded_database(&self.path, reset.old_device)
    }

    /// Publish the loaded, open store by removing its durable marker, then any
    /// reset record: the replacement database already holds its notice.
    /// After a `Create(Published { .. })` error it is already visible.
    pub fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        let _reader = self.directory().lock_read_only()?;
        let published = |source: io::Error| -> BootstrapDirectoryError {
            DirectoryCreationError::Published {
                id: self.id,
                source,
            }
            .into()
        };
        let result = match super::atomic_file::remove(&marker(&self.path)) {
            // Directory removal retains the IO error from syncing its parent.
            Ok(()) => Ok(()),
            Err(FileError::AfterRemove { source, .. }) => Err(published(
                *source
                    .downcast::<io::Error>()
                    .expect("directory removal retains its IO cause"),
            )),
            Err(error) => return Err(error.into()),
        };
        self.lease.release();
        result?;
        device_reset::finish(&self.path).map_err(|error| published(io::Error::other(error)))
    }

    /// Remove unpublished data and locks after all database work has closed.
    /// An absent directory succeeds; a published store cannot be cancelled.
    pub fn cancel(&self) -> Result<(), BootstrapDirectoryError> {
        let guard = self.directory().lock_for_deletion()?;
        if exists("inspect bootstrap path", &self.path)? && !is_pending(&self.path)? {
            return Err(DirectoryCreationError::AlreadyExists(self.id).into());
        }
        if let Some(guard) = guard {
            guard.remove_directory()?;
        }
        self.lease.release();
        Ok(())
    }
}

pub(super) fn marker(directory: &Path) -> PathBuf {
    directory.join(".coven-bootstrap")
}

pub(super) fn is_pending(directory: &Path) -> Result<bool, FileError> {
    exists("inspect bootstrap path", &marker(directory))
}

pub(super) async fn reserve_bootstrap_directory(
    root: &Path,
    id: StoreId,
    name: &str,
    ids: &dyn IdSource,
) -> Result<BootstrapStore, BootstrapDirectoryError> {
    creation::create_directory_tree(root)
        .map_err(|source| FileError::at("create bootstrap layout", root, source))?;
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve bootstrap layout", root, source))?;
    let path = root.join(id.to_string());
    let lock = lock::acquire_bootstrap(&path, id).await?;
    let closing = lock.closing_guard();
    let lease = Arc::new(BootstrapLease(Mutex::new(Some(lock))));
    if !exists("inspect bootstrap path", &path)? {
        creation::create(&root, id, name, ids, true)?;
    }
    let metadata = fs::symlink_metadata(&path)
        .map_err(|source| FileError::at("inspect unpublished store", &path, source))?;
    if !metadata.is_dir() {
        return Err(FileError::at(
            "inspect unpublished store",
            &path,
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "unpublished store is not a directory",
            ),
        )
        .into());
    }
    // A published or interrupted installation may still have a live writer,
    // including one in another process. Refuse it before inspecting its data.
    let _writer = lock::acquire_for_open(&path, id).await?;
    // A recorded reset continues by moving the discarded copy aside (§10).
    if device_reset::read(&path)?.is_some() {
        device_reset::begin(&path, id)?;
    }
    if !is_pending(&path)? {
        return Err(DirectoryCreationError::AlreadyExists(id).into());
    }
    let store = BootstrapStore {
        _closing: closing,
        path,
        id,
        lease,
    };
    store
        .directory()
        .settings()
        .map_err(DirectoryCreationError::Settings)?;
    Ok(store)
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
