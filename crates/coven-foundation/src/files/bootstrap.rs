//! Hidden stores retain interrupted bootstrap work until explicit retry or cancellation.

use super::{
    atomic_file::exists, creation, device_reset, lock, DirectoryCreationError, FileError, StoreDir,
    StoreLockError,
};
use crate::files::StoreDirAccess;
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
    pub(super) fn new(lock: lock::RegisteredLock) -> Self {
        Self(Mutex::new(Some(lock)))
    }

    pub(super) fn is_held(&self) -> bool {
        self.0
            .lock()
            .expect("bootstrap lease lock poisoned")
            .is_some()
    }

    pub(super) fn release(&self) {
        self.0.lock().expect("bootstrap lease lock poisoned").take();
    }
}

impl BootstrapStore {
    /// The unpublished directory; its capability retains the bootstrap lease.
    pub fn directory(&self) -> StoreDir {
        StoreDir::bootstrapping(self.path.clone(), self.id, self.lease.clone())
    }

    /// Publish the loaded, open store by removing its durable marker.
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
        let result = remove_marker(&self.path, published);
        self.lease.release();
        result
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
            *source
                .downcast::<io::Error>()
                .expect("directory removal retains its IO cause"),
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
) -> Result<BootstrapStore, BootstrapDirectoryError> {
    creation::create_directory_tree(root)
        .map_err(|source| FileError::at("create bootstrap layout", root, source))?;
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve bootstrap layout", root, source))?;
    let path = root.join(id.to_string());
    let lock = lock::acquire_bootstrap(&path, id).await?;
    let closing = lock.closing_guard();
    let lease = Arc::new(BootstrapLease::new(lock));
    if !exists("inspect bootstrap path", &path)? {
        creation::create(&root, id, ids, true)?;
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
    // A store in the middle of a swap is finished by the call that began it.
    if device_reset::read(&path)?.is_some() {
        return Err(StoreLockError::BootstrapPending(id).into());
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
