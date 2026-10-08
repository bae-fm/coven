//! Hidden stores retain interrupted bootstrap work until explicit retry or cancellation.

use super::{
    atomic_file::exists, creation, lock, FileError, StoreCreationError, StoreDir, StoreLockError,
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
    Create(#[from] StoreCreationError),
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
    path: PathBuf,
    id: StoreId,
    lease: Arc<BootstrapLease>,
}

/// Unpublished directory clones keep the lease until publication or cancellation.
#[derive(Debug)]
pub(super) struct BootstrapLease(Mutex<Option<lock::LockedFile>>);

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

    /// Publish the loaded, open store by removing its durable marker.
    /// After a `Create(Published { .. })` error it is already visible.
    pub fn publish(&self) -> Result<(), BootstrapDirectoryError> {
        let _reader = self.directory().lock_read_only()?;
        let result = match super::atomic_file::remove(&marker(&self.path)) {
            Ok(()) => Ok(()),
            Err(FileError::AfterRemove { source, .. }) => Err(StoreCreationError::Published {
                id: self.id,
                source,
            }
            .into()),
            Err(error) => return Err(error.into()),
        };
        self.lease.release();
        result
    }

    /// Remove unpublished data and locks after all database work has closed.
    /// An absent directory succeeds; a published store cannot be cancelled.
    pub fn cancel(&self) -> Result<(), BootstrapDirectoryError> {
        let guard = self.directory().lock_for_deletion()?;
        if exists("inspect bootstrap path", &self.path)? && !is_pending(&self.path)? {
            return Err(StoreCreationError::AlreadyExists(self.id).into());
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

pub(super) fn reserve_bootstrap_directory(
    root: &Path,
    id: StoreId,
    name: &str,
    ids: &dyn IdSource,
) -> Result<BootstrapStore, BootstrapDirectoryError> {
    creation::create_directory_tree(root)
        .map_err(|source| FileError::at("create bootstrap layout", root, source))?;
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve bootstrap layout", root, source))?;
    let lease = Arc::new(BootstrapLease(Mutex::new(Some(lock::try_lock(
        &root.join(format!(".coven-bootstrap-{id}.lock")),
        id,
        false,
    )?))));
    let path = root.join(id.to_string());
    if !exists("inspect bootstrap path", &path)? {
        creation::create(&root, id, name, ids, |_| Ok(()), true)?;
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
    if !is_pending(&path)? {
        return Err(StoreCreationError::AlreadyExists(id).into());
    }
    let store = BootstrapStore { path, id, lease };
    store
        .directory()
        .settings()
        .map_err(StoreCreationError::Settings)?;
    Ok(store)
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
