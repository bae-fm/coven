//! Hidden stores retain interrupted bootstrap work until explicit retry or cancellation.

use super::{atomic_file, creation, lock, FileError, StoreCreationError, StoreDir, StoreLockError};
use crate::id_source::{IdSource, StoreId};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// Filesystem failures while reserving, publishing or cancelling a new device.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapDirectoryError {
    /// A store already exists or preparing its directory failed.
    #[error(transparent)]
    Create(#[from] StoreCreationError),
    /// Another bootstrap or an open staged database prevents this operation.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
    /// A filesystem operation failed.
    #[error(transparent)]
    File(#[from] FileError),
}

/// A per-store bootstrap lease and the paths of its unpublished and final stores.
/// Dropping preserves resumable work; `cancel` explicitly removes it. Callers
/// close every database and file stream before publishing or cancelling.
pub struct BootstrapStore {
    root: PathBuf,
    staged: PathBuf,
    id: StoreId,
    _lease: lock::LockedFile,
}

impl BootstrapStore {
    /// A scoped directory capability for composing the unpublished database.
    pub fn directory(&self) -> StoreDir {
        StoreDir::new(self.staged.clone(), self.id)
    }

    /// Publish the fully loaded directory without replacing an existing store.
    /// After a `Create(Published { .. })` error the final store is already visible.
    pub fn publish(&self) -> Result<StoreDir, BootstrapDirectoryError> {
        let destination = self.root.join(self.id.to_string());
        let _layout = lock::lock_layout(&self.root)?;
        let staged_root = self.staged.parent().expect("bootstrap parent");
        let _staged_layout = lock::lock_layout(staged_root)?;
        let paths = lock::lock_paths(&self.staged, self.id);
        let writer = lock::try_lock(&paths[0], self.id, false)?;
        let readers = lock::try_lock(&paths[1], self.id, false)?;
        // A deletion of a previous installation must finish removing its locks.
        for path in lock::lock_paths(&destination, self.id).into_iter().chain([
            destination.clone(),
            lock::deletion_path(&destination, self.id),
        ]) {
            if exists(&path)? {
                return Err(StoreCreationError::AlreadyExists(self.id).into());
            }
        }
        atomic_file::rename_new(&self.staged, &destination)
            .map_err(|source| FileError::at("publish restored store", &destination, source))?;
        drop((writer, readers));
        // From here every error identifies the published store. Its keys must
        // remain committed even if syncing or removing obsolete locks fails.
        let finish = || -> io::Result<()> {
            #[cfg(unix)]
            {
                atomic_file::sync_directory(&self.root)?;
                atomic_file::sync_directory(staged_root)?;
            }
            for path in paths {
                fs::remove_file(path)?;
            }
            #[cfg(unix)]
            atomic_file::sync_directory(staged_root)?;
            Ok(())
        };
        finish().map_err(|source| StoreCreationError::Published {
            id: self.id,
            source,
        })?;
        Ok(StoreDir::new(destination, self.id))
    }

    /// Remove unpublished data and its file locks. An absent directory succeeds.
    pub fn cancel(&self) -> Result<(), BootstrapDirectoryError> {
        if let Some(lock) = self.directory().lock_for_deletion()? {
            lock.remove_directory()?;
        }
        Ok(())
    }
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
    let lease = lock::try_lock(&root.join(format!(".coven-bootstrap-{id}.lock")), id, false)?;
    if exists(&root.join(id.to_string()))? {
        return Err(StoreCreationError::AlreadyExists(id).into());
    }
    let staged_root = root.join(".coven-bootstrap");
    let staged = staged_root.join(id.to_string());
    if !exists(&staged)? {
        creation::create(&staged_root, id, name, ids, |_| Ok(()))?;
    }
    let metadata = fs::symlink_metadata(&staged)
        .map_err(|source| FileError::at("inspect unpublished store", &staged, source))?;
    if !metadata.is_dir() {
        return Err(FileError::at(
            "inspect unpublished store",
            &staged,
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "unpublished store is not a directory",
            ),
        )
        .into());
    }
    let store = BootstrapStore {
        root,
        staged,
        id,
        _lease: lease,
    };
    // Fail loudly on an incomplete or mismatched directory instead of replacing it.
    store
        .directory()
        .settings()
        .map_err(StoreCreationError::Settings)?;
    Ok(store)
}

fn exists(path: &Path) -> Result<bool, FileError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(FileError::at("inspect bootstrap path", path, source)),
    }
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
