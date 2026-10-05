//! Publish a complete store directory for one new device.

use std::fs;
use std::io;
use std::path::Path;

use crate::files::{directory, FileError, SettingsError, StoreDir, StoreSettings};
use crate::id_source::{IdSource, StoreId};

/// A store creation failure, with publication and rollback failures explicit.
#[derive(Debug, thiserror::Error)]
pub enum StoreCreationError {
    /// A directory or file already occupies this store's id. It is untouched.
    #[error("store {0} already exists on this device")]
    AlreadyExists(StoreId),
    /// An operation failed before publication.
    #[error("create store: {0}")]
    File(#[from] FileError),
    /// Writing the new device's settings failed before publication.
    #[error("create store settings: {0}")]
    Settings(#[from] SettingsError),
    /// The entire store is visible, but the parent directory could not be
    /// synced. The caller can identify and reopen the published store.
    #[error("store {id} was published, but its directory could not be synced: {source}")]
    Published {
        /// The store that is already visible.
        id: StoreId,
        /// The durability error.
        #[source]
        source: io::Error,
    },
    /// Removing the unpublished directory also failed.
    #[error("{operation}; removing unpublished directory failed: {cleanup}")]
    Rollback {
        /// The original creation failure.
        #[source]
        operation: Box<StoreCreationError>,
        /// The error removing the directory.
        cleanup: io::Error,
    },
}

pub(crate) fn create(
    root: &Path,
    id: StoreId,
    name: &str,
    ids: &dyn IdSource,
) -> Result<StoreDir, StoreCreationError> {
    create_directory_tree(root)
        .map_err(|source| FileError::at("create stores directory", root, source))?;
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve stores directory", root, source))?;
    let destination = root.join(id.to_string());
    let stage = tempfile::Builder::new()
        .prefix(".coven-create-")
        .tempdir_in(&root)
        .map_err(|source| FileError::at("create unpublished store directory", &root, source))?;
    let settings = StoreSettings {
        id,
        name: name.to_owned(),
        device_id: ids.new_device_id(),
    };
    let prepare = directory::initialize(stage.path(), &settings)
        .map_err(StoreCreationError::from)
        .and_then(|()| {
            rename_new_directory(stage.path(), &destination).map_err(|source| {
                if source.kind() == io::ErrorKind::AlreadyExists {
                    StoreCreationError::AlreadyExists(id)
                } else {
                    FileError::at("publish store directory", &destination, source).into()
                }
            })
        });
    if let Err(operation) = prepare {
        return match stage.close() {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(StoreCreationError::Rollback {
                operation: Box::new(operation),
                cleanup,
            }),
        };
    }
    // The stage's old name is absent; disarm its destructor before a new
    // temporary sibling could ever reuse that name.
    let _unpublished_path = stage.keep();
    #[cfg(unix)]
    crate::files::atomic_file::sync_directory(&root)
        .map_err(|source| StoreCreationError::Published { id, source })?;
    Ok(StoreDir::new(destination, id))
}

#[cfg(windows)]
fn create_directory_tree(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)
}

#[cfg(unix)]
fn create_directory_tree(path: &Path) -> io::Result<()> {
    create_directory_tree_with_sync(path, &crate::files::atomic_file::sync_directory)
}

#[cfg(unix)]
fn create_directory_tree_with_sync(
    path: &Path,
    sync: &impl Fn(&Path) -> io::Result<()>,
) -> io::Result<()> {
    fs::create_dir_all(path)?;
    // Existence does not prove durability: an earlier attempt may have made
    // any ancestor and failed before syncing its parent. Repeat each barrier
    // on retry, including directories another creator made concurrently.
    for ancestor in path
        .ancestors()
        .filter(|ancestor| !ancestor.as_os_str().is_empty())
    {
        sync(ancestor)?;
    }
    if path.is_relative() {
        sync(Path::new("."))?;
    }
    Ok(())
}

#[cfg(unix)]
fn rename_new_directory(from: &Path, to: &Path) -> io::Result<()> {
    use rustix::fs::{renameat_with, RenameFlags, CWD};
    renameat_with(CWD, from, CWD, to, RenameFlags::NOREPLACE).map_err(Into::into)
}

#[cfg(windows)]
fn rename_new_directory(from: &Path, to: &Path) -> io::Result<()> {
    crate::files::atomic_file::windows_rename(from, to, false)
}

#[cfg(test)]
#[path = "creation_tests.rs"]
mod tests;
