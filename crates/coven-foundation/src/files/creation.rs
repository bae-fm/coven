//! Publish a complete store directory for one new device.

use std::fs;
use std::io;
use std::path::Path;

use crate::files::protection::Protection;
use crate::files::{directory, lock, FileError, SettingsError, StoreDir, StoreSettings};
use crate::id_source::{IdSource, StoreId};

/// A store creation failure, with publication and rollback failures explicit.
#[derive(Debug, thiserror::Error)]
pub enum DirectoryCreationError {
    /// A directory or file already occupies this store's id. It is untouched.
    #[error("store {0} already exists on this device")]
    AlreadyExists(StoreId),
    /// An operation failed before publication.
    #[error("create store: {0}")]
    File(#[from] FileError),
    /// Writing the new device's settings failed before publication.
    #[error("create store settings: {0}")]
    Settings(#[from] SettingsError),
    /// The entire store is visible, but syncing its parent failed.
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
        operation: Box<DirectoryCreationError>,
        /// The error removing the directory.
        cleanup: io::Error,
    },
}

pub(crate) fn create(
    root: &Path,
    id: StoreId,
    ids: &dyn IdSource,
    unpublished: bool,
    protection: Protection,
) -> Result<StoreDir, DirectoryCreationError> {
    create_directory(root)
        .map_err(|source| FileError::at("create stores directory", root, source))?;
    let root = fs::canonicalize(root)
        .map_err(|source| FileError::at("resolve stores directory", root, source))?;
    let _layout = lock::lock_layout(&root)?;
    let destination = root.join(id.to_string());
    // A deletion must finish before the id can be reused, including removal
    // of its locks. Otherwise cleanup could unlink the replacement's lock.
    for path in [destination.clone(), lock::deletion_path(&destination, id)]
        .into_iter()
        .chain(lock::lock_paths(&destination, id))
    {
        if super::atomic_file::exists("inspect store destination", &path)? {
            return Err(DirectoryCreationError::AlreadyExists(id));
        }
    }
    let stage = tempfile::Builder::new()
        .prefix(".coven-create-")
        .tempdir_in(&root)
        .map_err(|source| FileError::at("create unpublished store directory", &root, source))?;
    let settings = StoreSettings {
        id,
        device_id: ids.new_device_id(),
    };
    // The stage takes the class before anything is made in it (E1).
    let prepare = protection
        .begin(stage.path())
        .map_err(DirectoryCreationError::from)
        .and_then(|()| {
            directory::initialize(stage.path(), &settings).map_err(DirectoryCreationError::from)
        })
        .and_then(|()| {
            if unpublished {
                super::atomic_file::replace(&super::bootstrap::marker(stage.path()), b"")?;
            }
            Ok(())
        })
        .and_then(|()| {
            crate::files::atomic_file::rename_new(stage.path(), &destination).map_err(|source| {
                if source.kind() == io::ErrorKind::AlreadyExists {
                    DirectoryCreationError::AlreadyExists(id)
                } else {
                    FileError::at("publish store directory", &destination, source).into()
                }
            })
        });
    if let Err(operation) = prepare {
        return match stage.close() {
            Ok(()) => Err(operation),
            Err(cleanup) => Err(DirectoryCreationError::Rollback {
                operation: Box::new(operation),
                cleanup,
            }),
        };
    }
    // The stage's old name is absent; disarm its destructor before a new
    // temporary sibling could ever reuse that name.
    let _unpublished_path = stage.keep();
    #[cfg(unix)]
    crate::files::atomic_file::sync_directory(&root).map_err(|source| {
        if unpublished {
            // The marker still hides this directory; it has not published a store.
            FileError::at("sync unpublished store directory", &root, source).into()
        } else {
            DirectoryCreationError::Published { id, source }
        }
    })?;
    Ok(StoreDir::new(destination, id, protection))
}

/// Create `path` unless it is already a directory. Its parent must exist:
/// coven creates directories only inside the app directory, one level at a
/// time, and never opens anything above it. Android refuses an app access to
/// the directories above its own.
#[cfg(windows)]
pub(crate) fn create_directory(path: &Path) -> io::Result<()> {
    make_directory(path)
}

/// Create `path` unless it is already a directory, then sync its parent so
/// the entry is durable. Its parent must exist: coven creates directories
/// only inside the app directory, one level at a time, and never opens
/// anything above it. Android refuses an app access to the directories above
/// its own.
#[cfg(unix)]
pub(crate) fn create_directory(path: &Path) -> io::Result<()> {
    create_directory_with_sync(path, &crate::files::atomic_file::sync_directory)
}

#[cfg(unix)]
fn create_directory_with_sync(
    path: &Path,
    sync: &impl Fn(&Path) -> io::Result<()>,
) -> io::Result<()> {
    make_directory(path)?;
    // Existence does not prove durability: an earlier attempt may have made
    // the directory and failed before syncing its parent. Repeat the barrier
    // on retry, including when another creator made it concurrently.
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    sync(parent)
}

fn make_directory(path: &Path) -> io::Result<()> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if fs::metadata(path)?.is_dir() {
                Ok(())
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[path = "creation_tests.rs"]
mod tests;
