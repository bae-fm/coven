//! Publish a complete store directory for one new device.

use std::fs;
use std::io;
use std::path::Path;

use crate::files::{directory, lock, FileError, SettingsError, StoreDir, StoreSettings};
use crate::id_source::{IdSource, StoreId};

/// A store creation failure, with publication and rollback failures explicit.
#[derive(Debug)]
pub enum StoreCreationError<E = std::convert::Infallible> {
    /// Keeping state outside the directory failed before publication. The id
    /// lets the caller remove any externally retained state before retrying.
    Initialization {
        /// The unpublished store.
        id: StoreId,
        /// The initializer's typed failure.
        source: E,
    },
    /// Removing externally initialized state after an unpublished failure also failed.
    InitializationCleanup {
        /// The original creation failure.
        operation: Box<StoreCreationError<E>>,
        /// The external cleanup failure.
        cleanup: E,
    },
    /// A directory or file already occupies this store's id. It is untouched.
    AlreadyExists(StoreId),
    /// An operation failed before publication.
    File(FileError),
    /// Writing the new device's settings failed before publication.
    Settings(SettingsError),
    /// The entire store is visible, but the parent directory could not be
    /// synced. The caller can identify and reopen the published store.
    Published {
        /// The store that is already visible.
        id: StoreId,
        /// The durability error.
        source: io::Error,
    },
    /// Removing the unpublished directory also failed.
    Rollback {
        /// The original creation failure.
        operation: Box<StoreCreationError<E>>,
        /// The error removing the directory.
        cleanup: io::Error,
    },
}

// Deriving recursive generic error bounds would require Self: Error while
// proving that same implementation. Keep the bound on the initializer's cause.
impl<E: std::fmt::Display> std::fmt::Display for StoreCreationError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialization { id, source } => {
                write!(f, "initializing store {id} failed: {source}")
            }
            Self::InitializationCleanup { operation, cleanup } => write!(
                f,
                "{operation}; removing external initialization failed: {cleanup}"
            ),
            Self::AlreadyExists(id) => write!(f, "store {id} already exists on this device"),
            Self::File(error) => write!(f, "create store: {error}"),
            Self::Settings(error) => write!(f, "create store settings: {error}"),
            Self::Published { id, source } => write!(
                f,
                "store {id} was published, but its directory could not be synced: {source}"
            ),
            Self::Rollback { operation, cleanup } => write!(
                f,
                "{operation}; removing unpublished directory failed: {cleanup}"
            ),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for StoreCreationError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Initialization { source, .. } => Some(source),
            Self::InitializationCleanup { cleanup, .. } => Some(cleanup),
            Self::AlreadyExists(_) => None,
            Self::File(error) => Some(error),
            Self::Settings(error) => Some(error),
            Self::Published { source, .. } => Some(source),
            Self::Rollback { operation, .. } => Some(operation.as_ref()),
        }
    }
}
impl<E> From<FileError> for StoreCreationError<E> {
    fn from(error: FileError) -> Self {
        Self::File(error)
    }
}
impl<E> From<SettingsError> for StoreCreationError<E> {
    fn from(error: SettingsError) -> Self {
        Self::Settings(error)
    }
}

pub(crate) fn create<E: std::error::Error + Send + Sync + 'static>(
    root: &Path,
    id: StoreId,
    name: &str,
    ids: &dyn IdSource,
    initialize: impl FnOnce(&StoreSettings) -> Result<(), E>,
) -> Result<StoreDir, StoreCreationError<E>> {
    create_directory_tree(root)
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
        match fs::symlink_metadata(&path) {
            Ok(_) => return Err(StoreCreationError::AlreadyExists(id)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(FileError::at("inspect store destination", &path, source).into())
            }
        }
    }
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
            initialize(&settings)
                .map_err(|source| StoreCreationError::Initialization { id, source })
        })
        .and_then(|()| {
            crate::files::atomic_file::rename_new(stage.path(), &destination).map_err(|source| {
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
pub(crate) fn create_directory_tree(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)
}

#[cfg(unix)]
pub(crate) fn create_directory_tree(path: &Path) -> io::Result<()> {
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

#[cfg(test)]
#[path = "creation_tests.rs"]
mod tests;
