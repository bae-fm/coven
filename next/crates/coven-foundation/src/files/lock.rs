//! Store locks live beside the directory so Windows can unpublish it.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::files::{atomic_file, settings, FileError, SettingsError, StoreSettings};
use crate::id_source::{DeviceId, StoreId};

/// An exclusive writer lock. Dropping it unlocks and closes the OS handle.
/// Readers use a separate shared lock so they can coexist with the writer.
#[derive(Debug)]
pub struct StoreLock {
    store: LockedStore,
}

#[derive(Debug)]
struct LockedStore {
    _file: LockedFile,
    directory: PathBuf,
    id: StoreId,
}

impl StoreLock {
    /// Read the settings while holding the store's writer lock.
    pub fn settings(&self) -> Result<StoreSettings, SettingsError> {
        settings::read(&self.store.directory, self.store.id)
    }

    /// Replace a restored installation's device id before it can write (§10).
    pub fn set_device_id(&self, device: DeviceId) -> Result<(), SettingsError> {
        let mut settings = self.settings()?;
        settings.device_id = device;
        settings::write(&self.store.directory, &settings)
    }

    pub(crate) fn verify(&self, directory: &Path, id: StoreId) -> Result<(), StoreLockError> {
        let path = std::fs::canonicalize(directory)
            .map_err(|source| FileError::at("resolve locked store", directory, source))?;
        if path != self.store.directory || id != self.store.id {
            return Err(StoreLockError::WrongDirectory(id));
        }
        Ok(())
    }
}

/// A shared lock that prevents deletion while a reader holds store files open.
#[derive(Debug)]
pub struct StoreReadLock {
    _file: LockedFile,
}

/// Exclusive access to both the writer and readers, authorizing deletion.
#[derive(Debug)]
pub struct StoreDeletionLock {
    store: LockedStore,
    _readers: LockedFile,
}

impl StoreDeletionLock {
    /// Unpublish the directory and remove its contents before releasing and
    /// removing its sibling locks. Retry failures through `lock_for_deletion`,
    /// including a failure after only the lock files remain.
    pub fn remove_directory(mut self) -> Result<(), FileError> {
        let root = self.store.directory.parent().expect("store parent");
        // Opening and creation also take this lock. Neither can reuse a lock
        // file between releasing its OS lock and unlinking its name.
        let _layout = lock_layout(root)?;
        let paths = lock_paths(&self.store.directory, self.store.id);
        let destination = deletion_path(&self.store.directory, self.store.id);
        if inspect_directory(&self.store.directory)? {
            if self.store.directory != destination {
                crate::files::atomic_file::rename_new(&self.store.directory, &destination)
                    .map_err(|source| {
                        FileError::at("unpublish store", &self.store.directory, source)
                    })?;
                self.store.directory = destination;
            }
            #[cfg(unix)]
            sync_removal(&self.store.directory)?;
            std::fs::remove_dir_all(&self.store.directory).map_err(|source| {
                FileError::at("remove store directory", &self.store.directory, source)
            })?;
        }
        #[cfg(unix)]
        sync_removal(&self.store.directory)?;
        // Windows must see closed handles before their lock files are removed.
        drop(self);
        for path in paths {
            atomic_file::remove(&path)?;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn sync_removal(directory: &Path) -> Result<(), FileError> {
    crate::files::atomic_file::sync_directory(directory.parent().expect("store parent")).map_err(
        |source| FileError::AfterRemove {
            path: directory.to_owned(),
            source,
        },
    )
}

/// A store in use is distinct from a filesystem failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreLockError {
    /// An open writer, reader or deletion prevents the requested access.
    #[error("store {0} is already open")]
    AlreadyOpen(StoreId),
    /// An explicit database recovery has not published its replacement.
    #[error("store {0} requires open_reloading to finish database recovery")]
    RecoveryPending(StoreId),
    /// A supplied lock protects a different directory.
    #[error("lock does not protect store {0}")]
    WrongDirectory(StoreId),
    /// Opening or locking the lock file failed for another reason.
    #[error("store lock: {0}")]
    File(#[from] FileError),
}

#[derive(Debug)]
pub(crate) struct LockedFile {
    file: File,
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        // Closing alone leaves the lock held while a concurrently forked child
        // retains the open file before exec closes its inherited descriptors.
        self.file.unlock().expect("release owned store lock");
    }
}

fn open_lock(path: &Path) -> Result<File, FileError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|source| FileError::at("open store lock", path, source))
}

pub(crate) fn try_lock(
    path: &Path,
    id: StoreId,
    shared: bool,
) -> Result<LockedFile, StoreLockError> {
    let file = open_lock(path)?;
    let result = if shared {
        file.try_lock_shared()
    } else {
        file.try_lock()
    };
    match result {
        Ok(()) => Ok(LockedFile { file }),
        Err(TryLockError::WouldBlock) => Err(StoreLockError::AlreadyOpen(id)),
        Err(TryLockError::Error(source)) => Err(FileError::at("lock store", path, source).into()),
    }
}

// This file belongs to the layout and is never removed. It serializes lock
// acquisition, creation, and lock-file removal across processes, so no caller
// can hold an unlinked lock while another locks a new file at the same path.
pub(crate) fn lock_layout(root: &Path) -> Result<LockedFile, FileError> {
    let path = root.join(".coven-stores.lock");
    let file = open_lock(&path)?;
    file.lock()
        .map_err(|source| FileError::at("lock store layout", &path, source))?;
    Ok(LockedFile { file })
}

pub(crate) fn acquire(directory: &Path, id: StoreId) -> Result<StoreLock, StoreLockError> {
    let (directory, file) = lock_existing(directory, id, false)?;
    Ok(StoreLock {
        store: LockedStore {
            _file: file,
            directory,
            id,
        },
    })
}

pub(super) fn exclude_readers(directory: &Path, id: StoreId) -> Result<LockedFile, StoreLockError> {
    let _layout = lock_layout(directory.parent().expect("store parent"))?;
    try_lock(&lock_paths(directory, id)[1], id, false)
}

pub(crate) fn acquire_reader(
    directory: &Path,
    id: StoreId,
) -> Result<StoreReadLock, StoreLockError> {
    let (_, file) = lock_existing(directory, id, true)?;
    Ok(StoreReadLock { _file: file })
}

fn lock_existing(
    directory: &Path,
    id: StoreId,
    shared: bool,
) -> Result<(PathBuf, LockedFile), StoreLockError> {
    let directory = std::fs::canonicalize(directory)
        .map_err(|source| FileError::at("resolve store directory", directory, source))?;
    let _layout = lock_layout(directory.parent().expect("store parent"))?;
    // Deletion may have completed between canonicalization and the layout lock.
    std::fs::canonicalize(&directory)
        .map_err(|source| FileError::at("resolve store directory", &directory, source))?;
    let [writer, readers] = lock_paths(&directory, id);
    let path = if shared { readers } else { writer };
    let file = try_lock(&path, id, shared)?;
    Ok((directory, file))
}

pub(crate) fn lock_paths(directory: &Path, id: StoreId) -> [PathBuf; 2] {
    [
        directory.with_file_name(format!(".{id}.lock")),
        directory.with_file_name(format!(".{id}.readers.lock")),
    ]
}

pub(crate) fn deletion_path(directory: &Path, id: StoreId) -> PathBuf {
    directory.with_file_name(format!(".coven-delete-{id}"))
}

fn inspect_directory(directory: &Path) -> Result<bool, FileError> {
    match std::fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(FileError::at(
            "inspect deletion directory",
            directory,
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "store is not a directory"),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path = %directory.display(), "store directory is absent");
            Ok(false)
        }
        Err(source) => Err(FileError::at("inspect store directory", directory, source)),
    }
}

pub(crate) fn for_deletion(
    directory: &Path,
    id: StoreId,
) -> Result<Option<StoreDeletionLock>, StoreLockError> {
    let root = directory.parent().expect("store parent");
    let root = match std::fs::canonicalize(root) {
        Ok(root) => root,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path = %root.display(), "store layout is absent");
            return Ok(None);
        }
        Err(source) => return Err(FileError::at("resolve store layout", root, source).into()),
    };
    let _layout = lock_layout(&root)?;
    let published = root.join(id.to_string());
    let directory = if inspect_directory(&published)? {
        published
    } else {
        deletion_path(&published, id)
    };
    let paths = lock_paths(&directory, id);
    if !inspect_directory(&directory)? {
        let mut remaining = false;
        for path in &paths {
            match std::fs::symlink_metadata(path) {
                Ok(_) => remaining = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(FileError::at("inspect store lock", path, source).into()),
            }
        }
        if !remaining {
            // Retry the durability barrier even if the preceding attempt
            // removed the last lock file before its directory sync failed.
            #[cfg(unix)]
            sync_removal(&directory)?;
            tracing::debug!(store = %id, "store and its locks are absent");
            return Ok(None);
        }
    }
    let [writer, readers] = paths;
    Ok(Some(StoreDeletionLock {
        store: LockedStore {
            _file: try_lock(&writer, id, false)?,
            directory,
            id,
        },
        _readers: try_lock(&readers, id, false)?,
    }))
}

/// Entries in a stores directory besides the layout lock, for tests.
#[cfg(test)]
pub(crate) fn store_entries(root: &Path) -> usize {
    std::fs::read_dir(root)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().file_name() != ".coven-stores.lock")
        .count()
}

#[cfg(test)]
#[path = "lock_tests.rs"]
mod tests;
