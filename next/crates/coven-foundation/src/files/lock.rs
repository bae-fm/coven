//! The exclusive OS lock held for the lifetime of a writable store.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::files::{settings, FileError, SettingsError, StoreSettings};
use crate::id_source::{DeviceId, StoreId};

/// An exclusive store lock. Dropping it unlocks and closes the OS handle.
/// Read-only opens take no lock, so they can coexist with a writer (§20.1).
#[derive(Debug)]
pub struct StoreLock {
    file: File,
    directory: PathBuf,
    id: StoreId,
}

impl StoreLock {
    /// Read the settings while holding the store's writer lock.
    pub fn settings(&self) -> Result<StoreSettings, SettingsError> {
        settings::read(&self.directory, self.id)
    }

    /// Replace a restored installation's device id before it can write (§10).
    pub fn set_device_id(&self, device: DeviceId) -> Result<(), SettingsError> {
        let mut settings = self.settings()?;
        settings.device_id = device;
        settings::write(&self.directory, &settings)
    }

    /// Unpublish the locked directory, then remove its contents. A failed
    /// removal can be retried through `StoreDir::lock_for_deletion`.
    pub fn remove_directory(mut self) -> Result<(), FileError> {
        let destination = deletion_path(&self.directory, self.id);
        if self.directory != destination {
            crate::files::creation::rename_new_directory(&self.directory, &destination)
                .map_err(|source| FileError::at("unpublish store", &self.directory, source))?;
            self.directory = destination;
        }
        #[cfg(unix)]
        crate::files::atomic_file::sync_directory(self.directory.parent().expect("store parent"))
            .map_err(|source| FileError::AfterRemove {
            path: self.directory.clone(),
            source,
        })?;
        std::fs::remove_dir_all(&self.directory)
            .map_err(|source| FileError::at("remove store directory", &self.directory, source))?;
        #[cfg(unix)]
        crate::files::atomic_file::sync_directory(self.directory.parent().expect("store parent"))
            .map_err(|source| FileError::AfterRemove {
            path: self.directory.clone(),
            source,
        })?;
        Ok(())
    }

    pub(crate) fn verify(&self, directory: &Path, id: StoreId) -> Result<(), StoreLockError> {
        let path = std::fs::canonicalize(directory)
            .map_err(|source| FileError::at("resolve locked store", directory, source))?;
        if path != self.directory || id != self.id {
            return Err(StoreLockError::WrongDirectory(id));
        }
        Ok(())
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        // Closing alone leaves the lock held while a concurrently forked child
        // retains the open file before exec closes its inherited descriptors.
        self.file.unlock().expect("release owned store lock");
    }
}

/// A store already open for writing is distinct from a filesystem failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreLockError {
    /// Another handle or process holds the store's exclusive lock.
    #[error("store {0} is already open for writing")]
    AlreadyOpen(StoreId),
    /// A supplied lock protects a different directory.
    #[error("lock does not protect store {0}")]
    WrongDirectory(StoreId),
    /// Opening or locking the lock file failed for another reason.
    #[error("store lock: {0}")]
    File(#[from] FileError),
}

pub(crate) fn acquire(directory: &Path, id: StoreId) -> Result<StoreLock, StoreLockError> {
    let directory = std::fs::canonicalize(directory)
        .map_err(|source| FileError::at("resolve store directory", directory, source))?;
    let path = directory.join(".coven-lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| FileError::at("open store lock", &path, source))?;
    match file.try_lock() {
        Ok(()) => Ok(StoreLock {
            file,
            directory,
            id,
        }),
        Err(TryLockError::WouldBlock) => Err(StoreLockError::AlreadyOpen(id)),
        Err(TryLockError::Error(source)) => Err(FileError::at("lock store", &path, source).into()),
    }
}

fn deletion_path(directory: &Path, id: StoreId) -> PathBuf {
    directory.with_file_name(format!(".coven-delete-{id}"))
}

pub(crate) fn for_deletion(
    directory: &Path,
    id: StoreId,
) -> Result<Option<StoreLock>, StoreLockError> {
    let directory = match std::fs::symlink_metadata(directory) {
        Ok(_) => directory.to_owned(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => deletion_path(directory, id),
        Err(source) => {
            return Err(FileError::at("inspect store directory", directory, source).into())
        }
    };
    match std::fs::symlink_metadata(&directory) {
        Ok(metadata) if metadata.file_type().is_dir() => acquire(&directory, id).map(Some),
        Ok(_) => Err(FileError::at(
            "inspect deletion directory",
            &directory,
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "store is not a directory"),
        )
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(FileError::at("inspect deletion directory", &directory, source).into()),
    }
}

#[cfg(test)]
#[path = "lock_tests.rs"]
mod tests;
