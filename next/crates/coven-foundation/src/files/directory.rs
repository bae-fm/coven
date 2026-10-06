//! The paths and filesystem capabilities inside one store directory.

use std::path::PathBuf;

use crate::files::{
    lock, settings, AtomicFile, SettingsError, StoreLock, StoreLockError, StoreSettings,
};
use crate::id_source::StoreId;

pub(crate) const APP_FILES: &str = "files";
pub(crate) const CACHE: &str = "cache";

/// One file owned by a crate, beside the database and store settings.
/// The enum reserves the name so a caller cannot replace the lock or database.
#[derive(Clone, Copy, Debug)]
pub enum StoreFile {
    /// coven-storage's provider settings, encoded and interpreted by that crate.
    StorageSettings,
    /// coven-crypto's passphrase-sealed store and circle keys (§20.1).
    StoreKeys,
    /// coven-crypto's passphrase-sealed member key pairs (§20.1).
    MemberKeys,
}

/// The two places coven keeps file bytes inside a store (§20.1).
#[derive(Clone, Copy, Debug)]
pub enum FileArea {
    /// Coven's own copies of app-provided files (§16.1).
    AppProvided,
    /// The cache of uploaded files and chunks (§16.4).
    Cache,
}

/// A portable single filename: ASCII letters, digits, `-`, `_` and interior
/// dots, up to 255 bytes. Windows device names are refused on every platform.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FileName(String);

/// A name that could escape its directory or alias a Windows device.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FileNameError {
    /// The name is empty or exceeds a filesystem component's length.
    #[error("file name must contain 1 to 255 bytes")]
    Length,
    /// The name contains a forbidden byte, starts with a dot or ends with one.
    #[error("file name requires ASCII letters, digits, hyphens, underscores or interior dots")]
    Character,
    /// The name names a Windows device, even with an extension.
    #[error("file name is reserved for a Windows device")]
    Reserved,
}

impl FileName {
    /// The validated filename, for recording an owned file in local metadata.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate a single filename before it is joined to an owned directory.
    pub fn new(name: impl Into<String>) -> Result<Self, FileNameError> {
        let name = name.into();
        if name.is_empty() || name.len() > 255 {
            return Err(FileNameError::Length);
        }
        if name.starts_with('.')
            || name.ends_with('.')
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(FileNameError::Character);
        }
        let stem = name
            .split('.')
            .next()
            .expect("a nonempty name has a stem")
            .to_ascii_uppercase();
        if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || ((stem.starts_with("COM") || stem.starts_with("LPT"))
                && stem.len() == 4
                && matches!(stem.as_bytes()[3], b'1'..=b'9'))
        {
            return Err(FileNameError::Reserved);
        }
        Ok(Self(name))
    }
}

/// One store's directory, owning the paths to its database, app-provided files,
/// cache, settings and lock. It never exposes the directory as a raw path.
#[derive(Clone, Debug)]
pub struct StoreDir {
    path: PathBuf,
    id: StoreId,
}

impl StoreDir {
    pub(crate) fn new(path: PathBuf, id: StoreId) -> Self {
        Self { path, id }
    }

    /// The store identified by this directory.
    pub fn id(&self) -> StoreId {
        self.id
    }

    /// The database file's exact path for coven-database to open with SQLite.
    /// Callers never append path components to it.
    pub fn database_path(&self) -> PathBuf {
        self.path.join("store.db")
    }

    /// The settings written when this store was created, restored or joined.
    /// Reading takes no lock; a read-only open uses it alongside a writer.
    pub fn settings(&self) -> Result<StoreSettings, SettingsError> {
        settings::read(&self.path, self.id)
    }

    /// Take the exclusive OS lock for a writable open. Read-only opens skip
    /// this method. The returned guard must outlive every writable owner.
    pub fn lock_exclusive(&self) -> Result<StoreLock, StoreLockError> {
        lock::acquire(&self.path, self.id)
    }

    /// Lock an existing store or a directory left by an interrupted deletion.
    /// An absent store needs no guard. No directory is created by this call.
    pub fn lock_for_deletion(&self) -> Result<Option<StoreLock>, StoreLockError> {
        lock::for_deletion(&self.path, self.id)
    }

    /// Check that a supplied lock protects this exact store directory.
    pub fn verify_lock(&self, lock: &StoreLock) -> Result<(), StoreLockError> {
        lock.verify(&self.path, self.id)
    }

    /// Give a crate the capability for its reserved file, without exposing
    /// the store path or letting it name any other top-level file.
    pub fn owned_file(&self, file: StoreFile) -> AtomicFile {
        let name = match file {
            StoreFile::StorageSettings => "storage.json",
            StoreFile::StoreKeys => "store-keys.sealed",
            StoreFile::MemberKeys => "member-keys.sealed",
        };
        AtomicFile::new(self.path.join(name))
    }

    /// The capability for one app-provided copy or cache entry. All path
    /// construction stays here; the name cannot traverse directories.
    pub fn file(&self, area: FileArea, name: &FileName) -> AtomicFile {
        let area = match area {
            FileArea::AppProvided => APP_FILES,
            FileArea::Cache => CACHE,
        };
        AtomicFile::new(self.path.join(area).join(&name.0))
    }
}

pub(crate) fn initialize(
    path: &std::path::Path,
    settings: &StoreSettings,
) -> Result<(), SettingsError> {
    for name in [APP_FILES, CACHE] {
        let directory = path.join(name);
        std::fs::create_dir(&directory).map_err(|source| {
            crate::files::FileError::at("create store files directory", &directory, source)
        })?;
        #[cfg(unix)]
        crate::files::atomic_file::sync_directory(&directory).map_err(|source| {
            crate::files::FileError::at("sync store files directory", &directory, source)
        })?;
    }
    settings::write(path, settings)
}

#[cfg(test)]
#[path = "directory_tests.rs"]
mod tests;
