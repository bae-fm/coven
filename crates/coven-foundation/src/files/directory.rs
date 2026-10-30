//! The paths and filesystem capabilities inside one store directory.

use std::path::PathBuf;

use crate::files::{
    lock, settings, AtomicFile, CacheWriteLock, FileError, SettingsError, StoreDeletionLock,
    StoreLock, StoreLockError, StoreReadLock, StoreSettings,
};
use crate::id_source::StoreId;

/// Whether `path` names an existing folder, following symlinks. A folder
/// this process can't inspect counts as absent.
pub fn is_folder(path: &std::path::Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.is_dir())
}

/// The store database file, also its name in a discarded device archive.
pub(super) const DATABASE: &str = "store.db";

pub(crate) const APP_FILES: &str = "files";
pub(crate) const CACHE: &str = "cache";

/// One file owned by a crate, beside the database and store settings.
/// The enum reserves the name so a caller cannot replace the database or settings.
#[derive(Clone, Copy, Debug)]
pub enum StoreFile {
    /// coven-storage's provider settings, encoded and interpreted by that crate.
    StorageSettings,
    /// coven-crypto's passphrase-sealed store and circle keys (E1).
    StoreKeys,
    /// coven-crypto's passphrase-sealed member key pairs (E1).
    MemberKeys,
    /// Encrypted joining identity and fixed request, interpreted by coven-sync.
    Bootstrap,
}

/// The two places coven keeps file bytes inside a store (E1).
#[derive(Clone, Copy, Debug)]
pub enum FileArea {
    /// Coven's own copies of app-provided files (§17.1).
    AppProvided,
    /// The cache of uploaded files and chunks (§17.7).
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
        let stem = name.split('.').next().expect("a nonempty name has a stem");
        if super::relative_path::windows_device(stem) {
            return Err(FileNameError::Reserved);
        }
        Ok(Self(name))
    }
}

/// One store's directory, owning the paths to its database, app-provided files,
/// cache, settings and sibling locks. It never exposes the directory as a raw path.
#[derive(Clone, Debug)]
pub struct StoreDir {
    path: PathBuf,
    id: StoreId,
    bootstrap: Option<std::sync::Arc<super::bootstrap::BootstrapLease>>,
}

impl StoreDir {
    pub(crate) fn new(path: PathBuf, id: StoreId) -> Self {
        Self {
            path,
            id,
            bootstrap: None,
        }
    }

    pub(super) fn bootstrapping(
        path: PathBuf,
        id: StoreId,
        lease: std::sync::Arc<super::bootstrap::BootstrapLease>,
    ) -> Self {
        Self {
            path,
            id,
            bootstrap: Some(lease),
        }
    }

    fn is_bootstrapping(&self) -> bool {
        self.bootstrap.as_ref().is_some_and(|lease| lease.is_held())
    }

    /// A marker beside a replacement record belongs to a swap under way;
    /// alone, it belongs to a first installation.
    fn check_published(&self) -> Result<(), StoreLockError> {
        if !self.is_bootstrapping() && super::bootstrap::is_pending(&self.path)? {
            return Err(match super::device_reset::read(&self.path)? {
                Some(record) => StoreLockError::ReplacementUnfinished {
                    store: self.id,
                    by: record.by,
                },
                None => StoreLockError::BootstrapPending(self.id),
            });
        }
        Ok(())
    }

    /// The store identified by this directory.
    pub fn id(&self) -> StoreId {
        self.id
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

/// Crate-to-crate operations for coven's owners; not re-exported by the app API.
pub trait StoreDirAccess: Sized {
    /// The database file's exact path for coven-database to open with SQLite.
    /// Callers never append path components to it.
    fn database_path(&self) -> PathBuf;
    /// Retained remote input has its own SQLite owner: receiving bytes must not
    /// wait for a transaction applying those bytes to the application database.
    fn input_database_path(&self) -> PathBuf;
    /// The record of a replacement whose swap is under way (§19.3). A record
    /// that remains only because publication was interrupted after removing
    /// the bootstrap marker, with settings already naming the new device, is
    /// not returned; the next replacement removes it. Reads only.
    fn unfinished_replacement(&self) -> Result<Option<super::ReplacementRecord>, SettingsError>;
    /// The settings written when this store was created, restored or joined.
    /// Reading takes no lock; a read-only open uses it alongside a writer.
    fn settings(&self) -> Result<StoreSettings, SettingsError>;
    /// Probe the OS writer lock without waiting, for lifetime assertions in tests.
    #[cfg(any(test, feature = "test-utils"))]
    fn lock_exclusive(&self) -> Result<StoreLock, StoreLockError>;
    /// Take the writer lock, awaiting release by a closing instance in this
    /// process. A live instance or another process still refuses immediately.
    fn lock_for_open(
        &self,
    ) -> impl std::future::Future<Output = Result<StoreLock, StoreLockError>> + Send;
    /// Prevent deletion while reading, alongside other readers and the writer.
    /// The guard must outlive every open file and database connection it protects.
    fn lock_read_only(&self) -> Result<StoreReadLock, StoreLockError>;
    /// Hold while a cache range recorded as being written gets its bytes
    /// with no database writer held. Shared by such writes in every process;
    /// `exclude_cache_writes` waits for all of them (Appendix C.3).
    fn lock_cache_write(&self) -> Result<CacheWriteLock, FileError>;
    /// Wait until no cache range is being written in any process and keep new
    /// writes waiting, so a range still recorded as being written has no
    /// live writer (Appendix C.3).
    fn exclude_cache_writes(&self) -> Result<CacheWriteLock, FileError>;
    /// Lock an existing store or a directory left by an interrupted deletion,
    /// holding its writer, reader and bootstrap locks so that removal takes
    /// every lock file named for the store. An absent store with leftover
    /// locks can finish their removal on retry. A running restore, join or
    /// replacement refuses with `StoreLockError::AlreadyOpen`. A store whose
    /// replacement is unfinished is locked for deletion; an unfinished first
    /// installation is refused with `StoreLockError::BootstrapPending`.
    /// No directory is created by this call.
    fn lock_for_deletion(&self) -> Result<Option<StoreDeletionLock>, StoreLockError>;
    /// Check that a supplied lock protects this exact store directory.
    fn verify_lock(&self, lock: &StoreLock) -> Result<(), StoreLockError>;
    /// Give a crate the capability for its reserved file, without exposing
    /// the store path or letting it name any other top-level file.
    fn owned_file(&self, file: StoreFile) -> AtomicFile;
    /// The capability for one app-provided copy or cache entry. All path
    /// construction stays here; the name cannot traverse directories.
    fn file(&self, area: FileArea, name: &FileName) -> AtomicFile;
}

impl StoreDirAccess for StoreDir {
    fn database_path(&self) -> PathBuf {
        self.path.join(DATABASE)
    }
    fn input_database_path(&self) -> PathBuf {
        self.path.join("inputs.db")
    }
    fn unfinished_replacement(&self) -> Result<Option<super::ReplacementRecord>, SettingsError> {
        super::device_reset::unfinished(&self.path, self.id)
    }
    fn settings(&self) -> Result<StoreSettings, SettingsError> {
        settings::read(&self.path, self.id)
    }
    #[cfg(any(test, feature = "test-utils"))]
    fn lock_exclusive(&self) -> Result<StoreLock, StoreLockError> {
        let guard = lock::acquire(&self.path, self.id)?;
        self.check_published()?;
        Ok(guard)
    }
    async fn lock_for_open(&self) -> Result<StoreLock, StoreLockError> {
        let guard = lock::acquire_for_open(&self.path, self.id).await?;
        self.check_published()?;
        Ok(guard)
    }
    fn lock_read_only(&self) -> Result<StoreReadLock, StoreLockError> {
        let guard = lock::acquire_reader(&self.path, self.id)?;
        self.check_published()?;
        Ok(guard)
    }
    fn lock_cache_write(&self) -> Result<CacheWriteLock, FileError> {
        lock::cache_writes(&self.path, true)
    }
    fn exclude_cache_writes(&self) -> Result<CacheWriteLock, FileError> {
        lock::cache_writes(&self.path, false)
    }
    fn lock_for_deletion(&self) -> Result<Option<StoreDeletionLock>, StoreLockError> {
        let lease = self.bootstrap.clone().filter(|lease| lease.is_held());
        let bootstrapping = lease.is_some();
        let locked = lock::for_deletion(&self.path, self.id, lease);
        // A marker beside a replacement record belongs to a swap under way,
        // which deletion removes whole: both installations, the record and
        // the replacement's directory. Alone, it belongs to a first
        // installation, which only its own call resumes or cancels, whether
        // or not that call is running and holding the bootstrap lock.
        if !bootstrapping
            && matches!(locked, Ok(Some(_)) | Err(StoreLockError::AlreadyOpen(_)))
            && super::device_reset::read(&self.path)?.is_none()
            && super::bootstrap::is_pending(&self.path)?
        {
            return Err(StoreLockError::BootstrapPending(self.id));
        }
        locked
    }
    fn verify_lock(&self, lock: &StoreLock) -> Result<(), StoreLockError> {
        lock.verify(&self.path, self.id)
    }
    fn owned_file(&self, file: StoreFile) -> AtomicFile {
        let name = match file {
            StoreFile::StorageSettings => "storage.json",
            StoreFile::StoreKeys => "store-keys.sealed",
            StoreFile::MemberKeys => "member-keys.sealed",
            StoreFile::Bootstrap => "bootstrap.sealed",
        };
        AtomicFile::new(self.path.join(name))
    }
    fn file(&self, area: FileArea, name: &FileName) -> AtomicFile {
        let area = match area {
            FileArea::AppProvided => APP_FILES,
            FileArea::Cache => CACHE,
        };
        AtomicFile::new(self.path.join(area).join(&name.0))
    }
}

#[cfg(test)]
#[path = "directory_tests.rs"]
mod tests;
