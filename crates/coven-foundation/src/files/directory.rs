//! The paths and filesystem capabilities inside one store directory.

use std::path::PathBuf;

use crate::files::protection::Protection;
use crate::files::{
    lock, settings, AtomicFile, CacheWriteLock, FileError, FileWriter, SettingsError,
    StoreDeletionLock, StoreLock, StoreLockError, StoreReadLock, StoreSettings,
};
use crate::id_source::StoreId;

/// The folder an app names for SQLite's temporary tables (§21.2), outside
/// coven's directories. It is made where the app's call names it.
#[derive(Debug, PartialEq, Eq)]
pub struct TemporaryFolder {
    path: PathBuf,
}

impl TemporaryFolder {
    /// The folder at `path`, when it names an existing folder, following
    /// symlinks. A folder this process can't inspect counts as absent.
    pub fn at(path: &std::path::Path) -> Option<Self> {
        std::fs::metadata(path)
            .is_ok_and(|metadata| metadata.is_dir())
            .then(|| Self { path: path.to_owned() })
    }

    /// Its path.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

/// The store database file, also its name in a discarded device archive.
pub(super) const DATABASE: &str = "store.db";

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

impl StoreFile {
    /// The file's name in the store's directory.
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::StorageSettings => "storage.json",
            Self::StoreKeys => "store-keys.sealed",
            Self::MemberKeys => "member-keys.sealed",
            Self::Bootstrap => "bootstrap.sealed",
        }
    }
}

/// The two places coven keeps file bytes inside a store (E1).
#[derive(Clone, Copy, Debug)]
pub enum FileArea {
    /// Coven's own copies of app-provided files (§17.1).
    AppProvided,
    /// The cache of uploaded files and chunks (§17.8).
    Cache,
}

impl FileArea {
    const ALL: [Self; 2] = [Self::AppProvided, Self::Cache];

    /// The area's folder in the store's directory.
    const fn name(self) -> &'static str {
        match self {
            Self::AppProvided => "files",
            Self::Cache => "cache",
        }
    }
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
        if name.is_empty() || name.len() > super::relative_path::NAME_LIMIT {
            return Err(FileNameError::Length);
        }
        if name.starts_with('.')
            || name.ends_with('.')
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
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
    /// The bootstrap lock of the bootstrap that gave the directory out, which
    /// opens the unpublished store while that bootstrap holds it.
    bootstrap: Option<lock::LockInstance>,
    /// The class its layout gives the store's files (E1).
    protection: Protection,
}

impl StoreDir {
    pub(crate) fn new(path: PathBuf, id: StoreId, protection: Protection) -> Self {
        Self { path, id, bootstrap: None, protection }
    }

    pub(super) fn bootstrapping(
        path: PathBuf,
        id: StoreId,
        bootstrap: lock::LockInstance,
        protection: Protection,
    ) -> Self {
        Self { path, id, bootstrap: Some(bootstrap), protection }
    }

    fn is_bootstrapping(&self) -> bool {
        self.bootstrap
            .is_some_and(|instance| lock::bootstrap_running(&self.path, self.id, instance))
    }

    /// A marker beside a replacement record belongs to a swap under way;
    /// alone, it belongs to a first installation.
    fn check_published(&self) -> Result<(), StoreLockError> {
        if !self.is_bootstrapping() && super::bootstrap::is_pending(&self.path)? {
            return Err(match super::device_reset::read(&self.path)? {
                Some(record) => {
                    StoreLockError::ReplacementUnfinished { store: self.id, by: record.by }
                }
                None => StoreLockError::BootstrapPending(self.id),
            });
        }
        Ok(())
    }

    /// The store identified by this directory.
    pub fn id(&self) -> StoreId {
        self.id
    }

    /// The folder of the store's file `area`, for a test that looks at the
    /// files coven keeps there.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn area_path(&self, area: FileArea) -> std::path::PathBuf {
        self.path.join(area.name())
    }
}

pub(crate) fn initialize(
    path: &std::path::Path,
    settings: &StoreSettings,
) -> Result<(), SettingsError> {
    for area in FileArea::ALL {
        let directory = path.join(area.name());
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

/// Why a store's file could not be created for writing: the store's read
/// lock, which the writer holds, or the file itself.
#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    /// The store could not be held for the write.
    #[error(transparent)]
    Lock(#[from] StoreLockError),
    /// Creating the file failed.
    #[error(transparent)]
    File(#[from] FileError),
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
    /// Wait until no instance of this store in this process is closing, as
    /// `lock_for_open` does, so a deletion that follows is not refused by an
    /// instance only finishing its close. A live instance is not waited for.
    fn closed(&self) -> impl std::future::Future<Output = Result<(), FileError>> + Send;
    /// Give a crate the capability for its reserved file, without exposing
    /// the store path or letting it name any other top-level file.
    fn owned_file(&self, file: StoreFile) -> AtomicFile;
    /// The capability for one app-provided copy or cache entry. All path
    /// construction stays here; the name cannot traverse directories.
    fn file(&self, area: FileArea, name: &FileName) -> AtomicFile;
    /// Create a named file in `area` for asynchronous streaming without
    /// replacing anything. The writer holds the store's read lock until its
    /// handle and any cancelled I/O finish, so the store can't be deleted
    /// under it. The caller records the name first and removes partial bytes
    /// after a failure.
    fn create_writer(&self, area: FileArea, name: &FileName) -> Result<FileWriter, WriterError>;
}

/// Check that `lock` protects exactly the store at `directory`.
pub fn verify_lock(directory: &StoreDir, lock: &StoreLock) -> Result<(), StoreLockError> {
    lock.verify(&directory.path, directory.id)
}

/// Give every folder and file of the store `lock` holds for writing the class
/// its layout gives the store's files, as `directory` names it, unless the
/// store's record shows they were last given it (E1). Blocks while it walks
/// the store.
pub fn protect_files(directory: &StoreDir, lock: &StoreLock) -> Result<(), FileError> {
    directory.protection.apply(lock.directory())
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
        let bootstrapping = self.is_bootstrapping();
        let locked = lock::for_deletion(&self.path, self.id, None);
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
    async fn closed(&self) -> Result<(), FileError> {
        lock::closed(&self.path, self.id).await
    }
    fn owned_file(&self, file: StoreFile) -> AtomicFile {
        AtomicFile::new(self.path.join(file.name()))
    }
    fn file(&self, area: FileArea, name: &FileName) -> AtomicFile {
        AtomicFile::new(self.path.join(area.name()).join(&name.0))
    }
    fn create_writer(&self, area: FileArea, name: &FileName) -> Result<FileWriter, WriterError> {
        let lock = self.lock_read_only()?;
        Ok(FileWriter::create(&self.file(area, name), lock)?)
    }
}

#[cfg(test)]
#[path = "directory_tests.rs"]
mod tests;
