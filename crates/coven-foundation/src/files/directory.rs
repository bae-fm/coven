//! The paths and filesystem capabilities inside one store directory.

use std::path::PathBuf;
use std::sync::Arc;

#[cfg(unix)]
use crate::files::flush_batch::BatchFolder;
use crate::files::protection::Protection;
use crate::files::{
    lock, settings, AtomicFile, CacheWriteLock, ClosedStoreLock, FileError, FileWriter, FlushBatch,
    SettingsError, StoreDeletionLock, StoreLock, StoreLockError, StoreReadLock, StoreSettings,
    WriterFolder,
};
use crate::id_source::StoreId;
use crate::task::BugSinkRef;

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
    /// Coven's own copies of app-provided files, each staging's in a folder
    /// of its own, and captured snapshots at the top (§17.1, Appendix C.3).
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

/// Where a staged copy lies among the store's app-provided files: in the
/// folder its staging made, under its own name, recorded as
/// `<folder>/<copy>` (Appendix C.3).
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StagedName(String);

impl StagedName {
    /// The copy `name` in the staging's folder `folder`.
    pub fn new(folder: &FileName, name: &FileName) -> Self {
        Self(format!("{}/{}", folder.0, name.0))
    }

    /// A recorded name read back, when it is two portable names joined by
    /// `/`.
    pub fn parse(recorded: &str) -> Option<Self> {
        let (folder, name) = recorded.split_once('/')?;
        Some(Self::new(&FileName::new(folder).ok()?, &FileName::new(name).ok()?))
    }

    /// The name as records hold it.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The staging's folder.
    pub fn folder(&self) -> FileName {
        FileName(self.parts().0.to_owned())
    }

    /// The copy's own name in its staging's folder.
    pub fn copy(&self) -> FileName {
        FileName(self.parts().1.to_owned())
    }

    /// The staging's folder and the copy's name in it.
    fn parts(&self) -> (&str, &str) {
        self.0.split_once('/').expect("a staged name holds its folder")
    }
}

/// Why a staging's folder could not be made.
#[derive(Debug, thiserror::Error)]
pub enum StagingFolderError {
    /// A folder of that name is already there: the id source gave out the
    /// name before.
    #[error("staging folder {} is already there", .0.as_str())]
    Taken(FileName),
    /// The store could not be held, or the folder made.
    #[error(transparent)]
    Writer(#[from] WriterError),
}

/// One store's directory, owning the paths to its database, app-provided files,
/// cache, settings and sibling locks. It never exposes the directory as a raw path.
#[derive(Clone)]
pub struct StoreDir {
    path: PathBuf,
    id: StoreId,
    /// The bootstrap lock of the bootstrap that gave the directory out, which
    /// opens the unpublished store while that bootstrap holds it.
    bootstrap: Option<lock::LockInstance>,
    /// The class its layout gives the store's files (E1).
    protection: Protection,
    /// Where a bug in the store's work that nobody waits for goes (E15).
    bugs: BugSinkRef,
}

impl std::fmt::Debug for StoreDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreDir")
            .field("path", &self.path)
            .field("id", &self.id)
            .field("bootstrap", &self.bootstrap)
            .field("protection", &self.protection)
            .finish_non_exhaustive()
    }
}

impl StoreDir {
    pub(crate) fn new(
        path: PathBuf,
        id: StoreId,
        protection: Protection,
        bugs: BugSinkRef,
    ) -> Self {
        Self { path, id, bootstrap: None, protection, bugs }
    }

    pub(super) fn bootstrapping(
        path: PathBuf,
        id: StoreId,
        bootstrap: lock::LockInstance,
        protection: Protection,
        bugs: BugSinkRef,
    ) -> Self {
        Self { path, id, bootstrap: Some(bootstrap), protection, bugs }
    }

    /// Whether the store is still an unpublished installation: its marker is
    /// there (§13.4).
    pub fn is_unpublished(&self) -> Result<bool, FileError> {
        super::bootstrap::is_pending(&self.path)
    }

    fn is_bootstrapping(&self) -> bool {
        self.bootstrap
            .is_some_and(|instance| lock::bootstrap_running(&self.path, self.id, instance))
    }

    /// An unfinished replacement's record refuses opens, from before its
    /// fresh device registers until it is published; a marker alone belongs
    /// to a first installation.
    fn check_published(&self) -> Result<(), StoreLockError> {
        if self.is_bootstrapping() {
            return Ok(());
        }
        let unfinished =
            super::device_reset::unfinished(&self.path, self.id).map_err(|error| match error {
                SettingsError::File(error) => StoreLockError::from(error),
                SettingsError::Bug(bug) => StoreLockError::Bug(bug),
                error => FileError::at(
                    "read the store's settings beside its replacement record",
                    &self.path,
                    std::io::Error::other(error),
                )
                .into(),
            })?;
        if let Some(record) = unfinished {
            return Err(StoreLockError::ReplacementUnfinished { store: self.id, by: record.by });
        }
        if super::bootstrap::is_pending(&self.path)? {
            return Err(StoreLockError::BootstrapPending(self.id));
        }
        Ok(())
    }

    /// Remove the installations resets kept in this store's directory
    /// (§19.3). The store's own files stay. A failure midway leaves the rest
    /// for the next call; with none left it succeeds.
    pub fn delete_discarded_devices(&self) -> Result<(), FileError> {
        super::device_reset::delete_archives(&self.path)
    }

    /// What this store's directory holds, in file sizes (§17.8, §19.3): its
    /// own files, apart from the cache, and the installations resets kept.
    pub fn local_bytes(&self) -> Result<super::DirectoryBytes, FileError> {
        super::local_bytes::measure(&self.path, FileArea::Cache.name())
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

/// The folder of the store's folder that holds a rebuild's store (§18.3).
const REBUILD: &str = "rebuild";

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

/// Probes the OS writer lock without waiting, for lifetime assertions in
/// tests. Production opens take the lock through `StoreDirAccess`.
#[cfg(any(test, feature = "test-utils"))]
pub trait StoreLockProbe {
    /// Take the writer lock, refusing at once when it is held.
    fn lock_exclusive(&self) -> Result<StoreLock, StoreLockError>;
}

#[cfg(any(test, feature = "test-utils"))]
impl StoreLockProbe for StoreDir {
    fn lock_exclusive(&self) -> Result<StoreLock, StoreLockError> {
        let guard = lock::acquire(&self.path, self.id)?;
        self.check_published()?;
        Ok(guard)
    }
}

/// Crate-to-crate operations for coven's owners; not re-exported by the app API.
pub trait StoreDirAccess: Sized {
    /// The database file's exact path for coven-database to open with SQLite.
    /// Callers never append path components to it.
    fn database_path(&self) -> PathBuf;
    /// Retained remote input has its own SQLite owner: receiving bytes must not
    /// wait for a transaction applying those bytes to the application database.
    fn input_database_path(&self) -> PathBuf;
    /// The database a breaking migration's per-audience check runs in, which
    /// lives only for that check (§18.3).
    fn audience_database_path(&self) -> PathBuf;
    /// Remove that database's file and the files SQLite keeps beside it; one
    /// that is absent is already removed.
    fn remove_audience_database(&self) -> Result<(), FileError>;
    /// The store a rebuild runs in (§18.3): a store of its own in the
    /// folder `rebuild` of this store's folder, with this store's id and this
    /// device's, the file areas empty and the class this layout gives the
    /// store's files. A leftover from a crash goes first, once an instance of
    /// it in this process has finished closing. The caller removes it with
    /// [`Self::remove_rebuild_directory`].
    fn rebuild_directory(
        &self,
    ) -> impl std::future::Future<Output = Result<Self, SettingsError>> + Send;
    /// Remove the folder `rebuild` whole, with its locks; absent is removed.
    fn remove_rebuild_directory(&self) -> Result<(), FileError>;
    /// The record of a replacement whose swap is under way (§19.3). A record
    /// that remains only because publication was interrupted after removing
    /// the bootstrap marker, with settings already naming the new device, is
    /// not returned; the next replacement removes it. Reads only.
    fn unfinished_replacement(&self) -> Result<Option<super::ReplacementRecord>, SettingsError>;
    /// What the store's unfinished deletion does with unsent work, when this
    /// device recorded that it is deleting the store and leaving it (E1).
    /// Reads only.
    fn unfinished_deletion(&self) -> Result<Option<super::IfUnsent>, FileError>;
    /// Record that this device is deleting the store and leaving it, with
    /// what the deletion does with unsent work, before it journals the
    /// device's removal. The record goes with the directory.
    fn record_deletion(&self, unsent: super::IfUnsent) -> Result<(), FileError>;
    /// Remove the record of a deletion that ended before it journaled the
    /// device's removal. An absent record succeeds.
    fn forget_deletion(&self) -> Result<(), FileError>;
    /// The settings written when this store was created, restored or joined.
    /// Reading takes no lock; a read-only open uses it alongside a writer.
    fn settings(&self) -> Result<StoreSettings, SettingsError>;
    /// Where a bug in the store's work that nobody waits for goes (E15):
    /// the app's own reporter, which its directories were given.
    fn bugs(&self) -> &BugSinkRef;
    /// Take the writer lock, awaiting release by a closing instance in this
    /// process. A live instance or another process still refuses immediately.
    fn lock_for_open(
        &self,
    ) -> impl std::future::Future<Output = Result<StoreLock, StoreLockError>> + Send;
    /// Take the writer lock to read a closed store without opening it: a live
    /// instance or another process refuses immediately, and an instance
    /// closing in this process is waited for. Registered as closing, so an
    /// open in this process waits for its release (api.md E1).
    fn lock_closed(
        &self,
    ) -> impl std::future::Future<Output = Result<ClosedStoreLock, StoreLockError>> + Send;
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
    /// instance only finishing its close, a replacement's loading
    /// installation in its hidden directory included. A live instance is not
    /// waited for.
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
    /// Hold `area`'s folder for a run of files made in it: the store's read
    /// lock is taken and the folder resolved once, for every writer
    /// [`WriterFolder::create`] makes, each as `create_writer`'s does.
    fn writer_folder(&self, area: FileArea) -> Result<WriterFolder, WriterError>;
    /// The capability for one staged copy, in its staging's folder.
    fn staged_file(&self, name: &StagedName) -> AtomicFile;
    /// Make `folder` among the app-provided files for one staging's copies,
    /// refusing one already there, and hold it as `writer_folder` holds an
    /// area's folder. The app-provided files' folder now holds the new
    /// folder, so `batch`'s flush syncs it too (Appendix C.3). The caller
    /// records the copies' names first, which name the folder.
    fn staging_folder(
        &self,
        folder: &FileName,
        batch: &mut FlushBatch,
    ) -> Result<WriterFolder, StagingFolderError>;
    /// Remove a staged copy, then its staging's folder once nothing is in
    /// it, syncing the folder that held each (Appendix C.3). An absent copy
    /// or folder is gone.
    fn remove_staged(&self, name: &StagedName) -> Result<(), FileError>;
    /// Reopen a named file of `area` that an earlier writer left unfinished,
    /// for appending after its first `length` bytes, which the caller
    /// recorded as synced; anything past them is cut off. A file with fewer
    /// bytes is refused. The writer holds the store's read lock as
    /// `create_writer`'s does.
    fn resume_writer(
        &self,
        area: FileArea,
        name: &FileName,
        length: u64,
    ) -> Result<FileWriter, WriterError>;
}

/// Check that `lock` protects exactly the store at `directory`.
pub fn verify_lock(directory: &StoreDir, lock: &StoreLock) -> Result<(), StoreLockError> {
    lock.verify(&directory.path, directory.id)
}

/// Remove what unfinished writes left at the top of the store `lock` holds
/// for writing (Appendix C.3), and the joining keys a published store still
/// holds when the join that published it stopped before deleting them
/// (§13.4), then give every folder and file the class its
/// layout gives the store's files, as `directory` names it, unless the
/// store's record shows they were last given it (E1). Blocks while it walks
/// the store.
pub fn settle_files(directory: &StoreDir, lock: &StoreLock) -> Result<(), FileError> {
    super::atomic_file::remove_temporaries(lock.directory())?;
    // A store still unpublished keeps them: its join resumes from them.
    if !super::bootstrap::is_pending(lock.directory())?
        && super::atomic_file::exists(
            "inspect the joining keys",
            &lock.directory().join(StoreFile::Bootstrap.name()),
        )?
    {
        directory.owned_file(StoreFile::Bootstrap).remove_with_temporaries()?;
    }
    directory.protection.apply(lock.directory())
}

impl StoreDirAccess for StoreDir {
    fn database_path(&self) -> PathBuf {
        self.path.join(DATABASE)
    }
    fn input_database_path(&self) -> PathBuf {
        self.path.join("inputs.db")
    }
    fn audience_database_path(&self) -> PathBuf {
        self.path.join("audiences.db")
    }
    fn remove_audience_database(&self) -> Result<(), FileError> {
        let path = self.audience_database_path();
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let mut file = path.as_os_str().to_owned();
            file.push(suffix);
            match std::fs::remove_file(PathBuf::from(&file)) {
                Err(source) if source.kind() != std::io::ErrorKind::NotFound => {
                    return Err(FileError::at(
                        "remove the audience database",
                        &PathBuf::from(file),
                        source,
                    ));
                }
                _ => (),
            }
        }
        Ok(())
    }
    async fn rebuild_directory(&self) -> Result<StoreDir, SettingsError> {
        let settings = self.settings()?;
        let scratch = self.path.join(REBUILD).join(self.id.to_string());
        lock::closed(&scratch, self.id).await?;
        self.remove_rebuild_directory()?;
        let root = self.path.join(REBUILD);
        for folder in [&root, &scratch] {
            std::fs::create_dir(folder).map_err(|source| {
                FileError::at("create the rebuild store's folder", folder, source)
            })?;
            self.protection.begin(folder)?;
        }
        initialize(&scratch, &settings)?;
        Ok(StoreDir::new(scratch, self.id, self.protection, self.bugs.clone()))
    }
    fn remove_rebuild_directory(&self) -> Result<(), FileError> {
        let root = self.path.join(REBUILD);
        match std::fs::remove_dir_all(&root) {
            Err(source) if source.kind() != std::io::ErrorKind::NotFound => {
                Err(FileError::at("remove the rebuild store's folder", &root, source))
            }
            _ => Ok(()),
        }
    }
    fn unfinished_replacement(&self) -> Result<Option<super::ReplacementRecord>, SettingsError> {
        super::device_reset::unfinished(&self.path, self.id)
    }
    fn unfinished_deletion(&self) -> Result<Option<super::IfUnsent>, FileError> {
        super::deletion::read(&self.path)
    }
    fn record_deletion(&self, unsent: super::IfUnsent) -> Result<(), FileError> {
        super::deletion::record(&self.path, unsent)
    }
    fn forget_deletion(&self) -> Result<(), FileError> {
        super::deletion::forget(&self.path)
    }
    fn settings(&self) -> Result<StoreSettings, SettingsError> {
        settings::read(&self.path, self.id)
    }
    fn bugs(&self) -> &BugSinkRef {
        &self.bugs
    }
    async fn lock_for_open(&self) -> Result<StoreLock, StoreLockError> {
        let guard = lock::acquire_for_open(&self.path, self.id, &self.bugs).await?;
        self.check_published()?;
        Ok(guard)
    }
    async fn lock_closed(&self) -> Result<ClosedStoreLock, StoreLockError> {
        let guard = lock::acquire_closed(&self.path, self.id, &self.bugs).await?;
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
        lock::closed(&self.path, self.id).await?;
        // A replacement loads in a hidden directory of its own, a lock
        // instance apart from the store's. Dropping the call that loads it
        // releases the store's locks at once, but leaves the loading
        // database to close as its last holders end, its files open until
        // then.
        lock::closed(&super::device_reset::loading(&self.path, self.id), self.id).await
    }
    fn owned_file(&self, file: StoreFile) -> AtomicFile {
        AtomicFile::new(self.path.join(file.name()))
    }
    fn file(&self, area: FileArea, name: &FileName) -> AtomicFile {
        AtomicFile::new(self.path.join(area.name()).join(&name.0))
    }
    fn create_writer(&self, area: FileArea, name: &FileName) -> Result<FileWriter, WriterError> {
        Ok(self.writer_folder(area)?.create(name)?)
    }
    fn writer_folder(&self, area: FileArea) -> Result<WriterFolder, WriterError> {
        let lock = self.lock_read_only()?;
        Ok(WriterFolder::open(&self.path.join(area.name()), Arc::new(lock), self.bugs.clone())?)
    }
    fn resume_writer(
        &self,
        area: FileArea,
        name: &FileName,
        length: u64,
    ) -> Result<FileWriter, WriterError> {
        let lock = self.lock_read_only()?;
        Ok(FileWriter::resume(&self.file(area, name), lock, self.bugs.clone(), length)?)
    }
    fn staged_file(&self, name: &StagedName) -> AtomicFile {
        let (folder, copy) = name.parts();
        AtomicFile::new(self.path.join(FileArea::AppProvided.name()).join(folder).join(copy))
    }
    fn staging_folder(
        &self,
        folder: &FileName,
        #[cfg_attr(
            not(unix),
            expect(unused_variables, reason = "Windows writes a folder's entries through")
        )]
        batch: &mut FlushBatch,
    ) -> Result<WriterFolder, StagingFolderError> {
        let lock = Arc::new(self.lock_read_only().map_err(WriterError::from)?);
        let files = self.path.join(FileArea::AppProvided.name());
        let path = files.join(&folder.0);
        match std::fs::create_dir(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(StagingFolderError::Taken(folder.clone()));
            }
            Err(source) => {
                let error = FileError::at("create staging folder", &path, source);
                return Err(WriterError::from(error).into());
            }
        }
        #[cfg(unix)]
        batch.add_folder(
            BatchFolder::open(files.clone(), Some(lock.clone())).map_err(|source| {
                WriterError::from(FileError::at("open folder", &files, source))
            })?,
        );
        Ok(WriterFolder::open(&path, lock, self.bugs.clone()).map_err(WriterError::from)?)
    }
    fn remove_staged(&self, name: &StagedName) -> Result<(), FileError> {
        let copy = self.staged_file(name);
        copy.remove()?;
        let (folder, _) = name.parts();
        super::atomic_file::remove_empty_folder(
            &self.path.join(FileArea::AppProvided.name()).join(folder),
        )
    }
}

#[cfg(test)]
#[path = "directory_tests.rs"]
mod tests;
