//! Store locks live beside the directory so Windows can unpublish it.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use crate::files::file_lock::{FileLock, LockMode};
use crate::files::{atomic_file, settings, FileError, SettingsError, StoreSettings};
use crate::id_source::StoreId;

use super::lock_registry as registry;
use registry::LockKind;
pub use registry::StoreClosingGuard;
pub(super) use registry::{bootstrap_running, LockInstance, RegisteredLock};

/// An exclusive writer lock. Dropping it unlocks and closes the OS handle.
/// Readers use a separate shared lock so they can coexist with the writer.
#[derive(Debug)]
pub struct StoreLock {
    store: LockedStore<RegisteredLock>,
}

#[derive(Debug)]
struct LockedStore<L> {
    _file: L,
    directory: PathBuf,
    id: StoreId,
}

impl StoreLock {
    /// Mark this instance as closing when the caller's owning scope ends.
    /// This guard does not retain the OS lock or any database connection.
    pub fn closing_guard(&self) -> StoreClosingGuard {
        self.store._file.closing_guard()
    }

    /// Read the settings while holding the store's writer lock.
    pub fn settings(&self) -> Result<StoreSettings, SettingsError> {
        settings::read(&self.store.directory, self.store.id)
    }

    /// The canonical directory of the store this lock holds.
    pub(super) fn directory(&self) -> &Path {
        &self.store.directory
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
    _file: RegisteredLock,
}

impl StoreReadLock {
    /// Mark this reader as closing when its owner begins to close, so a
    /// deletion in this process waits for its release instead of refusing.
    /// This guard does not retain the OS lock.
    pub fn closing_guard(&self) -> StoreClosingGuard {
        self._file.closing_guard()
    }
}

/// The writer lock, held while a closed store is read without opening it:
/// holding it shows that no handle runs the store's sync. Registered in this
/// process as closing from the start, so an open waits for its release.
/// Dropping it unlocks.
#[derive(Debug)]
pub struct ClosedStoreLock {
    _file: RegisteredLock,
}

/// A held lock on the cache's range writes: shared by a write in progress,
/// exclusive while opening finds abandoned ones. Dropping it unlocks.
#[derive(Debug)]
pub struct CacheWriteLock {
    _file: FileLock,
}

/// The cache-write lock file, in the store directory beside the cache folder,
/// which holds only cache entries.
const CACHE_WRITES: &str = ".coven-cache-writes.lock";

/// Wait for the cache-write lock, shared or exclusive. Unlike the store
/// locks it waits: a range write is short, and opening needs them finished.
pub(super) fn cache_writes(store: &Path, shared: bool) -> Result<CacheWriteLock, FileError> {
    let path = store.join(CACHE_WRITES);
    let mode = if shared { LockMode::Shared } else { LockMode::Exclusive };
    Ok(CacheWriteLock { _file: wait_for_lock(&path, mode, "lock cache writes")? })
}

/// Wait for the lock on the file at `path`. A test hears when the lock is
/// held and the call is about to wait for it (`when_reached`).
fn wait_for_lock(path: &Path, mode: LockMode, what: &'static str) -> Result<FileLock, FileError> {
    let file = open_lock(path)?;
    let attempt = file.try_clone().map_err(|source| FileError::at(what, path, source))?;
    match FileLock::try_lock(attempt, mode) {
        Ok(file) => return Ok(file),
        Err(TryLockError::WouldBlock) => {
            #[cfg(any(test, feature = "test-utils"))]
            super::reached::reached(path);
        }
        Err(TryLockError::Error(source)) => return Err(FileError::at(what, path, source)),
    }
    FileLock::lock(file, mode).map_err(|source| FileError::at(what, path, source))
}

/// The cache-write lock file of the store at `store`, which a test waits on
/// to know a range write or an open waits for it.
#[cfg(any(test, feature = "test-utils"))]
pub fn cache_writes_lock(store: &Path) -> std::path::PathBuf {
    store.join(CACHE_WRITES)
}

/// Exclusive access to the writer, readers and bootstrap, authorizing
/// deletion. Holding the bootstrap lock keeps every restore, join and
/// replacement of the store out, including a `reset_device` or
/// `restore_device` that holds only that lock between the steps of an
/// unfinished swap (§19.3), and lets deletion remove its lock file too.
#[derive(Debug)]
pub struct StoreDeletionLock {
    store: LockedStore<FileLock>,
    _readers: FileLock,
    bootstrap: HeldBootstrap,
}

/// How a deletion holds the store's bootstrap lock.
#[derive(Debug)]
enum HeldBootstrap {
    /// Taken by the deletion itself.
    Locked(FileLock),
    /// Handed over by a bootstrap cancelling its own unpublished store.
    Leased(RegisteredLock),
}

impl HeldBootstrap {
    fn release(self) {
        match self {
            Self::Locked(file) => drop(file),
            Self::Leased(lock) => drop(lock),
        }
    }
}

impl StoreDeletionLock {
    /// Rename the store's directory to its deletion name, durably: the store
    /// leaves every listing and open, and the directory under that name, or
    /// the store's lock files without a directory, record that its deletion
    /// is under way, for the next listing, restore or join of the store to
    /// finish (§11.6, E1). Done already, it changes nothing.
    pub fn unpublish(&mut self) -> Result<(), FileError> {
        let _layout = lock_layout(self.store.directory.parent().expect("store parent"))?;
        self.unpublish_locked()
    }

    /// `unpublish`, under the layout lock.
    fn unpublish_locked(&mut self) -> Result<(), FileError> {
        let destination = deletion_path(&self.store.directory, self.store.id);
        if self.store.directory != destination && inspect_directory(&self.store.directory)? {
            crate::files::atomic_file::rename_new(&self.store.directory, &destination).map_err(
                |source| FileError::at("unpublish store", &self.store.directory, source),
            )?;
            self.store.directory = destination;
            #[cfg(unix)]
            sync_removal(&self.store.directory)?;
        }
        Ok(())
    }

    /// Unpublish the directory, as `unpublish` does, and remove its contents
    /// before releasing and removing its sibling locks. Retry failures
    /// through `lock_for_deletion`, including a failure after only the lock
    /// files remain.
    pub fn remove_directory(mut self) -> Result<(), FileError> {
        let root = self.store.directory.parent().expect("store parent");
        // Every lock acquisition and creation also takes this lock. None can
        // open a lock file between its release here and the removal of its
        // name, so no one holds an unlinked lock while another locks a new
        // file at the same path.
        let _layout = lock_layout(root)?;
        let paths = deletion_lock_paths(&self.store.directory, self.store.id);
        self.unpublish_locked()?;
        if inspect_directory(&self.store.directory)? {
            std::fs::remove_dir_all(&self.store.directory).map_err(|source| {
                FileError::at("remove store directory", &self.store.directory, source)
            })?;
        }
        #[cfg(unix)]
        sync_removal(&self.store.directory)?;
        // Release before removing, on every platform. Windows keeps a file
        // deleted while a handle is open until that handle closes, and
        // refuses to open its name meanwhile, which would refuse the next
        // lock of this store. Under the layout lock, releasing first is as
        // safe as removing a held file.
        let Self { store, _readers, bootstrap } = self;
        drop((store, _readers));
        bootstrap.release();
        for path in paths {
            atomic_file::remove(&path)?;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn sync_removal(directory: &Path) -> Result<(), FileError> {
    crate::files::atomic_file::sync_directory(directory.parent().expect("store parent")).map_err(
        |source| FileError::AfterRemove { path: directory.to_owned(), source: Box::new(source) },
    )
}

/// A store in use is distinct from a filesystem failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreLockError {
    /// A live instance or another process prevents the requested access.
    /// Async opens wait for a closing instance in this process to release it.
    #[error("store {0} is already open")]
    AlreadyOpen(StoreId),
    /// An unfinished first installation must be resumed by the call that
    /// began it: a restore or join with its code. A restore on a join's
    /// installation, or a join on a recorded restore's, is refused too (§13.4).
    #[error("store {0} requires restore or join to finish bootstrap")]
    BootstrapPending(StoreId),
    /// A replacement of this device's installation recorded itself, just
    /// before its fresh device registered, and did not finish. Only the call
    /// it names finishes it; the other is refused before anything changes.
    /// Deletion removes it instead (§19.3).
    #[error("store {store} requires {by} to finish replacing this device's installation")]
    ReplacementUnfinished {
        /// The store whose installation is being replaced.
        store: StoreId,
        /// The call that began the replacement.
        by: super::ReplacedBy,
    },
    /// This device is deleting the store and leaving it, and its deletion
    /// recorded itself, just before it journaled the device's removal, and
    /// did not finish. Opening and replacing the store are refused before
    /// anything changes; only finishing the deletion, or discarding the
    /// store, ends it (E1).
    #[error("store {0} is being deleted from this device")]
    DeletionUnfinished(StoreId),
    /// A supplied lock protects a different directory.
    #[error("lock does not protect store {0}")]
    WrongDirectory(StoreId),
    /// Opening or locking the lock file failed for another reason, or did
    /// not run because its runtime is shutting down (`FileError::Shutdown`).
    #[error("store lock: {0}")]
    File(FileError),
    /// Taking the lock panicked on the blocking pool: a bug, which the
    /// caller reports (E15).
    #[error(transparent)]
    Bug(#[from] crate::task::Bug),
}

impl From<FileError> for StoreLockError {
    fn from(error: FileError) -> Self {
        match error {
            FileError::Bug(bug) => Self::Bug(bug),
            error => Self::File(error),
        }
    }
}

impl crate::task::CarriesBug for StoreLockError {
    fn bug(&self) -> Option<&crate::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::File(error) => error.bug(),
            _ => None,
        }
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

pub(crate) fn try_lock(path: &Path, id: StoreId, shared: bool) -> Result<FileLock, StoreLockError> {
    let mode = if shared { LockMode::Shared } else { LockMode::Exclusive };
    match FileLock::try_lock(open_lock(path)?, mode) {
        Ok(file) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(StoreLockError::AlreadyOpen(id)),
        Err(TryLockError::Error(source)) => Err(FileError::at("lock store", path, source).into()),
    }
}

// This file belongs to the layout and is never removed. It serializes lock
// acquisition, creation, and lock-file removal across processes, so no caller
// can hold an unlinked lock while another locks a new file at the same path.
pub(crate) fn lock_layout(root: &Path) -> Result<FileLock, FileError> {
    wait_for_lock(&layout_lock(root), LockMode::Exclusive, "lock store layout")
}

/// The layout's lock file under `root`, which a test waits on to know a
/// lock, creation or removal waits for the layout.
pub(crate) fn layout_lock(root: &Path) -> PathBuf {
    root.join(".coven-stores.lock")
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn acquire(directory: &Path, id: StoreId) -> Result<StoreLock, StoreLockError> {
    let directory = resolve_directory(directory)?;
    let file = registry::try_acquire(&directory, id, LockKind::Writer)?;
    Ok(StoreLock { store: LockedStore { _file: file, directory, id } })
}

pub(crate) async fn acquire_for_open(
    directory: &Path,
    id: StoreId,
    bugs: &crate::task::BugSinkRef,
) -> Result<StoreLock, StoreLockError> {
    let directory = resolve_directory(directory)?;
    let file = registry::acquire_registered(&directory, id, LockKind::Writer, bugs).await?;
    Ok(StoreLock { store: LockedStore { _file: file, directory, id } })
}

/// Take the writer lock of a closed store to read it, without waiting for
/// another process or a live instance in this one; an instance closing in
/// this process is waited for first.
pub(crate) async fn acquire_closed(
    directory: &Path,
    id: StoreId,
    bugs: &crate::task::BugSinkRef,
) -> Result<ClosedStoreLock, StoreLockError> {
    let directory = resolve_directory(directory)?;
    Ok(ClosedStoreLock { _file: registry::acquire_closing_writer(&directory, id, bugs).await? })
}

/// Wait until no instance of the store at `directory` in this process is
/// closing. An absent directory has none.
pub(crate) async fn closed(directory: &Path, id: StoreId) -> Result<(), FileError> {
    let directory = match std::fs::canonicalize(directory) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(FileError::at("resolve store directory", directory, source)),
    };
    registry::closed(&directory, id).await;
    Ok(())
}

pub(super) async fn acquire_bootstrap(
    directory: &Path,
    id: StoreId,
    bugs: &crate::task::BugSinkRef,
) -> Result<RegisteredLock, StoreLockError> {
    registry::acquire_registered(directory, id, LockKind::Bootstrap, bugs).await
}

fn resolve_directory(directory: &Path) -> Result<PathBuf, FileError> {
    std::fs::canonicalize(directory)
        .map_err(|source| FileError::at("resolve store directory", directory, source))
}

pub(super) fn exclude_readers(directory: &Path, id: StoreId) -> Result<FileLock, StoreLockError> {
    let _layout = lock_layout(directory.parent().expect("store parent"))?;
    try_lock(&lock_paths(directory, id)[1], id, false)
}

pub(crate) fn acquire_reader(
    directory: &Path,
    id: StoreId,
) -> Result<StoreReadLock, StoreLockError> {
    let directory = resolve_directory(directory)?;
    Ok(StoreReadLock { _file: registry::acquire_reader(&directory, id)? })
}

pub(super) fn lock_existing(
    directory: &Path,
    id: StoreId,
    shared: bool,
) -> Result<(PathBuf, FileLock), StoreLockError> {
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

/// The lock a bootstrap or replacement holds for its whole call, beside the
/// store's directory.
fn bootstrap_lock_path(directory: &Path, id: StoreId) -> PathBuf {
    directory.with_file_name(format!(".coven-bootstrap-{id}.lock"))
}

/// The writer lock file of the store at `directory`, which a test waits on
/// (`when_reached`) to know an open finds the store's writer closing and
/// waits for it.
#[cfg(any(test, feature = "test-utils"))]
pub fn writer_lock(directory: &Path, id: StoreId) -> PathBuf {
    let [writer, _] = lock_paths(directory, id);
    writer
}

/// The bootstrap lock file of the store at `directory`, which a test waits
/// on to know an open finds the store's bootstrap closing and waits for it.
#[cfg(any(test, feature = "test-utils"))]
pub fn bootstrap_lock(directory: &Path, id: StoreId) -> PathBuf {
    bootstrap_lock_path(directory, id)
}

/// The file `kind` of the store's locks is held on.
#[cfg(any(test, feature = "test-utils"))]
pub(super) fn kind_lock(directory: &Path, id: StoreId, kind: LockKind) -> PathBuf {
    match kind {
        LockKind::Writer => writer_lock(directory, id),
        LockKind::Reader => lock_paths(directory, id)[1].clone(),
        LockKind::Bootstrap => bootstrap_lock(directory, id),
    }
}

/// Take the bootstrap lock without waiting. Under the layout lock, so a
/// deletion never removes the file between this open and this lock.
pub(super) fn lock_bootstrap(directory: &Path, id: StoreId) -> Result<FileLock, StoreLockError> {
    let _layout = lock_layout(directory.parent().expect("store parent"))?;
    try_lock(&bootstrap_lock_path(directory, id), id, false)
}

/// Every lock file named for a store, in the order deletion removes them.
/// The bootstrap lock goes first: while the writer or reader lock file
/// remains, creation refuses to reuse the id (`creation::create`).
fn deletion_lock_paths(directory: &Path, id: StoreId) -> [PathBuf; 3] {
    let [writer, readers] = lock_paths(directory, id);
    [bootstrap_lock_path(directory, id), writer, readers]
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

/// Lock a store, or what an interrupted deletion left of it, for deletion.
/// `lease` holds the bootstrap lock of a bootstrap deleting its own store,
/// which the deletion lock takes from it once it holds the store's other
/// locks; otherwise deletion takes that lock itself.
pub(super) fn for_deletion(
    directory: &Path,
    id: StoreId,
    lease: Option<&mut Option<RegisteredLock>>,
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
    let directory =
        if inspect_directory(&published)? { published } else { deletion_path(&published, id) };
    let paths = deletion_lock_paths(&directory, id);
    if !inspect_directory(&directory)? {
        let mut remaining = false;
        for path in &paths {
            remaining |= atomic_file::exists("inspect store lock", path)?;
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
    let [bootstrap, writer, readers] = paths;
    let others =
        || Ok::<_, StoreLockError>((try_lock(&writer, id, false)?, try_lock(&readers, id, false)?));
    let (bootstrap, (writer, readers)) = match lease {
        // First, so a bootstrap that has not yet made the directory is
        // refused before any other lock file is created for its id.
        None => (HeldBootstrap::Locked(try_lock(&bootstrap, id, false)?), others()?),
        // Last, so a refusal leaves the bootstrap holding its lock.
        Some(lease) => {
            let others = others()?;
            let held = lease.take().expect("a bootstrap hands over the bootstrap lock it holds");
            (HeldBootstrap::Leased(held), others)
        }
    };
    Ok(Some(StoreDeletionLock {
        store: LockedStore { _file: writer, directory, id },
        _readers: readers,
        bootstrap,
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
