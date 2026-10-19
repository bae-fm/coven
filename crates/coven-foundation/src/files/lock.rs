//! Store locks live beside the directory so Windows can unpublish it.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::files::bootstrap::BootstrapLease;
use crate::files::{atomic_file, settings, FileError, SettingsError, StoreSettings};
use crate::id_source::StoreId;

use super::lock_registry as registry;
use registry::LockKind;
pub(super) use registry::RegisteredLock;
pub use registry::StoreClosingGuard;

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

/// A held lock on the cache's range writes: shared by a write in progress,
/// exclusive while opening finds abandoned ones. Dropping it unlocks.
#[derive(Debug)]
pub struct CacheWriteLock {
    _file: LockedFile,
}

/// The cache-write lock file, in the store directory beside the cache folder,
/// which holds only cache entries.
const CACHE_WRITES: &str = ".coven-cache-writes.lock";

/// Wait for the cache-write lock, shared or exclusive. Unlike the store
/// locks it waits: a range write is short, and opening needs them finished.
pub(super) fn cache_writes(store: &Path, shared: bool) -> Result<CacheWriteLock, FileError> {
    let path = store.join(CACHE_WRITES);
    let file = open_lock(&path)?;
    let locked = if shared {
        file.lock_shared()
    } else {
        file.lock()
    };
    locked.map_err(|source| FileError::at("lock cache writes", &path, source))?;
    Ok(CacheWriteLock {
        _file: LockedFile { file },
    })
}

/// Exclusive access to the writer, readers and bootstrap, authorizing
/// deletion. Holding the bootstrap lock keeps every restore, join and
/// replacement of the store out, including a `reset_device` or
/// `restore_device` that holds only that lock between the steps of an
/// unfinished swap (§19.3), and lets deletion remove its lock file too.
#[derive(Debug)]
pub struct StoreDeletionLock {
    store: LockedStore<LockedFile>,
    _readers: LockedFile,
    bootstrap: HeldBootstrap,
}

/// How a deletion holds the store's bootstrap lock.
#[derive(Debug)]
enum HeldBootstrap {
    /// Taken by the deletion itself.
    Locked(LockedFile),
    /// The lease of a bootstrap cancelling its own unpublished store.
    Leased(Arc<BootstrapLease>),
}

impl HeldBootstrap {
    fn release(self) {
        match self {
            Self::Locked(file) => drop(file),
            Self::Leased(lease) => lease.release(),
        }
    }
}

impl StoreDeletionLock {
    /// Unpublish the directory and remove its contents before releasing and
    /// removing its sibling locks. Retry failures through `lock_for_deletion`,
    /// including a failure after only the lock files remain.
    pub fn remove_directory(mut self) -> Result<(), FileError> {
        let root = self.store.directory.parent().expect("store parent");
        // Every lock acquisition and creation also takes this lock. None can
        // open a lock file between its release here and the removal of its
        // name, so no one holds an unlinked lock while another locks a new
        // file at the same path.
        let _layout = lock_layout(root)?;
        let paths = deletion_lock_paths(&self.store.directory, self.store.id);
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
        // Release before removing, on every platform. Windows keeps a file
        // deleted while a handle is open until that handle closes, and
        // refuses to open its name meanwhile, which would refuse the next
        // lock of this store. Under the layout lock, releasing first is as
        // safe as removing a held file.
        let Self {
            store,
            _readers,
            bootstrap,
        } = self;
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
        |source| FileError::AfterRemove {
            path: directory.to_owned(),
            source: Box::new(source),
        },
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
    /// began it: a restore or join with its code.
    #[error("store {0} requires restore or join to finish bootstrap")]
    BootstrapPending(StoreId),
    /// A replacement of this device's installation swapped in but did not
    /// finish. Only the call it names finishes it; the other is refused
    /// before anything changes. Deletion removes it instead (§19.3).
    #[error("store {store} requires {by} to finish replacing this device's installation")]
    ReplacementUnfinished {
        /// The store whose installation is being replaced.
        store: StoreId,
        /// The call that began the replacement.
        by: super::ReplacedBy,
    },
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

impl LockedFile {
    #[cfg(test)]
    pub(super) fn inherited_file(&self) -> File {
        self.file.try_clone().unwrap()
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

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn acquire(directory: &Path, id: StoreId) -> Result<StoreLock, StoreLockError> {
    let directory = resolve_directory(directory)?;
    let file = registry::try_acquire(&directory, id, LockKind::Writer)?;
    Ok(StoreLock {
        store: LockedStore {
            _file: file,
            directory,
            id,
        },
    })
}

pub(crate) async fn acquire_for_open(
    directory: &Path,
    id: StoreId,
) -> Result<StoreLock, StoreLockError> {
    let directory = resolve_directory(directory)?;
    let file = registry::acquire_registered(&directory, id, LockKind::Writer).await?;
    Ok(StoreLock {
        store: LockedStore {
            _file: file,
            directory,
            id,
        },
    })
}

pub(super) async fn acquire_bootstrap(
    directory: &Path,
    id: StoreId,
) -> Result<RegisteredLock, StoreLockError> {
    registry::acquire_registered(directory, id, LockKind::Bootstrap).await
}

fn resolve_directory(directory: &Path) -> Result<PathBuf, FileError> {
    std::fs::canonicalize(directory)
        .map_err(|source| FileError::at("resolve store directory", directory, source))
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

pub(super) fn lock_existing(
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

/// The lock a bootstrap or replacement holds for its whole call, beside the
/// store's directory.
fn bootstrap_lock_path(directory: &Path, id: StoreId) -> PathBuf {
    directory.with_file_name(format!(".coven-bootstrap-{id}.lock"))
}

/// Take the bootstrap lock without waiting. Under the layout lock, so a
/// deletion never removes the file between this open and this lock.
pub(super) fn lock_bootstrap(directory: &Path, id: StoreId) -> Result<LockedFile, StoreLockError> {
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
/// `lease` is the bootstrap lock of a bootstrap deleting its own store;
/// otherwise deletion takes that lock itself.
pub(super) fn for_deletion(
    directory: &Path,
    id: StoreId,
    lease: Option<Arc<BootstrapLease>>,
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
    // First, so a bootstrap that has not yet made the directory is refused
    // before any other lock file is created for its id.
    let bootstrap = match lease {
        Some(lease) => HeldBootstrap::Leased(lease),
        None => HeldBootstrap::Locked(try_lock(&bootstrap, id, false)?),
    };
    Ok(Some(StoreDeletionLock {
        store: LockedStore {
            _file: try_lock(&writer, id, false)?,
            directory,
            id,
        },
        _readers: try_lock(&readers, id, false)?,
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
