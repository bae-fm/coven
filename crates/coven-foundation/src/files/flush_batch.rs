//! The files one commit records, made durable together before it (Appendix
//! C.3). Each file is synced as it finishes; its folder and the drive's own
//! cache are flushed once for the whole batch.
//!
//! Per file, a batched write hands the bytes to the drive with a plain
//! `fsync` on Unix and `FlushFileBuffers` on Windows. On Apple systems that
//! `fsync` can leave them in the drive's cache, in any order. At the batch's
//! flush, each folder a file was made in is synced with `fsync`, then Apple
//! systems flush the drive's cache once per device with `F_FULLFSYNC`. The
//! cache is the device's, so that one flush covers every file the batch
//! already handed to it. Elsewhere on Unix `fsync` already reaches the
//! drive's permanent storage, and the batch's flush is the folders' `fsync`.
//! Windows writes coven's files through and has no folder to sync: its
//! `FlushFileBuffers` per file already flushed the drive's cache, and the
//! batch adds nothing.

use super::FileError;
#[cfg(unix)]
use super::StoreReadLock;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::Arc;

/// Files written for one commit, each synced as it finished, whose folders
/// and drive cache are flushed together by [`FlushBatch::flush`]. The commit
/// that records them takes the [`Flushed`] it returns, so nothing commits
/// ahead of its bytes reaching the drive.
#[must_use = "a batch's files may still be in the drive's cache until it is flushed"]
pub struct FlushBatch {
    files: usize,
    /// Each folder a file was made in, once, holding the store read lock of
    /// the first file made there until the flush ends.
    #[cfg(unix)]
    folders: Vec<BatchFolder>,
    #[cfg(any(test, feature = "test-utils"))]
    hook: Option<FlushHook>,
}

/// A batch's files reached the drive: what a commit recording them takes.
#[derive(Debug)]
#[must_use = "a flushed batch is for the commit that records its files"]
pub struct Flushed(());

/// Run where a batch's flush has returned, in tests; an error from it fails
/// the flush as the drive's would.
#[cfg(any(test, feature = "test-utils"))]
pub type FlushHook = std::sync::Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>;

/// An open folder a batch made a file in, known by its device and inode.
#[cfg(unix)]
pub(super) struct BatchFolder {
    path: PathBuf,
    handle: Arc<rustix::fd::OwnedFd>,
    stat: rustix::fs::Stat,
    _lock: Option<StoreReadLock>,
}

#[cfg(unix)]
impl BatchFolder {
    fn is(&self, other: &Self) -> bool {
        (self.stat.st_dev, self.stat.st_ino) == (other.stat.st_dev, other.stat.st_ino)
    }
}

#[cfg(unix)]
impl BatchFolder {
    /// Open the folder at `path`, keeping `lock` until the batch's flush ends.
    pub(super) fn open(path: PathBuf, lock: Option<StoreReadLock>) -> std::io::Result<Self> {
        use rustix::fs::{Mode, OFlags};
        let handle = rustix::fs::open(
            &path,
            OFlags::DIRECTORY | OFlags::RDONLY | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        Self::held(path, Arc::new(handle), lock)
    }

    /// A folder already open at `handle`.
    pub(super) fn held(
        path: PathBuf,
        handle: Arc<rustix::fd::OwnedFd>,
        lock: Option<StoreReadLock>,
    ) -> std::io::Result<Self> {
        let stat = rustix::fs::fstat(&*handle)?;
        Ok(Self {
            path,
            handle,
            stat,
            _lock: lock,
        })
    }
}

impl FlushBatch {
    /// An empty batch; flushing it does nothing.
    pub fn new() -> Self {
        Self {
            files: 0,
            #[cfg(unix)]
            folders: Vec::new(),
            #[cfg(any(test, feature = "test-utils"))]
            hook: None,
        }
    }

    /// Run `hook` where this batch's flush returns, if it holds a file.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_hook(mut self, hook: Option<FlushHook>) -> Self {
        self.hook = hook;
        self
    }

    /// Count one synced file, whose folder, on Unix, is `folder`.
    #[cfg(unix)]
    pub(super) fn add(&mut self, folder: BatchFolder) {
        self.files += 1;
        if !self.folders.iter().any(|known| known.is(&folder)) {
            self.folders.push(folder);
        }
    }

    /// Count one synced file.
    #[cfg(windows)]
    pub(super) fn add(&mut self) {
        self.files += 1;
    }

    /// Sync each folder, then flush each device's cache once, so every file
    /// of the batch is on the drive's permanent storage. A failure means the
    /// files may not be, and the commit that would record them doesn't run.
    pub async fn flush(self) -> Result<Flushed, FileError> {
        if self.files == 0 {
            return Ok(Flushed(()));
        }
        match tokio::task::spawn_blocking(move || self.flush_now()).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("batch flush task was cancelled: {error}"),
        }
    }

    fn flush_now(self) -> Result<Flushed, FileError> {
        #[cfg(unix)]
        {
            for folder in &self.folders {
                rustix::fs::fsync(&*folder.handle)
                    .map_err(|errno| FileError::at("sync folder", &folder.path, errno.into()))?;
            }
            // After every folder's `fsync`, so the flush covers them all.
            #[cfg(target_vendor = "apple")]
            {
                let mut devices = std::collections::BTreeSet::new();
                for folder in &self.folders {
                    if devices.insert(folder.stat.st_dev) {
                        rustix::fs::fcntl_fullfsync(&*folder.handle).map_err(|errno| {
                            FileError::at("flush drive cache", &folder.path, errno.into())
                        })?;
                    }
                }
            }
        }
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(hook) = &self.hook {
            hook().map_err(|source| {
                FileError::at("flush drive cache", std::path::Path::new("."), source)
            })?;
        }
        tracing::debug!(files = self.files, "flushed a batch of files");
        Ok(Flushed(()))
    }
}

#[cfg(test)]
#[path = "flush_batch_tests.rs"]
mod tests;
