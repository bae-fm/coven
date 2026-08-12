//! Installing a local file's complete contents so a crash leaves either the
//! old bytes or the new ones, never a partial write.
//!
//! This module is the crate's blessed implementation of the durable write, and
//! the only place allowed to call the raw sync methods that `clippy.toml`
//! disallows everywhere else. Keeping it to one place is what lets the
//! platform split below — Unix flushes the parent directory, nothing else can —
//! stay correct in a single spot instead of in every caller.
#![allow(clippy::disallowed_methods)]

use std::io::Write as _;
use std::path::{Path, PathBuf};

#[cfg(any(test, feature = "test-utils"))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Debug)]
pub(crate) enum FileSync {
    Enabled,
    Disabled,
    #[cfg(any(test, feature = "test-utils"))]
    ObservedDisabled(std::sync::Arc<AtomicUsize>),
}

impl FileSync {
    fn requested(&self) {
        #[cfg(any(test, feature = "test-utils"))]
        if let Self::ObservedDisabled(requests) = self {
            requests.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub(crate) async fn finish_async_write(
        &self,
        file: &mut tokio::fs::File,
    ) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;

        file.flush().await?;
        self.sync_file(file).await
    }

    async fn sync_file(&self, file: &tokio::fs::File) -> std::io::Result<()> {
        self.requested();
        match self {
            Self::Enabled => file.sync_all().await,
            Self::Disabled => Ok(()),
            #[cfg(any(test, feature = "test-utils"))]
            Self::ObservedDisabled(_) => Ok(()),
        }
    }

    pub(crate) fn sync_file_blocking(&self, file: &std::fs::File) -> std::io::Result<()> {
        self.requested();
        match self {
            Self::Enabled => file.sync_all(),
            Self::Disabled => Ok(()),
            #[cfg(any(test, feature = "test-utils"))]
            Self::ObservedDisabled(_) => Ok(()),
        }
    }

    pub(crate) async fn sync_parent(&self, path: &Path) -> Result<(), FileError> {
        let parent = parent_of(path)?;
        self.requested();
        match self {
            Self::Enabled => flush_directory(parent)
                .await
                .map_err(|source| FileError::at("fsync parent directory", parent, source)),
            Self::Disabled => Ok(()),
            #[cfg(any(test, feature = "test-utils"))]
            Self::ObservedDisabled(_) => Ok(()),
        }
    }

    pub(crate) fn sync_parent_blocking(&self, path: &Path) -> Result<(), FileError> {
        let parent = parent_of(path)?;
        self.requested();
        match self {
            Self::Enabled => flush_directory_blocking(parent)
                .map_err(|source| FileError::at("fsync parent directory", parent, source)),
            Self::Disabled => Ok(()),
            #[cfg(any(test, feature = "test-utils"))]
            Self::ObservedDisabled(_) => Ok(()),
        }
    }
}

/// A local filesystem operation that failed without erasing its I/O cause.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    #[error("{operation} {}: {source}", path.display())]
    Path {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{subject} is not a file: {}", path.display())]
    NotFile {
        subject: &'static str,
        path: PathBuf,
    },
    #[error("{operation} {} -> {}: {source}", from.display(), to.display())]
    BetweenPaths {
        operation: &'static str,
        from: PathBuf,
        to: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("path has no parent directory: {}", path.display())]
    NoParent { path: PathBuf },
    #[error("atomic stage in {} cannot commit to {}", stage_parent.display(), destination.display())]
    InvalidAtomicDestination {
        stage_parent: PathBuf,
        destination: PathBuf,
    },
    #[error("{subject} size overflow: {}", path.display())]
    SizeOverflow {
        subject: &'static str,
        path: PathBuf,
    },
    #[error("local file range is too large: {len} bytes")]
    RangeTooLarge { len: u64 },
    #[error("file modification time for {} predates the Unix epoch: {source}", path.display())]
    ModifiedBeforeUnixEpoch {
        path: PathBuf,
        #[source]
        source: std::time::SystemTimeError,
    },
    #[error("atomic write {}: {source}", path.display())]
    AtomicWrite {
        path: PathBuf,
        #[source]
        source: WriteError<std::io::Error>,
    },
    #[error("{operation}; rollback failed: {rollback}")]
    RollbackFailed {
        operation: Box<FileError>,
        rollback: Box<FileError>,
    },
}

impl FileError {
    pub fn at(operation: &'static str, path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Path {
            operation,
            path: path.into(),
            source,
        }
    }

    pub fn between(
        operation: &'static str,
        from: impl Into<PathBuf>,
        to: impl Into<PathBuf>,
        source: std::io::Error,
    ) -> Self {
        Self::BetweenPaths {
            operation,
            from: from.into(),
            to: to.into(),
            source,
        }
    }

    pub fn rollback(operation: FileError, rollback: FileError) -> Self {
        Self::RollbackFailed {
            operation: Box::new(operation),
            rollback: Box::new(rollback),
        }
    }
}

pub(crate) const TEMP_FILE_PREFIX: &str = ".tmp.";

/// A failed atomic write, tagged with whether the write had already committed.
///
/// The distinction is what a caller holding in-memory state needs: after
/// [`WriteError::committed`] the bytes are installed and readers already see
/// them, so the caller's own copy must move forward even though the call
/// failed. Before commit the target file is untouched and the caller keeps
/// what it had.
#[derive(Debug)]
pub enum WriteError<E> {
    /// The target file is untouched; nothing was installed.
    BeforeCommit(E),
    /// The rename landed and readers see the new bytes; only the durability
    /// work that follows it failed.
    AfterCommit(E),
}

impl<E> WriteError<E> {
    /// Whether the new bytes are already installed at the target path.
    pub fn committed(&self) -> bool {
        matches!(self, Self::AfterCommit(_))
    }

    pub fn into_inner(self) -> E {
        match self {
            Self::BeforeCommit(error) | Self::AfterCommit(error) => error,
        }
    }

    /// Convert the payload, preserving the commit phase.
    pub fn map<F>(self, convert: impl FnOnce(E) -> F) -> WriteError<F> {
        match self {
            Self::BeforeCommit(error) => WriteError::BeforeCommit(convert(error)),
            Self::AfterCommit(error) => WriteError::AfterCommit(convert(error)),
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for WriteError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforeCommit(error) => write!(f, "{error}"),
            Self::AfterCommit(error) => write!(f, "{error} (after the write committed)"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for WriteError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BeforeCommit(error) | Self::AfterCommit(error) => Some(error),
        }
    }
}

/// Install `bytes` as the complete contents of `path`.
///
/// The bytes go to a temporary sibling that is flushed to disk and then
/// renamed onto `path`, so a concurrent reader sees either the old file or the
/// whole new one. `path`'s parent directory must already exist.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), WriteError<std::io::Error>> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    let mut temp = tempfile::Builder::new()
        .prefix(TEMP_FILE_PREFIX)
        .tempfile_in(parent)
        .map_err(WriteError::BeforeCommit)?;
    temp.write_all(bytes).map_err(WriteError::BeforeCommit)?;
    temp.as_file()
        .sync_all()
        .map_err(WriteError::BeforeCommit)?;
    // Dropping the `NamedTempFile` on any failure above removes the sibling.
    temp.persist(path)
        .map_err(|error| WriteError::BeforeCommit(error.error))?;
    flush_directory_blocking(parent).map_err(WriteError::AfterCommit)
}

/// One unpublished file whose destination is chosen after its bytes have been
/// written. This is the streaming counterpart to [`write_atomic`]: callers can
/// compute a content address while implementing [`std::io::Write`], then commit the
/// completed file under that address with the owning durability policy's
/// file-sync, rename, and directory-sync sequence.
pub struct AtomicFileStage {
    parent: PathBuf,
    temp: tempfile::NamedTempFile,
    file_sync: FileSync,
}

impl AtomicFileStage {
    pub fn create_in(parent: &Path) -> Result<Self, std::io::Error> {
        Self::create_in_with_file_sync(parent, FileSync::Enabled)
    }

    pub(crate) fn create_in_with_file_sync(
        parent: &Path,
        file_sync: FileSync,
    ) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(parent)?;
        let temp = tempfile::Builder::new()
            .prefix(TEMP_FILE_PREFIX)
            .tempfile_in(parent)?;
        Ok(Self {
            parent: parent.to_path_buf(),
            temp,
            file_sync,
        })
    }

    pub fn commit(self, destination: &Path) -> Result<(), WriteError<FileError>> {
        if destination.parent() != Some(self.parent.as_path()) {
            return Err(WriteError::BeforeCommit(
                FileError::InvalidAtomicDestination {
                    stage_parent: self.parent,
                    destination: destination.to_path_buf(),
                },
            ));
        }
        let staged_path = self.temp.path().to_path_buf();
        self.file_sync
            .sync_file_blocking(self.temp.as_file())
            .map_err(|source| {
                WriteError::BeforeCommit(FileError::at("sync staged file", &staged_path, source))
            })?;
        self.temp.persist(destination).map_err(|error| {
            WriteError::BeforeCommit(FileError::between(
                "persist atomic stage",
                &staged_path,
                destination,
                error.error,
            ))
        })?;
        self.file_sync
            .sync_parent_blocking(destination)
            .map_err(WriteError::AfterCommit)
    }
}

impl std::io::Write for AtomicFileStage {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.temp.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.temp.flush()
    }
}

/// One local file whose complete contents are installed with a durable rename.
pub struct AtomicFile {
    path: PathBuf,
}

impl AtomicFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn read_optional(&self) -> Result<Option<Vec<u8>>, FileError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(FileError::at("read file", &self.path, source)),
        }
    }

    pub fn replace(&self, bytes: &[u8]) -> Result<(), FileError> {
        let parent = parent_of(&self.path)?;
        std::fs::create_dir_all(parent)
            .map_err(|source| FileError::at("create parent directory", parent, source))?;
        write_atomic(&self.path, bytes).map_err(|source| FileError::AtomicWrite {
            path: self.path.clone(),
            source,
        })
    }

    pub fn remove(&self) -> Result<(), FileError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FileError::at("remove file", &self.path, source)),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// The directory holding the entry that a rename onto `path` creates.
fn parent_of(path: &Path) -> Result<&Path, FileError> {
    path.parent().ok_or_else(|| FileError::NoParent {
        path: path.to_path_buf(),
    })
}

/// Flush the directory entry a rename onto `path` just wrote, so the installed
/// file survives a crash. Every durable rename in the crate ends here.
pub async fn sync_parent_dir(path: &Path) -> Result<(), FileError> {
    let parent = parent_of(path)?;
    flush_directory(parent)
        .await
        .map_err(|source| FileError::at("fsync parent directory", parent, source))
}

/// [`sync_parent_dir`] for callers that are not on the async runtime.
pub fn sync_parent_dir_blocking(path: &Path) -> Result<(), FileError> {
    let parent = parent_of(path)?;
    flush_directory_blocking(parent)
        .map_err(|source| FileError::at("fsync parent directory", parent, source))
}

#[cfg(unix)]
async fn flush_directory(directory: &Path) -> std::io::Result<()> {
    tokio::fs::File::open(directory).await?.sync_all().await
}

#[cfg(unix)]
fn flush_directory_blocking(directory: &Path) -> std::io::Result<()> {
    std::fs::File::open(directory)?.sync_all()
}

// Outside Unix the POSIX idiom has no counterpart: `FlushFileBuffers` on a
// Windows directory handle needs write access that opening a directory cannot
// grant, so the fsync fails with `ERROR_ACCESS_DENIED` (os error 5) and takes
// every blob install and spool removal down with it. NTFS journals its own
// metadata, so a rename's durability does not hang on a directory flush the way
// it does on POSIX — the rename plus the file's own `sync_all` is the durability
// the platform offers, which is why storage engines skip directory syncing here.
#[cfg(not(unix))]
async fn flush_directory(_directory: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn flush_directory_blocking(_directory: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_atomic_replaces_existing_contents() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("config.yaml");
        std::fs::write(&path, b"old").expect("seed");

        write_atomic(&path, b"new").expect("atomic write");

        assert_eq!(std::fs::read(&path).expect("read"), b"new");
    }

    #[test]
    fn write_atomic_leaves_no_temporary_sibling() {
        let directory = tempfile::tempdir().expect("temporary directory");

        write_atomic(&directory.path().join("config.yaml"), b"bytes").expect("atomic write");

        let leftovers: Vec<_> = std::fs::read_dir(directory.path())
            .expect("read dir")
            .map(|entry| entry.expect("entry").file_name())
            .filter(|name| name.to_string_lossy().starts_with(TEMP_FILE_PREFIX))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn write_atomic_requires_an_existing_parent() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("missing").join("config.yaml");

        let error = write_atomic(&path, b"bytes").expect_err("absent parent");

        assert!(!error.committed());
        assert_eq!(error.into_inner().kind(), std::io::ErrorKind::NotFound);
        assert!(!path.exists());
    }

    #[test]
    fn streaming_stage_commits_only_the_complete_file() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("payload");
        let mut stage = AtomicFileStage::create_in(directory.path()).expect("create stage");

        stage.write_all(b"first ").expect("write first part");
        stage.write_all(b"second").expect("write second part");
        assert!(!path.exists());
        stage.commit(&path).expect("commit stage");

        assert_eq!(std::fs::read(path).expect("read payload"), b"first second");
    }

    /// The durable-rename tail must succeed on every platform coven supports,
    /// not only the ones with a POSIX directory fsync.
    #[test]
    fn write_atomic_commits_without_a_posix_directory_fsync() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("installed");

        write_atomic(&path, b"bytes").expect("atomic write");
        sync_parent_dir_blocking(&path).expect("sync parent directory");

        assert_eq!(std::fs::read(&path).expect("read"), b"bytes");
    }

    #[tokio::test]
    async fn sync_parent_dir_accepts_an_installed_file() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("installed");
        tokio::fs::write(&path, b"bytes").await.expect("write");

        sync_parent_dir(&path).await.expect("sync parent directory");
    }

    #[tokio::test]
    async fn sync_parent_dir_rejects_a_path_with_no_parent() {
        let error = sync_parent_dir(Path::new("/"))
            .await
            .expect_err("root has no parent");

        assert!(matches!(
            error,
            FileError::NoParent { path } if path == Path::new("/")
        ));
    }

    #[test]
    fn atomic_file_round_trips_through_a_created_parent() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let file = AtomicFile::new(directory.path().join("nested").join("config.yaml"));

        assert_eq!(file.read_optional().expect("absent read"), None);
        file.replace(b"first").expect("install");
        file.replace(b"second").expect("replace");

        assert_eq!(
            file.read_optional().expect("read"),
            Some(b"second".to_vec())
        );
    }
}
