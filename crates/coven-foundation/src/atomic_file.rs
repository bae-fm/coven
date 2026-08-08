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

/// One local file whose complete contents are installed with a durable rename.
pub struct AtomicFile {
    path: PathBuf,
}

impl AtomicFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn read_optional(&self) -> Result<Option<Vec<u8>>, String> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("read {}: {error}", self.path.display())),
        }
    }

    pub fn replace(&self, bytes: &[u8]) -> Result<(), String> {
        let parent = parent_of(&self.path)?;
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create parent directory {}: {error}", parent.display()))?;
        write_atomic(&self.path, bytes)
            .map_err(|error| format!("atomic write {}: {error}", self.path.display()))
    }

    pub fn remove(&self) -> Result<(), String> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("remove {}: {error}", self.path.display())),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// The directory holding the entry that a rename onto `path` creates.
fn parent_of(path: &Path) -> Result<&Path, String> {
    path.parent()
        .ok_or_else(|| format!("path has no parent directory: {}", path.display()))
}

/// Flush the directory entry a rename onto `path` just wrote, so the installed
/// file survives a crash. Every durable rename in the crate ends here.
pub async fn sync_parent_dir(path: &Path) -> Result<(), String> {
    let parent = parent_of(path)?;
    flush_directory(parent)
        .await
        .map_err(|error| format!("fsync parent directory {}: {error}", parent.display()))
}

/// [`sync_parent_dir`] for callers that are not on the async runtime.
pub fn sync_parent_dir_blocking(path: &Path) -> Result<(), String> {
    let parent = parent_of(path)?;
    flush_directory_blocking(parent)
        .map_err(|error| format!("fsync parent directory {}: {error}", parent.display()))
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

        assert!(error.contains("no parent directory"), "{error}");
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
