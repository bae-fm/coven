use std::path::{Path, PathBuf};

use async_trait::async_trait;

/// The filename prefix an atomic blob write gives its in-progress temp sibling
/// (`.tmp.<uuid>`) before the fsync-then-rename that makes it the committed
/// destination. Every backend that writes through temp+rename names the temp with
/// this prefix, and the two readers that must tell a temp apart from a committed blob
/// — the eviction sweep ([`crate::blob::cache::evict_to_budget`]) and the host's
/// open-time orphan sweep — match on it. A file carrying this prefix is a write in
/// flight or a crash leftover, never a blob whose bytes are reclaimable.
pub const TEMP_BLOB_PREFIX: &str = ".tmp.";

/// Whether `path`'s file name marks it as an atomic-write temp sibling
/// ([`TEMP_BLOB_PREFIX`]) rather than a committed blob. A non-UTF-8 name is never one
/// of coven's temp names, so it is not a temp.
pub fn is_temp_blob_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(TEMP_BLOB_PREFIX))
}

pub struct PlaintextReader(Box<dyn PlatformPlaintextReader>);

#[derive(Debug, thiserror::Error)]
pub enum VerifiedReadError {
    #[error("local blob I/O error: {0}")]
    Io(String),
    #[error("local blob has {actual} bytes, expected {expected}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("local blob content hash is {actual}, expected {expected}")]
    HashMismatch { expected: String, actual: String },
    #[error("local blob range {offset}..{end} exceeds {size} bytes")]
    Range { offset: u64, end: u64, size: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum CommitNoReplaceError {
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("destination was created but the commit did not finish: {0}")]
    DestinationCreated(String),
    #[error("{0}")]
    Other(String),
}

impl PlaintextReader {
    pub async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String> {
        self.0.next_chunk(max).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PlatformPlaintextReader: crate::MaybeThreadSafe {
    async fn next_chunk(&mut self, max: usize) -> Result<Vec<u8>, String>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PlatformLocalBlobBackend: crate::MaybeThreadSafe {
    async fn open_reader(&self, path: &Path) -> Result<Box<dyn PlatformPlaintextReader>, String>;
    async fn file_len(&self, path: &Path) -> Result<u64, String>;
    async fn copy_atomic(&self, src: &Path, dst: &Path) -> Result<(), String>;
    async fn write_stream_atomic(
        &self,
        path: &Path,
        source: &mut dyn PlatformPlaintextReader,
    ) -> Result<u64, String>;
    async fn read(&self, path: &Path) -> Result<Vec<u8>, String>;
    async fn read_range(&self, path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String>;
    async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), String>;
    async fn commit_temp_no_replace(
        &self,
        temp: &Path,
        destination: &Path,
    ) -> Result<(), CommitNoReplaceError> {
        Err(CommitNoReplaceError::Other(format!(
            "atomic no-replace commit is unavailable for {} -> {}",
            temp.display(),
            destination.display()
        )))
    }
    /// Like [`write_atomic`](Self::write_atomic), but the committed directory
    /// entry also survives power loss: after the atomic rename, the parent
    /// directory is fsynced so the entry that now names the new bytes is itself
    /// durable, not only the bytes inside the file.
    ///
    /// [`write_atomic`](Self::write_atomic) deliberately skips that
    /// parent-directory fsync because a cache blob mirrors a cloud object —
    /// losing the post-rename entry to a crash only means a re-fetchable blob is
    /// absent. A caller whose file is the *sole* record of a fact that cannot be
    /// re-derived after reboot needs the entry to survive, and uses this.
    ///
    /// The default composes the two operations, which is correct on every
    /// backend. On a backend whose storage writes whole objects in place, with
    /// no temp-file/rename split and so no separate directory entry that can be
    /// lost, [`sync_parent_dir`](Self::sync_parent_dir) is a no-op and this is
    /// identical to [`write_atomic`](Self::write_atomic): the OPFS `wasm`
    /// backend rewrites the file the path already names through one
    /// sync-access-handle write, so the write is already durable through the
    /// operation.
    async fn write_atomic_durable(&self, path: &Path, bytes: &[u8]) -> Result<(), String> {
        self.write_atomic(path, bytes).await?;
        self.sync_parent_dir(path).await
    }
    async fn exists(&self, path: &Path) -> Result<bool, String>;
    async fn rename(&self, from: &Path, to: &Path) -> Result<(), String>;
    async fn remove_file(&self, path: &Path) -> Result<bool, String>;
    async fn remove_dir_all(&self, path: &Path) -> Result<bool, String>;
    async fn create_dir_all(&self, path: &Path) -> Result<(), String>;
    async fn sync_parent_dir(&self, _path: &Path) -> Result<(), String> {
        Ok(())
    }
    async fn walk_files(&self, dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String>;
}

pub async fn open_reader(path: &Path) -> Result<PlaintextReader, String> {
    backend().open_reader(path).await.map(PlaintextReader)
}

pub async fn file_len(path: &Path) -> Result<u64, String> {
    backend().file_len(path).await
}

pub async fn copy_atomic(src: &Path, dst: &Path) -> Result<(), String> {
    backend().copy_atomic(src, dst).await
}

pub async fn write_stream_atomic(
    path: &Path,
    source: &mut dyn PlatformPlaintextReader,
) -> Result<u64, String> {
    backend().write_stream_atomic(path, source).await
}

pub async fn read(path: &Path) -> Result<Vec<u8>, String> {
    backend().read(path).await
}

pub async fn read_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, String> {
    backend().read_range(path, offset, len).await
}

/// Read and authenticate a complete local plaintext file through one opened
/// handle. The returned bytes are the bytes that were hashed; no path is reopened
/// between verification and use.
pub async fn read_verified(
    path: &Path,
    expected_size: u64,
    expected_hash: &str,
) -> Result<Vec<u8>, VerifiedReadError> {
    let mut reader = open_reader(path).await.map_err(VerifiedReadError::Io)?;
    let mut bytes = Vec::new();
    loop {
        let chunk = reader
            .next_chunk(64 * 1024)
            .await
            .map_err(VerifiedReadError::Io)?;
        if chunk.is_empty() {
            break;
        }
        bytes.extend_from_slice(&chunk);
    }
    let actual_size = bytes.len() as u64;
    if actual_size != expected_size {
        return Err(VerifiedReadError::SizeMismatch {
            expected: expected_size,
            actual: actual_size,
        });
    }
    let actual_hash = crate::blob::content_hash(&bytes);
    if actual_hash != expected_hash {
        return Err(VerifiedReadError::HashMismatch {
            expected: expected_hash.to_string(),
            actual: actual_hash,
        });
    }
    Ok(bytes)
}

/// Authenticate the complete file through one opened handle, then return a
/// range from those same verified bytes.
pub async fn read_range_verified(
    path: &Path,
    expected_size: u64,
    expected_hash: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, VerifiedReadError> {
    let bytes = read_verified(path, expected_size, expected_hash).await?;
    let end = offset.checked_add(len).ok_or(VerifiedReadError::Range {
        offset,
        end: u64::MAX,
        size: expected_size,
    })?;
    if end > expected_size {
        return Err(VerifiedReadError::Range {
            offset,
            end,
            size: expected_size,
        });
    }
    let start = usize::try_from(offset).map_err(|_| VerifiedReadError::Range {
        offset,
        end,
        size: expected_size,
    })?;
    let end = usize::try_from(end).map_err(|_| VerifiedReadError::Range {
        offset,
        end,
        size: expected_size,
    })?;
    Ok(bytes[start..end].to_vec())
}

/// Authenticate `src` through one opened handle and atomically publish those
/// verified bytes at `dst`.
pub async fn copy_verified_atomic(
    src: &Path,
    dst: &Path,
    expected_size: u64,
    expected_hash: &str,
) -> Result<(), VerifiedReadError> {
    let bytes = read_verified(src, expected_size, expected_hash).await?;
    write_atomic(dst, &bytes)
        .await
        .map_err(VerifiedReadError::Io)
}

pub async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    backend().write_atomic(path, bytes).await
}

pub async fn commit_temp_no_replace(
    temp: &Path,
    destination: &Path,
) -> Result<(), CommitNoReplaceError> {
    backend().commit_temp_no_replace(temp, destination).await
}

pub async fn write_atomic_durable(path: &Path, bytes: &[u8]) -> Result<(), String> {
    backend().write_atomic_durable(path, bytes).await
}

pub async fn exists(path: &Path) -> Result<bool, String> {
    backend().exists(path).await
}

pub async fn rename(from: &Path, to: &Path) -> Result<(), String> {
    backend().rename(from, to).await
}

pub async fn remove_file(path: &Path) -> Result<bool, String> {
    backend().remove_file(path).await
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn remove_dir_all(path: &Path) -> Result<bool, String> {
    backend().remove_dir_all(path).await
}

pub async fn create_dir_all(path: &Path) -> Result<(), String> {
    backend().create_dir_all(path).await
}

pub async fn sync_parent_dir(path: &Path) -> Result<(), String> {
    backend().sync_parent_dir(path).await
}

/// Every committed file under `dir` as `(path, mtime_ms, size)`. In-flight
/// atomic-write temps ([`TEMP_BLOB_PREFIX`]) are excluded here, at the boundary,
/// so no reader of a blob tree ever counts or deletes a concurrent write's
/// temp; only the open-time orphan sweep (which reads directories itself)
/// touches temps.
pub async fn walk_files(dir: &Path) -> Result<Vec<(PathBuf, u64, u64)>, String> {
    let mut entries = backend().walk_files(dir).await?;
    entries.retain(|(path, _, _)| !is_temp_blob_path(path));
    Ok(entries)
}

#[cfg(not(target_arch = "wasm32"))]
static PLATFORM_BACKEND: std::sync::OnceLock<&'static dyn PlatformLocalBlobBackend> =
    std::sync::OnceLock::new();

#[cfg(target_arch = "wasm32")]
thread_local! {
    static PLATFORM_BACKEND: std::cell::Cell<Option<&'static dyn PlatformLocalBlobBackend>> =
        std::cell::Cell::new(None);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn register_platform_local_blob_backend(backend: &'static dyn PlatformLocalBlobBackend) {
    let _ = PLATFORM_BACKEND.set(backend);
}

#[cfg(target_arch = "wasm32")]
pub fn register_platform_local_blob_backend(backend: &'static dyn PlatformLocalBlobBackend) {
    PLATFORM_BACKEND.with(|slot| slot.set(Some(backend)));
}

#[cfg(not(target_arch = "wasm32"))]
fn backend() -> &'static dyn PlatformLocalBlobBackend {
    if let Some(backend) = PLATFORM_BACKEND.get().copied() {
        backend
    } else {
        #[cfg(any(test, feature = "test-utils"))]
        {
            &crate::local_blob_tests::TEST_LOCAL_BLOB_BACKEND
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            panic!("platform local blob backend is not registered")
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn backend() -> &'static dyn PlatformLocalBlobBackend {
    PLATFORM_BACKEND
        .with(|slot| slot.get())
        .expect("platform local blob backend is not registered")
}
