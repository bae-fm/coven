//! The local store: the home of a **host-provided Local** blob.
//!
//! A host-provided blob (the host hands coven the data — cover art, an artist
//! image, and keeps nothing) lives, while its release is **Local**, at
//! `storage/local/<namespace>/<id>/<content_hash>`. This is not a copy of anything: it IS the
//! blob, its only instance, for as long as the blob is Local. The contrast with
//! the [`cache`](super::cache):
//!
//! - The cache holds copies of **Remote** blobs — re-fetchable from the cloud,
//!   evictable to a size budget, kept-or-dropped per pin. Its bytes are a mirror.
//! - The local store holds **Local** host-provided blobs — there is no cloud copy
//!   to re-fetch, so it is **never evicted**: the budget sweep walks only
//!   [`StoreDir::cache_dir`], never `storage/local`.
//!
//! (A user-provided Local blob is the user's own file at a path, tracked as an
//! external ref — see `local_blob_refs` — not in this store.)
//!
//! A host-provided Remote blob can also have a short-lived upload staging file here:
//! `CovenHandle::write` stores newly written bytes in the local store, then the sync
//! cycle uploads those bytes and drops this file. The Remote read path checks
//! `pinned/` and `cache/` first, then reads this staging file for host-provided
//! Remote blobs only, and finally reads the cloud. A host-provided Local blob still
//! reads this store as its required source.
//!
//! See the [blob concept tree](crate::blob) for where the local store sits in the
//! whole storage model.

use crate::store_dir::{PathTokenError, StoreDir};
use std::path::PathBuf;

/// Why a local-store operation failed.
#[derive(Debug)]
pub enum LocalBlobError {
    /// A `namespace`/`id` that can't form a safe path — bad data that could escape
    /// the store. The blob is refused before any path is built.
    Path(PathTokenError),
    /// A local-disk failure (a store write, a read, a remove). Carries a
    /// human-readable cause.
    Io(String),
}

impl std::fmt::Display for LocalBlobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalBlobError::Path(e) => write!(f, "local blob path error: {e}"),
            LocalBlobError::Io(e) => write!(f, "local blob I/O error: {e}"),
        }
    }
}

impl std::error::Error for LocalBlobError {}

impl From<PathTokenError> for LocalBlobError {
    fn from(e: PathTokenError) -> Self {
        LocalBlobError::Path(e)
    }
}

/// Store a host-provided blob in the local store at
/// `storage/local/<namespace>/<id>/<content_hash>`, so a read serves it locally while its release
/// is Local. Reads verify the file length against the blob's declared row size.
/// There is NO eviction: the local store is never swept (the bytes here ARE the
/// blob while Local, not a re-fetchable cache mirror).
#[cfg(any(test, feature = "test-utils"))]
pub async fn store(
    store_dir: &StoreDir,
    namespace: &str,
    id: &str,
    bytes: &[u8],
) -> Result<(), LocalBlobError> {
    let hash = crate::blob::content_hash(bytes);
    let dest = store_dir.local_blob_path(namespace, id, &hash)?;
    crate::local_blob::write_atomic(&dest, bytes)
        .await
        .map_err(LocalBlobError::Io)
}

/// Read a host-provided blob from the local store, or `None` when no file is stored
/// there. For a host-provided Local blob, absence is fail-loud corruption to
/// [`cache::read_blob`](super::cache::read_blob). For a host-provided Remote blob,
/// absence means the upload staging file is gone and the Remote read continues to
/// cloud.
pub async fn read(
    store_dir: &StoreDir,
    namespace: &str,
    id: &str,
    expected_size: u64,
    expected_hash: &str,
) -> Result<Option<Vec<u8>>, LocalBlobError> {
    match present_path(store_dir, namespace, id, expected_hash).await? {
        Some(path) => crate::local_blob::read_verified(&path, expected_size, expected_hash)
            .await
            .map(Some)
            .map_err(|error| LocalBlobError::Io(error.to_string())),
        None => Ok(None),
    }
}

/// Return the local-store file path when a host-provided blob is present and has
/// exactly `expected_size` bytes.
pub async fn path_if_present(
    store_dir: &StoreDir,
    namespace: &str,
    id: &str,
    expected_size: u64,
    expected_hash: &str,
) -> Result<Option<PathBuf>, LocalBlobError> {
    let path = store_dir.local_blob_path(namespace, id, expected_hash)?;
    match crate::local_blob::exists(&path).await {
        Ok(true) => {
            ensure_file_len(&path, expected_size).await?;
            Ok(Some(path))
        }
        Ok(false) => Ok(None),
        Err(e) => Err(LocalBlobError::Io(e)),
    }
}

/// Serve `len` plaintext bytes at `offset` from the local store, or `None` when no
/// file is stored there. The local store holds the whole blob, so a request the
/// caller has bounded against the blob's length reads exactly `len`; a file whose
/// length differs from the declared size fails before the range read.
pub async fn read_range(
    store_dir: &StoreDir,
    namespace: &str,
    id: &str,
    expected_size: u64,
    expected_hash: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>, LocalBlobError> {
    match present_path(store_dir, namespace, id, expected_hash).await? {
        Some(path) => {
            crate::local_blob::read_range_verified(&path, expected_size, expected_hash, offset, len)
                .await
                .map(Some)
                .map_err(|error| LocalBlobError::Io(error.to_string()))
        }
        None => Ok(None),
    }
}

/// Copy a present local-store blob only after authenticating its complete bytes
/// through the same opened source handle.
pub async fn copy_verified_if_present(
    store_dir: &StoreDir,
    namespace: &str,
    id: &str,
    expected_size: u64,
    expected_hash: &str,
    destination: &std::path::Path,
) -> Result<bool, LocalBlobError> {
    let Some(path) = present_path(store_dir, namespace, id, expected_hash).await? else {
        return Ok(false);
    };
    crate::local_blob::copy_verified_atomic(&path, destination, expected_size, expected_hash)
        .await
        .map_err(|error| LocalBlobError::Io(error.to_string()))?;
    Ok(true)
}

async fn present_path(
    store_dir: &StoreDir,
    namespace: &str,
    id: &str,
    expected_hash: &str,
) -> Result<Option<PathBuf>, LocalBlobError> {
    let path = store_dir.local_blob_path(namespace, id, expected_hash)?;
    match crate::local_blob::exists(&path).await {
        Ok(true) => Ok(Some(path)),
        Ok(false) => Ok(None),
        Err(error) => Err(LocalBlobError::Io(error)),
    }
}

async fn ensure_file_len(path: &std::path::Path, expected_size: u64) -> Result<(), LocalBlobError> {
    let actual = crate::local_blob::file_len(path)
        .await
        .map_err(LocalBlobError::Io)?;
    if actual != expected_size {
        return Err(LocalBlobError::Io(format!(
            "local blob {} has {actual} bytes, expected {expected_size}",
            path.display()
        )));
    }
    Ok(())
}

/// Drop a host-provided blob from the local store, returning whether a file
/// was there. An already-absent file is the expected case (the blob became Remote,
/// or was never Local here), not an error.
pub async fn drop_blob(
    store_dir: &StoreDir,
    namespace: &str,
    id: &str,
    content_hash: &str,
) -> Result<bool, LocalBlobError> {
    let path = store_dir.local_blob_path(namespace, id, content_hash)?;
    crate::local_blob::remove_file(&path)
        .await
        .map_err(LocalBlobError::Io)
}
