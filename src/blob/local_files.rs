//! The local store: the home of a **host-provided Local** blob.
//!
//! A host-provided blob (the host hands coven the data — cover art, an artist
//! image, and keeps nothing) lives, while its release is **Local**, at
//! `storage/local/<namespace>/<id>`. This is not a copy of anything: it IS the
//! blob, its only instance, for as long as the blob is Local. The contrast with
//! the [`cache`](super::cache):
//!
//! - The cache holds copies of **Remote** blobs — re-fetchable from the cloud,
//!   evictable to a size budget, kept-or-dropped per pin. Its bytes are a mirror.
//! - The local store holds **Local** host-provided blobs — there is no cloud copy
//!   to re-fetch, so it is **never evicted**: the budget sweep walks only
//!   [`LibraryDir::cache_dir`], never `storage/local`.
//!
//! (A user-provided Local blob is the user's own file at a path, tracked as an
//! external ref — see `local_blob_refs` — not in this store.)
//!
//! The transitions move a host-provided blob between the local store and the cache:
//! `make_remote` uploads it and moves its bytes into the cache (now a cache copy of
//! the cloud blob, so it becomes Remote);
//! `make_local` restores it from the cache/cloud back into the local store (it
//! becomes Local). The read path checks the local store before the cache (see
//! [`cache::read_blob`](super::cache::read_blob)), so a host-provided Local blob is
//! served from here with no cloud round-trip.
//!
//! See the [blob concept tree](crate::blob) for where the local store sits in the
//! whole storage model.

use crate::library_dir::{LibraryDir, PathTokenError};

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
/// `storage/local/<namespace>/<id>`, so a read serves it locally while its release
/// is Local. The write is atomic ([`crate::local_blob::write_atomic`]), the same
/// torn-file guard every blob write relies on. There is NO eviction: the local
/// store is never swept (the bytes here ARE the blob while Local, not a re-fetchable
/// cache mirror).
pub async fn store(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
    bytes: &[u8],
) -> Result<(), LocalBlobError> {
    let dest = library_dir.local_blob_path(namespace, id)?;
    crate::local_blob::write_atomic(&dest, bytes)
        .await
        .map_err(LocalBlobError::Io)
}

/// Read a host-provided blob from the local store, or `None` when no file is stored
/// there. No cloud fallback — the local store is the blob's home while Local; a
/// `None` for a blob the gate says is Local is fail-loud corruption to its caller
/// ([`cache::read_blob`](super::cache::read_blob)), not a cache miss to refetch.
pub async fn read(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
) -> Result<Option<Vec<u8>>, LocalBlobError> {
    let path = library_dir.local_blob_path(namespace, id)?;
    match crate::local_blob::exists(&path).await {
        Ok(true) => crate::local_blob::read(&path)
            .await
            .map(Some)
            .map_err(LocalBlobError::Io),
        Ok(false) => Ok(None),
        Err(e) => Err(LocalBlobError::Io(e)),
    }
}

/// Serve `len` plaintext bytes at `offset` from the local store, or `None` when no
/// file is stored there. The local store holds the whole blob, so a request the
/// caller has bounded against the blob's length reads
/// exactly `len`; a short file is torn and fails loud in
/// [`crate::local_blob::read_range`].
pub async fn read_range(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>, LocalBlobError> {
    let path = library_dir.local_blob_path(namespace, id)?;
    match crate::local_blob::exists(&path).await {
        Ok(true) => crate::local_blob::read_range(&path, offset, len)
            .await
            .map(Some)
            .map_err(LocalBlobError::Io),
        Ok(false) => Ok(None),
        Err(e) => Err(LocalBlobError::Io(e)),
    }
}

/// Drop a host-provided blob from the local store, returning whether a file
/// was there. An already-absent file is the expected case (the blob became Remote,
/// or was never Local here), not an error.
pub async fn drop_blob(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
) -> Result<bool, LocalBlobError> {
    let path = library_dir.local_blob_path(namespace, id)?;
    crate::local_blob::remove_file(&path)
        .await
        .map_err(LocalBlobError::Io)
}
