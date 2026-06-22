//! The device-local blob cache: bytes on disk, keyed by blob id, with the folder
//! the file lives in as the only retention truth.
//!
//! There is no cache table. A blob is in **exactly one** of two folders under the
//! library dir, or in neither:
//!
//! - `storage/pinned/{ab}/{cd}/<id>` — protected, budget-exempt. A `Mirrored` blob
//!   system-pinned on pull (cover art every device keeps), or an `OnDemand` blob the
//!   user pinned for offline.
//! - `storage/cache/{ab}/{cd}/<id>` — opportunistic, evictable. Fetched-on-read
//!   `OnDemand` audio, or bytes a host staged before a push uploads them.
//! - neither — remote-only. No file, no row; fetched from the cloud on the next read.
//!
//! Presence is the file on disk; pinned-ness is which folder. Nothing the two
//! `readdir`s can't answer, so no metadata sidecar to keep in sync with the disk.
//! Every write is atomic ([`crate::local_blob::write_atomic`]) so a crash can't leave
//! a torn file a read would trust, and pin/unpin are a `rename` within `storage/`
//! (one filesystem, atomic) so a blob never appears in both folders or neither
//! mid-move.
//!
//! [`read_blob`] returns the entire blob (a miss fetches + decrypts it and
//! populates `cache/`); [`open_blob_stream`] serves a plaintext byte range for a
//! host streaming or seeking (a miss range-reads + decrypts from the cloud but
//! populates nothing — a partial file would be read as the whole blob, since
//! presence is the only truth). [`clear_cache`] drops all of `cache/` in one
//! sweep — it does not evict selectively by a size budget; a pinned blob (in
//! `pinned/`) is exempt because it lives in the other folder.

use crate::blob::{BlobRef, BlobSync};
use crate::database::Database;
use crate::library_dir::{LibraryDir, PathTokenError};
use crate::sync::storage::{StorageError, SyncStorage};

/// Why a blob-cache operation failed.
#[derive(Debug)]
pub enum BlobCacheError {
    /// A blob `id`/`namespace`/`cloud_path` that can't form a safe path — bad data
    /// that could escape the library dir or can't be partitioned. The blob is
    /// refused before any path is built (the same gate the pull runs).
    Path(PathTokenError),
    /// A cloud read failed: the blob isn't in the cloud, or the backend errored
    /// (surfaced from [`SyncStorage::get_blob`]).
    Storage(StorageError),
    /// A local-disk failure (a cache write, a folder move, the `clear_cache`
    /// sweep), or a scope that couldn't be resolved to an encryption key. Carries a
    /// human-readable cause.
    Io(String),
}

impl std::fmt::Display for BlobCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobCacheError::Path(e) => write!(f, "blob path error: {e}"),
            BlobCacheError::Storage(e) => write!(f, "blob cache storage error: {e}"),
            BlobCacheError::Io(e) => write!(f, "blob cache I/O error: {e}"),
        }
    }
}

impl std::error::Error for BlobCacheError {}

impl From<PathTokenError> for BlobCacheError {
    fn from(e: PathTokenError) -> Self {
        BlobCacheError::Path(e)
    }
}

impl From<StorageError> for BlobCacheError {
    fn from(e: StorageError) -> Self {
        BlobCacheError::Storage(e)
    }
}

/// Stage host bytes into the cache so a later read serves them locally and a pin can
/// promote them to `pinned/` without a cloud round-trip. The bytes land at
/// `storage/cache/<id>` (unpinned, evictable) via an atomic write.
///
/// This is for bytes a host wants a local copy of that the push then uploads — e.g.
/// a release the user is taking from cloud-only to pinned, whose audio bae copies in
/// before the push reads it. It is NOT for an unmanaged (invisible-to-coven) file or
/// a cloud-only source that stays external: those are never ingested, only read by
/// the push from [`BlobRef::local_path`].
pub async fn write_blob(
    library_dir: &LibraryDir,
    blob: &BlobRef,
    bytes: &[u8],
) -> Result<(), BlobCacheError> {
    let dest = library_dir.cache_blob_path(&blob.id)?;
    crate::local_blob::write_atomic(&dest, bytes)
        .await
        .map_err(BlobCacheError::Io)
}

/// Read a blob's whole contents, serving the local file on a hit and fetching from
/// the cloud (into `cache/`) on a miss.
///
/// A hit is the file existing in `pinned/<id>` OR `cache/<id>` — its existence is
/// the entire test, no table consulted. A miss resolves the blob's scope to its
/// encryption key, downloads + decrypts it via [`SyncStorage::get_blob`], writes it
/// atomically to `cache/<id>` (unpinned — a plain read populates the evictable
/// cache, never the protected one), and returns the bytes it just fetched.
pub async fn read_blob(
    db: &Database,
    library_dir: &LibraryDir,
    storage: &dyn SyncStorage,
    blob: &BlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    let pinned = library_dir.pinned_blob_path(&blob.id)?;
    let cache = library_dir.cache_blob_path(&blob.id)?;

    // A hit in either folder serves the file. Check pinned first only because that
    // is where a kept-local blob is the common case; either location is equally a
    // hit. A failure to even check existence (broken filesystem) is surfaced, not
    // collapsed into "miss" — re-downloading over a present file would be wasteful
    // and could mask a real fault.
    for path in [&pinned, &cache] {
        match crate::local_blob::exists(path).await {
            Ok(true) => {
                return crate::local_blob::read(path)
                    .await
                    .map_err(BlobCacheError::Io);
            }
            Ok(false) => {}
            Err(e) => return Err(BlobCacheError::Io(e)),
        }
    }

    // Miss: fetch from the cloud and populate the evictable cache.
    let bytes = fetch_from_cloud(db, storage, blob).await?;
    crate::local_blob::write_atomic(&cache, &bytes)
        .await
        .map_err(BlobCacheError::Io)?;
    Ok(bytes)
}

/// Serve `len` plaintext bytes of a blob starting at `offset`, for a host
/// streaming or seeking it (playback) without loading the whole file. The ranged
/// sibling of [`read_blob`]: same arguments plus `(source_size, offset, len)`,
/// returning the plaintext slice.
///
/// `source_size` is the blob's plaintext length — the host knows it (the row that
/// owns the blob carries it) and both serving paths need it to bound the range
/// (the cloud path also needs it to find the covering encrypted chunks; see
/// [`SyncStorage::read_blob_range`]). The range is validated once here, against
/// `source_size`, so a request behaves identically whether it is served from the
/// local file or the cloud: `len == 0` is an empty result, and an `offset + len`
/// past `source_size` (or an overflow) is an error, never a short read — the same
/// contract [`crate::sync::cloud_storage::BlobRangeReader::read`] enforces.
///
/// **Cache hit** (`pinned/<id>` OR `cache/<id>` exists): the local file is the
/// whole plaintext, so the slice is read straight off disk at `offset` — no
/// decryption, no cloud. **Cache miss**: the range is fetched and decrypted from
/// the cloud via [`SyncStorage::read_blob_range`]. A miss **never writes a cache
/// file** — a ranged read populates nothing, because a truncated/partial file
/// under `cache/<id>` would be read as the whole blob by [`read_blob`] (presence
/// is the only truth). Only the whole-file [`read_blob`] populates the cache.
///
/// As in [`read_blob`], a failure to even check a file's existence is surfaced,
/// never collapsed into a miss (which would re-fetch over a present file and could
/// mask a real fault).
pub async fn open_blob_stream(
    db: &Database,
    library_dir: &LibraryDir,
    storage: &dyn SyncStorage,
    blob: &BlobRef,
    source_size: u64,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, BlobCacheError> {
    // The range contract, applied once for both serving paths. A zero-length read
    // is empty without touching disk or cloud; an out-of-range read is an error
    // before either path runs, so the local-file path can't silently short-read.
    if len == 0 {
        return Ok(Vec::new());
    }
    let end = offset.checked_add(len).ok_or_else(|| {
        BlobCacheError::Io(format!(
            "blob range overflow for {}: offset={offset}, len={len}",
            blob.id
        ))
    })?;
    if end > source_size {
        return Err(BlobCacheError::Io(format!(
            "blob range {offset}..{end} for {} exceeds blob size {source_size}",
            blob.id
        )));
    }

    let pinned = library_dir.pinned_blob_path(&blob.id)?;
    let cache = library_dir.cache_blob_path(&blob.id)?;

    // A hit in either folder serves the slice from the local plaintext file. The
    // file is the whole blob (cache writes are whole-file), so the validated range
    // is in bounds and `read_range` reads exactly `len` bytes. An existence-check
    // failure is surfaced, not read as a miss.
    for path in [&pinned, &cache] {
        match crate::local_blob::exists(path).await {
            Ok(true) => {
                return crate::local_blob::read_range(path, offset, len)
                    .await
                    .map_err(BlobCacheError::Io);
            }
            Ok(false) => {}
            Err(e) => return Err(BlobCacheError::Io(e)),
        }
    }

    // Miss: serve the range from the cloud (range read + decrypt over the resolved
    // scope) WITHOUT writing a cache file — a partial file would be mistaken for
    // the whole blob by `read_blob`. Only `read_blob` populates the cache.
    crate::library_dir::validate_path_token(&blob.namespace)?;
    crate::library_dir::validate_path_token(&blob.id)?;
    if let Some(cloud_path) = blob.cloud_path.as_deref() {
        crate::library_dir::validate_cloud_path(cloud_path)?;
    }
    let resolved = db
        .resolve_blob_scope(blob.scope.clone())
        .await
        .map_err(|e| BlobCacheError::Io(format!("resolve blob scope for {}: {e}", blob.id)))?;
    storage
        .read_blob_range(
            &blob.namespace,
            &blob.id,
            resolved,
            blob.cloud_path.as_deref(),
            source_size,
            offset,
            len,
        )
        .await
        .map_err(BlobCacheError::Storage)
}

/// Ensure a blob is local AND protected: present in `storage/pinned/<id>`, exempt
/// from the evictable cache. A pin POPULATES — if the blob isn't cached it is
/// fetched first — so it is not a flag flip. Idempotent.
///
/// Three cases per blob: already in `pinned/` (nothing to do); in `cache/` (rename
/// it into `pinned/`, so a previously-staged or read-populated blob is promoted with
/// no cloud fetch); in neither (fetch from the cloud and write straight to
/// `pinned/`). `&[BlobRef]` rather than ids because the fetch needs the blob's cloud
/// coordinates (namespace, scope, cloud_path) an id alone lacks.
pub async fn pin(
    db: &Database,
    library_dir: &LibraryDir,
    storage: &dyn SyncStorage,
    blobs: &[BlobRef],
) -> Result<(), BlobCacheError> {
    for blob in blobs {
        let pinned = library_dir.pinned_blob_path(&blob.id)?;
        let cache = library_dir.cache_blob_path(&blob.id)?;

        // Already protected — idempotent no-op. A failure to even check existence
        // (broken filesystem) is surfaced, not collapsed into "absent": fetching and
        // overwriting a present pinned blob would be wasteful and could mask a real
        // fault, the same posture `read_blob` takes on its hit check.
        match crate::local_blob::exists(&pinned).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => return Err(BlobCacheError::Io(e)),
        }

        // Staged or read-populated in the evictable cache — promote it with a rename
        // (no cloud fetch). `rename` within `storage/` is atomic on one filesystem,
        // so the blob is never in both folders or neither mid-move. An `exists`
        // failure here is surfaced too, never read as "not cached" (which would
        // re-fetch over a present file).
        match crate::local_blob::exists(&cache).await {
            Ok(true) => {
                rename_within_storage(&cache, &pinned).await?;
                continue;
            }
            Ok(false) => {}
            Err(e) => return Err(BlobCacheError::Io(e)),
        }

        // In neither folder — fetch from the cloud straight into `pinned/`.
        let bytes = fetch_from_cloud(db, storage, blob).await?;
        crate::local_blob::write_atomic(&pinned, &bytes)
            .await
            .map_err(BlobCacheError::Io)?;
    }
    Ok(())
}

/// Drop a blob's protection: move `storage/pinned/<id>` → `storage/cache/<id>` so
/// the file stays (still readable) but is now evictable. Not a delete.
///
/// Only valid on an `OnDemand` blob. A `Mirrored` blob's pin is a SYSTEM pin —
/// re-asserted every pull because the blob is part of having the library — so
/// unpinning it is meaningless and REJECTED with an error (fail loud, not a silent
/// skip that would leave the caller thinking it succeeded). The class is checked
/// before any file is touched.
pub async fn unpin(library_dir: &LibraryDir, blobs: &[BlobRef]) -> Result<(), BlobCacheError> {
    for blob in blobs {
        if blob.sync == BlobSync::Mirrored {
            return Err(BlobCacheError::Io(format!(
                "cannot unpin Mirrored blob {}: its system pin is not user-removable",
                blob.id
            )));
        }
        let pinned = library_dir.pinned_blob_path(&blob.id)?;
        let cache = library_dir.cache_blob_path(&blob.id)?;

        // Move it into the evictable cache if it is currently pinned. If it isn't in
        // `pinned/` (already in `cache/`, or remote), there is nothing to demote —
        // the blob is already as-evictable-as-it-gets, so this is a no-op. A failure
        // to even check existence is surfaced, never collapsed into "absent": unpin
        // must not report success over a broken-filesystem check.
        match crate::local_blob::exists(&pinned).await {
            Ok(true) => rename_within_storage(&pinned, &cache).await?,
            Ok(false) => {}
            Err(e) => return Err(BlobCacheError::Io(e)),
        }
    }
    Ok(())
}

/// Drop everything in the evictable cache: delete all of `storage/cache/`, leaving
/// `storage/pinned/` untouched. A whole-directory sweep, not a per-blob size-budget
/// eviction — every unpinned blob goes, and a pinned blob (in `pinned/`) survives
/// because it lives in the other folder.
///
/// An absent `cache/` is the only failure that is not an error: it means nothing has
/// been cached yet, so there is nothing to clear. Every other I/O failure is
/// returned — a swept directory must actually be gone, never reported clear over a
/// failed delete.
pub async fn clear_cache(library_dir: &LibraryDir) -> Result<(), BlobCacheError> {
    let cache_dir = library_dir.cache_dir();
    match tokio::fs::remove_dir_all(&cache_dir).await {
        Ok(()) => Ok(()),
        // No cache dir yet — nothing has been cached, so it is already clear.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(
                "clear_cache: no cache dir at {}, nothing to clear",
                cache_dir.display()
            );
            Ok(())
        }
        Err(e) => Err(BlobCacheError::Io(format!(
            "clear cache dir {}: {e}",
            cache_dir.display()
        ))),
    }
}

/// Resolve a blob's scope to its encryption key and download + decrypt its bytes
/// from the cloud. Shared by the read-miss and pin-from-absent paths.
///
/// `id`/`namespace`/`cloud_path` come from a host-built [`BlobRef`] whose row was
/// authored by any write-capable member, so they are validated as safe path tokens
/// (the same gate the pull's `download_blobs` runs) before reaching storage with a
/// key that could escape its prefix. The `id` is also validated by the cache
/// path-builders, but `namespace`/`cloud_path` feed only the cloud key, so they are
/// checked here.
async fn fetch_from_cloud(
    db: &Database,
    storage: &dyn SyncStorage,
    blob: &BlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    crate::library_dir::validate_path_token(&blob.namespace)?;
    crate::library_dir::validate_path_token(&blob.id)?;
    if let Some(cloud_path) = blob.cloud_path.as_deref() {
        crate::library_dir::validate_cloud_path(cloud_path)?;
    }

    let resolved = db
        .resolve_blob_scope(blob.scope.clone())
        .await
        .map_err(|e| BlobCacheError::Io(format!("resolve blob scope for {}: {e}", blob.id)))?;
    storage
        .get_blob(
            &blob.namespace,
            &blob.id,
            resolved,
            blob.cloud_path.as_deref(),
        )
        .await
        .map_err(BlobCacheError::Storage)
}

/// Move a blob file from one cache folder to the other (`cache/`↔`pinned/`). Both
/// roots are under `storage/`, so the `rename` is within one filesystem and atomic —
/// the blob is never visible in both folders or neither. Creates the destination's
/// `{ab}/{cd}` shard directory first (a folder a blob has never lived in yet).
async fn rename_within_storage(
    from: &std::path::Path,
    to: &std::path::Path,
) -> Result<(), BlobCacheError> {
    if let Some(parent) = to.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            BlobCacheError::Io(format!("create dir {} for move: {e}", parent.display()))
        })?;
    }
    tokio::fs::rename(from, to).await.map_err(|e| {
        BlobCacheError::Io(format!(
            "move blob {} -> {}: {e}",
            from.display(),
            to.display()
        ))
    })
}
