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
//! presence is the only truth).
//!
//! The cache has a size budget, `max_cache_size`, the host sets per device (see
//! [`Database::set_max_cache_size`]). It counts **only** the files under `cache/`
//! — `pinned/` is structurally exempt, since the sweep never looks there. After
//! every populate ([`read_blob`]'s miss-write and [`write_blob`]),
//! [`evict_to_budget`] sums the `cache/` files and, if their total exceeds the
//! budget, deletes the oldest by modification time until the total is back under
//! it. Modification time is the recency proxy — there is no `last_accessed`
//! column, the same folder-truth trade-off the whole cache makes; pinning already
//! retains the blobs the user chose to keep local. With the budget unset eviction is off and the cache
//! grows without bound. [`clear_cache`] drops all of `cache/` in one sweep
//! regardless of the budget; a pinned blob (in `pinned/`) survives either way
//! because it lives in the other folder.

use crate::blob::{BlobRef, BlobSync};
use crate::database::Database;
use crate::library_dir::{LibraryDir, PathTokenError};
use crate::sync::storage::{StorageError, SyncStorage};

/// `sync_state` key holding the device-local cache-size budget in bytes (a single
/// decimal value, not per-blob accounting). Absent ⇒ no budget ⇒ eviction off.
/// Read/written through [`Database::get_max_cache_size`] /
/// [`Database::set_max_cache_size`].
pub const MAX_CACHE_SIZE_STATE_KEY: &str = "max_cache_size";

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
///
/// After the bytes land, [`evict_to_budget`] runs so a stage that pushes `cache/`
/// over `max_cache_size` evicts its oldest files back under budget (a no-op when
/// no budget is set). The just-written file is passed as `protect`, so it is
/// excluded from eviction — this stage can never drop the very bytes it produced.
/// Eviction is best-effort: the stage has already succeeded, so an eviction failure
/// is logged and swallowed, not returned (see below).
pub async fn write_blob(
    db: &Database,
    library_dir: &LibraryDir,
    blob: &BlobRef,
    bytes: &[u8],
) -> Result<(), BlobCacheError> {
    let dest = library_dir.cache_blob_path(&blob.id)?;
    crate::local_blob::write_atomic(&dest, bytes)
        .await
        .map_err(BlobCacheError::Io)?;
    // Staging into `cache/` may have pushed it over budget; evict the oldest files
    // back under it, never the file just written (passed as `protect`). A no-op when
    // no budget is set.
    //
    // Eviction is best-effort and must not fail the stage: the write above already
    // succeeded, so the bytes are durably in `cache/`. The cache being briefly over
    // its budget is not wrong state — it self-corrects on the next populate's sweep —
    // so failing a successful stage because cleanup failed would be wrong. Log and
    // continue.
    if let Err(e) = evict_to_budget(db, library_dir, Some(&dest)).await {
        tracing::warn!(
            "write_blob: staged {} but eviction failed (cache may be over budget until the next populate): {e}",
            dest.display()
        );
    }
    Ok(())
}

/// Write a blob's plaintext straight into the PROTECTED cache folder
/// (`storage/pinned/<id>`), so a just-uploaded pinned managed blob is kept local
/// and budget-exempt with no later cloud round-trip. The pinned sibling of
/// [`write_blob`] (which stages into the evictable `storage/cache/<id>`).
///
/// Called by the upload drain after a successful upload whose entry is
/// `retain_pinned`: the same plaintext the drain already read to seal is written
/// here, so the pin is populate-on-write rather than fetch-on-read. The bytes are
/// the plaintext (what the cache stores and serves), not the sealed ciphertext in
/// the cloud.
///
/// Unlike [`write_blob`] there is NO post-write eviction: `pinned/` is structurally
/// exempt from the size budget (the sweep never walks it), so a pinned populate can
/// neither push the evictable cache over budget nor be trimmed. The write is atomic
/// ([`crate::local_blob::write_atomic`]), the same torn-file guard every cache write
/// relies on.
pub(crate) async fn populate_pinned(
    library_dir: &LibraryDir,
    id: &str,
    plaintext: &[u8],
) -> Result<(), BlobCacheError> {
    let dest = library_dir.pinned_blob_path(id)?;
    crate::local_blob::write_atomic(&dest, plaintext)
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
/// cache, never the protected one), and returns the bytes it just fetched. The
/// post-populate [`evict_to_budget`] sweep is best-effort: a fetch that succeeded
/// returns its bytes even if eviction then fails (logged, not returned).
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
    // The populate may have pushed `cache/` over budget; evict the oldest files
    // back under it, never the file just written (passed as `protect`) — so this
    // read's own sweep can't drop the bytes it just fetched, which it returns below.
    // A no-op when no budget is set.
    //
    // Eviction is best-effort and must not fail the read: the fetch + cache write
    // above already succeeded, so we have the bytes to return. The cache being
    // briefly over its budget is not wrong state — it self-corrects on the next
    // populate's sweep — so failing a successful read because cleanup failed would be
    // wrong. Log and return the bytes anyway.
    if let Err(e) = evict_to_budget(db, library_dir, Some(&cache)).await {
        tracing::warn!(
            "read_blob: populated {} but eviction failed (cache may be over budget until the next populate): {e}",
            cache.display()
        );
    }
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

/// Evict the oldest files from `storage/cache/` until its total size is back within
/// the device's `max_cache_size` budget. The cache layer's size enforcement, run
/// synchronously after every populate ([`read_blob`]'s miss-write, [`write_blob`]).
///
/// The budget counts **only** the files under `cache/` — `pinned/` is never walked,
/// so a pinned (or system-pinned `Mirrored`) blob is structurally exempt and can
/// never be evicted. With no budget set this is a no-op: the cache is unlimited
/// until the host opts into one.
///
/// Recency is the file's modification time. There is no `last_accessed` column —
/// the same folder-truth trade-off the whole cache makes — so the oldest-written
/// file is evicted first; pinning, not access tracking, is how a blob is kept.
///
/// `protect` is the file a just-finished populate wrote (the trigger passes its
/// `cache/<id>` path; a bare sweep passes `None`): it is **excluded from the
/// candidates outright**, never deleted, so the populate that triggered this sweep
/// can't evict the very bytes it just produced. Its size still counts toward the
/// total it must fit under, so if that one file alone exceeds the budget the cache
/// is left holding exactly it and over budget by that much — the caller still gets
/// its bytes, and the next populate's sweep is unaffected. This makes survival
/// structural rather than reliant on mtime granularity (two writes within one
/// filesystem mtime tick would otherwise be unordered).
///
/// If every evictable candidate is deleted and the total is still over budget — the
/// protected in-use file alone exceeds `max_cache_size` — this returns `Ok(())` (the
/// file being served can't be evicted), but logs that the cache stays over budget
/// because a single in-use blob is larger than the whole budget. It is surfaced, not
/// silently reported as if the budget were met.
///
/// A file that has vanished by the time it is deleted (a concurrent `clear_cache`
/// or sweep already removed it) is the one legitimate skip — logged at debug, its
/// now-absent bytes dropped from the running total. Every other stat or delete
/// failure is surfaced, never swallowed: a cache that can't be measured or trimmed
/// must fail loudly, not silently drift over budget.
pub async fn evict_to_budget(
    db: &Database,
    library_dir: &LibraryDir,
    protect: Option<&std::path::Path>,
) -> Result<(), BlobCacheError> {
    let budget = match db
        .get_max_cache_size()
        .await
        .map_err(|e| BlobCacheError::Io(format!("read max_cache_size: {e}")))?
    {
        Some(budget) => budget,
        // No budget set — the cache is unlimited, so there is nothing to enforce.
        None => return Ok(()),
    };

    let mut entries = collect_cache_files(&library_dir.cache_dir()).await?;
    // The protected file's bytes count toward the total it must fit under, but it is
    // never a deletion candidate — drop it from the list, not the sum.
    let mut total: u64 = entries.iter().map(|(_, _, size)| size).sum();
    if let Some(protect) = protect {
        entries.retain(|(path, _, _)| path.as_path() != protect);
    }
    if total <= budget {
        return Ok(());
    }

    // Oldest modification time first: that file is evicted first. A stable sort is
    // fine — files with the same mtime are interchangeable for the budget, and the
    // just-written file (the one survival depends on) is already excluded above.
    entries.sort_by_key(|(_, mtime, _)| *mtime);

    // Each `size` here was part of the `total` sum above, so subtracting it as its
    // file is evicted can't underflow as long as that invariant holds. `checked_sub`
    // rather than `saturating_sub`: flooring at 0 would mask a genuine accounting
    // miscount (a `size` not actually in the sum), so a violation panics loudly
    // instead of silently mis-measuring the cache.
    for (path, _mtime, size) in entries {
        if total <= budget {
            break;
        }
        let subtract = |total: u64| {
            total.checked_sub(size).unwrap_or_else(|| {
                panic!(
                    "evict accounting underflow at {}: size {size} > running total {total} \
                     (invariant: every cache file's size was summed into the total)",
                    path.display()
                )
            })
        };
        match tokio::fs::remove_file(&path).await {
            Ok(()) => total = subtract(total),
            // The file is already gone (a concurrent sweep/clear). Its bytes are no
            // longer on disk, so drop them from the total and move on — the one
            // legitimate skip, not a masked failure.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!("evict: {} already gone, skipping", path.display());
                total = subtract(total);
            }
            Err(e) => {
                return Err(BlobCacheError::Io(format!(
                    "evict cache file {}: {e}",
                    path.display()
                )));
            }
        }
    }

    // Every evictable candidate is gone and the cache is still over budget: the
    // protected in-use file alone exceeds `max_cache_size`. We can't evict the file
    // being served, so return Ok — but surface that the budget is unmet rather than
    // reporting success silently.
    if total > budget {
        tracing::warn!(
            "evict: cache stays {} bytes over budget ({total} > {budget}) — a single in-use blob exceeds the whole cache budget",
            total - budget
        );
    }
    Ok(())
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

/// Walk `cache/` and return every file as `(path, mtime, size)` — the input to a
/// budget eviction. The cache stores files under a two-level shard tree
/// (`cache/{ab}/{cd}/<id>`), so this descends directories with an explicit stack
/// and collects only the leaf files; `pinned/` is a sibling root and is never
/// reached.
///
/// An absent `cache/` means nothing has been cached yet — an empty result, not an
/// error (the same posture `clear_cache` takes on a missing dir). Every other
/// failure to read a directory or stat a file is surfaced: the budget cannot be
/// enforced over a cache it cannot fully measure, so a measurement failure fails
/// loudly rather than under-counting and leaving the cache silently over budget.
async fn collect_cache_files(
    cache_dir: &std::path::Path,
) -> Result<Vec<(std::path::PathBuf, std::time::SystemTime, u64)>, BlobCacheError> {
    let mut files = Vec::new();
    let mut dirs = vec![cache_dir.to_path_buf()];

    while let Some(dir) = dirs.pop() {
        let mut read_dir = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            // No cache dir (or a shard dir removed mid-walk) — nothing more to
            // measure down this branch. A legitimate skip, but logged so it is not
            // silent.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(
                    "collect_cache_files: {} absent, skipping (empty cache or concurrently-removed shard)",
                    dir.display()
                );
                continue;
            }
            Err(e) => {
                return Err(BlobCacheError::Io(format!(
                    "read cache dir {}: {e}",
                    dir.display()
                )));
            }
        };

        loop {
            let entry = match read_dir.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    return Err(BlobCacheError::Io(format!(
                        "read cache dir entry under {}: {e}",
                        dir.display()
                    )));
                }
            };
            let path = entry.path();
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                // The entry vanished between listing and stat (a concurrent
                // clear/sweep) — it no longer occupies the cache, so it drops out of
                // the measurement. A legitimate skip, but logged so it is not silent.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(
                        "collect_cache_files: {} vanished between listing and stat, skipping",
                        path.display()
                    );
                    continue;
                }
                Err(e) => {
                    return Err(BlobCacheError::Io(format!(
                        "stat cache entry {}: {e}",
                        path.display()
                    )));
                }
            };
            if metadata.is_dir() {
                dirs.push(path);
            } else {
                let mtime = metadata.modified().map_err(|e| {
                    BlobCacheError::Io(format!("modified time of {}: {e}", path.display()))
                })?;
                files.push((path, mtime, metadata.len()));
            }
        }
    }
    Ok(files)
}
