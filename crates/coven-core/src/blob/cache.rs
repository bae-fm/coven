//! The device-local cache for **Remote** blobs: bytes on disk, keyed by blob id,
//! with the folder the file lives in as the only retention truth.
//!
//! The cache holds copies of Remote blobs only — re-fetchable from the cloud,
//! evictable to a size budget, kept-or-dropped per pin. A **Local** blob is not in
//! the cache: a user-provided Local blob is the user's own file at a path (an
//! external ref); a host-provided Local blob is in the local store (see
//! [`local_files`](super::local_files)). So `CacheEager`/`CacheLazy`/pin/budget all
//! describe a blob only while it is Remote. See the [blob concept tree](crate::blob)
//! for where the cache sits in the whole storage model.
//!
//! There is no cache table. A cached Remote blob is in **exactly one** of two
//! folders under the library dir, or in neither. Both are segmented by the blob's
//! namespace, so each namespace's cache evicts against its own budget without
//! touching another's:
//!
//! - `storage/pinned/<namespace>/{ab}/{cd}/<id>` — kept, budget-exempt. A Remote
//!   blob's cache copy the user pinned for offline (kept from eviction).
//! - `storage/cache/<namespace>/{ab}/{cd}/<id>` — opportunistic, evictable. A blob
//!   fetched on read (`CacheLazy`) or eagerly on pull (`CacheEager`).
//! - neither — not cached. No file; fetched from the cloud on the next read.
//!
//! Presence is the file on disk; kept-ness is which folder. Nothing the two
//! `readdir`s can't answer, so no metadata sidecar to keep in sync with the disk.
//! Every write is atomic ([`crate::local_blob::write_atomic`]) so a crash can't leave
//! a torn file a read would trust, and pin/unpin are a `rename` within `storage/`
//! (one filesystem, atomic) so a blob never appears in both folders or neither
//! mid-move.
//!
//! Both reads **dispatch on coven's own authoritative state** — they never probe
//! every store and take the first hit. The discriminator is the **locality root**
//! plus the blob's intrinsic **provenance**, not "is there a local file here." Coven
//! resolves the blob's backing row (found in the table its `namespace` declares) up
//! to its gated root or remote root (see
//! [`Gates::root_kept_of`](crate::sync::gate::Gates::root_kept_of)), then dispatches:
//!
//! - **Remote** (gate on, or remote root) ⇒ the bytes live in the cloud fronted by
//!   the device cache. The first legitimate probe runs per-device cache
//!   materialization — which no shared state records — checking `pinned/` then
//!   `cache/`. For **host-provided** Remote blobs only, a cache miss then checks the
//!   local store for upload staging: `CovenHandle::write` stores the plaintext there
//!   before the upload cycle seals it and drops that staging file. A miss there means
//!   upload staging is already gone, so the read fetches from the cloud. A
//!   **user-provided** Remote blob never reads the local store.
//! - **Local** (gate off) ⇒ the bytes are on-device; provenance picks the copy. A
//!   **user-provided** blob is the user's own external file (`local_blob_refs`), read
//!   straight from its path and validated by presence + size — its ref MUST exist
//!   ([`BlobCacheError::NoExternalRef`] otherwise). A **host-provided** blob is in the
//!   **local store** ([`local_files`](super::local_files)), its only copy — a miss is
//!   fail-loud corruption ([`BlobCacheError::NoLocalCopy`]). Neither falls through to
//!   the cloud: a Local blob has no cloud copy.
//!
//! [`read_blob`] returns the entire blob (a cloud miss fetches + decrypts it and
//! populates `cache/`); [`open_blob_stream`] serves a plaintext byte range for a host
//! streaming or seeking (a cloud miss range-reads + decrypts but populates nothing — a
//! partial file would be read as the whole blob, since presence is the only truth).
//!
//! The cache has a **per-namespace** size budget the host sets per device (see
//! [`Database::set_cache_budget`]), so a small namespace (`covers`) is never wiped by
//! pressure from a big one (`release_files`). A namespace's budget counts **only**
//! the files under `cache/<namespace>/` — `pinned/` is structurally exempt, and
//! `storage/local` (the local store) is never walked at all. After every populate
//! into a namespace ([`read_blob`]'s miss-write and [`write_blob`]),
//! [`evict_to_budget`] sums that namespace's `cache/<namespace>/` files and, if their
//! total exceeds its budget, deletes the oldest by modification time until the total
//! is back under it — touching only that namespace's subtree. Modification time is
//! the recency proxy — there is no `last_accessed` column, the same folder-truth
//! trade-off the whole cache makes; pinning retains the Remote blobs the user chose
//! to keep local. With a namespace's budget unset eviction is off for it and its
//! cache grows without bound. [`clear_cache`] drops all of `cache/` (every namespace)
//! in one sweep regardless of any budget; a pinned blob (in `pinned/`) survives
//! either way because it lives in the other folder.

use crate::blob::decl::BlobDecls;
use crate::blob::{BlobRef, Provenance};
use crate::database::{Database, DbError};
use crate::library_dir::{LibraryDir, PathTokenError};
use crate::sync::gate::Gates;
use crate::sync::storage::{StorageError, SyncStorage};

/// Prefix for the `sync_state` keys holding each namespace's device-local cache-size
/// budget in bytes (a single decimal value per namespace, not per-blob accounting).
/// The key for one namespace is [`cache_budget_state_key`]. A namespace with no such
/// key has no budget ⇒ eviction off for it ⇒ that namespace's cache grows unbounded.
/// Read/written through [`Database::get_cache_budget`] /
/// [`Database::set_cache_budget`].
pub const CACHE_BUDGET_STATE_KEY_PREFIX: &str = "cache_budget:";

/// The `sync_state` key holding `namespace`'s cache-size budget. Namespaces are safe
/// path tokens (no `:`), so the `cache_budget:` prefix never collides with one.
pub fn cache_budget_state_key(namespace: &str) -> String {
    format!("{CACHE_BUDGET_STATE_KEY_PREFIX}{namespace}")
}

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
    /// A Remote blob's bytes were needed from the cloud but no cloud home is
    /// connected, so there is no storage to fetch them from. A home-less library
    /// holds only Local blobs (external refs + the local store), which serve
    /// straight off disk and never reach the cloud-miss path; reaching here means
    /// a Remote blob was read with no provider connected — a real fault, surfaced
    /// rather than masked.
    NoCloudHome,
    /// A local-disk failure (a cache write, a folder move, the `clear_cache`
    /// sweep), or a scope that couldn't be resolved to an encryption key. Carries a
    /// human-readable cause.
    Io(String),
    /// The old-value and new-value walks of one changeset disagreed on row count.
    /// They are two views of the same changeset; a mismatch means cleanup cannot
    /// pair updated rows with their previous blob ids.
    ChangesetWalkMismatch { old_count: usize, new_count: usize },
    /// A registered external blob ref (a user-provided Local blob's user-owned
    /// file) points at a file that is no longer there — the user moved, renamed, or
    /// deleted it. Terminal: an external blob has no cloud copy to fall back to, so
    /// this never re-fetches. The host surfaces a "files missing / moved" state
    /// whose actions are relocate (pick the new folder, re-register) or re-import.
    ExternalMissing {
        id: String,
        path: std::path::PathBuf,
        /// The underlying read failure — a missing file or a real I/O error,
        /// preserved rather than collapsed so the host sees why the read failed.
        source: String,
    },
    /// A registered external blob's file is present but its length no longer matches
    /// the registered `size` — the user truncated it or replaced it with a
    /// different-length file. Terminal like [`Self::ExternalMissing`]: validate-on-
    /// read is presence + size, and a mismatch is not the bytes coven registered.
    ExternalSizeMismatch {
        id: String,
        path: std::path::PathBuf,
    },
    /// A **Local** blob (its gated locality root's gate is off) has no copy in the
    /// local store. A Local blob has no cloud copy, so there is nothing to fall back
    /// to: the state is broken, not a cache miss. Surfaced loud rather than silently
    /// fetching from the cloud — a make_local rollback leftover, an interrupted
    /// materialize, or a lost local file would otherwise be papered over. The host
    /// re-materializes or repairs.
    NoLocalCopy { namespace: String, id: String },
    /// A blob could not be resolved to a locality: its namespace declares no
    /// blob-bearing table, or that table has no row with the id, or the row reaches no
    /// gated root or remote root — so the source of Local-vs-Remote truth can't be
    /// read. In a consistent library every readable blob has a locality root, so this
    /// is a real fault — surfaced rather than guessing a source by probing.
    LocalityUnresolved { id: String },
    /// The gate resolved a blob to **Local + user-provided**, but no external-ref row
    /// is registered for it. A user-provided Local blob's bytes live only at the user's
    /// path, tracked by that ref; its absence is corruption (a lost or never-written
    /// ref), not a cache miss to fall through — surfaced loud so the host repairs or
    /// re-imports.
    NoExternalRef { id: String },
}

impl std::fmt::Display for BlobCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobCacheError::Path(e) => write!(f, "blob path error: {e}"),
            BlobCacheError::Storage(e) => write!(f, "blob cache storage error: {e}"),
            BlobCacheError::NoCloudHome => {
                write!(f, "no cloud home connected to read a Remote blob")
            }
            BlobCacheError::Io(e) => write!(f, "blob cache I/O error: {e}"),
            BlobCacheError::ChangesetWalkMismatch {
                old_count,
                new_count,
            } => write!(
                f,
                "changeset old/new walks disagree: {old_count} old rows, {new_count} new rows"
            ),
            BlobCacheError::ExternalMissing { id, path, source } => write!(
                f,
                "external blob {id} could not be read at {}: {source}",
                path.display()
            ),
            BlobCacheError::ExternalSizeMismatch { id, path } => write!(
                f,
                "external blob {id} at {} no longer matches its registered size",
                path.display()
            ),
            BlobCacheError::NoLocalCopy { namespace, id } => write!(
                f,
                "local blob {namespace}/{id} is gated Local but absent from the local store"
            ),
            BlobCacheError::LocalityUnresolved { id } => write!(
                f,
                "cannot resolve locality for blob {id}: no locality root determines where it lives"
            ),
            BlobCacheError::NoExternalRef { id } => write!(
                f,
                "user-provided Local blob {id} has no registered external ref"
            ),
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

impl From<crate::blob::local_files::LocalBlobError> for BlobCacheError {
    fn from(e: crate::blob::local_files::LocalBlobError) -> Self {
        use crate::blob::local_files::LocalBlobError;
        match e {
            LocalBlobError::Path(p) => BlobCacheError::Path(p),
            LocalBlobError::Io(s) => BlobCacheError::Io(s),
        }
    }
}

/// Write a Remote blob's plaintext into the evictable cache (`storage/cache/<id>`)
/// via an atomic write, so a later read serves it locally without a cloud
/// round-trip and a pin can promote it to `pinned/`.
///
/// Used when a blob becomes Remote and coven already has its plaintext in hand —
/// the inline push moving a just-uploaded host-provided blob's local-store copy
/// into the cache — so the cache is populated on write rather than fetch-on-read.
///
/// After the bytes land, [`evict_to_budget`] runs so a write that pushes the blob's
/// namespace cache over that namespace's budget evicts its oldest files back under
/// budget (a no-op when the namespace has no budget set). The just-written file is
/// passed as `protect`, so it is excluded from eviction — this write can never drop
/// the very bytes it produced. An eviction failure is returned to the caller instead
/// of reporting a budgeted write as complete while the cache could not be trimmed.
pub(crate) async fn write_blob(
    db: &Database,
    library_dir: &LibraryDir,
    blob: &BlobRef,
    bytes: &[u8],
) -> Result<(), BlobCacheError> {
    let dest = library_dir.cache_blob_path(&blob.namespace, &blob.id)?;
    crate::local_blob::write_atomic(&dest, bytes)
        .await
        .map_err(BlobCacheError::Io)?;
    // The write into `cache/<namespace>/` may have pushed that namespace over its
    // budget; evict its oldest files back under it, never the file just written
    // (passed as `protect`). A no-op when the namespace has no budget set.
    evict_to_budget(db, library_dir, &blob.namespace, Some(&dest)).await?;
    Ok(())
}

/// Write a Remote blob's plaintext straight into the KEPT cache folder
/// (`storage/pinned/<id>`), so a just-uploaded blob the user pinned for offline is
/// kept local and budget-exempt with no later cloud round-trip. The kept sibling of
/// [`write_blob`] (which writes into the evictable `storage/cache/<id>`).
///
/// Called by the upload drain after a successful upload whose entry is
/// `retain_pinned`: the same plaintext the drain already read to seal is written
/// here, so the pin is populate-on-write rather than fetch-on-read. The bytes are
/// the plaintext (what the cache stores and serves), not the sealed ciphertext in
/// the cloud.
///
/// Unlike [`write_blob`] there is NO post-write eviction: `pinned/` is structurally
/// exempt from the size budget (the sweep never walks it), so a kept populate can
/// neither push the evictable cache over budget nor be trimmed. The write is atomic
/// ([`crate::local_blob::write_atomic`]), the same torn-file guard every cache write
/// relies on.
pub(crate) async fn populate_pinned(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
    src_path: &std::path::Path,
) -> Result<(), BlobCacheError> {
    let dest = library_dir.pinned_blob_path(namespace, id)?;
    crate::local_blob::copy_atomic(src_path, &dest)
        .await
        .map_err(BlobCacheError::Io)
}

/// Read a Remote blob's plaintext from the cache only — `pinned/<id>` then
/// `cache/<id>`, in order — returning `None` when it is in neither folder. No cloud
/// fetch and no local-store check: just the two cache folders. A failure to even
/// check existence (broken filesystem) is surfaced, never collapsed into `None`. Two
/// callers share this probe:
///
/// - [`read_remote_whole`]: the Remote read's cache-hit check — a hit serves the file,
///   a `None` means fetch from the cloud and populate the cache.
/// - the inline push's crash-recovery read of a host-provided blob whose local-store
///   copy a prior cycle already moved into the cache. The push's primary read is from
///   the local store ([`local_files::read`](super::local_files::read)); this is the
///   fallback, and a `None` from both tells the push the blob is not ready, so it
///   aborts rather than publishing a row whose blob never reached the cloud.
pub async fn read_staged(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
) -> Result<Option<Vec<u8>>, BlobCacheError> {
    let pinned = library_dir.pinned_blob_path(namespace, id)?;
    let cache = library_dir.cache_blob_path(namespace, id)?;
    for path in [&pinned, &cache] {
        match crate::local_blob::exists(path).await {
            Ok(true) => {
                return crate::local_blob::read(path)
                    .await
                    .map(Some)
                    .map_err(BlobCacheError::Io);
            }
            Ok(false) => {}
            Err(e) => return Err(BlobCacheError::Io(e)),
        }
    }
    Ok(None)
}

/// Drop a Remote blob's cache copy from BOTH folders (`pinned/<id>` and
/// `cache/<id>`), part of the apply-side cleanup when an incoming changeset deletes
/// a blob-bearing row (a gate retract or a genuine delete). A peer drops only its
/// own cache copy here — it never writes a cloud tombstone, which belongs to the
/// deleting / make-Local owner. The `pinned/` copy is budget-exempt, so without
/// this it would leak forever once the row is gone. (The local store is dropped
/// separately by the apply-side caller — a peer holds the blob in the cache, not
/// the local store, but the caller drops both wherever the bytes are.)
///
/// An absent file in either folder is the expected case (a blob is in at most one
/// folder, or neither), not an error. Every other I/O failure is surfaced.
pub async fn drop_cached_blob(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
) -> Result<(), BlobCacheError> {
    for path in [
        library_dir.pinned_blob_path(namespace, id)?,
        library_dir.cache_blob_path(namespace, id)?,
    ] {
        // An absent file in either folder is the expected case (`remove_file`
        // reports it as `Ok(false)`, not an error); every real I/O failure surfaces.
        crate::local_blob::remove_file(&path)
            .await
            .map_err(BlobCacheError::Io)?;
    }
    Ok(())
}

/// Drop every on-device copy of a blob: its cache copies (pinned + evictable) via
/// [`drop_cached_blob`] and its host-provided local-store copy via
/// [`local_files::drop_blob`](crate::blob::local_files::drop_blob). The single
/// "delete the bytes wherever they live" step shared by the apply-side delete
/// cleanup and the host's `CovenHandle::evict_blob`.
pub async fn drop_all_local_copies(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
) -> Result<(), BlobCacheError> {
    drop_cached_blob(library_dir, namespace, id).await?;
    crate::blob::local_files::drop_blob(library_dir, namespace, id)
        .await
        .map_err(BlobCacheError::from)?;
    Ok(())
}

/// Whether a Remote blob's cache copy is currently pinned — present in
/// `storage/pinned/<namespace>/<id>`. The pin truth is the folder a blob's file
/// lives in, not a table (see the module docs), so this is a single existence
/// check on the kept folder: a blob in `cache/` or in neither folder is not
/// pinned. A failure to even check existence (broken filesystem) is surfaced,
/// never collapsed into "not pinned".
pub async fn is_pinned(
    library_dir: &LibraryDir,
    namespace: &str,
    id: &str,
) -> Result<bool, BlobCacheError> {
    let pinned = library_dir.pinned_blob_path(namespace, id)?;
    crate::local_blob::exists(&pinned)
        .await
        .map_err(BlobCacheError::Io)
}

/// Read a blob's whole contents, dispatching on coven's authoritative state — the
/// blob's locality root, then its intrinsic provenance — rather than probing every
/// store and taking the first hit.
///
/// [`resolve_source`] reads the locality root first: **Remote** (gate on, or remote
/// root) ⇒ the bytes live in the cloud fronted by the device cache, so the first
/// legitimate probe checks `pinned/<id>` then `cache/<id>` for a per-device cache
/// copy and serves a hit. For host-provided Remote blobs only, a cache miss checks
/// the local store for upload staging. A miss there resolves the blob's scope to its
/// encryption key, downloads + decrypts it via [`SyncStorage::get_blob`], writes it
/// atomically to `cache/<id>` (evictable — a fetch-on-read populates the evictable
/// cache, never the kept folder), and returns the bytes it just fetched (the
/// post-populate [`evict_to_budget`] sweep is best-effort: a successful fetch returns
/// its bytes even if eviction then fails).
///
/// **Local** (gate off) ⇒ the bytes are on-device, and provenance picks which copy:
/// a **user-provided** blob is the user's own external file (`local_blob_refs` row),
/// read straight from its path and validated by presence + size — its ref MUST exist
/// ([`BlobCacheError::NoExternalRef`] if not: a Local user-provided blob without its
/// ref is corruption, not a fall-through), and a vanished/short file is
/// [`BlobCacheError::ExternalMissing`] / [`BlobCacheError::ExternalSizeMismatch`]. A
/// **host-provided** blob is in the **local store** (`storage/local/<namespace>/<id>`,
/// see [`local_files`](super::local_files)), its only copy — a miss is
/// [`BlobCacheError::NoLocalCopy`], fail-loud corruption, never a cloud fetch.
pub async fn read_blob(
    db: &Database,
    library_dir: &LibraryDir,
    storage: Option<&dyn SyncStorage>,
    blob: &BlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    match resolve_source(db, blob).await? {
        // Remote: the bytes live in the cloud fronted by the device cache.
        BlobSource::Cache => read_remote_whole(db, library_dir, storage, blob).await,
        // Local + user-provided: the user's own external file. Its ref must be present
        // — gate-resolved Local + UserProvided with no ref is corruption, not a miss.
        BlobSource::External => {
            let ext = lookup_external_ref(db, &blob.id).await?.ok_or_else(|| {
                BlobCacheError::NoExternalRef {
                    id: blob.id.clone(),
                }
            })?;
            read_external_file(&blob.id, ext, ExternalRead::Whole).await
        }
        // Local + host-provided: the local store is the ONLY copy (a Local blob has no
        // cloud copy). A miss is fail-loud corruption, not a cache miss to refetch.
        BlobSource::LocalStore => {
            crate::blob::local_files::read(library_dir, &blob.namespace, &blob.id)
                .await?
                .ok_or_else(|| BlobCacheError::NoLocalCopy {
                    namespace: blob.namespace.clone(),
                    id: blob.id.clone(),
                })
        }
    }
}

/// Serve a Remote blob whole. The one legitimate probe — per-device cache
/// materialization, a filesystem fact no shared state holds — checks `pinned/<id>`
/// then `cache/<id>` (via [`read_staged`]) and serves a hit. For host-provided Remote
/// blobs only, a cache miss checks the local store for upload staging before a cloud
/// read. Split from [`read_blob`] so the whole-blob Remote path reads as one branch
/// of the locality dispatch.
async fn read_remote_whole(
    db: &Database,
    library_dir: &LibraryDir,
    storage: Option<&dyn SyncStorage>,
    blob: &BlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    // A cache hit (`pinned/` or `cache/`) serves the file straight off disk — the same
    // pinned→cache probe [`read_staged`] runs. An existence-check failure there is
    // surfaced, not collapsed into a miss: re-downloading over a present file would be
    // wasteful and could mask a real fault.
    if let Some(bytes) = read_staged(library_dir, &blob.namespace, &blob.id).await? {
        return Ok(bytes);
    }

    if blob.provenance == Provenance::HostProvided {
        if let Some(bytes) =
            crate::blob::local_files::read(library_dir, &blob.namespace, &blob.id).await?
        {
            return Ok(bytes);
        }
    }

    // Miss: fetch from the cloud and populate the evictable cache. A home-less
    // library reaches here only when a Remote blob is read with no provider
    // connected — there is no storage to fetch it from, so surface that fault.
    let cache = library_dir.cache_blob_path(&blob.namespace, &blob.id)?;
    let storage = storage.ok_or(BlobCacheError::NoCloudHome)?;
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
    if let Err(e) = evict_to_budget(db, library_dir, &blob.namespace, Some(&cache)).await {
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
/// The serving paths mirror [`read_blob`], dispatched the same way ([`resolve_source`]
/// reads the gate, then provenance): **Remote** serves the range off a cache hit
/// (`pinned/<id>` OR `cache/<id>`) — the whole-plaintext local file read at `offset`,
/// no decryption, no cloud. For host-provided Remote blobs only, a cache miss checks
/// the local store for upload staging and serves the same bounded range from that
/// whole plaintext file when present. A miss there fetches and decrypts the range
/// from the cloud via [`SyncStorage::read_blob_range`] and **never writes a cache
/// file** (a truncated/partial file under `cache/<id>` would be read as the whole
/// blob by [`read_blob`]; only the whole-file read populates). **Local +
/// user-provided** reads the range off the user's external file (ref required —
/// [`NoExternalRef`] otherwise; a vanished/short file is [`ExternalMissing`]).
/// **Local + host-provided** reads the range off the local store ([`NoLocalCopy`] on
/// a miss, never a cloud fetch).
///
/// As in [`read_blob`], a failure to even check a file's existence is surfaced,
/// never collapsed into a miss (which would re-fetch over a present file and could
/// mask a real fault).
///
/// [`NoExternalRef`]: BlobCacheError::NoExternalRef
/// [`ExternalMissing`]: BlobCacheError::ExternalMissing
/// [`NoLocalCopy`]: BlobCacheError::NoLocalCopy
pub async fn open_blob_stream(
    db: &Database,
    library_dir: &LibraryDir,
    storage: Option<&dyn SyncStorage>,
    blob: &BlobRef,
    source_size: u64,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, BlobCacheError> {
    // The range contract, applied once for all serving paths. A zero-length read
    // is empty without touching disk or cloud; an out-of-range read is an error
    // before any path runs, so the local-file path can't silently short-read.
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

    match resolve_source(db, blob).await? {
        // Remote: the cache copy, else a ranged cloud read (populating nothing).
        BlobSource::Cache => {
            read_remote_range(db, library_dir, storage, blob, source_size, offset, len).await
        }
        // Local + user-provided: range-read the user's external file. The window was
        // validated against `source_size`, and `read_range` reads exactly `len`
        // (failing loud on a short file). The ref must be present — no fallback.
        BlobSource::External => {
            let ext = lookup_external_ref(db, &blob.id).await?.ok_or_else(|| {
                BlobCacheError::NoExternalRef {
                    id: blob.id.clone(),
                }
            })?;
            read_external_file(&blob.id, ext, ExternalRead::Range { offset, len }).await
        }
        // Local + host-provided: range-read the local store, coven's only copy. A miss
        // is fail-loud corruption, never a cloud fetch.
        BlobSource::LocalStore => crate::blob::local_files::read_range(
            library_dir,
            &blob.namespace,
            &blob.id,
            offset,
            len,
        )
        .await?
        .ok_or_else(|| BlobCacheError::NoLocalCopy {
            namespace: blob.namespace.clone(),
            id: blob.id.clone(),
        }),
    }
}

/// Serve a Remote blob's plaintext range. A cache hit (`pinned/<id>` OR `cache/<id>`)
/// reads the slice off the whole-plaintext local file. For host-provided Remote
/// blobs only, a cache miss checks the local store for upload staging. A miss there
/// range-reads + decrypts from the cloud and writes NO cache file (a partial file
/// would be mistaken for the whole blob by [`read_blob`]). Split from
/// [`open_blob_stream`] so the Remote path reads as one branch of the locality
/// dispatch; the range was already validated by the caller against `source_size`.
async fn read_remote_range(
    db: &Database,
    library_dir: &LibraryDir,
    storage: Option<&dyn SyncStorage>,
    blob: &BlobRef,
    source_size: u64,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, BlobCacheError> {
    let pinned = library_dir.pinned_blob_path(&blob.namespace, &blob.id)?;
    let cache = library_dir.cache_blob_path(&blob.namespace, &blob.id)?;

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

    if blob.provenance == Provenance::HostProvided {
        if let Some(bytes) = crate::blob::local_files::read_range(
            library_dir,
            &blob.namespace,
            &blob.id,
            offset,
            len,
        )
        .await?
        {
            return Ok(bytes);
        }
    }

    // Miss: serve the range from the cloud (range read + decrypt over the resolved
    // scope) WITHOUT writing a cache file — a partial file would be mistaken for
    // the whole blob by `read_blob`. Only `read_blob` populates the cache. A
    // home-less library has no storage to range-read a Remote blob from; surface it.
    let storage = storage.ok_or(BlobCacheError::NoCloudHome)?;
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
/// it into `pinned/`, so a read-populated or eagerly-pulled blob is promoted with no
/// cloud fetch); in neither (fetch from the cloud and write straight to `pinned/`).
/// `&[BlobRef]` rather than ids because the fetch needs the blob's cloud coordinates
/// (namespace, scope, cloud_path) an id alone lacks.
pub async fn pin(
    db: &Database,
    library_dir: &LibraryDir,
    storage: Option<&dyn SyncStorage>,
    blobs: &[BlobRef],
) -> Result<(), BlobCacheError> {
    for blob in blobs {
        let pinned = library_dir.pinned_blob_path(&blob.namespace, &blob.id)?;
        let cache = library_dir.cache_blob_path(&blob.namespace, &blob.id)?;

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

        // In neither folder — fetch from the cloud straight into `pinned/`. A
        // home-less library has no storage to fetch a Remote blob from; surface it.
        let storage = storage.ok_or(BlobCacheError::NoCloudHome)?;
        let bytes = fetch_from_cloud(db, storage, blob).await?;
        crate::local_blob::write_atomic(&pinned, &bytes)
            .await
            .map_err(BlobCacheError::Io)?;
    }
    Ok(())
}

/// Drop a Remote blob's pin: move `storage/pinned/<id>` → `storage/cache/<id>` so
/// the cache copy stays (still readable) but is now evictable. Not a delete.
///
/// A pin keeps a specific Remote blob's cache copy from eviction; unpin reverses it
/// regardless of the blob's [`CacheFill`] — a `CacheEager` blob lands in the
/// evictable cache on pull (it is not auto-pinned), so unpinning one that was never
/// pinned is simply a no-op (it is already as-evictable-as-it-gets).
pub async fn unpin(library_dir: &LibraryDir, blobs: &[BlobRef]) -> Result<(), BlobCacheError> {
    for blob in blobs {
        let pinned = library_dir.pinned_blob_path(&blob.namespace, &blob.id)?;
        let cache = library_dir.cache_blob_path(&blob.namespace, &blob.id)?;

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
#[cfg(test)]
pub async fn clear_cache(library_dir: &LibraryDir) -> Result<(), BlobCacheError> {
    let cache_dir = library_dir.cache_dir();
    match crate::local_blob::remove_dir_all(&cache_dir).await {
        Ok(true) => Ok(()),
        // No cache dir yet — nothing has been cached, so it is already clear.
        Ok(false) => {
            tracing::debug!(
                "clear_cache: no cache dir at {}, nothing to clear",
                cache_dir.display()
            );
            Ok(())
        }
        Err(e) => Err(BlobCacheError::Io(e)),
    }
}

/// Evict the oldest files from `namespace`'s cache subtree
/// (`storage/cache/<namespace>/`) until its total size is back within that
/// namespace's [`Database::get_cache_budget`] budget. The cache layer's per-namespace
/// size enforcement, run synchronously after every populate into that namespace
/// ([`read_blob`]'s miss-write, [`write_blob`]).
///
/// Each namespace evicts independently against its own budget, walking **only** its
/// own subtree: evicting `release_files` (big) never touches `covers` (a small
/// reserved slice). The budget counts **only** the files under
/// `cache/<namespace>/` — `pinned/` is never walked (nor is the local store under
/// `storage/local/`), so a pinned blob is structurally exempt and can never be
/// evicted. With no budget set for this namespace this is a no-op: that namespace's
/// cache is unlimited until the host opts it into a budget.
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
/// protected in-use file alone exceeds this namespace's budget — this returns
/// `Ok(())` (the file being served can't be evicted), but logs that the cache stays
/// over budget because a single in-use blob is larger than the whole budget. It is
/// surfaced, not silently reported as if the budget were met.
///
/// A file that has vanished by the time it is deleted (a concurrent `clear_cache`
/// or sweep already removed it) is the one legitimate skip — logged at debug, its
/// now-absent bytes dropped from the running total. Every other stat or delete
/// failure is surfaced, never swallowed: a cache that can't be measured or trimmed
/// must fail loudly, not silently drift over budget.
pub async fn evict_to_budget(
    db: &Database,
    library_dir: &LibraryDir,
    namespace: &str,
    protect: Option<&std::path::Path>,
) -> Result<(), BlobCacheError> {
    let budget = match db
        .get_cache_budget(namespace)
        .await
        .map_err(|e| BlobCacheError::Io(format!("read cache budget for {namespace:?}: {e}")))?
    {
        Some(budget) => budget,
        // This namespace has no budget set — its cache is unlimited, so there is
        // nothing to enforce. Another namespace's budget never reaches here.
        None => return Ok(()),
    };

    let mut entries = crate::local_blob::walk_files(&library_dir.cache_namespace_dir(namespace)?)
        .await
        .map_err(BlobCacheError::Io)?;
    // The protected file's bytes count toward the total it must fit under, but it is
    // never a deletion candidate — drop it from the list, not the sum.
    let mut total: u64 = entries.iter().map(|(_, _, size)| size).sum();
    if let Some(protect) = protect {
        entries.retain(|(path, _, _)| path.as_path() != protect);
    }
    if total <= budget {
        return Ok(());
    }

    // Oldest modification time first: that file is evicted first. The recency key is
    // milliseconds since the Unix epoch (file mtime), so the smallest sorts first. A
    // stable sort is fine — files with the same recency are interchangeable for the
    // budget, and the just-written file (the one survival depends on) is already
    // excluded above.
    entries.sort_by_key(|(_, recency, _)| *recency);

    // Each `size` here was part of the `total` sum above, so subtracting it as its
    // file is evicted can't underflow as long as that invariant holds. `checked_sub`
    // rather than `saturating_sub`: flooring at 0 would mask a genuine accounting
    // miscount (a `size` not actually in the sum), so a violation panics loudly
    // instead of silently mis-measuring the cache.
    for (path, _recency, size) in entries {
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
        match crate::local_blob::remove_file(&path).await {
            Ok(true) => total = subtract(total),
            // The file is already gone (a concurrent sweep/clear). Its bytes are no
            // longer on disk, so drop them from the total and move on — the one
            // legitimate skip, not a masked failure.
            Ok(false) => {
                tracing::debug!("evict: {} already gone, skipping", path.display());
                total = subtract(total);
            }
            Err(e) => return Err(BlobCacheError::Io(e)),
        }
    }

    // Every evictable candidate is gone and the cache is still over budget: the
    // protected in-use file alone exceeds this namespace's budget. We can't evict the
    // file being served, so return Ok — but surface that the budget is unmet rather
    // than reporting success silently.
    if total > budget {
        tracing::warn!(
            "evict: cache stays {} bytes over budget ({total} > {budget}) — a single in-use blob exceeds the whole cache budget",
            total - budget
        );
    }
    Ok(())
}

/// The single store a blob's bytes live in, resolved from coven's authoritative
/// state — the blob's locality root, then the blob's intrinsic provenance — the
/// [`read_blob`] / [`open_blob_stream`] dispatch key. Neither component is stored on
/// the [`BlobRef`]: gated locality is mutable shared state (a make_remote/make_local
/// flips it), remote-root locality is declared by the table, and provenance, though
/// intrinsic, is read from the row's declaration, not trusted off the address.
enum BlobSource {
    /// Remote (gate on, or remote root): the cloud, fronted by the device's evictable
    /// cache (`pinned/` or `cache/`, else fetched). Host-provided Remote blobs may
    /// also have a local-store upload staging copy before the cloud read.
    Cache,
    /// Local (gate off) + user-provided: the user's own external file
    /// (`local_blob_refs`).
    External,
    /// Local (gate off) + host-provided: coven's local store
    /// (`storage/local/<namespace>/<id>`).
    LocalStore,
}

/// Resolve the single store a blob's bytes live in from coven's own authoritative
/// state — never a probe. Reads the **locality root** first: the carrying row is found in the
/// table the blob's `namespace` declares ([`BlobDecls::row_for_blob_in_namespace`] —
/// the namespace is the blob's address, so an id colliding across namespaces still
/// reads the right table), then walked up to its gated root or remote root
/// ([`Gates::root_kept_of`]) — the same row→root→gate resolution the make_remote drain
/// runs ([`crate::blob::upload`]), built once here from the declared synced set + the
/// live schema. Gate on, or a remote root, ⇒ [`BlobSource::Cache`] (Remote); gate off
/// ⇒ provenance picks the Local copy: user-provided ⇒ [`BlobSource::External`],
/// host-provided ⇒ [`BlobSource::LocalStore`]. A blob whose namespace declares no
/// table, or whose row reaches no locality root, has no determinable source —
/// [`BlobCacheError::LocalityUnresolved`], surfaced rather than guessed.
async fn resolve_source(db: &Database, blob: &BlobRef) -> Result<BlobSource, BlobCacheError> {
    let tables = db.synced_tables().to_vec();
    let namespace = blob.namespace.clone();
    let id = blob.id.clone();
    let kept: Option<bool> = db
        .call(move |conn| {
            let gates = Gates::from_tables(conn, &tables).map_err(|e| DbError(e.to_string()))?;
            let decls =
                BlobDecls::from_tables(conn, &tables).map_err(|e| DbError(e.to_string()))?;
            match decls
                .row_for_blob_in_namespace(conn, &namespace, &id)
                .map_err(|e| DbError(e.to_string()))?
            {
                Some((table, pk)) => gates
                    .root_kept_of(conn, &table, &pk)
                    .map_err(|e| DbError(e.to_string())),
                // The namespace declares no table, or no row in it carries this id —
                // no gate to read.
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| BlobCacheError::Io(format!("resolve locality for {}: {e}", blob.id)))?;
    match kept {
        // Remote: same source for both provenances.
        Some(true) => Ok(BlobSource::Cache),
        // Local: provenance — intrinsic to the blob — picks which on-device copy.
        Some(false) => Ok(match blob.provenance {
            Provenance::UserProvided => BlobSource::External,
            Provenance::HostProvided => BlobSource::LocalStore,
        }),
        None => Err(BlobCacheError::LocalityUnresolved {
            id: blob.id.clone(),
        }),
    }
}

/// Look up the external file ref for `id`, mapping the DB error into the cache's
/// error type. Used by the Local + user-provided dispatch arm of [`read_blob`] /
/// [`open_blob_stream`]: a `None` there is [`BlobCacheError::NoExternalRef`] (the gate
/// said Local + user-provided, so the ref must exist), not a fall-through.
async fn lookup_external_ref(
    db: &Database,
    id: &str,
) -> Result<Option<crate::db::ExternalBlob>, BlobCacheError> {
    db.external_blob(id)
        .await
        .map_err(|e| BlobCacheError::Io(format!("look up external blob ref for {id}: {e}")))
}

/// A whole-blob vs ranged read of an external (user-provided Local) file. The two
/// reads share the validate-on-read; only the local read primitive differs.
enum ExternalRead {
    Whole,
    Range { offset: u64, len: u64 },
}

/// Read a user-provided Local blob from its registered external file `ext`. The
/// external file is the only copy — no fallback. A failed read surfaces its
/// underlying cause as [`BlobCacheError::ExternalMissing`] (the error is preserved,
/// not collapsed); for a whole read, a length that no longer matches the registered
/// `size` is [`BlobCacheError::ExternalSizeMismatch`].
async fn read_external_file(
    id: &str,
    ext: crate::db::ExternalBlob,
    op: ExternalRead,
) -> Result<Vec<u8>, BlobCacheError> {
    match op {
        ExternalRead::Whole => {
            let bytes = crate::local_blob::read(&ext.path).await.map_err(|e| {
                BlobCacheError::ExternalMissing {
                    id: id.to_string(),
                    path: ext.path.clone(),
                    source: e,
                }
            })?;
            if bytes.len() as u64 != ext.size {
                return Err(BlobCacheError::ExternalSizeMismatch {
                    id: id.to_string(),
                    path: ext.path,
                });
            }
            Ok(bytes)
        }
        ExternalRead::Range { offset, len } => {
            crate::local_blob::read_range(&ext.path, offset, len)
                .await
                .map_err(|e| BlobCacheError::ExternalMissing {
                    id: id.to_string(),
                    path: ext.path,
                    source: e,
                })
        }
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
/// roots are under `storage/`, so on native the `rename` is within one filesystem
/// and atomic — the blob is never visible in both folders or neither. Creates the destination's
/// `{ab}/{cd}` shard directory first (a folder a blob has never lived in yet).
async fn rename_within_storage(
    from: &std::path::Path,
    to: &std::path::Path,
) -> Result<(), BlobCacheError> {
    if let Some(parent) = to.parent() {
        crate::local_blob::create_dir_all(parent)
            .await
            .map_err(BlobCacheError::Io)?;
    }
    crate::local_blob::rename(from, to)
        .await
        .map_err(BlobCacheError::Io)
}
