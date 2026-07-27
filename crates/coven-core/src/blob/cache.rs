//! The device-local cache for **Remote** blobs: bytes on disk, keyed by the exact
//! locator hash,
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
//! folders under the store dir, or in neither. Both are segmented by the blob's
//! namespace, so each namespace's cache evicts against its own budget without
//! touching another's:
//!
//! - `storage/pinned/<namespace>/{ab}/{cd}/<locator-hash>` — kept, budget-exempt. A Remote
//!   blob's cache copy the user pinned for offline (kept from eviction).
//! - `storage/cache/<namespace>/{ab}/{cd}/<locator-hash>` — opportunistic, evictable. A blob
//!   fetched on read (`CacheLazy`) or eagerly on pull (`CacheEager`).
//! - neither — not cached. No file; fetched from the cloud on the next read.
//!
//! Presence is the file on disk; kept-ness is which folder. Nothing the two
//! `readdir`s can't answer, so no metadata sidecar to keep in sync with the disk.
//! Whole reads verify both plaintext size and content hash against the exact
//! row-bound locator before trusting cached bytes; ranged reads do not (see
//! below — a stream cannot afford a whole-file scan per range). A corrupt
//! occupied path fails loudly and is never replaced. Pin/unpin stage a verified copy, publish it without replacing
//! an occupied destination, then remove the source.
//!
//! Both reads **dispatch on coven's own authoritative state** — they never probe
//! every store and take the first hit. The discriminator is the **locality root**
//! plus the blob's intrinsic **provenance**, not "is there a local file here." Coven
//! resolves the blob's backing row (found in the table its `namespace` declares) up
//! to its gated root or remote root (see
//! `Gates::root_kept_of`, then dispatches:
//!
//! - **Remote** with an exact locator ⇒ the bytes live in the cloud fronted by
//!   the device cache. The first legitimate probe runs per-device cache
//!   materialization — which no shared state records — checking `pinned/` then
//!   `cache/`, then fetching the exact cloud object.
//! - **PendingRemote** ⇒ the row's audience is remote but its exact cloud object is
//!   not published yet. Provenance selects the verified upload source: the external
//!   file for a user-provided blob or the local store for a host-provided blob.
//! - **Local** ⇒ the bytes are on-device; provenance picks the copy. A
//!   **user-provided** blob is the user's own external file (`local_blob_refs`), read
//!   straight from its path and validated by size + content hash — its ref MUST exist
//!   ([`BlobCacheError::NoExternalRef`] otherwise). A **host-provided** blob is in the
//!   **local store** ([`local_files`](super::local_files)), its only copy — a miss is
//!   fail-loud corruption ([`BlobCacheError::NoLocalCopy`]). Neither falls through to
//!   the cloud: a Local blob has no cloud copy.
//!
//! `read_blob` returns the entire blob in one call; `open_blob_stream` returns a
//! [`BlobStream`] a host reads ranges from while streaming or seeking. Both resolve
//! the source the same way, and each verifies what its own shape allows:
//!
//! - `read_blob` reads every byte, so it checks the plaintext's size and content
//!   hash against the exact row-bound locator — including on a **cache hit**. That
//!   check is not about cloud authenticity, which the AEAD already settled when the
//!   bytes were fetched: a cache file is unsealed plaintext sitting on local disk,
//!   carrying no tags of its own, so the row's hash is the only thing that can
//!   refuse a file that rotted, was truncated by a partial write, or was edited.
//!   It is free here precisely because this read touches every byte anyway. A cloud
//!   miss fetches + decrypts the exact object once and populates `cache/`.
//! - `open_blob_stream` costs each range its own bytes, so it cannot make that
//!   check — re-hashing per range is the whole-file scan the stream exists to
//!   avoid. A **local** source (including a cache hit) is read plain: its current
//!   bytes are the answer to a read of it, and the one place a blob's bytes are
//!   checked against the row's hash is publication, where they become canonical
//!   synced content. A **Remote uncached** blob is read from the cloud object a
//!   chunk at a time: each sealed chunk's tag covers its bytes, its index, and the
//!   header framing the blob, so a chunk that opens is authentic and verification
//!   is per chunk rather than per object. A blob stored in the clear (a browsable
//!   home) has no tags to check a range against, so it takes the whole-object path
//!   instead — see `open_blob_stream`.
//!
//! A local stream holds the file it opened for its whole life. That is a property,
//! not an optimization — a path can be swapped between two reads, a descriptor
//! cannot, so the stream keeps serving the file it opened even after that file is
//! evicted, renamed, or replaced.
//!
//! The cache has a **per-namespace** size budget the host sets per device (see
//! [`Database::set_cache_budget`]), so a small namespace (`covers`) is never wiped by
//! pressure from a big one (`release_files`). A namespace's budget counts **only**
//! the files under `cache/<namespace>/` — `pinned/` is structurally exempt, and
//! `storage/local` (the local store) is never walked at all. After every populate
//! into a namespace (`read_blob`'s miss-write and `write_blob`),
//! [`evict_to_budget`] sums that namespace's `cache/<namespace>/` files and, if their
//! total exceeds its budget, deletes the oldest by modification time until the total
//! is back under it — touching only that namespace's subtree. Modification time is
//! the recency proxy — there is no `last_accessed` column, the same folder-truth
//! trade-off the whole cache makes; pinning retains the Remote blobs the user chose
//! to keep local. With a namespace's budget unset eviction is off for it and its
//! cache grows without bound. Tests can reset all of `cache/` in one sweep; a pinned
//! blob (in `pinned/`) survives because it lives in the other folder.

use crate::blob::{Provenance, RowBlobAuthority, RowBlobRef};
use crate::database::{Database, DbError};
use crate::store_dir::{PathTokenError, StoreDir};
use crate::sync::storage::{StorageError, SyncStorage};

/// Closed cloud access for one exact Remote blob. Store code resolves the
/// authority; the cache only reads bytes with the supplied protection.
pub(crate) struct RemoteBlobAccess<'a> {
    storage: &'a dyn SyncStorage,
    protection: crate::sync::storage::BlobSpoolProtection,
}

impl<'a> RemoteBlobAccess<'a> {
    pub(crate) fn new(
        storage: &'a dyn SyncStorage,
        protection: crate::sync::storage::BlobSpoolProtection,
    ) -> Self {
        Self {
            storage,
            protection,
        }
    }
}

/// Prefix for the `protocol_state` keys holding each namespace's device-local cache-size
/// budget in bytes (a single decimal value per namespace, not per-blob accounting).
/// The key for one namespace is [`cache_budget_state_key`]. A namespace with no such
/// key has no budget ⇒ eviction off for it ⇒ that namespace's cache grows unbounded.
/// Read/written through [`Database::get_cache_budget`] /
/// [`Database::set_cache_budget`].
pub const CACHE_BUDGET_STATE_KEY_PREFIX: &str = "cache_budget:";

/// The `protocol_state` key holding `namespace`'s cache-size budget. Namespaces are safe
/// path tokens (no `:`), so the `cache_budget:` prefix never collides with one.
pub fn cache_budget_state_key(namespace: &str) -> String {
    format!("{CACHE_BUDGET_STATE_KEY_PREFIX}{namespace}")
}

/// Why a blob-cache operation failed.
#[derive(Debug)]
pub enum BlobCacheError {
    /// A blob `id`/`namespace`/`cloud_path` that can't form a safe path — bad data
    /// that could escape the store dir or can't be partitioned. The blob is
    /// refused before any path is built (the same gate the pull runs).
    Path(PathTokenError),
    /// A cloud read failed: the blob isn't in the cloud, or the backend errored
    /// (surfaced from the exact blob operations on `SyncStorage`).
    Storage(StorageError),
    /// A Remote blob's bytes were needed from the cloud but no cloud home is
    /// connected, so there is no storage to fetch them from. A home-less store
    /// holds only Local blobs (external refs + the local store), which serve
    /// straight off disk and never reach the cloud-miss path; reaching here means
    /// a Remote blob was read with no provider connected — a real fault, surfaced
    /// rather than masked.
    NoCloudHome,
    /// A local-disk failure: a cache write, a folder move, or a test cache reset.
    /// Carries a human-readable cause.
    Io(String),
    /// A blob-metadata query failed — resolving the blob's locality, looking up its
    /// external ref, or reading its cache budget or expected size. A database read
    /// the blob path depends on, distinct from a disk I/O failure.
    Metadata(DbError),
    /// Building the sync storage from config failed — missing credentials or cloud
    /// configuration — when a Remote blob needed it. A configuration fault, not a
    /// disk I/O error.
    StorageSetup(String),
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
    /// different-length file. Terminal like [`Self::ExternalMissing`]: a mismatch
    /// means this is not the exact file coven registered.
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
    /// read. In a consistent store every readable blob has a locality root, so this
    /// is a real fault — surfaced rather than guessing a source by probing.
    LocalityUnresolved { id: String },
    /// The gate resolved a blob to **Local + user-provided**, but no external-ref row
    /// is registered for it. A user-provided Local blob's bytes live only at the user's
    /// path, tracked by that ref; its absence is corruption (a lost or never-written
    /// ref), not a cache miss to fall through — surfaced loud so the host repairs or
    /// re-imports.
    NoExternalRef { id: String },
    /// An authoritative local plaintext file exists at the exact path but its
    /// bytes differ from the row's signed size/hash.
    LocalIntegrity {
        path: std::path::PathBuf,
        expected_size: u64,
        actual_size: u64,
        expected_hash: crate::sync::store_commit::ObjectHash,
        actual_hash: crate::sync::store_commit::ObjectHash,
    },
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
            BlobCacheError::Metadata(e) => write!(f, "blob metadata error: {e}"),
            BlobCacheError::StorageSetup(e) => write!(f, "sync storage setup failed: {e}"),
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
            BlobCacheError::LocalIntegrity {
                path,
                expected_size,
                actual_size,
                expected_hash,
                actual_hash,
            } => write!(
                f,
                "local blob {} has size/hash {actual_size}/{actual_hash}, expected {expected_size}/{expected_hash}",
                path.display()
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

/// Write a Remote blob's plaintext into the evictable cache under a synthetic exact
/// locator hash, so a later test read serves it locally without a cloud round-trip and
/// a pin can promote it to `pinned/`.
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
#[cfg(test)]
pub(crate) async fn write_blob(
    db: &Database,
    store_dir: &StoreDir,
    namespace: &str,
    locator_hash: crate::sync::store_commit::ObjectHash,
    bytes: &[u8],
) -> Result<(), BlobCacheError> {
    let dest = store_dir.cache_blob_path(namespace, locator_hash)?;
    crate::local_blob::write_atomic(&dest, bytes)
        .await
        .map_err(BlobCacheError::Io)?;
    // The write into `cache/<namespace>/` may have pushed that namespace over its
    // budget; evict its oldest files back under it, never the file just written
    // (passed as `protect`). A no-op when the namespace has no budget set.
    evict_to_budget(db, store_dir, namespace, Some(&dest)).await?;
    Ok(())
}

/// Copy a Remote blob's plaintext source file into the evictable cache without
/// holding the whole blob in memory. Uses the same cache placement and eviction
/// contract as the byte-slice cache writer used by tests.
pub(crate) async fn write_blob_from_file(
    db: &Database,
    store_dir: &StoreDir,
    namespace: &str,
    locator_hash: crate::sync::store_commit::ObjectHash,
    plaintext_size: u64,
    plaintext_hash: crate::sync::store_commit::ObjectHash,
    src_path: &std::path::Path,
) -> Result<(), BlobCacheError> {
    let dest = store_dir.cache_blob_path(namespace, locator_hash)?;
    let staged = crate::local_blob::stage_atomic_destination(&dest)
        .await
        .map_err(BlobCacheError::Io)?;
    crate::local_blob::copy_atomic(src_path, staged.path())
        .await
        .map_err(BlobCacheError::Io)?;
    verify_local_file_facts(staged.path(), plaintext_size, plaintext_hash).await?;
    publish_exact_file(staged, plaintext_size, plaintext_hash).await?;
    evict_to_budget(db, store_dir, namespace, Some(&dest)).await?;
    Ok(())
}

/// Write a Remote blob's plaintext straight into the KEPT cache folder
/// (`storage/pinned/<locator-hash>`), so a just-uploaded blob the user pinned for offline is
/// kept local and budget-exempt with no later cloud round-trip. The kept sibling of
/// [`write_blob`] (which writes into the evictable locator-keyed cache).
///
/// Called by the upload drain after a successful upload whose entry is
/// `retain_pinned`: the same plaintext the drain already read to seal is written
/// here, so the pin is populate-on-write rather than fetch-on-read. The bytes are
/// the plaintext (what the cache stores and serves), not the sealed ciphertext in
/// the cloud.
///
/// Unlike [`write_blob`] there is NO post-write eviction: `pinned/` is structurally
/// exempt from the size budget (the sweep never walks it), so a kept populate can
/// neither push the evictable cache over budget nor be trimmed. Later reads verify
/// the file's exact size and hash before trusting the pinned bytes.
pub(crate) async fn populate_pinned(
    store_dir: &StoreDir,
    stored: &crate::blob::locator::StoredBlobRef,
    src_path: &std::path::Path,
) -> Result<(), BlobCacheError> {
    let locator = stored.locator();
    populate_pinned_from_file(
        store_dir,
        locator.namespace(),
        locator.locator_hash(),
        locator.plaintext_size(),
        locator.plaintext_hash(),
        src_path,
    )
    .await
}

pub(crate) async fn populate_pinned_from_file(
    store_dir: &StoreDir,
    namespace: &str,
    locator_hash: crate::sync::store_commit::ObjectHash,
    plaintext_size: u64,
    plaintext_hash: crate::sync::store_commit::ObjectHash,
    src_path: &std::path::Path,
) -> Result<(), BlobCacheError> {
    let dest = store_dir.pinned_blob_path(namespace, locator_hash)?;
    let staged = crate::local_blob::stage_atomic_destination(&dest)
        .await
        .map_err(BlobCacheError::Io)?;
    crate::local_blob::copy_atomic(src_path, staged.path())
        .await
        .map_err(BlobCacheError::Io)?;
    verify_local_file_facts(staged.path(), plaintext_size, plaintext_hash).await?;
    publish_exact_file(staged, plaintext_size, plaintext_hash).await
}

/// Drop one exact Remote blob cache copy from both locator-keyed folders, part of
/// apply-side cleanup when an incoming changeset deletes
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
    db: &Database,
    store_dir: &StoreDir,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    validate_row_reference(db, reference).await?;
    drop_cached_stored_blob(store_dir, remote_stored_ref(reference)?).await
}

pub(crate) async fn drop_cached_stored_blob(
    store_dir: &StoreDir,
    stored: &crate::blob::locator::StoredBlobRef,
) -> Result<(), BlobCacheError> {
    let locator = stored.locator();
    drop_cached_locator(store_dir, locator.namespace(), locator.locator_hash()).await
}

/// Drop one exact locator's cache and pinned copies without touching the
/// logical-id-keyed local source. The source and cache represent different
/// ownership states and can be live independently when logical IDs are reused.
pub(crate) async fn drop_cached_locator(
    store_dir: &StoreDir,
    namespace: &str,
    locator_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    let pinned = store_dir.pinned_blob_path(namespace, locator_hash)?;
    let cache = store_dir.cache_blob_path(namespace, locator_hash)?;
    for path in [pinned, cache] {
        // An absent file in either folder is the expected case (`remove_file`
        // reports it as `Ok(false)`, not an error); every real I/O failure surfaces.
        crate::local_blob::remove_file(&path)
            .await
            .map_err(BlobCacheError::Io)?;
    }
    Ok(())
}

/// Whether a Remote blob's cache copy is currently pinned — present in
/// `storage/pinned/<namespace>/<locator-hash>`. The pin truth is the folder a blob's file
/// lives in, not a table (see the module docs), so this is a single existence
/// check on the kept folder: a blob in `cache/` or in neither folder is not
/// pinned. A failure to even check existence (broken filesystem) is surfaced,
/// never collapsed into "not pinned".
pub async fn is_pinned(
    db: &Database,
    store_dir: &StoreDir,
    reference: &RowBlobRef,
) -> Result<bool, BlobCacheError> {
    validate_row_reference(db, reference).await?;
    let (pinned, _) = remote_cache_paths(store_dir, reference)?;
    match crate::local_blob::exists(&pinned).await {
        Ok(true) => {
            verify_exact_local_file(&pinned, reference).await?;
            Ok(true)
        }
        Ok(false) => Ok(false),
        Err(error) => Err(BlobCacheError::Io(error)),
    }
}

/// Read a blob's whole contents, dispatching on coven's authoritative state — the
/// blob's locality root, then its intrinsic provenance — rather than probing every
/// store and taking the first hit.
///
/// [`resolve_source`] reads the row authority first: **Remote** with an exact
/// locator ⇒ the bytes live in the cloud fronted by the device cache, so the first
/// legitimate probe checks the exact locator's pinned path then cache path for a per-device cache
/// copy and serves a hit. A miss resolves the blob's scope to its encryption key,
/// downloads + decrypts it via [`SyncStorage::stage_verified_blob_plaintext`], writes the
/// whole blob to its locator-keyed cache path (evictable — a fetch-on-read populates the evictable
/// cache, never the kept folder), and returns the bytes it just fetched. Later cache
/// hits verify the file size and hash against the row before trusting it. The read reports
/// success only after the post-populate [`evict_to_budget`] sweep succeeds.
///
/// **Local** or **PendingRemote** ⇒ the bytes are on-device, and provenance picks which copy:
/// a **user-provided** blob is the user's own external file (`local_blob_refs` row),
/// read straight from its path and validated by size + content hash — its ref MUST exist
/// ([`BlobCacheError::NoExternalRef`] if not: a Local user-provided blob without its
/// ref is corruption, not a fall-through), and a vanished/short file is
/// [`BlobCacheError::ExternalMissing`] / [`BlobCacheError::ExternalSizeMismatch`]. A
/// **host-provided** blob is in the **local store** (`storage/local/<namespace>/<id>`,
/// see [`local_files`](super::local_files)), its only copy — a miss is
/// [`BlobCacheError::NoLocalCopy`], fail-loud corruption, never a cloud fetch.
pub(crate) async fn read_blob(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    validate_row_reference(db, reference).await?;
    let blob = reference.blob();
    let bytes = match resolve_source(reference)? {
        // Remote: the bytes live in the cloud fronted by the device cache.
        BlobSource::Cache => read_remote_whole(db, store_dir, remote, reference).await,
        // Local + user-provided: the user's own external file. Its ref must be present
        // — gate-resolved Local + UserProvided with no ref is corruption, not a miss.
        BlobSource::External => {
            let ext = lookup_external_ref(db, reference).await?.ok_or_else(|| {
                BlobCacheError::NoExternalRef {
                    id: blob.id.clone(),
                }
            })?;
            read_external_file(reference, ext).await
        }
        // Local + host-provided: the local store is the ONLY copy (a Local blob has no
        // cloud copy). A miss is fail-loud corruption, not a cache miss to refetch.
        BlobSource::LocalStore => {
            let path = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
            match crate::local_blob::exists(&path).await {
                Ok(true) => {}
                Ok(false) => {
                    return Err(BlobCacheError::NoLocalCopy {
                        namespace: blob.namespace.clone(),
                        id: blob.id.clone(),
                    });
                }
                Err(error) => return Err(BlobCacheError::Io(error)),
            }
            read_exact_local_file(&path, reference).await
        }
    }?;
    validate_row_reference(db, reference).await?;
    Ok(bytes)
}

/// Serve a Remote blob whole. The one legitimate probe — per-device cache
/// materialization, a filesystem fact no shared state holds — checks the exact
/// locator's pinned path then cache path and serves a hit, otherwise reading the
/// exact cloud object. Split from [`read_blob`] so the whole-blob Remote path reads
/// as one branch of the authority dispatch.
async fn read_remote_whole(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    let blob = reference.blob();
    // A cache hit (`pinned/` or `cache/`) serves the file straight off disk — the
    // pinned→cache probe `read_cached_exact` runs. An existence-check failure there is
    // surfaced, not collapsed into a miss: re-downloading over a present file would be
    // wasteful and could mask a real fault.
    if let Some(bytes) = read_cached_exact(store_dir, reference).await? {
        return Ok(bytes);
    }

    // Miss: fetch from the cloud and populate the evictable cache. A home-less
    // store reaches here only when a Remote blob is read with no provider
    // connected — there is no storage to fetch it from, so surface that fault.
    let (_, cache) = remote_cache_paths(store_dir, reference)?;
    let remote = remote.ok_or(BlobCacheError::NoCloudHome)?;
    let stored = remote_stored_ref(reference)?;
    let staged = remote
        .storage
        .stage_verified_blob_plaintext(stored, remote.protection, &cache)
        .await?;
    let bytes = crate::local_blob::read(staged.path())
        .await
        .map_err(BlobCacheError::Io)?;
    validate_row_reference(db, reference).await?;
    publish_materialization(staged, reference).await?;
    // The populate may have pushed `cache/` over budget; evict the oldest files
    // back under it, never the file just written (passed as `protect`) — so this
    // read's own sweep can't drop the bytes it just fetched, which it returns below.
    // A no-op when no budget is set.
    //
    evict_to_budget(db, store_dir, &blob.namespace, Some(&cache)).await?;
    Ok(bytes)
}

/// One opened blob, ready to serve ranges. Held by a host that is streaming or
/// seeking a blob (playback probing a codec header, then a tail, then decoding
/// forward) rather than loading it whole.
///
/// A range costs the bytes it returns, from either source, but for different
/// reasons:
///
/// - **Local** (an external file, the local store, or a cache copy) — the stream
///   holds the open file and every range is one positioned read of it. No
///   hashing: a local file's current bytes are the answer to a read of it, and a
///   blob's bytes are checked against the hash its row declares at publication,
///   which is where they become canonical synced content.
/// - **Remote, uncached** — the stream fetches only the sealed chunks covering
///   the range and opens them. A chunk that opens is authentic: the provider
///   holds no key and cannot forge a tag, and the tag covers the chunk's bytes,
///   its index, and the header framing the blob. So verification is per chunk,
///   which is what lets a range cost a range rather than the object.
///
/// Holding the local descriptor is a property, not an optimization: a path can be
/// replaced between two reads, a descriptor cannot, so the stream keeps serving
/// the file it opened even if that file is later evicted, renamed, or replaced.
/// An **in-place** rewrite of that same file does reach the stream — that is a
/// file the user owns and edits; coven's own copies are published by rename or
/// hard link and never written in place.
pub struct BlobStream {
    blob: crate::blob::BlobRef,
    source: BlobStreamSource,
}

/// Where an open stream's ranges come from.
enum BlobStreamSource {
    /// A file on this device: the user's own external file, the local store, or
    /// a cache copy of a Remote blob.
    Local(crate::local_blob::OpenFile),
    /// A Remote blob with no cache copy: ranges are served from the cloud object
    /// a chunk at a time.
    Remote(crate::sync::cloud_storage::BlobRangeReader),
}

impl BlobStream {
    /// The blob's whole plaintext length. Every range must lie inside it.
    pub fn plaintext_size(&self) -> u64 {
        match &self.source {
            BlobStreamSource::Local(file) => file.size(),
            BlobStreamSource::Remote(reader) => reader.plaintext_size(),
        }
    }

    /// Serve `len` plaintext bytes starting at `offset`.
    ///
    /// `len == 0` is an empty result, and an `offset + len` past the blob's
    /// plaintext size (or an overflow) is an error, never a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, BlobCacheError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            BlobCacheError::Io(format!(
                "blob range overflow for {}: offset={offset}, len={len}",
                self.blob.id
            ))
        })?;
        let source_size = self.plaintext_size();
        if end > source_size {
            return Err(BlobCacheError::Io(format!(
                "blob range {offset}..{end} for {} exceeds blob size {source_size}",
                self.blob.id
            )));
        }
        match &self.source {
            BlobStreamSource::Local(file) => {
                file.read_at(offset, len).await.map_err(BlobCacheError::Io)
            }
            BlobStreamSource::Remote(reader) => reader
                .read_at(offset, len)
                .await
                .map_err(BlobCacheError::Storage),
        }
    }
}

/// Open a blob for ranged reading, for a host streaming or seeking it without
/// loading the whole file. The ranged sibling of [`read_blob`], which stays the
/// primitive for a one-shot whole read.
///
/// Opening resolves the blob's source the same way [`read_blob`] does
/// ([`resolve_source`] reads the locality root, then provenance) and reads no
/// content: every range costs its own bytes, from either source.
///
/// - **Remote** ⇒ the exact locator's `pinned/` then `cache/` path; a hit opens
///   the cache file. A miss reads the cloud object itself, fetching only the
///   sealed chunks each range covers — so serving a range never waits on the
///   whole object, and a miss does not populate the cache (that would be the
///   whole-object download this path exists to remove; [`read_blob`] still
///   populates). [`NoCloudHome`] when a Remote blob is opened with no provider
///   connected.
/// - **Local + user-provided** ⇒ the user's own external file ([`NoExternalRef`] if
///   its ref is absent, [`ExternalMissing`] if the file is gone,
///   [`ExternalSizeMismatch`] if its length no longer matches the registered size).
/// - **Local + host-provided** ⇒ the local store, coven's only copy
///   ([`NoLocalCopy`] on a miss, never a cloud fetch).
///
/// The row reference is validated when the stream opens, which binds the stream to
/// that exact row version's plaintext. Ranges are not revalidated against the
/// database: a later row replacement produces a *different* blob, and this stream
/// was opened for the one the caller asked for — it keeps serving those proven
/// bytes rather than half a file from each.
///
/// [`NoCloudHome`]: BlobCacheError::NoCloudHome
/// [`NoExternalRef`]: BlobCacheError::NoExternalRef
/// [`ExternalMissing`]: BlobCacheError::ExternalMissing
/// [`ExternalSizeMismatch`]: BlobCacheError::ExternalSizeMismatch
/// [`NoLocalCopy`]: BlobCacheError::NoLocalCopy
pub(crate) async fn open_blob_stream(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
) -> Result<BlobStream, BlobCacheError> {
    validate_row_reference(db, reference).await?;
    let blob = reference.blob();
    let source = match resolve_source(reference)? {
        // Remote: the cache copy, else the cloud object read a chunk at a time.
        BlobSource::Cache => open_remote_stream(db, store_dir, remote, reference).await,
        // Local + user-provided: the user's own external file. Its ref must be present
        // — gate-resolved Local + UserProvided with no ref is corruption, not a miss.
        BlobSource::External => {
            let ext = lookup_external_ref(db, reference).await?.ok_or_else(|| {
                BlobCacheError::NoExternalRef {
                    id: blob.id.clone(),
                }
            })?;
            open_external_file(reference, ext).await
        }
        // Local + host-provided: the local store is the ONLY copy (a Local blob has no
        // cloud copy). A miss is fail-loud corruption, not a cache miss to refetch.
        BlobSource::LocalStore => {
            let path = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
            match crate::local_blob::exists(&path).await {
                Ok(true) => {}
                Ok(false) => {
                    return Err(BlobCacheError::NoLocalCopy {
                        namespace: blob.namespace.clone(),
                        id: blob.id.clone(),
                    });
                }
                Err(error) => return Err(BlobCacheError::Io(error)),
            }
            open_local_file(&path).await
        }
    }?;
    validate_row_reference(db, reference).await?;
    Ok(BlobStream {
        blob: blob.clone(),
        source,
    })
}

/// Open a Remote blob for a stream. A cache hit opens that file and serves
/// positioned reads of it. A miss opens the cloud object itself: ranges fetch
/// only the sealed chunks covering them, so a stream that reads a codec header
/// and a tail transfers a header and a tail — never the whole object, and never
/// as a precondition for serving the first range.
///
/// So a sealed miss does not populate the cache: doing so would mean downloading
/// the whole blob before answering a range, which is the cost this path exists to
/// remove. A whole read ([`read_blob`]) still populates, and that is the
/// operation that legitimately reads every byte.
///
/// A blob stored in the clear (a browsable home) has no per-chunk tags, so a
/// range cannot be verified; that one takes the whole-object path instead.
async fn open_remote_stream(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
) -> Result<BlobStreamSource, BlobCacheError> {
    // The one legitimate probe — per-device cache materialization, a filesystem fact
    // no shared state holds. An existence-check failure is surfaced, not read as a miss.
    if let Some(hit) = cached_blob_path(store_dir, reference).await? {
        return open_local_file(hit.path()).await;
    }

    // Not cached: read the cloud object directly. A home-less store reaches here
    // only when a Remote blob is read with no provider connected — there is no
    // storage to fetch it from, so surface that fault.
    let remote = remote.ok_or(BlobCacheError::NoCloudHome)?;
    let stored = remote_stored_ref(reference)?;
    if !stored.locator().is_sealed() {
        // A browsable home stores the plaintext in the clear, so its objects
        // carry no tags and a range has nothing to check the provider's answer
        // against. There the whole blob is materialized and checked against the
        // row's content hash before a byte is served — a ranged read of an
        // unauthenticated object would serve whatever the provider returned.
        return materialize_and_open_remote(db, store_dir, remote, reference).await;
    }
    let reader = remote
        .storage
        .open_blob_range_reader(stored, remote.protection)
        .await?;
    Ok(BlobStreamSource::Remote(reader))
}

/// Download, verify, and cache a Remote blob's whole plaintext, then stream from
/// the cached file. The path for a blob whose stored form cannot be verified a
/// range at a time.
///
/// The handle is opened on the staged file *before* publication, so it refers to
/// the published inode: neither the publish nor a later eviction can take the
/// bytes this stream is serving.
async fn materialize_and_open_remote(
    db: &Database,
    store_dir: &StoreDir,
    remote: RemoteBlobAccess<'_>,
    reference: &RowBlobRef,
) -> Result<BlobStreamSource, BlobCacheError> {
    let stored = remote_stored_ref(reference)?;
    let (_, cache) = remote_cache_paths(store_dir, reference)?;
    let staged = remote
        .storage
        .stage_verified_blob_plaintext(stored, remote.protection, &cache)
        .await?;
    verify_exact_local_file(staged.path(), reference).await?;
    let source = open_local_file(staged.path()).await?;
    validate_row_reference(db, reference).await?;
    publish_materialization(staged, reference).await?;
    // The populate may have pushed `cache/` over budget; evict the oldest files
    // back under it, never the file just written (passed as `protect`). This
    // stream's handle survives either way — it holds the inode, not the name.
    evict_to_budget(db, store_dir, &reference.blob().namespace, Some(&cache)).await?;
    Ok(source)
}

/// Open a local plaintext file for a stream: no content read, no hashing.
async fn open_local_file(path: &std::path::Path) -> Result<BlobStreamSource, BlobCacheError> {
    crate::local_blob::OpenFile::open(path)
        .await
        .map(BlobStreamSource::Local)
        .map_err(BlobCacheError::Io)
}

/// Open a user-provided Local blob's registered external file for a stream. The
/// external file is the only copy — no fallback — so a failure to open it is
/// [`BlobCacheError::ExternalMissing`] with its underlying cause preserved, and a
/// length that no longer matches the registered `size` is
/// [`BlobCacheError::ExternalSizeMismatch`]. The length is all that is checked:
/// a file the user is free to edit answers a read with its current bytes, and
/// the row's hash is what publication checks, not what a read does.
async fn open_external_file(
    reference: &RowBlobRef,
    ext: crate::db::ExternalBlob,
) -> Result<BlobStreamSource, BlobCacheError> {
    let id = &reference.blob().id;
    let file = crate::local_blob::OpenFile::open(&ext.path)
        .await
        .map_err(|source| BlobCacheError::ExternalMissing {
            id: id.to_string(),
            path: ext.path.clone(),
            source,
        })?;
    if file.size() != ext.size || file.size() != reference.plaintext_size() {
        return Err(BlobCacheError::ExternalSizeMismatch {
            id: id.clone(),
            path: ext.path,
        });
    }
    Ok(BlobStreamSource::Local(file))
}

/// Stage a Remote blob's whole verified plaintext beside `dest` without making
/// `dest` visible. Uses an exact cache copy when present (`pinned/` or `cache/`),
/// otherwise streams the exact cloud object. The returned stage is not visible at
/// the destination; its caller chooses how to publish it.
pub(crate) async fn stage_remote_blob_plaintext(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
    dest: &std::path::Path,
) -> Result<crate::local_blob::AtomicStagedFile, BlobCacheError> {
    validate_row_reference(db, reference).await?;
    if let Some(hit) = cached_blob_path(store_dir, reference).await? {
        let staged = stage_exact_local_copy(hit.path(), dest, reference).await?;
        validate_row_reference(db, reference).await?;
        return Ok(staged);
    }

    let remote = remote.ok_or(BlobCacheError::NoCloudHome)?;
    let stored = remote_stored_ref(reference)?;
    let staged = remote
        .storage
        .stage_verified_blob_plaintext(stored, remote.protection, dest)
        .await?;
    validate_row_reference(db, reference).await?;
    Ok(staged)
}

/// Ensure the exact current row blob plaintext is durable on this device.
/// Remote blobs publish to their locator-keyed evictable cache path. Local and
/// pending-remote blobs already have an authoritative local source, which this
/// operation exact-verifies without creating a remote cache entry.
pub(crate) async fn materialize_row_blob(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    validate_row_reference(db, reference).await?;
    match resolve_source(reference)? {
        BlobSource::Cache => {
            let (_, destination) = remote_cache_paths(store_dir, reference)?;
            if cached_blob_path_with_facts(store_dir, reference)
                .await?
                .is_some()
            {
                validate_row_reference(db, reference).await?;
                return Ok(());
            }
            let staged =
                stage_remote_blob_plaintext(db, store_dir, remote, reference, &destination).await?;
            verify_exact_local_file(staged.path(), reference).await?;
            validate_row_reference(db, reference).await?;
            publish_materialization(staged, reference).await
        }
        BlobSource::External => {
            let external = lookup_external_ref(db, reference).await?.ok_or_else(|| {
                BlobCacheError::NoExternalRef {
                    id: reference.blob().id.clone(),
                }
            })?;
            verify_external_file(reference, &external).await?;
            validate_row_reference(db, reference).await
        }
        BlobSource::LocalStore => {
            let blob = reference.blob();
            let path = store_dir.local_blob_path(&blob.namespace, &blob.id)?;
            match crate::local_blob::exists(&path).await {
                Ok(true) => verify_exact_local_file(&path, reference).await?,
                Ok(false) => {
                    return Err(BlobCacheError::NoLocalCopy {
                        namespace: blob.namespace.clone(),
                        id: blob.id.clone(),
                    });
                }
                Err(error) => return Err(BlobCacheError::Io(error)),
            }
            validate_row_reference(db, reference).await
        }
    }
}

async fn publish_materialization(
    staged: crate::local_blob::AtomicStagedFile,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    publish_exact_file(
        staged,
        reference.plaintext_size(),
        reference.plaintext_hash(),
    )
    .await
}

async fn publish_exact_file(
    staged: crate::local_blob::AtomicStagedFile,
    expected_size: u64,
    expected_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    match staged.commit_new().await {
        Ok(()) => Ok(()),
        Err(crate::local_blob::CommitNewFileError::DestinationExists(path)) => {
            verify_local_file_facts(&path, expected_size, expected_hash).await
        }
        Err(error) => Err(BlobCacheError::Io(error.to_string())),
    }
}

/// Materialize a Remote blob's whole plaintext into a coven-owned destination
/// without replacing an occupied path. An occupied exact file is idempotent; an
/// occupied file with different size or bytes fails loudly.
pub(crate) async fn materialize_remote_blob_to_file(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
    dest: &std::path::Path,
) -> Result<u64, BlobCacheError> {
    let staged = stage_remote_blob_plaintext(db, store_dir, remote, reference, dest).await?;
    verify_exact_local_file(staged.path(), reference).await?;
    validate_row_reference(db, reference).await?;
    publish_materialization(staged, reference).await?;
    Ok(reference.plaintext_size())
}

/// Ensure a blob is local AND protected: present at its locator-keyed pinned path, exempt
/// from the evictable cache. A pin POPULATES — if the blob isn't cached it is
/// fetched first — so it is not a flag flip. Idempotent.
///
/// Pin one Remote blob into its locator-keyed pinned path: a no-op if already pinned, a verified move
/// from the evictable cache if staged there, else a cloud fetch straight into
/// `pinned/`. [`crate::sync::store::blob::pin`] dispatches this per blob, up to
/// its concurrency limit, and takes `&[RowBlobRef]` rather than ids because
/// every operation must use and revalidate the exact row version and locator.
pub(crate) async fn pin_one(
    db: &Database,
    store_dir: &StoreDir,
    remote: Option<RemoteBlobAccess<'_>>,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    validate_row_reference(db, reference).await?;
    let (pinned, _) = remote_cache_paths(store_dir, reference)?;

    // Already protected — idempotent no-op. A failure to even check existence
    // (broken filesystem) is surfaced, not collapsed into "absent": fetching and
    // overwriting a present pinned blob would be wasteful and could mask a real
    // fault, the same posture `read_blob` takes on its hit check.
    match crate::local_blob::exists(&pinned).await {
        Ok(true) => {
            verify_exact_local_file(&pinned, reference).await?;
            return Ok(());
        }
        Ok(false) => {}
        Err(e) => return Err(BlobCacheError::Io(e)),
    }

    // Staged or read-populated in the evictable cache — promote it from a staged
    // verified copy (no cloud fetch). An `exists`
    // failure here is surfaced too, never read as "not cached" (which would
    // re-fetch over a present file).
    match cached_blob_path_with_facts(store_dir, reference).await? {
        Some(CachedBlobPath::Pinned(_)) => return Ok(()),
        Some(CachedBlobPath::Cache(path)) => {
            return move_exact_cache_file(db, &path, &pinned, reference).await;
        }
        None => {}
    }

    // In neither folder — fetch from the cloud straight into `pinned/`. A
    // home-less store has no storage to fetch a Remote blob from; surface it.
    materialize_remote_blob_to_file(db, store_dir, remote, reference, &pinned).await?;
    Ok(())
}

/// Drop a Remote blob's pin: move its locator-keyed pinned file to the evictable path so
/// the cache copy stays (still readable) but is now evictable. Not a delete.
///
/// A pin keeps a specific Remote blob's cache copy from eviction; unpin reverses it
/// regardless of the blob's [`CacheFill`](crate::blob::CacheFill) — a `CacheEager` blob lands in the
/// evictable cache on pull (it is not auto-pinned), so unpinning one that was never
/// pinned is simply a no-op (it is already as-evictable-as-it-gets).
pub async fn unpin(
    db: &Database,
    store_dir: &StoreDir,
    blobs: &[RowBlobRef],
) -> Result<(), BlobCacheError> {
    for reference in blobs {
        validate_row_reference(db, reference).await?;
        let (pinned, cache) = remote_cache_paths(store_dir, reference)?;

        // Move it into the evictable cache if it is currently pinned. If it isn't in
        // `pinned/` (already in `cache/`, or remote), there is nothing to demote —
        // the blob is already as-evictable-as-it-gets, so this is a no-op. A failure
        // to even check existence is surfaced, never collapsed into "absent": unpin
        // must not report success over a broken-filesystem check.
        match crate::local_blob::exists(&pinned).await {
            Ok(true) => move_exact_cache_file(db, &pinned, &cache, reference).await?,
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
pub async fn clear_cache(store_dir: &StoreDir) -> Result<(), BlobCacheError> {
    let cache_dir = store_dir.cache_dir();
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
/// (`read_blob`'s miss-write, `write_blob`).
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
/// locator-keyed cache path; a bare sweep passes `None`): it is **excluded from the
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
/// A file that has vanished by the time it is deleted (a concurrent sweep or test
/// cache reset already removed it) is the one legitimate skip — logged at debug,
/// its now-absent bytes dropped from the running total. Every other stat or delete
/// failure is surfaced, never swallowed: a cache that can't be measured or trimmed
/// must fail loudly, not silently drift over budget.
pub async fn evict_to_budget(
    db: &Database,
    store_dir: &StoreDir,
    namespace: &str,
    protect: Option<&std::path::Path>,
) -> Result<(), BlobCacheError> {
    let budget = match db
        .get_cache_budget(namespace)
        .await
        .map_err(BlobCacheError::Metadata)?
    {
        Some(budget) => budget,
        // This namespace has no budget set — its cache is unlimited, so there is
        // nothing to enforce. Another namespace's budget never reaches here.
        None => return Ok(()),
    };

    let mut entries = crate::local_blob::walk_files(&store_dir.cache_namespace_dir(namespace)?)
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
            // The file is already gone (a concurrent sweep or test reset). Its bytes
            // are no longer on disk, so drop them from the total and move on — the
            // one legitimate skip, not a masked failure.
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
    /// cache (`pinned/` or `cache/`, else fetched).
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
/// ([`Gates::root_kept_of`]) using the database's open-time schema models — the same
/// row→root→gate resolution the make_remote drain runs ([`crate::blob::upload`]).
/// Gate on, or a remote root, ⇒ [`BlobSource::Cache`] (Remote); gate off ⇒ provenance
/// picks the Local copy: user-provided ⇒ [`BlobSource::External`], host-provided ⇒
/// [`BlobSource::LocalStore`]. A blob whose namespace declares no table, or whose row
/// reaches no locality root, has no determinable source —
/// [`BlobCacheError::LocalityUnresolved`], surfaced rather than guessed.
fn resolve_source(reference: &RowBlobRef) -> Result<BlobSource, BlobCacheError> {
    match reference.authority() {
        RowBlobAuthority::Remote(_) => Ok(BlobSource::Cache),
        RowBlobAuthority::Local | RowBlobAuthority::PendingRemote(_) => {
            Ok(match reference.blob().provenance {
                Provenance::UserProvided => BlobSource::External,
                Provenance::HostProvided => BlobSource::LocalStore,
            })
        }
    }
}

async fn validate_row_reference(
    db: &Database,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    db.validate_row_blob_ref(reference)
        .await
        .map_err(BlobCacheError::Metadata)
}

fn remote_stored_ref(
    reference: &RowBlobRef,
) -> Result<&crate::blob::locator::StoredBlobRef, BlobCacheError> {
    reference
        .stored()
        .ok_or_else(|| BlobCacheError::LocalityUnresolved {
            id: reference.blob().id.clone(),
        })
}

fn remote_cache_paths(
    store_dir: &StoreDir,
    reference: &RowBlobRef,
) -> Result<(std::path::PathBuf, std::path::PathBuf), BlobCacheError> {
    let stored = remote_stored_ref(reference)?;
    let namespace = stored.locator().namespace();
    let locator_hash = stored.locator().locator_hash();
    Ok((
        store_dir.pinned_blob_path(namespace, locator_hash)?,
        store_dir.cache_blob_path(namespace, locator_hash)?,
    ))
}

async fn verify_exact_local_file(
    path: &std::path::Path,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    verify_local_file_facts(path, reference.plaintext_size(), reference.plaintext_hash()).await
}

async fn verify_local_file_facts(
    path: &std::path::Path,
    expected_size: u64,
    expected_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    let (actual_size, actual_hash) = crate::local_blob::exact_file_facts(path)
        .await
        .map_err(BlobCacheError::Io)?;
    verify_local_file_identity_values(path, expected_size, expected_hash, actual_size, actual_hash)
}

async fn read_exact_local_file(
    path: &std::path::Path,
    reference: &RowBlobRef,
) -> Result<Vec<u8>, BlobCacheError> {
    let (bytes, actual_size, actual_hash) = crate::local_blob::read_with_facts(path)
        .await
        .map_err(BlobCacheError::Io)?;
    verify_local_file_identity(path, reference, actual_size, actual_hash)?;
    Ok(bytes)
}

fn verify_local_file_identity(
    path: &std::path::Path,
    reference: &RowBlobRef,
    actual_size: u64,
    actual_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    verify_local_file_identity_values(
        path,
        reference.plaintext_size(),
        reference.plaintext_hash(),
        actual_size,
        actual_hash,
    )
}

fn verify_local_file_identity_values(
    path: &std::path::Path,
    expected_size: u64,
    expected_hash: crate::sync::store_commit::ObjectHash,
    actual_size: u64,
    actual_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    if actual_size != expected_size || actual_hash != expected_hash {
        return Err(BlobCacheError::LocalIntegrity {
            path: path.to_path_buf(),
            expected_size,
            actual_size,
            expected_hash,
            actual_hash,
        });
    }
    Ok(())
}

/// Look up the external file ref for `id`, mapping the DB error into the cache's
/// error type. Used by the Local + user-provided dispatch arm of [`read_blob`] /
/// [`open_blob_stream`]: a `None` there is [`BlobCacheError::NoExternalRef`] (the gate
/// said Local + user-provided, so the ref must exist), not a fall-through.
async fn lookup_external_ref(
    db: &Database,
    reference: &RowBlobRef,
) -> Result<Option<crate::db::ExternalBlob>, BlobCacheError> {
    db.external_blob_for_row(reference)
        .await
        .map_err(BlobCacheError::Metadata)
}

/// The cache folder path that currently holds a Remote blob's plaintext, checking
/// `pinned/` then `cache/`. A failure to check either path is surfaced, never
/// collapsed into a miss.
enum CachedBlobPath {
    Pinned(std::path::PathBuf),
    Cache(std::path::PathBuf),
}

impl CachedBlobPath {
    fn path(&self) -> &std::path::Path {
        match self {
            CachedBlobPath::Pinned(path) | CachedBlobPath::Cache(path) => path,
        }
    }
}

async fn cached_blob_path_with_facts(
    store_dir: &StoreDir,
    reference: &RowBlobRef,
) -> Result<Option<CachedBlobPath>, BlobCacheError> {
    let hit = cached_blob_path(store_dir, reference).await?;
    if let Some(hit) = &hit {
        verify_exact_local_file(hit.path(), reference).await?;
    }
    Ok(hit)
}

async fn cached_blob_path(
    store_dir: &StoreDir,
    reference: &RowBlobRef,
) -> Result<Option<CachedBlobPath>, BlobCacheError> {
    let (pinned, cache) = remote_cache_paths(store_dir, reference)?;
    for hit in [CachedBlobPath::Pinned(pinned), CachedBlobPath::Cache(cache)] {
        match crate::local_blob::exists(hit.path()).await {
            Ok(true) => return Ok(Some(hit)),
            Ok(false) => {}
            Err(error) => return Err(BlobCacheError::Io(error)),
        }
    }
    Ok(None)
}

/// Read a Remote blob's plaintext from the cache only — the locator-keyed pinned
/// path then the locator-keyed evictable path, in order — returning `None` when it
/// is in neither folder. No cloud fetch and no local-store check. A failure to
/// even check existence (broken filesystem) is surfaced, never collapsed into
/// `None`.
pub(crate) async fn read_cached_exact(
    store_dir: &StoreDir,
    reference: &RowBlobRef,
) -> Result<Option<Vec<u8>>, BlobCacheError> {
    let Some(hit) = cached_blob_path(store_dir, reference).await? else {
        return Ok(None);
    };
    read_exact_local_file(hit.path(), reference).await.map(Some)
}

/// Read a user-provided Local blob whole from its registered external file `ext`.
/// The external file is the only copy — no fallback. A failed read surfaces its
/// underlying cause as [`BlobCacheError::ExternalMissing`] (the error is preserved,
/// not collapsed).
async fn read_external_file(
    reference: &RowBlobRef,
    ext: crate::db::ExternalBlob,
) -> Result<Vec<u8>, BlobCacheError> {
    let (bytes, actual_size, actual_hash) = crate::local_blob::read_with_facts(&ext.path)
        .await
        .map_err(|source| BlobCacheError::ExternalMissing {
            id: reference.blob().id.clone(),
            path: ext.path.clone(),
            source,
        })?;
    verify_external_facts(reference, &ext, actual_size, actual_hash)?;
    Ok(bytes)
}

async fn verify_external_file(
    reference: &RowBlobRef,
    ext: &crate::db::ExternalBlob,
) -> Result<(), BlobCacheError> {
    let (actual_size, actual_hash) = crate::local_blob::exact_file_facts(&ext.path)
        .await
        .map_err(|source| BlobCacheError::ExternalMissing {
            id: reference.blob().id.clone(),
            path: ext.path.clone(),
            source,
        })?;
    verify_external_facts(reference, ext, actual_size, actual_hash)
}

/// Check an external file's measured facts against both the registered ref length
/// and the exact row-bound locator. A user-owned file can drift either way: a length
/// that no longer matches the registered `size` is
/// [`BlobCacheError::ExternalSizeMismatch`], and same-length different bytes are
/// caught by the locator's content hash.
fn verify_external_facts(
    reference: &RowBlobRef,
    ext: &crate::db::ExternalBlob,
    actual_size: u64,
    actual_hash: crate::sync::store_commit::ObjectHash,
) -> Result<(), BlobCacheError> {
    if actual_size != ext.size || actual_size != reference.plaintext_size() {
        return Err(BlobCacheError::ExternalSizeMismatch {
            id: reference.blob().id.clone(),
            path: ext.path.clone(),
        });
    }
    verify_local_file_identity(&ext.path, reference, actual_size, actual_hash)
}

/// Move one exact locator file between cache folders. The source is copied into an
/// operation-owned stage and exact-verified before no-replace publication, so an
/// eviction cannot invalidate the bytes being published. The row reference is
/// revalidated immediately before publication.
async fn move_exact_cache_file(
    db: &Database,
    from: &std::path::Path,
    to: &std::path::Path,
    reference: &RowBlobRef,
) -> Result<(), BlobCacheError> {
    let staged = stage_exact_local_copy(from, to, reference).await?;
    validate_row_reference(db, reference).await?;
    publish_materialization(staged, reference).await?;
    crate::local_blob::remove_file(from)
        .await
        .map_err(BlobCacheError::Io)?;
    crate::local_blob::sync_parent_dir(from)
        .await
        .map_err(BlobCacheError::Io)
}

async fn stage_exact_local_copy(
    from: &std::path::Path,
    to: &std::path::Path,
    reference: &RowBlobRef,
) -> Result<crate::local_blob::AtomicStagedFile, BlobCacheError> {
    let staged = crate::local_blob::stage_atomic_destination(to)
        .await
        .map_err(BlobCacheError::Io)?;
    let (actual_size, actual_hash) = crate::local_blob::copy_atomic_with_facts(from, staged.path())
        .await
        .map_err(BlobCacheError::Io)?;
    verify_local_file_identity(from, reference, actual_size, actual_hash)?;
    Ok(staged)
}
