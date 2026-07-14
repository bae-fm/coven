//! The blob engine: coven's single owner of a blob's whole durability lifecycle.
//!
//! coven syncs opaque encrypted blobs referenced by DB rows. By default it owns
//! the cloud layout (an immutable generated location containing the blob id) and
//! encryption; the host decides which rows carry blobs, where their plaintext
//! lives locally, and how each is scoped for encryption. A home configured for
//! the unobfuscated blob-path scheme instead stores each blob at the consumer's
//! readable [`BlobRef::cloud_path`] so the bucket is browsable.
//!
//! # The coven concept tree
//!
//! A blob has two **declared** properties — [`Provenance`] (its Local story) and
//! [`CacheFill`] (its Remote story) — and one **state**, locality, flipped by the
//! transitions. The cache is a *mechanism* that serves Remote blobs; it is not a
//! kind of blob.
//!
//! ```text
//! A blob the host declares with:
//!
//! provenance — its LOCAL story: where the bytes live when Local, and the
//!              Remote→Local path requirement
//!    ├─ user-provided   the user's file at a path; coven references it.
//!    │                  Remote→Local writes the bytes back to a user file → NEEDS A PATH.
//!    └─ host-provided   bae hands coven the data; coven keeps it in its local store.
//!                       Remote→Local restores it to the local store → no path.
//!
//! cache fill — its REMOTE story: how a device gets the bytes when the release is
//!              Remote. A cache-mechanism setting; applies to ANY blob, regardless of
//!              provenance, once it is Remote.
//!    ├─ CacheEager   fetched into the cache on pull, with the SQL row   (covers)
//!    └─ CacheLazy    fetched into the cache on first read               (audio — big, fetch what you play)
//!
//! and a current state:
//!
//! locality
//!    ├─ Local    bytes on-device — the user's path (user-provided) or coven's local store (host-provided)
//!    └─ Remote   bytes in the cloud; each device's local copy is a CACHE copy, filled
//!                per `cache fill`, kept-or-evicted per `pin`
//!
//! namespace (bucket)   the blob's category — release_files · covers · artist_images
//!
//! transitions
//!    ├─ Local → Remote   upload the bytes; now cache-distributed to every device per cache fill
//!    └─ Remote → Local   bring the bytes back to a local file — path required iff user-provided
//!
//! cache budget   per-NAMESPACE size limit; each namespace evicts independently, so
//!                evicting release_files (big) never touches covers (small reserved slice)
//! pin            keep one specific Remote blob's cache copy from eviction (e.g. a
//!                release the user pinned for offline)
//! ```
//!
//! ## The cache vs local files
//!
//! The cache holds local copies of **Remote** blobs (filled per `cache fill`,
//! evicted per budget unless pinned). It is **segmented by namespace**: each
//! namespace has its own configurable cache budget and evicts independently, so
//! evicting `release_files` (big) never touches `covers` (a small reserved slice). A
//! `CacheEager` cover that falls out of its namespace budget shows a placeholder
//! until the next read re-fetches it — covers are not pinned. A **Local** blob is not
//! in the cache: a user-provided Local blob is the user's file at its path (an
//! external ref); a host-provided Local blob is in coven's local store (see
//! [`local_files`]). The cache is the mechanism for *remoteness* — so
//! `CacheEager`/`CacheLazy`/pin/budget describe a blob only while it is Remote, never
//! while it is Local.
//!
//! # The engine's halves
//!
//! This module is the engine; its halves move a blob through its lifecycle:
//!
//! - [`cache`] — the device-local cache for **Remote** blobs: bytes on disk keyed
//!   by namespace, blob id, and signed content hash, with the folder a file lives
//!   in as the only retention truth
//!   (`storage/pinned/` protected, `storage/cache/` evictable). Read (whole and
//!   ranged), pin/unpin, clear, and budget eviction.
//! - [`local_files`] — coven's own copy of a **host-provided Local** blob, in the
//!   local store (`storage/local/<namespace>/<id>/<content_hash>`). Never evicted; the budget
//!   sweep never walks it. Store, read, drop.
//! - [`upload`] — the cloud-write half: drain the durable upload queue, sealing
//!   each blob under its scope and writing it to the cloud with coalesced progress,
//!   so a local-only blob becomes uploaded. The sync cycle calls the drain
//!   each round before it pushes.
//! - [`delete`] — the cloud-delete half: turn a queued deletion into a signed
//!   cloud tombstone, hold the blob for a convergence grace so a lagging peer
//!   isn't stranded, then GC the blob once the grace has passed. The sync cycle
//!   drains tombstones and runs the GC each round after it pulls.
//!
//! The types below ([`BlobRef`], [`BlobScope`], [`Provenance`],
//! [`CacheFill`], [`BlobTransitionObserver`]) are the vocabulary both halves and
//! the host speak. Which rows carry blobs is not a runtime callback but a per-table
//! declaration ([`crate::sync::session::BlobDecl`]) coven resolves into a
//! [`decl::BlobDecls`] each cycle to derive the blob set itself.
//!
//! coven also owns the two locality transitions ([`transition`]): `make_remote`
//! (Local → Remote: upload the bytes, then flip the gate) and `make_local`
//! (Remote → Local: bring each blob back to a local file, then retract). The
//! make-Remote *completion* — flipping the gate the instant the last user-provided
//! upload lands — lives in the [`upload`] drain, the one place that knows an upload
//! just succeeded.

pub mod cache;
pub mod decl;
pub mod delete;
#[doc(hidden)]
pub mod local_cleanup;
pub mod local_files;
pub mod transition;
pub mod upload;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudBlobLocation {
    pub uploader: String,
    pub generation: String,
    /// Causal order of this location assignment. Kept separate from the fixed-size
    /// object token so location arbitration does not depend on decoding a path.
    pub version: String,
}

impl CloudBlobLocation {
    pub fn generated(uploader: impl Into<String>, stamp: &str) -> Self {
        Self {
            uploader: uploader.into(),
            generation: hex::encode(Sha256::digest(stamp.as_bytes())),
            version: stamp.to_string(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        crate::store_dir::validate_path_token(&self.uploader).map_err(|error| error.to_string())?;
        crate::store_dir::validate_path_token(&self.generation)
            .map_err(|error| error.to_string())?;
        if self.generation.len() != 64
            || !self
                .generation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(
                "blob generation must be exactly 64 lowercase hexadecimal characters".to_string(),
            );
        }
        if crate::sync::hlc::Timestamp::parse(&self.version).is_none() {
            return Err("blob location version must be an HLC timestamp".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod cloud_blob_location_tests {
    use super::CloudBlobLocation;

    #[test]
    fn generated_location_uses_a_fixed_length_object_token() {
        let stamp = format!("0000000001000-0000-{}", "d".repeat(64));
        let location = CloudBlobLocation::generated("aa11", &stamp);

        assert_eq!(location.generation.len(), 64);
        location.validate().expect("generated location is valid");
    }

    #[test]
    fn location_rejects_missing_or_malformed_current_fields() {
        let missing_generation = serde_json::json!({
            "uploader": "aa11",
            "version": format!("0000000001000-0000-{}", "d".repeat(64)),
        });
        assert!(serde_json::from_value::<CloudBlobLocation>(missing_generation).is_err());

        for generation in ["a".repeat(63), "a".repeat(65), "A".repeat(64)] {
            let location: CloudBlobLocation = serde_json::from_value(serde_json::json!({
                "uploader": "aa11",
                "generation": generation,
                "version": format!("0000000001000-0000-{}", "d".repeat(64)),
            }))
            .unwrap();
            assert!(location.validate().is_err());
        }
    }
}

/// The content hash a blob-bearing row carries: the lowercase-hex SHA-256 of the
/// blob's plaintext bytes, computed at import and stored in the row's blob columns
/// alongside the declared size. The row is carried in a signed changeset (and in a
/// signed snapshot), so this hash is signed by the row's author — that is what
/// makes it authoritative: on download coven hashes the decrypted plaintext and
/// requires equality with the row's hash, so the bytes are pinned by the author,
/// not by the cloud key they happened to arrive under. A host computes this over a
/// blob's plaintext at import and writes it into the row's declared hash column,
/// the same way it writes the plaintext length into the size column.
pub fn content_hash(plaintext: &[u8]) -> String {
    hex::encode(Sha256::digest(plaintext))
}

pub(crate) async fn content_hash_file(path: &std::path::Path) -> Result<String, String> {
    let mut reader = crate::local_blob::open_reader(path).await?;
    let mut hasher = ContentHasher::new();
    loop {
        let chunk = reader.next_chunk(64 * 1024).await?;
        if chunk.is_empty() {
            return Ok(hasher.finish());
        }
        hasher.update(&chunk);
    }
}

/// An incremental SHA-256 over a blob's plaintext, so the streaming download path
/// verifies a large blob's content hash without ever holding the whole plaintext
/// in memory — feed each decrypted chunk to [`update`](Self::update) as it is read,
/// then [`verify`](Self::verify) against the row's hash before the bytes are
/// committed to the cache. The hex-encoded digest matches [`content_hash`] over the
/// same bytes.
pub struct ContentHasher(Sha256);

impl ContentHasher {
    pub fn new() -> Self {
        ContentHasher(Sha256::new())
    }

    /// Fold the next plaintext chunk into the running digest.
    pub fn update(&mut self, chunk: &[u8]) {
        self.0.update(chunk);
    }

    /// The lowercase-hex digest of everything fed so far.
    pub fn finish(self) -> String {
        hex::encode(self.0.finalize())
    }
}

impl Default for ContentHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// How many blob transfers coven runs at once in each of its two transfer loops:
/// the upload drain ([`upload::drain_uploads`]) and the pin/download loop
/// ([`cache::pin`]). An open-time blob-engine tunable the host sets on the builder,
/// carried on [`Database`](crate::database::Database) alongside the other open-time
/// blob config and read back by each loop, which holds `&Database`.
///
/// Each bound is a [`NonZeroUsize`], so a zero — which would leave a loop admitting
/// nothing and never completing — is unrepresentable rather than clamped or rejected
/// at open. `serial()` (both `1`) is the default and reproduces the fully-serial
/// loops: one transfer at a time, in queue order.
///
/// [`NonZeroUsize`]: std::num::NonZeroUsize
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferLimits {
    /// Maximum concurrent blob uploads in one upload-drain pass.
    pub uploads: std::num::NonZeroUsize,
    /// Maximum concurrent blob downloads (fetches) in one pin call.
    pub downloads: std::num::NonZeroUsize,
}

impl TransferLimits {
    /// One at a time in each loop — the fully-serial default.
    pub fn serial() -> Self {
        Self {
            uploads: std::num::NonZeroUsize::MIN,
            downloads: std::num::NonZeroUsize::MIN,
        }
    }
}

// The cache's own tests: real `Database` + `MockSyncStorage` over a temp store
// dir, asserting hits/misses, the pinned/cache folder split, and pin/unpin/clear.
// Native-only because they drive a real temp directory on the filesystem; the
// Browser cache storage assembly lives in `coven-wasm`. See [`cache`].
#[cfg(all(test, not(target_arch = "wasm32")))]
mod cache_tests;
// The upload drain's tests: real `Database` (the `cloud_outbox` queue) driven
// against `InMemoryCloudHome`/`FailingCloudHome`, asserting record-and-continue,
// per-entry backoff, scope-resolved sealing, and the observer callbacks. See
// [`upload`].
#[cfg(test)]
mod upload_tests;
// The coven-owned make-Remote / make-Local transition tests: multi-device
// make_remote + make_local through the real cycle, cancel both directions, the
// drain's completion flip + cancel-in-gap, crash-idempotency at each commit
// boundary, and a round-trip. Uses a `watch` cancel signal + `run_single_sync_cycle`,
// both native-only. See [`transition`].
#[cfg(all(test, not(target_arch = "wasm32")))]
mod transition_tests;
// The local-files store's tests: store/read round-trip, a host-provided Local blob
// surviving a budget sweep (the sweep never walks `local/`), and drop. Native-only
// because they drive a real temp directory. See [`local_files`].
#[cfg(all(test, not(target_arch = "wasm32")))]
mod local_files_tests;
// The delete half's tests: tombstone signing, the drain that writes tombstones,
// the graced GC that reclaims blobs, immutable-generation races, and the shared
// `cloud_outbox` row shape. Driven against `InMemoryCloudHome` and
// `MockSyncStorage`. See [`delete`].
#[cfg(test)]
mod delete_tests;

/// Which key encrypts a blob, as a host names it on a [`BlobRef`].
///
/// The host names *what* a blob is scoped to — the whole store or a derived
/// per-scope key — never the raw key bytes. Storage and encryption consume this
/// same type; there is no key material in it to leak.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobScope {
    /// The store master key — every member reads it.
    Master,
    /// A per-scope key derived from the master key (e.g. one key per item).
    Derived(String),
}

impl BlobScope {
    /// Serialize for the `cloud_outbox.scope` column. The audio outbox persists
    /// the scope at enqueue and resolves it to a key at drain, so the string
    /// must round-trip every variant. The variant tag is split from the payload
    /// at the first `:`; the payload (a derived scope name) is stored verbatim,
    /// so it may itself contain `:`.
    pub fn to_outbox_str(&self) -> String {
        match self {
            BlobScope::Master => "master".to_string(),
            BlobScope::Derived(s) => format!("derived:{s}"),
        }
    }

    /// Parse a `cloud_outbox.scope` value written by [`Self::to_outbox_str`].
    /// Returns `None` on an unknown tag (a corrupt row), which the drain surfaces
    /// rather than silently defaulting to the master key.
    pub fn from_outbox_str(s: &str) -> Option<Self> {
        match s.split_once(':') {
            None if s == "master" => Some(BlobScope::Master),
            Some(("derived", rest)) => Some(BlobScope::Derived(rest.to_string())),
            _ => None,
        }
    }
}

/// A blob's **Local story**: where its bytes live while the blob is Local, and
/// whether bringing it back from Remote needs a destination path. Orthogonal to
/// [`CacheFill`] (the Remote story) — a blob declares both.
///
/// The cache never enters into this: a Local blob is not a cache copy. Provenance
/// decides which of the two Local homes holds it, and what `make_local` does to
/// restore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provenance {
    /// The user's own file at a path; coven references it but does not own it
    /// (tracked as an external ref — see `local_blob_refs`). `make_local` writes
    /// the bytes back to a user file, so it **needs a destination path**.
    UserProvided,
    /// The host hands coven the data; coven keeps its own copy in the local store
    /// (`storage/local/<namespace>/<id>/<content_hash>`, see [`local_files`]). `make_local`
    /// restores it to the local store, so it needs **no path**.
    HostProvided,
}

/// A blob's **Remote story**: how a device gets the bytes once the blob is Remote.
/// A cache-mechanism setting — it describes a blob only while Remote — that applies
/// to ANY blob regardless of [`Provenance`]. Orthogonal to provenance; a blob
/// declares both.
///
/// Both classes are declared per blob and are global (every device reads the same
/// class from the blob's [`BlobRef`]); the difference is what a device does with
/// the blob on pull. The distinction has to be a declared property and not a
/// per-device choice: device B, deciding during its own pull whether to fetch a
/// blob, can only read the blob's declared class — it cannot see what device A
/// chose locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheFill {
    /// Fetched into the cache on pull, right away, on every device — part of
    /// "having the store" (e.g. cover art, so the grid renders from local bytes
    /// without a fetch). The cache copy is evictable + re-fetchable, not pinned.
    CacheEager,
    /// Not fetched on pull: a pulling device skips it and fetches it into the cache
    /// on first read — e.g. audio, which is big and streams on demand.
    CacheLazy,
}

/// A blob a row references: its cloud identity, encryption scope, and the two
/// declared properties ([`provenance`](BlobRef::provenance) +
/// [`fill`](BlobRef::fill)). coven derives it from the row's declared columns
/// ([`crate::sync::session::BlobDecl`]) via [`decl::BlobDecls`]. Where its bytes
/// live depends on its locality and provenance: a user-provided Local blob is the
/// user's file at its path; a host-provided Local blob is in coven's local store
/// (`storage/local/<namespace>/<id>/<content_hash>`); a Remote blob's device-local copy is a cache
/// copy (`storage/pinned/<namespace>/<id>/<content_hash>` /
/// `storage/cache/<namespace>/<id>/<content_hash>`, built from the validated
/// namespace, id, and signed content hash — see [`cache`]).
#[derive(Debug, Clone)]
pub struct BlobRef {
    /// Cloud namespace, e.g. `"images"`. It is the first segment of either current
    /// generated cloud-key layout.
    pub namespace: String,
    /// Blob id (typically the id of the blob-bearing row).
    pub id: String,
    /// Encryption scope for this blob.
    pub scope: BlobScope,
    /// The consumer's readable cloud-relative path for this blob, e.g.
    /// `"Artist - Album/cover.jpg"`. Used as the readable suffix when the home's
    /// [`crate::sync::cloud_storage::BlobPathScheme`] is `Plain`;
    /// ignored when `Hashed`. `None` is only valid for a `Hashed` home — a `Plain`
    /// home with no `cloud_path` is a surfaced error, never a silent fallback.
    pub cloud_path: Option<String>,
    /// The blob's **Local story**: where its bytes live while Local, and whether
    /// `make_local` needs a destination path. See [`Provenance`].
    pub provenance: Provenance,
    /// The blob's **Remote story**: whether a pulling device fetches it into the
    /// cache right away ([`CacheFill::CacheEager`]) or on first read
    /// ([`CacheFill::CacheLazy`]). See [`CacheFill`].
    pub fill: CacheFill,
}

/// One live blob-bearing row's exact content identity, read as a single database
/// snapshot. The logical [`BlobRef`] and its required signed plaintext size and
/// content hash travel together so filesystem and cloud work cannot mix metadata
/// from two same-id row versions.
#[derive(Debug, Clone)]
pub(crate) struct LiveBlobContent {
    pub blob: BlobRef,
    pub plaintext_size: u64,
    pub content_hash: String,
}

/// Notified about coven's blob transitions, for host-specific bookkeeping and UI:
/// per-blob upload progress while a make_remote uploads, per-blob materialize
/// progress while a make_local copies files back, and a completion hook per
/// direction the host turns into its own UI event.
///
/// The host no longer drives the transition — coven owns flipping the gate and
/// deciding when a cycle publishes — so this observer only *reports*. The upload
/// callbacks fire as the drain works: `on_blob_upload_started` before each
/// attempt, `on_blob_upload_progress` zero or more times as encrypted bytes reach
/// the cloud (backends that can't report sub-file progress call it once at the end
/// with `bytes_done == bytes_total`), `on_blob_uploaded` on success (notification
/// only — coven, not the host, flips the gate and breaks the drain to publish),
/// and `on_blob_upload_failed` when an attempt fails and its entry stays queued.
///
/// `on_root_made_remote` / `on_root_made_local` fire whenever coven *completes* a
/// transition — including one resumed after a restart — so the host's own
/// row-updated event survives a restart rather than being lost with an in-memory
/// flag. `on_blob_materialize_progress` moves a make_local's per-file progress bar.
///
/// `should_skip_uploads` lets the host pause the upload pipeline without touching
/// the queue: the sync cycle consults it before draining so a paused queue still
/// accepts new entries but doesn't drain ([`upload::drain_uploads`] checks once at
/// the top of each entry; in-flight uploads complete normally).
///
/// `Send + Sync` with `Send` method futures on native; `?Send` on wasm. See
/// [`crate::MaybeThreadSafe`] for why the bound is cfg'd — the browser drives every
/// future on one thread.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait BlobTransitionObserver: crate::MaybeThreadSafe {
    /// An upload attempt for this blob is starting now.
    async fn on_blob_upload_started(&self, blob_id: &str);

    /// `bytes_done` of `bytes_total` encrypted bytes have reached the cloud for
    /// this in-flight blob. `bytes_done` is cumulative and monotonic within one
    /// upload attempt. The default is a no-op so observers that don't surface
    /// sub-file progress don't need a stub.
    async fn on_blob_upload_progress(&self, blob_id: &str, bytes_done: u64, bytes_total: u64) {
        let _ = (blob_id, bytes_done, bytes_total);
    }

    /// The blob was uploaded to the cloud successfully — notification only. coven
    /// owns flipping the gate and breaking the drain to publish a completed
    /// make_remote.
    async fn on_blob_uploaded(&self, blob_id: &str);

    /// An upload attempt failed; the entry remains queued for retry.
    async fn on_blob_upload_failed(&self, blob_id: &str, error: &str);

    /// If true, the sync cycle skips the upload drain this round and
    /// [`upload::drain_uploads`] short-circuits before pulling the next queued
    /// entry. The default is `false` so existing implementations don't need a stub.
    fn should_skip_uploads(&self) -> bool {
        false
    }

    /// coven completed a make_remote of `(root_table, root_id)`: every blob is
    /// uploaded and the gate is flipped true (the subtree publishes this cycle).
    /// Fires for a restart-resumed completion too, so the host's row-updated event
    /// is not lost to a crash. The default is a no-op.
    async fn on_root_made_remote(&self, root_table: &str, root_id: &str) {
        let _ = (root_table, root_id);
    }

    /// coven completed a make_local of `(root_table, root_id)`: every blob is back
    /// to a local file (a user file for user-provided, the local store for
    /// host-provided), the gate is flipped false (the subtree retracts from peers),
    /// and the cloud blobs are queued for tombstoning. The default is a no-op.
    async fn on_root_made_local(&self, root_table: &str, root_id: &str) {
        let _ = (root_table, root_id);
    }

    /// `done` of `total` of a make_local's blobs have been materialized back to a
    /// local file, so the host can move a per-file progress bar. The default is a
    /// no-op.
    async fn on_blob_materialize_progress(
        &self,
        root_table: &str,
        root_id: &str,
        blob_id: &str,
        done: u64,
        total: u64,
    ) {
        let _ = (root_table, root_id, blob_id, done, total);
    }
}
