//! The blob engine: coven's single owner of a blob's whole durability lifecycle.
//!
//! coven syncs opaque encrypted blobs referenced by DB rows. By default it owns
//! the cloud layout (the content-addressed `{namespace}/{ab}/{cd}/{id}`) and
//! encryption; the host decides which rows carry blobs, where their plaintext
//! lives locally, and how each is scoped for encryption. A home configured for
//! the unobfuscated blob-path scheme instead stores each blob at the consumer's
//! readable [`BlobRef::cloud_path`] so the bucket is browsable.
//!
//! This module is the engine; its halves move a blob through its lifecycle
//! (stage → upload → download → pin/unpin → evict → delete):
//!
//! - [`cache`] — the device-local half: bytes on disk keyed by blob id, with the
//!   folder a file lives in as the only retention truth (`storage/pinned/`
//!   protected, `storage/cache/` evictable). Stage, read (whole and ranged),
//!   pin/unpin, clear, and budget eviction.
//! - [`upload`] — the cloud-write half: drain the durable upload queue, sealing
//!   each blob under its scope and writing it to the cloud with coalesced progress,
//!   so a staged local-only blob becomes uploaded. The sync cycle calls the drain
//!   each round before it pushes.
//! - [`delete`] — the cloud-delete half: turn a queued deletion into a signed
//!   cloud tombstone, hold the blob for a convergence grace so a lagging peer
//!   isn't stranded, then GC the blob once the grace has passed. The sync cycle
//!   drains tombstones and runs the GC each round after it pulls.
//!
//! The types below ([`BlobRef`], [`BlobScope`]/[`ResolvedScope`], [`BlobSync`],
//! [`BlobUploadObserver`]/[`DrainControl`]) are the vocabulary both halves and the
//! host speak. Which rows carry blobs is not a runtime callback but a per-table
//! declaration ([`crate::sync::session::BlobDecl`]) coven resolves into a
//! [`decl::BlobDecls`] each cycle to derive the blob set itself.

pub mod cache;
pub mod decl;
pub mod delete;
pub mod upload;

// The cache's own tests: real `Database` + `MockSyncStorage` over a temp library
// dir, asserting hits/misses, the pinned/cache folder split, and pin/unpin/clear.
// Native-only because they drive a real temp directory on the filesystem; the
// cache's wasm/OPFS path is exercised by `wasm_blob_opfs_test`. See [`cache`].
#[cfg(all(test, not(target_arch = "wasm32")))]
mod cache_tests;
// The upload drain's tests: real `Database` (the `cloud_outbox` queue) driven
// against `InMemoryCloudHome`/`FailingCloudHome`, asserting record-and-continue,
// per-entry backoff, scope-resolved sealing, and the observer callbacks. See
// [`upload`].
#[cfg(test)]
mod upload_tests;
// The delete half's tests: tombstone signing, the drain that writes tombstones,
// the graced GC that reclaims blobs, upload-cancels-delete at both layers, and the
// shared `cloud_outbox` row shape. Driven against `InMemoryCloudHome` and
// `MockSyncStorage`. See [`delete`].
#[cfg(test)]
mod delete_tests;

#[cfg(feature = "share-proxy")]
use serde::{Deserialize, Serialize};

/// A blob's logical cloud reference: just its `(namespace, id)`, with none of the
/// encryption-scope detail a [`BlobRef`] carries. This is the shape that may cross
/// into a cloud manifest — a share authorizes blobs by their logical id, and coven
/// hashes each to its `{namespace}/{ab}/{cd}/{id}` cloud key internally. A
/// `BlobRef`'s `scope` must never reach the cloud, so the manifest references this
/// `(namespace, id)`-only type instead.
#[cfg(feature = "share-proxy")]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobId {
    /// Cloud namespace, e.g. `"audio"`.
    pub namespace: String,
    /// Blob id (typically the id of the blob-bearing row).
    pub id: String,
}

/// Which key encrypts a blob, as a host names it on a [`BlobRef`].
///
/// The host names *what* a blob is scoped to — the whole library, a derived
/// per-scope key, or a coven-managed item — never the raw key bytes. coven
/// resolves the scope to a [`ResolvedScope`] (looking up the item's key in the
/// `item_keys` table for [`BlobScope::Item`]) before it touches storage, so a
/// host can't hand coven raw bytes and bypass `item_keys`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobScope {
    /// The library master key — every member reads it.
    Master,
    /// A per-scope key derived from the master key (e.g. one key per item).
    Derived(String),
    /// A coven-managed item key, named by `item_id`. coven mints the random
    /// per-item key with [`crate::Database::mint_item_key`], syncs it on the
    /// `item_keys` table, and resolves this scope to that key. Independent of
    /// the master, so the item can be rotated or shared to a non-member without
    /// exposing the whole library.
    Item(String),
}

/// The key a blob is actually encrypted under, resolved from a [`BlobScope`].
///
/// coven resolves [`BlobScope`] to this before reading or writing storage:
/// [`BlobScope::Item`] becomes [`ResolvedScope::Key`] by looking up the
/// `item_keys` row. [`SyncStorage`](crate::sync::storage::SyncStorage) blob
/// methods take this internal form — the public `Item` scope never reaches
/// storage or encryption.
///
/// Internal to coven, though `pub` by necessity: it appears in the `SyncStorage`
/// trait, which a tree of `pub` sync entry points (the cycle, pull, snapshot, and
/// membership functions a host reaches only through `SyncManager`) references in
/// turn, so narrowing it would force that whole tree private. A host never names
/// this type — it tags a blob with the public [`BlobScope`] and coven resolves it
/// here — so the raw-key `Key([u8; 32])` variant stays a coven concern in
/// practice even while the type is reachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedScope {
    /// The library master key.
    Master,
    /// A per-scope key derived from the master key.
    Derived(String),
    /// An explicit 32-byte key (a resolved item key).
    Key([u8; 32]),
}

impl BlobScope {
    /// Serialize for the `cloud_outbox.scope` column. The audio outbox persists
    /// the public scope at enqueue and resolves it to a key at drain, so the
    /// string must round-trip every variant. The variant tag is split from the
    /// payload at the first `:`; the payload (a derived scope name or an item id)
    /// is stored verbatim, so it may itself contain `:`.
    pub fn to_outbox_str(&self) -> String {
        match self {
            BlobScope::Master => "master".to_string(),
            BlobScope::Derived(s) => format!("derived:{s}"),
            BlobScope::Item(id) => format!("item:{id}"),
        }
    }

    /// Parse a `cloud_outbox.scope` value written by [`Self::to_outbox_str`].
    /// Returns `None` on an unknown tag (a corrupt row), which the drain surfaces
    /// rather than silently defaulting to the master key.
    pub fn from_outbox_str(s: &str) -> Option<Self> {
        match s.split_once(':') {
            None if s == "master" => Some(BlobScope::Master),
            Some(("derived", rest)) => Some(BlobScope::Derived(rest.to_string())),
            Some(("item", rest)) => Some(BlobScope::Item(rest.to_string())),
            _ => None,
        }
    }
}

/// How a blob is retained across devices — the one blob knob the host turns.
///
/// Both classes are declared per blob and are global (every device reads the same
/// class from the blob's [`BlobRef`]); the difference is what a device does with
/// the blob on pull. The distinction has to be a declared property and not a
/// per-device choice: device B, deciding during its own pull whether to fetch a
/// blob, can only read the blob's declared class — it cannot see what device A
/// chose locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobSync {
    /// Synced to every device: downloaded on pull and kept local. Part of "having
    /// the library" — e.g. cover art.
    Mirrored,
    /// Uploaded on push but not downloaded on pull: a pulling device skips it and
    /// fetches it on first read — e.g. audio, which streams on demand.
    OnDemand,
}

/// A blob a row references: its cloud identity and encryption scope. coven derives
/// it from the row's declared columns ([`crate::sync::session::BlobDecl`]) via
/// [`decl::BlobDecls`]; the bytes always live in coven's own cache
/// (`storage/pinned/<id>` / `storage/cache/<id>`, built from the validated id —
/// see [`cache`]), never at a host path.
#[derive(Debug, Clone)]
pub struct BlobRef {
    /// Cloud namespace, e.g. `"images"`. Becomes `{namespace}/{ab}/{cd}/{id}`
    /// under the hashed scheme, or `{namespace}/{cloud_path}` under the plain one.
    pub namespace: String,
    /// Blob id (typically the id of the blob-bearing row).
    pub id: String,
    /// Encryption scope for this blob.
    pub scope: BlobScope,
    /// The consumer's readable cloud-relative path for this blob, e.g.
    /// `"Artist - Album/cover.jpg"`. Used as the object key under `namespace` when
    /// the home's [`crate::sync::cloud_storage::BlobPathScheme`] is `Plain`;
    /// ignored when `Hashed`. `None` is only valid for a `Hashed` home — a `Plain`
    /// home with no `cloud_path` is a surfaced error, never a silent fallback.
    pub cloud_path: Option<String>,
    /// The blob's retention class. Decides whether a pulling device downloads it
    /// ([`BlobSync::Mirrored`]) or skips it for later on-demand fetch
    /// ([`BlobSync::OnDemand`]).
    pub sync: BlobSync,
}

/// What the outbox drain should do after a successful upload, returned by
/// [`BlobUploadObserver::on_blob_uploaded`].
///
/// coven drains the outbox in one pass per cycle, then the cycle captures and
/// pushes the changeset. For a host that gates rows on blob upload (keeping a
/// row's gate column off until its blobs land, then flipping it in
/// `on_blob_uploaded`), draining the whole queue before publishing means every
/// row waits on the slowest upload in the batch. Returning [`Self::Publish`]
/// lets the host break the drain the moment an upload makes new rows shareable,
/// so the cycle publishes them and the loop resumes draining the rest next
/// cycle — turning all-or-nothing propagation into per-unit propagation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainControl {
    /// Keep draining the next queued upload before publishing.
    Continue,
    /// Stop draining now so this sync cycle publishes before continuing. The
    /// host returns this when the upload just made new rows shareable (e.g. it
    /// flipped a gate column on). Entries still queued drain on the next cycle,
    /// which the loop runs promptly rather than after the idle interval.
    Publish,
}

/// Notified about the lifecycle of a blob upload, for host-specific
/// bookkeeping and UI (e.g. transitioning a record from "uploading" to
/// "cloud-only", or surfacing a failure). `on_blob_upload_started` fires before
/// each attempt — the host tracks the transient in-flight set in memory;
/// `on_blob_uploaded` on success; `on_blob_upload_failed` when an attempt fails
/// and the entry is left queued for retry.
///
/// `on_blob_upload_progress` reports how many bytes of the blob have reached
/// the cloud so far, between start and the terminal callback, so the host can
/// move a per-file progress bar mid-upload instead of jumping from 0 to 100% at
/// completion. The byte counts are of the encrypted payload (what the cloud
/// backend actually transfers), which is marginally larger than the plaintext
/// file. It fires zero or more times per upload; backends that can't report
/// sub-file progress call it once at the end with `bytes_done == bytes_total`.
///
/// `on_blob_uploaded` returns a [`DrainControl`] so a host that gates rows on
/// upload can publish each unit as its blobs land instead of after the whole
/// queue drains.
///
/// `should_skip_uploads` lets the host pause the upload pipeline without
/// touching the queue contents. The sync cycle consults it before draining the
/// upload queue so a paused queue still accepts new entries but doesn't drain;
/// in-flight uploads complete normally ([`upload::drain_uploads`] checks once at
/// the top of each entry).
/// `Send + Sync` with `Send` method futures on native; `?Send` on wasm. See
/// [`crate::MaybeThreadSafe`] for why the bound is cfg'd — the browser drives
/// every upload future on one thread.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait BlobUploadObserver: crate::MaybeThreadSafe {
    /// An upload attempt for this blob is starting now.
    async fn on_blob_upload_started(&self, file_id: &str);

    /// `bytes_done` of `bytes_total` encrypted bytes have reached the cloud for
    /// this in-flight blob. `bytes_done` is cumulative and monotonic within one
    /// upload attempt. The default is a no-op so observers that don't surface
    /// sub-file progress don't need a stub.
    async fn on_blob_upload_progress(&self, file_id: &str, bytes_done: u64, bytes_total: u64) {
        let _ = (file_id, bytes_done, bytes_total);
    }

    /// The blob was uploaded to the cloud successfully. Returns whether the
    /// drain should keep going or stop so the cycle can publish now (see
    /// [`DrainControl`]). A host that doesn't gate rows on upload returns
    /// [`DrainControl::Continue`].
    async fn on_blob_uploaded(&self, file_id: &str) -> DrainControl;

    /// An upload attempt failed; the entry remains queued for retry.
    async fn on_blob_upload_failed(&self, file_id: &str, error: &str);

    /// If true, the sync cycle skips the upload drain this round and
    /// [`upload::drain_uploads`] short-circuits before pulling the next queued
    /// entry. The default is `false` so existing implementations don't need a stub.
    fn should_skip_uploads(&self) -> bool {
        false
    }
}
