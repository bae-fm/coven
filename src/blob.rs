//! Blob plumbing for sync.
//!
//! coven syncs opaque encrypted blobs referenced by DB rows. It owns the cloud
//! layout (`{namespace}/{ab}/{cd}/{id}`) and encryption; the host decides which
//! rows carry blobs, where their plaintext lives locally, and how each is
//! scoped for encryption.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::changeset::RowChange;

/// A blob's logical cloud reference: just its `(namespace, id)`, with none of the
/// local-disk or encryption-scope detail a [`BlobRef`] carries. This is the
/// shape that may cross into a cloud manifest — a share authorizes blobs by their
/// logical id, and coven hashes each to its `{namespace}/{ab}/{cd}/{id}` cloud
/// key internally. A `BlobRef`'s `local_path`/`scope` must never reach the cloud,
/// so the manifest references this `(namespace, id)`-only type instead.
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

/// A blob referenced by a changeset: its cloud identity plus the local file.
#[derive(Debug, Clone)]
pub struct BlobRef {
    /// Cloud namespace, e.g. `"images"`. Becomes `{namespace}/{ab}/{cd}/{id}`.
    pub namespace: String,
    /// Blob id (typically the id of the blob-bearing row).
    pub id: String,
    /// Local plaintext file: the source on push, the destination on pull.
    pub local_path: PathBuf,
    /// Encryption scope for this blob.
    pub scope: BlobScope,
}

/// Maps changeset row-changes to the blobs that must move with them.
///
/// The host knows which of its tables carry blobs and how to locate the local
/// file. coven uploads referenced blobs before pushing a changeset and
/// downloads them after applying an incoming one.
pub trait BlobPlan: Send + Sync {
    /// Blobs to upload before pushing an outgoing changeset.
    fn blobs_to_push(&self, changes: &[RowChange]) -> Vec<BlobRef>;

    /// Blobs to download after applying an incoming changeset.
    fn blobs_to_pull(&self, changes: &[RowChange]) -> Vec<BlobRef>;
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
/// `should_skip_uploads` lets the host pause the upload pipeline without
/// touching the queue contents. The sync cycle consults it before processing
/// the outbox so a paused queue still accepts new entries but doesn't drain;
/// in-flight uploads complete normally (process_uploads checks once at the top
/// of each entry).
#[async_trait::async_trait]
pub trait BlobUploadObserver: Send + Sync {
    /// An upload attempt for this blob is starting now.
    async fn on_blob_upload_started(&self, file_id: &str);

    /// `bytes_done` of `bytes_total` encrypted bytes have reached the cloud for
    /// this in-flight blob. `bytes_done` is cumulative and monotonic within one
    /// upload attempt. The default is a no-op so observers that don't surface
    /// sub-file progress don't need a stub.
    async fn on_blob_upload_progress(&self, file_id: &str, bytes_done: u64, bytes_total: u64) {
        let _ = (file_id, bytes_done, bytes_total);
    }

    /// The blob was uploaded to the cloud successfully.
    async fn on_blob_uploaded(&self, file_id: &str);

    /// An upload attempt failed; the entry remains queued for retry.
    async fn on_blob_upload_failed(&self, file_id: &str, error: &str);

    /// If true, the sync cycle skips outbox upload processing this round and
    /// `process_uploads` short-circuits before pulling the next queued entry.
    /// The default is `false` so existing implementations don't need a stub.
    fn should_skip_uploads(&self) -> bool {
        false
    }
}
