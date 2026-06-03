//! Blob plumbing for sync.
//!
//! coven syncs opaque encrypted blobs referenced by DB rows. It owns the cloud
//! layout (`{namespace}/{ab}/{cd}/{id}`) and encryption; the host decides which
//! rows carry blobs, where their plaintext lives locally, and how each is
//! scoped for encryption.

use std::path::PathBuf;

use crate::changeset::RowChange;

/// Which key encrypts a blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobScope {
    /// The library master key.
    Master,
    /// A per-scope key derived from the master key (e.g. one key per item).
    Derived(String),
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
/// `should_skip_uploads` lets the host pause the upload pipeline without
/// touching the queue contents. The sync cycle consults it before processing
/// the outbox so a paused queue still accepts new entries but doesn't drain;
/// in-flight uploads complete normally (process_uploads checks once at the top
/// of each entry).
#[async_trait::async_trait]
pub trait BlobUploadObserver: Send + Sync {
    /// An upload attempt for this blob is starting now.
    async fn on_blob_upload_started(&self, file_id: &str);

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
