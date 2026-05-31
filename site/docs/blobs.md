# Blobs

coven syncs opaque encrypted blobs referenced by synced rows. The host
decides which rows carry blobs and where the plaintext lives locally; coven
owns the cloud layout, encryption, and retry.

## Blob plan

The host supplies a [`blob::BlobPlan`](rustdoc:trait:coven::blob::BlobPlan):

```rust
pub trait BlobPlan: Send + Sync {
    fn blobs_to_push(&self, changes: &[RowChange]) -> Vec<BlobRef>;
    fn blobs_to_pull(&self, changes: &[RowChange]) -> Vec<BlobRef>;
}

pub struct BlobRef {
    pub namespace: String,   // e.g. "images", "audio"
    pub id: String,           // typically the blob-bearing row's id
    pub local_path: PathBuf,  // source on push, destination on pull
    pub scope: BlobScope,     // Master | Derived(scope_id)
}
```

For each outgoing or applied changeset, coven asks the plan which blobs
move with it. The plan reads the host's domain (which column points at
which file) without coven needing to know any of it.

## Encryption scope

- **`BlobScope::Master`** — encrypted with the library's master key; every
  member can decrypt.
- **`BlobScope::Derived(scope_id)`** — encrypted with a per-scope key
  derived from the master. Lets the host scope blobs more finely (e.g. one
  derived key per album for cover-art) so a future membership-revocation
  story can target a subset.

## Outbox

Upload intent is durable. When the host writes a blob locally and wants it
synced, it enqueues a row in the `cloud_outbox` table (one of coven's
bookkeeping tables — created by
[`db::MIGRATION_SQL`](rustdoc:const:coven::db::MIGRATION_SQL)). The next
sync cycle drains the queue (`outbox::process_uploads`), encrypts each
file, writes it to the cloud home at `{namespace}/{ab}/{cd}/{id}`, and
clears the entry.

A failed upload stays queued with an incremented `attempt_count` and a
backoff window: `30s · 2^(n-1)` capped at 1 hour. Other items keep
flowing — a persistently-failing entry doesn't block the queue.

A host can cancel a queued entry through the manager; the local file
stays, only the upload intent is dropped.

## Pull side (no inbox)

The pull side is inline, not queued.
[`pull::pull_changes`](rustdoc:fn:coven::sync::pull::pull_changes) walks
each just-applied changeset, asks the plan for the referenced blobs, and
downloads any whose `local_path` doesn't already exist — idempotent, re-
runs are safe. On failure, `PullResult::asset_downloads_failed` is set, the
cursor doesn't advance for that changeset, and the whole changeset re-pulls
next cycle. The changeset + cursor are the durable record of "what to
fetch", so a separate inbox table isn't needed.

## Observer

The host can provide a
[`blob::BlobUploadObserver`](rustdoc:trait:coven::blob::BlobUploadObserver)
to react to each upload's lifecycle:

```rust
#[async_trait::async_trait]
pub trait BlobUploadObserver: Send + Sync {
    async fn on_blob_upload_started(&self, file_id: &str);
    async fn on_blob_uploaded(&self, file_id: &str);
    async fn on_blob_upload_failed(&self, file_id: &str, error: &str);
}
```

The canonical use is wiring per-item state into the host's UI — `started`
flips a row to "uploading", `uploaded` flips it to "ready", `failed` flips
it to "failed, will retry" and surfaces the error string in a tooltip.

## Storage shape

Blobs are encrypted before upload and stored at
`{namespace}/{first_byte_pair}/{second_byte_pair}/{id}`. Two levels of hex
fan-out (`LibraryDir::hashed_path`) keep a large library from landing as a
single huge prefix the storage layer has to list. Cloud providers see
encrypted bytes and the path; never the plaintext file.
