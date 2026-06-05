# Blobs

A synced row is small: a few columns of text and numbers in a changeset. A photo
attached to a todo is not. coven syncs those large files separately from the
changesets that reference them. The host decides which rows carry a file and
where the plaintext lives on disk; coven owns the encryption, the cloud layout,
and the retry.

The examples use a todos app where a todo can carry photo attachments. A
`todo_attachments` row points at one photo file. When that row syncs, the photo
has to reach every other device too.

## The blob plan

The host implements
[`BlobPlan`](rustdoc:trait:coven::blob::BlobPlan), which has two methods and is
the only place coven learns anything about the host's schema:

```rust
pub trait BlobPlan: Send + Sync {
    fn blobs_to_push(&self, changes: &[RowChange]) -> Vec<BlobRef>;
    fn blobs_to_pull(&self, changes: &[RowChange]) -> Vec<BlobRef>;
}
```

Both take a slice of [`RowChange`](rustdoc:struct:coven::changeset::RowChange):
the rows in one changeset. `blobs_to_push` runs over an outgoing changeset before
it is pushed; `blobs_to_pull` runs over an incoming one after it is applied. The
implementation walks the changes, picks out the rows that carry a file (an
insert or update to `todo_attachments`, say), and returns a
[`BlobRef`](rustdoc:struct:coven::blob::BlobRef) for each:

```rust
pub struct BlobRef {
    pub namespace: String,   // cloud namespace, e.g. "attachments"
    pub id: String,          // blob id, typically the attachment row's id
    pub local_path: PathBuf, // source on push, destination on pull
    pub scope: BlobScope,    // Master | Derived(scope_id)
}
```

The same row appearing in two changesets is two calls and should return a fresh
`BlobRef` each time; coven does not cache across calls. `local_path` is plaintext
on the local disk: coven reads it on push and writes it on pull, and never stores
the plaintext file in the cloud or encrypts the on-disk copy.

## Encryption scope

[`BlobScope`](rustdoc:enum:coven::blob::BlobScope) selects the key the blob is
encrypted under:

- `BlobScope::Master` encrypts with the library master key. Every member holds
  that key, so every member can decrypt the blob.
- `BlobScope::Derived(scope_id)` encrypts with a key derived from the master via
  [`derive_scoped`](rustdoc:method:coven::encryption::EncryptionService::derive_scoped),
  one distinct key per `scope_id`. A blob scoped to `Derived("todo-42")` is
  encrypted under a different key from one scoped to `Derived("todo-99")`.

The derivation is deterministic: the same `scope_id` always yields the same key,
on push and on pull alike. That is what lets a puller decrypt: it passes the same
scope back through `blobs_to_pull`, coven re-derives the key, and the bytes
decrypt. The corollary is that `scope_id` must be stable. If it is a row id that
later changes, the re-derived key will not match and the stored blob will not
decrypt.

Use `Master` when every member should read the blob, which is the common case.
`Derived` exists for a finer split: scoping a blob to a key only some members
hold is the building block for revoking access to a subset of blobs without
rotating the whole library key. The split happens at encryption time, so the
choice is made now even though the revocation path that consumes it is future
work.

## How a blob moves out

A blob reaches the cloud one of two ways. A `BlobRef` returned from
`blobs_to_push` is uploaded by the cycle itself, inside `sync`, before the
envelope: coven reads `local_path`, encrypts under the ref's `scope` (master or
derived), and writes to `{namespace}/{ab}/{cd}/{id}`. That path is synchronous
and reports no progress. The `cloud_outbox` queue described below is the
separate, durable path, with retry and progress, and it does not read `scope`:
it encrypts under the master key only.

For the queue, the host writes the plaintext file to `local_path`, then enqueues
an upload in the `cloud_outbox` table. That table is one of coven's bookkeeping
tables, created by [`MIGRATION_SQL`](rustdoc:const:coven::db::MIGRATION_SQL). An entry is
an [`OutboxEntry`](rustdoc:struct:coven::db::OutboxEntry) with
[`OutboxOperation::Upload`](rustdoc:variant:coven::db::OutboxOperation::Upload): it carries
the `file_id`, the `cloud_key` (the blob's cloud path), and an optional
`source_path` that overrides where the plaintext is read from when the file lives
outside the library directory.

The queue is the durable record of upload intent. Nothing uploads at enqueue
time; the next sync cycle drains the queue in `process_uploads`. For each entry
it reads the local file, encrypts it under the library master key, writes the
encrypted bytes to the cloud at the entry's `cloud_key`, and removes the entry on
success.

Uploads run before the changeset push, and the cycle will not publish a changeset
while uploads are still pending. A peer therefore never pulls a changeset that
points at a blob the cloud does not yet hold.

The drain does not stop on a failure. A failed entry stays queued with its
`attempt_count` bumped and `last_error` and `last_attempt_at` recorded, and the
loop moves to the next entry, so one file the cloud keeps rejecting does not
block the rest of the queue (no head-of-line blocking). Before retrying an entry
the loop checks a per-entry backoff window:

```
30s · 2^(attempt_count - 1), capped at 1 hour
```

A freshly queued entry (`attempt_count == 0`) is eligible immediately. After the
first failure the wait is 30s, then 60s, 120s, and so on up to a one-hour ceiling
at which a persistently failing entry retries hourly rather than every cycle. The
base equals the sync loop's interval, so the first retry rides the next natural
cycle.

## The pull side

The pull side has no inbox table. It is inline:
[`pull_changes`](rustdoc:fn:coven::sync::pull::pull_changes) applies a changeset,
then immediately asks `blobs_to_pull` for the blobs that changeset references and
downloads each one to its `local_path`.

A download is skipped when `local_path` already exists, which makes the whole
step idempotent and safe to re-run. coven creates the parent directories, then
decrypts under the blob's scope and writes the plaintext file.

The cursor is what makes this durable without a queue. A changeset's cursor only
advances once all of that changeset's blobs have arrived. If any download fails,
coven sets `asset_downloads_failed` on the
[`PullResult`](rustdoc:struct:coven::sync::pull::PullResult) and holds the cursor
where it was, so the next cycle re-pulls that changeset and retries the blob. The
changeset plus the held cursor are the record of what still needs fetching; a
separate inbox would duplicate that.

## Cloud layout

A blob is stored at:

```
{namespace}/{ab}/{cd}/{id}
```

`ab` and `cd` are the first two pairs of hex characters of the dash-stripped
`id`, built by
[`LibraryDir::hashed_path`](rustdoc:method:coven::library_dir::LibraryDir::hashed_path).
The two levels of fan-out keep a library with many blobs from landing under one
flat prefix that the storage layer would have to list in a single call. The
cloud provider sees this path and the encrypted bytes, never the plaintext file
or its contents.

## Observing uploads

The host can pass a
[`BlobUploadObserver`](rustdoc:trait:coven::blob::BlobUploadObserver) to watch
each upload. The whole observer is optional. Three methods are required
(`on_blob_upload_started`, `on_blob_uploaded`, `on_blob_upload_failed`); the
other two default to a no-op:

```rust
#[async_trait::async_trait]
pub trait BlobUploadObserver: Send + Sync {
    async fn on_blob_upload_started(&self, file_id: &str);
    async fn on_blob_upload_progress(&self, file_id: &str, bytes_done: u64, bytes_total: u64) {}
    async fn on_blob_uploaded(&self, file_id: &str);
    async fn on_blob_upload_failed(&self, file_id: &str, error: &str);
    fn should_skip_uploads(&self) -> bool { false }
}
```

The callbacks track attempts, not blobs. `on_blob_upload_started` fires once
before each attempt, so a blob that fails twice and then succeeds fires it three
times. `on_blob_uploaded` fires once, when the entry leaves the queue.
`on_blob_upload_failed` fires on each failed attempt and carries the error
string; the entry stays queued for retry. A todos app wires these into the
attachment's row: `started` shows "uploading", `uploaded` shows "synced",
`failed` shows "will retry" with the error in a tooltip.

`on_blob_upload_progress` reports bytes reaching the cloud between start and the
terminal callback, so a per-file bar moves instead of jumping from 0 to 100% at
the end. `bytes_done` is cumulative and monotonic within one attempt. The counts
are of the encrypted payload, which is marginally larger than the plaintext file.
coven coalesces the underlying per-chunk reports to a 300ms tick so the host is
not rebuilt on every chunk, and emits one final progress call at
`bytes_done == bytes_total` on success. On failure it leaves the last observed
count. A backend that cannot report sub-file progress calls it once at the end.

`should_skip_uploads` is the pause switch. `process_uploads` checks it before
pulling each entry: while it returns true the queue still accepts new entries but
does not drain, and an upload already in flight finishes normally. Flip it back
and the next cycle picks the queue up where it left off.
