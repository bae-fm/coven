# Blobs

A synced row is small: a few columns of text and numbers in a changeset. A photo
attached to a todo is not. coven syncs those large files separately from the
changesets that reference them. The host declares which rows carry a file and which
columns locate it; coven derives the blob set itself and owns the encryption, the
cloud layout, the upload, and the retry.

This page is the cloud lifecycle: how a blob is described, uploaded, pulled, and
deleted across devices. The one device's copy (where a pulled blob lands, how it
is kept and read offline) is the [cache](/docs/cache).

The examples use a todos app where a todo can carry photo attachments. A
`todo_attachments` row points at one photo file. When that row syncs, the photo
has to reach every other device too.

## Declaring which rows carry blobs

Blob-bearing-ness is a per-table declaration, not a runtime callback. The host
marks a synced table with
[`carries_blob`](rustdoc:method:coven::sync::session::SyncedTable::carries_blob),
passing a [`BlobDecl`](rustdoc:struct:coven::sync::session::BlobDecl) that names the
columns locating each blob plus its namespace, encryption scope, and retention
class:

```rust
SyncedTable::new("todo_attachments").carries_blob(
    BlobDecl::new("attachments", BlobSync::Mirrored)
        // .with_id_column("file_id")              // defaults to the PK ("id")
        // .with_cloud_path_column("path")         // for a browsable home
        // .with_scope(BlobScopeSpec::ItemColumn("todo_id"))
)
```

```rust
pub struct BlobDecl {
    pub id_column: String,                  // blob-id column; defaults to the PK ("id")
    pub namespace: String,                  // cloud namespace, e.g. "attachments"
    pub cloud_path_column: Option<String>,  // readable-key column for a browsable home
    pub scope: BlobScopeSpec,               // Master | Derived(name) | ItemColumn(col)
    pub sync: BlobSync,                     // Mirrored | OnDemand
}
```

coven resolves the declaration's column *names* against the live schema once per
cycle into [`BlobDecls`](rustdoc:struct:coven::blob::decl::BlobDecls), then derives
every blob set it needs itself, reading the declared columns off a row:

- over an outgoing changeset's rows — what to upload;
- over an incoming changeset's rows — what to download (and, for a deleted row,
  whose local cache to drop);
- over the whole database after a [snapshot bootstrap](/docs/bootstrap) — the
  backfill. A bootstrapped device receives the catalog rows but not the per-row
  blobs (the snapshot is a whole-database image, and the incremental pull that
  follows starts past the changesets that carried them), so coven re-derives them
  from the live rows via
  [`refs_in_db`](rustdoc:method:coven::blob::decl::BlobDecls::refs_in_db).

A row maps to the same blob whichever way it moves, so one declaration serves every
path. coven reads the declared columns off the row to build a
[`BlobRef`](rustdoc:struct:coven::blob::BlobRef), one blob's resolved reference:

```rust
pub struct BlobRef {
    pub namespace: String,          // cloud namespace, e.g. "attachments"
    pub id: String,                 // blob id, from the id column (the row's id by default)
    pub scope: BlobScope,           // Master | Derived(id) | Item(id)
    pub cloud_path: Option<String>, // readable path for a browsable home
    pub sync: BlobSync,             // Mirrored | OnDemand
}
```

A pulled blob's bytes always land in coven's own [cache](/docs/cache)
(`storage/pinned/<id>` / `storage/cache/<id>`, built from the validated id); the
host never names where a blob file lives.

`cloud_path` is consulted only by a [browsable home](#browsable-home-blob-paths);
an opaque home (the default) ignores it, so leave the `cloud_path_column` unset
unless the home is browsable.

### Retention class

[`BlobSync`](rustdoc:enum:coven::blob::BlobSync) is the one retention knob the
host turns, declared per blob and read the same way on every device:

- `Mirrored`: downloaded on pull and kept on every device, part of "having the
  library". A todo's photo, an album's cover art.
- `OnDemand`: uploaded on push but skipped on pull. A pulling device fetches it on
  first read instead of up front. Large blobs a device may never open, audio being
  the case it exists for.

The class has to be a declared property, not a per-device choice: a device
deciding during its own pull whether to fetch a blob can only read the blob's
declared class, never what another device chose locally. What a device then does
with a downloaded blob (keep it, evict it, pin it) is the [cache](/docs/cache)'s
job.

## Encryption scope

The declaration's
[`BlobScopeSpec`](rustdoc:enum:coven::sync::session::BlobScopeSpec) says where a
blob's scope comes from: `Master`, a fixed `Derived(name)`, or `ItemColumn(col)`
(the item id is the value of that column in the blob's row). coven resolves it per
row to a [`BlobScope`](rustdoc:enum:coven::blob::BlobScope), which selects the key
the blob is encrypted under. Either way the host names *what* a blob is scoped to,
never the raw key bytes:

- `Master` encrypts with the library master key. Every member holds it, so every
  member can decrypt the blob. The common case, with no key management at all.
- `Derived(scope_id)` encrypts with a key derived from the master via
  [`derive_scoped`](rustdoc:method:coven::encryption::EncryptionService::derive_scoped),
  one distinct key per `scope_id`. Deterministic: the same `scope_id` yields the
  same key on push and on pull, which is what lets a puller re-derive it and
  decrypt. The corollary is that `scope_id` must be stable; a row id that later
  changes would re-derive a different key and the stored blob would not decrypt.
- `Item(item_id)` encrypts with a coven-managed **item key**: a random per-item
  key coven mints with
  [`mint_item_key`](rustdoc:method:coven::database::Database::mint_item_key),
  keeps in the synced `item_keys` table (so it reaches every member and survives a
  snapshot), and resolves by `item_id` on push and pull. The host names the item;
  coven holds the key. Unlike `Derived`, an item key is independent of the master,
  so coven can hand it to a non-member without exposing the library. That export
  is a [share](/docs/sharing#creating-and-opening-a-share).

coven resolves the public scope to an internal key (looking up the `item_keys` row
for `Item`) before it touches storage, at one resolution point shared by all three
blob paths (inline push, pull, outbox drain). A missing `item_keys` row is a host
bug, surfaced as an error rather than silently falling back to the master key
(which no share recipient could read).

Item keys are opt-in. An app that never emits `Item` stays on `Master`/`Derived`
and the `item_keys` table stays empty.

## How a blob moves out

A blob reaches the cloud one of two ways.

**Inline with the changeset.** A `Mirrored` blob a row in the *outgoing* changeset
references is uploaded by the cycle itself, before the envelope is packed and
pushed: coven reads the plaintext from its [cache](/docs/cache) (the host stages it
there when it writes the row — see [Staging a blob](#staging-a-blob-for-upload)),
resolves the scope to a key, encrypts, and writes to the blob's cloud key. Only
`Mirrored` rides this inline path; an `OnDemand` blob is uploaded through the outbox
below. This path is synchronous and reports no progress. If the blob is not staged
in the cache, the cycle **aborts** rather than publishing a row that points at a
blob the cloud does not hold: the authoring device is the only one with the file, so
a published-but-missing blob would 404 on every puller permanently. The next cycle
retries once the blob is staged.

**Through the upload outbox.** For a blob uploaded out of band, with progress and
retry (audio is the case), the host writes the plaintext and enqueues an upload:

```rust
db.enqueue_upload(file_id, cloud_key, source_path, scope, created_at).await?;
```

`cloud_key` is the final cloud object key, persisted verbatim on the row.
`source_path` overrides where the plaintext is read from when the file lives
outside the library directory (`None` means coven's default storage path for
`file_id`). `scope` is the blob's [`BlobScope`](#encryption-scope); coven persists
it and resolves it to a key when the upload drains, long after the enqueue site is
gone. Enqueuing an upload also cancels any pending delete of the same key (latest
intent wins), so a re-upload is never tombstoned in the same cycle.

The outbox is coven's `cloud_outbox` table, created in
[`Database::open`](rustdoc:method:coven::database::Database::open). The host never
mutates it by hand: it enqueues and reads through the
[`Database`](rustdoc:struct:coven::database::Database) methods
(`enqueue_upload`/`enqueue_delete`, `get_pending_cloud_uploads`/`get_pending_cloud_deletes`,
`remove_cloud_outbox_uploads_for_key`, `reset_cloud_outbox_backoff`), or reads the
shared table in its own queries. Each row is an
[`OutboxEntry`](rustdoc:struct:coven::db::OutboxEntry) whose
[`OutboxOperation`](rustdoc:enum:coven::db::OutboxOperation) is an `Upload`,
`Delete`, or `Cancel`.

Nothing uploads at enqueue time. The next sync cycle's
[`drain_uploads`](rustdoc:fn:coven::blob::upload::drain_uploads) reads each pending
entry, reads the local file, resolves the scope to a key, encrypts, writes the
bytes to the entry's `cloud_key`, and removes the entry on success. The drain runs
before the changeset push, but the cycle does not hold the whole changeset back
while it runs.

Blob-before-row ordering is the host's job, per row, not a global push gate. A
host keeps a blob-bearing row's [gate column](/docs/local-data) off until that
row's blobs upload, then flips it on (typically in `on_blob_uploaded`). The gate
cuts the row while its column is off and re-emits the row's full subtree when it
flips on, so a peer never pulls a changeset that points at a blob the cloud does
not yet hold. Because this is per row, one slow or stuck upload holds back only its
own row, and a row whose upload fails for good never wedges the rest.

The drain does not stop on a failure. A failed entry stays queued with its
`attempt_count` bumped and `last_error`/`last_attempt_at` recorded, and the loop
moves on, so one file the cloud keeps rejecting does not block the queue. Before
retrying an entry the loop checks a per-entry backoff window:

```
30s · 2^(attempt_count - 1), capped at 1 hour
```

A freshly queued entry (`attempt_count == 0`) is eligible immediately. After the
first failure the wait is 30s, then 60s, 120s, and so on up to an hourly ceiling.
The base equals the sync loop's interval, so the first retry rides the next
natural cycle.

## The pull side

The pull has no inbox table. It is inline:
[`pull_changes`](rustdoc:fn:coven::sync::pull::pull_changes) downloads the blobs an
incoming changeset references (derived from the declarations) *before* applying it,
so a row is never applied before its blobs are durable. A downloaded blob lands in
the [cache](/docs/cache), at `storage/pinned/<id>` under the library directory,
decrypted under its scope. A download is skipped when the file is already present,
which makes the step idempotent.

Only `Mirrored` blobs download here. An `OnDemand` blob is skipped on pull and
fetched on its first [`read_blob`](/docs/cache#reading-a-blob).

When the applied changeset **deletes** a blob-bearing row (a
[gate retract](/docs/local-data) or a genuine delete), coven drops that blob from
both cache folders on this device. A peer only drops its own local cache here; it
never writes a cloud tombstone — that belongs to the deleting owner (see
[Deleting a blob](#deleting-a-blob)).

The cursor is what makes this durable without a queue. A changeset's cursor
advances only once all of that changeset's blobs have arrived. If a download
fails, coven holds the cursor where it was and reports it through
[`PullResult::asset_downloads_failed`](rustdoc:struct:coven::sync::pull::PullResult),
so the next cycle re-pulls that changeset and retries the blob. The changeset plus
the held cursor are the record of what still needs fetching; a separate inbox would
duplicate it.

## Deleting a blob

A blob is shared cloud state that rows on every device may still reference.
Deleting it the instant the deletion drains would strand a device that has not yet
pulled the row removal: it would see a row pointing at nothing. So a delete is not
immediate. The host enqueues it:

```rust
db.enqueue_delete(cloud_key, created_at).await?;
```

The next cycle's [`drain_tombstones`](rustdoc:fn:coven::blob::delete::drain_tombstones)
writes a signed **tombstone** (a durable, signed record that the blob was deleted,
and when) and keeps the blob. The tombstone is signed because the bucket is
untrusted: the at-rest cipher proves only confidentiality, not authorship, so the
deletion is signed by its author like every other control object, and a later GC
verifies the signature and that the author is a current write-capable member
before acting on it.

The blob is held for [`BLOB_TOMBSTONE_GRACE`](rustdoc:const:coven::blob::delete::BLOB_TOMBSTONE_GRACE)
(7 days), the convergence window. A device offline for less than the grace is never
stranded: it comes back, pulls the row removal, and the blob is still there in the
meantime. Once the grace passes,
[`gc_tombstones`](rustdoc:fn:coven::blob::delete::gc_tombstones) on any device
verifies the tombstone, authorizes the author against the membership chain, deletes
the blob, then deletes the tombstone. An unreferenced-but-not-yet-deleted blob is
*correct* state during the window, not garbage a later pass repairs.

A re-upload wins over a pending deletion by construction. Enqueuing an upload drops
a same-device pending delete row; and after a successful (re-)upload the drain
cancels any tombstone a prior cycle (possibly another device) already wrote, so the
GC never reclaims a blob that has just been re-uploaded.

## Cloud layout

Under an opaque home (the default) a blob is stored at a content-addressed key:

```
{namespace}/{ab}/{cd}/{id}
```

`ab` and `cd` are the first two byte-pairs of the dash-stripped `id`, built by
[`LibraryDir::hashed_path`](rustdoc:method:coven::library_dir::LibraryDir::hashed_path).
The two levels of fan-out keep a library with many blobs off a single flat prefix
the storage layer would have to list in one call. The provider sees this key and
the encrypted bytes, never the plaintext file or its name.

## Browsable-home blob paths

A [browsable home](/docs/encryption#opaque-and-browsable-homes) stores each blob
verbatim at a readable path the consumer supplies, instead of by id:

```
{namespace}/{cloud_path}
```

where `cloud_path` is the value coven reads from the declaration's
`cloud_path_column` for that row, e.g. `attachments/Project Plan/diagram.png` rather
than `attachments/0e/f7/0ef7…`. Anyone with bucket access then sees the names the
consumer chose. This is one half of what a browsable home selects; the other half
is that it stores its objects in the clear (see
[encryption](/docs/encryption#opaque-and-browsable-homes)). The two are one choice,
not two.

coven never invents these names. The consumer owns them: it declares a
`cloud_path_column` and stores a readable key in it on every blob-bearing row. A
browsable home with a blob whose `cloud_path` is absent is a surfaced error, never a
silent fall back to the hashed layout. A home cannot be both browsable and shared:
[sharing](/docs/sharing#creating-and-opening-a-share) recomputes a blob's key from
its id, which only the content-addressed layout supports.

The two schemes at a glance:

| | Opaque home (default) | Browsable home |
| --- | --- | --- |
| Config `cloud_home.storage` | `opaque` | `browsable` |
| Runtime scheme | `BlobPathScheme::Hashed` | `BlobPathScheme::Plain` |
| `cloud_path_column` | ignored (leave unset) | required |
| Cloud key | `{namespace}/{ab}/{cd}/{id}` | `{namespace}/{cloud_path}` |
| Blob with no `cloud_path` | keyed by id | surfaced error |

## Staging a blob for upload

coven never reaches outside its own storage for a blob's bytes: the plaintext lives
in coven's [cache](/docs/cache), keyed by the validated blob id, and flows one way
out (cache → cloud) while a pulled blob flows the other (cloud → cache, read back
through [`read_blob`](/docs/cache#reading-a-blob)). The host puts a blob's bytes
into the cache with
[`stage_blob`](rustdoc:fn:coven::blob::cache::stage_blob) when it writes the
blob-bearing row: `pinned` for a `Mirrored` blob (system-pinned on every device,
which the inline push reads back to upload) or unpinned for one the outbox uploads.
A blob the cycle finds unstaged is not ready to publish (see
[How a blob moves out](#how-a-blob-moves-out)).

## Observing uploads

The host can pass a
[`BlobUploadObserver`](rustdoc:trait:coven::blob::BlobUploadObserver) to watch each
outbox upload. The whole observer is optional; two of its methods default to a
no-op:

```rust
#[async_trait::async_trait]
pub trait BlobUploadObserver: Send + Sync {
    async fn on_blob_upload_started(&self, file_id: &str);
    async fn on_blob_upload_progress(&self, file_id: &str, bytes_done: u64, bytes_total: u64) {}
    async fn on_blob_uploaded(&self, file_id: &str) -> DrainControl;
    async fn on_blob_upload_failed(&self, file_id: &str, error: &str);
    fn should_skip_uploads(&self) -> bool { false }
}
```

The callbacks track attempts, not blobs. `on_blob_upload_started` fires once before
each attempt, so a blob that fails twice then succeeds fires it three times.
`on_blob_uploaded` fires once, when the entry leaves the queue.
`on_blob_upload_failed` fires on each failed attempt and carries the error string;
the entry stays queued for retry. A todos app wires these into the attachment's row:
started shows "uploading", uploaded shows "synced", failed shows "will retry".

`on_blob_uploaded` returns a
[`DrainControl`](rustdoc:enum:coven::blob::DrainControl). `Continue` keeps draining
the next queued upload; `Publish` stops the drain so the current cycle publishes
before continuing. A host that flips a gate column on once a unit's last blob lands
returns `Publish` from that same call, so the unit's now-shareable rows reach peers
without waiting for the rest of the batch; the entries still queued drain on the
next cycle, which the loop runs promptly.

`on_blob_upload_progress` reports bytes reaching the cloud between start and the
terminal callback, so a per-file bar moves instead of jumping from 0 to 100%.
`bytes_done` is cumulative and monotonic within one attempt, counting the encrypted
payload (marginally larger than the plaintext). coven coalesces the per-chunk
reports to a 300ms tick so the host is not rebuilt on every chunk, and emits one
final call at `bytes_done == bytes_total` on success. A backend that cannot report
sub-file progress calls it once at the end.

`should_skip_uploads` is the pause switch. The drain checks it before pulling each
entry: while it returns true the queue still accepts new entries but does not drain,
and an upload already in flight finishes. Flip it back and the next cycle picks up
where it left off.
