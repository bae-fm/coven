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
columns locating each blob plus its namespace, encryption scope, provenance, and
cache fill:

```rust
SyncedTable::new("todo_attachments").carries_blob(
    BlobDecl::new("attachments", Provenance::UserProvided, CacheFill::CacheEager)
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
    pub provenance: Provenance,             // UserProvided | HostProvided  (the Local story)
    pub fill: CacheFill,                    // CacheEager | CacheLazy       (the Remote story)
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
    pub provenance: Provenance,     // UserProvided | HostProvided
    pub fill: CacheFill,            // CacheEager | CacheLazy
}
```

A pulled blob is Remote: its bytes land in coven's own [cache](/docs/cache)
(`storage/cache/<namespace>/<id>`, built from the validated namespace + id); the
host never names where a blob file lives.

`cloud_path` is consulted only by a [browsable home](#browsable-home-blob-paths);
an opaque home (the default) ignores it, so leave the `cloud_path_column` unset
unless the home is browsable.

### Cache fill

[`CacheFill`](rustdoc:enum:coven::blob::CacheFill) is the blob's **Remote story**:
how a device gets the bytes once the blob is Remote, declared per blob and read the
same way on every device:

- `CacheEager`: fetched into the cache on pull, on every device — part of "having
  the library". A todo's photo, an album's cover art.
- `CacheLazy`: skipped on pull; a device fetches it into the cache on first read
  instead of up front. Large blobs a device may never open, audio being the case it
  exists for.

The fill has to be a declared property, not a per-device choice: a device deciding
during its own pull whether to fetch a blob can only read the blob's declared fill,
never what another device chose locally. The cache is a Remote-only mechanism — what
a device does with a cached blob (keep it, evict it, pin it) is the
[cache](/docs/cache)'s job — so `CacheEager`/`CacheLazy`/pin/budget describe a blob
only while it is Remote, never while it is Local.

### Provenance

[`Provenance`](rustdoc:enum:coven::blob::Provenance) is the blob's **Local story**:
where the bytes live while the blob is Local. Orthogonal to the cache fill — a blob
declares both:

- `UserProvided`: the user's own file at a path coven references but does not own.
  Bringing the blob back from Remote writes the bytes to a user file, so it needs a
  destination path.
- `HostProvided`: data the host hands coven, kept in coven's own local store at
  `storage/local/<namespace>/<id>`. Bringing it back from Remote restores it there,
  no path needed.

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

A blob reaches the cloud one of two ways, split by [provenance](#provenance).

**Inline with the changeset (host-provided).** coven owns a host-provided blob's
bytes — in its local store or its cache — so it uploads each one inline as its row
reaches the *outgoing* changeset, before the envelope is packed and pushed: it reads
the plaintext, resolves the scope to a key, encrypts, and writes to the blob's cloud
key. This is provenance-based, regardless of cache fill. If the bytes are not on
disk the cycle **aborts** rather than publishing a row that points at a blob the
cloud does not hold: a published-but-missing blob would 404 on every puller
permanently. The next cycle retries once the bytes are present.

**Through the upload outbox (user-provided).** A user-provided blob is the user's
own file; coven uploads it from that path through the durable upload outbox, with
progress and retry. The `make_remote` transition enqueues one upload per
user-provided blob of a gated root; a host can also enqueue an upload directly for
an out-of-band file (audio is the case):

```rust
db.enqueue_upload(file_id, cloud_key, source_path, scope, retain_pinned, created_at).await?;
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
the [cache](/docs/cache), at `storage/cache/<namespace>/<id>` under the library
directory, decrypted under its scope. A download is skipped when the file is already
present, which makes the step idempotent.

Only `CacheEager` blobs download here. A `CacheLazy` blob is skipped on pull and
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

## Where a blob's bytes come from

coven uploads a blob from whichever local copy its [provenance](#provenance) names.
A **host-provided** blob is data the host hands coven, which coven keeps in its own
local store at `storage/local/<namespace>/<id>` (via
[`local_files::store`](rustdoc:fn:coven::blob::local_files::store)); the inline push
reads it back to upload, then moves the copy into the [cache](/docs/cache) as the
blob becomes Remote. A **user-provided** blob is the user's own file at a path coven
references; `make_remote` uploads it straight from that path. Either way coven never
reaches outside the copy it was given, and a blob whose bytes aren't present is not
ready to publish (see [How a blob moves out](#how-a-blob-moves-out)).

## Observing transitions and uploads

The host can pass a
[`BlobTransitionObserver`](rustdoc:trait:coven::blob::BlobTransitionObserver) to
watch uploads and the locality transitions. It only *reports* — coven owns flipping
the gate and deciding when a cycle publishes. The whole observer is optional; most
methods default to a no-op:

```rust
#[async_trait::async_trait]
pub trait BlobTransitionObserver: Send + Sync {
    async fn on_blob_upload_started(&self, blob_id: &str);
    async fn on_blob_upload_progress(&self, blob_id: &str, bytes_done: u64, bytes_total: u64) {}
    async fn on_blob_uploaded(&self, blob_id: &str);
    async fn on_blob_upload_failed(&self, blob_id: &str, error: &str);
    fn should_skip_uploads(&self) -> bool { false }

    // make_remote / make_local completion, and make_local per-blob progress:
    async fn on_root_made_remote(&self, root_table: &str, root_id: &str) {}
    async fn on_root_made_local(&self, root_table: &str, root_id: &str) {}
    async fn on_blob_materialize_progress(
        &self, root_table: &str, root_id: &str, blob_id: &str, done: u64, total: u64,
    ) {}
}
```

The upload callbacks track attempts, not blobs. `on_blob_upload_started` fires once
before each attempt, so a blob that fails twice then succeeds fires it three times.
`on_blob_uploaded` fires once, when the entry leaves the queue — notification only,
since coven, not the host, flips the gate and breaks the drain to publish a completed
`make_remote`. `on_blob_upload_failed` fires on each failed attempt and carries the
error string; the entry stays queued for retry. A todos app wires these into the
attachment's row: started shows "uploading", uploaded shows "synced", failed shows
"will retry".

`on_root_made_remote` / `on_root_made_local` fire when coven *completes* a
transition (including one resumed after a restart), so the host's row-updated event
survives a crash rather than being lost with an in-memory flag.
`on_blob_materialize_progress` moves a `make_local`'s per-file progress bar.

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
