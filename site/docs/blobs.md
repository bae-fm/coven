# Blobs

A synced row is small: a few columns of text and numbers in a changeset. A photo
attached to a todo is not. coven syncs those large files separately from the
changesets that reference them. The host declares which rows carry a file and which
columns locate it; coven derives the blob set itself and owns the encryption, the
cloud layout, the upload, and the retry.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

This page is the cloud lifecycle: how a blob is described, uploaded, pulled, and
deleted across devices. The one device's copy (where a pulled blob lands, how it
is kept and read offline) is the [cache](/docs/cache).

Examples: a `todo_attachments` row points at one photo file. When that row
syncs, the photo has to reach every other device too.

## Declaring which rows carry blobs

Three different paths must each work out which files a set of rows carries:
an outgoing changeset, an incoming one, and a whole database after bootstrap.
All three must derive the same set from the rows alone, and two of them run
where the host is not involved at all. So blob-bearing-ness is a per-table
declaration coven can evaluate anywhere, not a runtime callback. The host
marks a synced table with
[`carries_blob`](rustdoc:method:coven::SyncedTable::carries_blob),
passing a [`BlobDecl`](rustdoc:struct:coven::BlobDecl) that names the
columns locating each blob plus its namespace, encryption scope, provenance, and
cache fill:

```rust
SyncedTable::new("todo_attachments", RowIdentity::IndependentUuid).carries_blob(
    BlobDecl::new("attachments", Provenance::UserProvided, CacheFill::CacheEager)
        // .with_id_column("file_id")              // defaults to the PK ("id")
        // .with_cloud_path_column("path")         // for a browsable home
        // .with_scope(BlobScope::Derived("attachments".into()))
        // .write_once()                           // this row is never repointed
)
```

The attachment rows are independently created, so their row ids are canonical
UUIDv4 or UUIDv7 values. The row id remains the global logical identity whether
or not the table carries a blob.

```rust
pub struct BlobDecl {
    pub id_column: String,                  // blob-id column; defaults to the PK ("id")
    pub namespace: String,                  // cloud namespace, e.g. "attachments"
    pub cloud_path_column: Option<String>,  // readable-key column for a browsable home
    pub scope: BlobScope,                   // Master | Derived(name)
    pub provenance: Provenance,             // UserProvided | HostProvided  (the Local story)
    pub fill: CacheFill,                    // CacheEager | CacheLazy       (the Remote story)
    pub replacement: BlobReplacement,       // Replaceable | WriteOnce      (the replacement story)
}
```

coven resolves the declaration's column *names* against the live schema once per
cycle, then derives every blob set it needs itself, reading the declared columns
off a row:

- over an outgoing changeset's rows: what to upload;
- over an incoming changeset's rows: what to download (and, for a deleted row,
  whose local cache to drop);
- over the whole database after a [snapshot bootstrap](/docs/bootstrap): the
  backfill. A bootstrapped device receives the catalog rows but not the per-row
  blobs (the snapshot is a whole-database image, and the incremental pull that
  follows starts past the changesets that carried them), so coven re-derives them
  from the live rows.

A row maps to the same blob whichever way it moves, so one declaration serves every
path. coven reads the declared columns off the row to build a
[`BlobRef`](rustdoc:struct:coven::BlobRef), one blob's resolved reference:

```rust
pub struct BlobRef {
    pub namespace: String,          // cloud namespace, e.g. "attachments"
    pub id: String,                 // blob id, from the id column (the row's id by default)
    pub scope: BlobScope,           // Master | Derived(id)
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
unless the home is browsable. What a browsable home requires of that path depends on
whether the blob is [replaceable or write-once](#a-cloud-object-is-never-rewritten).

### Cache fill

[`CacheFill`](rustdoc:enum:coven::CacheFill) is the blob's **Remote story**:
how a device gets the bytes once the blob is Remote, declared per blob and read the
same way on every device:

- `CacheEager`: fetched into the cache on pull, on every device, part of "having
  the store". A todo's photo, an album's cover art.
- `CacheLazy`: skipped on pull; a device fetches it into the cache on first read
  instead of up front. Large blobs a device may never open, audio being the case it
  exists for.

The fill has to be a declared property, not a per-device choice: a device deciding
during its own pull whether to fetch a blob can only read the blob's declared fill,
never what another device chose locally. The cache is a Remote-only mechanism, and what
a device does with a cached blob (keep it, evict it, pin it) is the
[cache](/docs/cache)'s job, so `CacheEager`/`CacheLazy`/pin/budget describe a blob
only while it is Remote, never while it is Local.

### Provenance

[`Provenance`](rustdoc:enum:coven::Provenance) is the blob's **Local story**:
where the bytes live while the blob is Local. Orthogonal to the cache fill; a blob
declares both:

- `UserProvided`: the user's own file at a path coven references but does not own.
  Bringing the blob back from Remote writes the bytes to a user file, so it needs a
  destination path.
- `HostProvided`: data the host hands coven, kept in coven's own local store at
  `storage/local/<namespace>/<id>`. Bringing it back from Remote restores it there,
  no path needed.


<svg class="flow" viewBox="0 0 660 244" role="img" aria-label="A blob is Local (a user file or coven's local store) or Remote (a sealed cloud object with per-device cache copies); offloading uploads the bytes, the cloud copy becomes the source, devices keep cache copies">
<text class="hdr" x="155" y="30" text-anchor="middle">LOCAL</text>
<text class="hdr" x="505" y="30" text-anchor="middle">REMOTE</text>
<rect class="lane" x="20" y="40" width="270" height="150" rx="10"/>
<rect class="lane" x="370" y="40" width="270" height="150" rx="10"/>
<rect class="chip" x="40" y="56" width="230" height="26" rx="7"/>
<text class="lbl s11" x="155" y="73" text-anchor="middle">the user's file at a path</text>
<text class="sub" x="155" y="96" text-anchor="middle">user-provided</text>
<rect class="chip" x="40" y="112" width="230" height="26" rx="7"/>
<text class="lbl s11" x="155" y="129" text-anchor="middle">coven's local store</text>
<text class="sub" x="155" y="152" text-anchor="middle">host-provided</text>
<rect class="chipo" x="390" y="56" width="230" height="26" rx="7"/>
<path class="glyph" d="M404 67v-3a3 3 0 0 1 6 0v3"/>
<rect class="glyphf" x="402.5" y="67" width="9" height="7" rx="1.5"/>
<text class="lbl s11" x="512" y="73" text-anchor="middle">cloud object · sealed</text>
<rect class="chipd" x="390" y="112" width="230" height="26" rx="7"/>
<text class="lbl s11" x="505" y="129" text-anchor="middle">cache copy on each device</text>
<text class="sub" x="505" y="152" text-anchor="middle">eager or lazy fill · pin · budget</text>
<line class="arr" x1="294" y1="74" x2="366" y2="74" marker-end="url(#fa)"/>
<text class="sub" x="330" y="64" text-anchor="middle">make_remote</text>
<line class="arr" x1="366" y1="156" x2="294" y2="156" marker-end="url(#fa)"/>
<text class="sub" x="330" y="176" text-anchor="middle">make_local</text>
<circle class="numc" cx="330" cy="88" r="8"/>
<text class="num" x="330" y="91.5" text-anchor="middle">1</text>
<circle class="numc" cx="382" cy="69" r="8"/>
<text class="num" x="382" y="72.5" text-anchor="middle">2</text>
<circle class="numc" cx="382" cy="125" r="8"/>
<text class="num" x="382" y="128.5" text-anchor="middle">3</text>
<text class="sub" x="330" y="232" text-anchor="middle">offloading, in time: 1 the upload runs · 2 the sealed cloud object becomes the source · 3 each device keeps a re-fetchable cache copy</text>
</svg>

## Encryption scope

The declaration's [`BlobScope`](rustdoc:enum:coven::BlobScope) selects the
key the blob is encrypted under. The host names *what* a blob is scoped to,
never the raw key bytes:

- `Master` encrypts with the store master key. Every member holds it, so every
  member can decrypt the blob. The common case, with no key management at all.
- `Derived(scope_id)` encrypts with a key coven derives from the master key,
  one distinct key per `scope_id` (see
  [Chunked encryption](/docs/encryption#chunked-encryption) for the
  derivation). Deterministic: the same `scope_id` yields the same key on push
  and on pull, which is what lets a puller re-derive it and decrypt. The
  corollary is that `scope_id` must be stable for the lifetime of the reference.
  When the scope is the row id, a primary-key change deletes the old identity
  and inserts a new row whose scope derives a different key.

## How a blob moves out

A blob reaches the cloud one of two ways, split by [provenance](#provenance).

**With the changeset (host-provided).** coven owns a host-provided blob's bytes,
in its local store or its cache, so it uploads each one before the row is
published. A host writes the row and bytes together through
[`CovenHandle::write`](rustdoc:method:coven::CovenHandle::write); a
`make_remote` transition uploads any host-provided blobs before flipping the
root's gate. If the bytes are not on disk the cycle aborts rather than publishing
a row that points at a blob the cloud does not hold.

**Through the upload outbox (user-provided).** A user-provided blob is the
user's own file, often large, on a connection that can drop; the upload has to
survive restarts and retries without re-asking the host. That is what the
durable upload outbox is for: coven uploads the file from its path with
progress and retry. The host starts that transition through
[`CovenHandle::make_remote`](rustdoc:method:coven::CovenHandle::make_remote):

```rust
handle.make_remote("todos", todo_id, pin).await?;
```

`make_remote` enqueues one upload per user-provided blob of the gated root.
Coven persists each blob's final cloud key and [`BlobScope`](#encryption-scope),
then resolves the scope to a key when the upload drains, long after the enqueue
site is gone. Enqueuing an upload also cancels any pending delete of the same key
(latest intent wins), so a re-upload is never tombstoned in the same cycle.

The outbox is coven's `cloud_outbox` table, created by the handle open path. The
host does not mutate it by hand; user-provided transitions and sync enqueue rows
through coven, and host-provided row+blob writes go through `handle.write(...)`.
Each row is an
`OutboxEntry` whose
`OutboxOperation` is an `Upload`,
`Delete`, or `Cancel`.

Nothing uploads at enqueue time. The next sync cycle's
`drain_uploads` works through
the pending entries; for each one it:

1. reads the local file,
2. resolves the persisted scope to a key,
3. seals the bytes and writes them to the entry's `cloud_key`,
4. removes the entry on success.

The drain runs before the changeset push, but the cycle does not hold the
whole changeset back while it runs.

Blob-before-row ordering is owned by coven, per gated root, not by a global push
gate. `make_remote` keeps the root's [gate column](/docs/local-data) off until
the root's blobs upload, then coven flips it on. The gate cuts the row while its
column is off and re-emits the row's full subtree when it flips on, so a peer
never pulls a changeset that points at a blob the cloud does not yet hold.
Because this is per root, one slow or stuck upload holds back only its own root.

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


<svg class="flow" viewBox="0 0 660 128" role="img" aria-label="make_remote enqueues an outbox row; the drain seals and writes the blob; success removes the entry, failure retries with backoff">
<rect class="chip" x="15" y="44" width="140" height="30" rx="7"/>
<text class="lbl s11" x="85" y="63" text-anchor="middle">make_remote(root)</text>
<line class="arr" x1="159" y1="59" x2="176" y2="59" marker-end="url(#fa)"/>
<rect class="chipo" x="180" y="44" width="140" height="30" rx="7"/>
<text class="lbl s11" x="250" y="63" text-anchor="middle">cloud_outbox row</text>
<text class="sub" x="250" y="92" text-anchor="middle">upload · delete · cancel</text>
<line class="arr" x1="324" y1="59" x2="341" y2="59" marker-end="url(#fa)"/>
<rect class="chip" x="345" y="44" width="140" height="30" rx="7"/>
<text class="lbl s11" x="415" y="63" text-anchor="middle">drain: seal + write</text>
<text class="sub" x="415" y="92" text-anchor="middle">retry 30s doubling, cap 1 h</text>
<line class="arr" x1="489" y1="59" x2="506" y2="59" marker-end="url(#fa)"/>
<rect class="chipo" x="510" y="44" width="140" height="30" rx="7"/>
<path class="glyph" d="M524 56v-3a3 3 0 0 1 6 0v3"/>
<rect class="glyphf" x="522.5" y="56" width="9" height="7" rx="1.5"/>
<text class="lbl s11" x="588" y="63" text-anchor="middle">blob object</text>
</svg>

## The pull side

Pull derives the blobs an incoming Store package references from the declarations
and downloads required eager bytes before materializing the commit, so a row is
never accepted before its required blobs are durable. A downloaded blob lands in
the [cache](/docs/cache), at `storage/cache/<namespace>/<id>` under the store
directory, decrypted under its scope. A download is skipped when the exact file is
already present.

Only `CacheEager` blobs download here. A `CacheLazy` blob is skipped on pull and
fetched on its first [`read_blob`](/docs/cache#reading-a-blob).

When the applied changeset **deletes** a blob-bearing row (a
[gate retract](/docs/local-data) or a genuine delete), coven drops that blob from
both cache folders on this device. A peer only drops its own local cache here; it
never writes a cloud tombstone; that belongs to the deleting owner (see
[Deleting a blob](#deleting-a-blob)).

The exact materialized position is the durable boundary. It advances in the same
SQLite transaction as the package rows only after required blob work succeeds. If
a download fails, coven leaves that device sequence and commit hash unmaterialized
and reports `asset_downloads_failed`, so the commit and its blobs retry together.

## Deleting a blob

A blob is shared cloud state that rows on every device may still reference.
Deleting it the instant the deletion drains would strand a device that has not yet
pulled the row removal: it would see a row pointing at nothing. So a delete is not
immediate. The host requests blob replacement or row removal through
[`CovenHandle::write`](rustdoc:method:coven::CovenHandle::write), and coven
enqueues the cloud delete with the row change.

The next cycle's `drain_tombstones`
writes a signed **tombstone** (a durable, signed record that the blob was deleted,
and when) and keeps the blob. The tombstone is signed because the bucket is
untrusted: the at-rest cipher proves only confidentiality, not authorship, so the
deletion is signed by its author like every other control object, and a later GC
verifies the signature and that the author is a current write-capable member
before acting on it.

The blob is held for the tombstone grace, the convergence window: 7 days by
default
([`BLOB_TOMBSTONE_GRACE`](rustdoc:const:coven_protocol::blob::BLOB_TOMBSTONE_GRACE)),
host-configurable through
[`CovenBuilder::blob_tombstone_grace`](rustdoc:method:coven::CovenBuilder::blob_tombstone_grace)
(a zero-or-negative grace is refused at open). A device offline for less than the grace is never
stranded: it comes back, pulls the row removal, and the blob is still there in the
meantime. Once the grace passes,
`gc_tombstones` on any device
verifies the tombstone, authorizes the author against the membership chain, deletes
the blob, then deletes the tombstone. An unreferenced-but-not-yet-deleted blob is
*correct* state during the window, not garbage a later pass repairs.


<svg class="flow" viewBox="0 0 660 118" role="img" aria-label="A deletion writes a signed tombstone; the blob survives a seven-day grace window; then GC verifies the tombstone and deletes both">
<line class="arrd" x1="30" y1="66" x2="640" y2="66" marker-end="url(#fam)"/>
<circle class="glyphf" cx="80" cy="66" r="4"/>
<text class="lbl s11" x="80" y="44" text-anchor="middle">row deleted</text>
<circle class="glyphf" cx="210" cy="66" r="4"/>
<text class="lbl s11" x="210" y="92" text-anchor="middle">signed tombstone written</text>
<rect class="tx" x="210" y="54" width="290" height="24" rx="6"/>
<text class="sub" x="355" y="44" text-anchor="middle">grace (default 7 days) · blob still readable by laggards</text>
<circle class="glyphf" cx="540" cy="66" r="4"/>
<text class="lbl s11" x="540" y="92" text-anchor="middle">GC verifies, deletes both</text>
</svg>

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
[`StoreDir::hashed_path`](rustdoc:method:coven::StoreDir::hashed_path).
The two levels of fan-out keep a store with many blobs off a single flat prefix
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
silent fall back to the hashed layout.

The two schemes at a glance:

| | Opaque home (default) | Browsable home |
| --- | --- | --- |
| Config `cloud_home.storage` | `opaque` | `browsable` |
| Runtime scheme | `BlobPathScheme::Hashed` | `BlobPathScheme::Plain` |
| `cloud_path_column` | ignored (leave unset) | required |
| Cloud key | `{namespace}/{ab}/{cd}/{id}` | `{namespace}/{cloud_path}` |
| Blob with no `cloud_path` | keyed by id | surfaced error |
| Key names its blob | by the `{id}` in the key | [depends on the blob](#a-cloud-object-is-never-rewritten) |

### A cloud object is never rewritten

A cloud object is never rewritten with different bytes. Pull verifies an object
against its row's content hash, and a Store position materializes only after every
required blob arrives. Replacing bytes at one blob key would make an earlier valid
commit impossible to materialize.

A hashed key gets this for free: it carries the blob id, and a blob id names one immutable
byte-string, minted fresh for every stored blob. A readable path is the consumer's own, so
coven asks the consumer one question about each blob-bearing table — **can this row ever be
repointed at a different blob?** — and enforces whichever guarantee follows.

#### Replaceable (the default)

The row may be repointed: a cover is changed, an attachment is swapped. Then the **key must
move with the blob**, so the path has to name it. Its file name — the last `/`-segment,
extension stripped — must be the blob id, or end with `-{blob_id}`:

```
covers/Live at Leeds/cover-0ef7a1c9.jpg     ✓ names its blob
covers/Live at Leeds/0ef7a1c9.jpg           ✓ names its blob
covers/Live at Leeds/cover.jpg              ✗ surfaced error — names no blob
covers/Live at Leeds/0ef7a1c9/cover.jpg     ✗ surfaced error — the id must name the object,
                                              not a directory above it
```

The id must *end* the file name's stem, so that blob `1` cannot satisfy blob `11`'s path and
the two be keyed at one object. Replacing the blob then writes a *new* object beside the one
it replaced, which stands at its own key until its [tombstone](#deleting-a-blob) is
collected.

#### Write-once

```rust
SyncedTable::new("release_files", RowIdentity::IndependentUuid).carries_blob(
    BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy)
        .with_cloud_path_column("cloud_path")
        .write_once()
)
```

The row is never repointed: the blob it names when it is inserted is the blob it names for
life. Nothing ever rewrites the object at its key, so there is nothing to protect it from —
and the path is free to be a **stable, fully readable name**, which is what a browsable home
is for:

```
audio/Live at Leeds/01 Sonata No. 3.flac    ✓ the consumer's own name, no blob id
```

coven holds the consumer to the declaration: **repointing a write-once row is a surfaced
error.** A changeset UPDATE reports only the columns whose values changed, so a blob-id
column appearing in one *is* the repointing, and coven refuses it there rather than
discovering a rewritten object later.

Write-once is the weaker of the two guarantees, and it is opt-in for that reason. coven
refuses the reuse it can see — the repointing. It cannot see a consumer *deleting* a row and
inserting a different blob at the same `cloud_path`: the deleted row is gone, and coven keeps
no history of the paths it has handed out. **Declaring `write_once()` is therefore also a
promise that a path is never reused by a different blob.** Derive the path from data that
never repeats and it holds by construction — a path carrying a freshly minted id for the
thing being imported can never be handed out twice, even though no blob id appears in the
name a human reads.

#### What either guarantee buys

Because no two blobs ever share a key, an object standing at a blob's key *is* that blob's
bytes. A sealed cloud object never has to be asked what it holds, and coven keeps no record
of what it wrote where — the push simply skips an upload when the object is already there.
And two failures that no retry can repair become unrepresentable:

- **Two devices replacing the same blob at once** would otherwise write one key, leaving the
  object holding whichever device the bucket saw last while the row holds whichever device
  last-write-wins picked. With the key moving with the blob, they are two objects, and the
  row's winner names one of them.
- **A changeset written before a replacement** would otherwise name bytes that were
  overwritten, and the device that pulls it late could never satisfy its hash. The
  superseded object instead stands at its own key for the whole tombstone grace — the same
  convergence window coven already promises a device that has been away.

## Where a blob's bytes come from

coven uploads a blob from whichever local copy its [provenance](#provenance) names.
A **host-provided** blob is data the host hands coven, which coven keeps in its own
local store at `storage/local/<namespace>/<id>` (via
`local_files::store`); the inline push
reads it back to upload, then moves the copy into the [cache](/docs/cache) as the
blob becomes Remote. A **user-provided** blob is the user's own file at a path coven
references; `make_remote` uploads it straight from that path. Either way coven never
reaches outside the copy it was given, and a blob whose bytes aren't present is not
ready to publish (see [How a blob moves out](#how-a-blob-moves-out)).

## Observing transitions and uploads

The host can pass a
[`BlobTransitionObserver`](rustdoc:trait:coven::BlobTransitionObserver) to
watch uploads and the locality transitions. It only *reports*; coven owns flipping
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
`on_blob_uploaded` fires once, when the entry leaves the queue. Notification only:
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
