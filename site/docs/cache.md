# Cache

A blob lives in the cloud, encrypted. To read it (show a photo, play a track) a
device needs the plaintext bytes on local disk. The cache is where those bytes
live: a device-local store of blob files, keyed by blob id, that serves a read
from disk on a hit and fetches from the cloud on a miss.

It is a separate layer from the [cloud blob lifecycle](/docs/blobs). That page
covers how a blob is uploaded, downloaded, and deleted across devices. This page
covers the one device's copy: where it sits, when it is fetched, how long it is
kept, and how a user pins a blob to keep it offline.

The cache is coven's, under the library directory. The examples use a todos app
whose `todo_attachments` rows each point at a photo; a music app would point at
audio instead, which is the case the lazy half of the cache exists for.

The cache holds **Remote** blobs only — bytes that also live in the cloud, so a
cache copy is always re-fetchable. A **Local** blob is never in the cache: it is
the user's own file at a path (user-provided) or coven's own copy in the local
store (host-provided). See [Blobs](/docs/blobs) for that distinction.

## Two folders, no table

A Remote blob is in exactly one of two folders under the library directory, or in
neither. Both are segmented by the blob's namespace, so each namespace's cache
evicts against its own budget without touching another's:

```
storage/pinned/<namespace>/{ab}/{cd}/<id>   protected, never evicted
storage/cache/<namespace>/{ab}/{cd}/<id>    opportunistic, evictable
neither                                     remote-only: no file, fetched on next read
```

`{ab}` and `{cd}` are the first two byte-pairs of the dash-stripped id (the same
content-addressed shard the cloud layout uses, built by
[`LibraryDir::pinned_blob_path`](rustdoc:method:coven::library_dir::LibraryDir::pinned_blob_path)
/ [`cache_blob_path`](rustdoc:method:coven::library_dir::LibraryDir::cache_blob_path)).

There is no cache table. The file on disk *is* the presence record, and the
folder it sits in *is* the retention class. Nothing the two directory listings
can't answer, so there is no metadata sidecar to keep in step with the disk.
Every cache write is atomic
([`local_blob::write_atomic`](rustdoc:fn:coven::local_blob::write_atomic): write a
temp file, then rename it over the destination), so a crash mid-write cannot
leave a torn file that a later read would trust as whole. Pinning and unpinning
are a `rename` within `storage/` (one filesystem, atomic), so a blob is never
visible in both folders or in neither mid-move.

Cache files are plaintext. Encryption happens on the way to the cloud, not on
local disk; a blob comes back from the cloud decrypted and lands in the cache as
the bytes the host reads.

## Reading a blob

[`CovenHandle::read_blob`](rustdoc:method:coven::CovenHandle::read_blob) serves a
blob's whole contents.

```rust
let bytes = handle.read_blob(&blob).await?;
```

It resolves by where the bytes are, in order: a **user-provided Local** blob is
read from the user's own file (an external ref), a **host-provided Local** blob
from the local store — both with no cloud fallback. Otherwise the **cache** —
`pinned/<namespace>/<id>` then `cache/<namespace>/<id>`, a file in either folder a
hit read straight off disk. On a cache miss it resolves the blob's
[scope](/docs/blobs#encryption-scope) to a key, downloads and decrypts the object
from the cloud, writes the plaintext to `cache/<namespace>/<id>` (the evictable
folder, never the protected one), and returns the bytes it just fetched. A plain
read populates the cache; it never pins.

[`CovenHandle::open_blob_stream`](rustdoc:method:coven::CovenHandle::open_blob_stream)
is the ranged sibling, for a host that streams or seeks a large blob (audio
playback) without loading the whole file:

```rust
let slice = handle
    .open_blob_stream(&blob, source_size, offset, len)
    .await?;
```

`source_size` is the blob's plaintext length, which the host knows (the row that
owns the blob carries it). The range is validated against it once, so the request
behaves identically whether served from the local file or the cloud: a zero
length is an empty result, and a range past the end is an error rather than a
short read. A cache hit reads the slice straight off the local plaintext file at
`offset`, with no decryption. A miss range-reads and decrypts only the covering
chunks from the cloud (see [`BlobRangeReader`](/docs/storage#ranged-reads)).

A ranged miss writes **no** cache file. A partial file under `cache/<namespace>/<id>`
would be read as the whole blob by `read_blob`, since presence is the only truth,
so only the whole-file `read_blob` ever populates the cache.

On either path, a failure to even check whether a file exists (a broken
filesystem) is surfaced, never collapsed into a miss: re-downloading over a
present file would be wasteful and could hide a real fault.

## Cache fill: eager and lazy

Whether a Remote blob lands in the cache automatically depends on its **cache
fill**, declared per blob in the table's
[declaration](/docs/blobs#declaring-which-rows-carry-blobs) as a
[`CacheFill`](rustdoc:enum:coven::blob::CacheFill):

- `CacheEager`: fetched into the cache on every device's pull. Part of "having the
  library", e.g. an album's cover art, so a grid renders from local bytes without a
  fetch. It lands in the **evictable** `storage/cache/<namespace>/<id>` — it is not
  pinned, so if it later falls out of its namespace's budget it shows a placeholder
  until the next read re-fetches it.
- `CacheLazy`: skipped on pull. A pulling device does not fetch it up front; the
  first `read_blob` does, populating `cache/<namespace>/<id>`. This is for large
  blobs a device may never open, audio being the motivating case.

Both fills cache evictably; the difference is only *when* the bytes arrive (on pull
vs. on first read). Neither is pinned automatically — pinning is a separate, manual
gesture below.

## Pinning

The cache is evictable. Pinning is how a user keeps a chosen Remote blob local and
safe from eviction (an offline-for-the-flight gesture).

[`CovenHandle::pin`](rustdoc:method:coven::CovenHandle::pin) ensures a blob is
both present and protected, in `storage/pinned/<namespace>/<id>`:

```rust
handle.pin(&blobs).await?;
```

A pin *populates*, it is not a flag flip. Three cases per blob: already in
`pinned/` (nothing to do); in `cache/` (rename it into `pinned/`, promoting a
blob a read or an eager pull already fetched with no cloud round-trip); in neither
(fetch from the cloud straight into `pinned/`). It takes `BlobRef`s rather than
bare ids because the from-absent case needs the blob's cloud coordinates
(namespace, scope, cloud_path), which an id alone lacks. It is idempotent.

[`CovenHandle::unpin`](rustdoc:method:coven::CovenHandle::unpin) drops the
protection: it moves `pinned/<namespace>/<id>` back to `cache/<namespace>/<id>`,
so the file stays readable but becomes evictable again. It is not a delete.
Unpin works on any blob regardless of its `CacheFill`; a `CacheEager` blob that
was never pinned is already evictable, so unpinning it is a no-op.

## The size budget

`pinned/` grows with what the user chose to keep; `cache/` grows with what gets
fetched. Left unbounded the evictable cache would grow forever, so the host can set
a **per-namespace** budget with `handle.set_cache_budget(...)`, in bytes:

```rust
handle.set_cache_budget("audio", 2 * 1024 * 1024 * 1024).await?; // 2 GiB for audio
handle.set_cache_budget("covers", 64 * 1024 * 1024).await?;      // 64 MiB for covers
```

Each namespace evicts independently against its own budget, so a small namespace
(`covers`) is never wiped by pressure from a big one (`audio`). The budget counts
**only** the files under that namespace's `cache/<namespace>/`. `pinned/` is
structurally exempt, because the eviction sweep never looks there: a pinned blob
can never be evicted, whatever the budget; the local store is never walked either.
With no budget set for a namespace (`handle.get_cache_budget(...)` returns
`None`) eviction is off for it and that namespace's cache is unbounded until the
host opts into a limit.

Coven runs the budget sweep after every populate into a namespace. It sums that
namespace's `cache/<namespace>/` files and, if the total is over budget, deletes
the oldest by modification time until the total is back under it. Modification
time is the recency proxy: there is no `last_accessed` column (the same
folder-truth tradeoff the whole cache makes), so the oldest-written file goes
first. Pinning, not access tracking, is how a blob is kept.

The file a populate just wrote is excluded from the candidates outright, so a read
or stage can never evict the very bytes it just produced (its size still counts
toward the total it must fit under). If that one in-use file alone is larger than
the whole budget, the cache is left holding exactly it and over budget by that
much; the caller still gets its bytes, and the over-budget condition is logged
rather than reported as met.

Eviction is best-effort and never fails the populate that triggered it: the write
already succeeded, so the bytes are durably cached. A cache briefly over budget is
not wrong state (the next populate's sweep corrects it), so an eviction failure is
logged and the read or stage still returns its bytes.

## Explicit Eviction

[`CovenHandle::evict_blob`](rustdoc:method:coven::CovenHandle::evict_blob) removes
one blob's local cache copies from both `cache/<namespace>/` and
`pinned/<namespace>/`. It does not delete the cloud blob or the row that
references it; a later `read_blob` can fetch the bytes again if the row still
exists.

## At a glance

| | `storage/pinned/<namespace>/` | `storage/cache/<namespace>/` |
| --- | --- | --- |
| Holds | user-pinned Remote blobs | eagerly-pulled (`CacheEager`) + read-populated (`CacheLazy`) Remote blobs |
| Counts toward the namespace budget | no (exempt) | yes |
| Evicted by budget sweep | never | oldest-by-mtime when over the namespace budget |
| Removed by `evict_blob` | yes | yes |
| Populated by | `pin` | the pull's `CacheEager` download, `read_blob` miss, `write_blob` |
