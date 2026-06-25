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
audio instead, which is the case the on-demand half of the cache exists for.

## Two folders, no table

A blob is in exactly one of two folders under the library directory, or in
neither:

```
storage/pinned/{ab}/{cd}/<id>    protected, never evicted
storage/cache/{ab}/{cd}/<id>     opportunistic, evictable
neither                          remote-only: no file, fetched on next read
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

[`read_blob`](rustdoc:fn:coven::blob::cache::read_blob) serves a blob's whole
contents.

```rust
let bytes = coven::blob::cache::read_blob(&db, &library_dir, &storage, &blob).await?;
```

It checks `pinned/<id>` then `cache/<id>`; a file in either folder is a hit and
is read straight off disk. On a miss it resolves the blob's
[scope](/docs/blobs#encryption-scope) to a key, downloads and decrypts the object
from the cloud, writes the plaintext to `cache/<id>` (the evictable folder, never
the protected one), and returns the bytes it just fetched. A plain read populates
the cache; it never pins.

[`open_blob_stream`](rustdoc:fn:coven::blob::cache::open_blob_stream) is the
ranged sibling, for a host that streams or seeks a large blob (audio playback)
without loading the whole file:

```rust
let slice = coven::blob::cache::open_blob_stream(
    &db, &library_dir, &storage, &blob,
    source_size, offset, len,
).await?;
```

`source_size` is the blob's plaintext length, which the host knows (the row that
owns the blob carries it). The range is validated against it once, so the request
behaves identically whether served from the local file or the cloud: a zero
length is an empty result, and a range past the end is an error rather than a
short read. A cache hit reads the slice straight off the local plaintext file at
`offset`, with no decryption. A miss range-reads and decrypts only the covering
chunks from the cloud (see [`BlobRangeReader`](/docs/storage#ranged-reads)).

A ranged miss writes **no** cache file. A partial file under `cache/<id>` would
be read as the whole blob by `read_blob`, since presence is the only truth, so
only the whole-file `read_blob` ever populates the cache.

On either path, a failure to even check whether a file exists (a broken
filesystem) is surfaced, never collapsed into a miss: re-downloading over a
present file would be wasteful and could hide a real fault.

## Mirrored and on-demand

Whether a blob lands in the cache automatically depends on its retention class,
declared per blob in the table's
[declaration](/docs/blobs#declaring-which-rows-carry-blobs) as a
[`BlobSync`](rustdoc:enum:coven::blob::BlobSync):

- `Mirrored`: downloaded on pull and kept on every device. Part of "having the
  library", e.g. a todo's photo or an album's cover art. The pull writes it into
  `storage/pinned/<id>` directly, so it is present and protected from the moment
  the row arrives.
- `OnDemand`: uploaded on push but skipped on pull. A pulling device does not
  fetch it up front; the first `read_blob` does, populating `cache/<id>`. This is
  for large blobs a device may never open, audio being the motivating case.

So a `Mirrored` blob is system-pinned, and an `OnDemand` blob is cached lazily
and evictably unless the user pins it.

## Pinning

`Mirrored` blobs aside, a device caches `OnDemand` blobs only as they are read,
and the cache is evictable. Pinning is how a user keeps a chosen blob local and
safe from eviction (an offline-for-the-flight gesture).

[`pin`](rustdoc:fn:coven::blob::cache::pin) ensures a blob is both present and
protected, in `storage/pinned/<id>`:

```rust
coven::blob::cache::pin(&db, &library_dir, &storage, &blobs).await?;
```

A pin *populates*, it is not a flag flip. Three cases per blob: already in
`pinned/` (nothing to do); in `cache/` (rename it into `pinned/`, promoting a
blob a read already fetched with no cloud round-trip); in neither (fetch from the
cloud straight into `pinned/`). It takes `BlobRef`s rather than bare ids because
the from-absent case needs the blob's cloud coordinates (namespace, scope,
cloud_path), which an id alone lacks. It is idempotent.

[`unpin`](rustdoc:fn:coven::blob::cache::unpin) drops the protection: it moves
`pinned/<id>` back to `cache/<id>`, so the file stays (still readable) but becomes
evictable again. It is not a delete.

Unpinning is valid only on an `OnDemand` blob. A `Mirrored` blob's pin is a
*system* pin, re-asserted on every pull because the blob is part of having the
library, so unpinning one is meaningless and is rejected with an error rather
than silently skipped. The class is checked before any file is touched.

## The size budget

`pinned/` grows with what the user chose to keep and what the library mirrors;
`cache/` grows with what gets read. Left unbounded the evictable cache would grow
forever, so the host can set a per-device budget,
[`max_cache_size`](rustdoc:method:coven::database::Database::set_max_cache_size),
in bytes:

```rust
db.set_max_cache_size(2 * 1024 * 1024 * 1024).await?; // 2 GiB
```

The budget counts **only** the files under `cache/`. `pinned/` is structurally
exempt, because the eviction sweep never looks there: a pinned blob (user or
system) can never be evicted, whatever the budget. With no budget set
([`get_max_cache_size`](rustdoc:method:coven::database::Database::get_max_cache_size)
returns `None`) eviction is off and the cache is unbounded until the host opts
into a limit.

[`evict_to_budget`](rustdoc:fn:coven::blob::cache::evict_to_budget) runs
synchronously after every populate (a `read_blob` miss-write, or a
[`write_blob`](rustdoc:fn:coven::blob::cache::write_blob) stage). It sums the
`cache/` files and, if the total is over budget, deletes the oldest by
modification time until the total is back under it. Modification time is the
recency proxy: there is no `last_accessed` column (the same folder-truth tradeoff
the whole cache makes), so the oldest-written file goes first. Pinning, not access
tracking, is how a blob is kept.

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

## Staging and clearing

[`write_blob`](rustdoc:fn:coven::blob::cache::write_blob) stages host bytes into
`cache/<id>` so a later read serves them locally and a pin can promote them
without a cloud round-trip. This is for bytes a host already holds and is about to
upload (a release going from cloud-only to pinned, whose audio the host copies in
before the push reads it). It is not for a blob that stays external and is only
read as an upload source; see [local files](/docs/blobs#local-files).

[`clear_cache`](rustdoc:fn:coven::blob::cache::clear_cache) drops the whole
evictable cache in one sweep: it removes all of `storage/cache/` and leaves
`storage/pinned/` untouched. Every unpinned blob goes; a pinned blob survives
because it lives in the other folder. An absent `cache/` is not an error (nothing
has been cached yet); every other I/O failure is, because a swept directory must
actually be gone.

## At a glance

| | `storage/pinned/` | `storage/cache/` |
| --- | --- | --- |
| Holds | system-pinned `Mirrored` blobs, user-pinned `OnDemand` blobs | read-populated `OnDemand` blobs, host-staged bytes |
| Counts toward `max_cache_size` | no (exempt) | yes |
| Evicted by budget sweep | never | oldest-by-mtime when over budget |
| Cleared by `clear_cache` | no | yes |
| Populated by | `pin`, the pull's `Mirrored` download | `read_blob` miss, `write_blob` |
