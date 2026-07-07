# Cache

A blob lives in the cloud, encrypted. To read it (show a photo, play a track) a
device needs the plaintext bytes on local disk. The cache is where those bytes
live: a device-local store of blob files, keyed by blob id, that serves a read
from disk on a hit and fetches from the cloud on a miss.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

It is a separate layer from the [cloud blob lifecycle](/docs/blobs). That page
covers how a blob is uploaded, downloaded, and deleted across devices. This page
covers the one device's copy: where it sits, when it is fetched, how long it is
kept, and how a user pins a blob to keep it offline.

The cache is coven's, under the library directory. Examples: a todos app whose
`todo_attachments` rows each point at a photo; audio is the case the lazy half
exists for.

The cache holds **Remote** blobs only: bytes that also live in the cloud, so a
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

There is no cache table, because a table would be a second copy of the truth:
every crash between a file write and its row would leave the two disagreeing,
and the disagreement would surface as a phantom hit or a wasted re-download.
The file on disk *is* the presence record, and the folder it sits in *is* the
retention class. Nothing the two directory listings
can't answer, so there is no metadata sidecar to keep in step with the disk.
Every cache write is atomic
([`local_blob::write_atomic`](rustdoc:fn:coven::local_blob::write_atomic): write a
temp file, then rename it over the destination), so a crash mid-write cannot
leave a torn file that a later read would trust as whole. Pinning and unpinning
are a `rename` within `storage/` (one filesystem, atomic), so a blob is never
visible in both folders or in neither mid-move.


<svg class="flow" viewBox="0 0 660 170" role="img" aria-label="A Remote blob's cache state: absent, evictable in cache/, or protected in pinned/; fills, pins, unpins, and eviction move it between them">
<path class="arrd" d="M110 58 C 200 6, 460 6, 550 58" marker-end="url(#fam)"/>
<text class="sub" x="330" y="20" text-anchor="middle">pin fetches an absent blob straight into pinned/</text>
<rect class="chipd" x="30" y="64" width="165" height="30" rx="8"/>
<text class="lbl s11" x="112" y="83" text-anchor="middle">neither · remote-only</text>
<rect class="chip" x="250" y="64" width="165" height="30" rx="8"/>
<text class="lbl s11" x="332" y="83" text-anchor="middle">cache/ · evictable</text>
<rect class="chipa" x="470" y="64" width="165" height="30" rx="8"/>
<text class="lbl s11" x="552" y="83" text-anchor="middle">pinned/ · protected</text>
<line class="arr" x1="199" y1="72" x2="246" y2="72" marker-end="url(#fa)"/>
<text class="sub" x="222" y="60" text-anchor="middle">fill</text>
<line class="arrd" x1="246" y1="88" x2="199" y2="88" marker-end="url(#fam)"/>
<text class="sub" x="222" y="112" text-anchor="middle">eviction</text>
<line class="arr" x1="419" y1="72" x2="466" y2="72" marker-end="url(#fa)"/>
<text class="sub" x="442" y="60" text-anchor="middle">pin</text>
<line class="arrd" x1="466" y1="88" x2="419" y2="88" marker-end="url(#fam)"/>
<text class="sub" x="442" y="112" text-anchor="middle">unpin</text>
<text class="sub" x="330" y="140" text-anchor="middle">fill = eager pull, first read, or write · every move is one atomic rename or write</text>
</svg>

Cache files are plaintext. Encryption happens on the way to the cloud, not on
local disk; a blob comes back from the cloud decrypted and lands in the cache as
the bytes the host reads.

## Reading a blob

The host should never have to know where a blob's bytes are this minute
(a user file, the local store, a cache folder, or only the cloud); it asks for
the blob, and dispatch is coven's problem.
[`CovenHandle::read_blob`](rustdoc:method:coven::CovenHandle::read_blob) serves a
blob's whole contents.

```rust
let bytes = handle.read_blob(&blob).await?;
```


<svg class="flow" viewBox="0 0 660 158" role="img" aria-label="read_blob dispatches on the blob's provenance and locality: the user's file, the local store, or the cache with a cloud fetch on miss">
<rect class="chipa" x="255" y="14" width="150" height="28" rx="8"/>
<text class="lbl s11" x="330" y="32" text-anchor="middle">read_blob(blob)</text>
<line class="arr" x1="290" y1="46" x2="130" y2="76" marker-end="url(#fa)"/>
<line class="arr" x1="330" y1="46" x2="330" y2="76" marker-end="url(#fa)"/>
<line class="arr" x1="370" y1="46" x2="530" y2="76" marker-end="url(#fa)"/>
<rect class="chip" x="40" y="82" width="160" height="28" rx="8"/>
<text class="lbl s11" x="120" y="100" text-anchor="middle">the user's file</text>
<text class="sub" x="120" y="126" text-anchor="middle">Local · user-provided</text>
<rect class="chip" x="250" y="82" width="160" height="28" rx="8"/>
<text class="lbl s11" x="330" y="100" text-anchor="middle">the local store</text>
<text class="sub" x="330" y="126" text-anchor="middle">Local · host-provided</text>
<rect class="chip" x="460" y="82" width="160" height="28" rx="8"/>
<text class="lbl s11" x="540" y="100" text-anchor="middle">cache, else cloud</text>
<text class="sub" x="540" y="126" text-anchor="middle">Remote · pinned/, cache/, then fetch</text>
<text class="sub" x="330" y="150" text-anchor="middle">the blob's declared provenance and its gate decide the branch; nothing is probed</text>
</svg>

It resolves by where the bytes are, in order: a **user-provided Local** blob is
read from the user's own file (an external ref), a **host-provided Local** blob
from the local store, both with no cloud fallback. Otherwise the **cache**:
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
  fetch. It lands in the **evictable** `storage/cache/<namespace>/<id>`; it is not
  pinned, so if it later falls out of its namespace's budget it shows a placeholder
  until the next read re-fetches it.
- `CacheLazy`: skipped on pull. A pulling device does not fetch it up front; the
  first `read_blob` does, populating `cache/<namespace>/<id>`. This is for large
  blobs a device may never open, audio being the motivating case.

Both fills cache evictably; the difference is only *when* the bytes arrive (on pull
vs. on first read). Neither is pinned automatically; pinning is a separate, manual
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
