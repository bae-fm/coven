# Coven blob engine

Coven owns **all** blob functionality for the host. The host says which rows
carry files and picks a retention class per blob; coven does everything else —
upload, the prefetch/on-demand split, the local cache, pinning, eviction, ranged
streaming, decryption, delete, and GC. The host never touches the cloud, the
cipher, byte ranges, or a cache.

This collapses coven's two present blob mechanisms (`BlobPlan` + the imperative
outbox) into one, and pulls bae's local cache and its streaming/decryption down
into coven so **any** coven host reuses them.

## Three layers

Keeping these separate is the point.

1. **Cloud store** — `(cloud_key, scope) ↔ bytes`. Seal/upload, download/decrypt,
   delete. Raw cloud bytes. (Mostly exists today.)
2. **Cache** — "blob bytes, locally." `get / put / pin / unpin / evict / stream`
   keyed by blob id, backed by disk; on a miss it calls layer 1. Knows nothing
   about rows or releases — only bytes, recency, and a pinned flag. **Orthogonal**
   to the declarations.
3. **Lifecycle / declarations** — rows→blobs (`BlobSource`), upload-on-push
   (gated), prefetch/on-demand-on-pull, delete-on-row-delete, GC. Drives *what*
   blobs exist and *when* to cache/pin them. **Uses** layer 2; never reimplements it.

The lifecycle uses the cache through its API; that is the only coupling.

## Retention model — two declared classes + a per-device pin

This is the load-bearing distinction (and the one the first draft muddled).

- **`Mirrored`** (declared per blob, all devices) — prefetched on pull on every
  device and kept local, never evicted. It's part of "having the library."
  Example: cover art.
- **`OnDemand`** (declared per blob, all devices) — not fetched on pull; fetched
  on first read into the cache; LRU-evictable. Example: audio.
- **`pin`** (per device, applies only to `OnDemand`) — keep this OnDemand blob
  local *here*, exempt from eviction. The user's "download for offline." Pinning a
  `Mirrored` blob is meaningless — it's already always-kept.

Why two concepts and not just "pinned": **scope.** `Mirrored` is *declared,
global, automatic* — every device prefetches+keeps it from the blob's policy
alone. `pin` is *per-device, manual*. Device B deciding whether to prefetch a
cover during sync can only read the blob's declared class — it can't see that
device A pinned it. So "mirror this everywhere" must be a declared property, not a
local pin.

At the **cache** there is one retention flag, `pinned`, with two sources: the
lifecycle pins `Mirrored` blobs when it prefetches them (a system pin, not
user-removable); the user pins `OnDemand` blobs on demand. The cache doesn't care
which — it evicts unpinned. So the *concept* split is at the declaration layer;
the *mechanism* (a kept-local file) is one thing in the cache.

## Host interface

```
trait BlobSource {                                  // host implements
    fn blobs_for_change(&self, change: &RowChange) -> Vec<BlobRef>;
    fn blobs_in_db(&self, conn) -> Vec<BlobRef>;     // enumerate all: backfill, GC
}

BlobRef { namespace, id, scope, cloud_path, sync: Mirrored | OnDemand }
```

The class is the only blob knob the host turns. There is no imperative
`enqueue_upload`/`enqueue_delete` — upload and delete intent are **derived from
row state**, which makes the outbox-reorder bugs (#96/#97) unrepresentable.

## Cloud store (layer 1)

Exists today, kept: seal under the blob's scope (`Master` / `Derived` / per-item
`Item`), upload/download by cloud key (hashed or readable), decrypt on read,
delete by key. The other layers call into this.

## Cache (layer 2) — disk is the source of truth

The cache holds **only cloud-durable blobs**, so everything in it is
re-fetchable. That makes the disk authoritative and keeps this layer simple.

- **Presence = the file on disk.** `storage/<id>` exists ⇒ cached. A read opens
  the file; absent ⇒ fetch from cloud, decrypt, write, serve. No table decides
  presence.
- **Writes are atomic** — temp → fsync → rename — so a crash can't leave a
  half-written file the read would trust.
- **A metadata sidecar** `blob_cache(blob_id, pinned, last_accessed_at,
  size_bytes)` holds only what the disk can't: `pinned` (policy) and
  `last_accessed_at` (atime is unreliable). **Best-effort, not the presence
  authority** — a row for a missing file is harmless (re-fetch); a file with no
  row is treated as unpinned. Disk wins; no row⟺file invariant to maintain.
- **Read / populate** — a hit serves the file and bumps `last_accessed_at`; a
  miss fetches+decrypts, writes the file (full-length only — a ranged read serves
  its range but never caches a truncated file), and serves.
- **Eviction** — `max_cache_size` counts **only unpinned** bytes. Over budget →
  drop unpinned files LRU until under it. `clear_cache()` drops all unpinned.
  Because the cache holds only re-fetchable blobs, the only rule is **"never evict
  pinned."**

### Pinning — first-class, in the cache

The host *invokes* pinning; the cache implements all of it.

- **`pin(ids)`** — ensure local **and** protected: if not cached,
  fetch+decrypt+write first, then set `pinned`. A pin *populates*; not a flag
  flip. Idempotent.
- **`unpin(ids)`** — clear `pinned`; the file stays, now evictable. Not a delete.
  (Only valid on `OnDemand` blobs; a `Mirrored` blob's system pin isn't
  user-removable — the lifecycle re-asserts it each pull.)
- **Budget-exempt** — pinned bytes don't count toward `max_cache_size`, so a pin
  never evicts others and always succeeds.

### Not the cache: the upload-source

A blob whose only durable copy is the local file (just imported, not yet
uploaded) is **not** a cache entry — losing it loses data. That's the host's
authoritative source (bae's `unmanaged_source`); the upload reads from it. Its
safety is the lifecycle's concern, not an eviction rule.

## Streaming / decryption / range (layer 2)

`read_blob(id)` (whole file) and `open_blob_stream(id, range)` (ranged, for
playback/seek) decrypt and slice **in coven**, next to the cipher and scope
resolution it already holds. bae's cloud reader / ranged-playback path is deleted
and replaced by calls to these. Any coven host gets streaming + decryption free.

## Lifecycle (layer 3)

- **push** — for each pushed row, `blobs_for_change` → seal + upload each blob
  (background, retried). The row is **gated** (not durable/shareable) until its
  blobs are durable in the cloud (#83).
- **pull** — `Mirrored`: download+verify+fsync **before** the row is applied,
  then pin (#111). `OnDemand`: record presence; download nothing.
- **delete** — a row delete whose blob no live row still references deletes the
  cloud object **and** the cache entry — reference-checked so an offline/peer
  device isn't stranded (#96), ordered after any in-flight upload of the same key
  (#97).
- **GC / backfill** — `blobs_in_db` drives orphan GC (delete cloud objects no row
  references, #115) and snapshot-bootstrap backfill (fetch+pin the `Mirrored`
  blobs a freshly bootstrapped catalog references **before** it goes live — the
  deferred #111 case).

## Coven / bae seam

**Coven (the engine):** the cloud store, the cache, pinning, eviction,
read/stream/decrypt, and the whole upload/download/delete/GC lifecycle.

**bae (domain):**
- `releases.managed` — "this album's files are all durable in the cloud." An
  aggregate over per-blob durability; bae rolls it up.
- `release_unmanaged_source` — the user's own files imported in place (the only
  copy; the upload-source), bae's import model.
- the `Mirrored`/`OnDemand` choice per blob, *invoking* `pin`/`unpin` from a
  per-release action, and *deriving* its display state from the engine's `pinned`
  flags. The aggregate and the display are bae's; the mechanism is the engine's.
- the domain half of the read rule: "unmanaged source for this release? → read the
  user's file; else → `engine.read_blob` / `open_blob_stream`."

## Invariants

- A blob exists in the cloud ⟺ a live row references it (upload-on-push,
  delete-on-orphan, derived from rows). So #96/#97 are unrepresentable.
- A row is never durable/visible before its blobs are durable (push gate #83;
  Mirrored download-before-apply #111; snapshot backfill before catalog-live).
- The cache holds only cloud-durable (re-fetchable) blobs; the disk is its source
  of truth; the only eviction rule is "never evict pinned".
- `Mirrored` ⟹ local on every device (system-pinned). A user `pin` ⟹ that
  `OnDemand` blob is local here. Pinned bytes are budget-exempt.

## Issues absorbed

- Done (the prefetch half): #83 (push gate), #111 (pull download-before-apply).
- Folded in: #96 (reference-checked delete), #97 (row-derived single-op-per-key),
  #112/#113 (push/blob lifecycle), #115 (orphan GC), the deferred #111
  snapshot-bootstrap backfill, and bae's cache + streaming.

## Implementation phases

Each phase is shippable; greenfield, so change the shape + every caller per phase,
no migration shims. Phases 1 and 6 span both repos (the trait change breaks bae's
`blob_plan.rs`, which moves with it).

**1. Lifecycle unification.**
- Add `BlobRef.sync: Mirrored | OnDemand`; define the `BlobSource` trait
  (`blobs_for_change` + `blobs_in_db`), replacing `BlobPlan`.
- Route push uploads through one engine path derived from the pushed row-changes
  (folding in `blobs_to_push` + the outbox `enqueue_upload`); keep the push gate
  (#83). Route deletes from row deletes (folding in the outbox `enqueue_delete`).
- bae's `blob_plan.rs` implements `BlobSource`: images `Mirrored`, audio
  `OnDemand`; its `add_cloud_outbox_upload`/`delete` calls disappear.
- Behavior parity: `OnDemand` reads fetch-each-time for now (no cache yet).
- Verify: existing image-sync + audio-upload tests pass through the new path.

**2. Cache layer.**
- `blob_cache(blob_id, pinned, last_accessed_at, size_bytes)` device-local table;
  `storage/<id>` files; disk-as-truth presence; atomic temp→fsync→rename writes.
- `read_blob(id)`: hit (bump `last_accessed_at`) or fetch+decrypt+populate
  (full-file only).
- `pin(ids)`/`unpin(ids)`: pin populates then flags; unpin clears (file stays).
- `Mirrored` blobs system-pinned on pull (the lifecycle calls `pin`).
- Verify: a second read is a local hit; a pinned `OnDemand` blob stays after a
  cache sweep stub.

**3. Streaming / decryption extraction.**
- `open_blob_stream(id, range) -> ByteStream` in coven (decrypt + slice over the
  cipher/scope it holds); `read_blob` for whole-file.
- bae's cloud reader / ranged-playback path deleted; playback calls
  `open_blob_stream`. A ranged read never writes a truncated cache file.
- Verify: seek/partial playback reads decrypt correctly; full reads populate.

**4. Eviction.**
- `max_cache_size` setting (bytes); accounting sums **unpinned** `size_bytes`.
- LRU sweep over unpinned by `last_accessed_at` until under budget, deleting file
  (+ sidecar row); `clear_cache()`. Trigger synchronously after each populate.
- Verify: over-budget populate evicts the LRU unpinned entry; pinned + `Mirrored`
  untouched; budget never drifts.

**5. Safe delete + GC + snapshot backfill.**
- Reference-checked delete: an orphaned blob (no live row references it) is
  deleted from cloud + cache, gated by a refcount/tombstone-grace so an offline
  device's reference isn't stranded (#96), ordered after any in-flight upload of
  the same key (#97).
- Orphan GC over `blobs_in_db` (#115).
- Snapshot bootstrap fetches+pins the `Mirrored` blobs the catalog references
  before it goes live (closes the deferred #111 case).
- Verify: deleting a row removes its blob only when unreferenced; a bootstrapped
  catalog has no `Mirrored` blob missing.

**6. bae rewire.**
- bae's `blob_plan.rs`, outbox calls, `file_cache`/`storage/`, eviction, pinning,
  and cloud reader all delegate to the engine.
- bae keeps only `releases.managed`, `release_unmanaged_source`, the
  `Mirrored`/`OnDemand` choice, invoking `pin`/`unpin`, and deriving display state.
- Verify: import/manage/pin/unpin/unmanage transitions still land valid storage
  states; cloud-only import is playable in place.

**7. UI wiring.**
- `max_cache_size` setting + "clear cache" action + any new state surfaced across
  macOS / iOS / Android / Windows.

## Open decisions

1. **`Mirrored` blob location** — in `storage/<id>` (system-pinned) or a host-named
   permanent path? Leaning: one store, system-pinned.
2. **Reference source for safe delete** — refcount the engine maintains from
   `blobs_for_change`, or a host query; how an offline device's reference is
   respected (#96) — likely a tombstone/grace window, not an immediate delete.
3. **Push gating** — keep bae's per-row gate column, or the engine owns the gate
   (affects who flips `releases.managed`).

## Where the rest of the coven issue sequence fits

- Blob-lifecycle issues land **on** this engine (phase 5): #96, #97, #112, #113,
  #115, deferred #111 snapshot-bootstrap.
- Non-blob issues proceed independently around it: #85, #86, #87, #89, #90, #91,
  #92, #93, #94, #98, #99, #100, #106, #107, #109, #110, #114, #116.
