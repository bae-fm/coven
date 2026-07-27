# Chunked blob ranges: decryption is the verification

## Status

Built, and the playback receipt landed on the bae side: bae's
`seek_over_remote_cloud_costs_chunks_not_the_whole_blob` (commit 27794490)
proves a seek over a remote blob transfers the covering chunks, not the
object. Restores O(range) remote blob reads by making the AEAD chunk the
unit of verification, and strips read-path proof from local sources
entirely. Supersedes the whole-file-prove-once stream design for remote
ranges (13cb4d6), which fixed per-range O(blob) but still downloads and
hashes the whole object once per stream. History: coven once had chunked
range reads (`BlobRangeReader`/`RangeEncryption`, deleted as orphaned by
7dd8dd9's whole-file-exactness unification — recover intent from git
history, not the code shape).

## The verification model, by source

- **Remote (cloud) blobs**: sealed per-chunk with the AEAD
  (XChaCha20-Poly1305). A chunk that decrypts is authentic — the provider
  holds no keys and cannot forge a tag. Range reads fetch and open only
  the chunks covering the range. No whole-file hash on the read path.
- **Local sources (external user files, local-store copies)**: reads are
  plain reads — open, positioned read, no proof. The user's own file's
  current bytes are the correct answer to a read. Integrity has exactly
  one home: **publication** (`make_remote` materialization), where the
  bytes must match the row's declared hash before becoming canonical
  synced content — that check exists and stays.

## Design

### 1. Sealed chunk format

Plaintext is split into fixed-size chunks (last one short). Each chunk is
sealed independently; ciphertext layout is a small header followed by
concatenated sealed chunks:

- Header: version, chunk_size, plaintext_len (authenticated — seal it or
  bind it as AAD into every chunk so a tampered header fails the first
  open).
- Chunk N's nonce derives from the blob's key material + N (no stored
  nonces). Chunk N's ciphertext offset = header_len + N × (chunk_size +
  TAG_LEN). All chunk arithmetic is computed, nothing per-chunk persisted.
- Cross-blob and cross-position splicing must fail: bind blob identity
  and chunk index into the AAD, so chunk N of blob A refuses to open as
  chunk M of blob B.

### 2. Chunk size

Default **64 KiB**. A seal-time parameter recorded in the header — readers
honor whatever the blob says, so mixed sizes coexist and the default can
change without migration. Host-configurable through the coven builder
(per-installation). The fetch window (how many bytes one cloud request
spans — many chunks) is a separate reader-side runtime knob; requests span
chunks, so throughput is governed by the window, latency-to-first-byte by
the chunk.

### 3. Read path

`BlobStream` keeps its API (`read_at`, `plaintext_size`) with new
internals per source:

- Remote, cached: the cache holds plaintext today — cached reads are
  local-source reads (plain). Unchanged disposition.
- Remote, uncached: `read_at` computes covering chunks, issues one ranged
  cloud read for the span (`CloudHome::read_range` — its caller returns;
  supersedes task #33), opens each chunk, serves the range. Optionally
  populate the cache with opened chunks as they accumulate — but never
  require the whole object before serving a range (that is the point).
- Local (external / local-store): open once, positioned reads, no
  hashing, no prove-once pass.

### 4. Write path

Blob upload seals chunks streaming (the multipart upload machinery already
streams; seal per chunk as parts are produced). The exact object ref
(size+hash of *ciphertext*) continues to pin the stored object identity
for slot/object bookkeeping — computed over the sealed stream during
upload as today.

### 5. What the read path stops doing

- No whole-file SHA-256 on any read.
- No `OpenExactFile` prove-once pass for local streams (delete it and its
  counters; the invariant flips — see Tests).
- The publication-time hash check is untouched and remains the only
  plaintext-hash verification.

## Compatibility

Greenfield wire policy per repo convention: no compatibility readers for
the current whole-object sealed format. Existing stored blobs re-seal on
next publication or via re-upload; if the repo's release posture requires
reading old objects, that is a product decision to surface, not silently
build.

## Tests

1. **O(range) receipt (remote)**: N small reads across one uncached
   stream transfer only covering-chunk bytes (storage fake counts request
   spans) — no whole-object download, ever. Sabotage: reintroduce a
   whole-object fetch → fails.
2. **Zero-proof receipt (local)**: reads on local sources perform zero
   hash scans (flip the existing counting test's assertion; opens stay
   1 per stream).
3. **Tamper**: flip a byte in one stored chunk → exactly the reads
   touching that chunk fail typed; other ranges still serve. Header
   tamper → first open fails.
4. **Splice**: swap chunk N of blob A into blob B / move a chunk to a
   different index → refused (AAD binding).
5. **Boundary sweep**: reads straddling chunk boundaries, 1-byte reads,
   tail reads, whole-file read equals the plaintext, empty range.
6. **Mixed chunk sizes**: a 64 KiB blob and a 4 MiB-chunk blob coexist;
   readers honor each header.
7. **Publication check unchanged**: tampered local file still refused at
   make_remote (existing test keeps passing).
8. **Playback end-to-end (bae side)**: the playback trio stays green; seek
   time-to-first-byte over the fake cloud is bounded by one chunk + RTT, not
   file size. Landed as `seek_over_remote_cloud_costs_chunks_not_the_whole_blob`.

## Deviations, as built

Four departures from the design above, each ratified:

- **A remote miss populates nothing.** The plan left cache population optional
  ("optionally populate the cache with opened chunks"); the reader does not
  populate at all. Populating means holding the whole blob, which is the
  download a ranged read exists to avoid — a stream that asks for a kilobyte
  must not pay for the object. `read_blob` still populates, because reading
  every byte is what it does anyway.
- **A blob stored in the clear refuses ranged reading.** The verification model
  above reads as if every remote blob were sealed, but a **browsable** home
  stores plaintext verbatim and carries no tags, so a range there has nothing to
  check the provider's answer against. Rather than serve unverified bytes,
  `open_blob_range_reader` refuses a browsable locator and the stream falls back
  to whole-object materialization, where the row's content hash still applies.
  Per-source verification stories are the point of this design, so the third
  source gets its own stated story instead of silently inheriting the sealed
  one. Receipt: `a_blob_stored_in_the_clear_refuses_ranged_reading`.
- **The whole-blob reader keeps its content hash for browsable homes only.**
  Same reason: for a sealed blob the AEAD is the whole verification and the
  SHA-256 was a second mechanism for one guarantee, so it went; for a plaintext
  home it is the only mechanism, so it stays.

- **A whole read still hashes a cache hit.** `read_blob`'s cache-hit path checks
  the cached file's size and content hash against the row unconditionally, which
  the "no whole-file SHA-256 on any read" line above would forbid. Kept, because
  it answers a different threat than cloud authenticity: the AEAD settled that
  when the bytes were fetched, but a cache file is unsealed plaintext sitting on
  local disk, carrying no tags of its own — so against rot, a truncated write, or
  an edit, the row's hash is the only thing that can refuse it. This is exactly
  the browsable story applied to the cache copy: wherever bytes rest
  unauthenticated, the row's hash is the check. It is affordable only because a
  whole read touches every byte regardless; the ranged path over the same cache
  file deliberately does not hash, since re-hashing per range is the scan the
  stream exists to avoid. The carve-out is stated in `blob/cache.rs`'s module
  doc.

Deleted as orphaned once blobs left the whole-object format:
`decrypt_range_with_offset`, `decrypt_chunk`, `encrypted_chunk_range` and their
self-tests; `ChunkSealer` narrowed to `encrypt`'s own use. The whole-object
chunked format itself stays for protocol objects and sealed app data — those are
always read whole, so they need no header describing their framing.

## Out of scope

- Per-chunk plaintext hashes / Merkle identities (AEAD tags carry
  integrity; a second mechanism for the same guarantee is the smell).
- Changing snapshot/package object formats — this is the blob namespace
  only.
- The reader-side prefetch policy beyond the fetch-window knob.
