# Protocol payload storage

## What a payload is

A protocol payload is bytes a coven bookkeeping row owns: a captured changeset,
an audience partition, a snapshot image and its prepared ciphertext, a
membership rollup, a retained replay baseline image and its canonical authority,
a Circle bootstrap image, a remote object's plaintext and ciphertext, the
plaintext an audience move captured. Unlike a host blob, a payload is never
leased, never packaged for an audience, and never evicted: it exists for exactly
as long as a durable row names it.

This is not the host blob layer and not the playback blob cache. Those keep
their own lifecycles.

## The shape

One representation, one resource, one transaction.

```
payload_storage(payload_hash PK, payload_size, compressed_size, chunk_count)
payload_chunks(payload_hash, ordinal, bytes, PK (payload_hash, ordinal),
               FOREIGN KEY (payload_hash) REFERENCES payload_storage(payload_hash)
                   ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)
payload_owners(payload_hash, owner_key, PK both)
```

A payload is its `payload_storage` row and the `payload_chunks` rows that row's
`chunk_count` names. Every payload is compressed, and its compressed frame is
written one bounded row at a time as it is produced, so a payload larger than
memory is never in memory: the encoder's block is pinned to 64 KiB and each
chunk row is 64 KiB, so a writer holds a block and a chunk regardless of what it
is compressing. Reading assembles chunks back in ordinal order through a
`std::io::Read`, one chunk at a time, so a bounded copy out stays bounded.

`chunk_count` and `compressed_size` are what a reader checks a chunk set
against before it decodes anything, so a lost, extra or resized chunk fails the
read instead of feeding the decoder bytes no writer produced. The reference from
a chunk to its catalog row is deferred because a payload is compressed as it is
read — its chunks land before its final length and count are known — so the
check moves to the commit, where a writer that never settled its payload fails
the transaction it wrote into rather than committing bytes no row names.

### Identity comes from the owner, not from the write

A streaming writer needs the payload's address before its last byte is read, and
every streaming caller already holds it: a blob fact carries
`plaintext_hash`/`plaintext_size`, snapshot metadata carries the signed
`image_hash`, and a byte-slice installation has its input in hand. So the writer
takes the expected identity, writes chunks under it, and verifies the digest it
computed against it at settle time. Knowing the address is not permission to
skip the check — a source that changed under the writer still fails, and the
failure fails the enclosing transaction, which is the only place the chunks
exist.

A payload already in the catalog is still read and verified: its bytes are
hashed and dropped rather than rewritten, so re-retaining a source neither
replaces committed chunks with a differently framed compression of the same
content nor stops checking what it was handed.

### Lifetime

Rows of different kinds can name the same payload — a Circle operation and the
remote object it prepared both need one object's bytes — so a row does not
delete the storage it is done with. It replaces its whole claim set with
`set_payload_owner_claims_on`, called in the transaction that writes the row
holding the claim; the whole set is replaced rather than one hash added or
dropped because the flows that rewrite a journal in place carry one owner key
across both the payloads they drop and the payloads they take on, and a payload
named by both must not pass through a moment of being deleted.

The transaction that drops the last claim deletes the payload, chunks and all,
in that same transaction. There is no deletion obligation to record, no
post-commit pass to discharge it, and no window in which a row names storage
that is gone. A payload exists exactly while a durable owner claims it because
those are the same commit.

### Images carry rows that name payloads, never payloads

A serialized database image that travels — a published Store or Circle snapshot,
a private retained replay baseline — carries the bookkeeping rows that name
payloads and none of `payload_storage`, `payload_chunks` or `payload_owners`.
The device reading an image resolves those names in its own catalog; an image
that carried its own enclosing payload history would nest every predecessor
image inside its successor, once per capture.

Three producers clear the payload tables and one validator refuses an image that
carries them:

- `ReplayProjection::capture_replay_baseline` clears them in the transaction it
  already holds, before serializing.
- `install_snapshot_replay_baseline_records` and `replace_retained_replay_image`
  serialize the live connection, so they go through
  `replay_baseline_image_of`, which copies into memory, clears, vacuums and
  serializes.
- `snapshot_image::project` already clears every table that is neither synced
  nor snapshot-preserved, which is where the payload tables fall.
- `RetainedReplayBaseline::validate_open_image` refuses an image with any
  payload row, and every install path runs it before committing the baseline.

`circle_bootstrap_coverage` states the same contract from the other side: it
travels inside images, and `clear_imported_circle_bootstrap_coverage` refuses an
imported row that arrived carrying payload claims.

## How it got here

The first version of this work moved payload bytes out of SQLite rows, where
they had been stored as BLOBs or as `Vec<u8>` serialized into JSON number arrays
(~4x inflation) and rewritten whole on every journal progress step. Measured
2026-08-07: `coven-replication` burned 1,180s of the suite's 1,308s CPU, almost
entirely in serde_json; one 18s Circle test round-tripped 346MB of JSON. The
offenders were the Circle operation journal, outbound snapshot staging, retained
replay baselines, Circle bootstrap coverage, remote object state, Store write
changesets and partitions, and the membership mutation journal. Two findings
from that arc still bind:

- **Derivable bytes are never stored.** Five journal fields that were re-encodings
  of a typed field sitting beside them became `ExactObjectRef`s and are rebuilt
  at egress, where a mismatch fails loud. `RemoteObjectBytes` was deleted rather
  than converted.
- **Verification happens at ingestion and at egress, not on routine reads of
  local durable state.** Untrusted data is verified when it is parsed and again
  before it leaves the device; re-parsing and re-verifying a signed object on
  every local load was the remaining CPU sink and was removed.

Those payloads first went to content-addressed files in `spool/payloads` beside
the database. That worked, and it bought three independent rules that this
version removes: choosing file versus inline storage by compressed size;
publishing a filesystem object before its owning SQL transaction commits, which
needed out-of-band tracking of created files so a rollback could unpublish them;
and a post-commit physical-deletion obligation discharged after every Store call
and at open. It also made a store a directory rather than a file — a
`VACUUM INTO` copy of the database named payloads that were not there.

The profile that closed that arc is why the file layer is gone rather than
merely tidied: under `fcntl`, payload-spool `File::sync_all` and parent-directory
syncs accounted for 1,385 samples against **14** for SQLite's own commit fsync in
the same run. Each payload write paid two macOS `F_FULLFSYNC`s while the row
naming it committed at a weaker tier, so the extra strength bought nothing.
Payload bytes now commit with exactly the durability of the row that names them.

## Measured (2026-09-17, five write/snapshot/baseline-advance cycles)

Before the move, over five cycles of three 256 KiB writes, a snapshot capture and
publication, and a baseline advance:

- the spool grew from 7 files/5.9 MB to 31 files/24.1 MB, adding seven payloads
  and removing exactly one per cycle;
- the one removed each cycle was the superseded replay baseline image, deleted in
  the turn that released it, and no payload was ever left owed a deletion;
- the private baseline image carried three more `payload_storage` and
  `payload_owners` rows per cycle, without bound — harmless only because every
  one of them was a file reference carrying no bytes.

So growth was, and remains, the growth of the durable owner set: published
snapshot images and their prepared ciphertexts, membership rollups, retained
materialization changesets, and one baseline. The lifecycle contributes none of
its own.

The database has `journal_mode = wal`, `synchronous = FULL` and no `auto_vacuum`,
so pages a payload deletion frees are reused but the file does not shrink: the
file's high-water mark is now the peak concurrent payload set. This is deliberate.
An incremental vacuum after a payload deletion would be a post-commit obligation
with a window — the shape this design exists to remove — and the space is reused
rather than lost.

## Durability contract

Coven rides SQLite's `synchronous = FULL` with `journal_mode = wal`; the journal
bridge (atomic local intent, then idempotent remote steps) depends on
commit-means-on-disk. `PRAGMA fullfsync` is deliberately not set: the stance is
durable-to-OS-crash, and a genuine power cut can lose the WAL tail (checksummed,
so lost rather than torn). If rung-3 durability is ever wanted, the shape is a
full-flush barrier before an operation's first external upload, not a per-commit
fullfsync.

## Out of scope

Host blobs and their leases, packaging, eviction and local/remote transitions;
the playback blob cache; streaming an audience move's download straight into the
payload writer instead of through a scratch file in `spool/blob-moves`.
