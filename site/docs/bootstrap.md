# Bootstrap

A device that joins or restores a store needs the current shared database without
replaying every retained Store commit. Coven publishes signed database snapshots
with exact commit coverage. The new device installs one snapshot, then pulls the
commits beyond that coverage.

## Snapshot contents

Snapshot capture uses SQLite `VACUUM INTO` to copy the live database. It then removes:

- rows from tables the host did not declare as synced;
- coven's device-local bookkeeping rows;
- gated-false roots and every descendant whose sharing follows that gate.

The copy retains the application schema and `user_version`. Its metadata names
the schema version, SHA-256 image hash, author, Store protocol root, creation
time, and an exact map from every covered device id to its sequence and commit
hash. A snapshot therefore claims only rows that are present in the image and
only Store history the image has fully materialized.

The database image contains catalog rows, not their blob files. Bootstrap
downloads every referenced `CacheEager` blob before accepting the store;
`CacheLazy` blobs remain fetch-on-read.

## Publication

Snapshot images and signed metadata use content-addressed append-only Store
paths:

```text
store-v1/snapshot-images/{author}/{image_hash}.db
store-v1/membership-rollups/{author}/{rollup_hash}.json
store-v1/snapshots/{author}/{snapshot_hash}.json
```

Before storage I/O, coven commits the exact image bytes, membership rollup
bytes, metadata bytes, image hash, and snapshot hash to its durable snapshot
publication record. It creates and reads back the exact image object, then the
rollup the metadata names, then the exact metadata object. Only after all of
them verify does local completion clear that publication record and record the
snapshot hash and coverage.

A failed or lost storage response leaves the exact publication record intact.
Retry reuses its reserved exact image and metadata objects; different bytes at
either occupied identity are rejected.

Only a current Owner can publish snapshot metadata. The signature binds the
metadata to the signed Store protocol root, image hash, coverage, schema version,
author, and creation time.

## The membership rollup

A joining device opens the store keyring out of the membership chain, so the
chain is the first thing it reads and none of it can be encrypted under the key
the chain is what unlocks. Read change by change, that cost two provider round
trips per membership change the store has ever made — a head, then the entry it
selects — back to the founding entry, and on a real store it was most of a
device join.

Each snapshot therefore publishes a **membership rollup** beside its image: one
signed, plaintext object holding every membership head and entry up to the
frontier the snapshot pins, plus the conflict resolutions they depend on. A
reader takes the rollup in one read and then walks the provider only for what
was published after the snapshot, so what it spends on membership stops growing
with the store's membership history.

The rollup carries objects, not conclusions. Every head in it goes through the
identical check a head read off the provider goes through — the coordinate its
slot claims, its author's registration, its own signature — and a head that
fails refuses the whole rollup. Predecessor linkage, grant authority, the Store
commit that activates an authority change, and conflict-resolution layering are
all still decided afterwards by the same anchored walk. A reader that finds no
rollup, or one that does not verify, walks the chain exactly as it did before.

A membership head lives at a create-once slot, so which of an author's heads
sits at a sequence is settled by the provider rather than by whoever describes
it. The newest head of each stream is therefore left out of what a rollup stands
in for: the reader reads that one from its own slot, and its signed predecessor
names the head below it, whose entry names the hash of the one below that, down
to the sequence-one slot the signed Store protocol root names. One read per
stream pins every head under it, so a rollup cannot show a reader one branch of
a forked author stream while the provider holds the other.

## Selecting a snapshot

`PreparedSnapshotBootstrap::prepare` starts from the expected Store protocol
root hash, founder key, and membership
floor carried by the device invitation or restore code. It:

1. Loads and verifies the exact Store protocol root.
2. Loads the founder-anchored membership chain at the supplied floor.
3. Lists snapshot metadata and keeps only valid metadata signed by a current
   Owner under that root.
4. Selects a snapshot whose exact coverage is not dominated by another
   authorized snapshot; equal maximal candidates resolve by semantic hash.
5. Refuses metadata whose schema version is newer than the binary supports.
6. Loads the content-addressed image and verifies its bytes against the signed
   image hash.

No mutable current-snapshot pointer chooses authority. Snapshot selection comes
from signed coverage and verified append-only objects.

## Installing coverage

The downloaded image and its signed coverage stay bound together through a
single-use `PreparedSnapshotBootstrap`. The value binds the destination path,
image hash, verified Store protocol root, snapshot hash, and exact coverage.
Its fields are private and it cannot be cloned.

Consuming it through `PreparedSnapshotBootstrap::install` rechecks the destination
and image bytes, opens the database with the application's normal migration
ladder and synced-table declarations, then installs the protocol root, snapshot
hash, and every exact covered position in one SQLite transaction. An invalid row
identity, migration failure, changed image, wrong store, or wrong destination
removes the incomplete database and returns the error.

The installed `snapshot_coverage` is a signed base, not fabricated
`materialized_commits` rows. Pull accepts a dependency at or below that base only
when retained signed commit ancestry proves its exact sequence and hash is
covered. Commits accepted after bootstrap create factual materialized ledger
rows.

Join and restore then pin the owner and membership watermark, download required
eager blobs, publish this device's registration, and pull every commit beyond
the installed coverage. The store is returned only after that work succeeds.

## Schema versions

The snapshot image carries its SQLite schema and the signed metadata repeats its
schema version before download:

- A binary at or above the snapshot version opens the image and runs the same
  migration ladder used by an existing device.
- A binary below the snapshot version refuses it with `SnapshotError::SchemaTooNew`
  before installing the database.

See [Schema evolution](/docs/schema-evolution) for live-commit version handling.

## Reclamation

A snapshot does not authorize package deletion by itself. Reclamation requires:

- a verified snapshot image and signed coverage;
- a complete membership and device-registration view;
- a valid signed acknowledgement chain for every device still obligated to
  cover the package;
- exact commit ancestry proving each acknowledgement covers the snapshot
  position; and
- complete package listing and deletion results.

An incomplete listing, unreadable candidate, fork, missing predecessor,
hash-mismatched acknowledgement, or partial exact-object deletion refuses the
reclamation and reports no package reclaimed. Packages beyond the proven
coverage remain.
