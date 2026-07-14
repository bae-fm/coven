# Bootstrap

A device that joins or restores a store needs the current shared database without
replaying every retained Store commit. Coven publishes signed database snapshots
with exact commit coverage. The new device installs one snapshot, then pulls the
commits beyond that coverage.

## Snapshot contents

[`create_snapshot`](rustdoc:fn:coven::sync::snapshot::create_snapshot) uses
SQLite `VACUUM INTO` to copy the live database. It then removes:

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
store-v1/snapshot-images/{author}/{image_hash}/copies/{copy_id}.db
store-v1/snapshots/{author}/{snapshot_hash}/copies/{copy_id}.json
```

Before storage I/O, coven commits the exact image bytes, metadata bytes, image
hash, and snapshot hash to its durable snapshot publication record. It appends
and reads back an image copy, then appends and reads back the metadata copy.
Only after both verify does local completion clear that publication record and
record the snapshot hash and coverage.

A failed or lost storage response leaves the exact publication record intact.
Retry appends another physical copy of the same semantic image and metadata;
different valid bytes at the same semantic identity are rejected.

Only a current Owner can publish snapshot metadata. The signature binds the
metadata to the signed Store protocol root, image hash, coverage, schema version,
author, and creation time.

## Selecting a snapshot

[`bootstrap_from_snapshot`](rustdoc:fn:coven::sync::snapshot::bootstrap_from_snapshot)
starts from the expected Store protocol root hash, founder key, and membership
floor carried by the invite or restore code. It:

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
single-use [`BootstrapResult`](rustdoc:struct:coven::sync::snapshot::BootstrapResult).
The result binds the store id, destination path, image hash, Store protocol root,
snapshot hash, and exact coverage. Its fields are private and it cannot be
cloned.

Consuming it through `BootstrapResult::open_database` rechecks the destination
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
- A binary below the snapshot version refuses it with
  [`SnapshotError::SchemaTooNew`](rustdoc:enum:coven::sync::snapshot::SnapshotError)
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
hash-mismatched acknowledgement, or partial physical-copy deletion refuses the
reclamation and reports no package reclaimed. Packages beyond the proven
coverage remain.
