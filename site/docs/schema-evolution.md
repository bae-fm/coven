# Schema evolution

Devices that share a library do not all run the same app version at once. Someone
updates their phone on Monday and their laptop the following week; in between, two
versions of the schema are live against one cloud home. This page is how coven
keeps those versions from corrupting each other after release, once `rm -rf` is no
longer an option.

The examples use the todos app from [Sync](/docs/sync-model): a `list` holds
`todos`, Alice and Bob share the library.

Three things are in play, and they are independent:

- **Local migrations** move *one device's own* database from the old shape to the
  new one when its app updates.
- A per-changeset **`schema_version`** lets a reader recognize, and skip, a
  change written by a newer schema than its own.
- A **`min_schema_version`** floor in storage hard-stops a client that is too old
  to safely participate at all.

Migrations and the two version numbers solve different problems. A migration
fixes *your local database*; it does not rewrite the changesets other devices
already wrote to the cloud, nor the ones you already published. So "can I migrate
my data forward" and "can these two app versions share a library" are separate
questions, answered by different mechanisms.

## Local migrations

coven owns the SQLite connection. The host passes a `migrate` closure to
`Coven::builder(config).open(...)`; coven runs its own bookkeeping migration,
then the host's, against the connection it owns, every time the database opens:

```rust
let handle = Coven::builder(config)
    .synced_tables(synced_tables)
    .open(|conn| {
        conn.execute_batch(include_str!("migrations/0001_initial.sql"))?;
        conn.execute_batch(include_str!("migrations/0002_add_due_date.sql"))?;
        Ok(())
    })?;
```

Each device runs this on the version of the binary it happens to be on, so a
device updates *its own* schema when *its* app updates. That is exactly what a
migration is for, including non-additive transforms (renaming a column, splitting
a table, backfilling), a migration can do anything to the local file.

coven keeps **no migration ledger**, no version table, no `user_version` pragma.
The SQLite file's own schema is the truth, and migrations are written
`IF NOT EXISTS` / idempotent so re-running them over an already-migrated database
(for example one [bootstrapped from a snapshot](/docs/bootstrap)) is a no-op.
coven's own notion of "what version produced this data" is a single compile-time
constant, [`SCHEMA_VERSION`](rustdoc:const:coven::sync::push::SCHEMA_VERSION),
baked into the binary, never read back from the database.

## Additive vs. structural changes

The kind of change decides what it costs to sync across versions.

Conflict resolution reads each column's index from `PRAGMA table_info` at apply
time, and changesets address columns positionally. So **appending** a column or
adding a table is *wire-compatible*: an older reader sees a prefix of the columns
it knows and ignores the rest. A **structural** change (reordering, removing, or
renaming a column, splitting a table) breaks that positional alignment: an older
reader would map a changeset's values onto the wrong columns.

Both are valid *local* migrations. The difference is only whether a device on the
other version can still read the changesets, which is what the two version
numbers below gate.

## `SCHEMA_VERSION`: skip changes from a newer schema

Every outgoing changeset is stamped with the producer's `SCHEMA_VERSION`. On
pull, a reader applies a changeset only when its `schema_version` is **at or below**
the reader's own. When it sees a higher one
([`pull.rs`](rustdoc:fn:coven::sync::pull::pull_changes)), it skips that
changeset, **leaves its cursor where it is**, and stops pulling that device for
this cycle, every later sequence from that device is at least as new, so nothing
past it could apply either. Counted in `PullResult::skipped_schema`.

Leaving the cursor put is the point: the changeset is genuine and becomes
applicable the moment the app updates. The device does **not** advance past it
(that would strand those rows, a running device does not re-bootstrap from a
snapshot mid-life) and does **not** reconcile them from a snapshot. It re-fetches
from the parked sequence on the next cycle after it upgrades.

Worked example. bae ships v5, which adds a `due_date` column to `todos`
(additive), and bumps `SCHEMA_VERSION` from 4 to 5. Bob updates; Alice has not.

- **Bob (v5) → Alice (v4):** Bob's changesets carry `schema_version = 5`. Alice
  sees `5 > 4`, skips them, parks her cursor for Bob, and keeps working on her v4
  schema. She does not see Bob's new-schema rows yet.
- **Alice (v4) → Bob (v5):** Alice's changesets carry `schema_version = 4`. Bob
  applies them, a newer reader understands an older changeset (v4's columns are a
  prefix of v5's).
- **Alice updates to v5:** her next cycle re-pulls Bob's changesets from the
  parked cursor and applies them. She converges fully.

So the two versions coexist on one library. Sync stays live; the only effect is
that the older client lags on the newer schema's rows until it upgrades, then
catches up. No positional mismatch ever happens, because a device never applies a
changeset stamped newer than itself.

## `min_schema_version`: the hard floor

The skip above keeps an old client *safe but behind*. That is the right behavior
for an additive change. A **structural** change is different: an old client must
not keep going at all, because (a) it could not read the new state even after
bootstrapping, and (b) the v4 changesets it keeps *writing* would now misalign
against the v5 shape when a v5 device applies them. The skip only protects reads
in one direction; it does nothing about the old client's writes.

For that case, the release also raises the floor, the `min_schema_version` value
in storage, written through `SyncStorage::set_min_schema_version`. Pull checks it
before anything else: a client whose `SCHEMA_VERSION` is below the
stored `min_schema_version` gets
[`PullError::SchemaVersionTooOld`](rustdoc:enum:coven::sync::pull::PullError) and
syncs nothing, no reads, no writes, until the user updates the app. Its
`Display` is the message shown to the user. This is a permanent stop, not a
transient skip.

So the rule for a post-release schema change:

| Change | Bump `SCHEMA_VERSION` | Raise `min_schema_version` | Effect on an un-updated client |
| --- | --- | --- | --- |
| Additive (append column / add table) | yes | no | keeps syncing; lags on the new rows until it updates, then catches up |
| Structural (rename / remove / reorder / split) | yes | yes | hard-stopped (`SchemaVersionTooOld`) until it updates |

Pre-1.0 there is a third option the floor exists to avoid: coven's host can treat
the store as disposable (`rm -rf` and re-sync). This page is the path once that is
no longer acceptable.

## Snapshots carry the schema

A [snapshot](/docs/bootstrap) is a physical `VACUUM INTO` image of the
snapshotting device's database, so its bytes already hold that device's full
schema. A snapshot generation's signed metadata records per-device cursors and a
database hash, **no schema version**. A snapshot therefore sidesteps the
positional-changeset problem entirely: it can faithfully represent any schema,
because it is a SQLite file, not a positional row encoding.

When a device bootstraps, it adopts that file and then runs **its own** `migrate`
closure over it. So:

- **Joiner at or above the snapshot's version (forward):** safe by construction.
  A snapshot taken at v4, adopted by a v5 binary, has v5's migrations run over it
  and lands at v5. The `IF NOT EXISTS` migrations for versions already present are
  no-ops.
- **Joiner below the snapshot's version (reverse):** has no per-snapshot guard.
  [`bootstrap_from_snapshot`](rustdoc:fn:coven::sync::snapshot::bootstrap_from_snapshot)
  writes the file unconditionally, and the handle open path has no "this database is
  from the future" detector. The only protection is the floor, checked on the
  pull that immediately follows bootstrap, so a too-old joiner that should be
  fenced fails the join with `SchemaVersionTooOld`, plus additive-tolerance for
  changes that never raised the floor.

This is the same contract as changesets, with one difference in shape: a changeset
degrades *per message* (skip the newer ones, park, catch up later), while a
snapshot is *all-or-nothing* against the floor, it is adopted whole or the join
is refused. The takeaway is the same either way: raise `min_schema_version` on any
change an older binary cannot safely operate against, and the rest takes care of
itself.
