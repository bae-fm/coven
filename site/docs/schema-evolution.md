# Schema evolution

Devices that share a store do not all run the same app version at once. Someone
updates their phone on Monday and their laptop the following week; in between, two
versions of the schema are live against one cloud home. This page is how coven
keeps those versions from corrupting each other after release, once `rm -rf` is no
longer an option.

The examples use the todos app from [Sync](/docs/sync-model): a `list` holds
`todos`, Alice and Bob share the store.

Three things are in play:

- The **migration ladder** moves *one device's own* database from the old shape
  to the new one when its app updates. Its top rung is the device's schema
  version.
- The per-changeset **`schema_version`** stamp lets a reader recognize, and
  skip, a change written by a newer schema than its own.
- A **`min_schema_version`** floor in storage hard-stops a client that is too old
  to safely participate at all.

A migration fixes *your local database*; it does not rewrite the changesets
other devices already wrote to the cloud, nor the ones you already published. So
"can I migrate my data forward" and "can these two app versions share a store"
are separate questions, answered by different mechanisms.

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

## The migration ladder

Each device has to move its own database forward on its own schedule, and the
database itself has to know how far it got: version as a fact of the *file*,
not of whichever binary happens to open it. That is what the ladder over
`PRAGMA user_version` provides.

The host passes an ordered ladder of
[`Migration`](rustdoc:struct:coven::migration::Migration)s to the builder;
coven applies it over `PRAGMA user_version` at every open, after reconciling
its own bookkeeping tables:

```rust
let handle = Coven::builder(config)
    .synced_tables(synced_tables)
    .migrations(vec![
        Migration::sql(1, "initial", include_str!("migrations/0001_initial.sql")),
        Migration::sql(2, "add_due_date", include_str!("migrations/0002_add_due_date.sql")),
    ])
    .open()?;
```

<svg class="flow" viewBox="0 0 660 150" role="img" aria-label="The ladder walks user_version from 0 through each migration to the top rung, which is the wire schema version">
<rect class="chipd" x="16" y="48" width="120" height="30" rx="7"/>
<text class="lbl s11" x="76" y="67" text-anchor="middle">user_version 0</text>
<line class="arr" x1="140" y1="63" x2="184" y2="63" marker-end="url(#fa)"/>
<text class="sub" x="162" y="48" text-anchor="middle">1 · initial</text>
<rect class="chip" x="188" y="48" width="120" height="30" rx="7"/>
<text class="lbl s11" x="248" y="67" text-anchor="middle">user_version 1</text>
<line class="arr" x1="312" y1="63" x2="356" y2="63" marker-end="url(#fa)"/>
<text class="sub" x="334" y="48" text-anchor="middle">2 · add_due_date</text>
<rect class="chipa" x="360" y="48" width="120" height="30" rx="7"/>
<text class="lbl s11" x="420" y="67" text-anchor="middle">user_version 2</text>
<line class="arrd" x1="484" y1="63" x2="524" y2="63" marker-end="url(#fam)"/>
<rect class="chipo" x="528" y="48" width="118" height="30" rx="7"/>
<text class="lbl s11" x="587" y="67" text-anchor="middle">wire version 2</text>
<text class="sub" x="330" y="112" text-anchor="middle">each rung runs in its own transaction with the version bump: a failed step rolls back whole</text>
<text class="sub" x="330" y="130" text-anchor="middle">the top rung is the schema_version every changeset is stamped with</text>
</svg>

The rules, all enforced before any database access:

- Versions must be exactly contiguous `1..=N`. A gap, a duplicate, or a set
  that does not start at 1 is a startup error, not a silent skip.
- Each step (`Migration::sql` for a DDL batch, or a closure for rebuilds and
  backfills DDL cannot express) runs in its own transaction together with its
  `PRAGMA user_version` bump, so the ledger never advances over a half-applied
  migration.
- If the on-disk `user_version` exceeds the ladder's top, the binary refuses to
  open with
  [`MigrationError::SchemaTooNew`](rustdoc:enum:coven::migration::MigrationError)
  ("update the app") rather than run against a schema it does not know.

`user_version` is a SQLite header field, so it travels inside a snapshot's
byte-for-byte image: a device that bootstraps inherits the writer's applied
version directly. And the same number, reported by
[`Database::schema_version`](rustdoc:method:coven::database::Database::schema_version),
is the wire `schema_version` every changeset is stamped with. Bumping the
schema *is* adding a migration; a device cannot stamp a version it has not
migrated to.

The ladder covers the host's *synced* schema only. Coven creates its bookkeeping
tables in the current shape and rejects a non-current shape at open; those tables
are outside the host migration ladder.

## Additive vs. structural changes

The kind of change decides what it costs to sync across versions.

Conflict resolution reads each column's index from `PRAGMA table_info` at apply
time, and changesets address columns positionally. So **appending** a column or
adding a table is *wire-compatible*: an older reader sees a prefix of the columns
it knows and ignores the rest. A **structural** change (reordering, removing, or
renaming a column, splitting a table) breaks that positional alignment: an older
reader would map a changeset's values onto the wrong columns.

A table added in a later migration is still a synced table, so it is still
checked at open: declare it `STRICT` along with the rest of the [synced-table
contract](/docs/local-data).

Both are valid *local* migrations. The difference is only whether a device on the
other version can still read the changesets, which is what the two version
numbers below gate.

## `schema_version`: skip changes from a newer schema

Every outgoing changeset is stamped with the producer's ladder top. On
pull, a reader applies a changeset only when its `schema_version` is **at or below**
the reader's own. When it sees a higher one
([`pull_changes`](rustdoc:fn:coven::sync::pull::pull_changes)), it skips that
changeset, **leaves its cursor where it is**, and stops pulling that device for
this cycle; every later sequence from that device is at least as new, so nothing
past it could apply either. Counted in `PullResult::skipped_schema`.

Leaving the cursor put is the point: the changeset is genuine and becomes
applicable the moment the app updates. The device does **not** advance past it
(that would strand those rows; a running device does not re-bootstrap from a
snapshot mid-life) and does **not** reconcile them from a snapshot. It re-fetches
from the parked sequence on the next cycle after it upgrades.

Worked example. The app ships v5, which adds a `due_date` column to `todos`
(additive) as ladder rung 5. Bob updates; Alice has not.

- **Bob (v5) → Alice (v4):** Bob's changesets carry `schema_version = 5`. Alice
  sees `5 > 4`, skips them, parks her cursor for Bob, and keeps working on her v4
  schema. She does not see Bob's new-schema rows yet.
- **Alice (v4) → Bob (v5):** Alice's changesets carry `schema_version = 4`. Bob
  applies them; a newer reader understands an older changeset (v4's columns are a
  prefix of v5's).
- **Alice updates to v5:** her open runs rung 5, and her next cycle re-pulls
  Bob's changesets from the parked cursor and applies them. She converges fully.

So the two versions coexist on one store. Sync stays live; the only effect is
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
in storage. Pull checks it before anything else: a client whose ladder top is
below the stored `min_schema_version` gets
[`PullError::SchemaVersionTooOld`](rustdoc:enum:coven::sync::pull::PullError) and
syncs nothing, no reads, no writes, until the user updates the app. Its
`Display` is the message shown to the user. This is a permanent stop, not a
transient skip. The floor object itself is untrusted input: with a membership
chain present it is honored only when signed by a current Owner, so a non-owner
cannot freeze the fleet or roll the floor back.

So the rule for a post-release schema change:

| Change | Add a ladder rung | Raise `min_schema_version` | Effect on an un-updated client |
| --- | --- | --- | --- |
| Additive (append column / add table) | yes | no | keeps syncing; lags on the new rows until it updates, then catches up |
| Structural (rename / remove / reorder / split) | yes | yes | hard-stopped (`SchemaVersionTooOld`) until it updates |

## Snapshots carry the schema, and say so

A [snapshot](/docs/bootstrap) is a physical `VACUUM INTO` image of the
snapshotting device's database, so its bytes hold that device's full schema
*including* its `user_version`, and the generation's signed metadata records
that `schema_version` alongside the cursors. A snapshot sidesteps the
positional-changeset problem entirely: it is a SQLite file, not a positional
row encoding, so it can faithfully represent any schema.

Version skew at bootstrap is handled on both sides:

- **Joiner at or above the snapshot's version (forward):** safe by
  construction. A snapshot taken at v4, adopted by a v5 binary, has rung 5 run
  over it at open and lands at v5.
- **Joiner below the snapshot's version (reverse):** refused *before the
  download*. The metadata's recorded version exceeds the joiner's ladder top,
  so bootstrap fails with
  [`SnapshotError::SchemaTooNew`](rustdoc:enum:coven::sync::snapshot::SnapshotError)
  and writes nothing. The at-open
  [`MigrationError::SchemaTooNew`](rustdoc:enum:coven::migration::MigrationError)
  check backs this up for a database file that is already on disk (a copied
  file, a downgraded app): a binary never runs against a schema from its
  future.

This is the same contract as changesets, with one difference in shape: a changeset
degrades *per message* (skip the newer ones, park, catch up later), while a
snapshot is *all-or-nothing*: it is adopted whole or the bootstrap is refused.
The takeaway is the same either way: add a ladder rung for every change, and
raise `min_schema_version` on any change an older binary cannot safely operate
against.
