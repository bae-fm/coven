# Bootstrap

A device that joins a library, or restores one on new hardware, needs the whole
current state of every synced table. Replaying the full changeset history would
work but grows without bound: a library that has run for a year holds a year of
changesets. Instead, coven keeps one full snapshot of the database in the cloud
and lets a fresh device download that, then pull only the changesets created
after it.

The examples use a todos app: `workspaces` hold `lists`, a `list` holds `todos`,
a `todo` has `todo_attachments`, and `todos` carry `labels` through a
`todo_labels` join. A `list` has a boolean `shared` column gating it.

## What a snapshot is

A snapshot is a full copy of the database, made with SQLite's `VACUUM INTO`, then
scoped down to exactly the rows eligible to cross devices, then sealed for storage.
[`create_snapshot`](rustdoc:fn:coven::sync::snapshot::create_snapshot) does this
in one pass:

1. `VACUUM INTO` writes a clean, defragmented copy of the live database to a temp
   file. This copy still holds every table, including ones that never sync.
2. Local-only tables (any table the host did not pass to
   [`Database::open`](rustdoc:method:coven::database::Database::open) as a
   [`SyncedTable`](rustdoc:enum:coven::sync::session::SyncedTable), plus coven's
   own `sync_cursors`, `sync_state`, and `cloud_outbox`) have their rows deleted.
   Their schema stays, so the restored database opens against the same schema it
   was snapshotted at, but a device-local row (say a `device_settings` table
   holding a filesystem path) never rides along to a peer. coven keeps no
   migration ledger: the snapshot bytes carry every table's schema, so a restored
   database is already at the schema the snapshotting device ran.
3. Row-level gating is applied: gated-false roots and their foreign-key
   descendants are deleted. A private list (`shared = 0`) and the todos under it
   are removed from the copy. This reuses the same
   [`Gates`](rustdoc:struct:coven::sync::gate::Gates) model the outbound
   changeset filter uses, so the snapshot carries the exact same set of rows the
   changeset path would have sent. See [Local data](local-data.md) for the gate.
4. A second `VACUUM` reclaims the pages freed by those deletes, then the bytes are
   read and sealed by the home's cipher. On an encrypted home that means
   encrypting with the library key and storing the snapshot at `snapshot.db.enc`;
   on a [plaintext home](encryption.md#the-optional-plaintext-cloud-home) the
   bytes are stored verbatim at `snapshot.db`, so the snapshot is a directly
   readable SQLite image.

Because the snapshot and the changeset path share one gate, a device that
bootstraps from a snapshot and a device that applied live changesets converge on
the same rows. A private subtree cannot leak through the snapshot channel.

If the synced set is empty,
[`create_snapshot`](rustdoc:fn:coven::sync::snapshot::create_snapshot) returns
[`SnapshotError::NoSyncedTables`](rustdoc:enum:coven::sync::snapshot::SnapshotError)
rather than emit a snapshot. With no synced tables it could not tell which tables
are shareable: it would either clear the whole database or leak every local-only
table. (Sync as a whole refuses an empty set earlier, when
[`init_sync`](rustdoc:fn:coven::sync::cycle::init_sync) checks the set the host
passed to [`Database::open`](rustdoc:method:coven::database::Database::open).)

## Snapshot policy

[`should_create_snapshot`](rustdoc:fn:coven::sync::snapshot::should_create_snapshot)
decides when a cycle creates one. The defaults:

- 100 changesets since the last snapshot, or
- 24 hours since the last snapshot, but only if at least one changeset was pushed
  in that window, or
- no snapshot has ever been made and the device has pushed at least one
  changeset.

```rust
pub fn should_create_snapshot(
    local_seq: u64,
    last_snapshot_seq: Option<u64>,
    hours_since_snapshot: Option<u64>,
) -> bool
```

The cycle adds one more trigger that the policy function does not cover: the
*initial sync* of an existing library. When a host connects a cloud provider to a
database that already holds rows, the session produces no changeset (the data was
written before sync started). The cycle detects `local_seq == 0`, no prior
snapshot, and no outgoing changeset, and pushes a snapshot so that existing data
reaches the cloud at all.

After a snapshot uploads, the cycle records `snapshot_seq` (the local seq the
snapshot was taken at) and `last_snapshot_time` in `sync_state`, which feed the
next policy check.

## Push

[`push_snapshot`](rustdoc:fn:coven::sync::snapshot::push_snapshot) uploads two
objects and updates the device head:

- the sealed snapshot blob (overwriting any previous one) — encrypted on an
  encrypted home, stored verbatim on a plaintext one, and
- per-device cursor metadata as
  [`SnapshotMeta`](rustdoc:struct:coven::sync::snapshot::SnapshotMeta), holding a
  `device_id -> seq` map and an RFC 3339 `created_at` timestamp.

The cursor map is the snapshotting device's *applied* cursors (what it has pulled
and applied from every other device), plus its own `current_seq`. These cursors
describe exactly what the snapshot database contains. They must reflect applied
state, never another device's published head: see Cursors below for why
overclaiming is unsafe.

## Join and restore

Bootstrapping happens inside the join flow (a new member added by an owner) and
the restore flow (the owner recovering the library on new hardware). Both call
[`bootstrap_from_snapshot`](rustdoc:fn:coven::sync::snapshot::bootstrap_from_snapshot):

1. Download the snapshot metadata first. Its absence is a torn bucket (see
   below), and fetching it before writing anything means a torn bucket leaves no
   half-written database on disk.
2. Download the snapshot blob, open it through the home's cipher (decrypt on an
   encrypted home; pass through on a plaintext one), and write the resulting bytes
   directly to `target_path`. There is no migration replay: the snapshot bytes
   *are* the database file.
3. Return a
   [`BootstrapResult`](rustdoc:struct:coven::sync::snapshot::BootstrapResult)
   carrying the per-device cursors from the metadata.

The device then opens the bootstrapped file with
[`Database::open`](rustdoc:method:coven::database::Database::open) and pulls every
changeset newer than the bootstrap cursors, so it catches up on anything written
between the snapshot and now. coven owns the connection from this point: there is
no host reopen step. The snapshot already carries the full schema (the host's
tables and coven's bookkeeping), so `Database::open`'s bookkeeping migration
finds its `IF NOT EXISTS` tables already present and the host's `migrate` closure
is a no-op here. There is no migration ordering for the host to get wrong; coven
runs both migrations against the connection it owns.

The bootstrap cursors are passed straight into the pull as the starting point;
the pull returns advanced cursors as it applies changesets. Just after
`Database::open`, the fresh capture session is suspended: a just-bootstrapped
library has no local changes to capture, and the pull's apply must run with no
session active so the applied rows are not re-recorded as local writes.

```rust
let bootstrap_result = bootstrap_from_snapshot(storage, encryption, &db_path).await?;
let (db, _stamper) = Database::open(
    &db_path,
    synced_tables.to_vec(),
    device_id.to_string(),
    |_conn| Ok(()), // schema already in the snapshot; nothing to migrate
)?;
db.take_changeset_and_suspend().await?;
let (_cursors, pull_result) = pull_changes(
    &db,
    synced_tables,
    storage,
    device_id,
    &bootstrap_result.cursors,
    library_dir,
    blob_plan,
).await?;
```

The snapshot's row-clearing step empties `sync_cursors`, so a bootstrapped
database starts with no cursor rows. The metadata cursors are the only seed for
where to resume pulling.

## Cursors

The `sync_cursors` table maps each remote `device_id` to the highest seq this
device has applied from it. Device sequence numbers start at 1, so a cursor of 0
means "no changesets applied from this device yet" and selects every changeset it
has ever produced. A device with no row in `sync_cursors` is treated as cursor 0:
a missing entry and an explicit 0 are the same thing. The pull resolves this
through `cursor_for_device`, which logs the first time it sees a device so the
"never seen" case is visible in the trace.

Each cycle, the pull lists device heads, compares each head's seq to the local
cursor, fetches the changesets in between, applies them, and advances the cursor
past each applied seq. The cursor is the device's idea of how far it has caught up
with each peer.

## Garbage collection

Once a snapshot covers a range of changesets, those changesets are redundant: any
device joining now bootstraps from the snapshot instead of replaying them.
[`garbage_collect`](rustdoc:fn:coven::sync::snapshot::garbage_collect) deletes
them, returning a
[`GcResult`](rustdoc:struct:coven::sync::snapshot::GcResult) with counts of
deleted changesets and non-fatal errors.

The safety rule is per-device. For each device head, GC reads that device's
`safe_seq` from the snapshot metadata cursors and deletes only that device's
changesets with seq <= `safe_seq`. A device that appears in no head, or in no
metadata entry, is left untouched.

This is why the metadata cursors must be honest about *applied* state. Consider
two devices. Device A creates a snapshot while device B is at seq 30, then device
B pushes seq 31 through 35 afterward. The metadata records B at 30, so GC deletes
B's 1 through 30 and leaves 31 through 35 alone: those changesets came after the
snapshot and are not in it, so a future restore still needs them. Had the metadata
overclaimed (recorded B's published head of 35 instead of the 30 actually applied
into the snapshot), GC would delete 31 through 35, and no future restore could
recover them.

## Torn bucket safety

A snapshot is two uploads: the blob, then its metadata.
[`push_snapshot`](rustdoc:fn:coven::sync::snapshot::push_snapshot) writes them in
that order, so a push that fails between the two leaves a blob with no metadata.
[`bootstrap_from_snapshot`](rustdoc:fn:coven::sync::snapshot::bootstrap_from_snapshot)
fetches the metadata first and refuses to continue if it is missing. Without the
metadata it could not know the per-device cursors, and guessing them from the
device heads would overclaim coverage and feed the same unsafe deletion GC guards
against. Bootstrap fails cleanly rather than restore from incomplete data.
