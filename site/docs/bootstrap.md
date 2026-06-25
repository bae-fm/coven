# Bootstrap

A device that joins a library, or restores one on new hardware, needs the whole
current state of every synced table. Replaying the full changeset history would
work but grows without bound: a library that has run for a year holds a year of
changesets. Instead, coven keeps a full snapshot of the database in the cloud and
lets a fresh device download that, then pull only the changesets created after it.

The examples use a todos app: `workspaces` hold `lists`, a `list` holds `todos`,
a `todo` has `todo_attachments`, and `todos` carry `labels` through a
`todo_labels` join. A `list` has a boolean `shared` column gating it.

## What a snapshot is

A snapshot is a full copy of the database, made with SQLite's `VACUUM INTO`, then
scoped down to exactly the rows eligible to cross devices, then sealed for storage.
[`create_snapshot`](rustdoc:fn:coven::sync::snapshot::create_snapshot) does this in
one pass:

1. `VACUUM INTO` writes a clean, defragmented copy of the live database to a temp
   file. This copy still holds every table, including ones that never sync.
2. Local-only tables (any table the host did not pass to
   [`Database::open`](rustdoc:method:coven::database::Database::open) as a
   [`SyncedTable`](rustdoc:struct:coven::sync::session::SyncedTable), plus coven's
   own `sync_cursors`, `sync_state`, and `cloud_outbox`) have their rows deleted.
   Their schema stays, so the restored database opens against the same schema it
   was snapshotted at, but a device-local row (say a `device_settings` table
   holding a filesystem path) never rides along to a peer. coven keeps no
   migration ledger: the snapshot bytes carry every table's schema, so a restored
   database is already at the schema the snapshotting device ran.
3. Row-level gating is applied: gated-false roots and their foreign-key
   descendants are deleted. A private list (`shared = 0`) and the todos under it
   are removed from the copy. This reuses the same
   [`Gates`](rustdoc:struct:coven::sync::gate::Gates) model the outbound changeset
   filter uses, so the snapshot carries the exact same set of rows the changeset
   path would have sent. See [Local data](/docs/local-data) for the gate.
4. The bytes are read and sealed by the home's cipher: encrypted with the library
   key on an opaque home, stored verbatim on a
   [browsable home](/docs/encryption#opaque-and-browsable-homes).

Because the snapshot and the changeset path share one gate, a device that
bootstraps from a snapshot and a device that applied live changesets converge on
the same rows. A private subtree cannot leak through the snapshot channel.

If the synced set is empty,
[`create_snapshot`](rustdoc:fn:coven::sync::snapshot::create_snapshot) returns
[`SnapshotError::NoSyncedTables`](rustdoc:enum:coven::sync::snapshot::SnapshotError)
rather than emit a snapshot. With no synced tables it could not tell which tables
are shareable: it would either clear the whole database or leak every local-only
table.

## Generations and the pointer

A snapshot is not a single object that gets overwritten. Each publish is a
**generation**, keyed under the publishing device's own `{author}` (its hex public
key) and the seq it was taken at:

```text
snapshot/{author}/{seq}.db.enc         the database image
snapshot/{author}/{seq}_meta.json.enc  per-device cursors, signed
snapshot/current.json.enc              the pointer naming the live {author, seq}, signed
```

[`push_snapshot`](rustdoc:fn:coven::sync::snapshot::push_snapshot) writes the
database image first, then the metadata, then the single `current.json` pointer
**last**. The pointer is the commit: a reader resolves the pointer, then reads the
generation it names, so it always sees a whole, self-consistent generation. There
is no window where a new database image is paired with a stale or missing meta.

Keying each device's generations under its own `{author}` makes them globally
unique. `seq` is the publisher's own `local_seq`, not a global id, so two devices
can publish at the same seq, but their objects are distinct keys and a publish can
never overwrite a peer's generation. Reclaiming a superseded generation is
therefore owned by its author: a device lists and deletes only objects under its
own `{author}` prefix.

The publish is atomic by construction. Until the pointer flips, every reader still
resolves the previous generation (itself complete) or none. A crash after the
image and meta but before the pointer leaves orphan objects that nothing
references and the old pointer still valid; a later sweep by that device reclaims
them. No half-published state is ever observable, and nothing relies on a later
pass to repair a wrong state.

## Signing and authorization

The bucket is untrusted: the at-rest cipher proves only confidentiality (the
library key is shared by every member), not authorship. So the metadata and the
pointer are each **signed** by the publishing device and bound to the library id.
The metadata signs the per-device cursors and a hash of the database image; the
pointer signs the generation seq and the same database hash.

A reader (bootstrap or GC) authenticates a generation before trusting it:

- the pointer's signature must verify (under this library id, which also refuses a
  different library's pointer replayed here) and its author must be a current
  write-capable member, so a non-member cannot repoint the live snapshot;
- the named generation's metadata signature must verify and its author must be a
  current write-capable member, so a forged or cursor-poisoned meta is refused;
- the pointer and the meta must agree on the database hash, and the downloaded
  database's hash must match what the meta commits to, so a substituted image is
  refused.

Membership is anchored to the library's owner when the owner is pinned (on join,
the invite pins the founder), so a wiped-and-refounded chain under an attacker's
key fails authorization. On restore there is no pinned owner yet, so the chain is
anchored to its own founder and the owner is adopted trust-on-first-use after the
pull.

## Snapshot policy

[`should_create_snapshot`](rustdoc:fn:coven::sync::snapshot::should_create_snapshot)
decides when a cycle creates one. The defaults:

- 100 changesets since the last snapshot, or
- 24 hours since the last snapshot, but only if at least one changeset was pushed
  in that window, or
- no snapshot has ever been made and the device has pushed at least one changeset.

```rust
pub fn should_create_snapshot(
    local_seq: u64,
    last_snapshot_seq: Option<u64>,
    hours_since_snapshot: Option<u64>,
) -> bool
```

The cycle adds one trigger the policy function does not cover: the *initial sync*
of an existing library. When a host connects a cloud provider to a database that
already holds rows, the session produces no changeset (the data was written before
sync started). The cycle detects `local_seq == 0`, no prior snapshot, and no
outgoing changeset, and pushes a snapshot so that existing data reaches the cloud
at all.

After a snapshot uploads, the cycle records the seq it was taken at and the time
in `sync_state`, which feed the next policy check.

## Join and restore

Bootstrapping happens inside the join flow (a new member added by an owner) and
the restore flow (the owner recovering the library on new hardware). Both call
[`bootstrap_from_snapshot`](rustdoc:fn:coven::sync::snapshot::bootstrap_from_snapshot):

1. Resolve the `current.json` pointer to the live generation, authenticating the
   whole generation (the signatures, the authors' membership, the database-hash
   agreement) before touching disk. Because the reader resolves the pointer first,
   it always sees a complete generation, so there is no torn-read window and no
   half-written database is left on disk on a failure.
2. Download that generation's database image, confirm its hash matches the signed
   metadata, open it through the home's cipher (decrypt on an opaque home, pass
   through on a browsable one), and write the resulting bytes directly to
   `target_path`. There is no migration replay: the snapshot bytes *are* the
   database file.
3. Return a
   [`BootstrapResult`](rustdoc:struct:coven::sync::snapshot::BootstrapResult)
   carrying the per-device cursors from the metadata.

The device then opens the bootstrapped file with
[`Database::open`](rustdoc:method:coven::database::Database::open) and pulls every
changeset newer than the bootstrap cursors, so it catches up on anything written
between the snapshot and now. coven owns the connection from this point: there is
no host reopen step. The snapshot already carries the full schema (the host's
tables and coven's bookkeeping), so `Database::open`'s bookkeeping migration finds
its `IF NOT EXISTS` tables already present and the host's `migrate` closure is a
no-op here.

Capture stays enabled through the bootstrap pull. A just-bootstrapped library has
no local writer, so there is no whole-cycle suspend to manage; the pull disables
capture only around each apply, exactly as a steady-state cycle does.

```rust
let bootstrap = bootstrap_from_snapshot(storage, library_id, &cipher, owner_pubkey, &db_path).await?;
let (db, _stamper) = Database::open(
    &db_path,
    synced_tables.to_vec(),
    device_id.to_string(),
    |_conn| Ok(()), // schema already in the snapshot; nothing to migrate
)?;
let applied = pull_changes(
    &db,
    synced_tables,
    storage,
    device_id,
    owner_pubkey,
    &bootstrap.cursors,
    library_dir,
    blob_source,
).await?;
```

The snapshot's row-clearing step empties `sync_cursors`, so a bootstrapped database
starts with no cursor rows. The metadata cursors are the only seed for where to
resume pulling.

### Reconciling blobs

The snapshot carries the catalog rows but not the per-row blob files. The
incremental pull that follows starts past the snapshot's cursors, so the original
changesets that carried each row's image never re-walk, and the per-changeset blob
download never fires for them. A bootstrapped device would otherwise have the rows
but none of the files they point at (a synced album shows a placeholder cover).

[`reconcile_snapshot_blobs`](rustdoc:fn:coven::sync::snapshot::reconcile_snapshot_blobs)
closes that gap. It derives the blobs the
[declarations](/docs/blobs#declaring-which-rows-carry-blobs) find in the
bootstrapped database and downloads the `Mirrored` ones into the [cache](/docs/cache)
(`storage/pinned/<id>`), skipping any already present. `OnDemand` blobs are left
for first read, the same as in a steady-state pull. The bootstrap records a pending
flag in `sync_state`; each later cycle re-runs the reconciliation until every
referenced `Mirrored` blob is on disk, so a blob whose object was not yet in the
cloud at bootstrap is fetched on a later cycle rather than lost. A caught-up library
clears the flag and pays nothing.

## Cursors

The `sync_cursors` table maps each remote `device_id` to the highest seq this
device has applied from it. Device sequence numbers start at 1, so a cursor of 0
means "no changesets applied from this device yet" and selects every changeset it
has ever produced. A device with no row in `sync_cursors` is treated as cursor 0:
a missing entry and an explicit 0 are the same thing.

Each cycle, the pull lists device heads, compares each head's seq to the local
cursor, fetches the changesets in between, applies them, and advances the cursor
past each applied seq. The cursor is the device's idea of how far it has caught up
with each peer.

## Garbage collection

Once a snapshot covers a range of changesets, those changesets are redundant: any
device joining now bootstraps from the snapshot instead of replaying them.
[`garbage_collect`](rustdoc:fn:coven::sync::snapshot::garbage_collect) reclaims
them, returning a [`GcResult`](rustdoc:struct:coven::sync::snapshot::GcResult) with
counts of deleted changesets and non-fatal errors. It reclaims two kinds of
superseded object:

- **Old changesets.** It resolves the live generation (authenticating the pointer
  and meta first, since their cursors decide what is deleted fleet-wide) and, per
  device, deletes only that device's changesets with seq at or below that device's
  cursor in the snapshot. Changesets pushed *after* the snapshot are preserved even
  if their seq is below another device's snapshot cursor.
- **Old generations.** It lists only this device's own `snapshot/{own_author}/`
  prefix and deletes the generations it published that are neither live (the
  pointer still names them) nor the one it just published. Because the prefix is
  the author, it never touches a peer's generations.

The metadata cursors must be honest about *applied* state. Consider two devices.
Device A creates a snapshot while device B is at seq 30, then device B pushes seq
31 through 35 afterward. The metadata records B at 30, so GC deletes B's 1 through
30 and leaves 31 through 35 alone: those changesets came after the snapshot and are
not in it, so a future restore still needs them. Had the metadata overclaimed
(recorded B's published head of 35 instead of the 30 actually applied into the
snapshot), GC would delete 31 through 35, and no future restore could recover them.
This is why the cursors are the snapshotting device's *applied* cursors, never
another device's published head.
