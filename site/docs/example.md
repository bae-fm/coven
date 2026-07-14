# Example

Here we build a small todo app to show how coven fits into a host. The app owns
its schema, its UI, and its product policy. coven owns the SQLite connection,
change capture, encrypted sync, membership, and blob transfer. The two meet at
one call to open the database and a handful of methods after that.

The data model: a `workspaces` table holds `lists`, a `list` holds `todos`, and
a list has a boolean `shared` column. Lists marked `shared` (and the todos under
them) reach teammates; the rest stay on the device that wrote them.

## Open the store

coven owns the connections. The host opens one native handle with
[`Coven::builder`](rustdoc:struct:coven::Coven), handing over the set of tables
that sync and the [migration ladder](/docs/schema-evolution) that creates the
app's own tables. coven runs its bookkeeping migration first, then any ladder
rungs above the database's version, seeds its clock off the rows already on
disk, attaches the change-capture session to the synced tables, and spawns the
threads that own the connections — a writer, and a read-only companion that
backs `handle.sql_read`.

```rust
use coven::{Coven, Migration, RowIdentity, SyncedTable};

const SCHEMA: &str = "
CREATE TABLE workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    _updated_at TEXT NOT NULL
) STRICT;
CREATE TABLE lists (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    name TEXT NOT NULL,
    shared INTEGER NOT NULL DEFAULT 0,
    _updated_at TEXT NOT NULL
) STRICT;
CREATE TABLE todos (
    id TEXT PRIMARY KEY,
    list_id TEXT NOT NULL REFERENCES lists(id),
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0,
    _updated_at TEXT NOT NULL
) STRICT;
";

let handle = Coven::builder(config)
    .synced_tables(vec![
        SyncedTable::new("workspaces", RowIdentity::IndependentUuid),
        SyncedTable::new("lists", RowIdentity::IndependentUuid).gated_by("shared"),
        SyncedTable::new("todos", RowIdentity::IndependentUuid),
    ])
    .migrations(vec![Migration::sql(1, "initial", SCHEMA)])
    .open()?;
```

Every synced table is declared `STRICT`, carries an `id` text primary key at
column 0, and has an `_updated_at TEXT NOT NULL` column. `(table, id)` is the
logical row identity on every device. These app rows are independently created,
so their ids are canonical UUIDv4 or UUIDv7 values. A table instead uses
`RowIdentity::SharedKey` only when equal application keys intentionally merge as
one row. SQLite represents a primary-key change as deleting the old identity
and inserting the new validated identity in the same transaction. The
[`SyncedTable`](rustdoc:struct:coven::sync::session::SyncedTable) values declare how
each table is gated:
[`new`](rustdoc:method:coven::sync::session::SyncedTable::new) syncs every row,
[`remote_root`](rustdoc:method:coven::sync::session::SyncedTable::remote_root)
syncs every row and makes blobs on those rows and descendants always Remote,
[`gated_by`](rustdoc:method:coven::sync::session::SyncedTable::gated_by) makes a
row sync only while its boolean column is true, and
[`gated_by_descendants`](rustdoc:method:coven::sync::session::SyncedTable::gated_by_descendants)
keeps an ancestor row alive only while a gated descendant survives. Here `lists`
is a gated root, and `todos` inherit that gate down the foreign key. Tables you
don't pass are local-only and never leave the device. The gating rules are in
[Local data](/docs/local-data).

The handle's SQL context mints `_updated_at` stamps from a clock already seeded
past every value on disk, so a write made right after a restart can't mint a
stamp that sorts behind an existing row.

## Write a row

The host runs SQL through [`handle.sql`](rustdoc:struct:coven::CovenHandle), an
async method that takes a closure over a SQL context. coven re-exports the exact
rusqlite it owns, so use `coven::rusqlite` rather than depending on rusqlite
directly. Bind `sql.stamp()` into the `_updated_at` column of every synced-row
write; that value is coven's register for ordering writes across devices. The
session attached inside the connection captures the write for the next sync
cycle.

```rust
use coven::rusqlite::params;

handle.sql(move |sql| {
    let todo_id = uuid::Uuid::new_v4().to_string();
    sql.tx().execute(
        "INSERT INTO todos (id, list_id, title, done, _updated_at)
         VALUES (?1, ?2, ?3, 0, ?4)",
        params![todo_id, list_id, title, sql.stamp()],
    )?;
    Ok(())
})
.await?;
```

Don't read the stamp as a wall-clock time or compare two of them as dates. It is
an opaque clock value coven advances past pulled rows so a later local write
always sorts after them.

## Read a row

Reads go through [`handle.sql_read`](rustdoc:struct:coven::CovenHandle), which
runs the closure on a read-only companion connection: no change capture, and
reads run concurrently with the writer instead of queuing behind it. The
closure gets the `&Connection` directly (there is no stamp to mint on a read),
and a write inside it is refused by SQLite — the connection is read-only. A
read issued after an awaited write sees that write.

```rust
let titles: Vec<String> = handle
    .sql_read(move |conn| {
        let mut stmt = conn.prepare(
            "SELECT title FROM todos WHERE list_id = ?1 ORDER BY _updated_at",
        )?;
        let rows = stmt
            .query_map([list_id], |row| row.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    })
    .await?;
```

A local-only app stops here: open the handle, write through `handle.sql`, read
through `handle.sql_read`, and never build any of the sync machinery below.

## Turn on sync

Sync needs a master key and a cloud provider. The master key is protected by
*custody* — where it's unlocked from, and where it's written when
established — which defaults to the OS keyring and can be selected on the
builder with
[`key_custody`](rustdoc:method:coven::CovenBuilder::key_custody) before
`open()`; the `open` call above didn't set one, so it got the default. Either
way, the host names its keyring service once at startup with
[`set_keyring_service`](rustdoc:fn:coven::set_keyring_service) — coven never
reads or writes a keyring entry without it. See [Keys](/docs/keys) for every
preset and what each one protects against.

```rust
coven::set_keyring_service("todos")?;
```

A fresh store has no master key yet:
[`initialize_master_key`](rustdoc:method:coven::CovenHandle::initialize_master_key)
generates one and establishes it under whatever custody the builder
selected; coven refuses to run it again once one is established, so a
corrupt entry is never silently overwritten. This store also needs its own
signing identity — coven never mints one implicitly (see
[Keys](/docs/keys#no-silent-identity-minting)) —
[`initialize_identity`](rustdoc:method:coven::CovenHandle::initialize_identity)
establishes it explicitly, the same way, for a store created fresh (joining
or restoring an existing store establishes its identity as part of that
instead).

```rust
handle.initialize_master_key()?;
handle.initialize_identity()?;
```

Once a provider is connected, start sync through the handle. Which rows carry
blobs is declared on the synced tables passed to `open` (see
[Attachments](#attachments)), not here.

```rust
handle.connect_sync().await?;
```

After a write, nudge the loop with `sync_now` so the local edit goes out
promptly; the loop also runs on its own timer, so a missed trigger still syncs.

```rust
handle.sync_now();
```

The trigger is a no-op until the loop is running, so the host can call it after
every write without checking.

## React to remote changes

The loop emits a `SyncLoopStatus` after each cycle. The host reads it through
`handle.subscribe_sync_status()`, a broadcast receiver. When a pull applied rows,
`data_changed` is true and `row_changes` carries the rows so the host can refresh
the affected views.

```rust
let mut status = handle.subscribe_sync_status()?;
tokio::spawn(async move {
    while let Ok(s) = status.recv().await {
        if s.data_changed {
            if let Some(changes) = s.row_changes {
                // changes lists the tables and rows a pull touched;
                // map them to UI updates.
            }
        }
        if let Some(msg) = s.error {
            // show msg as worded; None clears it.
        }
    }
});
```

`error` holds a message coven already worded (a changeset from a newer app
version, a file that failed to download); show it as is.

## Attachments

If a todo carries a file, that file is a blob. coven moves blobs with the rows
that reference them, and it learns which rows carry one from a per-table
*declaration*, not a runtime callback. The host marks the blob-bearing synced table
with [`carries_blob`](rustdoc:method:coven::sync::session::SyncedTable::carries_blob)
when it builds the set it passes to `open`, naming the columns that locate each blob
plus its cloud namespace, a
[`BlobScope`](rustdoc:enum:coven::blob::BlobScope)
(`Master` for a key every member holds, `Derived` for a fixed per-scope key), a
[`Provenance`](rustdoc:enum:coven::blob::Provenance) (the Local story:
`UserProvided` for the user's own file, `HostProvided` for data coven keeps), and a
[`CacheFill`](rustdoc:enum:coven::blob::CacheFill) (the Remote story: `CacheEager` to
fetch it into the cache on pull, `CacheLazy` to fetch on first read):

```rust
use coven::{BlobDecl, CacheFill, Provenance};

// In the set you pass to `Coven::builder(...).synced_tables(...)`, declare the blob on `todos`:
//   blob id = the row's primary key; opaque home, so no cloud_path column;
//   master-scoped; the user's own file, fetched into every device's cache on pull.
SyncedTable::new("todos", RowIdentity::IndependentUuid).carries_blob(
    BlobDecl::new("todo-files", Provenance::UserProvided, CacheFill::CacheEager),
)
```

coven resolves the declaration against the live schema each cycle and derives every
blob a row references itself: what to upload on push, what to download on pull,
whose cache to drop on a delete, and what to backfill after a snapshot bootstrap.
It encrypts the file on upload; on pull it downloads, decrypts, and writes the
plaintext into its own [cache](/docs/cache), which the host reads back through
`handle.read_blob`. Where the bytes come from is the blob's provenance: the
user's own file (user-provided) or coven's local store (host-provided). Use
`handle.write(...)` when writing a row together with host-provided bytes. A blob
table under `remote_root()` has no Local state: rows sync normally, host-provided
blobs upload before the row is pushed, and reads resolve through the cache/cloud.
The [Blobs](/docs/blobs) page covers the declaration, the outbox, and the cloud
layout, and the [Cache](/docs/cache) page covers the device-local read side.

## Share with a teammate

A store starts with one member, the device that created it. To add a teammate,
the owner calls
`handle.invite_member(...)` with the teammate's public key and a
[`MemberRole`](rustdoc:enum:coven::sync::membership::MemberRole) (`Owner`,
`Member`, or read-only `Follower`). It returns a code the teammate redeems.

```rust
let invite_code = handle
    .invite_member(&teammate_pubkey_hex, None, MemberRole::Member)
    .await?;
```

On the teammate's device,
[`join_from_invite_code`](rustdoc:fn:coven::sync::join::join_from_invite_code)
decodes the code, runs the provider's auth flow, unwraps the store keyring sealed to
the teammate's key, downloads the snapshot, and pulls the changesets written
since. It returns a `Config` for the now-local store; from there the teammate
opens a `CovenHandle` exactly as above. `handle.remove_member(...)` appends a fresh
key generation the removed member never receives. The signed membership
chain, key wrapping, and the join flow are covered in [Sharing](/docs/sharing).
