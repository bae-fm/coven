# Example

Here we build a small todo app to show how coven fits into a host. The app owns
its schema, its UI, and its product policy. coven owns the SQLite connection,
change capture, encrypted sync, membership, and blob transfer. The two meet at
one call to open the database and a handful of methods after that.

The data model: a `workspaces` table holds `lists`, a `list` holds `todos`, and
a list has a boolean `shared` column. Lists marked `shared` (and the todos under
them) reach teammates; the rest stay on the device that wrote them.

## Open the database

coven owns the connection. The host opens it once with
[`Database::open`](rustdoc:method:coven::database::Database::open), handing over
the file path, the set of tables that sync, this device's id, and a closure that
creates the app's own tables. coven runs its bookkeeping migration first, then
calls the closure, seeds its last-writer-wins clock off the rows already on disk,
attaches the change-capture session to the synced tables, and spawns the thread
that owns the connection.

```rust
use coven::Database;
use coven::sync::session::SyncedTable;

const SCHEMA: &str = "
CREATE TABLE workspaces (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    _updated_at TEXT NOT NULL
);
CREATE TABLE lists (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id),
    name TEXT NOT NULL,
    shared INTEGER NOT NULL DEFAULT 0,
    _updated_at TEXT NOT NULL
);
CREATE TABLE todos (
    id TEXT PRIMARY KEY,
    list_id TEXT NOT NULL REFERENCES lists(id),
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0,
    _updated_at TEXT NOT NULL
);
";

let (db, stamper) = Database::open(
    &db_path,
    vec![
        SyncedTable::new("workspaces"),
        SyncedTable::new("lists").gated_by("shared"),
        SyncedTable::new("todos"),
    ],
    device_id.clone(),
    |conn| conn.execute_batch(SCHEMA).map_err(Into::into),
)?;
```

Every synced table carries an `id` text primary key at column 0 and an
`_updated_at TEXT NOT NULL` column. The
[`SyncedTable`](rustdoc:struct:coven::sync::session::SyncedTable) values declare how
each table is gated:
[`new`](rustdoc:method:coven::sync::session::SyncedTable::new) syncs every row,
[`gated_by`](rustdoc:method:coven::sync::session::SyncedTable::gated_by) makes a
row sync only while its boolean column is true, and
[`gated_by_descendants`](rustdoc:method:coven::sync::session::SyncedTable::gated_by_descendants)
keeps an ancestor row alive only while a gated descendant survives. Here `lists`
is a gated root, and `todos` inherit that gate down the foreign key. Tables you
don't pass are local-only and never leave the device. The gating rules are in
[Local data](/docs/local-data).

`open` returns the handle and an
[`UpdatedAtStamper`](rustdoc:struct:coven::sync::hlc::UpdatedAtStamper). The
stamper is already seeded past every value on disk, so a write made right after a
restart can't mint a stamp that sorts behind an existing row.

## Write a row

The host runs all of its SQL through
[`db.call`](rustdoc:method:coven::database::Database::call), an async method that
takes a closure over the connection. coven re-exports the exact rusqlite it owns,
so use `coven::rusqlite` rather than depending on rusqlite directly. Bind
[`stamper.stamp()`](rustdoc:method:coven::sync::hlc::UpdatedAtStamper::stamp) into
the `_updated_at` column of every synced-row write; that value is coven's
register for ordering writes across devices. The session attached inside the
connection captures the write for the next sync cycle.

```rust
use coven::rusqlite::params;

db.call({
    let stamp = stamper.stamp();
    move |conn| {
        conn.execute(
            "INSERT INTO todos (id, list_id, title, done, _updated_at)
             VALUES (?1, ?2, ?3, 0, ?4)",
            params![todo_id, list_id, title, stamp],
        )?;
        Ok(())
    }
})
.await?;
```

Don't read the stamp as a wall-clock time or compare two of them as dates. It is
an opaque clock value coven advances past pulled rows so a later local write
always sorts after them.

## Read a row

Reads go through the same `db.call`. Nothing about reading is coven-specific, the
closure just runs your query.

```rust
let titles: Vec<String> = db
    .call(move |conn| {
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

A local-only app stops here: open the database, write and read through `db.call`,
and never build any of the sync machinery below.

## Turn on sync

Sync needs an encryption key and a cloud provider. Keys come from the OS keyring;
the host names its keyring service once at startup with
[`set_keyring_service`](rustdoc:fn:coven::keys::set_keyring_service), then builds
the [`KeyService`](rustdoc:struct:coven::keys::KeyService) and
[`EncryptionService`](rustdoc:struct:coven::encryption::EncryptionService) for the
library.

```rust
coven::keys::set_keyring_service("todos");
let key_service = coven::keys::KeyService::new(library_id.clone());
let encryption_key = key_service.get_or_create_encryption_key()?;
let encryption_service = coven::encryption::EncryptionService::new(&encryption_key)?;
```

Once a provider is connected, build the
[`SyncManager`](rustdoc:struct:coven::sync::sync_manager::SyncManager). It takes
the same `db` handle (and shares its clock), a config provider that returns the
host's current config on each call, and a wall-clock source. Which rows carry blobs
is declared on the synced tables passed to `open` (see [Attachments](#attachments)),
not here. Construction is synchronous and never fails, the seeding already happened
in `open`.

```rust
use std::sync::Arc;

let config_provider = Arc::new(move || app_state.current_config());

let manager = coven::sync::sync_manager::SyncManager::new(
    config_provider,
    key_service,
    encryption_service,
    db.clone(),
    Arc::new(coven::clock::SystemClock),
    None, // optional BlobUploadObserver
);

manager.start_sync().await;
```

[`start_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::start_sync)
builds the cloud home from current config and spawns the background loop. After a
write, nudge the loop with
[`trigger_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::trigger_sync)
so the local edit goes out promptly; the loop also runs on its own timer, so a
missed trigger still syncs.

```rust
manager.trigger_sync();
```

The trigger is a no-op until the loop is running, so the host can call it after
every write without checking.

## React to remote changes

The loop emits a
[`SyncLoopStatus`](rustdoc:struct:coven::sync::sync_loop::SyncLoopStatus) after
each cycle. The host reads it through the loop handle's
[`subscribe`](rustdoc:method:coven::sync::sync_loop::SyncLoopHandle::subscribe), a
broadcast receiver. When a pull applied rows, `data_changed` is true and
`row_changes` carries the rows so the host can refresh the affected views.

```rust
if let Some(handle) = manager.sync_loop_handle() {
    let mut status = handle.subscribe();
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
}
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
[`BlobScopeSpec`](rustdoc:enum:coven::sync::session::BlobScopeSpec)
(`Master` for a key every member holds, `Derived` for a fixed per-scope key, or
`ItemColumn` for a per-item key keyed by a column), and a
[`BlobSync`](rustdoc:enum:coven::blob::BlobSync) retention class (`Mirrored` to keep
it on every device, `OnDemand` to fetch it on first read):

```rust
use coven::blob::BlobSync;
use coven::sync::session::BlobDecl;

// In the set you pass to `Database::open`, declare the blob on `todos`:
//   blob id = the row's primary key; opaque home, so no cloud_path column;
//   master-scoped; kept on every device.
SyncedTable::new("todos").carries_blob(BlobDecl::new("todo-files", BlobSync::Mirrored))
```

coven resolves the declaration against the live schema each cycle and derives every
blob a row references itself — what to upload on push, what to download on pull,
whose cache to drop on a delete, and what to backfill after a snapshot bootstrap.
It encrypts the file on upload; on pull it downloads, decrypts, and writes the
plaintext into its own [cache](/docs/cache), which the host reads back through
`read_blob`. The host stages a blob's bytes into the cache with
[`stage_blob`](rustdoc:fn:coven::blob::cache::stage_blob) when it writes the
blob-bearing row, and can enqueue out-of-band uploads/deletes with
[`enqueue_upload`](rustdoc:method:coven::database::Database::enqueue_upload) and
[`enqueue_delete`](rustdoc:method:coven::database::Database::enqueue_delete). The
[Blobs](/docs/blobs) page covers the declaration, the outbox, and the cloud layout,
and the [Cache](/docs/cache) page covers the device-local read side.

## Share with a teammate

A library starts with one member, the device that created it. To add a teammate,
the owner calls
[`invite_member`](rustdoc:method:coven::sync::sync_manager::SyncManager::invite_member)
with the teammate's public key and a
[`MemberRole`](rustdoc:enum:coven::sync::membership::MemberRole) (`Owner`,
`Member`, or read-only `Follower`). It returns a code the teammate redeems.

```rust
let invite_code = manager
    .invite_member(teammate_pubkey_hex, coven::sync::membership::MemberRole::Member)
    .await?;
```

On the teammate's device,
[`join_from_invite_code`](rustdoc:fn:coven::sync::join::join_from_invite_code)
decodes the code, runs the provider's auth flow, unwraps the library key sealed to
the teammate's key, downloads the snapshot, and pulls the changesets written
since. It returns a `Config` for the now-local library; from there the teammate
opens the database and starts a `SyncManager` exactly as above.
[`remove_member`](rustdoc:method:coven::sync::sync_manager::SyncManager::remove_member)
rotates the library key and returns its new fingerprint. The signed membership
chain, key wrapping, and the join flow are covered in [Sharing](/docs/sharing).
