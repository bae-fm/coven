# Example

This page wires coven into a host app end to end. The app owns its schema, its
SQLite driver, its UI, and its product policy. coven owns change capture,
encrypted sync, membership, storage movement, and blob transfer. The two meet at
a handful of traits the host implements and one startup sequence whose order
matters.

The example is a todos app: a `workspaces` table holds `lists`, a `list` holds
`todos`, and a todo can carry `todo_attachments`. A list has a boolean `shared`
column.

## Host schema and coven's tables

The host keeps its own tables. Every table that syncs carries `id` at column `0`
and an `_updated_at` column holding the row's hybrid logical clock stamp (coven
compares these for last-writer-wins; never parse one as wall-clock time).

```sql
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

CREATE TABLE todo_attachments (
    id TEXT PRIMARY KEY,
    todo_id TEXT NOT NULL REFERENCES todos(id),
    file_name TEXT NOT NULL,
    _updated_at TEXT NOT NULL
);
```

coven owns three bookkeeping tables in the same database: `sync_cursors` (one
`last_seq` per peer device), `sync_state` (key/value, including the clock's
persisted high-water mark and snapshot bookkeeping), and `cloud_outbox` (pending
blob uploads and deletes). The host creates them by applying
[`MIGRATION_SQL`](rustdoc:const:coven::db::MIGRATION_SQL) beside its own schema.

```rust
connection.execute_batch(coven::db::MIGRATION_SQL)?;
connection.execute_batch(APP_SCHEMA)?;
```

`MIGRATION_SQL` uses `CREATE TABLE IF NOT EXISTS`, so it is idempotent on a fresh
table but will not add columns to a `cloud_outbox` left over from an older coven
version. A host upgrading coven adds new columns (such as `attempt_count`,
`last_error`, `last_attempt_at`) through its own `ALTER TABLE` migration; the
`IF NOT EXISTS` guard will not do it.

## The database traits

coven reaches the host's database through two traits, and uses the pair together
as [`SyncDb`](rustdoc:trait:coven::db::SyncDb).

[`SyncBookkeeping`](rustdoc:trait:coven::db::SyncBookkeeping) is the SQL coven
needs against its own tables: read and write `sync_state`, read all cursors as a
`device_id -> last_seq` map and upsert one, list and remove `cloud_outbox`
entries, and record a failed upload attempt. coven never runs this SQL itself; it
calls these methods so the host stays the only writer of its connection and coven
imposes no SQLite driver.

```rust
#[async_trait::async_trait]
impl coven::db::SyncBookkeeping for TodoDb {
    async fn get_sync_state(&self, key: &str)
        -> Result<Option<String>, coven::db::DbError> {
        // SELECT value FROM sync_state WHERE key = ?
        todo!()
    }

    async fn set_sync_state(&self, key: &str, value: &str)
        -> Result<(), coven::db::DbError> {
        // INSERT INTO sync_state(key, value) VALUES (?, ?)
        //   ON CONFLICT(key) DO UPDATE SET value = excluded.value
        todo!()
    }

    async fn get_all_sync_cursors(&self)
        -> Result<std::collections::HashMap<String, u64>, coven::db::DbError> {
        // SELECT device_id, last_seq FROM sync_cursors
        todo!()
    }

    async fn set_sync_cursor(&self, device_id: &str, seq: u64)
        -> Result<(), coven::db::DbError> {
        // upsert into sync_cursors
        todo!()
    }

    // get_pending_cloud_uploads, get_pending_cloud_deletes,
    // has_pending_cloud_uploads, remove_cloud_outbox_entry,
    // record_cloud_upload_failure: all against cloud_outbox.
}
```

The pending-upload and delete methods return
[`OutboxEntry`](rustdoc:struct:coven::db::OutboxEntry) rows
([`OutboxOperation`](rustdoc:enum:coven::db::OutboxOperation) is `Upload` or
`Delete`). `has_pending_cloud_uploads` gates the changeset push: coven holds a
changeset back while a blob it references is still queued, so a peer never pulls
a row whose attachment has not landed yet.

[`RawDbHandle`](rustdoc:trait:coven::db::RawDbHandle) hands coven the raw
`*mut sqlite3` write pointer. The session extension attaches to this connection
to capture changes, and the register clock scans it for `MAX(_updated_at)` at
startup. It must be the same connection the host writes its rows through.

```rust
#[async_trait::async_trait]
impl coven::db::RawDbHandle for TodoDb {
    async fn raw_write_handle(&self)
        -> Result<*mut libsqlite3_sys::sqlite3, coven::db::DbError> {
        // Return the sqlite3 write connection pointer.
        todo!()
    }
}
```

[`SyncDb`](rustdoc:trait:coven::db::SyncDb) is implemented for any type that
implements both, so `TodoDb` is a `SyncDb` with no extra code.

## Declaring synced tables

The host names which tables sync, and how each is gated, with
[`set_synced_tables`](rustdoc:fn:coven::sync::session::set_synced_tables). It
takes [`SyncedTable`](rustdoc:enum:coven::sync::session::SyncedTable) values, not
bare names, because each table can carry a gate.

```rust
use coven::sync::session::{set_synced_tables, SyncedTable};

set_synced_tables(&[
    SyncedTable::new("workspaces").gated_by_descendants(),
    SyncedTable::new("lists").gated_by("shared"),
    SyncedTable::new("todos"),
    SyncedTable::new("todo_attachments"),
]);
```

`lists` is a gated root: a list syncs only while its `shared` column is true, and
`todos` and `todo_attachments` inherit that gate down their foreign keys.
`workspaces` sits above the gate, so it is marked
`gated_by_descendants()` and syncs only while it still has a shared list under
it. The gating forms, the keep rule, and what happens when a gate flips are
covered in [Local data](/docs/local-data).

This call must run once at startup, before the register clock opens and before
any sync session. coven holds the first set it is given (a second call with a
different set warns and keeps the first). A never-registered set would make sync
capture nothing and produce only empty changesets, so `init_sync` refuses: it
logs an error and does not start the loop, rather than letting sync silently
become a no-op.

## Startup order

Four steps run in order, and the order is load-bearing.

1. **`set_synced_tables`.** Without it the register clock has no tables to scan
   and sync has nothing to capture.

2. **[`RegisterClock::open`](rustdoc:method:coven::sync::register_clock::RegisterClock::open).**
   This builds coven's `_updated_at` clock and seeds it past every value already
   on disk: it reads the persisted high-water mark from `sync_state` and scans
   `MAX(_updated_at)` across every synced table on the raw write handle, seeding
   the clock to the larger. Seeding past on-disk rows is what keeps a write made
   right after a restart from minting a stamp behind an existing row and losing a
   merge. Because it scans the synced tables, it has to run after
   `set_synced_tables`; because it reads the database, a registered table that
   does not exist on disk is a host integration error and surfaces as `Err`, not
   a skip.

   ```rust
   let register_clock =
       coven::sync::register_clock::RegisterClock::open(device_id.clone(), db.as_ref())
           .await?;
   ```

3. **Inject the stamper into every synced-row write.** The host obtains an
   [`UpdatedAtStamper`](rustdoc:struct:coven::sync::hlc::UpdatedAtStamper) from
   the register clock and calls
   [`stamp`](rustdoc:method:coven::sync::hlc::UpdatedAtStamper::stamp) to produce
   the `_updated_at` value for each write. The stamper wraps the same clock coven
   later advances on pull, so the host's writes and coven's pulled rows order
   against one register. Inject it before the first synced-row write: a row
   stamped off some other clock breaks last-writer-wins for that row.

   ```rust
   let stamper = register_clock.updated_at_stamper();

   connection.execute(
       "INSERT INTO todos (id, list_id, title, done, _updated_at)
        VALUES (?1, ?2, ?3, 0, ?4)
        ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            done = excluded.done,
            _updated_at = excluded._updated_at",
       (todo_id, list_id, title, stamper.stamp()),
   )?;
   ```

4. **Construct [`SyncManager`](rustdoc:struct:coven::sync::sync_manager::SyncManager),
   when a provider is connected.** A local-only library never reaches this step:
   steps 1 through 3 give it a stamper and a working local database with no
   encryption key and no cloud provider. The manager is built lazily, once the
   user connects storage.

## ConfigProvider

[`Config`](rustdoc:struct:coven::config::Config) is a plain runtime struct the
host fills from its own config: `library_id`, `device_id`, `library_dir`,
`library_name`, two encryption-key hint fields, and a
[`CloudHomeConfig`](rustdoc:struct:coven::config::CloudHomeConfig) naming the
provider and its settings. coven never persists `Config`; the host owns its
config file.

The manager does not take a `Config` snapshot. It takes a
[`ConfigProvider`](rustdoc:type:coven::sync::sync_manager::ConfigProvider), an
`Arc<dyn Fn() -> Config + Send + Sync>` coven calls fresh on each operation
(`start_sync`, `get_members`, and the rest). A host whose config is reactive
returns the current value each call, so a change (swapping S3 buckets, say)
propagates without rebuilding the manager. The closure must be cheap and return
the live config, not a cached snapshot.

```rust
let config_provider: coven::sync::sync_manager::ConfigProvider =
    Arc::new(move || app_state.current_config());
```

## Constructing the manager

[`SyncManager::new`](rustdoc:method:coven::sync::sync_manager::SyncManager::new)
is synchronous and infallible: the one async, fallible step, seeding, already
happened in `RegisterClock::open`. It borrows the register clock's shared
`Arc<Hlc>`, so the loop's advance-on-pull and the host's stamps drive one clock.

```rust
use std::sync::Arc;

let db: Arc<dyn coven::db::SyncDb> = Arc::new(todo_db);

coven::keys::set_keyring_service("todos");
let key_service =
    coven::keys::KeyService::new(coven::config::Config::is_dev_mode(), library_id.clone());
let encryption_key = key_service.get_or_create_encryption_key()?;
let encryption_service = coven::encryption::EncryptionService::new(&encryption_key)?;

let clock: coven::clock::ClockRef = Arc::new(coven::clock::SystemClock);

let manager = coven::sync::sync_manager::SyncManager::new(
    config_provider,
    key_service,
    encryption_service,
    db,
    clock,
    &register_clock,
    Arc::new(TodoBlobPlan),
    None, // optional BlobUploadObserver
);
```

[`KeyService`](rustdoc:struct:coven::keys::KeyService) reads the library's
encryption key and cloud credentials, from the OS keyring in production and from
the environment in dev mode (`Config::is_dev_mode`, set by `COVEN_DEV_MODE` or a
`.env` file). Call `set_keyring_service` once with the app's name so the keyring
entries are attributed to it.
[`EncryptionService`](rustdoc:struct:coven::encryption::EncryptionService) must
be built from the real decrypted key; for a brand-new library the host mints and
stores one first (`get_or_create_encryption_key` does both).
[`ClockRef`](rustdoc:type:coven::clock::ClockRef) is coven's wall-clock source
for OAuth expiry and request signing; production uses
[`SystemClock`](rustdoc:struct:coven::clock::SystemClock), tests use the fakes in
that module. This clock is separate from the register clock: one is wall time,
the other is the `_updated_at` register.

The last two arguments are the blob handling: a
[`BlobPlan`](rustdoc:trait:coven::blob::BlobPlan), and an optional
[`BlobUploadObserver`](rustdoc:trait:coven::blob::BlobUploadObserver) for upload
lifecycle callbacks (`None` is a no-op).

## Start and stop

[`start_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::start_sync)
reads the current config, builds the cloud home from it, and (if
`sync_enabled` is true) spawns the background sync loop.
[`stop_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::stop_sync)
drops both. The host drives this on connect and disconnect, no app restart: call
`start_sync` at launch if a provider is already configured, and again right after
the user connects one.

```rust
manager.start_sync().await;
```

[`is_sync_ready`](rustdoc:method:coven::sync::sync_manager::SyncManager::is_sync_ready)
reports whether the loop is running.

## Writing and triggering

The host writes its rows normally, stamping `_updated_at` with the injected
stamper, then nudges the loop with
[`trigger_sync`](rustdoc:method:coven::sync::sync_manager::SyncManager::trigger_sync).
The loop also runs on its own timer, so a missed trigger still syncs; the trigger
just makes a local edit go out promptly.

```rust
// ... write the todo row with stamper.stamp() ...
manager.trigger_sync();
```

The trigger is a no-op until a provider is connected and the loop is running, so
the host can call it unconditionally after a write.

## Reacting to status

The sync loop emits a
[`SyncLoopStatus`](rustdoc:struct:coven::sync::sync_loop::SyncLoopStatus) after
each cycle. The host reads it through the loop handle's
[`subscribe`](rustdoc:method:coven::sync::sync_loop::SyncLoopHandle::subscribe),
a Tokio broadcast receiver.

```rust
if let Some(handle) = manager.sync_loop_handle() {
    let mut status = handle.subscribe();
    tokio::spawn(async move {
        while let Ok(s) = status.recv().await {
            if s.data_changed {
                // s.row_changes lists the rows a pull applied; map them to
                // domain events and refresh the UI.
            }
            if let Some(msg) = s.error {
                // show msg in a banner; None clears it.
            }
        }
    });
}
```

`data_changed` is true when a pull applied at least one changeset, and
`row_changes` then carries the applied rows so the host can turn them into its
own change notifications. `error` holds a user-facing message coven has already
worded (schema too old, a blob download that failed); the host shows it as is and
does not reword it.

## Blobs

A todo attachment is a file referenced by a `todo_attachments` row.
[`BlobPlan`](rustdoc:trait:coven::blob::BlobPlan) maps the rows in a changeset to
the files coven should move: `blobs_to_push` on the way out, `blobs_to_pull` on
the way in. Each returns a
[`BlobRef`](rustdoc:struct:coven::blob::BlobRef) naming a namespace, the blob id,
its local plaintext path, and a [`BlobScope`](rustdoc:enum:coven::blob::BlobScope)
(`Master` for a key all members hold, or `Derived(scope_id)` for a per-scope
key).

```rust
struct TodoBlobPlan;

impl coven::blob::BlobPlan for TodoBlobPlan {
    fn blobs_to_push(
        &self,
        changes: &[coven::changeset::RowChange],
    ) -> Vec<coven::blob::BlobRef> {
        changes
            .iter()
            .filter(|c| c.table == "todo_attachments")
            .filter_map(|c| {
                let id = c.pk()?.to_string();
                Some(coven::blob::BlobRef {
                    namespace: "todo-attachments".to_string(),
                    local_path: attachment_path(&id),
                    scope: coven::blob::BlobScope::Master,
                    id,
                })
            })
            .collect()
    }

    fn blobs_to_pull(
        &self,
        changes: &[coven::changeset::RowChange],
    ) -> Vec<coven::blob::BlobRef> {
        self.blobs_to_push(changes)
    }
}
```

coven encrypts the file on upload, drains the `cloud_outbox` queue with its own
retry and backoff, and writes the decrypted bytes to `local_path` on pull. The
file on disk is always plaintext; coven encrypts only the copy in the cloud. The
[Blobs](/docs/blobs) page covers the outbox, the observer, and the cloud layout.

## Members and recovery

The manager carries the host-facing membership calls. They run against the
connected provider's storage. `invite_member` and `remove_member` need a running
sync loop and error with "Sync is not configured" without one. `get_members`
builds its own storage from current config and returns an empty list while sync
is not enabled. `generate_restore_code` needs a configured provider and an
encryption key, not a running loop.

```rust
let invite_code = manager
    .invite_member(teammate_pubkey_hex, coven::sync::membership::MemberRole::Member)
    .await?;

let members: Vec<coven::sync::sync_manager::MemberInfo> = manager.get_members().await?;

manager.remove_member(removed_pubkey_hex).await?; // returns the new key fingerprint

let restore_code = manager.generate_restore_code()?;
```

`invite_member` returns a code the new member redeems to join; `remove_member`
rotates the library key and returns its new fingerprint; `generate_restore_code`
produces a code that recovers the library on a fresh device. Roles are
[`MemberRole`](rustdoc:enum:coven::sync::membership::MemberRole) (`Owner`,
`Member`, `Follower`). The signed chain, key wrapping, and the join and restore
flows are covered in [Sharing](/docs/sharing).
