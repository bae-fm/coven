# Example

Here's a simple shared todos app that demonstrates how coven works. The app owns
the todo schema, UI, and SQLite driver. coven owns change capture, encrypted
sync, membership, storage movement, and blob transfer.

## Host-owned schema

The app stores todos and optional attachment metadata in normal SQLite tables.
Every synced table has `id` at column `0` and `_updated_at` for row-level
last-writer-wins conflict resolution.

```sql
CREATE TABLE todos (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    _updated_at TEXT NOT NULL
);

CREATE TABLE todo_attachments (
    id TEXT PRIMARY KEY,
    todo_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    local_path TEXT NOT NULL,
    _updated_at TEXT NOT NULL
);
```

Apply [`db::MIGRATION_SQL`](rustdoc:const:coven::db::MIGRATION_SQL) beside the
app schema. That creates coven's `sync_cursors`, `sync_state`, and
`cloud_outbox` tables.

```rust
connection.execute_batch(coven::db::MIGRATION_SQL)?;
connection.execute_batch(APP_SCHEMA)?;

coven::sync::session::set_synced_tables(&["todos", "todo_attachments"]);
```

## SQLite session changesets

When a user edits a todo, the app writes its own row and updates `_updated_at`
with the current hybrid logical clock timestamp. coven reads changes from the
SQLite session extension attached to the host write connection.

```rust
connection.execute(
    "
    INSERT INTO todos (id, title, completed, _updated_at)
    VALUES (?1, ?2, 0, ?3)
    ON CONFLICT(id) DO UPDATE SET
        title = excluded.title,
        _updated_at = excluded._updated_at
    ",
    (todo_id, title, hlc_timestamp),
)?;

manager.trigger_sync();
```

The database adapter exposes coven's bookkeeping tables and the raw SQLite
connection pointer.

```rust
struct TodoDb {
    // Host-owned SQLite connection, pool, actor, or database service.
}

#[async_trait::async_trait]
impl coven::db::SyncBookkeeping for TodoDb {
    async fn get_sync_state(
        &self,
        key: &str,
    ) -> Result<Option<String>, coven::db::DbError> {
        // SELECT value FROM sync_state WHERE key = ?
        todo!()
    }

    async fn set_sync_state(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(), coven::db::DbError> {
        // INSERT INTO sync_state ... ON CONFLICT DO UPDATE
        todo!()
    }

    // Implement cursor and cloud_outbox methods against coven's tables.
}

#[async_trait::async_trait]
impl coven::db::RawDbHandle for TodoDb {
    async fn raw_write_handle(
        &self,
    ) -> Result<*mut libsqlite3_sys::sqlite3, coven::db::DbError> {
        // Return the sqlite3 write connection pointer.
        todo!()
    }
}
```

Any type implementing both traits is a [`db::SyncDb`](rustdoc:trait:coven::db::SyncDb).

## Multi-writer sync

Each device has its own author key. coven signs each outgoing changeset, wraps it
in an encrypted envelope, pushes it through storage, then verifies and applies
remote envelopes on pull.

```rust
use std::sync::Arc;

let db: Arc<dyn coven::db::SyncDb> = Arc::new(todo_db);
let key_service = coven::keys::KeyService::new(dev_mode, library_id.clone());
let encryption_key = key_service.get_or_create_encryption_key()?;
let encryption_service = coven::encryption::EncryptionService::new(&encryption_key)?;
let clock: coven::clock::ClockRef = Arc::new(coven::clock::SystemClock);

let config_provider: coven::sync::sync_manager::ConfigProvider = Arc::new({
    let config = config.clone();
    move || config.clone()
});

let manager = coven::sync::sync_manager::SyncManager::new(
    config_provider,
    key_service,
    encryption_service,
    db,
    clock,
    Arc::new(TodoBlobPlan),
    None,
);

manager.start_sync().await;
```

If two members edit the same todo, the row with the later `_updated_at` value
wins. The app keeps that rule visible in its schema instead of sending conflict
policy to a server.

## Bring-your-own storage

The todo app chooses a `CloudHome` provider through `config::Config`: S3, Google
Drive, Dropbox, OneDrive, iCloud, or local storage. The provider stores encrypted
changesets, membership data, and encrypted blobs. It does not see todo titles or
attachment contents.

```rust
let config = coven::config::Config::with_defaults(
    library_dir,
    device_id,
    Some(coven::config::CloudHomeConfig {
        provider: coven::config::CloudProvider::Dropbox,
        account_email: Some(user_email),
        container: None,
    }),
);
```

## Cryptographic membership

Sharing a todo list means adding another member to the signed membership chain.
The library key is wrapped to that member's X25519 key, and their changesets are
accepted only when the chain authorizes them.

```rust
let invite_code = manager
    .invite_member(
        teammate_public_key_hex,
        coven::sync::membership::MemberRole::Writer,
    )
    .await?;

let members = manager.get_members().await?;
```

For recovery, the app can show a restore code generated from the same manager.

```rust
let restore_code = manager.generate_restore_code()?;
```

## Encrypted blob store

Attachments are files referenced by `todo_attachments` rows. The app maps row
changes to [`blob::BlobRef`](rustdoc:struct:coven::blob::BlobRef) values; coven
encrypts and moves the files through the cloud outbox.

```rust
struct TodoBlobPlan;

impl coven::blob::BlobPlan for TodoBlobPlan {
    fn blobs_to_push(
        &self,
        changes: &[coven::changeset::RowChange],
    ) -> Vec<coven::blob::BlobRef> {
        changes
            .iter()
            .filter(|change| change.table == "todo_attachments")
            .filter_map(|change| {
                Some(coven::blob::BlobRef {
                    namespace: "todo-attachments".to_string(),
                    id: change.pk()?.to_string(),
                    local_path: attachment_path(change.pk()?),
                    scope: coven::blob::BlobScope::Derived(change.pk()?.to_string()),
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

The synced todo row and the encrypted attachment move through the same sync
cycle: row data as signed encrypted changesets, file data as encrypted opaque
blobs.
