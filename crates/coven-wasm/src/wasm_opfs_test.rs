//! Headless wasm proof that coven's `Database` persists on OPFS through its own
//! API: a journaled write records a pending changeset, and a row written before a
//! drop reads back after reopening the same database.
//!
//! Runs in a dedicated Worker because the opfs-sahpool VFS's sync access handles
//! exist only there. Drive it with:
//! `wasm-pack test --headless --firefox`.

use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

use crate::database::{Database, DbError};
use crate::migration::Migration;
use crate::sync::session::SyncedTable;
use crate::wasm::install_browser_storage;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Two synced tables — a plain one and a foreign-key child — so a journaled
/// write's capture session attaches more than one table, as a real store does.
fn synced_tables() -> Vec<SyncedTable> {
    vec![SyncedTable::new("items"), SyncedTable::new("item_tags")]
}

/// The host migration step: both tables carry the required `id` text primary key
/// at column 0 and an `_updated_at TEXT NOT NULL` register column.
///
/// Idempotent (`IF NOT EXISTS`). The migration ladder runs at every open, but on a
/// reopen `PRAGMA user_version` is already at the top so this step does not re-run
/// (the `IF NOT EXISTS` is belt-and-suspenders): the tables persist in OPFS, so the
/// reopen below reads them without re-creating.
fn migrate(conn: &rusqlite::Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            _updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS item_tags (
            id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            _updated_at TEXT NOT NULL,
            FOREIGN KEY (item_id) REFERENCES items (id) ON DELETE CASCADE
        );",
    )
    .map_err(DbError::from)
}

/// Open a `Database` on the OPFS-backed VFS at `name`. The browser VFS hashes the
/// SQLite path into a flat OPFS storage name, so this path resolves to the same
/// OPFS file on every open with the same `name`.
fn open(name: &str) -> Database {
    let migrations = vec![Migration::run(1, "test-schema", migrate)];
    let (db, _stamper) = Database::open(
        std::path::Path::new(name),
        synced_tables(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        "wasm-test-device".to_string(),
        &migrations,
    )
    .expect("open Database on OPFS VFS");
    db
}

/// Write a row through coven's journaled write path, confirm it recorded a pending
/// changeset, drop the `Database`, reopen the same OPFS file, and read the row back
/// — proving the connection is durable in OPFS through coven's own `Database`.
#[wasm_bindgen_test]
async fn opfs_database_persists_through_drop_and_reopen() {
    console_error_panic_hook::set_once();

    // Make the opfs-sahpool VFS the default before opening any connection.
    // Idempotent: a second call returns the existing registration.
    install_browser_storage()
        .await
        .expect("install opfs-sahpool VFS");
    install_browser_storage()
        .await
        .expect("install is idempotent");

    // A unique name per run so reruns don't read a prior run's row (the OPFS
    // store outlives the test process).
    let name = format!("coven-opfs-test-{}.db", uuid::Uuid::new_v4());

    {
        let db = open(&name);

        // Write a synced row through coven's journaled write path — the same path
        // the host's `exec` takes — so the write records into the pending-changeset
        // journal as it commits.
        crate::wasm_test_support::journaled_exec(
            &db,
            "INSERT INTO items (id, title, _updated_at) VALUES \
             ('item-1', 'durable title', '2026-01-01T00:00:00Z-0000-wasm-test-device')",
        )
        .await
        .expect("insert row");

        // The journaled write recorded the change: the INSERT into a synced table
        // left exactly one pending changeset for a sync cycle to push.
        let pending = db
            .call(|conn| {
                conn.query_row("SELECT COUNT(*) FROM pending_changesets", [], |r| {
                    r.get::<_, i64>(0)
                })
                .map_err(DbError::from)
            })
            .await
            .expect("count pending changesets");
        assert_eq!(
            pending, 1,
            "the journaled write recorded no pending changeset for an INSERT into a synced table"
        );

        // Drop the Database, closing the OPFS file. The row is now only durable
        // in OPFS, not in this process's memory.
    }

    // Reopen the same OPFS file and read the row back.
    {
        let db = open(&name);
        let title = db
            .call(|conn| {
                conn.query_row("SELECT title FROM items WHERE id = 'item-1'", [], |r| {
                    r.get::<_, String>(0)
                })
                .map_err(DbError::from)
            })
            .await
            .expect("read row back after reopen");
        assert_eq!(
            title, "durable title",
            "row written before drop did not survive reopen — OPFS persistence failed"
        );
    }
}
