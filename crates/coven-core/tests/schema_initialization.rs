use std::path::Path;

use coven_core::database::Database;
use coven_core::{Migration, RowIdentity, SyncedTable, WritePolicy};
use rusqlite::Connection;

#[test]
fn ordinary_open_rejects_coven_schema_without_initialization_marker() {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("unmarked-coven-schema.sqlite");
    let conn = Connection::open(&path).expect("open database");
    conn.execute_batch(
        "CREATE TABLE protocol_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) STRICT;",
    )
    .expect("plant Coven schema without metadata");
    drop(conn);

    let error = match Database::open(
        &path,
        vec![SyncedTable::new("things", RowIdentity::SharedKey)],
        coven_core::blob::BLOB_TOMBSTONE_GRACE,
        coven_core::blob::TransferLimits::serial(),
        WritePolicy::MergeConcurrent,
        "unmarked-schema-open".to_string(),
        &[Migration::sql(
            1,
            "things",
            "CREATE TABLE things (
                id TEXT PRIMARY KEY,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    ) {
        Ok(_) => panic!("ordinary open must reject an unmarked Coven schema"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("initialization marker"), "{error}");

    assert_rejected_database_unchanged(&path);
}

fn assert_rejected_database_unchanged(path: &Path) {
    let conn = Connection::open(path).expect("inspect rejected database");
    let marker_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM protocol_state WHERE key = 'coven_initialized'",
            [],
            |row| row.get(0),
        )
        .expect("count initialization markers");
    let host_table_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'things'",
            [],
            |row| row.get(0),
        )
        .expect("count host tables");
    assert_eq!(marker_count, 0);
    assert_eq!(host_table_count, 0);
}
