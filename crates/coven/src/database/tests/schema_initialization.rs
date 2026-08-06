use crate::database::Database;
use crate::{Migration, RowIdentity, SyncedTable};
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
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "unmarked-schema-open".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
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

    let conn = Connection::open(&path).expect("inspect rejected database");
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
