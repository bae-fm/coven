use coven::{Config, Coven, CovenError, DbError, Migration, RowIdentity, StoreDir, SyncedTable};

#[tokio::test]
async fn read_on_write_path_is_rejected_after_payload_spooling() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let config = Config::with_defaults(
        "sql-write-tripwire".to_string(),
        "test-device".to_string(),
        "SQL write tripwire".to_string(),
    );
    let handle = Coven::builder(store_dir, config)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )])
        .open()
        .expect("open coven store");

    let result = handle
        .write(|sql| {
            sql.query_row("SELECT count(*) FROM notes", [], |_row| Ok(()))
                .map_err(CovenError::from)
        })
        .await;
    assert!(matches!(
        result,
        Err(CovenError::Database(error))
            if matches!(error.as_ref(), DbError::ReadOnlyWriteTransaction)
    ));
}

#[tokio::test]
async fn no_op_update_is_still_a_write() {
    let temp = tempfile::tempdir().expect("store directory");
    let store_dir = StoreDir::new_ephemeral(temp.path());
    let config = Config::with_defaults(
        "sql-no-op-write".to_string(),
        "test-device".to_string(),
        "SQL no-op write".to_string(),
    );
    let handle = Coven::builder(store_dir, config)
        .synced_tables(vec![SyncedTable::new("notes", RowIdentity::SharedKey)])
        .migrations(vec![Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY,
                body TEXT NOT NULL,
                _updated_at TEXT NOT NULL
            ) STRICT;",
        )])
        .open()
        .expect("open coven store");

    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET body = body WHERE id = 'missing'", [])?;
            Ok::<_, CovenError>(())
        })
        .await
        .expect("no-op update uses the write path");
}
