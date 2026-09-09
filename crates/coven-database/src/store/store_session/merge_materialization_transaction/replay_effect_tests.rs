use super::*;

#[test]
fn recorded_foreign_key_conflict_identifies_declared_child_without_rowid() {
    for suffix in ["STRICT", "STRICT, WITHOUT ROWID"] {
        let connection = rusqlite::Connection::open_in_memory().expect("open scratch database");
        connection
            .pragma_update(None, "foreign_keys", false)
            .expect("seed an invalid replay graph");
        connection.execute_batch(&format!(
            "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE) STRICT; \
             CREATE TABLE children (id TEXT PRIMARY KEY, parent_code INTEGER, \
                _updated_at TEXT NOT NULL, FOREIGN KEY(parent_code) REFERENCES parents(code)) {suffix}; \
             INSERT INTO parents VALUES ('parent', '001'); \
             INSERT INTO children VALUES ('actual-child', 1, '0000000001000-0000-author');"
        )).expect("seed parent-affinity mismatch");
        let tables = [coven_protocol::synced_schema::SyncedTable::new(
            "children",
            coven_protocol::synced_schema::RowIdentity::SharedKey,
        )];
        let schema = TableSchema::from_db(&connection, &tables).expect("read child schema");
        let directory = tempfile::tempdir().expect("scratch directory");
        let store_dir = coven_foundation::store_dir::StoreDir::new_ephemeral(directory.path());
        let transaction = connection
            .unchecked_transaction()
            .expect("begin replay transaction");
        let write_id = WriteId::from_generated("recorded-parent-edit".to_string());
        let error = MergeMaterializationTransaction::from_store(
            crate::store::store_session::StoreTransaction::new(&transaction, &store_dir),
        )
        .validate_recorded_foreign_keys(&write_id, &schema)
        .expect_err("typed FK conflict");
        let DbError::WriteRebaseConflict(conflict) = error else {
            panic!("expected actual child conflict: {error:?}");
        };
        assert_eq!(conflict.write_id, write_id);
        assert_eq!(
            conflict.affected_rows,
            vec![coven_protocol::write::AffectedRow {
                table: "children".to_string(),
                primary_key: "actual-child".to_string(),
            }]
        );
        assert!(
            matches!(conflict.reason, coven_protocol::write::WriteRebaseConflictReason::Constraint { message } if message.contains("parents"))
        );
    }
}
