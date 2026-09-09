use crate::{
    ChangesetIdentityError, CovenMigrationPolicy, Database, DatabaseImageTest, DbError,
    HostWriteError, HostWriteOperation, Migration, StoreDatabase, StoreRowWrites, WriteBatch,
};
use coven_foundation::store_dir::StoreDir;
use coven_protocol::synced_schema::{RowIdentity, RowIdentityError, SyncedTable};

#[tokio::test]
async fn captured_routing_rows_do_not_hide_invalid_host_identity_or_escape_rollback() {
    let temporary = tempfile::tempdir().expect("Store directory");
    let directory = StoreDir::new_ephemeral(temporary.path());
    let database = Database::open(
        &directory.db_path(),
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience")],
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        "captured-identity".to_string(),
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        CovenMigrationPolicy::ApplyPending,
        &[Migration::sql(
            1,
            "notes",
            "CREATE TABLE notes (
                id TEXT PRIMARY KEY NOT NULL,
                title TEXT NOT NULL,
                audience TEXT,
                _updated_at TEXT NOT NULL
             ) STRICT;",
        )],
    )
    .expect("open scoped Store");
    let writes = StoreRowWrites::new(StoreDatabase::new(&database));
    let error = writes
        .execute(
            HostWriteOperation::new(WriteBatch::new(), |sql| -> Result<(), DbError> {
                let stamp = sql.stamp();
                sql.execute(
                    "INSERT INTO notes VALUES (?1, 'Valid sibling', NULL, ?2)",
                    ("018f0000-0000-7000-8000-000000000001", &stamp),
                )?;
                sql.execute(
                    "INSERT INTO notes VALUES ('not-a-uuid', 'Invalid sibling', NULL, ?1)",
                    [&stamp],
                )?;
                // The capture owner journals internal routing alongside host
                // rows. Seed that input through internal SQL authority while
                // exercising the real host transaction and identity boundary.
                crate::with_coven_sql_authority(|| -> rusqlite::Result<()> {
                    let routing_id = "a".repeat(64);
                    sql.execute(
                        "INSERT INTO _coven_audience VALUES (?1, NULL, ?2)",
                        (&routing_id, &stamp),
                    )?;
                    sql.execute(
                        "INSERT INTO _coven_row_routes VALUES (?1, 'notes', ?2, ?3)",
                        (&routing_id, "018f0000-0000-7000-8000-000000000001", &stamp),
                    )?;
                    Ok(())
                })?;
                Ok(())
            }),
            Some(coven_keys::encryption::EncryptionService::from_key([7; 32])),
            None,
        )
        .await
        .expect_err("the complete captured write must reject its invalid host identity");
    assert!(
        matches!(
            &error,
            HostWriteError::Database(DbError::ChangesetIdentity(ChangesetIdentityError::Row(
                RowIdentityError::InvalidIndependentUuid { table, value }
            ))) if table == "notes" && value == "not-a-uuid"
        ),
        "routing rows must not mask the invalid host identity: {error:?}",
    );
    let image = DatabaseImageTest::open(&directory.db_path()).expect("inspect durable Store");
    for table in [
        "notes",
        "_coven_audience",
        "_coven_row_routes",
        "store_writes",
        "store_write_partitions",
    ] {
        let count = image
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("inspect rollback");
        assert_eq!(count, 0, "rejected capture left rows in {table}");
    }
}
