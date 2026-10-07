use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{ApplyOutcome, DbError};
use crate::{RowIdentity, SyncedTable};
use coven_foundation::id_source::SequentialIds;

#[tokio::test]
async fn download_suppresses_shared_triggers_and_runs_local_triggers_with_applying_true() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let tables = || {
        vec![
            SyncedTable::new("notes", RowIdentity::SharedKey).shared_trigger("shared"),
            SyncedTable::new("totals", RowIdentity::SharedKey),
        ]
    };
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE totals(id TEXT NOT NULL PRIMARY KEY,n INTEGER NOT NULL); CREATE TABLE audit(applying INTEGER); CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN INSERT INTO totals VALUES('all',1) ON CONFLICT(id) DO UPDATE SET n=n+1; END; CREATE TRIGGER local AFTER INSERT ON notes BEGIN INSERT INTO audit VALUES(coven_applying()); END";
    let a = a_store.schema(tables(), schema).await.unwrap();
    let b = b_store.schema(tables(), schema).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('one')").await.unwrap();
    b.apply_downloaded(records(&a).remove(0).into())
        .await
        .unwrap();
    let result = b
        .read(|sql| {
            Ok((
                sql.query_row("SELECT n FROM totals", [], |r| r.get::<_, i64>(0))?,
                sql.query_row("SELECT applying FROM audit", [], |r| r.get::<_, i64>(0))?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(result, (1, 1));
    assert_eq!(count(&b, "_coven_uploads"), 0);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_fingerprint_storage_failure_rolls_back_the_download_and_allows_retry() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let record = records(&a).remove(0);
    b.inspect_writer(|sql| sql.batch("CREATE TRIGGER fail_hash AFTER INSERT ON _coven_fingerprint_leaves BEGIN SELECT RAISE(ABORT,'fingerprint failure'); END").unwrap());
    assert!(
        matches!(b.apply_downloaded(record.clone().into()).await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="fingerprint failure")
    );
    for table in [
        "notes",
        "_coven_rows",
        "_coven_cells",
        "_coven_writes",
        "_coven_positions",
        "_coven_lost",
        "_coven_fingerprint_leaves",
        "_coven_fingerprint_sums",
    ] {
        assert_eq!(count(&b, table), 0, "{table}");
    }
    b.inspect_writer(|sql| sql.batch("DROP TRIGGER fail_hash").unwrap());
    assert_eq!(
        b.apply_downloaded(record.into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(count(&b, "notes"), 1);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_trigger_ending_the_transaction_returns_its_original_sqlite_error() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let mut databases = Vec::new();
    for (store, seconds) in [(&a_store, 2), (&b_store, 1)] {
        databases.push(
            store
                .builder(notes(), vec![crate::Migration::sql(1, "notes", NOTES)])
                .clock(std::sync::Arc::new(
                    coven_foundation::clock::FixedClock::new(
                        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
                    ),
                ))
                .open()
                .await
                .unwrap(),
        );
    }
    let b = databases.pop().unwrap();
    let a = databases.pop().unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','title','body')")
        .await
        .unwrap();
    let record = records(&a).remove(0);
    b.inspect_writer(|sql| sql.batch("CREATE TRIGGER fail_apply AFTER INSERT ON notes BEGIN SELECT RAISE(ROLLBACK,'transaction refused'); END").unwrap());
    assert!(
        matches!(b.apply_downloaded(record.clone().into()).await,Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="transaction refused")
    );
    for table in [
        "notes",
        "_coven_rows",
        "_coven_cells",
        "_coven_writes",
        "_coven_positions",
        "_coven_lost",
        "_coven_fingerprint_leaves",
        "_coven_fingerprint_sums",
    ] {
        assert_eq!(count(&b, table), 0, "{table}");
    }
    b.inspect_writer(|sql| sql.batch("DROP TRIGGER fail_apply").unwrap());
    sql(&b, "INSERT INTO notes VALUES('local','local','')")
        .await
        .unwrap();
    assert_eq!(records(&b)[0].header.timestamp.milliseconds(), 1_000);
    assert_eq!(
        b.apply_downloaded(record.into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

type StoredTables = std::collections::BTreeMap<String, Vec<Vec<crate::types::Value>>>;

fn stored_tables(database: &crate::Database) -> StoredTables {
    database.inspect_writer(|db| {
        let tables = db
            .query(
                "SELECT name FROM main.sqlite_schema WHERE type='table' ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        tables
            .into_iter()
            .map(|table| {
                let rows = db
                    .query(
                        &format!("SELECT * FROM main.{}", crate::sql::identifier(&table)),
                        [],
                        |r| {
                            (0..r.as_ref().column_count())
                                .map(|column| r.get(column))
                                .collect()
                        },
                    )
                    .unwrap();
                (table, rows)
            })
            .collect()
    })
}

async fn assert_rejected_download(
    database: &crate::Database,
    record: crate::DownloadedWrite,
    expected: coven_merge::MergeError,
) {
    let before = stored_tables(database);
    let mut query = database
        .subscribe(|sql| Ok(sql.query("SELECT * FROM notes", [], |r| r.get::<_, String>(0))?));
    query.next().await.unwrap();
    for _ in 0..2 {
        let error = database.apply_downloaded(record.clone()).await.unwrap_err();
        assert!(
            matches!(error, DbError::InvalidWrite { write, error } if write == record.header.position && error == expected)
        );
        assert_eq!(stored_tables(database), before);
        assert!(!query.is_marked_for_rerun());
    }
}

#[tokio::test]
async fn downloaded_timestamps_no_later_than_their_past_are_refused() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','original','')")
        .await
        .unwrap();
    sql(&a, "UPDATE notes SET title='valid'").await.unwrap();
    let writes = records(&a);
    b.apply_downloaded(writes[0].clone().into()).await.unwrap();
    for timestamp in [
        writes[0].header.timestamp,
        coven_merge::Timestamp::new(
            writes[0].header.timestamp.milliseconds() - 1,
            0,
            writes[1].header.position.device,
        )
        .unwrap(),
    ] {
        let mut invalid = writes[1].clone();
        invalid.header.timestamp = timestamp;
        assert_rejected_download(
            &b,
            invalid.into(),
            coven_merge::MergeError::CausalTimestamp(writes[1].header.position),
        )
        .await;
    }
    assert_eq!(
        b.apply_downloaded(writes[1].clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_downloaded_update_naming_a_stale_deleted_generation_is_refused() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    for statement in [
        "INSERT INTO notes VALUES('n','first','')",
        "DELETE FROM notes",
        "INSERT INTO notes VALUES('n','readded','')",
        "UPDATE notes SET title='valid'",
    ] {
        sql(&a, statement).await.unwrap();
    }
    let writes = records(&a);
    for write in &writes[..3] {
        b.apply_downloaded(write.clone().into()).await.unwrap();
    }
    let mut invalid = writes[3].clone();
    assert_eq!(invalid.parts[0].rows[0].change.generation, 3);
    invalid.parts[0].rows[0].change.generation = 2;
    assert_rejected_download(
        &b,
        invalid.into(),
        coven_merge::MergeError::GenerationParity(2),
    )
    .await;
    assert_eq!(
        b.apply_downloaded(writes[3].clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_locally_built_invalid_write_still_panics_and_rolls_back() {
    let store = TestStore::new();
    let db = store.schema(notes(), NOTES).await.unwrap();
    sql(&db, "INSERT INTO notes VALUES('n','original','')")
        .await
        .unwrap();
    // Simulate broken local numbering: the next authored position repeats an
    // already-applied write, which must remain an internal invariant failure.
    db.inspect_writer(|db| {
        db.internal_execute("DELETE FROM _coven_positions", [])
            .unwrap()
    });
    let before = stored_tables(&db);
    let writer = db.clone();
    let panic = tokio::spawn(async move { sql(&writer, "UPDATE notes SET title='invalid'").await })
        .await
        .unwrap_err();
    assert!(panic.is_panic());
    assert_eq!(stored_tables(&db), before);
    db.close().await.unwrap();
}

#[tokio::test]
async fn rejected_timestamps_cannot_advance_skipped_or_excluded_writes() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','initial','')")
        .await
        .unwrap();
    sql(&a, "UPDATE notes SET title='valid'").await.unwrap();
    let writes = records(&a);
    b.apply_downloaded(writes[0].clone().into()).await.unwrap();
    for lost in [false, true] {
        let mut invalid: crate::DownloadedWrite = writes[1].clone().into();
        invalid.header.timestamp = coven_merge::Timestamp::new(
            writes[0].header.timestamp.milliseconds() - 1,
            0,
            invalid.header.position.device,
        )
        .unwrap();
        if lost {
            invalid.header.disposition = coven_format::write::WriteDisposition::Lost(1);
        } else {
            invalid.parts = vec![crate::DownloadedPart::Skipped(coven_merge::Audience::Store)];
        }
        assert_rejected_download(
            &b,
            invalid,
            coven_merge::MergeError::CausalTimestamp(writes[1].header.position),
        )
        .await;
    }
    assert_eq!(
        b.apply_downloaded(writes[1].clone().into()).await.unwrap(),
        ApplyOutcome::Applied
    );
    for db in [a, b] {
        db.close().await.unwrap();
    }
}
