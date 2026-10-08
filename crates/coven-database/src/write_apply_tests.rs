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

#[tokio::test]
async fn failure_after_every_remote_mutation_rolls_back_rows_metadata_and_notifications() {
    use coven_foundation::clock::FixedClock;
    use std::{
        sync::Arc,
        time::{Duration, UNIX_EPOCH},
    };
    let ids = SequentialIds::new();
    let source_store = TestStore::with_ids(&ids);
    let tables = || {
        vec![
            SyncedTable::new("notes", RowIdentity::SharedKey),
            SyncedTable::new("tags", RowIdentity::SharedKey),
        ]
    };
    let schema = "
        CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL UNIQUE,body TEXT NOT NULL);
        CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY,note TEXT REFERENCES notes(id) ON DELETE CASCADE);
        CREATE TABLE audit(id INTEGER PRIMARY KEY,event TEXT NOT NULL);
        CREATE TRIGGER note_insert AFTER INSERT ON notes BEGIN INSERT INTO audit(event) VALUES('insert'); END;
        CREATE TRIGGER note_update AFTER UPDATE ON notes BEGIN INSERT INTO audit(event) VALUES('update'); END;
        CREATE TRIGGER note_delete AFTER DELETE ON notes BEGIN INSERT INTO audit(event) VALUES('delete'); END;";
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let source = source_store
        .builder(tables(), vec![crate::Migration::sql(1, "records", schema)])
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    sql(&source, "INSERT INTO notes VALUES('a','A','initial'),('b','B','initial'),('d','D','delete'); INSERT INTO tags VALUES('old','b')").await.unwrap();
    let initial = records(&source).remove(0);
    clock.set(UNIX_EPOCH + Duration::from_secs(1002));
    sql(&source, "UPDATE notes SET body='remote' WHERE id='a'; UPDATE notes SET title='collision' WHERE id='b'; DELETE FROM notes WHERE id='d'; INSERT INTO notes VALUES('new','new','inserted'); INSERT INTO tags VALUES('new','a')").await.unwrap();
    let incoming = records(&source).remove(1);
    for streamed in [false, true] {
        let receiver_store = TestStore::with_ids(&ids);
        let receiver_clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1001)));
        let db = receiver_store
            .builder(tables(), vec![crate::Migration::sql(1, "records", schema)])
            .clock(receiver_clock)
            .open()
            .await
            .unwrap();
        db.apply_downloaded(initial.clone().into()).await.unwrap();
        sql(&db, "UPDATE notes SET body='local' WHERE id='a'; INSERT INTO notes VALUES('c','collision','local')").await.unwrap();
        let before = stored_tables(&db);
        let mut query = db.subscribe(|sql| {
            Ok(
                sql.query("SELECT id,title,body FROM notes ORDER BY id", [], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })?,
            )
        });
        let old_rows = query.next().await.unwrap();
        // Count actual row mutations, including repeated changes to one table,
        // local trigger effects, losses, fingerprints and applied positions.
        db.inspect_writer(|writer| {
            writer.batch("CREATE TEMP TABLE failure_step(number INTEGER NOT NULL,fail_at INTEGER NOT NULL); INSERT INTO failure_step VALUES(0,1)").unwrap();
            for (index, table) in before.keys().filter(|name| !name.starts_with("sqlite_")).enumerate() {
                for action in ["INSERT", "UPDATE", "DELETE"] {
                    writer.batch(&format!("CREATE TEMP TRIGGER fault_{index}_{action} AFTER {action} ON main.{} BEGIN UPDATE failure_step SET number=number+1; SELECT CASE WHEN number=fail_at THEN RAISE(ABORT,'injected apply failure') END FROM failure_step; END", crate::sql::identifier(table))).unwrap();
                }
            }
        });
        let mut applied = false;
        for step in 1..1000 {
            db.inspect_writer(|writer| {
                writer
                    .internal_execute("UPDATE failure_step SET number=0,fail_at=?1", [step])
                    .unwrap()
            });
            let result = if streamed {
                let encoder = coven_format::write_stream::WriteEncoder::new(&incoming).unwrap();
                let input = crate::DownloadedWriteStream {
                    header: encoder.header().clone(),
                    parts: (0..incoming.parts.len())
                        .map(|i| {
                            crate::DownloadedPartStream::Opened(std::io::Cursor::new(
                                encoder
                                    .part_chunks(i)
                                    .unwrap()
                                    .collect::<Result<Vec<_>, _>>()
                                    .unwrap()
                                    .concat(),
                            ))
                        })
                        .collect(),
                };
                db.apply_downloaded_stream(
                    input,
                    coven_format::value::EntryPositions(Vec::new()),
                    || Ok(()),
                )
                .await
            } else {
                db.apply_downloaded(incoming.clone().into()).await
            };
            match result {
                Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(_, Some(message))))
                    if message == "injected apply failure" =>
                {
                    assert_eq!(
                        stored_tables(&db),
                        before,
                        "streamed={streamed}, step={step}"
                    );
                    assert!(
                        !query.is_marked_for_rerun(),
                        "streamed={streamed}, step={step}"
                    );
                    let rows = db
                        .read(|sql| {
                            Ok(sql.query(
                                "SELECT id,title,body FROM notes ORDER BY id",
                                [],
                                |r| {
                                    Ok((
                                        r.get::<_, String>(0)?,
                                        r.get::<_, String>(1)?,
                                        r.get::<_, String>(2)?,
                                    ))
                                },
                            )?)
                        })
                        .await
                        .unwrap();
                    assert_eq!(rows, old_rows);
                    db.inspect_writer(|writer| assert_eq!(writer.query_row("SELECT count(*) FROM temp.sqlite_schema WHERE name GLOB '_coven_download_*'", [], |r| r.get::<_, i64>(0)).unwrap(), 0));
                }
                Ok(ApplyOutcome::Applied) => {
                    let mutations = db.inspect_writer(|writer| {
                        writer
                            .query_row("SELECT number FROM failure_step", [], |r| {
                                r.get::<_, i64>(0)
                            })
                            .unwrap()
                    });
                    assert_eq!(mutations, step - 1);
                    assert!(
                        mutations > 20,
                        "fixture must exercise merge persistence and materialization"
                    );
                    applied = true;
                    break;
                }
                result => panic!("streamed={streamed}, step={step}: {result:?}"),
            }
        }
        assert!(applied, "all mutations must eventually succeed");
        let rows = query.next().await.unwrap();
        assert_eq!(
            rows,
            vec![
                ("a".into(), "A".into(), "remote".into()),
                ("c".into(), "collision".into(), "local".into()),
                ("new".into(), "new".into(), "inserted".into())
            ]
        );
        assert!(!db.lost_values().await.unwrap().is_empty());
        assert_eq!(count(&db, "tags"), 1);
        assert!(db
            .sync_state(Vec::new())
            .await
            .unwrap()
            .positions
            .covers(incoming.header.position));
        assert_eq!(
            records(&db).len(),
            1,
            "downloads never enter the local upload queue"
        );
        assert_eq!(
            db.apply_downloaded(incoming.clone().into()).await.unwrap(),
            ApplyOutcome::AlreadyApplied
        );
        db.close().await.unwrap();
    }
    source.close().await.unwrap();
}

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
