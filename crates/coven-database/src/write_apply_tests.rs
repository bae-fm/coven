use crate::tests::{contents, next, TestStore};
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{ApplyOutcome, DbError, StoreDatabase};
use crate::{RowIdentity, SyncedTable};
use coven_foundation::id_source::SequentialIds;
use sqlive::random_scopes::{a_value, b_value, key, query, write, Generated, Rng};
use sqlive::random_scopes::{CAPTURED, SCHEMA, TABLES};
use sqlive::SqlValue;

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
    let schema = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE totals(id TEXT NOT NULL PRIMARY KEY,n INTEGER NOT NULL); CREATE TABLE audit(id INTEGER PRIMARY KEY, applying INTEGER); CREATE TRIGGER shared AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN INSERT INTO totals VALUES('all',1) ON CONFLICT(id) DO UPDATE SET n=n+1; END; CREATE TRIGGER local AFTER INSERT ON notes BEGIN INSERT INTO audit VALUES(NULL,coven_applying()); END";
    let a = a_store.schema(tables(), schema).await.unwrap();
    let b = b_store.schema(tables(), schema).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('one')").await.unwrap();
    b.receive(records(&a).remove(0).into()).await.unwrap();
    let result = b
        .read(|sql| {
            Ok::<_, DbError>((
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
                .builder(notes(), vec![crate::Migration::addition(1, "notes", NOTES)])
                .clock(std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
                )))
                .open()
                .await
                .unwrap(),
        );
    }
    let b = databases.pop().unwrap();
    let a = databases.pop().unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','title','body')").await.unwrap();
    let record = records(&a).remove(0);
    b.inspect_writer(|sql, _| sql.batch("CREATE TRIGGER fail_apply AFTER INSERT ON notes BEGIN SELECT RAISE(ROLLBACK,'transaction refused'); END").unwrap());
    assert!(
        matches!(b.receive(record.clone().into()).await,Err(DbError::Sqlite(sqlive::rusqlite::Error::SqliteFailure(_,Some(message)))) if message=="transaction refused")
    );
    for table in ["notes", "_coven_rows", "_coven_cells", "_coven_writes", "_coven_lost"] {
        assert_eq!(count(&b, table), 0, "{table}");
    }
    b.inspect_writer(|sql, _| sql.batch("DROP TRIGGER fail_apply").unwrap());
    sql(&b, "INSERT INTO notes VALUES('local','local','')").await.unwrap();
    assert_eq!(records(&b)[0].header.timestamp.milliseconds(), 1_000);
    assert_eq!(b.receive(record.into()).await.unwrap(), ApplyOutcome::Applied);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

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
        .builder(tables(), vec![crate::Migration::addition(1, "records", schema)])
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
            .builder(tables(), vec![crate::Migration::addition(1, "records", schema)])
            .clock(receiver_clock)
            .open()
            .await
            .unwrap();
        db.receive(initial.clone().into()).await.unwrap();
        sql(&db, "UPDATE notes SET body='local' WHERE id='a'; INSERT INTO notes VALUES('c','collision','local')").await.unwrap();
        let before = contents(&db);
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(crate::test_utils::LiveSql::of(
            "SELECT id,title,body FROM notes ORDER BY id",
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
        ));
        let old_rows = query.next_inline().await.expect("the live query answers").unwrap();
        // Count actual row mutations, including repeated changes to one table,
        // local trigger effects, losses and the applied slot.
        db.inspect_writer(|writer, _| {
            writer.batch("CREATE TEMP TABLE failure_step(number INTEGER NOT NULL,fail_at INTEGER NOT NULL); INSERT INTO failure_step VALUES(0,1)").unwrap();
            for (index, table) in before.keys().filter(|name| !name.starts_with("sqlite_")).enumerate() {
                for action in ["INSERT", "UPDATE", "DELETE"] {
                    writer.batch(&format!("CREATE TEMP TRIGGER fault_{index}_{action} AFTER {action} ON main.{} BEGIN UPDATE failure_step SET number=number+1; SELECT CASE WHEN number=fail_at THEN RAISE(ABORT,'injected apply failure') END FROM failure_step; END", sqlive::sql::identifier(table))).unwrap();
                }
            }
        });
        let mut applied = false;
        for step in 1..1000 {
            db.inspect_writer(|writer, _| {
                writer.execute("UPDATE failure_step SET number=0,fail_at=?1", [step]).unwrap()
            });
            let result = if streamed {
                let encoder = coven_format::write_stream::WriteEncoder::new(&incoming).unwrap();
                let input = crate::DownloadedWriteStream {
                    signer: crate::test_utils::signer(),
                    authorship: crate::test_utils::TestAuthorship::write(&incoming),
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
                db.receive_stream(input, || Ok(())).await
            } else {
                db.receive(incoming.clone().into()).await
            };
            match result {
                Err(DbError::Sqlite(sqlive::rusqlite::Error::SqliteFailure(_, Some(message))))
                    if message == "injected apply failure" =>
                {
                    assert_eq!(contents(&db), before, "streamed={streamed}, step={step}");
                    assert!(!query.is_marked_for_rerun(), "streamed={streamed}, step={step}");
                    let rows = db
                        .read(|sql| {
                            Ok::<_, DbError>(sql.query(
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
                    // The staging tables stay on the writer, and the rollback
                    // empties them of what the failed slot staged.
                    db.inspect_writer(|writer, _| assert_eq!(writer.query_row("SELECT (SELECT count(*) FROM temp._coven_download_rows)+(SELECT count(*) FROM temp._coven_download_dismissals)+(SELECT count(*) FROM temp._coven_download_replaced)", [], |r| r.get::<_, i64>(0)).unwrap(), 0, "streamed={streamed}, step={step}"));
                }
                Ok(ApplyOutcome::Applied) => {
                    let mutations = db.inspect_writer(|writer, _| {
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
        let rows = query.next_inline().await.expect("the live query answers").unwrap();
        assert_eq!(
            rows,
            vec![
                ("a".into(), "A".into(), "remote".into()),
                ("b".into(), "collision".into(), "initial".into()),
                ("new".into(), "new".into(), "inserted".into())
            ]
        );
        assert!(!db.lost_values().await.unwrap().is_empty());
        assert_eq!(count(&db, "tags"), 2);
        assert!(db.checked_write_header(incoming.header.position).await.unwrap().is_some());
        assert_eq!(records(&db).len(), 1, "downloads never enter the local upload queue");
        assert_eq!(
            db.receive(incoming.clone().into()).await.unwrap(),
            ApplyOutcome::AlreadyApplied
        );
        db.close().await.unwrap();
    }
    source.close().await.unwrap();
}

async fn assert_rejected_download(
    database: &crate::StoreDatabase,
    record: crate::DownloadedWrite,
    expected: coven_merge::MergeError,
) {
    let before = contents(database);
    let mut query: crate::LiveQuery<_, DbError> =
        database.subscribe(crate::test_utils::LiveSql::of("SELECT * FROM notes", |r| {
            r.get::<_, String>(0)
        }));
    query.next_inline().await.expect("the live query answers").unwrap();
    for _ in 0..2 {
        let error = database.receive(record.clone()).await.unwrap_err();
        assert!(
            matches!(&error, DbError::InvalidWrite { write, error } if *write == record.header.position && *error == expected)
                || matches!(&error, DbError::WriteFormat(coven_format::Error::Merge(error)) if *error == expected),
            "{error:?}"
        );
        assert_eq!(contents(database), before);
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
    sql(&a, "INSERT INTO notes VALUES('n','original','')").await.unwrap();
    sql(&a, "UPDATE notes SET title='valid'").await.unwrap();
    let writes = records(&a);
    b.receive(writes[0].clone().into()).await.unwrap();
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
    assert_eq!(b.receive(writes[1].clone().into()).await.unwrap(), ApplyOutcome::Applied);
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
        b.receive(write.clone().into()).await.unwrap();
    }
    let mut invalid = writes[3].clone();
    assert_eq!(invalid.parts[0].rows[0].change.generation, 3);
    invalid.parts[0].rows[0].change.generation = 2;
    assert_rejected_download(&b, invalid.into(), coven_merge::MergeError::GenerationParity(2))
        .await;
    assert_eq!(b.receive(writes[3].clone().into()).await.unwrap(), ApplyOutcome::Applied);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn rejected_timestamps_cannot_advance_skipped_or_excluded_writes() {
    let ids = SequentialIds::new();
    let a_store = TestStore::with_ids(&ids);
    let b_store = TestStore::with_ids(&ids);
    let a = a_store.schema(notes(), NOTES).await.unwrap();
    let b = b_store.schema(notes(), NOTES).await.unwrap();
    sql(&a, "INSERT INTO notes VALUES('n','initial','')").await.unwrap();
    sql(&a, "UPDATE notes SET title='valid'").await.unwrap();
    let writes = records(&a);
    b.receive(writes[0].clone().into()).await.unwrap();
    b.close().await.unwrap();
    let b = crate::write_boundary::tests::version_two(&b_store, true).await;
    b.exclude_later_writes(2).await;
    for lost in [false, true] {
        let mut invalid: crate::DownloadedWrite = writes[1].clone().into();
        invalid.header.timestamp = coven_merge::Timestamp::new(
            writes[0].header.timestamp.milliseconds() - 1,
            0,
            invalid.header.position.device,
        )
        .unwrap();
        if !lost {
            invalid.parts = vec![crate::DownloadedPart::Skipped(coven_merge::Audience::Store)];
        }
        assert_rejected_download(
            &b,
            invalid,
            coven_merge::MergeError::CausalTimestamp(writes[1].header.position),
        )
        .await;
    }
    assert_eq!(b.receive(writes[1].clone().into()).await.unwrap(), ApplyOutcome::Applied);
    for db in [a, b] {
        db.close().await.unwrap();
    }
}

/// Applying another device's write records the values a waiting live
/// query's scope tests, as a local write does: a write to another release's
/// tracks wakes no query (§4).
#[tokio::test]
async fn another_devices_writes_record_scoped_values() {
    use coven_format::value::Value;
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("tracks", RowIdentity::SharedKey)],
            "CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY, release TEXT, title TEXT)",
        )
        .await
        .unwrap();
    sql(&db, "INSERT INTO tracks VALUES ('1','a','one'),('2','b','two')").await.unwrap();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(crate::test_utils::LiveSql::of(
        "SELECT title FROM tracks WHERE release='a' ORDER BY id",
        |r| r.get::<_, String>(0),
    ));
    assert_eq!(crate::tests::next(&mut query).await, ["one"]);
    crate::tests::remote_update(&db, "tracks", "2", &[("title", Value::Text("deux".into()))]).await;
    assert!(!query.is_marked_for_rerun());
    crate::tests::remote_update(&db, "tracks", "1", &[("title", Value::Text("uno".into()))]).await;
    assert_eq!(crate::tests::next(&mut query).await, ["uno"]);
    crate::tests::remote_update(&db, "tracks", "2", &[("release", Value::Text("a".into()))]).await;
    assert_eq!(crate::tests::next(&mut query).await, ["uno", "deux"]);
    db.close().await.unwrap();
}

/// Apply another device's update of one of `rows`, the synced table's keys,
/// and describe it. Whether the merge keeps its values or not, live queries
/// must follow what the table then holds.
async fn remote(db: &StoreDatabase, rng: &mut Rng, rows: &[String]) -> String {
    use coven_format::value::Value;
    let id = rng.pick(rows).clone();
    let (column, value) = match rng.below(3) {
        0 => ("a", Value::Integer(rng.below(5) as i64)),
        1 => ("b", Value::Text((*rng.pick(&["x", "k1", "k2", "k3", "1"])).into())),
        _ => ("c", Value::Integer(rng.below(10) as i64)),
    };
    let described = format!("another device sets {CAPTURED}.{column} = {value:?} where id = {id}");
    crate::tests::try_remote_update(db, CAPTURED, &id, &[(column, value)]).await.unwrap();
    described
}

type Rows = Vec<Vec<SqlValue>>;

fn read(
    sql: &crate::SqlReadContext<'_>,
    text: &str,
    parameters: &[SqlValue],
) -> crate::SqliteResult<Rows> {
    sql.query(text, sqlive::rusqlite::params_from_iter(parameters), |row| {
        (0..row.as_ref().column_count()).map(|index| row.get::<_, SqlValue>(index)).collect()
    })
}

async fn one_shot(db: &StoreDatabase, query: &Generated) -> Rows {
    let sql = query.sql.clone();
    let parameters = query.parameters.clone();
    db.read(move |context| Ok::<_, DbError>(read(&context, &sql, &parameters)?)).await.unwrap()
}

/// One seed's case: random app writes, the host's own transactions on the
/// device-local tables, and another device's updates of the synced table,
/// under random live queries (sqlive's `random_scopes`). Each commit marks
/// every query whose result it changed, and a rerun returns what a one-shot
/// read does.
async fn run_seed(seed: u64) {
    let mut rng = Rng::seeded(seed);
    let store = TestStore::new();
    let db = store
        .schema(vec![SyncedTable::new(CAPTURED, RowIdentity::SharedKey)], SCHEMA)
        .await
        .unwrap();
    // The test inserts rows into every table first, so joins, chained ones
    // too, find matching rows for the writes to change.
    let mut rows = Vec::new();
    for table in TABLES {
        for id in 0..6 {
            rows.push(format!(
                "INSERT INTO {table}(id, a, b, c) VALUES ({}, {}, {}, {})",
                key(table, id),
                a_value(&mut rng),
                b_value(&mut rng),
                rng.below(10)
            ));
        }
    }
    let seeded: &'static str = Box::leak(rows.join("; ").into_boxed_str());
    sql(&db, seeded).await.unwrap();
    let queries: Vec<Generated> = (0..16).map(|_| query(&mut rng)).collect();
    let mut live = Vec::new();
    for query in &queries {
        let values: Vec<&dyn sqlive::rusqlite::ToSql> =
            query.parameters.iter().map(|value| value as _).collect();
        let mut subscription: crate::LiveQuery<_, DbError> =
            db.subscribe(crate::test_utils::LiveSql::new(&query.sql, &values, |row| {
                let columns = 0..row.as_ref().column_count();
                columns.map(|index| row.get::<_, SqlValue>(index)).collect::<Result<Vec<_>, _>>()
            }));
        let rows = next(&mut subscription).await;
        live.push((subscription, rows));
    }
    for step in 0..24 {
        let generated = write(&mut rng);
        // The host's own transactions write device-local tables only: synced
        // rows change through writes, so their merge state stays whole.
        let host = !generated.captured && rng.chance(40);
        let mut described = generated.sql.clone();
        let synced: Vec<String> = db
            .read(|context| {
                Ok::<_, DbError>(
                    context.query(&format!("SELECT id FROM {CAPTURED}"), [], |r| r.get(0))?,
                )
            })
            .await
            .unwrap();
        if !synced.is_empty() && rng.chance(20) {
            described = remote(&db, &mut rng, &synced).await;
        } else if host {
            db.commit_writer(move |writer, _| writer.batch(&generated.sql).unwrap());
        } else {
            let leaked: &'static str = Box::leak(generated.sql.into_boxed_str());
            sql(&db, leaked).await.unwrap();
        }
        for (query, (subscription, last)) in queries.iter().zip(live.iter_mut()) {
            let fresh = one_shot(&db, query).await;
            let marked = subscription.is_marked_for_rerun();
            let case = || {
                format!(
                    "seed {seed}, step {step}: {} {:?} after `{described}`",
                    query.sql, query.parameters
                )
            };
            if fresh != *last {
                assert!(marked, "a write changed a result but didn't mark its query: {}", case());
            }
            // A marked query sometimes stays marked through the next write,
            // which its judging then skips.
            // A rerun whose rows equal the last ones returns nothing.
            if marked && rng.chance(75) {
                if let Some(rows) = subscription.run_changed() {
                    *last = rows.unwrap();
                }
                assert_eq!(*last, fresh, "a rerun differs from a read: {}", case());
            }
        }
    }
    db.close().await.unwrap();
}

/// Live queries follow the store's own writes and another device's alike.
/// `COVEN_SCOPE_SEEDS` sets how many seeds run (default 40).
#[tokio::test]
async fn live_queries_rerun_whenever_their_results_change() {
    let seeds: u64 = std::env::var("COVEN_SCOPE_SEEDS")
        .ok()
        .map_or(40, |seeds| seeds.parse().expect("COVEN_SCOPE_SEEDS is a count"));
    for seed in 0..seeds {
        run_seed(seed).await;
    }
}
