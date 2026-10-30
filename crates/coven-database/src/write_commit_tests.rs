use coven_format::value::Value;
use coven_foundation::id_source::{DeviceId, StoreId};
use coven_merge::{Timestamp, WriteId};

use crate::tests::{contents, TestStore};
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Database, Migration, SyncedTable};

const CIRCLE: &str = "00000000-0000-4000-8000-00000000000a";
const ROOT: &str = "00000000-0000-4000-8000-000000000001";
const CIRCLE_SCHEMA: &str = "
    CREATE TABLE roots(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,body TEXT);
    CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,root TEXT NOT NULL REFERENCES roots(id) ON DELETE CASCADE,body TEXT);
    CREATE TABLE events(id TEXT NOT NULL PRIMARY KEY,circle TEXT NOT NULL);
    CREATE TABLE local_rows(id TEXT PRIMARY KEY);
    CREATE TRIGGER event_root AFTER INSERT ON events WHEN NOT coven_applying()
    BEGIN INSERT INTO roots VALUES(new.id,new.circle,'trigger'); END;
";

fn circle() -> coven_foundation::id_source::CircleId {
    coven_foundation::id_source::CircleId(uuid::Uuid::parse_str(CIRCLE).unwrap())
}

fn circle_tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("roots", crate::RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("children", crate::RowIdentity::IndependentUuid).audience_from("root"),
        SyncedTable::new("events", crate::RowIdentity::SharedKey).shared_trigger("event_root"),
    ]
}

fn leave_circle(db: &Database) {
    let other = crate::tests::keys(2);
    let member = crate::tests::member(2);
    db.inspect_writer(|db| {
        db.transaction(|db| {
            db.internal_execute(
                "INSERT INTO _coven_members(member,signing,sealing,role,removed,access)
                 VALUES(?1,?2,?3,'member',0,?4)",
                (
                    member.0.as_bytes(),
                    other.signing.as_bytes(),
                    other.sealing.as_bytes(),
                    serde_json::to_vec(&coven_format::MemberAccess::S3AccessKey {
                        access_key_id: "other-access-key".into(),
                    })
                    .unwrap(),
                ),
            )?;
            db.internal_execute(
                "UPDATE _coven_circle_members SET member=?1 WHERE circle=?2",
                (member.0.as_bytes(), CIRCLE),
            )?;
            Ok(())
        })
        .unwrap();
    });
}

#[tokio::test]
async fn membership_refuses_sql_triggers_and_both_sides_of_moves_atomically() {
    let store = TestStore::new();
    let db = store.schema(circle_tables(), CIRCLE_SCHEMA).await.unwrap();
    store.join_circle(&db, circle());
    sql(&db, "
        INSERT INTO roots VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-00000000000a','circle'),
                                ('00000000-0000-4000-8000-000000000002','store','store');
        INSERT INTO children VALUES('00000000-0000-4000-8000-000000000003','00000000-0000-4000-8000-000000000001','circle'),
                                   ('00000000-0000-4000-8000-000000000004','00000000-0000-4000-8000-000000000002','store');
    ").await.unwrap();
    leave_circle(&db);
    db.close().await.unwrap();
    let db = store.schema(circle_tables(), CIRCLE_SCHEMA).await.unwrap();
    let before = contents(&db);
    for statement in [
        "INSERT INTO roots VALUES('00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-00000000000a','new')",
        "UPDATE roots SET body='changed' WHERE audience<>'store'",
        "DELETE FROM roots WHERE audience<>'store'",
        "INSERT INTO children VALUES('00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-000000000001','new')",
        "UPDATE children SET body='changed' WHERE id='00000000-0000-4000-8000-000000000003'",
        "DELETE FROM children WHERE id='00000000-0000-4000-8000-000000000003'",
        "UPDATE roots SET audience='00000000-0000-4000-8000-00000000000a' WHERE audience='store'",
        "UPDATE roots SET audience='store' WHERE audience<>'store'",
        "UPDATE children SET root='00000000-0000-4000-8000-000000000001' WHERE id='00000000-0000-4000-8000-000000000004'",
        "UPDATE children SET root='00000000-0000-4000-8000-000000000002' WHERE id='00000000-0000-4000-8000-000000000003'",
        "INSERT INTO events VALUES('00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-00000000000a')",
    ] {
        let error = db.write(move |sql| {
            sql.execute("INSERT INTO local_rows VALUES('local')", [])?;
            sql.execute("UPDATE roots SET body='allowed' WHERE audience='store'", [])?;
            sql.execute_batch(statement)?;
            Ok(())
        }).await.unwrap_err();
        assert!(matches!(error, crate::DbError::NotInCircle(id) if id == circle()), "{statement}: {error:?}");
        assert_eq!(contents(&db), before, "{statement}");
    }
    // Losing membership does not invalidate already committed work or store writes.
    sql(
        &db,
        "UPDATE roots SET body='allowed' WHERE audience='store'",
    )
    .await
    .unwrap();
    assert_eq!(records(&db).len(), 2);
    assert_eq!(records(&db)[1].header.position.number, 2);
    store.join_circle(&db, circle());
    sql(
        &db,
        "UPDATE roots SET body='joined' WHERE audience<>'store'",
    )
    .await
    .unwrap();
    assert_eq!(records(&db)[2].header.position.number, 3);
    db.close().await.unwrap();
}

const FILES_SCHEMA: &str = "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,file TEXT,audience TEXT NOT NULL); CREATE TABLE local_rows(id TEXT PRIMARY KEY)";

async fn files_store(store: &TestStore, kind: crate::Provenance) -> Database {
    store
        .builder(
            vec![
                SyncedTable::new("files", crate::RowIdentity::IndependentUuid)
                    .audience_column("audience")
                    .file_column(crate::FileColumn::new("file")),
            ],
            vec![Migration::addition(1, "files", FILES_SCHEMA)],
        )
        .namespaces(vec![crate::Namespace::new("files", kind, 1 << 30)])
        .open()
        .await
        .unwrap()
}

#[tokio::test]
async fn membership_is_rechecked_after_file_staging_and_refusal_discards_bytes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let store = TestStore::new();
    let db = files_store(&store, crate::Provenance::AppProvided).await;
    store.join_circle(&db, circle());
    let (mut producer, reader) = tokio::io::duplex(1);
    let writing = tokio::spawn({
        let db = db.clone();
        async move {
            db.write_with_files::<_, _, _, _, crate::DbError>(
                move |batch| {
                    Ok(batch.put_file(
                        "files",
                        crate::FileSource::Stream(Box::pin(reader.take(8))),
                        crate::Place::Cloud,
                    ))
                },
                |sql, file| {
                    sql.execute("INSERT INTO local_rows VALUES('file')", [])?;
                    sql.execute(
                        "INSERT INTO files(id,file,audience) VALUES(?1,?2,?3)",
                        (ROOT, file, CIRCLE),
                    )?;
                    Ok(())
                },
            )
            .await
        }
    });
    // A one-byte pipe cannot accept both bytes until staging starts consuming it.
    tokio::time::timeout(std::time::Duration::from_secs(5), producer.write_all(b"or"))
        .await
        .unwrap()
        .unwrap();
    leave_circle(&db);
    let mut before = contents(&db);
    before.get_mut("_coven_file_removals").unwrap().clear();
    producer.write_all(b"iginal").await.unwrap();
    producer.shutdown().await.unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), writing)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, crate::DbError::NotInCircle(id) if id == circle()));
    assert_eq!(contents(&db), before);
    assert!(crate::file_write::tests::owned_paths(&store).is_empty());
    db.close().await.unwrap();
}

/// A refused write leaves no record, no upload and no owned copy behind, and
/// never touches a user's original.
#[tokio::test]
async fn a_refused_attachment_rolls_back_its_file_and_keeps_the_original() {
    for kind in [
        crate::Provenance::AppProvided,
        crate::Provenance::UserProvided,
    ] {
        let store = TestStore::new();
        let db = files_store(&store, kind.clone()).await;
        let original = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(original.path(), b"original").unwrap();
        let before = contents(&db);
        let prepared = crate::prepare_user_file(original.path(), |_| {})
            .await
            .unwrap();
        let user = kind == crate::Provenance::UserProvided;
        let error = db
            .write_with_files::<_, _, _, _, crate::DbError>(
                move |batch| {
                    Ok(if user {
                        batch.put_user_file("files", crate::UserSource::Cloud(prepared))
                    } else {
                        batch.put_file("files", b"original".to_vec(), crate::Place::Cloud)
                    })
                },
                |sql, file| {
                    sql.execute(
                        "INSERT INTO files(id,file,audience) VALUES(?1,?2,?3)",
                        (ROOT, file, CIRCLE),
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, crate::DbError::NotInCircle(id) if id == circle()));
        assert_eq!(contents(&db), before);
        assert!(crate::file_write::tests::owned_paths(&store).is_empty());
        assert_eq!(std::fs::read(original.path()).unwrap(), b"original");
        db.close().await.unwrap();
    }
}

const STEPS: &[(&str, &str)] = &[
    ("notes", "UPDATE"),
    ("_coven_writes", "INSERT"),
    ("_coven_published_rows", "INSERT"),
    ("_coven_uploads", "INSERT"),
    ("_coven_rows", "INSERT"),
    ("_coven_cells", "INSERT"),
    ("_coven_lost", "DELETE"),
    ("_coven_references", "INSERT"),
];
const WRITE: &str = "UPDATE notes SET title='Weekly groceries'; INSERT INTO tags VALUES('errands','42'); INSERT INTO local_rows VALUES('committed')";

fn tables() -> Vec<SyncedTable> {
    let mut tables = notes();
    tables.push(SyncedTable::new("tags", crate::RowIdentity::SharedKey));
    tables
}

fn migrations() -> Vec<Migration> {
    vec![Migration::addition_with(1, "atomic writes", |context| {
        context.execute_batch(NOTES)?;
        context.execute_batch(
            "CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY,note TEXT REFERENCES notes(id))",
        )?;
        Ok(())
    })]
}

async fn seed(database: &Database) {
    sql(
        database,
        "INSERT INTO notes VALUES('42','Original','milk, eggs')",
    )
    .await
    .unwrap();
    sql(database, "UPDATE notes SET title='Groceries'")
        .await
        .unwrap();
    let writes = records(database);
    let mut concurrent = writes[1].clone();
    database.receive(writes[0].clone().into()).await.unwrap();
    concurrent.header.read_through = 1;
    let device = DeviceId(concurrent.header.position.device.0.checked_add(1).unwrap());
    concurrent.header.position = WriteId { device, number: 1 };
    concurrent.header.timestamp =
        Timestamp::new(concurrent.header.timestamp.milliseconds() + 1, 0, device).unwrap();
    let coven_merge::Operation::Update(columns) = &mut concurrent.parts[0].rows[0].change.operation
    else {
        panic!("seed update")
    };
    columns.get_mut("title").unwrap().value = Value::Text("Shopping".into());
    assert_eq!(
        database.receive(concurrent.into()).await.unwrap(),
        crate::ApplyOutcome::Applied
    );
    assert_eq!(count(database, "_coven_lost"), 1);
}

#[tokio::test]
async fn failure_after_every_step_restores_app_rows_records_and_metadata() {
    let store = TestStore::new();
    let database = store.builder(tables(), migrations()).open().await.unwrap();
    seed(&database).await;
    let before = contents(&database);
    for (table, action) in STEPS {
        database.inspect_writer(|db| {
            db.fail_at(
                "_coven_fail",
                &format!("AFTER {action} ON {table}"),
                "injected failure",
            )
        });
        let error = sql(&database, WRITE).await.unwrap_err();
        assert!(
            matches!(error, crate::DbError::Sqlite(_)),
            "{table}: {error:?}"
        );
        assert_eq!(contents(&database), before, "failure after {table}");
        database.inspect_writer(|db| db.batch("DROP TRIGGER _coven_fail").unwrap());
    }
    sql(&database, WRITE).await.unwrap();
    assert_eq!(records(&database)[2].header.position.number, 3);
    assert_eq!(count(&database, "_coven_lost"), 0);
    assert_eq!(count(&database, "local_rows"), 1);
    database.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_value_limit_refuses_the_whole_write_and_rolls_back() {
    let store = TestStore::new();
    let database = store.builder(tables(), migrations()).open().await.unwrap();
    seed(&database).await;
    let before = contents(&database);
    let old_limit = database.inspect_writer(|db| db.set_value_limit(4096));
    const INSERT: &str = "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100) INSERT INTO notes SELECT 'import-'||i,'Imported note','' FROM n; INSERT INTO local_rows VALUES('import')";
    let error = sql(&database, INSERT).await.unwrap_err();
    database.inspect_writer(|db| db.set_value_limit(old_limit));
    assert_eq!(contents(&database), before);
    assert!(
        matches!(error, crate::DbError::TooLarge { field: "write plaintext", actual, maximum: 4096 } if actual > 4096),
        "{error:?}"
    );
    sql(&database, INSERT).await.unwrap();
    let queued = records(&database);
    assert_eq!(queued.len(), 3);
    assert_eq!(queued[2].header.position.number, 3);
    assert_eq!(queued[2].parts[0].rows.len(), 100);
    assert_eq!(count(&database, "local_rows"), 1);
    database.close().await.unwrap();
}

#[tokio::test]
async fn queued_value_errors_preserve_full_width_lengths() {
    let store = TestStore::new();
    let database = store.builder(tables(), migrations()).open().await.unwrap();
    database.inspect_writer(|db| {
        db.set_value_limit(4096);
        assert_eq!(
            db.check_value_length("write plaintext", 4096).unwrap(),
            4096
        );
        for length in [4097, u64::from(u32::MAX) + 1, u64::MAX] {
            let error = db
                .check_value_length("write plaintext", length)
                .unwrap_err();
            let crate::DbError::TooLarge {
                field,
                actual,
                maximum,
            } = error
            else {
                panic!("expected TooLarge, got {error:?}");
            };
            assert_eq!(field, "write plaintext");
            assert_eq!(actual, length);
            assert_eq!(maximum, 4096u64);
        }
    });
    database.close().await.unwrap();
}

#[tokio::test]
async fn the_queue_limit_reserves_key_ids_without_reserving_ciphertext() {
    let source = TestStore::new();
    let source = source.schema(notes(), NOTES).await.unwrap();
    const INSERT: &str = "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100) INSERT INTO notes SELECT 'import-'||i,'Imported note','' FROM n";
    sql(&source, INSERT).await.unwrap();
    let record = records(&source).remove(0);
    let plaintext = coven_format::write_stream::WriteEncoder::new(&record)
        .unwrap()
        .plaintext_length();
    let target = TestStore::new();
    let db = target.schema(notes(), NOTES).await.unwrap();
    let old = db.inspect_writer(|db| db.set_value_limit(plaintext as i32 + 24));
    let result = sql(&db, INSERT).await;
    db.inspect_writer(|db| db.set_value_limit(old));
    assert!(
        matches!(result, Err(crate::DbError::TooLarge { .. })),
        "{result:?}"
    );
    for table in ["notes", "_coven_writes", "_coven_uploads"] {
        assert_eq!(count(&db, table), 0, "{table}");
    }
    let encoder = coven_format::write_stream::WriteEncoder::new(&record).unwrap();
    let sealed = coven_format::sealed_write::sealed_length(
        encoder.header_frame().len(),
        &encoder
            .header()
            .parts
            .iter()
            .map(|p| p.plaintext_length)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let limit = plaintext + 71; // One store key, one part key and the queue row overhead.
    assert!(sealed > limit);
    db.inspect_writer(|db| db.set_value_limit(limit as i32));
    sql(&db, INSERT).await.unwrap();
    crate::upload::tests::attempt(&db, 17).await.unwrap();
    assert_eq!(records(&db)[0].parts, record.parts);
    assert!(crate::upload::tests::selected(&db).await.1.is_some());
    db.inspect_writer(|db| db.set_value_limit(old));
    db.close().await.unwrap();
    source.close().await.unwrap();
}

#[tokio::test]
async fn seventy_thousand_rows_commit_as_one_queued_write_and_decode_after_reopen() {
    use coven_format::write_stream::{decode_plaintext, WriteEncoder};
    use coven_merge::{Audience, Operation};

    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    sql(&database, "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<70000) INSERT INTO notes SELECT printf('%05d',i),'Imported note','' FROM n").await.unwrap();
    assert_eq!(count(&database, "notes"), 70_000);
    assert_eq!(count(&database, "_coven_writes"), 1);
    assert_eq!(count(&database, "_coven_uploads"), 1);
    database.close().await.unwrap();

    let database = store.schema(notes(), NOTES).await.unwrap();
    let bytes = database.inspect_writer(|db| {
        db.query_row(
            "SELECT record FROM _coven_uploads WHERE sealing_keys IS NULL",
            [],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .unwrap()
    });
    let record = decode_plaintext(&bytes).unwrap();
    assert_eq!(record.header.position.number, 1);
    assert_eq!(record.parts.len(), 1);
    let part = &record.parts[0];
    assert_eq!(part.audience, Audience::Store);
    assert_eq!(part.rows.len(), 70_000);
    for (index, change) in part.rows.iter().enumerate() {
        let id = format!("{:05}", index + 1);
        assert_eq!(change.row.table, "notes");
        assert_eq!(change.row.audience, Audience::Store);
        assert_eq!(
            coven_format::key::decode_key(&change.row.key).unwrap(),
            [Value::Text(id)]
        );
        assert_eq!(change.change.generation, 0);
        assert!(change.old.is_empty());
        let Operation::Insert(columns) = &change.change.operation else {
            panic!("insert must carry its columns");
        };
        assert_eq!(columns["title"].value, Value::Text("Imported note".into()));
        assert_eq!(columns["body"].value, Value::Text(String::new()));
    }
    let encoder = WriteEncoder::new(&record).unwrap();
    assert_eq!(encoder.plaintext_length(), bytes.len() as u64);
    assert_eq!(encoder.header().parts[0].record_count, 70_000);
    let mut reencoded = vec![0; bytes.len()];
    encoder.encode_plaintext(&mut reencoded).unwrap();
    assert_eq!(reencoded, bytes);
    let mut offset = encoder.header_frame().len();
    assert_eq!(&bytes[..offset], encoder.header_frame());
    for chunk in encoder.part_chunks(0).unwrap() {
        let chunk = chunk.unwrap();
        assert_eq!(bytes[offset..offset + chunk.len()], chunk);
        offset += chunk.len();
    }
    assert_eq!(offset, bytes.len());
    assert_eq!(count(&database, "notes"), 70_000);
    database.close().await.unwrap();
}

#[test]
fn a_process_crash_after_every_step_leaves_no_partial_write() {
    let store = TestStore::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = runtime
        .block_on(store.builder(tables(), migrations()).open())
        .unwrap();
    runtime.block_on(seed(&database));
    let before = contents(&database);
    runtime.block_on(database.close()).unwrap();
    for (table, action) in STEPS {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "write_commit::tests::crashing_writer",
                "--nocapture",
            ])
            .env("COVEN_CRASH_FILE", store.database_path())
            .env("COVEN_CRASH_STORE", store.id().to_string())
            .env("COVEN_CRASH_TABLE", table)
            .env("COVEN_CRASH_ACTION", action)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let database = runtime
            .block_on(store.builder(tables(), migrations()).open())
            .unwrap();
        assert_eq!(contents(&database), before, "crash after {table}");
        database.inspect_writer(|db| db.batch("DROP TRIGGER _coven_crash").unwrap());
        runtime.block_on(database.close()).unwrap();
    }
    let database = runtime
        .block_on(store.builder(tables(), migrations()).open())
        .unwrap();
    runtime.block_on(sql(&database, WRITE)).unwrap();
    assert_eq!(records(&database)[2].header.position.number, 3);
    runtime.block_on(database.close()).unwrap();
}

#[test]
fn crashing_writer() {
    let Ok(path) = std::env::var("COVEN_CRASH_FILE") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let layout = coven_foundation::files::StoreDirectories::new(
        path.parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned(),
    );
    let store =
        StoreId(uuid::Uuid::parse_str(&std::env::var("COVEN_CRASH_STORE").unwrap()).unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = runtime
        .block_on(
            crate::DatabaseBuilder::new(layout.store_dir(&store))
                .synced_tables(tables())
                .migrations(migrations())
                .open(),
        )
        .unwrap();
    database.inspect_writer(|db| {
        db.crash_after(
            &std::env::var("COVEN_CRASH_TABLE").unwrap(),
            &std::env::var("COVEN_CRASH_ACTION").unwrap(),
        )
    });
    runtime.block_on(sql(&database, WRITE)).unwrap();
    panic!("write did not reach its crash point");
}

const CRASH_SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,parent TEXT REFERENCES notes(id) ON DELETE CASCADE); CREATE UNIQUE INDEX notes_root_title ON notes(title) WHERE parent IS NULL";

fn crash_statement(restoring: bool) -> &'static str {
    if restoring {
        "DELETE FROM notes WHERE id='45'"
    } else {
        "DELETE FROM notes WHERE id='3'; UPDATE notes SET title='Plan' WHERE id='1'"
    }
}

#[test]
fn a_process_crash_during_removal_or_restoration_keeps_the_previous_database() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    for (restoring, points) in [
        (
            false,
            vec![
                ("notes", "DELETE"),
                ("_coven_lost", "INSERT"),
                ("_coven_lost", "UPDATE"),
                ("_coven_constraints", "INSERT"),
                ("_coven_claims", "INSERT"),
            ],
        ),
        (
            true,
            vec![
                ("notes", "INSERT"),
                ("_coven_lost", "DELETE"),
                ("_coven_claims", "DELETE"),
            ],
        ),
    ] {
        let store = TestStore::new();
        let db = runtime
            .block_on(store.schema(
                vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
                CRASH_SCHEMA,
            ))
            .unwrap();
        if restoring {
            runtime
                .block_on(sql(&db, "INSERT INTO notes VALUES('45','Groceries',NULL)"))
                .unwrap();
            runtime
                .block_on(sql(&db, "INSERT INTO notes VALUES('46','Shopping',NULL)"))
                .unwrap();
            runtime.block_on(crate::tests::remote_update(
                &db,
                "notes",
                "46",
                &[("title", Value::Text("Groceries".into()))],
            ));
        } else {
            runtime.block_on(crate::removal::tests::hidden_plan_note(&db));
        }
        let before = contents(&db);
        runtime.block_on(db.close()).unwrap();
        for (target, action) in points {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "write_commit::tests::crashing_removal_writer",
                    "--nocapture",
                ])
                .env("COVEN_REMOVAL_FILE", store.database_path())
                .env("COVEN_REMOVAL_STORE", store.id().to_string())
                .env("COVEN_REMOVAL_RESTORE", restoring.to_string())
                .env("COVEN_REMOVAL_TABLE", target)
                .env("COVEN_REMOVAL_ACTION", action)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let db = runtime
                .block_on(store.schema(
                    vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
                    CRASH_SCHEMA,
                ))
                .unwrap();
            assert_eq!(contents(&db), before, "{target} {action}");
            db.inspect_writer(|db| db.batch("DROP TRIGGER _coven_crash").unwrap());
            runtime.block_on(db.close()).unwrap();
        }
        let db = runtime
            .block_on(store.schema(
                vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
                CRASH_SCHEMA,
            ))
            .unwrap();
        runtime
            .block_on(sql(&db, crash_statement(restoring)))
            .unwrap();
        assert_eq!(count(&db, "notes"), i64::from(restoring));
        runtime.block_on(db.close()).unwrap();
    }
}

#[test]
fn crashing_removal_writer() {
    let Ok(path) = std::env::var("COVEN_REMOVAL_FILE") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let layout = coven_foundation::files::StoreDirectories::new(
        path.parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned(),
    );
    let store = coven_foundation::id_source::StoreId(
        uuid::Uuid::parse_str(&std::env::var("COVEN_REMOVAL_STORE").unwrap()).unwrap(),
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let db = runtime
        .block_on(
            crate::DatabaseBuilder::new(layout.store_dir(&store))
                .synced_tables(vec![SyncedTable::new(
                    "notes",
                    crate::RowIdentity::SharedKey,
                )])
                .migrations(vec![crate::Migration::addition(1, "schema", CRASH_SCHEMA)])
                .open(),
        )
        .unwrap();
    db.inspect_writer(|db| {
        db.crash_after(
            &std::env::var("COVEN_REMOVAL_TABLE").unwrap(),
            &std::env::var("COVEN_REMOVAL_ACTION").unwrap(),
        )
    });
    runtime
        .block_on(sql(
            &db,
            crash_statement(std::env::var("COVEN_REMOVAL_RESTORE").unwrap() == "true"),
        ))
        .unwrap();
    panic!("write did not reach its removal crash point");
}

async fn run(db: &Database, statement: String) -> Result<(), crate::DbError> {
    db.write(move |sql| {
        sql.execute(&statement, [])?;
        Ok(())
    })
    .await
}

/// A-D2: receivers compare a root's audience cell with the circle id's
/// canonical lowercase text, so the author refuses every other spelling of
/// it, such as Swift's uppercase `uuidString`, instead of committing a write
/// every receiver refuses (§15). Statements, shared triggers and moves are
/// all refused, and the transaction rolls back.
#[tokio::test]
async fn a_root_audience_must_be_spelled_canonically() {
    let author = TestStore::new();
    let db = author.schema(circle_tables(), CIRCLE_SCHEMA).await.unwrap();
    author.join_circle(&db, circle());
    run(
        &db,
        format!("INSERT INTO roots VALUES('{ROOT}','store','moved')"),
    )
    .await
    .unwrap();
    let queued = records(&db).len();
    let other = "00000000-0000-4000-8000-000000000002";
    let spellings = [
        CIRCLE.to_uppercase(),
        format!("{{{CIRCLE}}}"),
        format!("urn:uuid:{CIRCLE}"),
        CIRCLE.replace('-', ""),
        "STORE".into(),
        "not a circle".into(),
    ];
    for spelling in spellings {
        for statement in [
            format!("INSERT INTO roots VALUES('{other}','{spelling}','new')"),
            format!("INSERT INTO events VALUES('{other}','{spelling}')"),
            format!("UPDATE roots SET audience='{spelling}' WHERE id='{ROOT}'"),
        ] {
            let error = db
                .write(move |sql| {
                    sql.execute("INSERT INTO local_rows VALUES('rollback')", [])?;
                    sql.execute(&statement, [])?;
                    Ok(())
                })
                .await
                .unwrap_err();
            let crate::DbError::InvalidAudience { table, key } = error else {
                panic!("{spelling}: expected InvalidAudience, received {error:?}");
            };
            assert_eq!(table, "roots");
            assert!(matches!(
                key.0.as_slice(),
                [rusqlite::types::Value::Text(id)] if id == other || id == ROOT
            ));
            assert_eq!(count(&db, "local_rows"), 0);
            assert_eq!(records(&db).len(), queued);
        }
    }
    run(
        &db,
        format!("UPDATE roots SET audience='{CIRCLE}' WHERE id='{ROOT}'"),
    )
    .await
    .unwrap();
    // The canonical spelling is the one a receiver accepts.
    let receiver = TestStore::new();
    let copy = receiver
        .schema(circle_tables(), CIRCLE_SCHEMA)
        .await
        .unwrap();
    receiver.join_circle(&copy, circle());
    for record in records(&db) {
        copy.receive(record.into()).await.unwrap();
    }
    db.close().await.unwrap();
    copy.close().await.unwrap();
}

/// A received root whose audience cell spells its circle another way is
/// refused, whoever wrote it (§15).
#[tokio::test]
async fn a_received_root_spelling_its_circle_another_way_is_refused() {
    let author = TestStore::new();
    let db = author.schema(circle_tables(), CIRCLE_SCHEMA).await.unwrap();
    author.join_circle(&db, circle());
    run(
        &db,
        format!("INSERT INTO roots VALUES('{ROOT}','{CIRCLE}','x')"),
    )
    .await
    .unwrap();
    let mut record = records(&db).pop().unwrap();
    db.close().await.unwrap();
    let coven_merge::Operation::Insert(columns) = &mut record.parts[0].rows[0].change.operation
    else {
        panic!("an insert");
    };
    columns.get_mut("audience").unwrap().value = Value::Text(CIRCLE.to_uppercase());
    let receiver = TestStore::new();
    let copy = receiver
        .schema(circle_tables(), CIRCLE_SCHEMA)
        .await
        .unwrap();
    receiver.join_circle(&copy, circle());
    assert!(matches!(
        copy.receive(record.into()).await,
        Err(crate::DbError::Snapshot(
            crate::SnapshotError::Inconsistent(_)
        ))
    ));
    assert_eq!(count(&copy, "roots"), 0);
    copy.close().await.unwrap();
}

/// A local write reads what it can't change once, not once a row: the
/// schema version, each column's registered id and its own authored part.
#[tokio::test]
async fn a_write_reads_its_constants_once() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)",
        )
        .await
        .unwrap();
    crate::write::tests::sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<50)
         INSERT INTO notes SELECT 'note ' || i, 'title', 'body' FROM n",
    )
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql.contains(text)).count();
    assert_eq!(runs("PRAGMA user_version"), 1);
    assert_eq!(
        runs("FROM _coven_columns WHERE table_name=?1 AND column_name=?2"),
        3
    );
    assert_eq!(
        runs("FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2"),
        1
    );
    db.close().await.unwrap();
}

/// A local write checks once, not once an app statement, that the schema
/// its kept statements were judged against is still the one in effect.
#[tokio::test]
async fn a_write_reads_the_schema_cookie_once() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)",
        )
        .await
        .unwrap();
    db.write(|sql| {
        for i in 0..50 {
            sql.execute(
                "INSERT INTO notes VALUES(?1,'title','body')",
                [format!("note {i}")],
            )?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql == text).count();
    assert_eq!(runs("PRAGMA main.schema_version"), 1);
    assert_eq!(runs("INSERT INTO notes VALUES(?1,'title','body')"), 50);
    db.close().await.unwrap();
}

/// A row's merge state is stored in a statement for each kind of record, not
/// one for each column: its cells in one and its written values cleared in
/// one. Its generation's id is never read back: storing the generation
/// returns it, and recording its removal state uses that.
#[tokio::test]
async fn a_row_stores_its_cells_in_one_statement() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)",
        )
        .await
        .unwrap();
    crate::write::tests::sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<50)
         INSERT INTO notes SELECT 'note ' || i, 'title', 'body' FROM n",
    )
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql.contains(text)).count();
    assert_eq!(runs("INSERT INTO _coven_cells"), 50);
    assert_eq!(runs("DELETE FROM _coven_written_values"), 50);
    assert_eq!(runs("SELECT id FROM _coven_rows WHERE"), 0);
    assert_eq!(count(&db, "_coven_cells"), 150);
    db.close().await.unwrap();
}
