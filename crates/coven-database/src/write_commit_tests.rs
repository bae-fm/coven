use coven_format::value::Value;
use coven_foundation::id_source::{DeviceId, StoreId};
use coven_merge::{Timestamp, WriteId};

use crate::tests::{contents, TestStore};
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Database, Migration, SyncedTable};

const STEPS: &[(&str, &str)] = &[
    ("notes", "UPDATE"),
    ("_coven_writes", "INSERT"),
    ("_coven_positions", "UPDATE"),
    ("_coven_uploads", "INSERT"),
    ("_coven_rows", "INSERT"),
    ("_coven_columns", "INSERT"),
    ("_coven_cells", "INSERT"),
    ("_coven_lost", "DELETE"),
    ("_coven_foreign_keys", "INSERT"),
    ("_coven_references", "INSERT"),
];
const WRITE: &str = "UPDATE notes SET title='Weekly groceries'; INSERT INTO tags VALUES('errands','42'); INSERT INTO local_rows VALUES('committed')";

fn tables() -> Vec<SyncedTable> {
    let mut tables = notes();
    tables.push(SyncedTable::new("tags", crate::RowIdentity::SharedKey));
    tables
}

fn migrations() -> Vec<Migration> {
    vec![Migration::run(1, "atomic writes", |context| {
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
    concurrent.header.had_read.0.push(writes[0].header.position);
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
        database.apply_downloaded(concurrent.into()).await.unwrap(),
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
    for table in [
        "notes",
        "_coven_writes",
        "_coven_uploads",
        "_coven_positions",
    ] {
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
    let layout = coven_foundation::files::StoreLayout::new(
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
    let layout = coven_foundation::files::StoreLayout::new(
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
                .migrations(vec![crate::Migration::sql(1, "schema", CRASH_SCHEMA)])
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
