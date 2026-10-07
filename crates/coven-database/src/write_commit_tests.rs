use coven_format::{merge_fields, value::Value};
use coven_foundation::id_source::{DeviceId, StoreId};
use coven_merge::{ColumnValue, Rule, Timestamp, WriteId};
use std::collections::BTreeSet;

use crate::tests::TestStore;
use crate::write::tests::{count, notes, records, sql, NOTES};
use crate::{Database, Migration, SyncedTable};

const TABLES: &[&str] = &[
    "notes",
    "tags",
    "local_rows",
    "_coven_writes",
    "_coven_uploads",
    "_coven_positions",
    "_coven_rows",
    "_coven_columns",
    "_coven_cells",
    "_coven_lost",
    "_coven_foreign_keys",
    "_coven_references",
    "_coven_constraints",
    "_coven_claims",
    "_coven_fingerprint_leaves",
    "_coven_fingerprint_sums",
];
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

fn state(database: &Database) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    database.inspect_writer(|db| {
        TABLES
            .iter()
            .map(|table| {
                db.query(&format!("SELECT * FROM {table} ORDER BY 1"), [], |row| {
                    (0..row.as_ref().column_count())
                        .map(|column| row.get(column))
                        .collect()
                })
                .unwrap()
            })
            .collect()
    })
}

async fn seed(database: &Database) {
    sql(
        database,
        "INSERT INTO notes VALUES('42','Groceries','milk, eggs')",
    )
    .await
    .unwrap();
    let original = records(database)[0].header.clone();
    let loser = WriteId {
        device: DeviceId(original.position.device.0.wrapping_add(1)),
        number: 1,
    };
    database.inspect_writer(|db| {
        db.internal_execute("INSERT INTO _coven_writes(timestamp,number,had_read) VALUES(?1,?2,?3)", crate::params![
            merge_fields::encode_timestamp(&Timestamp::new(original.timestamp.milliseconds() - 1, 0, loser.device).unwrap()).unwrap(),
            loser.number.to_be_bytes().as_slice(), merge_fields::encode_write_positions(&coven_format::value::WritePositions(vec![])).unwrap(),
        ]).unwrap();
        db.internal_execute("UPDATE _coven_rows SET write_id=last_insert_rowid() WHERE table_name='notes'",[]).unwrap();
        db.internal_execute("INSERT INTO _coven_positions(device,number) VALUES(?1,?2)",crate::params![loser.device.0.to_be_bytes().as_slice(),loser.number.to_be_bytes().as_slice()]).unwrap();
        db.internal_execute("INSERT INTO _coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES('notes',?1,'store',?2,(SELECT id FROM _coven_columns WHERE table_name='notes' AND column_name='title'),?3,?4,'write',?5)", crate::params![
            coven_format::key::encode_key(&[Value::Text("42".into())]).unwrap(), 1u64.to_be_bytes().as_slice(),
            merge_fields::encode_column_value(&ColumnValue { value: Value::Text("Shopping".into()), parents: Default::default() }).unwrap(),
            merge_fields::encode_write_id(&loser).unwrap(), merge_fields::encode_write_id(&original.position).unwrap(),
        ]).unwrap();
    });
}

#[tokio::test]
async fn failure_after_every_step_restores_app_rows_records_and_metadata() {
    let store = TestStore::new();
    let database = store.builder(tables(), migrations()).open().await.unwrap();
    seed(&database).await;
    let before = state(&database);
    for (table, action) in STEPS {
        database.inspect_writer(|db| db.batch(&format!("CREATE TRIGGER _coven_fail AFTER {action} ON {table} BEGIN SELECT RAISE(ABORT,'injected failure'); END")).unwrap());
        let error = sql(&database, WRITE).await.unwrap_err();
        assert!(
            matches!(error, crate::DbError::Sqlite(_)),
            "{table}: {error:?}"
        );
        assert_eq!(state(&database), before, "failure after {table}");
        database.inspect_writer(|db| db.batch("DROP TRIGGER _coven_fail").unwrap());
    }
    sql(&database, WRITE).await.unwrap();
    assert_eq!(records(&database)[1].header.position.number, 2);
    assert_eq!(count(&database, "_coven_lost"), 0);
    assert_eq!(count(&database, "local_rows"), 1);
    database.close().await.unwrap();
}

#[tokio::test]
async fn sqlite_value_limit_refuses_the_whole_write_and_rolls_back() {
    let store = TestStore::new();
    let database = store.builder(tables(), migrations()).open().await.unwrap();
    seed(&database).await;
    let before = state(&database);
    let old_limit = database.inspect_writer(|db| db.set_value_limit(4096));
    const INSERT: &str = "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100) INSERT INTO notes SELECT 'import-'||i,'Imported note','' FROM n; INSERT INTO local_rows VALUES('import')";
    let error = sql(&database, INSERT).await.unwrap_err();
    database.inspect_writer(|db| db.set_value_limit(old_limit));
    assert_eq!(state(&database), before);
    assert!(
        matches!(error, crate::DbError::TooLarge { field: "write plaintext", actual, maximum: 4096 } if actual > 4096),
        "{error:?}"
    );
    sql(&database, INSERT).await.unwrap();
    let queued = records(&database);
    assert_eq!(queued.len(), 2);
    assert_eq!(queued[1].header.position.number, 2);
    assert_eq!(queued[1].parts[0].rows.len(), 100);
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
async fn a_plaintext_that_fits_but_cannot_be_sealed_rolls_back_at_commit() {
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
            "SELECT record FROM _coven_uploads WHERE NOT EXISTS(SELECT 1 FROM _coven_upload_seals)",
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
    let before = state(&database);
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
        assert_eq!(state(&database), before, "crash after {table}");
        database.inspect_writer(|db| db.batch("DROP TRIGGER _coven_crash").unwrap());
        runtime.block_on(database.close()).unwrap();
    }
    let database = runtime
        .block_on(store.builder(tables(), migrations()).open())
        .unwrap();
    runtime.block_on(sql(&database, WRITE)).unwrap();
    assert_eq!(records(&database)[1].header.position.number, 2);
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
                .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
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

use crate::removal::tests::remove;

const CRASH_SCHEMA: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE,parent TEXT REFERENCES notes(id) ON DELETE CASCADE); CREATE UNIQUE INDEX notes_root_title ON notes(title) WHERE parent IS NULL";

fn crash_statement(restoring: bool) -> &'static str {
    if restoring {
        "DELETE FROM notes WHERE id='45'"
    } else {
        "UPDATE notes SET title='Plan' WHERE id='1'"
    }
}

fn removal_state(db: &Database) -> Vec<Vec<Vec<rusqlite::types::Value>>> {
    db.inspect_writer(|db| {
        [
            "notes",
            "_coven_writes",
            "_coven_uploads",
            "_coven_rows",
            "_coven_cells",
            "_coven_lost",
            "_coven_positions",
            "_coven_foreign_keys",
            "_coven_references",
            "_coven_constraints",
            "_coven_claims",
        ]
        .iter()
        .map(|table| {
            db.query(&format!("SELECT * FROM {table} ORDER BY 1"), [], |r| {
                (0..r.as_ref().column_count())
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
        })
        .collect()
    })
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
            remove(
                &db,
                "notes",
                "46",
                &[("title", Value::Text("Groceries".into()))],
                BTreeSet::from([Rule::Unique(["title"].into())]),
            );
        } else {
            runtime.block_on(sql(&db, "INSERT INTO notes VALUES('1','Ideas',NULL); INSERT INTO notes VALUES('2','Plan','1')")).unwrap();
            remove(
                &db,
                "notes",
                "2",
                &[],
                BTreeSet::from([Rule::Unique(["title"].into())]),
            );
        }
        let before = removal_state(&db);
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
            assert_eq!(removal_state(&db), before, "{target} {action}");
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
                .coven_migration_policy(crate::CovenMigrationPolicy::ApplyPending)
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
