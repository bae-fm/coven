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
    "coven_writes",
    "coven_uploads",
    "coven_positions",
    "coven_rows",
    "coven_columns",
    "coven_cells",
    "coven_lost",
    "coven_foreign_keys",
    "coven_references",
    "coven_constraints",
    "coven_claims",
];
const STEPS: &[(&str, &str)] = &[
    ("notes", "UPDATE"),
    ("coven_writes", "INSERT"),
    ("coven_positions", "UPDATE"),
    ("coven_uploads", "INSERT"),
    ("coven_rows", "INSERT"),
    ("coven_columns", "INSERT"),
    ("coven_cells", "INSERT"),
    ("coven_lost", "DELETE"),
    ("coven_foreign_keys", "INSERT"),
    ("coven_references", "INSERT"),
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
        db.internal_execute("INSERT INTO coven_writes(timestamp,number,had_read) VALUES(?1,?2,?3)", crate::params![
            merge_fields::encode_timestamp(&Timestamp::new(original.timestamp.milliseconds() - 1, 0, loser.device).unwrap()).unwrap(),
            loser.number.to_be_bytes().as_slice(), merge_fields::encode_write_positions(&coven_format::value::WritePositions(vec![])).unwrap(),
        ]).unwrap();
        db.internal_execute("UPDATE coven_rows SET write_id=last_insert_rowid() WHERE table_name='notes'",[]).unwrap();
        db.internal_execute("INSERT INTO coven_positions(device,number) VALUES(?1,?2)",crate::params![loser.device.0.to_be_bytes().as_slice(),loser.number.to_be_bytes().as_slice()]).unwrap();
        db.internal_execute("INSERT INTO coven_lost(table_name,key,audience,generation,column_id,value,set_by,replaced_by) VALUES('notes',?1,'store',?2,(SELECT id FROM coven_columns WHERE table_name='notes' AND column_name='title'),?3,?4,?5)", crate::params![
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
        database.inspect_writer(|db| db.batch(&format!("CREATE TRIGGER coven_fail AFTER {action} ON {table} BEGIN SELECT RAISE(ABORT,'injected failure'); END")).unwrap());
        let error = sql(&database, WRITE).await.unwrap_err();
        assert!(
            matches!(error, crate::DbError::Sqlite(_)),
            "{table}: {error:?}"
        );
        assert_eq!(state(&database), before, "failure after {table}");
        database.inspect_writer(|db| db.batch("DROP TRIGGER coven_fail").unwrap());
    }
    sql(&database, WRITE).await.unwrap();
    assert_eq!(records(&database)[1].header.position.number, 2);
    assert_eq!(count(&database, "coven_lost"), 0);
    assert_eq!(count(&database, "local_rows"), 1);
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
        database.inspect_writer(|db| db.batch("DROP TRIGGER coven_crash").unwrap());
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
            "coven_writes",
            "coven_uploads",
            "coven_rows",
            "coven_cells",
            "coven_lost",
            "coven_positions",
            "coven_foreign_keys",
            "coven_references",
            "coven_constraints",
            "coven_claims",
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
                ("coven_lost", "INSERT"),
                ("coven_lost", "UPDATE"),
                ("coven_constraints", "INSERT"),
                ("coven_claims", "INSERT"),
            ],
        ),
        (
            true,
            vec![
                ("notes", "INSERT"),
                ("coven_lost", "DELETE"),
                ("coven_claims", "DELETE"),
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
            db.inspect_writer(|db| db.batch("DROP TRIGGER coven_crash").unwrap());
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
