use std::collections::BTreeSet;

use coven_format::value::Value;
use coven_format::write::WriteRecord;
use coven_merge::Operation;

use crate::tests::TestStore;
use crate::{Database, DbError, RowIdentity, SyncedTable};

pub(crate) fn records(database: &Database) -> Vec<WriteRecord> {
    database
        .inspect_writer(|db| {
            db.query(
                "SELECT record FROM coven_uploads ORDER BY number",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .unwrap()
        })
        .into_iter()
        .map(
            |bytes| match coven_format::Object::decode(&bytes).unwrap() {
                coven_format::Object::Write(record) => record,
                _ => panic!("upload queue must contain kind-1 writes"),
            },
        )
        .collect()
}

pub(crate) async fn sql(database: &Database, sql: &'static str) -> Result<(), DbError> {
    database
        .write(BTreeSet::new(), move |context| {
            context.execute_batch(sql)?;
            Ok(())
        })
        .await
}

pub(crate) fn count(database: &Database, table: &str) -> i64 {
    database.inspect_writer(|db| {
        db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    })
}

pub(crate) const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL DEFAULT ''); CREATE TABLE local_rows(id TEXT PRIMARY KEY);";

pub(crate) fn notes() -> Vec<SyncedTable> {
    vec![SyncedTable::new("notes", RowIdentity::SharedKey)]
}

#[tokio::test]
async fn grocery_title_and_errands_tag_are_one_unsigned_write() {
    let store = TestStore::new();
    let database = store.schema(vec![
        SyncedTable::new("notes", RowIdentity::SharedKey),
        SyncedTable::new("tags", RowIdentity::SharedKey),
    ], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT); CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE local_rows(id TEXT PRIMARY KEY);").await.unwrap();
    sql(
        &database,
        "INSERT INTO notes VALUES('42','Grocry list','milk, eggs')",
    )
    .await
    .unwrap();
    sql(&database, "INSERT INTO tags VALUES('errands')")
        .await
        .unwrap();
    let returned = database
        .write(BTreeSet::new(), |context| {
            context.execute("UPDATE notes SET title=?1 WHERE id='42'", ["Grocery list"])?;
            context.execute("DELETE FROM tags WHERE id='errands'", [])?;
            context.execute("INSERT INTO local_rows VALUES('kept')", [])?;
            let title: String = context.query_row("SELECT title FROM notes", [], |r| r.get(0))?;
            Ok(title)
        })
        .await
        .unwrap();
    assert_eq!(returned, "Grocery list");
    let writes = records(&database);
    let write = &writes[2];
    assert_eq!(write.header.position.number, 3);
    assert_eq!(write.header.schema_version, 1);
    assert!(write.header.had_read.0.is_empty());
    assert_eq!(write.parts.len(), 1);
    let rows = &write.parts[0].rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].change.generation, 1);
    assert_eq!(
        rows[0].old,
        [("title".into(), Value::Text("Grocry list".into()))].into()
    );
    let Operation::Update(columns) = &rows[0].change.operation else {
        panic!("title update")
    };
    assert_eq!(columns.len(), 1);
    assert_eq!(columns["title"].value, Value::Text("Grocery list".into()));
    assert!(matches!(rows[1].change.operation, Operation::Delete));
    assert_eq!(rows[1].old["id"], Value::Text("errands".into()));
    assert_eq!(count(&database, "local_rows"), 1);
    database.inspect_writer(|db| {
        let setters: Vec<(String, Vec<u8>)> = db.query("SELECT c.column_name,w.number FROM coven_cells x JOIN coven_columns c ON c.id=x.column_id JOIN coven_writes w ON w.id=x.write_id ORDER BY c.column_name", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        assert_eq!(setters, vec![("body".into(), 1u64.to_be_bytes().to_vec()), ("id".into(), 1u64.to_be_bytes().to_vec()), ("title".into(), 3u64.to_be_bytes().to_vec())]);
    });
    database.close().await.unwrap();
}

#[tokio::test]
async fn no_net_synced_changes_commit_local_rows_without_consuming_numbers() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    sql(&database, "INSERT INTO local_rows VALUES('one'); INSERT INTO notes VALUES('42','x',''); DELETE FROM notes;").await.unwrap();
    assert_eq!(count(&database, "local_rows"), 1);
    assert!(records(&database).is_empty());
    sql(
        &database,
        "INSERT INTO notes VALUES('42','Groceries','milk')",
    )
    .await
    .unwrap();
    sql(&database, "UPDATE notes SET title='changed'; UPDATE notes SET title='Groceries'; INSERT INTO local_rows VALUES('two')").await.unwrap();
    assert_eq!(records(&database).len(), 1);
    assert_eq!(count(&database, "local_rows"), 2);
    sql(&database, "UPDATE notes SET body=NULL")
        .await
        .unwrap_err();
    sql(&database, "UPDATE notes SET body='eggs'")
        .await
        .unwrap();
    assert_eq!(records(&database)[1].header.position.number, 2);
    database.close().await.unwrap();
}

#[tokio::test]
async fn hardware_store_note_delete_and_readd_advance_generations() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    for statement in [
        "INSERT INTO notes VALUES('43','Hardware store','')",
        "DELETE FROM notes WHERE id='43'",
        "INSERT INTO notes VALUES('43','Hardware store','')",
        "UPDATE notes SET title='Hardware store, Saturday' WHERE id='43'",
    ] {
        sql(&database, statement).await.unwrap();
    }
    let writes = records(&database);
    assert_eq!(
        writes
            .iter()
            .map(|w| w.parts[0].rows[0].change.generation)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    database.inspect_writer(|db| {
        let generations = db.query("SELECT generation FROM coven_rows ORDER BY generation", [], |r| r.get::<_, Vec<u8>>(0)).unwrap();
        assert_eq!(generations, [1u64,2,3].map(|g| g.to_be_bytes().to_vec()));
        assert_eq!(db.query_row("SELECT count(*) FROM coven_cells c JOIN coven_rows r ON r.id=c.row_id WHERE r.generation=?1", [3u64.to_be_bytes().as_slice()], |r| r.get::<_, i64>(0)).unwrap(), 3);
    });
    database.close().await.unwrap();
}

#[tokio::test]
async fn app_errors_and_panics_roll_back_and_leave_the_writer_usable() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    let error = database.write(BTreeSet::new(), |context| {
        context.execute_batch("INSERT INTO notes VALUES('42','Groceries',''); INSERT INTO local_rows VALUES('one')")?;
        Err::<(), _>(DbError::ClockOutOfRange)
    }).await.unwrap_err();
    assert!(matches!(error, DbError::ClockOutOfRange));
    let clone = database.clone();
    let panic = tokio::spawn(async move {
        clone
            .write(BTreeSet::new(), |context| -> Result<(), DbError> {
                context.execute("INSERT INTO notes VALUES('43','Hardware store','')", [])?;
                std::panic::panic_any(43u64)
            })
            .await
    })
    .await
    .unwrap_err();
    assert_eq!(*panic.into_panic().downcast::<u64>().unwrap(), 43);
    for table in [
        "notes",
        "local_rows",
        "coven_writes",
        "coven_uploads",
        "coven_rows",
        "coven_cells",
    ] {
        assert_eq!(count(&database, table), 0);
    }
    sql(&database, "INSERT INTO notes VALUES('42','Groceries','')")
        .await
        .unwrap();
    assert_eq!(records(&database)[0].header.position.number, 1);
    database.close().await.unwrap();
}

#[tokio::test]
async fn writes_cannot_change_the_schema() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    for statement in [
        "DROP TABLE notes",
        "ALTER TABLE notes ADD COLUMN extra TEXT",
        "CREATE TABLE extra(id TEXT)",
    ] {
        assert!(
            matches!(
                sql(&database, statement).await,
                Err(DbError::StatementForbidden { .. })
            ),
            "{statement}"
        );
    }
    database.close().await.unwrap();
}

#[tokio::test]
async fn renaming_urgent_to_important_records_sqlites_on_update_cascade() {
    let store = TestStore::new();
    let database = store.schema(vec![
        SyncedTable::new("tags", RowIdentity::SharedKey),
        SyncedTable::new("note_tags", RowIdentity::SharedKey).key_columns(["note_id", "tag_id"]),
        SyncedTable::new("tag_details", RowIdentity::SharedKey),
    ], "CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE note_tags(note_id TEXT NOT NULL, tag_id TEXT NOT NULL REFERENCES tags(id) ON UPDATE CASCADE ON DELETE CASCADE, PRIMARY KEY(note_id,tag_id)); CREATE TABLE tag_details(id TEXT NOT NULL PRIMARY KEY, tag TEXT REFERENCES tags(id) ON UPDATE SET NULL);").await.unwrap();
    sql(&database, "INSERT INTO tags VALUES('urgent'); INSERT INTO note_tags VALUES('42','urgent'); INSERT INTO tag_details VALUES('one','urgent')").await.unwrap();
    sql(
        &database,
        "UPDATE tags SET id='important' WHERE id='urgent'",
    )
    .await
    .unwrap();
    let rows = &records(&database)[1].parts[0].rows;
    assert_eq!(rows.len(), 5);
    for table in ["tags", "note_tags"] {
        let changes: Vec<_> = rows.iter().filter(|r| r.row.table == table).collect();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].change.generation, 0);
        assert!(matches!(changes[0].change.operation, Operation::Insert(_)));
        assert_eq!(changes[1].change.generation, 1);
        assert!(matches!(changes[1].change.operation, Operation::Delete));
        if table == "note_tags" {
            let Operation::Insert(columns) = &changes[0].change.operation else {
                unreachable!()
            };
            assert_eq!(
                columns["tag_id"].parents
                    [&coven_merge::ForeignKey::new(["tag_id"], "tags", ["id"])]
                    .generation,
                1
            );
            assert_eq!(
                coven_format::key::decode_key(
                    &columns["tag_id"].parents
                        [&coven_merge::ForeignKey::new(["tag_id"], "tags", ["id"])]
                        .row
                        .key
                )
                .unwrap(),
                [Value::Text("important".into())]
            );
        }
    }
    let change = rows.iter().find(|r| r.row.table == "tag_details").unwrap();
    let Operation::Update(columns) = &change.change.operation else {
        panic!("ordinary SET NULL change")
    };
    assert_eq!(change.old["tag"], Value::Text("urgent".into()));
    assert_eq!(columns["tag"].value, Value::Null);
    assert!(columns["tag"].parents.is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn body_edit_records_shared_edited_at_but_keeps_the_search_index_local() {
    let store = TestStore::new();
    let database = store.schema(vec![SyncedTable::new("notes", RowIdentity::SharedKey).shared_trigger("notes_edited_at")], "
        CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT,body TEXT,edited_at TEXT);
        CREATE TABLE title_index(note_id TEXT PRIMARY KEY,title TEXT);
        CREATE TRIGGER notes_title_index AFTER UPDATE OF title ON notes BEGIN UPDATE title_index SET title=new.title WHERE note_id=new.id; END;
        CREATE TRIGGER notes_edited_at AFTER UPDATE OF body ON notes WHEN NOT coven_applying() BEGIN UPDATE notes SET edited_at='2026-10-02 15:00' WHERE id=new.id; END;
    ").await.unwrap();
    sql(&database, "INSERT INTO notes VALUES('42','Groceries','milk, eggs',NULL); INSERT INTO title_index VALUES('42','Groceries')").await.unwrap();
    sql(
        &database,
        "UPDATE notes SET body='milk, eggs, bread',title='Shopping'",
    )
    .await
    .unwrap();
    let rows = &records(&database)[1].parts[0].rows;
    assert_eq!(rows.len(), 1);
    let Operation::Update(columns) = &rows[0].change.operation else {
        panic!("shared trigger columns")
    };
    assert_eq!(
        columns.keys().map(String::as_str).collect::<Vec<_>>(),
        ["body", "edited_at", "title"]
    );
    assert_eq!(
        columns["edited_at"].value,
        Value::Text("2026-10-02 15:00".into())
    );
    assert_eq!(rows[0].old["edited_at"], Value::Null);
    database.inspect_writer(|db| {
        assert_eq!(
            db.query_row("SELECT title FROM title_index", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "Shopping"
        )
    });
    database.close().await.unwrap();
}

#[tokio::test]
async fn internal_tables_transaction_control_and_pragmas_remain_protected() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    for statement in [
        "SELECT * FROM coven_writes",
        "INSERT INTO coven_uploads(device,number,record) VALUES(x'',x'',x'')",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT a",
        "PRAGMA table_xinfo(notes)",
        "SELECT * FROM pragma_table_xinfo('notes')",
        "PRAGMA foreign_keys=OFF",
        "ATTACH ':memory:' AS outside",
        "SELECT load_extension('no')",
    ] {
        let error = sql(&database, statement).await.unwrap_err();
        assert!(
            matches!(
                error,
                DbError::InternalTable { .. } | DbError::StatementForbidden { .. }
            ),
            "{statement}: {error:?}"
        );
    }
    // A caller's row mapper must not inherit the session extension's narrowly
    // scoped permission to read table metadata during a SQLite step.
    database
        .write(Default::default(), |context| {
            context.query_row("SELECT 1", [], |_| {
                let error = context
                    .execute_batch("PRAGMA table_xinfo(notes)")
                    .unwrap_err();
                assert!(matches!(
                    DbError::from(error),
                    DbError::StatementForbidden { .. }
                ));
                Ok(())
            })?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(records(&database).is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn trigger_targets_remain_protected_in_writes() {
    for (trigger, shared) in [
        ("CREATE TRIGGER effect AFTER INSERT ON notes BEGIN UPDATE notes SET title='bad'; END;", false),
        ("CREATE TRIGGER effect AFTER INSERT ON notes WHEN NOT coven_applying() BEGIN INSERT INTO local_rows VALUES('bad'); END;", true),
    ] {
        let store = TestStore::new();
        let declaration = SyncedTable::new("notes", RowIdentity::SharedKey);
        let declaration = if shared { declaration.shared_trigger("effect") } else { declaration };
        let database = store.builder(vec![declaration], vec![crate::Migration::run(1, "trigger", move |context| {
            context.execute_batch(NOTES)?;
            context.execute_batch(trigger)?;
            Ok(())
        })]).open().await.unwrap();
        assert!(matches!(sql(&database, "INSERT INTO notes VALUES('42','Groceries','')").await, Err(DbError::TriggerTarget { trigger, .. }) if trigger == "effect"));
        assert_eq!(count(&database, "notes"), 0);
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_swallowed_sqlite_rollback_cannot_commit_a_write_record() {
    let store = TestStore::new();
    let database = store.schema(notes(), NOTES).await.unwrap();
    sql(&database, "INSERT INTO notes VALUES('42','Groceries','')")
        .await
        .unwrap();
    let result = database
        .write(Default::default(), |context| {
            context.execute("INSERT INTO local_rows VALUES('x')", [])?;
            assert!(context
                .execute(
                    "INSERT OR ROLLBACK INTO notes VALUES('42','duplicate','')",
                    []
                )
                .is_err());
            Ok(())
        })
        .await;
    assert!(matches!(result, Err(DbError::TransactionEnded)));
    assert_eq!(count(&database, "local_rows"), 0);
    assert_eq!(records(&database).len(), 1);
    database.close().await.unwrap();
}

#[tokio::test]
async fn a_null_key_is_refused_by_sqlite_before_capture() {
    let store = TestStore::new();
    let database = store
        .schema(
            vec![SyncedTable::new("tags", RowIdentity::SharedKey)],
            "CREATE TABLE tags(id TEXT NOT NULL PRIMARY KEY)",
        )
        .await
        .unwrap();
    let error = sql(&database, "INSERT INTO tags VALUES(NULL)")
        .await
        .unwrap_err();
    assert!(
        matches!(error, DbError::Sqlite(rusqlite::Error::SqliteFailure(e,_)) if e.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL)
    );
    assert_eq!(count(&database, "tags"), 0);
    assert!(records(&database).is_empty());
    database.close().await.unwrap();
}

#[tokio::test]
async fn collated_keys_keep_one_identity_across_equivalent_spellings() {
    let store = TestStore::new();
    let database = store
        .schema(
            vec![SyncedTable::new("tags", RowIdentity::SharedKey)],
            "CREATE TABLE tags(id TEXT COLLATE NOCASE NOT NULL PRIMARY KEY, label TEXT)",
        )
        .await
        .unwrap();
    sql(&database, "INSERT INTO tags VALUES('Urgent','one')")
        .await
        .unwrap();
    sql(&database, "UPDATE tags SET id='URGENT',label='two'")
        .await
        .unwrap();
    let writes = records(&database);
    let old = &writes[0].parts[0].rows[0];
    let new = &writes[1].parts[0].rows[0];
    assert_eq!(old.row, new.row);
    assert_eq!(new.change.generation, 1);
    assert!(matches!(new.change.operation, Operation::Update(_)));
    database.close().await.unwrap();
}

#[tokio::test]
async fn generated_columns_preserve_session_column_names_and_their_sql_values() {
    let store = TestStore::new();
    let database = store.schema(vec![SyncedTable::new("notes", RowIdentity::SharedKey)], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, length INTEGER GENERATED ALWAYS AS (length(body)) STORED, body TEXT)").await.unwrap();
    sql(&database, "INSERT INTO notes(id,body) VALUES('42','milk')")
        .await
        .unwrap();
    sql(&database, "UPDATE notes SET body='milk, eggs'")
        .await
        .unwrap();
    let writes = records(&database);
    let row = &writes[1].parts[0].rows[0];
    let Operation::Update(columns) = &row.change.operation else {
        panic!("column update")
    };
    assert_eq!(columns["body"].value, Value::Text("milk, eggs".into()));
    assert_eq!(columns["length"].value, Value::Integer(10));
    assert_eq!(row.old["length"], Value::Integer(4));
    database.close().await.unwrap();
}

#[tokio::test]
async fn key_normalization_matches_sqlites_builtin_collations_with_embedded_nul() {
    for (collation, before, after) in [("nocase", "A\0x", "a\0y"), ("rtrim", "urgent  ", "urgent")]
    {
        let store = TestStore::new();
        let database = store.builder(vec![SyncedTable::new("tags", RowIdentity::SharedKey)], vec![crate::Migration::run(1, "collated tags", move |context| {
            context.execute_batch(&format!("CREATE TABLE tags(id TEXT COLLATE {collation} NOT NULL PRIMARY KEY,label TEXT)"))?;
            Ok(())
        })]).open().await.unwrap();
        database
            .write(Default::default(), move |context| {
                let same: bool = context.query_row(
                    &format!("SELECT ?1 = ?2 COLLATE {collation}"),
                    [before, after],
                    |r| r.get(0),
                )?;
                assert!(same);
                context.execute("INSERT INTO tags VALUES(?1,'one')", [before])?;
                Ok(())
            })
            .await
            .unwrap();
        database
            .write(Default::default(), move |context| {
                context.execute("UPDATE tags SET id=?1,label='two'", [after])?;
                Ok(())
            })
            .await
            .unwrap();
        let writes = records(&database);
        assert_eq!(writes[1].parts[0].rows.len(), 1);
        assert_eq!(
            writes[0].parts[0].rows[0].row,
            writes[1].parts[0].rows[0].row
        );
        database.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_calls_share_the_writer_and_allocate_a_gapless_sequence() {
    let store = TestStore::new();
    let clock = std::sync::Arc::new(coven_foundation::clock::FixedClock::new(
        std::time::UNIX_EPOCH,
    ));
    let database = store
        .builder(notes(), vec![crate::Migration::sql(1, "notes", NOTES)])
        .clock(clock)
        .open()
        .await
        .unwrap();
    sql(&database, "INSERT INTO notes VALUES('42','Groceries','0')")
        .await
        .unwrap();
    let mut calls = Vec::new();
    for _ in 0..12 {
        let database = database.clone();
        calls.push(tokio::spawn(async move {
            database.write(BTreeSet::new(), |context| {
                context.query_row("UPDATE notes SET body=CAST(CAST(body AS INTEGER)+1 AS TEXT) RETURNING CAST(body AS INTEGER)", [], |row| row.get::<_, i64>(0)).map_err(Into::into)
            }).await.unwrap()
        }));
    }
    let mut values = Vec::new();
    for call in calls {
        values.push(call.await.unwrap());
    }
    values.sort();
    assert_eq!(values, (1..=12).collect::<Vec<_>>());
    let writes = records(&database);
    assert_eq!(writes.len(), 13);
    for (index, write) in writes.iter().enumerate() {
        assert_eq!(write.header.position.number, index as u64 + 1);
        assert_eq!(write.header.timestamp.counter(), index as u16);
    }
    database.close().await.unwrap();
    assert!(matches!(
        sql(&database, "DELETE FROM notes").await,
        Err(DbError::StoreClosed)
    ));
}

fn assert_indexed(database: &Database) {
    let statements = database.inspect_writer(|db| db.fullscan_statements());
    assert!(!statements.is_empty(), "the whole write must be traced");
    let mut scans = std::collections::BTreeMap::<&str, i64>::new();
    for (sql, steps) in &statements {
        if *steps != 0 {
            *scans.entry(sql).or_default() += i64::from(*steps);
        }
    }
    assert_eq!(
        statements
            .iter()
            .map(|(_, steps)| i64::from(*steps))
            .sum::<i64>(),
        0,
        "full scans: {scans:?}"
    );
}

#[tokio::test]
async fn writes_visit_only_indexed_rows_in_a_ten_thousand_row_store() {
    let store = TestStore::new();
    let db = store.schema(vec![
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        SyncedTable::new("tasks", RowIdentity::IndependentUuid).audience_from("note"),
        SyncedTable::new("items", RowIdentity::IndependentUuid).audience_from("task"),
    ], "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,body TEXT,UNIQUE(audience,body));
        CREATE TABLE tasks(id TEXT NOT NULL PRIMARY KEY,note TEXT NOT NULL REFERENCES notes(id) ON DELETE CASCADE,body TEXT);
        CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,task TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,body TEXT);
        CREATE INDEX tasks_note ON tasks(note); CREATE INDEX items_task ON items(task);
        CREATE TABLE selection(note TEXT REFERENCES notes(id) ON DELETE CASCADE); CREATE INDEX selection_note ON selection(note);").await.unwrap();
    db.write(Default::default(), |context| {
        for n in 0..2000 {
            let note = format!("n{n}");
            context.execute("INSERT INTO notes VALUES(?1,'store',?1)", [&note])?;
            context.execute("INSERT INTO selection VALUES(?1)", [&note])?;
            for c in 0..2 {
                let task = format!("t{n}-{c}");
                context.execute("INSERT INTO tasks VALUES(?1,?2,'body')", [&task, &note])?;
                context.execute("INSERT INTO items VALUES(?1,?1,'body')", [&task])?;
            }
        }
        Ok(())
    })
    .await
    .unwrap();
    sql(&db, "UPDATE items SET body='changed' WHERE id='t0-0'")
        .await
        .unwrap();
    assert_indexed(&db);
    sql(
        &db,
        "UPDATE notes SET audience='00000000-0000-4000-8000-000000000001' WHERE id='n1'",
    )
    .await
    .unwrap();
    assert_indexed(&db);
    for c in 0..2 {
        let child = format!("t3-{c}");
        crate::removal::tests::remove(
            &db,
            "items",
            &child,
            &[],
            [coven_merge::Rule::ForeignKey(coven_merge::ForeignKey::new(
                ["task"],
                "tasks",
                ["id"],
            ))]
            .into(),
        );
        crate::removal::tests::remove(
            &db,
            "tasks",
            &child,
            &[],
            [coven_merge::Rule::ForeignKey(coven_merge::ForeignKey::new(
                ["note"],
                "notes",
                ["id"],
            ))]
            .into(),
        );
    }
    crate::removal::tests::remove(
        &db,
        "notes",
        "n3",
        &[],
        [coven_merge::Rule::Unique(["audience", "body"].into())].into(),
    );
    sql(&db, "UPDATE notes SET body='n3' WHERE id='n2'")
        .await
        .unwrap();
    assert_indexed(&db);
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT count(*) FROM notes WHERE id='n2'", [], |r| r
                .get::<_, i64>(0))
            .unwrap()),
        0
    );
    assert_eq!(
        db.inspect_writer(|db| db
            .query_row("SELECT count(*) FROM notes WHERE id='n3'", [], |r| r
                .get::<_, i64>(0))
            .unwrap()),
        1
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn an_immediate_foreign_key_fails_at_the_app_statement() {
    let fixture = TestStore::new();
    let db = fixture.schema(vec![SyncedTable::new("parents",RowIdentity::SharedKey),SyncedTable::new("children",RowIdentity::SharedKey)],"CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,parent TEXT REFERENCES parents(id))").await.unwrap();
    db.write(Default::default(),|context| {
        let error = context.execute("INSERT INTO children VALUES('c','missing')",[]).expect_err("immediate foreign key must fail before the next statement");
        assert!(matches!(error,rusqlite::Error::SqliteFailure(e,_) if e.extended_code==rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY));
        context.execute("INSERT INTO parents VALUES('p')",[])?;
        Ok(())
    }).await.unwrap();
    assert_eq!(count(&db, "children"), 0);
    assert_eq!(count(&db, "parents"), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn deleted_circles_only_affect_the_local_writes_region_after_reopening_too() {
    const SCHEMA: &str = "CREATE TABLE roots(id TEXT NOT NULL PRIMARY KEY,body TEXT); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,root TEXT REFERENCES roots(id)); CREATE INDEX notes_root ON notes(root)";
    let store = TestStore::new();
    let tables = || {
        vec![
            SyncedTable::new("roots", RowIdentity::SharedKey),
            SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        ]
    };
    let circle = coven_foundation::id_source::CircleId(
        uuid::Uuid::parse_str("00000000-0000-4000-8000-00000000000a").unwrap(),
    );
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    sql(&db,"INSERT INTO roots VALUES('r','before'),('other','unrelated'); INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-00000000000a','r')").await.unwrap();
    db.write([circle].into(), |_| Ok(())).await.unwrap();
    assert_eq!(count(&db, "notes"), 1);
    assert_eq!(records(&db).len(), 1);
    db.close().await.unwrap();
    let db = store.schema(tables(), SCHEMA).await.unwrap();
    db.write([circle].into(), |c| {
        c.execute("UPDATE roots SET body='after' WHERE id='other'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(count(&db, "notes"), 1);
    assert!(
        matches!(db.write([circle].into(), |c| { c.execute("DELETE FROM notes",[])?; Ok(()) }).await,Err(DbError::DeletedCircle(id)) if id==circle)
    );
    // Changing the referenced store row puts its child in removal's region.
    db.write([circle].into(), |c| {
        c.execute("UPDATE roots SET body='after' WHERE id='r'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(count(&db, "notes"), 0);
    assert_eq!(count(&db, "coven_lost"), 1);
    assert_eq!(records(&db).len(), 3);
    db.close().await.unwrap();
}
