use super::*;
use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
};

thread_local! {
    static SCANS: std::cell::RefCell<Vec<(String, i32)>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub(super) struct WriteProfile<'a>(&'a DatabaseConnection);

impl Drop for WriteProfile<'_> {
    fn drop(&mut self) {
        self.0
            .connection
            .trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
        *self.0.scans.lock().unwrap() =
            SCANS.with(|scans| std::mem::take(&mut *scans.borrow_mut()));
    }
}

impl DatabaseConnection {
    pub(super) fn profile_write(&self) -> WriteProfile<'_> {
        SCANS.with(|scans| scans.borrow_mut().clear());
        self.connection.trace_v2(
            rusqlite::trace::TraceEventCodes::SQLITE_TRACE_PROFILE,
            Some(|event| {
                if let rusqlite::trace::TraceEvent::Profile(statement, _) = event {
                    SCANS.with(|scans| {
                        scans.borrow_mut().push((
                            statement.sql().into_owned(),
                            statement.get_status(rusqlite::StatementStatus::FullscanStep),
                        ))
                    });
                }
            }),
        );
        WriteProfile(self)
    }

    pub(crate) fn fullscan_statements(&self) -> Vec<(String, i32)> {
        self.scans.lock().unwrap().clone()
    }

    pub(crate) fn integrity_checks(&self) -> usize {
        self.authorization.integrity_checks()
    }

    pub(crate) fn crash_after(&self, table: &str, action: &str) {
        self.connection
            .create_scalar_function(
                "test_crash",
                0,
                rusqlite::functions::FunctionFlags::SQLITE_UTF8
                    | rusqlite::functions::FunctionFlags::SQLITE_INNOCUOUS,
                |_| -> rusqlite::Result<i64> { std::process::exit(86) },
            )
            .unwrap();
        self.batch(&format!(
            "CREATE TRIGGER coven_crash AFTER {action} ON {table} BEGIN SELECT test_crash(); END"
        ))
        .unwrap();
    }
}

#[tokio::test]
async fn damaged_files_fail_before_migration_or_wal_changes() {
    let store = TestStore::new();
    let bytes = vec![0xaa; 8192];
    std::fs::write(store.database_path(), &bytes).unwrap();
    for read_only in [false, true] {
        let builder = store.builder(notes_tables(), notes_migrations());
        let result = if read_only {
            builder.open_read_only().await.map(|_| ())
        } else {
            builder.open().await.map(|_| ())
        };
        assert!(matches!(
            result.err().unwrap(),
            crate::CovenError::Database(DbError::DamagedDatabase)
        ));
        assert_eq!(std::fs::read(store.database_path()).unwrap(), bytes);
    }
}

#[tokio::test]
async fn integrity_check_notices_btree_damage_in_a_valid_database_file() {
    use std::io::{Seek, SeekFrom, Write};
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let (page, page_size) = db.inspect_writer(|sql| {
        (
            sql.query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name='notes'",
                [],
                |r| r.get::<_, u32>(0),
            )
            .unwrap(),
            sql.query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
                .unwrap(),
        )
    });
    db.close().await.unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(store.database_path())
        .unwrap();
    file.seek(SeekFrom::Start(u64::from(page - 1) * u64::from(page_size)))
        .unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    let error = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        crate::CovenError::Database(DbError::DamagedDatabase)
    ));
}

#[tokio::test]
async fn internal_schema_migrations_obey_policy_and_roll_back_on_failure() {
    let store = TestStore::new();
    let error = store
        .builder(vec![], vec![])
        .coven_migration_policy(CovenMigrationPolicy::RefusePending)
        .open()
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        crate::CovenError::CovenMigration(CovenMigrationError::Pending)
    ));
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch("CREATE TABLE coven_columns(sentinel TEXT)")
        .unwrap();
    drop(raw);
    let error = store.builder(vec![], vec![]).open().await.err().unwrap();
    assert!(matches!(
        error,
        crate::CovenError::CovenMigration(CovenMigrationError::Failed { .. })
    ));
    let raw = Connection::open(store.database_path()).unwrap();
    assert_eq!(
        raw.query_row("PRAGMA application_id", [], |r| r.get::<_, i32>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        raw.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = 'coven_writes'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn readonly_and_older_coven_refuse_an_internal_version_they_cannot_use() {
    let store = TestStore::new();
    let raw = Connection::open(store.database_path()).unwrap();
    drop(raw);
    assert!(matches!(
        store
            .builder(vec![], vec![])
            .open_read_only()
            .await
            .err()
            .unwrap(),
        crate::CovenError::CovenMigration(CovenMigrationError::Pending)
    ));
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.close().await.unwrap();
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch("PRAGMA application_id=2").unwrap();
    drop(raw);
    assert!(matches!(
        store.builder(vec![], vec![]).open().await.err().unwrap(),
        crate::CovenError::CovenMigration(CovenMigrationError::Pending)
    ));
}

#[tokio::test]
async fn bundled_sqlite_has_the_session_extension() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|sql| {
        assert!(sql.query_row("SELECT sqlite_compileoption_used('ENABLE_SESSION') AND sqlite_compileoption_used('ENABLE_PREUPDATE_HOOK')", [], |r| r.get::<_, bool>(0)).unwrap());
    });
    db.close().await.unwrap();
}

#[test]
fn wal_refusal_names_the_selected_journal_mode() {
    let sql =
        DatabaseConnection::open(Path::new(":memory:"), false, SqlAuthorization::new(&[])).unwrap();
    assert!(matches!(sql.enable_wal(), Err(DbError::WalUnavailable { mode }) if mode == "memory"));
}

#[tokio::test]
async fn a_nonnull_set_null_reference_cycle_is_refused_on_open() {
    let store = TestStore::new();
    let error = store.schema(
        vec![SyncedTable::new("nodes", crate::RowIdentity::IndependentUuid).audience_column("audience")],
        "CREATE TABLE nodes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,parent TEXT NOT NULL REFERENCES nodes(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED); CREATE INDEX nodes_parent ON nodes(parent)",
    ).await.err().expect("impossible action must be refused");
    assert!(
        matches!(crate::tests::database_error(error), DbError::Schema(crate::SchemaError::ImpossibleAction { table, column }) if table == "nodes" && column == "parent")
    );
}

const CYCLE_FIRST: &str = "00000000-0000-4000-8000-000000000001";
const CYCLE_SECOND: &str = "00000000-0000-4000-8000-000000000002";
const CYCLE_DEFAULT: &str = "00000000-0000-4000-8000-000000000003";
const CYCLE_CIRCLE: &str = "00000000-0000-4000-8000-00000000000a";

async fn reference_cycle(store: &TestStore, reference: &str) -> crate::Database {
    let schema = format!("CREATE TABLE nodes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,body TEXT,{reference}); CREATE INDEX nodes_parent ON nodes(parent)");
    let db = store
        .builder(
            vec![
                SyncedTable::new("nodes", crate::RowIdentity::IndependentUuid)
                    .audience_column("audience"),
            ],
            vec![Migration::run(1, "reference cycle", move |context| {
                context.execute_batch(&schema)?;
                Ok(())
            })],
        )
        .open()
        .await
        .unwrap();
    db.write(BTreeSet::new(), |context| {
        context.execute(
            "INSERT INTO nodes VALUES(?1,?4,'first',?2),(?2,?4,'second',?1),(?3,'store','default',?3)",
            [CYCLE_FIRST, CYCLE_SECOND, CYCLE_DEFAULT, CYCLE_CIRCLE],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    db
}

#[tokio::test]
async fn native_cycle_deletes_commit_with_enforcement_and_transaction_deferral() {
    // Exercise the native SQL boundary used by materialization, on the actual
    // writer and a schema and cycle accepted by the public database API.
    for action in ["RESTRICT", "NO ACTION", "CASCADE"] {
        for timing in ["NOT DEFERRABLE", "DEFERRABLE INITIALLY DEFERRED"] {
            for (first, second) in [(CYCLE_FIRST, CYCLE_SECOND), (CYCLE_SECOND, CYCLE_FIRST)] {
                let store = TestStore::new();
                let db = reference_cycle(
                    &store,
                    &format!(
                        "parent TEXT NOT NULL REFERENCES nodes(id) ON DELETE {action} {timing}"
                    ),
                )
                .await;
                db.inspect_writer(|writer| {
                    writer
                        .transaction(|writer| {
                            writer.batch("PRAGMA defer_foreign_keys=ON")?;
                            assert!(writer
                                .query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0))?);
                            assert!(writer
                                .query_row("PRAGMA defer_foreign_keys", [], |r| r
                                    .get::<_, bool>(0))?);
                            writer.internal_execute("DELETE FROM nodes WHERE id=?1", [first])?;
                            writer.internal_execute("DELETE FROM nodes WHERE id=?1", [second])?;
                            Ok(())
                        })
                        .unwrap();
                    assert!(writer
                        .query("PRAGMA foreign_key_check", [], |_| Ok(()))
                        .unwrap()
                        .is_empty());
                });
                assert_eq!(crate::write::tests::count(&db, "nodes"), 1);
                db.close().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn materialization_rolls_back_when_a_cycle_default_fails_a_check() {
    for timing in ["NOT DEFERRABLE", "DEFERRABLE INITIALLY DEFERRED"] {
        let store = TestStore::new();
        let db = reference_cycle(&store, &format!("parent TEXT NOT NULL DEFAULT '{CYCLE_DEFAULT}' REFERENCES nodes(id) ON DELETE SET DEFAULT {timing}, CONSTRAINT parent_allowed CHECK(id='{CYCLE_DEFAULT}' OR parent<>'{CYCLE_DEFAULT}')")).await;
        // The default is non-null and names an existing store row, so the
        // failure is the child's CHECK, not ImpossibleAction or a missing parent.
        for first in [Some(CYCLE_FIRST), Some(CYCLE_SECOND), None] {
            let error = db.inspect_writer(|writer| {
                writer
                    .transaction(|writer| {
                        writer.batch("PRAGMA defer_foreign_keys=ON")?;
                        assert!(
                            writer.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0))?
                        );
                        assert!(writer
                            .query_row("PRAGMA defer_foreign_keys", [], |r| r.get::<_, bool>(0))?);
                        writer.internal_execute(
                            "DELETE FROM nodes WHERE audience=?1 AND (?2 IS NULL OR id=?2)",
                            rusqlite::params![CYCLE_CIRCLE, first],
                        )?;
                        Ok(())
                    })
                    .unwrap_err()
            });
            assert!(
                matches!(error, DbError::Sqlite(rusqlite::Error::SqliteFailure(error, _)) if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK)
            );
            assert_eq!(crate::write::tests::count(&db, "nodes"), 3);
        }
        let before = db.inspect_writer(|db| {
            db.query(
                "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap()
            .into_iter()
            .map(|table| {
                let values = db
                    .query(
                        &format!("SELECT * FROM {}", crate::sql::identifier(&table)),
                        [],
                        |r| {
                            (0..r.as_ref().column_count())
                                .map(|i| r.get::<_, rusqlite::types::Value>(i))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        },
                    )
                    .unwrap();
                (table, values)
            })
            .collect::<Vec<_>>()
        });
        let error = materialize_cycle(&db).unwrap_err();
        assert!(
            matches!(error, DbError::Sqlite(rusqlite::Error::SqliteFailure(error, _)) if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK)
        );
        db.inspect_writer(|db| {
            for (table, expected) in before {
                let actual = db
                    .query(
                        &format!("SELECT * FROM {}", crate::sql::identifier(&table)),
                        [],
                        |r| {
                            (0..r.as_ref().column_count())
                                .map(|i| r.get::<_, rusqlite::types::Value>(i))
                                .collect::<rusqlite::Result<Vec<_>>>()
                        },
                    )
                    .unwrap();
                assert_eq!(actual, expected, "rollback of {table}");
            }
            assert!(db
                .query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0))
                .unwrap());
        });
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn materialization_takes_out_synced_cycles_with_foreign_keys_on() {
    for action in ["RESTRICT", "NO ACTION", "CASCADE"] {
        for timing in ["NOT DEFERRABLE", "DEFERRABLE INITIALLY DEFERRED"] {
            let store = TestStore::new();
            let db = reference_cycle(
                &store,
                &format!("parent TEXT NOT NULL REFERENCES nodes(id) ON DELETE {action} {timing}"),
            )
            .await;
            materialize_cycle(&db).unwrap();
            assert_eq!(
                crate::write::tests::count(&db, "nodes"),
                1,
                "{action} {timing}"
            );
            assert_eq!(crate::write::tests::count(&db, "coven_lost"), 2);
            assert_eq!(crate::write::tests::records(&db).len(), 1);
            db.inspect_writer(|db| {
                assert!(db
                    .query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0))
                    .unwrap());
                assert!(db
                    .query("PRAGMA foreign_key_check", [], |_| Ok(()))
                    .unwrap()
                    .is_empty());
            });
            db.close().await.unwrap();
        }
    }
}

use crate::write::tests::{count, records, sql};
use crate::RowIdentity;

#[tokio::test]
async fn restoration_triggers_cannot_leave_invalid_local_references() {
    for shape in [
        "id INT NOT NULL PRIMARY KEY",
        "id INT NOT NULL PRIMARY KEY,rowid TEXT,oid TEXT,_rowid_ TEXT",
        "rowid TEXT,oid TEXT,_rowid_ TEXT,id INT",
    ] {
        for suffix in ["", " WITHOUT ROWID"] {
            if shape.starts_with("rowid") && !suffix.is_empty() {
                continue;
            }
            let store = TestStore::new();
            let schema=format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TABLE local({shape},note TEXT REFERENCES notes(id)){suffix}; CREATE TRIGGER restore AFTER INSERT ON notes WHEN coven_applying() BEGIN INSERT INTO local(id,note) VALUES(1,'missing'); END");
            let db = store
                .builder(
                    vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                    vec![crate::Migration::run(1, "schema", move |c| {
                        c.execute_batch(&schema)?;
                        Ok(())
                    })],
                )
                .open()
                .await
                .unwrap();
            sql(&db,"INSERT INTO notes VALUES('45','Groceries'); INSERT INTO notes VALUES('46','Shopping')").await.unwrap();
            crate::removal::tests::remove(
                &db,
                "notes",
                "46",
                &[(
                    "title",
                    coven_format::value::Value::Text("Groceries".into()),
                )],
                [coven_merge::Rule::Unique(["title"].into())].into(),
            );
            assert!(
                sql(&db, "DELETE FROM notes WHERE id='45'").await.is_err(),
                "{shape}{suffix}"
            );
            assert_eq!(count(&db, "local"), 0);
            assert_eq!(records(&db).len(), 1);
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn restoration_trigger_cannot_orphan_unchanged_local_children() {
    let store = TestStore::new();
    let db=store.schema(vec![SyncedTable::new("notes",RowIdentity::SharedKey)],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TABLE folders(id TEXT PRIMARY KEY); CREATE TABLE links(folder TEXT REFERENCES folders(id)); CREATE TRIGGER restore AFTER INSERT ON notes WHEN coven_applying() BEGIN DELETE FROM folders WHERE id='Work'; END").await.unwrap();
    sql(&db,"INSERT INTO folders VALUES('Work'); INSERT INTO links VALUES('Work'); INSERT INTO notes VALUES('45','Groceries'),('46','Shopping')").await.unwrap();
    crate::removal::tests::remove(
        &db,
        "notes",
        "46",
        &[(
            "title",
            coven_format::value::Value::Text("Groceries".into()),
        )],
        [coven_merge::Rule::Unique(["title"].into())].into(),
    );
    assert!(sql(&db, "DELETE FROM notes WHERE id='45'").await.is_err());
    assert_eq!(count(&db, "folders"), 1);
    assert_eq!(records(&db).len(), 1);
    db.close().await.unwrap();
}

fn materialize_cycle(db: &crate::Database) -> Result<(), DbError> {
    crate::removal::tests::materialize_deleted(
        db,
        "nodes",
        &[CYCLE_FIRST, CYCLE_SECOND],
        CircleId(uuid::Uuid::parse_str(CYCLE_CIRCLE).unwrap()),
    )
}

#[tokio::test]
async fn audience_validation_looks_up_children_only_when_the_parent_moves() {
    let fixture = TestStore::new();
    let db=fixture.schema(vec![SyncedTable::new("albums",RowIdentity::IndependentUuid).audience_column("audience"),SyncedTable::new("tracks",RowIdentity::SharedKey)],"CREATE TABLE albums(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT); CREATE TABLE tracks(id TEXT NOT NULL PRIMARY KEY,album TEXT REFERENCES albums(id)); CREATE INDEX tracks_album ON tracks(album)").await.unwrap();
    sql(
        &db,
        "INSERT INTO albums VALUES('a','store','Before'); INSERT INTO tracks VALUES('t','a')",
    )
    .await
    .unwrap();
    for moving in [false, true] {
        db.inspect_writer_schema(|writer,schema| {
            writer.batch("BEGIN IMMEDIATE").unwrap();
            let mut session=rusqlite::session::Session::new(&writer.connection).unwrap();
            session.attach(Some("albums")).unwrap();
            writer.internal_execute(if moving { "UPDATE albums SET audience='00000000-0000-4000-8000-00000000000a' WHERE id='a'" } else { "UPDATE albums SET title='After' WHERE id='a'" },[]).unwrap();
            let changeset = { let _scope=writer.authorization.internal(); session.changeset().unwrap() };
            drop(session);
            let captured=crate::write_capture::capture(&changeset,&schema.schema).unwrap();
            let before=crate::write_rows::AppView::before(writer,schema,&captured).unwrap();
            let after=crate::write_rows::AppView::after(writer,schema);
            let stored=crate::merge_store::MergeStore::new(writer,&before);
            let profile=writer.profile_write();
            let changes=crate::write_record::changes(writer,schema,&before,&after,&stored,&captured,&BTreeSet::new());
            drop(profile);
            let lookups=writer.fullscan_statements().into_iter().filter(|(sql,_)| sql.starts_with("SELECT DISTINCT r.table_name,r.key,r.audience FROM coven_references v")).count();
            if moving {
                assert!(matches!(changes,Err(DbError::ReferenceAudience{..})));
                assert_eq!(lookups,1);
            } else {
                assert_eq!(changes.unwrap().len(),1);
                assert_eq!(lookups,0,"a title change must not visit tracks");
            }
            writer.batch("ROLLBACK").unwrap();
        });
    }
    db.close().await.unwrap();
}
