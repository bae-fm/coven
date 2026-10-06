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

use crate::LiveQuery;
use std::time::Duration;

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("query must answer")
        .unwrap()
}

#[tokio::test]
async fn changeset_failure_rolls_back_and_reaches_the_writer_without_ending_queries() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)",
        )
        .await
        .unwrap();
    let mut query = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM items", [], |r| r.get::<_, String>(0))?));
    assert!(next(&mut query).await.is_empty());
    db.inspect_writer(|writer| {
        let result = writer.transaction(|writer| {
            writer.internal_execute("INSERT INTO items VALUES(1,'rolled back')", [])?;
            // Fail SQLite's own session lookup while taking its changeset.
            writer
                .connection
                .authorizer(Some(|context: AuthContext<'_>| {
                    if matches!(
                        context.action,
                        AuthAction::Read {
                            table_name: "items",
                            ..
                        }
                    ) {
                        Authorization::Deny
                    } else {
                        Authorization::Allow
                    }
                }))?;
            Ok(())
        });
        writer
            .connection
            .authorizer(Some(writer.authorization.callback()))
            .unwrap();
        assert!(matches!(
            result,
            Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(..)))
        ));
        assert_eq!(
            writer
                .query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    });
    assert!(!query.is_marked_for_rerun());
    db.commit_writer(|writer| {
        writer
            .internal_execute("INSERT INTO items VALUES(1,'committed')", [])
            .unwrap()
    });
    assert_eq!(next(&mut query).await, ["committed"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn commit_failure_and_net_zero_transactions_do_not_publish() {
    let store = TestStore::new();
    let db=store.schema(vec![],"CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE items(id INTEGER PRIMARY KEY,parent REFERENCES parent DEFERRABLE INITIALLY DEFERRED,value TEXT)").await.unwrap();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = calls.clone();
    let mut query = db.subscribe(move |sql| {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(sql.query("SELECT value FROM items", [], |r| r.get::<_, String>(0))?)
    });
    assert!(next(&mut query).await.is_empty());
    db.inspect_writer(|writer| {
        assert!(writer
            .transaction(|writer| writer.batch("INSERT INTO items VALUES(1,99,'invalid')"))
            .is_err());
    });
    assert!(!query.is_marked_for_rerun());
    db.commit_writer(|writer| {
        writer
            .batch("INSERT INTO items VALUES(1,NULL,'temporary'); DELETE FROM items")
            .unwrap()
    });
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn observation_and_local_write_sessions_cover_app_and_materialized_changes() {
    let store = TestStore::new();
    let db=store.schema(vec![SyncedTable::new("roots",RowIdentity::SharedKey),SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")],"CREATE TABLE roots(id TEXT NOT NULL PRIMARY KEY,value TEXT); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT,root TEXT REFERENCES roots(id)); CREATE INDEX notes_root ON notes(root); CREATE TABLE audit(value TEXT); CREATE TRIGGER gone AFTER DELETE ON notes BEGIN INSERT INTO audit VALUES(old.title); END").await.unwrap();
    let mut notes = db
        .subscribe(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?));
    let mut audit = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM audit", [], |r| r.get::<_, String>(0))?));
    let mut lost = db.subscribe_lost_values();
    assert!(next(&mut notes).await.is_empty());
    assert!(next(&mut audit).await.is_empty());
    assert!(next(&mut lost).await.is_empty());
    let circle = CircleId(uuid::Uuid::from_u128(7));
    db.write(BTreeSet::new(), move |sql| {
        sql.execute("INSERT INTO roots VALUES('root','before')", [])?;
        sql.execute(
            "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001',?1,'title','root')",
            [circle.to_string()],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(next(&mut notes).await, ["title"]);
    // Removal recomputation runs after part 2 drops its app-write session.
    db.write(BTreeSet::from([circle]), |sql| {
        sql.execute("UPDATE roots SET value='after'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(next(&mut notes).await.is_empty());
    assert_eq!(next(&mut audit).await, ["title"]);
    let losses = next(&mut lost).await;
    assert_eq!(losses.len(), 1);
    assert_eq!(
        losses[0].replaced_by,
        crate::Replacement::Rules(vec![crate::RemovalRule::DeletedCircle])
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn every_internal_statement_path_is_observed_by_the_transaction() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)",
        )
        .await
        .unwrap();
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT value FROM items", [], |r| r.get::<_, String>(0))?)
    });
    assert!(query.next().await.is_err());
    db.commit_writer(|writer| {
        writer
            .internal_execute("INSERT INTO items VALUES(1,'execute')", [])
            .unwrap()
    });
    assert_eq!(next(&mut query).await, "execute");
    db.commit_writer(|writer| {
        writer
            .query_row(
                "UPDATE items SET value='query_row' RETURNING value",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap()
    });
    assert_eq!(next(&mut query).await, "query_row");
    db.commit_writer(|writer| {
        writer
            .query("UPDATE items SET value='query' RETURNING value", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap()
    });
    assert_eq!(next(&mut query).await, "query");
    db.commit_writer(|writer| {
        writer
            .scan("UPDATE items SET value='scan' RETURNING value", [], |_| {
                Ok(())
            })
            .unwrap()
    });
    assert_eq!(next(&mut query).await, "scan");
    db.commit_writer(|writer| writer.batch("UPDATE items SET value='batch'").unwrap());
    assert_eq!(next(&mut query).await, "batch");
    db.close().await.unwrap();
}

#[tokio::test]
async fn hook_tables_are_selected_once_from_the_open_schema() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("synced", RowIdentity::SharedKey)],
            "
        CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY,value TEXT);
        CREATE TABLE ordinary(id INTEGER PRIMARY KEY,value TEXT);
        CREATE TABLE unkeyed(value TEXT);
        CREATE TABLE sqliteish(value TEXT);
        CREATE TABLE nullable(id TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE composite(a TEXT NOT NULL,b INTEGER,PRIMARY KEY(a,b));
        CREATE TABLE nonnull(id TEXT NOT NULL PRIMARY KEY,value TEXT);
        CREATE TABLE without_rowid(id TEXT PRIMARY KEY,value TEXT) WITHOUT ROWID;
        CREATE TABLE strict_key(id TEXT PRIMARY KEY,value TEXT) STRICT;
        CREATE TABLE descending(id INTEGER PRIMARY KEY DESC,value TEXT);
        CREATE TABLE alias_descending(id INTEGER,value TEXT,PRIMARY KEY(id DESC));
    ",
        )
        .await
        .unwrap();
    db.inspect_writer(|writer| {
        let tables = &writer.observation.as_ref().unwrap().supplemental;
        assert_eq!(
            tables.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "composite",
                "descending",
                "nullable",
                "sqliteish",
                "unkeyed"
            ]
        );
    });
    db.close().await.unwrap();
}

#[tokio::test]
async fn ordinary_tables_get_no_hook_change_and_retain_column_and_key_precision() {
    for (key, suffix, first, second) in [
        ("INTEGER PRIMARY KEY", "", "1", "2"),
        ("TEXT NOT NULL PRIMARY KEY", "", "'a'", "'b'"),
        ("TEXT PRIMARY KEY", " WITHOUT ROWID", "'a'", "'b'"),
        ("TEXT PRIMARY KEY", " STRICT", "'a'", "'b'"),
    ] {
        let store = TestStore::new();
        let schema = format!("CREATE TABLE items(id {key},value INTEGER,extra INTEGER){suffix}; INSERT INTO items VALUES({first},10,0),({second},20,0); CREATE TABLE hook_target(value TEXT)");
        let db = store
            .builder(
                vec![],
                vec![Migration::run(1, "schema", move |sql| {
                    sql.execute_batch(&schema)?;
                    Ok(())
                })],
            )
            .open()
            .await
            .unwrap();
        let runs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = runs.clone();
        let query = format!("SELECT value FROM items WHERE id={first}");
        let mut query = db.subscribe(move |sql| {
            let value = sql.query_row(&query, [], |r| r.get::<_, i64>(0))?;
            Ok((
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                value,
            ))
        });
        assert_eq!(next(&mut query).await, (0, 10));
        db.commit_writer(|writer| {
            writer.batch(&format!("UPDATE items SET extra=1 WHERE id={first}; UPDATE items SET value=21 WHERE id={second}; INSERT INTO hook_target VALUES('unrelated')")).unwrap();
        });
        assert!(!query.is_marked_for_rerun(), "{key}{suffix}");
        assert_eq!(
            runs.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "{key}{suffix}"
        );
        db.commit_writer(|writer| {
            writer
                .batch(&format!("UPDATE items SET value=11 WHERE id={first}"))
                .unwrap();
        });
        assert_eq!(next(&mut query).await, (1, 11));
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn hook_changes_publish_after_commit_and_are_discarded_on_transaction_failure() {
    let store = TestStore::new();
    let db=store.schema(vec![],"CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE local(id TEXT PRIMARY KEY,value TEXT,extra INTEGER,parent REFERENCES parent DEFERRABLE INITIALLY DEFERRED); INSERT INTO local VALUES(NULL,'unchanged',0,NULL)").await.unwrap();
    let count = std::sync::atomic::AtomicUsize::new(0);
    let mut query = db.subscribe(move |sql| {
        let values = sql.query("SELECT value FROM local WHERE id IS NULL", [], |r| {
            r.get::<_, String>(0)
        })?;
        Ok((
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            values,
        ))
    });
    assert_eq!(next(&mut query).await, (0, vec!["unchanged".into()]));
    db.inspect_writer(|writer| {
        let result = writer.transaction::<()>(|writer| {
            writer.batch("UPDATE local SET extra=1")?;
            Err(DbError::TransactionEnded)
        });
        assert!(matches!(result, Err(DbError::TransactionEnded)));
        assert!(!query.is_marked_for_rerun());
        assert!(writer
            .transaction(|writer| writer.batch("UPDATE local SET parent=99"))
            .is_err());
        assert!(!query.is_marked_for_rerun());
        // A later session-only commit must not publish stale hook changes.
        writer
            .transaction(|writer| writer.batch("INSERT INTO parent VALUES(1)"))
            .unwrap();
    });
    assert!(!query.is_marked_for_rerun());
    db.commit_writer(|writer| {
        writer.batch("UPDATE local SET extra=2").unwrap();
    });
    // Hook changes invalidate every column even when the selected value stayed equal.
    assert_eq!(next(&mut query).await, (1, vec!["unchanged".into()]));
    db.commit_writer(|writer| {
        writer.batch("DELETE FROM local").unwrap();
    });
    assert_eq!(next(&mut query).await, (2, vec![]));
    db.close().await.unwrap();
}

#[tokio::test]
async fn rowid_authorization_does_not_query_metadata_for_each_write_or_trigger() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,value BLOB); CREATE TABLE audit(value BLOB); INSERT INTO items VALUES('a',x'01'); CREATE TRIGGER effect AFTER UPDATE ON items BEGIN INSERT INTO audit VALUES(new.value); END").await.unwrap();
    db.inspect_writer(|writer| {
        writer
            .transaction(|writer| {
                let profile = writer.profile_write();
                for value in [vec![1u8; 4096], vec![2u8; 4096]] {
                    writer.app_execute("UPDATE items SET value=?1 WHERE id='a'", [value])?;
                }
                drop(profile);
                for (statement, _) in writer.fullscan_statements() {
                    assert!(
                        !statement.contains("pragma_table") && !statement.contains("sqlite_schema"),
                        "write queried metadata: {statement}"
                    );
                }
                Ok(())
            })
            .unwrap();
    });
    db.close().await.unwrap();
}
