use super::*;
use crate::read::tests::AppError;
use crate::tests::next;
use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
};
use coven_foundation::id_source::CircleId;
use std::sync::Arc;

/// One statement execution the profile saw.
#[derive(Clone, Debug)]
pub(crate) struct ProfiledStatement {
    pub(crate) sql: String,
    /// Full-scan steps of this execution alone.
    pub(crate) fullscan_steps: i32,
    /// Executions of this prepared statement so far, this one included.
    pub(crate) runs: i32,
    /// Virtual machine steps of this prepared statement so far, this
    /// execution's included.
    pub(crate) vm_steps: i32,
    /// Times SQLite prepared this statement again so far, this execution's
    /// included.
    pub(crate) reprepares: i32,
}

impl DatabaseConnection {
    pub(crate) fn fail_at(&self, name: &str, event: &str, message: &str) {
        self.batch(&format!(
            "CREATE TRIGGER {} {event} BEGIN SELECT RAISE(ABORT,'{}'); END",
            crate::sql::identifier(name),
            message.replace('\'', "''")
        ))
        .unwrap();
    }

    /// Prepare a statement and never finalize it, so SQLite refuses to close
    /// this connection, as it does with `SQLITE_BUSY`.
    pub(crate) fn leave_statement_open(&self) {
        let mut statement = std::ptr::null_mut();
        // SAFETY: the handle is this live connection's and the text is a
        // NUL-terminated literal. The statement is never used or finalized.
        let prepared = unsafe {
            rusqlite::ffi::sqlite3_prepare_v2(
                self.connection.handle(),
                c"SELECT 1".as_ptr(),
                -1,
                &mut statement,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(prepared, rusqlite::ffi::SQLITE_OK);
    }

    pub(crate) fn record_merge_load(&self, table: &str) {
        *self.merge_loads.lock().unwrap().entry(table.into()).or_default() += 1;
    }

    pub(crate) fn merge_loads(&self) -> BTreeMap<String, usize> {
        self.merge_loads.lock().unwrap().clone()
    }

    pub(crate) fn set_value_limit(&self, bytes: i32) -> i32 {
        self.connection.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, bytes).unwrap()
    }

    /// Trace this connection's statements until the matching
    /// [`Self::end_profile`]. A profile inside another, as a migration's
    /// inside the open's, leaves the outer one tracing.
    pub(super) fn start_profile(&mut self) {
        let open = self.profiles.get();
        self.profiles.set(open + 1);
        if open > 0 {
            return;
        }
        self.scans.lock().unwrap().clear();
        let scans = Arc::clone(&self.scans);
        self.connection
            .trace_v2(
                rusqlite::trace::TraceEventCodes::SQLITE_TRACE_PROFILE,
                Some(move |event: rusqlite::trace::TraceEvent<'_>| {
                    if let rusqlite::trace::TraceEvent::Profile(statement, _) = event {
                        scans.lock().unwrap().push(ProfiledStatement {
                            sql: statement.sql().into_owned(),
                            fullscan_steps: statement
                                .get_status(rusqlite::StatementStatus::FullscanStep),
                            runs: statement.get_status(rusqlite::StatementStatus::Run),
                            vm_steps: statement.get_status(rusqlite::StatementStatus::VmStep),
                            reprepares: statement.get_status(rusqlite::StatementStatus::RePrepare),
                        });
                    }
                }),
            )
            .unwrap();
    }

    /// End a profile; only the outermost stops tracing.
    pub(super) fn end_profile(&mut self) {
        let open = self.profiles.get() - 1;
        self.profiles.set(open);
        if open == 0 {
            self.connection
                .trace_v2(
                    rusqlite::trace::TraceEventCodes::empty(),
                    None::<fn(rusqlite::trace::TraceEvent<'_>)>,
                )
                .unwrap();
        }
    }

    pub(crate) fn profiled_statements(&self) -> Vec<ProfiledStatement> {
        self.scans.lock().unwrap().clone()
    }

    pub(crate) fn crash_after_next_commit(&mut self) {
        self.connection
            .wal_hook(Some(|_: &mut rusqlite::hooks::Wal, _| std::process::exit(86)))
            .unwrap();
    }

    /// Panic in the next call of one of coven's SQL functions on this
    /// connection.
    pub(crate) fn panic_next_function(&self) {
        self.authorization.panic_next_function();
    }

    /// Panic in the next judgment of this connection's authorizer.
    pub(crate) fn panic_next_authorization(&self) {
        self.authorization.panic_next();
    }

    /// Panic in the next call of a write transaction's hook on this
    /// connection.
    pub(crate) fn panic_in_next_hook(&self) {
        self.hook_panics.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn refuse_transaction_end(&mut self, refuse: bool) {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
        let mut authorize = self.authorization.callback();
        self.connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if refuse
                    && matches!(
                        context.action,
                        AuthAction::Transaction { operation }
                            if !matches!(operation, TransactionOperation::Begin)
                    )
                {
                    Authorization::Deny
                } else {
                    authorize(context)
                }
            }))
            .unwrap();
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
            "CREATE TRIGGER _coven_crash AFTER {action} ON {table} BEGIN SELECT test_crash(); END"
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
            crate::OpenError::Database(DbError::DamagedDatabase)
        ));
        assert_eq!(std::fs::read(store.database_path()).unwrap(), bytes);
    }
}

#[tokio::test]
async fn internal_schema_migrates_on_open_and_preserves_app_data() {
    let store = TestStore::new();
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch("CREATE TABLE local(value TEXT); INSERT INTO local VALUES('kept')").unwrap();
    drop(raw);
    for _ in 0..2 {
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        db.inspect_writer(|sql| {
            assert_eq!(
                sql.query_row("PRAGMA application_id", [], |r| r.get::<_, u32>(0)).unwrap(),
                internal_schema::VERSION
            );
        });
        assert_eq!(count(&db, "_coven_writes"), 0);
        assert_eq!(
            db.read(|sql| Ok::<_, DbError>(
                sql.query_row("SELECT value FROM local", [], |r| r.get::<_, String>(0))?
            ))
            .await
            .unwrap(),
            "kept"
        );
        db.close().await.unwrap();
    }
    let reader = store.builder(vec![], vec![]).open_read_only().await.unwrap();
    assert_eq!(
        reader
            .read(|sql| Ok::<_, DbError>(
                sql.query_row("SELECT value FROM local", [], |r| r.get::<_, String>(0))?
            ))
            .await
            .unwrap(),
        "kept"
    );
    reader.close().await.unwrap();
}

#[tokio::test]
async fn internal_schema_migrations_roll_back_on_failure() {
    let store = TestStore::new();
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch(
        "CREATE TABLE coven_files(sentinel TEXT); INSERT INTO coven_files VALUES('kept')",
    )
    .unwrap();
    drop(raw);
    let error = store.builder(vec![], vec![]).open().await.err().unwrap();
    assert!(matches!(error, crate::OpenError::CovenMigration(CovenMigrationError::Failed { .. })));
    let raw = Connection::open(store.database_path()).unwrap();
    assert_eq!(raw.query_row("PRAGMA application_id", [], |r| r.get::<_, i32>(0)).unwrap(), 0);
    assert_eq!(
        raw.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get::<_, i64>(0)).unwrap(),
        1
    );
    assert_eq!(
        raw.query_row("SELECT sentinel FROM coven_files", [], |r| r.get::<_, String>(0)).unwrap(),
        "kept"
    );
}

#[tokio::test]
async fn read_only_refuses_internal_migration_without_changing_database() {
    let store = TestStore::new();
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch("CREATE TABLE local(value TEXT); INSERT INTO local VALUES('kept')").unwrap();
    drop(raw);
    let before = std::fs::read(store.database_path()).unwrap();
    assert!(matches!(
        store.builder(vec![], vec![]).open_read_only().await.err().unwrap(),
        crate::OpenError::CovenMigration(CovenMigrationError::ReadOnly)
    ));
    assert_eq!(std::fs::read(store.database_path()).unwrap(), before);
}

#[tokio::test]
async fn newer_internal_schema_is_refused_without_changes() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.close().await.unwrap();
    for version in [internal_schema::VERSION + 1, u32::MAX] {
        let raw = Connection::open(store.database_path()).unwrap();
        raw.execute_batch(&format!("PRAGMA application_id={}", version as i32)).unwrap();
        drop(raw);
        let before = std::fs::read(store.database_path()).unwrap();
        for read_only in [false, true] {
            let builder = store.builder(vec![], vec![]);
            let result = if read_only {
                builder.open_read_only().await.map(|_| ())
            } else {
                builder.open().await.map(|_| ())
            };
            assert!(matches!(
                result,
                Err(crate::OpenError::CovenMigration(CovenMigrationError::SchemaTooNew {
                    current,
                    supported,
                })) if current == version && supported == internal_schema::VERSION
            ));
            assert_eq!(std::fs::read(store.database_path()).unwrap(), before);
        }
    }
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

/// SQLite keeps a statement's journal in memory up to 124 KiB (§21.2).
#[tokio::test]
async fn bundled_sqlite_keeps_a_statements_journal_in_memory_up_to_124_kib() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|sql| {
        assert!(sql
            .query_row("SELECT sqlite_compileoption_used('STMTJRNL_SPILL=126976')", [], |r| r
                .get::<_, bool>(0))
            .unwrap());
    });
    db.close().await.unwrap();
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
const CYCLE_CIRCLE: &str = "00000000-0000-4000-8000-00000000000a";

async fn reference_cycle(store: &TestStore, reference: &str) -> crate::Database {
    let schema = format!("CREATE TABLE nodes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,body TEXT,{reference}); CREATE INDEX nodes_parent ON nodes(parent)");
    let db = store
        .builder(
            vec![SyncedTable::new("nodes", crate::RowIdentity::IndependentUuid)
                .audience_column("audience")],
            vec![Migration::addition_with(1, "reference cycle", move |context| {
                context.execute_batch(&schema)?;
                Ok(())
            })],
        )
        .open()
        .await
        .unwrap();
    store.join_circle(&db, CircleId(uuid::Uuid::parse_str(CYCLE_CIRCLE).unwrap()));
    db.write(|context| {
        context.execute(
            "INSERT INTO nodes VALUES(?1,?3,'first',?2),(?2,?3,'second',?1)",
            [CYCLE_FIRST, CYCLE_SECOND, CYCLE_CIRCLE],
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
                assert_eq!(crate::write::tests::count(&db, "nodes"), 0);
                db.close().await.unwrap();
            }
        }
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
            assert_eq!(crate::write::tests::count(&db, "nodes"), 0, "{action} {timing}");
            assert_eq!(crate::write::tests::count(&db, "_coven_lost"), 2);
            assert_eq!(crate::write::tests::records(&db).len(), 1);
            db.inspect_writer(|db| {
                assert!(db.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0)).unwrap());
                assert!(db.query("PRAGMA foreign_key_check", [], |_| Ok(())).unwrap().is_empty());
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
            let schema=format!("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TABLE local({shape},note TEXT REFERENCES notes(id) ON DELETE CASCADE){suffix}; CREATE TRIGGER restore AFTER INSERT ON notes WHEN coven_applying() BEGIN INSERT INTO local(id,note) VALUES(1,'missing'); END");
            let db = store
                .builder(
                    vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
                    vec![crate::Migration::addition_with(1, "schema", move |c| {
                        c.execute_batch(&schema)?;
                        Ok(())
                    })],
                )
                .open()
                .await
                .unwrap();
            sql(&db,"INSERT INTO notes VALUES('45','Groceries'); INSERT INTO notes VALUES('46','Shopping')").await.unwrap();
            crate::tests::remote_update(
                &db,
                "notes",
                "46",
                &[("title", coven_format::value::Value::Text("Groceries".into()))],
            )
            .await;
            assert!(sql(&db, "DELETE FROM notes WHERE id='45'").await.is_err(), "{shape}{suffix}");
            assert_eq!(count(&db, "local"), 0);
            assert_eq!(records(&db).len(), 1);
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn restoration_trigger_cannot_orphan_unchanged_local_children() {
    let store = TestStore::new();
    let db=store.schema(vec![SyncedTable::new("notes",RowIdentity::SharedKey)],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TABLE folders(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(folder TEXT REFERENCES folders(id)); CREATE TRIGGER restore AFTER INSERT ON notes WHEN coven_applying() BEGIN DELETE FROM folders WHERE id='Work'; END").await.unwrap();
    sql(&db,"INSERT INTO folders VALUES('Work'); INSERT INTO links VALUES('Work'); INSERT INTO notes VALUES('45','Groceries'),('46','Shopping')").await.unwrap();
    crate::tests::remote_update(
        &db,
        "notes",
        "46",
        &[("title", coven_format::value::Value::Text("Groceries".into()))],
    )
    .await;
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
        "INSERT INTO albums VALUES('00000000-0000-4000-8000-000000000001','store','Before'); INSERT INTO tracks VALUES('t','00000000-0000-4000-8000-000000000001')",
    )
    .await
    .unwrap();
    for moving in [false, true] {
        db.inspect_writer_schema(|writer,schema| {
            // The profile holds the update and its recording too, which run
            // none of the statements counted below.
            let changes=writer.with_profile(|writer| {
                let mut transaction=super::write_transaction::WriteTransaction::begin(writer,false).unwrap();
                let recording=super::event_capture::EventCapture::begin(&mut transaction).unwrap();
                transaction.within(|db| db.internal_execute(if moving { "UPDATE albums SET audience='00000000-0000-4000-8000-00000000000a' WHERE id='00000000-0000-4000-8000-000000000001'" } else { "UPDATE albums SET title='After' WHERE id='00000000-0000-4000-8000-000000000001'" },[])).unwrap();
                transaction.stop_observing_operations().unwrap();
                let events=recording.events();
                let captured=crate::write_capture::capture(schema,&events).unwrap();
                transaction.within(|writer| {
                    let before=crate::write_rows::AppView::before(writer,schema,&captured).unwrap();
                    let after=crate::write_rows::AppView::after(writer,schema);
                    let stored=crate::merge_store::MergeStore::new(writer,&before);
                    crate::write_record::changes(writer,schema,&before,&after,&stored,&before.changes(&after, captured.keys().cloned()).unwrap(),&Default::default())
                })
            });
            let lookups=writer.profiled_statements().into_iter().filter(|s| s.sql.starts_with("SELECT DISTINCT r.table_name,r.key,r.audience FROM _coven_references v")).count();
            if moving {
                assert!(matches!(changes,Err(DbError::ReferenceAudience{..})));
                assert_eq!(lookups,1);
            } else {
                assert_eq!(changes.unwrap().len(),1);
                assert_eq!(lookups,0,"a title change must not visit tracks");
            }
        });
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn changeset_failure_rolls_back_and_reaches_the_writer_without_ending_queries() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)")
        .await
        .unwrap();
    let mut query: crate::LiveQuery<_, DbError> = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM items", [], |r| r.get::<_, String>(0))?));
    assert!(next(&mut query).await.is_empty());
    db.inspect_writer(|writer| {
        // Fail SQLite's own session lookup while taking its changeset, once
        // the transaction's own statements have run.
        let refusing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let refuses = Arc::clone(&refusing);
        writer
            .connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if refuses.load(std::sync::atomic::Ordering::Relaxed)
                    && matches!(context.action, AuthAction::Read { table_name: "items", .. })
                {
                    Authorization::Deny
                } else {
                    Authorization::Allow
                }
            }))
            .unwrap();
        let result = writer.transaction(|writer| {
            writer.internal_execute("INSERT INTO items VALUES(1,'rolled back')", [])?;
            refusing.store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        });
        writer.connection.authorizer(Some(writer.authorization.callback())).unwrap();
        assert!(matches!(result, Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(..)))));
        assert_eq!(
            writer.query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0)).unwrap(),
            0
        );
    });
    assert!(!query.is_marked_for_rerun());
    db.commit_writer(|writer| {
        writer.internal_execute("INSERT INTO items VALUES(1,'committed')", []).unwrap()
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
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |sql| {
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
        writer.batch("INSERT INTO items VALUES(1,NULL,'temporary'); DELETE FROM items").unwrap()
    });
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn observation_and_local_write_sessions_cover_app_and_materialized_changes() {
    let store = TestStore::new();
    let db=store.schema(vec![SyncedTable::new("roots",RowIdentity::SharedKey),SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")],"CREATE TABLE roots(id TEXT NOT NULL PRIMARY KEY,value TEXT); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT,root TEXT REFERENCES roots(id)); CREATE INDEX notes_root ON notes(root); CREATE TABLE audit(value TEXT); CREATE TRIGGER gone AFTER DELETE ON notes BEGIN INSERT INTO audit VALUES(old.title); END").await.unwrap();
    let mut notes: crate::LiveQuery<_, DbError> = db
        .subscribe(|sql| Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?));
    let mut audit: crate::LiveQuery<_, DbError> = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM audit", [], |r| r.get::<_, String>(0))?));
    let mut lost = db.subscribe_lost_values::<DbError>();
    assert!(next(&mut notes).await.is_empty());
    assert!(next(&mut audit).await.is_empty());
    assert!(next(&mut lost).await.is_empty());
    let circle = CircleId(uuid::Uuid::from_u128(7));
    store.join_circle(&db, circle);
    db.write(move |sql| {
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
    crate::tests::delete_circle(&db, circle).await.unwrap();
    assert!(next(&mut notes).await.is_empty());
    assert_eq!(next(&mut audit).await, ["title"]);
    let losses = next(&mut lost).await;
    assert_eq!(losses.len(), 1);
    assert_eq!(
        losses[0].replaced_by,
        crate::Replacement::Rules(vec![crate::RemovalRule::DeletedCircle {
            entry: db.store_state().await.unwrap().circle_deletions[&circle]
        }])
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn every_internal_statement_path_is_observed_by_the_transaction() {
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)")
        .await
        .unwrap();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT value FROM items", [], |r| r.get::<_, String>(0))?)
    });
    assert!(query.next().await.expect("the live query answers").is_err());
    db.commit_writer(|writer| {
        writer.internal_execute("INSERT INTO items VALUES(1,'execute')", []).unwrap()
    });
    assert_eq!(next(&mut query).await, "execute");
    db.commit_writer(|writer| {
        writer
            .query_row("UPDATE items SET value='query_row' RETURNING value", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap()
    });
    assert_eq!(next(&mut query).await, "query_row");
    db.commit_writer(|writer| {
        writer
            .query("UPDATE items SET value='query' RETURNING value", [], |r| r.get::<_, String>(0))
            .unwrap()
    });
    assert_eq!(next(&mut query).await, "query");
    db.commit_writer(|writer| writer.batch("UPDATE items SET value='batch'").unwrap());
    assert_eq!(next(&mut query).await, "batch");
    db.close().await.unwrap();
}

/// The tables an update hook watches beside the session are those of the
/// schema no primary key names.
#[tokio::test]
async fn hook_tables_are_the_schemas_tables_with_no_primary_key() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("synced", RowIdentity::SharedKey)],
            "
        CREATE TABLE synced(id TEXT NOT NULL PRIMARY KEY,value TEXT);
        CREATE TABLE ordinary(id INTEGER PRIMARY KEY,value TEXT);
        CREATE TABLE unkeyed(value TEXT);
        CREATE TABLE sqliteish(value TEXT);
        CREATE TABLE composite(a TEXT NOT NULL,b INTEGER NOT NULL,PRIMARY KEY(a,b));
        CREATE TABLE nonnull(id TEXT NOT NULL PRIMARY KEY,value TEXT);
        CREATE TABLE without_rowid(id TEXT PRIMARY KEY,value TEXT) WITHOUT ROWID;
        CREATE TABLE strict_key(id TEXT PRIMARY KEY,value TEXT) STRICT;
        CREATE TABLE descending(id INTEGER NOT NULL PRIMARY KEY DESC,value TEXT);
        CREATE TABLE alias_descending(id INTEGER,value TEXT,PRIMARY KEY(id DESC));
    ",
        )
        .await
        .unwrap();
    db.inspect_writer(|writer| {
        let shapes = writer.table_shapes().unwrap();
        let mut whole: Vec<&str> = shapes
            .iter()
            .filter(|(_, shape)| shape.whole)
            .map(|(_, shape)| shape.name.as_str())
            .collect();
        whole.sort_unstable();
        assert_eq!(whole, ["sqliteish", "unkeyed"]);
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
                vec![Migration::addition_with(1, "schema", move |sql| {
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
        let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |sql| {
            let value = sql.query_row(&query, [], |r| r.get::<_, i64>(0))?;
            Ok((count.fetch_add(1, std::sync::atomic::Ordering::SeqCst), value))
        });
        assert_eq!(next(&mut query).await, (0, 10));
        db.commit_writer(|writer| {
            writer.batch(&format!("UPDATE items SET extra=1 WHERE id={first}; UPDATE items SET value=21 WHERE id={second}; INSERT INTO hook_target VALUES('unrelated')")).unwrap();
        });
        assert!(!query.is_marked_for_rerun(), "{key}{suffix}");
        assert_eq!(runs.load(std::sync::atomic::Ordering::SeqCst), 1, "{key}{suffix}");
        db.commit_writer(|writer| {
            writer.batch(&format!("UPDATE items SET value=11 WHERE id={first}")).unwrap();
        });
        assert_eq!(next(&mut query).await, (1, 11));
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn hook_changes_publish_after_commit_and_are_discarded_on_transaction_failure() {
    let store = TestStore::new();
    let db=store.schema(vec![],"CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE local(id TEXT,value TEXT,extra INTEGER,parent REFERENCES parent DEFERRABLE INITIALLY DEFERRED); INSERT INTO local VALUES(NULL,'unchanged',0,NULL)").await.unwrap();
    let count = std::sync::atomic::AtomicUsize::new(0);
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |sql| {
        let values =
            sql.query("SELECT value FROM local WHERE id IS NULL", [], |r| r.get::<_, String>(0))?;
        Ok((count.fetch_add(1, std::sync::atomic::Ordering::SeqCst), values))
    });
    assert_eq!(next(&mut query).await, (0, vec!["unchanged".into()]));
    db.inspect_writer(|writer| {
        let result = writer.transaction::<()>(|writer| {
            writer.batch("UPDATE local SET extra=1")?;
            Err(DbError::TransactionEnded)
        });
        assert!(matches!(result, Err(DbError::TransactionEnded)));
        assert!(!query.is_marked_for_rerun());
        assert!(writer.transaction(|writer| writer.batch("UPDATE local SET parent=99")).is_err());
        assert!(!query.is_marked_for_rerun());
        // A later session-only commit must not publish stale hook changes.
        writer.transaction(|writer| writer.batch("INSERT INTO parent VALUES(1)")).unwrap();
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
    // The metadata reads of a write of two updates and one of six: each
    // update and the trigger it fires read none, so the two are the same,
    // whatever the commit reads once. A first write reads the tables'
    // shapes, once for the schema.
    let metadata_reads = |updates: u8| {
        db.inspect_writer(|writer| {
            writer
                .with_profile(|writer| {
                    writer.transaction(|writer| {
                        for value in 0..updates {
                            writer.app_execute(
                                "UPDATE items SET value=?1 WHERE id='a'",
                                [vec![value; 4096]],
                            )?;
                        }
                        Ok(())
                    })
                })
                .unwrap();
            writer
                .profiled_statements()
                .into_iter()
                .filter(|s| s.sql.contains("pragma_table") || s.sql.contains("sqlite_schema"))
                .map(|s| s.sql)
                .collect::<Vec<_>>()
        })
    };
    metadata_reads(1);
    assert_eq!(metadata_reads(2), metadata_reads(6));
    db.close().await.unwrap();
}

#[tokio::test]
async fn local_restrict_children_are_refused_before_circle_removal() {
    let store = TestStore::new();
    let result = store.schema(
        vec![SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")],
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL); CREATE TABLE pins(note TEXT REFERENCES notes(id) ON DELETE RESTRICT)",
    ).await;
    let error = crate::tests::database_error(
        result.err().expect("local pins must not block a synced deletion"),
    );
    assert!(
        matches!(error, DbError::Schema(crate::SchemaError::LocalChildAction { table, column }) if table == "pins" && column == "note")
    );
}

#[tokio::test]
async fn a_failed_rollback_fails_every_later_call_until_reopen() {
    for internal in [false, true] {
        let store = TestStore::new();
        let open = || store.builder(notes_tables(), notes_migrations()).open();
        let db = open().await.unwrap();
        let mut notes = db.subscribe(|sql| {
            Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
        });
        assert!(next(&mut notes).await.is_empty());
        db.inspect_writer(|writer| writer.refuse_transaction_end(true));
        // Neither transaction changes pending state; each fails after a change
        // that its refused ROLLBACK leaves on the writer.
        let error = if internal {
            db.inspect_writer(|writer| {
                writer.transaction(|writer| {
                    writer.internal_execute("INSERT INTO notes VALUES('n','held','')", [])?;
                    Err::<(), _>(DbError::StoreClosed)
                })
            })
        } else {
            db.write(|sql| {
                sql.execute("INSERT INTO notes VALUES('n','held','')", [])?;
                Err::<(), _>(DbError::StoreClosed)
            })
            .await
        }
        .unwrap_err();
        // An app write's file cleanup then fails too, inside the open transaction.
        let write = match &error {
            DbError::FileCleanup { write: Err(write), .. } if !internal => write.as_ref(),
            error => error,
        };
        assert!(
            matches!(write, DbError::Rollback { operation, .. } if matches!(**operation, DbError::StoreClosed)),
            "{error:?}"
        );
        let held = |db: &crate::Database| {
            db.inspect_writer(|writer| {
                writer.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))
            })
            .unwrap()
        };
        assert_eq!(held(&db), 1, "the writer still holds the uncommitted row");
        let rollback_failed = |error: DbError| matches!(error, DbError::RollbackFailed);
        assert!(rollback_failed(db.current_claim().await.unwrap_err()));
        assert!(rollback_failed(db.write(|_| Ok(())).await.unwrap_err()));
        assert!(rollback_failed(db.schema_version().await.unwrap_err()));
        assert!(matches!(db.read(|_| Ok(())).await.unwrap_err(), DbError::RollbackFailed));
        assert!(matches!(
            notes.next().await.expect("the live query answers").unwrap_err(),
            DbError::RollbackFailed
        ));
        db.close().await.unwrap();
        let db = open().await.unwrap();
        assert_eq!(held(&db), 0);
        assert!(db.current_claim().await.unwrap().is_none());
        db.close().await.unwrap();
    }
}

/// A callback that fails with its own error while SQLite also fails to roll
/// it back keeps both causes, and later calls fail until the store reopens
/// (§4).
#[tokio::test]
async fn a_callback_error_whose_rollback_fails_keeps_both_and_fails_later_calls() {
    let store = TestStore::new();
    let open = || store.builder(notes_tables(), notes_migrations()).open();
    let db = open().await.unwrap();
    db.inspect_writer(|writer| writer.refuse_transaction_end(true));
    let error = db
        .write_as(|sql| {
            sql.execute("INSERT INTO notes VALUES('n','held','')", [])?;
            Err::<(), _>(AppError::Refused("no"))
        })
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            AppError::Rollback { operation, .. }
                if matches!(**operation, AppError::Refused("no"))
        ),
        "{error:?}"
    );
    assert!(matches!(db.write(|_| Ok(())).await.unwrap_err(), DbError::RollbackFailed));
    assert!(matches!(db.schema_version().await.unwrap_err(), DbError::RollbackFailed));
    db.close().await.unwrap();
    let db = open().await.unwrap();
    db.write(|_| Ok(())).await.unwrap();
    db.close().await.unwrap();
}

/// The open's profile holds every statement it traced: a migration's, which
/// runs under a profile of its own, and those after it.
#[tokio::test]
async fn a_profile_inside_another_leaves_the_outer_tracing() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    let statements: Vec<String> = db
        .inspect_writer(|writer| writer.profiled_statements())
        .into_iter()
        .map(|s| s.sql)
        .collect();
    assert!(statements.iter().any(|sql| sql.starts_with("CREATE TABLE notes")), "{statements:#?}");
    assert!(statements.iter().any(|sql| sql == "ANALYZE sqlite_schema"), "{statements:#?}");
    db.close().await.unwrap();
}

/// A profile ends however its run ends, an error or a panic included: what
/// the connection runs afterwards is never traced.
#[tokio::test]
async fn a_profile_ends_on_every_path_out_of_its_run() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    db.inspect_writer(|writer| {
        let failed = writer.with_profile(|writer| -> Result<(), DbError> {
            writer.batch("SELECT 'traced'")?;
            Err(DbError::StoreClosed)
        });
        assert!(failed.is_err());
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.with_profile(|writer| {
                writer.batch("SELECT 'traced too'").unwrap();
                panic!("the run fails");
            })
        }));
        assert!(panicked.is_err());
        writer.batch("SELECT 'after'").unwrap();
        let traced: Vec<String> =
            writer.profiled_statements().into_iter().map(|statement| statement.sql).collect();
        assert_eq!(traced, ["SELECT 'traced too'"]);
    });
    db.close().await.unwrap();
}
