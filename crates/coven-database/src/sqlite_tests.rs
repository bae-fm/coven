use super::*;
use crate::read::tests::AppError;
use crate::tests::next;
use crate::write::tests::{count, records, sql};
use crate::{internal_schema, CovenSchemaError, DbError, Migration, RowIdentity, SyncedTable};
use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
};
use coven_foundation::id_source::CircleId;
use sqlive::rusqlite::Connection;

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
    raw.execute_batch(
        "CREATE TABLE local(value TEXT NOT NULL PRIMARY KEY); INSERT INTO local VALUES('kept')",
    )
    .unwrap();
    drop(raw);
    for _ in 0..2 {
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        db.inspect_writer(|sql, _| {
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
async fn creating_the_internal_schema_rolls_back_on_failure() {
    let store = TestStore::new();
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch(
        "CREATE TABLE coven_files(sentinel TEXT); INSERT INTO coven_files VALUES('kept')",
    )
    .unwrap();
    drop(raw);
    let error = store.builder(vec![], vec![]).open().await.err().unwrap();
    assert!(matches!(error, crate::OpenError::CovenSchema(CovenSchemaError::Failed { .. })));
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
async fn read_only_refuses_a_database_without_coven_tables_and_changes_nothing() {
    let store = TestStore::new();
    let raw = Connection::open(store.database_path()).unwrap();
    raw.execute_batch(
        "CREATE TABLE local(value TEXT NOT NULL PRIMARY KEY); INSERT INTO local VALUES('kept')",
    )
    .unwrap();
    drop(raw);
    let before = std::fs::read(store.database_path()).unwrap();
    assert!(matches!(
        store.builder(vec![], vec![]).open_read_only().await.err().unwrap(),
        crate::OpenError::CovenSchema(CovenSchemaError::ReadOnly)
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
                Err(crate::OpenError::CovenSchema(CovenSchemaError::SchemaTooNew {
                    current,
                    supported,
                })) if current == version && supported == internal_schema::VERSION
            ));
            assert_eq!(std::fs::read(store.database_path()).unwrap(), before);
        }
    }
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

async fn reference_cycle(store: &TestStore, reference: &str) -> crate::StoreDatabase {
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
                db.inspect_writer(|writer, _| {
                    writer
                        .transaction(|writer| {
                            writer.batch("PRAGMA defer_foreign_keys=ON")?;
                            assert!(writer
                                .query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0))?);
                            assert!(writer
                                .query_row("PRAGMA defer_foreign_keys", [], |r| r
                                    .get::<_, bool>(0))?);
                            writer.execute("DELETE FROM nodes WHERE id=?1", [first])?;
                            writer.execute("DELETE FROM nodes WHERE id=?1", [second])?;
                            Ok::<_, DbError>(())
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
            db.inspect_writer(|db, _| {
                assert!(db.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, bool>(0)).unwrap());
                assert!(db.query("PRAGMA foreign_key_check", [], |_| Ok(())).unwrap().is_empty());
            });
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn restoration_triggers_cannot_leave_invalid_local_references() {
    for shape in [
        "id INT NOT NULL PRIMARY KEY",
        "id INT NOT NULL PRIMARY KEY,rowid TEXT,oid TEXT,_rowid_ TEXT",
        "rowid TEXT,oid TEXT,_rowid_ TEXT,id INT NOT NULL PRIMARY KEY",
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
    let db=store.schema(vec![SyncedTable::new("notes",RowIdentity::SharedKey)],"CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,title TEXT UNIQUE); CREATE TABLE folders(id TEXT NOT NULL PRIMARY KEY); CREATE TABLE links(id INTEGER PRIMARY KEY, folder TEXT REFERENCES folders(id)); CREATE TRIGGER restore AFTER INSERT ON notes WHEN coven_applying() BEGIN DELETE FROM folders WHERE id='Work'; END").await.unwrap();
    sql(&db,"INSERT INTO folders VALUES('Work'); INSERT INTO links VALUES(NULL,'Work'); INSERT INTO notes VALUES('45','Groceries'),('46','Shopping')").await.unwrap();
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

fn materialize_cycle(db: &crate::StoreDatabase) -> Result<(), DbError> {
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
        db.inspect_writer_schema(|writer, _,schema| {
            // The profile holds the update and its recording too, which run
            // none of the statements counted below.
            let update = if moving { "UPDATE albums SET audience='00000000-0000-4000-8000-00000000000a' WHERE id='00000000-0000-4000-8000-000000000001'" } else { "UPDATE albums SET title='After' WHERE id='00000000-0000-4000-8000-000000000001'" };
            let mut changes = None;
            // The profile holds the update and its recording too, which run
            // none of the statements counted below. The write then rolls
            // back, keeping the album as it was.
            let rolled_back = writer.with_profile(|writer| {
                writer.write_with(
                    |db, _| {
                        db.execute(update, [])?;
                        Ok(())
                    },
                    |writer, events, ()| {
                        let captured = crate::write_capture::capture(schema, events).unwrap();
                        changes = Some(crate::store_call::outside_store(|call| {
                            let before=crate::write_rows::AppView::before(writer,schema,&captured).unwrap();
                            let after=crate::write_rows::AppView::after(writer,schema);
                            let stored=crate::merge_store::MergeStore::new(writer, call,&before);
                            crate::write_record::changes(writer, call,schema,&before,&after,&stored,&before.changes(&after, captured.keys().cloned()).unwrap(),&Default::default())
                        }));
                        Err::<(), _>(DbError::TransactionEnded)
                    },
                )
            });
            assert!(matches!(rolled_back, Err(DbError::TransactionEnded)));
            let changes = changes.expect("the host phase ran");
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
async fn observation_and_local_write_sessions_cover_app_and_materialized_changes() {
    let store = TestStore::new();
    let db=store.schema(vec![SyncedTable::new("roots",RowIdentity::SharedKey),SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience")],"CREATE TABLE roots(id TEXT NOT NULL PRIMARY KEY,value TEXT); CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL,title TEXT,root TEXT REFERENCES roots(id)); CREATE INDEX notes_root ON notes(root); CREATE TABLE audit(id INTEGER PRIMARY KEY, value TEXT); CREATE TRIGGER gone AFTER DELETE ON notes BEGIN INSERT INTO audit VALUES(NULL,old.title); END").await.unwrap();
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
async fn rowid_authorization_does_not_query_metadata_for_each_write_or_trigger() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,value BLOB); CREATE TABLE audit(id INTEGER PRIMARY KEY, value BLOB); INSERT INTO items VALUES('a',x'01'); CREATE TRIGGER effect AFTER UPDATE ON items BEGIN INSERT INTO audit VALUES(NULL,new.value); END").await.unwrap();
    // The metadata reads of a write of two updates and one of six: each
    // update and the trigger it fires read none, so the two are the same,
    // whatever the commit reads once. A first write reads the tables'
    // shapes, once for the schema.
    let metadata_reads = |updates: u8| {
        db.inspect_writer(|writer, _| {
            writer
                .with_profile(|writer| {
                    writer.transaction(|writer| {
                        for value in 0..updates {
                            writer.app_execute(
                                "UPDATE items SET value=?1 WHERE id='a'",
                                [vec![value; 4096]],
                            )?;
                        }
                        Ok::<_, DbError>(())
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
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY,audience TEXT NOT NULL); CREATE TABLE pins(id INTEGER PRIMARY KEY, note TEXT REFERENCES notes(id) ON DELETE RESTRICT)",
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
        db.inspect_writer(|writer, _| writer.refuse_transaction_end(true));
        // Neither transaction changes pending state; each fails after a change
        // that its refused ROLLBACK leaves on the writer.
        let error = if internal {
            db.inspect_writer(|writer, _| {
                writer.transaction(|writer| {
                    writer.execute("INSERT INTO notes VALUES('n','held','')", [])?;
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
        let held = |db: &crate::StoreDatabase| {
            db.inspect_writer(|writer, _| {
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
            notes.next_inline().await.expect("the live query answers").unwrap_err(),
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
    db.inspect_writer(|writer, _| writer.refuse_transaction_end(true));
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

/// A sum past `u64::MAX` carries the running total and the term that did not
/// fit; a sum that fits is the sum.
#[test]
fn add_total_names_the_terms_that_overflow() {
    assert_eq!(add_total("bytes", u64::MAX - 3, 3).unwrap(), u64::MAX);
    let error = add_total("bytes", u64::MAX - 3, 4).unwrap_err();
    assert!(matches!(
        error,
        DbError::TotalTooLarge { field: "bytes", total, addend: 4 } if total == u64::MAX - 3
    ));
}
