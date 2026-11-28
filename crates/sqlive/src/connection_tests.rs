use crate::tests::{first, next, no_values, QueryError, TestDatabase, TestError, TestOptions};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[test]
fn bundled_sqlite_has_the_session_extension() {
    let db = TestDatabase::open("");
    assert!(db.value::<bool>(
        "SELECT sqlite_compileoption_used('ENABLE_SESSION') AND sqlite_compileoption_used('ENABLE_PREUPDATE_HOOK')"
    ));
    db.close();
}

/// SQLite keeps a statement's journal in memory up to 124 KiB (coven.md §21.2).
#[test]
fn bundled_sqlite_keeps_a_statements_journal_in_memory_up_to_124_kib() {
    let db = TestDatabase::open("");
    assert!(db.value::<bool>("SELECT sqlite_compileoption_used('STMTJRNL_SPILL=126976')"));
    db.close();
}

#[tokio::test]
async fn changeset_failure_rolls_back_and_reaches_the_writer_without_ending_queries() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    let db = TestDatabase::open("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)");
    let mut query =
        db.subscribe_rows("SELECT value FROM items", no_values(), |r| r.get::<_, String>(0));
    assert!(next(&mut query).await.is_empty());
    db.host(|writer| {
        // Fail SQLite's own session lookup while taking its changeset, once
        // the transaction's own statements have run.
        let refusing = Arc::new(AtomicBool::new(false));
        let refuses = Arc::clone(&refusing);
        let mut judge = writer.authorization.callback();
        writer
            .connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if refuses.load(Ordering::Relaxed)
                    && matches!(context.action, AuthAction::Read { table_name: "items", .. })
                {
                    Authorization::Deny
                } else {
                    judge(context)
                }
            }))
            .unwrap();
        let result = writer.transaction(|writer| {
            writer.execute("INSERT INTO items VALUES(1,'rolled back')", [])?;
            refusing.store(true, Ordering::Relaxed);
            Ok::<_, TestError>(())
        });
        writer.connection.authorizer(Some(writer.authorization.callback())).unwrap();
        assert!(matches!(result, Err(crate::Error::Sqlite(rusqlite::Error::SqliteFailure(..)))));
        assert_eq!(
            writer.query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0)).unwrap(),
            0
        );
    });
    assert!(!query.is_marked_for_rerun());
    db.commit("INSERT INTO items VALUES(1,'committed')");
    assert_eq!(next(&mut query).await, ["committed"]);
    db.close();
}

#[tokio::test]
async fn commit_failure_and_net_zero_transactions_do_not_publish() {
    let db = TestDatabase::open("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE items(id INTEGER PRIMARY KEY,parent REFERENCES parent DEFERRABLE INITIALLY DEFERRED,value TEXT)");
    let mut query =
        db.subscribe_rows("SELECT value FROM items", no_values(), |r| r.get::<_, String>(0));
    assert!(next(&mut query).await.is_empty());
    db.host(|writer| {
        assert!(writer
            .transaction(|writer| writer.batch("INSERT INTO items VALUES(1,99,'invalid')"))
            .is_err());
    });
    assert!(!query.is_marked_for_rerun());
    db.commit("INSERT INTO items VALUES(1,NULL,'temporary'); DELETE FROM items");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(query.runs().runs(), 1);
    db.close();
}

#[tokio::test]
async fn every_host_statement_path_is_observed_by_the_transaction() {
    let db = TestDatabase::open("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)");
    let mut query = db
        .subscribe_rows("SELECT value FROM items", no_values(), |r| r.get::<_, String>(0))
        .process(first);
    assert!(query.next_inline().await.expect("the live query answers").is_err());
    let commit = |run: &dyn Fn(&crate::HostSql<crate::NoPolicy>)| {
        db.host(|writer| {
            writer
                .transaction(|writer| {
                    run(writer);
                    Ok::<_, TestError>(())
                })
                .unwrap()
        })
    };
    commit(&|writer| {
        writer.execute("INSERT INTO items VALUES(1,'execute')", []).unwrap();
    });
    assert_eq!(next(&mut query).await, "execute");
    commit(&|writer| {
        writer
            .query_row("UPDATE items SET value='query_row' RETURNING value", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap();
    });
    assert_eq!(next(&mut query).await, "query_row");
    commit(&|writer| {
        writer
            .query("UPDATE items SET value='query' RETURNING value", [], |r| r.get::<_, String>(0))
            .unwrap();
    });
    assert_eq!(next(&mut query).await, "query");
    db.commit("UPDATE items SET value='batch'");
    assert_eq!(next(&mut query).await, "batch");
    db.close();
}

/// A table with no primary key, whichever way SQLite stores its rows, refuses
/// the schema's shapes and so every capture and live read, naming the table.
#[test]
fn a_table_with_no_primary_key_refuses_the_schemas_shapes() {
    for keyless in ["CREATE TABLE unkeyed(value TEXT)", "CREATE TABLE unkeyed(value TEXT) STRICT"] {
        let db = TestDatabase::open(&format!(
            "CREATE TABLE keyed(id TEXT NOT NULL PRIMARY KEY,value TEXT); {keyless}"
        ));
        db.host(|writer| {
            let error = writer.table_shapes().err().unwrap();
            assert!(
                matches!(&error, crate::Error::TableWithoutPrimaryKey { table } if table == "unkeyed"),
                "{error:?}"
            );
        });
        db.close();
    }
}

/// Every kind of primary key keeps its table's shape.
#[test]
fn tables_with_a_primary_key_keep_their_shapes() {
    let db = TestDatabase::open(
        "
        CREATE TABLE keyed(id TEXT NOT NULL PRIMARY KEY,value TEXT);
        CREATE TABLE ordinary(id INTEGER PRIMARY KEY,value TEXT);
        CREATE TABLE composite(a TEXT NOT NULL,b INTEGER NOT NULL,PRIMARY KEY(a,b));
        CREATE TABLE without_rowid(id TEXT PRIMARY KEY,value TEXT) WITHOUT ROWID;
        CREATE TABLE strict_key(id TEXT PRIMARY KEY,value TEXT) STRICT;
        CREATE TABLE descending(id INTEGER NOT NULL PRIMARY KEY DESC,value TEXT);
        CREATE TABLE alias_descending(id INTEGER,value TEXT,PRIMARY KEY(id DESC));
    ",
    );
    db.host(|writer| assert_eq!(writer.table_shapes().unwrap().iter().count(), 7));
    db.close();
}

/// A write that captures changes in a schema with a table that has no
/// primary key fails, naming the table, and commits nothing.
#[test]
fn a_write_refuses_a_schema_with_a_table_that_has_no_primary_key() {
    let db = TestDatabase::open(
        "CREATE TABLE keyed(id TEXT NOT NULL PRIMARY KEY,value TEXT); CREATE TABLE unkeyed(value TEXT)",
    );
    let error = db.write("INSERT INTO keyed VALUES('a','b')").unwrap_err();
    assert!(format!("{error:?}").contains("TableWithoutPrimaryKey"), "{error:?}");
    assert!(format!("{error}").contains("unkeyed"), "{error}");
    assert_eq!(db.value::<i64>("SELECT count(*) FROM keyed"), 0);
    db.close();
}

/// A live query that reads in a schema with a table that has no primary key
/// fails with an error naming the table.
#[tokio::test]
async fn a_live_read_refuses_a_schema_with_a_table_that_has_no_primary_key() {
    let db = TestDatabase::open(
        "CREATE TABLE keyed(id TEXT NOT NULL PRIMARY KEY,value TEXT); CREATE TABLE unkeyed(value TEXT)",
    );
    let mut query =
        db.subscribe_rows("SELECT value FROM keyed", no_values(), |r| r.get::<_, String>(0));
    let error = crate::tests::next_result(&mut query).await.unwrap_err();
    assert!(format!("{error:?}").contains("unkeyed"), "{error:?}");
    db.close();
}

#[tokio::test]
async fn ordinary_tables_get_no_hook_change_and_retain_column_and_key_precision() {
    for (key, suffix, first, second) in [
        ("INTEGER PRIMARY KEY", "", "1", "2"),
        ("TEXT NOT NULL PRIMARY KEY", "", "'a'", "'b'"),
        ("TEXT PRIMARY KEY", " WITHOUT ROWID", "'a'", "'b'"),
        ("TEXT PRIMARY KEY", " STRICT", "'a'", "'b'"),
    ] {
        let db = TestDatabase::open(&format!("CREATE TABLE items(id {key},value INTEGER,extra INTEGER){suffix}; INSERT INTO items VALUES({first},10,0),({second},20,0); CREATE TABLE hook_target(id INTEGER PRIMARY KEY,value TEXT)"));
        let sql = format!("SELECT value FROM items WHERE id={first}");
        let mut query = db
            .subscribe_rows(sql, no_values(), |r| r.get::<_, i64>(0))
            .process(crate::tests::first);
        assert_eq!(next(&mut query).await, 10);
        db.commit(&format!("UPDATE items SET extra=1 WHERE id={first}; UPDATE items SET value=21 WHERE id={second}; INSERT INTO hook_target(value) VALUES('unrelated')"));
        assert!(!query.is_marked_for_rerun(), "{key}{suffix}");
        assert_eq!(query.runs().runs(), 1, "{key}{suffix}");
        db.commit(&format!("UPDATE items SET value=11 WHERE id={first}"));
        assert_eq!(next(&mut query).await, 11);
        db.close();
    }
}

/// A write of two updates and a write of six read the same metadata: no
/// update or trigger it fires reads any, and the commit reads the same
/// metadata once whatever the write did. The first write reads the tables'
/// shapes, once for the schema.
#[test]
fn app_writes_query_no_metadata_for_each_statement_or_trigger() {
    let db = TestDatabase::open("CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,value BLOB); CREATE TABLE audit(id INTEGER PRIMARY KEY,value BLOB); INSERT INTO items VALUES('a',x'01'); CREATE TRIGGER effect AFTER UPDATE ON items BEGIN INSERT INTO audit(value) VALUES(new.value); END");
    let metadata_reads = |updates: u8| {
        db.write_app(|writer| {
            for value in 0..updates {
                writer
                    .app_execute("UPDATE items SET value=?1 WHERE id='a'", [vec![value; 4096]])?;
            }
            Ok(())
        })
        .unwrap();
        db.host(|writer| {
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
    db.close();
}

/// A transaction whose rollback SQLite refuses fails with its own cause and
/// the rollback's beside it, and fails the handle: the live queries end with
/// the failure, and the writer still holds the uncommitted row until the
/// database reopens (SPEC.md section 2).
#[tokio::test]
async fn a_failed_rollback_keeps_both_causes_and_fails_the_handle_until_reopen() {
    for app in [false, true] {
        let db = TestDatabase::open(
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
        );
        let mut notes =
            db.subscribe_rows("SELECT title FROM notes", no_values(), |r| r.get::<_, String>(0));
        assert!(next(&mut notes).await.is_empty());
        db.host(|writer| writer.refuse_transaction_end(true));
        // Each fails after a change that its refused ROLLBACK leaves on the
        // writer.
        let error = if app {
            db.host(|writer| {
                writer.write_with(
                    |writer, _| {
                        let _write = writer.write_scope();
                        writer.app_batch("INSERT INTO notes VALUES('n','held')")?;
                        Err::<(), _>(crate::Error::Closed)
                    },
                    |_, _, ()| Ok(()),
                )
            })
        } else {
            db.host(|writer| {
                writer.transaction(|writer| {
                    writer.execute("INSERT INTO notes VALUES('n','held')", [])?;
                    Err::<(), _>(crate::Error::Closed)
                })
            })
        }
        .unwrap_err();
        assert!(
            matches!(&error, crate::Error::Rollback { operation, .. } if matches!(**operation, crate::Error::Closed)),
            "{error:?}"
        );
        let held: i64 = db.value("SELECT count(*) FROM notes");
        assert_eq!(held, 1, "the writer still holds the uncommitted row");
        let failed = db.handle().error::<Infallible>().expect("the handle failed");
        assert!(
            matches!(&failed, crate::Error::Host(handle) if matches!(handle.engine(), crate::EngineFailure::Rollback)),
            "{failed:?}"
        );
        assert!(matches!(
            notes.next_inline().await.expect("the live query answers"),
            Err(QueryError(crate::Error::Host(_)))
        ));
        let db = db.reopen(TestOptions::default());
        let held: i64 = db.value("SELECT count(*) FROM notes");
        assert_eq!(held, 0);
        db.close();
    }
}

/// A profile inside another, as a host's step inside the open's, leaves the
/// outer one tracing: the open's profile holds every statement it traced,
/// the inner profile's and those after it.
#[test]
fn a_profile_inside_another_leaves_the_outer_tracing() {
    let directory = tempfile::tempdir().unwrap();
    let bugs: panic_as_error::BugSinkRef = Arc::new(crate::tests::HeardBugs::default());
    let (database, ()) = crate::Database::open_with(
        &directory.path().join("profiled.sqlite"),
        crate::Handle::recording_all(bugs),
        crate::Options {
            settings: crate::SqliteSettings::default(),
            readers: std::num::NonZeroUsize::MIN,
            prepare_on_reader: false,
            policy: &|| crate::NoPolicy,
            setup: &|_, _| Ok(()),
            capture: None,
            checkpoint_pages: None,
            statistics: true,
        },
        |writer| {
            writer.with_profile(|writer| {
                writer
                    .batch("CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)")
            })
        },
    )
    .unwrap();
    let statements: Vec<String> =
        database.host(|writer| writer.profiled_statements()).into_iter().map(|s| s.sql).collect();
    assert!(statements.iter().any(|sql| sql.starts_with("CREATE TABLE notes")), "{statements:#?}");
    assert!(statements.iter().any(|sql| sql == "ANALYZE sqlite_schema"), "{statements:#?}");
    assert!(database.close().is_empty());
}

/// A profile ends however its run ends, an error or a panic included, so the
/// profile traces nothing the connection runs afterwards.
#[test]
fn a_profile_ends_on_every_path_out_of_its_run() {
    let db = TestDatabase::open("");
    db.host(|writer| {
        let failed = writer.with_profile(|writer| -> Result<(), TestError> {
            writer.batch("SELECT 'traced'")?;
            Err(crate::Error::Closed)
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
    db.close();
}
