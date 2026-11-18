use crate::tests::{next, QueryError, TestDatabase, TestError, TestOptions};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn bundled_sqlite_has_the_session_extension() {
    let db = TestDatabase::open("");
    assert!(db.value::<bool>(
        "SELECT sqlite_compileoption_used('ENABLE_SESSION') AND sqlite_compileoption_used('ENABLE_PREUPDATE_HOOK')"
    ));
    db.close();
}

/// SQLite keeps a statement's journal in memory up to 124 KiB (§21.2).
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
    let mut query = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM items", [], |r| r.get::<_, String>(0))?));
    assert!(next(&mut query).await.is_empty());
    db.host(|writer| {
        // Fail SQLite's own session lookup while taking its changeset, once
        // the transaction's own statements have run.
        let refusing = Arc::new(AtomicBool::new(false));
        let refuses = Arc::clone(&refusing);
        writer
            .connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if refuses.load(Ordering::Relaxed)
                    && matches!(context.action, AuthAction::Read { table_name: "items", .. })
                {
                    Authorization::Deny
                } else {
                    Authorization::Allow
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
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let mut query = db.subscribe(move |sql| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(sql.query("SELECT value FROM items", [], |r| r.get::<_, String>(0))?)
    });
    assert!(next(&mut query).await.is_empty());
    db.host(|writer| {
        assert!(writer
            .transaction(|writer| writer.batch("INSERT INTO items VALUES(1,99,'invalid')"))
            .is_err());
    });
    assert!(!query.is_marked_for_rerun());
    db.commit("INSERT INTO items VALUES(1,NULL,'temporary'); DELETE FROM items");
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.close();
}

#[tokio::test]
async fn every_host_statement_path_is_observed_by_the_transaction() {
    let db = TestDatabase::open("CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT)");
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT value FROM items", [], |r| r.get::<_, String>(0))?)
    });
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

/// Beside the session, an update hook watches the schema's tables that have
/// no primary key.
#[test]
fn hook_tables_are_the_schemas_tables_with_no_primary_key() {
    let db = TestDatabase::open(
        "
        CREATE TABLE keyed(id TEXT NOT NULL PRIMARY KEY,value TEXT);
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
    );
    db.host(|writer| {
        let shapes = writer.table_shapes().unwrap();
        let mut whole: Vec<&str> = shapes
            .iter()
            .filter(|(_, shape)| shape.whole)
            .map(|(_, shape)| shape.name.as_str())
            .collect();
        whole.sort_unstable();
        assert_eq!(whole, ["sqliteish", "unkeyed"]);
    });
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
        let db = TestDatabase::open(&format!("CREATE TABLE items(id {key},value INTEGER,extra INTEGER){suffix}; INSERT INTO items VALUES({first},10,0),({second},20,0); CREATE TABLE hook_target(value TEXT)"));
        let runs = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&runs);
        let query = format!("SELECT value FROM items WHERE id={first}");
        let mut query = db.subscribe(move |sql| {
            let value = sql.query_row(&query, [], |r| r.get::<_, i64>(0))?;
            Ok((count.fetch_add(1, Ordering::SeqCst), value))
        });
        assert_eq!(next(&mut query).await, (0, 10));
        db.commit(&format!("UPDATE items SET extra=1 WHERE id={first}; UPDATE items SET value=21 WHERE id={second}; INSERT INTO hook_target VALUES('unrelated')"));
        assert!(!query.is_marked_for_rerun(), "{key}{suffix}");
        assert_eq!(runs.load(Ordering::SeqCst), 1, "{key}{suffix}");
        db.commit(&format!("UPDATE items SET value=11 WHERE id={first}"));
        assert_eq!(next(&mut query).await, (1, 11));
        db.close();
    }
}

#[tokio::test]
async fn hook_changes_publish_after_commit_and_are_discarded_on_transaction_failure() {
    let db = TestDatabase::open("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE local(id TEXT,value TEXT,extra INTEGER,parent REFERENCES parent DEFERRABLE INITIALLY DEFERRED); INSERT INTO local VALUES(NULL,'unchanged',0,NULL)");
    let count = AtomicUsize::new(0);
    let mut query = db.subscribe(move |sql| {
        let values =
            sql.query("SELECT value FROM local WHERE id IS NULL", [], |r| r.get::<_, String>(0))?;
        Ok((count.fetch_add(1, Ordering::SeqCst), values))
    });
    assert_eq!(next(&mut query).await, (0, vec!["unchanged".into()]));
    db.host(|writer| {
        let result = writer.transaction::<(), TestError>(|writer| {
            writer.batch("UPDATE local SET extra=1")?;
            Err(crate::Error::TransactionEnded)
        });
        assert!(matches!(result, Err(crate::Error::TransactionEnded)));
        assert!(!query.is_marked_for_rerun());
        assert!(writer.transaction(|writer| writer.batch("UPDATE local SET parent=99")).is_err());
        assert!(!query.is_marked_for_rerun());
        // A later session-only commit must not publish stale hook changes.
        writer.transaction(|writer| writer.batch("INSERT INTO parent VALUES(1)")).unwrap();
    });
    assert!(!query.is_marked_for_rerun());
    db.commit("UPDATE local SET extra=2");
    // Hook changes invalidate every column even when the selected value stayed equal.
    assert_eq!(next(&mut query).await, (1, vec!["unchanged".into()]));
    db.commit("DELETE FROM local");
    assert_eq!(next(&mut query).await, (2, vec![]));
    db.close();
}

/// A write of two updates and a write of six read the same metadata: no
/// update or trigger it fires reads any, and the commit reads the same
/// metadata once whatever the write did. The first write reads the tables'
/// shapes, once for the schema.
#[test]
fn app_writes_query_no_metadata_for_each_statement_or_trigger() {
    let db = TestDatabase::open("CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,value BLOB); CREATE TABLE audit(value BLOB); INSERT INTO items VALUES('a',x'01'); CREATE TRIGGER effect AFTER UPDATE ON items BEGIN INSERT INTO audit VALUES(new.value); END");
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
/// database reopens (§4).
#[tokio::test]
async fn a_failed_rollback_keeps_both_causes_and_fails_the_handle_until_reopen() {
    for app in [false, true] {
        let db = TestDatabase::open(
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL)",
        );
        let mut notes = db.subscribe(|sql| {
            Ok(sql.query("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
        });
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
            whole_views: Arc::new(crate::WholeViews::new([])),
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
