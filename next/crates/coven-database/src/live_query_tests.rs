use super::*;
use crate::DbError;
use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> CovenResult<T> {
    tokio::time::timeout(Duration::from_secs(3), query.next())
        .await
        .expect("live query did not answer")
}

async fn next_after_equal_run<T: Clone + PartialEq + Send + std::fmt::Debug + 'static>(
    query: &mut LiveQuery<T>,
    equal_finished: tokio::sync::oneshot::Receiver<()>,
    commit: impl FnOnce(),
) -> CovenResult<T> {
    let answer = next(query);
    tokio::pin!(answer);
    tokio::select! {
        result = &mut answer => panic!("equal result was returned: {result:?}"),
        finished = equal_finished => finished.unwrap(),
    }
    // The equal snapshot was read before this commit; both runs must occur.
    commit();
    answer.await
}

impl<T> LiveQuery<T> {
    pub(crate) fn is_marked_for_rerun(&self) -> bool {
        self.inner.is_marked_for_rerun()
    }
}

impl<Q, T> ReconfigurableLiveQuery<Q, T> {
    pub(crate) fn is_marked_for_rerun(&self) -> bool {
        // The writer publishes commits synchronously. Once a run has installed
        // its dependencies, this flag decides whether next() will rerun it.
        assert!(
            self.current.is_some() && self.running.is_none(),
            "probe requires a completed, idle query"
        );
        self.commits.state().changed
    }
}

#[tokio::test]
async fn relevant_columns_in_any_row_rerun_and_equal_results_are_suppressed() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('a','Plan',''),('b','Other','')")
            .unwrap()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let (equal_run, equal_finished) = tokio::sync::oneshot::channel();
    let equal_run = Mutex::new(Some(equal_run));
    let mut query = db.subscribe(move |sql| {
        let values = sql.query(
            "SELECT id FROM notes WHERE title='Plan' ORDER BY id",
            [],
            |r| r.get::<_, String>(0),
        )?;
        if count.fetch_add(1, Ordering::SeqCst) == 1 {
            equal_run.lock().unwrap().take().unwrap().send(()).unwrap();
        }
        Ok(values)
    });
    assert_eq!(next(&mut query).await.unwrap(), ["a"]);
    db.commit_writer(|sql| sql.batch("UPDATE notes SET body='irrelevant'").unwrap());
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.commit_writer(|sql| {
        sql.batch("UPDATE notes SET title='Other again' WHERE id='b'")
            .unwrap()
    });
    assert!(query.is_marked_for_rerun());
    assert_eq!(
        next_after_equal_run(&mut query, equal_finished, || {
            db.commit_writer(|sql| {
                sql.batch("UPDATE notes SET title='Plan' WHERE id='b'")
                    .unwrap()
            });
        })
        .await
        .unwrap(),
        ["a", "b"]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    db.commit_writer(|sql| sql.batch("UPDATE notes SET title=title").unwrap());
    assert!(!query.is_marked_for_rerun());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('c','Plan','')")
            .unwrap()
    });
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await.unwrap(), ["a", "b", "c"]);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    db.commit_writer(|sql| sql.batch("DELETE FROM notes WHERE id='a'").unwrap());
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await.unwrap(), ["b", "c"]);
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    db.close().await.unwrap();
    assert!(matches!(
        next(&mut query).await,
        Err(crate::CovenError::Database(DbError::StoreClosed))
    ));
}

#[tokio::test]
async fn row_count_ignores_updates_and_rollback_journals_do_not_publish() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let mut query = db.subscribe(move |sql| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(next(&mut query).await.unwrap(), 0);
    db.commit_writer(|sql| sql.batch("INSERT INTO notes VALUES ('a','a',''); SAVEPOINT s; INSERT INTO notes VALUES ('b','b',''); ROLLBACK TO s").unwrap());
    assert_eq!(next(&mut query).await.unwrap(), 1);
    db.inspect_writer(|sql| {
        let error = sql.transaction::<()>(|sql| {
            sql.batch("INSERT INTO notes VALUES ('c','c','')")?;
            Err(DbError::TransactionEnded)
        });
        assert!(matches!(error, Err(DbError::TransactionEnded)));
        assert!(!query.is_marked_for_rerun());
        sql.transaction(|sql| {
            sql.batch("SAVEPOINT s; INSERT INTO notes VALUES ('d','d',''); ROLLBACK TO s")
        })
        .unwrap();
        assert!(!query.is_marked_for_rerun());
        sql.transaction(|sql| sql.batch("UPDATE notes SET title='a2'"))
            .unwrap();
        assert!(!query.is_marked_for_rerun());
        assert!(sql
            .transaction(
                |sql| sql.batch("INSERT INTO notes VALUES ('e','e',''),('a','duplicate','')")
            )
            .is_err());
        assert!(!query.is_marked_for_rerun());
    });
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_between_finished_read_and_wait_is_not_missed() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let calls = AtomicUsize::new(0);
    let query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?)
    });
    let mut query = query.process(move |value| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            entered.send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        Ok(value)
    });
    let task = tokio::spawn(async move {
        let value = next(&mut query).await.unwrap();
        (query, value)
    });
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    // process starts only after the snapshot and pool lease have ended.
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('a','Plan','')")
            .unwrap()
    });
    release.send(()).unwrap();
    let (mut query, value) = task.await.unwrap();
    assert_eq!(value, 0);
    assert!(query.is_marked_for_rerun());
    assert_eq!(next(&mut query).await.unwrap(), 1);
    db.close().await.unwrap();
}

#[tokio::test]
async fn errors_are_results_and_a_later_commit_can_recover() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let mut query = db.subscribe(|sql| {
        Ok(
            sql.query_row("SELECT title FROM notes ORDER BY id LIMIT 1", [], |r| {
                r.get::<_, String>(0)
            })?,
        )
    });
    assert!(matches!(
        next(&mut query).await,
        Err(crate::CovenError::Database(DbError::Sqlite(
            rusqlite::Error::QueryReturnedNoRows
        )))
    ));
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('a','Plan','')")
            .unwrap()
    });
    assert_eq!(next(&mut query).await.unwrap(), "Plan");
    db.commit_writer(|sql| sql.batch("DELETE FROM notes").unwrap());
    assert!(next(&mut query).await.is_err());
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('a','Plan','')")
            .unwrap()
    });
    assert_eq!(next(&mut query).await.unwrap(), "Plan");
    db.close().await.unwrap();
}

#[tokio::test]
async fn requests_retain_revisions_causes_and_dynamic_dependencies() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('a','same','same')")
            .unwrap()
    });
    let mut query = db.subscribe_reconfigurable(false, |body, sql| {
        Ok(sql.query_row(
            if *body {
                "SELECT body FROM notes"
            } else {
                "SELECT title FROM notes"
            },
            [],
            |r| r.get::<_, String>(0),
        )?)
    });
    let requests = query.requests();
    let first = query.next().await;
    assert_eq!(
        (
            first.request,
            first.revision,
            first.cause,
            first.result.unwrap()
        ),
        (
            false,
            LiveQueryRevision(0),
            LiveQueryCause::Request,
            "same".into()
        )
    );
    assert_eq!(requests.set(false).unwrap(), LiveQueryRevision(0));
    assert_eq!(requests.set(true).unwrap(), LiveQueryRevision(1));
    let changed_request = query.next().await;
    assert_eq!(
        (
            changed_request.request,
            changed_request.revision,
            changed_request.cause,
            changed_request.result.unwrap()
        ),
        (
            true,
            LiveQueryRevision(1),
            LiveQueryCause::Request,
            "same".into()
        )
    );
    db.commit_writer(|sql| sql.batch("UPDATE notes SET title='ignored'").unwrap());
    assert!(!query.is_marked_for_rerun());
    db.commit_writer(|sql| sql.batch("UPDATE notes SET body='new'").unwrap());
    assert!(query.is_marked_for_rerun());
    assert_eq!(requests.set(false).unwrap(), LiveQueryRevision(2));
    assert_eq!(requests.clone().set(true).unwrap(), LiveQueryRevision(3));
    assert_eq!(requests.set(true).unwrap(), LiveQueryRevision(3));
    let both = query.next().await;
    assert_eq!(
        (
            both.request,
            both.revision,
            both.cause,
            both.result.unwrap()
        ),
        (
            true,
            LiveQueryRevision(3),
            LiveQueryCause::RequestAndWrite,
            "new".into()
        )
    );
    assert!(!query.is_marked_for_rerun());
    assert!(query.lifetime.0.requests.lock().unwrap().pending.is_none());
    drop(query);
    assert_eq!(requests.set(false), Err(LiveQueryClosed));
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_next_keeps_its_request_and_inflight_result() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let mut query = db.subscribe_reconfigurable(5, move |request, sql| {
        if let Some(entered) = entered.lock().unwrap().take() {
            entered.send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
        Ok(sql.query_row("SELECT ?1", [request], |r| r.get::<_, i32>(0))?)
    });
    let requests = query.requests();
    tokio::select! { _ = entry => {}, _ = query.next() => panic!("run should be blocked"), }
    assert_eq!(requests.set(6).unwrap(), LiveQueryRevision(1));
    assert_eq!(requests.set(7).unwrap(), LiveQueryRevision(2));
    release.send(()).unwrap();
    let first = query.next().await;
    assert_eq!(
        (first.request, first.revision, first.result.unwrap()),
        (5, LiveQueryRevision(0), 5)
    );
    let second = query.next().await;
    assert_eq!(
        (second.request, second.revision, second.result.unwrap()),
        (7, LiveQueryRevision(2), 7)
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn process_compares_processed_results() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('a','Plan','')")
            .unwrap()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let (equal_run, equal_finished) = tokio::sync::oneshot::channel();
    let equal_run = Mutex::new(Some(equal_run));
    let mut query = db
        .subscribe(|sql| {
            Ok(sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?)
        })
        .process(move |value| {
            if count.fetch_add(1, Ordering::SeqCst) == 1 {
                equal_run.lock().unwrap().take().unwrap().send(()).unwrap();
            }
            Ok(value.len())
        });
    assert_eq!(next(&mut query).await.unwrap(), 4);
    db.commit_writer(|sql| sql.batch("UPDATE notes SET title='Test'").unwrap());
    assert!(query.is_marked_for_rerun());
    assert_eq!(
        next_after_equal_run(&mut query, equal_finished, || {
            db.commit_writer(|sql| sql.batch("UPDATE notes SET title='Changed'").unwrap());
        })
        .await
        .unwrap(),
        7
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(!query.is_marked_for_rerun());
    db.close().await.unwrap();
}

#[tokio::test]
async fn joins_views_generated_columns_collations_and_local_tables_are_observed() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE local(value TEXT COLLATE NOCASE, extra INTEGER, upper_value TEXT AS (upper(value))); CREATE TABLE keyed(id TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID; CREATE VIEW joined AS SELECT local.value, local.upper_value, keyed.value AS other FROM local JOIN keyed ON local.value=keyed.id").await.unwrap();
    db.commit_writer(|sql| {
        sql.batch(
            "INSERT INTO local(value,extra) VALUES ('a',0); INSERT INTO keyed VALUES ('a',x'01')",
        )
        .unwrap()
    });
    let mut query = db.subscribe(|sql| {
        Ok(
            sql.query("SELECT value,upper_value,other FROM joined", [], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })?,
        )
    });
    assert_eq!(
        next(&mut query).await.unwrap(),
        [("a".into(), "A".into(), vec![1])]
    );
    db.commit_writer(|sql| sql.batch("UPDATE local SET value='A'").unwrap());
    assert_eq!(
        next(&mut query).await.unwrap(),
        [("A".into(), "A".into(), vec![1])]
    );
    db.commit_writer(|sql| sql.batch("UPDATE keyed SET value=x'02'").unwrap());
    assert_eq!(
        next(&mut query).await.unwrap(),
        [("A".into(), "A".into(), vec![2])]
    );
    let mut generated = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT upper_value FROM local", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert_eq!(next(&mut generated).await.unwrap(), "A");
    db.commit_writer(|sql| sql.batch("UPDATE local SET value='b'").unwrap());
    assert_eq!(next(&mut generated).await.unwrap(), "B");
    assert!(next(&mut query).await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn triggers_and_foreign_key_cascades_publish_their_changes() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(parent REFERENCES parent ON DELETE CASCADE, title); CREATE TABLE audit(value); CREATE TRIGGER record_change AFTER UPDATE OF title ON child BEGIN INSERT INTO audit VALUES (new.title); END;").await.unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO parent VALUES (1); INSERT INTO child VALUES (1,'one')")
            .unwrap()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let (equal_run, equal_finished) = tokio::sync::oneshot::channel();
    let equal_run = Mutex::new(Some(equal_run));
    let mut children = db.subscribe(move |sql| {
        let value = sql.query_row("SELECT count(*) FROM child", [], |r| r.get::<_, i64>(0))?;
        if count.fetch_add(1, Ordering::SeqCst) == 1 {
            equal_run.lock().unwrap().take().unwrap().send(()).unwrap();
        }
        Ok(value)
    });
    let mut audit = db.subscribe(|sql| {
        Ok(
            sql.query("SELECT value FROM audit ORDER BY rowid", [], |r| {
                r.get::<_, String>(0)
            })?,
        )
    });
    assert_eq!(next(&mut children).await.unwrap(), 1);
    assert!(next(&mut audit).await.unwrap().is_empty());
    db.inspect_writer(|sql| {
        sql.transaction(|sql| {
            sql.app_execute("UPDATE child SET title='two'", [])?;
            Ok(())
        })
        .unwrap()
    });
    // The unkeyed child's update hook invalidates the count, which stays equal.
    assert!(children.is_marked_for_rerun());
    assert!(audit.is_marked_for_rerun());
    assert_eq!(next(&mut audit).await.unwrap(), ["two"]);
    assert_eq!(
        next_after_equal_run(&mut children, equal_finished, || {
            db.commit_writer(|sql| sql.batch("DELETE FROM parent").unwrap());
        })
        .await
        .unwrap(),
        0
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(!children.is_marked_for_rerun());
    db.close().await.unwrap();
}

#[tokio::test]
async fn virtual_table_queries_observe_updates() {
    let store = TestStore::new();
    let db = store
        .schema(vec![], "CREATE VIRTUAL TABLE search USING fts5(title,body)")
        .await
        .unwrap();
    let mut query = db.subscribe(|sql| {
        Ok(sql.query(
            "SELECT title FROM search WHERE search MATCH 'plan'",
            [],
            |r| r.get::<_, String>(0),
        )?)
    });
    assert!(next(&mut query).await.unwrap().is_empty());
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO search VALUES ('plan','body')")
            .unwrap()
    });
    assert_eq!(next(&mut query).await.unwrap(), ["plan"]);
    db.commit_writer(|sql| sql.batch("UPDATE search SET title='changed'").unwrap());
    assert!(next(&mut query).await.unwrap().is_empty());
    db.close().await.unwrap();
}

#[tokio::test]
async fn replacement_before_first_run_supersedes_the_initial_request() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut query = db.subscribe_reconfigurable(0, |request, _| Ok(*request));
    let requests = query.requests();
    assert_eq!(requests.set(1).unwrap(), LiveQueryRevision(1));
    assert_eq!(requests.set(2).unwrap(), LiveQueryRevision(2));
    let event = query.next().await;
    assert_eq!(
        (
            event.request,
            event.revision,
            event.cause,
            event.result.unwrap()
        ),
        (2, LiveQueryRevision(2), LiveQueryCause::Request, 2)
    );
    assert!(!query.is_marked_for_rerun());
    assert!(query.lifetime.0.requests.lock().unwrap().pending.is_none());
    db.close().await.unwrap();
}

#[tokio::test]
async fn shadow_changes_invalidate_every_virtual_column_without_reading_contents() {
    let store = TestStore::new();
    let db=store.schema(vec![],"CREATE VIRTUAL TABLE search USING fts5(title,body); INSERT INTO search(rowid,title,body) VALUES(1,'title','before')").await.unwrap();
    let runs = AtomicUsize::new(0);
    let mut query = db.subscribe(move |sql| {
        let titles = sql.query("SELECT title FROM search WHERE rowid=1", [], |r| {
            r.get::<_, String>(0)
        })?;
        Ok((runs.fetch_add(1, Ordering::SeqCst), titles))
    });
    assert_eq!(next(&mut query).await.unwrap(), (0, vec!["title".into()]));
    for run in 1..=3 {
        db.commit_writer(|writer| {
            writer
                .internal_execute(
                    "UPDATE search_content SET c1=?1 WHERE id=1",
                    [format!("body {run}")],
                )
                .unwrap();
        });
        // The unchanged title still reruns when another shadow column changes;
        // the result's run count makes that observable without timed polling.
        assert_eq!(next(&mut query).await.unwrap(), (run, vec!["title".into()]));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn hidden_rowid_inserts_are_observed_and_updates_are_refused() {
    let store = TestStore::new();
    let db=store.schema(vec![],"CREATE TABLE items(id TEXT NOT NULL PRIMARY KEY,value TEXT); INSERT INTO items(rowid,id,value) VALUES(7,'a','value')").await.unwrap();
    let mut query = db.subscribe(|sql| {
        Ok(sql.query("SELECT rowid FROM items ORDER BY id", [], |r| {
            r.get::<_, i64>(0)
        })?)
    });
    assert_eq!(next(&mut query).await.unwrap(), [7]);
    for (alias, rowid, id) in [("rowid", 8, "b"), ("_rowid_", 9, "c"), ("oid", 10, "d")] {
        db.write(Default::default(), move |sql| {
            sql.execute(
                &format!("INSERT INTO items({alias},id,value) VALUES(?1,?2,'other')"),
                crate::params![rowid, id],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            next(&mut query).await.unwrap(),
            (7..=rowid).collect::<Vec<_>>()
        );
    }
    let error = db
        .write(Default::default(), |sql| {
            sql.execute("UPDATE items SET rowid=2 WHERE id='a'", [])?;
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(!query.is_marked_for_rerun());
    assert!(
        matches!(
            error,
            DbError::StatementForbidden {
                operation: "changing a hidden rowid"
            }
        ),
        "{error:?}"
    );
    assert_eq!(
        db.read(|sql| Ok(
            sql.query_row("SELECT rowid FROM items WHERE id='a'", [], |r| r
                .get::<_, i64>(0))?
        ))
        .await
        .unwrap(),
        7
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn changing_an_integer_primary_key_through_rowid_is_observed() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO items(rowid,value) VALUES(1,'value')",
        )
        .await
        .unwrap();
    let mut query = db
        .subscribe(|sql| Ok(sql.query_row("SELECT rowid FROM items", [], |r| r.get::<_, i64>(0))?));
    assert_eq!(next(&mut query).await.unwrap(), 1);
    db.write(Default::default(), |sql| {
        sql.execute("UPDATE items SET rowid=2", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(next(&mut query).await.unwrap(), 2);
    db.close().await.unwrap();
}
