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

#[tokio::test]
async fn reopen_after_drop_waits_only_for_a_subscriptions_active_read() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let store = TestStore::new();
    let database = store.builder(vec![], vec![]).open().await.unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let mut query = database.subscribe(move |_| {
        entered.lock().unwrap().take().unwrap().send(()).unwrap();
        released
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        Ok(17)
    });
    tokio::select! {
        result = query.next() => panic!("held query returned: {result:?}"),
        result = entry => result.unwrap(),
    }
    drop(database);
    let mut reopen = Box::pin(store.builder(vec![], vec![]).open());
    let waiting = poll_fn(|cx| Poll::Ready(reopen.as_mut().poll(cx).is_pending())).await;
    release.send(()).unwrap();
    assert!(
        waiting,
        "a subscription must not keep its dropped instance live"
    );
    let reopened = reopen.await.unwrap();
    assert_eq!(
        query.next().await.expect("the live query answers").unwrap(),
        17
    );
    assert!(matches!(
        query.next().await.expect("the live query answers"),
        Err(crate::CovenError::Database(DbError::StoreClosed))
    ));
    reopened.close().await.unwrap();
}

async fn next<T: Clone + PartialEq + Send + 'static>(query: &mut LiveQuery<T>) -> CovenResult<T> {
    tokio::time::timeout(Duration::from_secs(3), query.next())
        .await
        .expect("live query did not answer")
        .expect("the live query answers")
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
            "SELECT id FROM notes WHERE title LIKE 'Plan' ORDER BY id",
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
    let first = query.next().await.expect("the live query answers");
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
    let changed_request = query.next().await.expect("the live query answers");
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
    let both = query.next().await.expect("the live query answers");
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
    let mut query = query.process(|value| Ok(format!("value {value}")));
    release.send(()).unwrap();
    let first = query.next().await.expect("the live query answers");
    assert_eq!(
        (first.request, first.revision, first.result.unwrap()),
        (5, LiveQueryRevision(0), "value 5".into())
    );
    let second = query.next().await.expect("the live query answers");
    assert_eq!(
        (second.request, second.revision, second.result.unwrap()),
        (7, LiveQueryRevision(2), "value 7".into())
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
    let event = query.next().await.expect("the live query answers");
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
        db.write(move |sql| {
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
        .write(|sql| {
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
    db.write(|sql| {
        sql.execute("UPDATE items SET rowid=2", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(next(&mut query).await.unwrap(), 2);
    db.close().await.unwrap();
}

#[tokio::test]
async fn processing_a_completed_query_preserves_a_newer_pending_request() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut query = db.subscribe_reconfigurable(1, |request, _| Ok(*request));
    let requests = query.requests();
    assert_eq!(
        query
            .next()
            .await
            .expect("the live query answers")
            .result
            .unwrap(),
        1
    );
    let revision = requests.set(2).unwrap();
    let mut query = query.process(|value| Ok(value.to_string()));
    let event = query.next().await.expect("the live query answers");
    assert_eq!(
        (
            event.request,
            event.revision,
            event.cause,
            event.result.unwrap()
        ),
        (2, revision, LiveQueryCause::Request, "2".into())
    );
    // With no pending replacement, changing output type answers the current request.
    let mut query = query.process(|value| Ok(value.len()));
    let event = query.next().await.expect("the live query answers");
    assert_eq!(
        (event.request, event.revision, event.result.unwrap()),
        (2, revision, 1)
    );
    drop(query);
    assert_eq!(requests.set(3), Err(LiveQueryClosed));
    db.close().await.unwrap();
}

#[tokio::test]
async fn processing_preserves_query_errors_and_recovers_from_callback_errors() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query = db
        .subscribe_reconfigurable(0, |request, sql| {
            Ok(sql.query_row("SELECT ?1 WHERE ?1 != 0", [request], |row| {
                row.get::<_, i32>(0)
            })?)
        })
        .process(move |value| {
            count.fetch_add(1, Ordering::SeqCst);
            if value == 1 {
                return Err(DbError::TransactionEnded.into());
            }
            Ok(value.to_string())
        });
    let requests = query.requests();
    let event = query.next().await.expect("the live query answers");
    assert!(matches!(
        event.result,
        Err(crate::CovenError::Database(DbError::Sqlite(
            rusqlite::Error::QueryReturnedNoRows
        )))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for request in [1, 2, 1, 2] {
        let revision = requests.set(request).unwrap();
        let event = query.next().await.expect("the live query answers");
        assert_eq!(
            (event.request, event.revision, event.cause),
            (request, revision, LiveQueryCause::Request)
        );
        if request == 1 {
            assert!(matches!(
                event.result,
                Err(crate::CovenError::Database(DbError::TransactionEnded))
            ));
        } else {
            assert_eq!(event.result.unwrap(), "2");
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconfigurable_processing_releases_the_connection_before_its_worker_runs() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let mut query = db
        .subscribe_reconfigurable(7, |request, sql| {
            Ok(sql.query_row("SELECT ?1", [request], |row| row.get::<_, i32>(0))?)
        })
        .process(move |value| {
            entered.lock().unwrap().take().unwrap().send(()).unwrap();
            released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            Ok(value.to_string())
        });
    tokio::select! {
        result = entry => result.unwrap(),
        _ = query.next() => panic!("processing is held"),
    }
    // Closing waits for active reads, so this would block if processing held its lease.
    tokio::time::timeout(Duration::from_secs(5), db.close())
        .await
        .unwrap()
        .unwrap();
    release.send(()).unwrap();
    let event = query.next().await.expect("the live query answers");
    assert_eq!(
        (event.request, event.revision, event.result.unwrap()),
        (7, LiveQueryRevision(0), "7".into())
    );
}

/// A synced table whose local trigger keeps a device-local FTS5 search index:
/// FTS5 reads `PRAGMA data_version` as it writes its shadow tables.
const SEARCHED_NOTES: &str =
    "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
    CREATE VIRTUAL TABLE note_search USING fts5(title);
    CREATE TRIGGER notes_searched AFTER INSERT ON notes BEGIN
      INSERT INTO note_search(rowid,title) VALUES((SELECT count(*) FROM note_search)+1,NEW.title);
    END;
    CREATE TRIGGER notes_retitled AFTER UPDATE OF title ON notes BEGIN
      UPDATE note_search SET title=NEW.title WHERE title=OLD.title;
    END";

/// An app write that a local trigger carries into an FTS5 index commits, as
/// does another device's write, and a live query over the index reruns on
/// both (§9.4, §4.1).
#[tokio::test]
async fn an_apps_write_keeps_a_local_fts5_index_and_its_live_query() {
    let store = TestStore::new();
    let db = store
        .builder(
            vec![crate::SyncedTable::new(
                "notes",
                crate::RowIdentity::SharedKey,
            )],
            vec![crate::Migration::addition(1, "notes", SEARCHED_NOTES)],
        )
        .open()
        .await
        .unwrap();
    let mut search = db.subscribe(|sql| {
        Ok(sql.query(
            "SELECT title FROM note_search WHERE note_search MATCH 'apples' ORDER BY title",
            [],
            |r| r.get::<_, String>(0),
        )?)
    });
    assert!(next(&mut search).await.unwrap().is_empty());
    crate::write::tests::sql(&db, "INSERT INTO notes VALUES ('1','green apples')")
        .await
        .unwrap();
    assert_eq!(next(&mut search).await.unwrap(), ["green apples"]);
    // Another device's write fires the local trigger as it applies.
    crate::tests::remote_update(
        &db,
        "notes",
        "1",
        &[(
            "title",
            coven_format::value::Value::Text("red apples".into()),
        )],
    )
    .await;
    assert_eq!(next(&mut search).await.unwrap(), ["red apples"]);
    // The app's own SQL still can't read the version.
    for statement in [
        "PRAGMA data_version",
        "INSERT INTO notes SELECT 'v', data_version FROM pragma_data_version",
    ] {
        match crate::write::tests::sql(&db, statement).await {
            Err(DbError::StatementForbidden { operation }) => assert_eq!(operation, "PRAGMA"),
            result => panic!("{statement}: expected the PRAGMA refusal, received {result:?}"),
        }
    }
    db.close().await.unwrap();
}

fn count_notes(sql: crate::SqlReadContext<'_>) -> CovenResult<i64> {
    Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get(0))?)
}

/// Once the store is closed, a live query says so once and then ends: a loop
/// over it stops instead of receiving the same error forever.
#[tokio::test]
async fn a_closed_stores_query_reports_once_then_ends() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let mut query = db.subscribe(count_notes);
    let mut requested = db.subscribe_reconfigurable(0, |_, sql| count_notes(sql));
    assert_eq!(query.next().await.unwrap().unwrap(), 0);
    assert_eq!(requested.next().await.unwrap().result.unwrap(), 0);
    db.close().await.unwrap();
    assert!(matches!(
        query.next().await,
        Some(Err(crate::CovenError::Database(DbError::StoreClosed)))
    ));
    assert!(query.next().await.is_none());
    assert!(query.next().await.is_none());
    // A new request doesn't revive it either.
    requested.requests().set(1).unwrap();
    assert!(matches!(
        requested.next().await.map(|event| event.result),
        Some(Err(crate::CovenError::Database(DbError::StoreClosed)))
    ));
    assert!(requested.next().await.is_none());
}

/// A handle whose rollback failed: its live queries report that once and end.
#[tokio::test]
async fn a_failed_handles_query_reports_once_then_ends() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let mut query = db.subscribe(count_notes);
    assert_eq!(query.next().await.unwrap().unwrap(), 0);
    db.inspect_writer(|writer| writer.refuse_transaction_end(true));
    assert!(db
        .write(|sql| -> Result<(), DbError> {
            sql.execute("INSERT INTO notes VALUES('n','held','')", [])?;
            Err(DbError::StoreClosed)
        })
        .await
        .is_err());
    assert!(matches!(
        query.next().await,
        Some(Err(crate::CovenError::Database(DbError::RollbackFailed)))
    ));
    assert!(query.next().await.is_none());
    db.close().await.unwrap();
}
