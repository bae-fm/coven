use crate::{
    Coven, CovenError, DbError, Migration, ReconfigurableLiveQueryCause, RowIdentity, StoreDir,
    SyncedTable,
};
use coven_foundation::config::Config;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::time::Duration;

const NOTE_ONE: &str = "018f4e91-bb24-7ed6-a9be-6b8a4c248551";
const NOTE_TWO: &str = "018f4e91-bb24-7ed6-a9be-6b8a4c248552";

#[derive(Clone)]
struct SnapshotPause {
    first_read: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl SnapshotPause {
    fn new() -> Self {
        Self {
            first_read: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn pause_after_first_read(&self) {
        self.first_read.notify_one();
        let (released, wake) = &*self.release;
        let mut released = released.lock().expect("snapshot release mutex poisoned");
        while !*released {
            released = wake
                .wait(released)
                .expect("snapshot release mutex poisoned");
        }
    }

    fn release(&self) {
        let (released, wake) = &*self.release;
        *released.lock().expect("snapshot release mutex poisoned") = true;
        wake.notify_one();
    }
}

fn read_note_around_pause(
    sql: crate::SqlReadContext<'_>,
    pause: &SnapshotPause,
) -> crate::CovenResult<(String, String)> {
    let before = sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
        row.get(0)
    })?;
    pause.pause_after_first_read();
    let after = sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
        row.get(0)
    })?;
    Ok((before, after))
}

async fn replace_note_while_snapshot_is_open(handle: &crate::CovenHandle, pause: &SnapshotPause) {
    pause.first_read.notified().await;
    tokio::time::timeout(
        Duration::from_secs(1),
        handle.write(|sql| {
            sql.execute("UPDATE notes SET body = 'After' WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        }),
    )
    .await
    .expect("WAL writer commits while the read snapshot remains open")
    .expect("commit replacement");
    pause.release();
}

fn open_handle() -> (tempfile::TempDir, crate::CovenHandle) {
    coven_keys::keys::test_keyring::install();
    let temp = tempfile::tempdir().expect("create store directory");
    let handle = Coven::builder(
        StoreDir::new_ephemeral(temp.path()),
        Config::with_defaults(
            "live-query-store".to_string(),
            "live-query-device".to_string(),
            "Test Store".to_string(),
        ),
    )
    .synced_tables(vec![SyncedTable::new(
        "notes",
        RowIdentity::IndependentUuid,
    )])
    .migrations(vec![Migration::sql(
        1,
        "live-query-schema",
        "CREATE TABLE notes (
            id TEXT PRIMARY KEY,
            body TEXT NOT NULL,
            rank INTEGER NOT NULL DEFAULT 0,
            _updated_at TEXT NOT NULL
        ) STRICT;
        CREATE TABLE labels (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        ) STRICT;
        CREATE TABLE scratch (value TEXT NOT NULL) STRICT;
        CREATE TRIGGER label_updates_note
        AFTER INSERT ON labels
        WHEN NEW.id = 'update-note-one'
        BEGIN
            UPDATE notes SET body = NEW.name WHERE id = NEW.name;
        END;",
    )])
    .open()
    .expect("open store");
    (temp, handle)
}

async fn assert_does_not_wake<T: Clone + PartialEq + Send + 'static>(
    query: &mut crate::LiveQuery<T>,
) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), query.next())
            .await
            .is_err(),
        "an irrelevant commit must not rerun the query"
    );
}

async fn insert_note(handle: &crate::CovenHandle, id: &str, body: &str) {
    let id = id.to_string();
    let body = body.to_string();
    handle
        .write(move |sql| {
            let stamp = sql.stamp();
            sql.execute(
                "INSERT INTO notes (id, body, _updated_at) VALUES (?1, ?2, ?3)",
                (id, body, stamp),
            )?;
            Ok(())
        })
        .await
        .expect("commit note");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_closure_sees_one_snapshot_across_statements() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Before").await;
    let pause = SnapshotPause::new();
    let read_pause = pause.clone();
    let reader = handle.clone();
    let read = tokio::spawn(async move {
        reader
            .read(move |sql| read_note_around_pause(sql, &read_pause))
            .await
    });

    replace_note_while_snapshot_is_open(&handle, &pause).await;

    assert_eq!(
        read.await.expect("read task").expect("read closure"),
        ("Before".to_string(), "Before".to_string())
    );
    let current = handle
        .read(|sql| {
            sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                row.get::<_, String>(0)
            })
            .map_err(CovenError::from)
        })
        .await
        .expect("read after snapshot closure");
    assert_eq!(current, "After");
}

#[tokio::test]
async fn read_transaction_ends_after_every_closure_exit() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Readable").await;

    let closure_error = handle
        .read(|_| {
            Err::<(), _>(CovenError::from(DbError::Message(
                "read refused".to_string(),
            )))
        })
        .await;
    assert!(closure_error.is_err());

    let database_error = handle
        .read(|sql| {
            sql.query_row("SELECT body FROM absent_table", [], |_| Ok(()))
                .map_err(CovenError::from)
        })
        .await;
    assert!(database_error.is_err());

    let panicking_handle = handle.clone();
    let panic = tokio::spawn(async move {
        panicking_handle
            .read(|_| -> crate::CovenResult<()> { panic!("read closure panic") })
            .await
    })
    .await;
    assert!(panic.is_err());

    let body = handle
        .read(|sql| {
            sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                row.get::<_, String>(0)
            })
            .map_err(CovenError::from)
        })
        .await
        .expect("read after closure exits");
    assert_eq!(body, "Readable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_live_query_sees_one_snapshot_across_statements() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Before").await;
    let pause = SnapshotPause::new();
    let query_pause = pause.clone();
    let mut query = handle.subscribe(move |sql| read_note_around_pause(sql, &query_pause));
    let delivery = tokio::spawn(async move { query.next().await });

    replace_note_while_snapshot_is_open(&handle, &pause).await;

    assert_eq!(
        delivery.await.expect("delivery task").expect("live query"),
        ("Before".to_string(), "Before".to_string())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconfigurable_live_query_sees_one_snapshot_across_statements() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Before").await;
    let pause = SnapshotPause::new();
    let query_pause = pause.clone();
    let mut query = handle
        .subscribe_reconfigurable((), move |(), sql| read_note_around_pause(sql, &query_pause));
    let delivery = tokio::spawn(async move { query.next().await });

    replace_note_while_snapshot_is_open(&handle, &pause).await;

    assert_eq!(
        delivery
            .await
            .expect("delivery task")
            .into_result()
            .expect("reconfigurable live query"),
        ("Before".to_string(), "Before".to_string())
    );
}

#[tokio::test]
async fn live_query_returns_initial_and_committed_results() {
    let (_temp, handle) = open_handle();
    let mut notes = handle.subscribe(|sql| {
        sql.query("SELECT body FROM notes ORDER BY id", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });

    assert_eq!(
        notes.next().await.expect("initial query"),
        Vec::<String>::new()
    );

    insert_note(
        &handle,
        "018f4e91-bb24-7ed6-b9be-6b8a4c248553",
        "First body",
    )
    .await;

    assert_eq!(
        notes.next().await.expect("query after commit"),
        vec!["First body".to_string()]
    );
}

#[tokio::test]
async fn query_errors_do_not_end_the_subscription() {
    let (_temp, handle) = open_handle();
    let fail = Arc::new(AtomicBool::new(true));
    let query_fail = fail.clone();
    let mut count = handle.subscribe(move |sql| {
        if query_fail.load(Ordering::Acquire) {
            return Err(CovenError::from(DbError::Message(
                "query failure".to_string(),
            )));
        }
        sql.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, i64>(0))
            .map_err(CovenError::from)
    });

    assert!(count.next().await.is_err(), "the query error is returned");
    fail.store(false, Ordering::Release);
    insert_note(
        &handle,
        "018f4e91-bb24-7ed6-a9be-6b8a4c248554",
        "Recovered query",
    )
    .await;

    assert_eq!(count.next().await.expect("query after later commit"), 1);
}

#[tokio::test]
async fn rolled_back_writes_do_not_wake_live_queries() {
    let (_temp, handle) = open_handle();
    let mut count = handle.subscribe(|sql| {
        sql.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, i64>(0))
            .map_err(CovenError::from)
    });
    assert_eq!(count.next().await.expect("initial count"), 0);

    let failed = handle
        .write(|sql| {
            let stamp = sql.stamp();
            sql.execute(
                "INSERT INTO notes (id, body, _updated_at) VALUES (?1, ?2, ?3)",
                ("note-rolled-back", "Discarded body", stamp),
            )?;
            Err::<(), _>(CovenError::from(DbError::Message(
                "refuse transaction".to_string(),
            )))
        })
        .await;
    assert!(failed.is_err(), "the host error rolls the transaction back");

    assert!(
        tokio::time::timeout(Duration::from_millis(100), count.next())
            .await
            .is_err(),
        "a rollback must not publish a live-query change"
    );
}

#[tokio::test]
async fn unrelated_tables_do_not_wake_live_queries() {
    let (_temp, handle) = open_handle();
    let mut notes = handle.subscribe(|sql| {
        sql.query("SELECT body FROM notes ORDER BY id", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(
        notes.next().await.expect("initial notes"),
        Vec::<String>::new()
    );

    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO labels (id, name) VALUES (?1, ?2)",
                ("label-one", "One"),
            )?;
            Ok(())
        })
        .await
        .expect("commit unrelated label");

    assert_does_not_wake(&mut notes).await;
}

#[tokio::test]
async fn unread_columns_do_not_wake_live_queries() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Body").await;
    let mut body = handle.subscribe(|sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(body.next().await.expect("initial body"), "Body");

    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET rank = 1 WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        })
        .await
        .expect("commit unread column");

    assert_does_not_wake(&mut body).await;
}

#[tokio::test]
async fn primary_key_filters_exclude_other_rows() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let mut body = handle.subscribe(|sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(body.next().await.expect("initial body"), "One");

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_TWO],
            )?;
            Ok(())
        })
        .await
        .expect("commit other row");

    assert_does_not_wake(&mut body).await;
}

#[tokio::test]
async fn text_primary_key_ranges_compare_sqlite_bytes_without_utf8_replacement() {
    let (_temp, handle) = open_handle();
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO labels (id, name) VALUES (CAST(x'ff' AS TEXT), 'Before')",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("insert non-UTF-8 text key");
    let mut names = handle.subscribe(|sql| {
        sql.query(
            "SELECT name FROM labels WHERE id >= ?1",
            ["\u{10000}"],
            |row| row.get::<_, String>(0),
        )
        .map_err(CovenError::from)
    });
    assert_eq!(
        names.next().await.expect("initial matching names"),
        vec!["Before".to_string()]
    );

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE labels SET name = 'After' WHERE id = CAST(x'ff' AS TEXT)",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("update non-UTF-8 text key");

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), names.next())
            .await
            .expect("matching byte range must rerun")
            .expect("names after matching update"),
        vec!["After".to_string()]
    );
}

#[tokio::test]
async fn a_relevant_commit_is_not_lost_behind_a_later_irrelevant_commit() {
    let (_temp, handle) = open_handle();
    let mut notes = handle.subscribe(|sql| {
        sql.query("SELECT body FROM notes ORDER BY id", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(
        notes.next().await.expect("initial notes"),
        Vec::<String>::new()
    );

    insert_note(&handle, NOTE_ONE, "One").await;
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO labels (id, name) VALUES (?1, ?2)",
                ("label-one", "One"),
            )?;
            Ok(())
        })
        .await
        .expect("commit later unrelated label");

    assert_eq!(
        notes.next().await.expect("query after relevant commit"),
        vec!["One".to_string()]
    );
}

#[tokio::test]
async fn a_relevant_column_and_primary_key_change_wakes_the_query() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let mut body = handle.subscribe(|sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(body.next().await.expect("initial body"), "One");

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit matching row");

    assert_eq!(body.next().await.expect("changed body"), "Changed");
}

#[tokio::test]
async fn in_and_range_primary_key_filters_exclude_other_rows() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let mut selected = handle.subscribe(|sql| {
        sql.query(
            "SELECT body FROM notes WHERE id IN (?1) AND id >= ?2 AND id <= ?3",
            (NOTE_ONE, NOTE_ONE, NOTE_ONE),
            |row| row.get::<_, String>(0),
        )
        .map_err(CovenError::from)
    });
    assert_eq!(
        selected.next().await.expect("initial selected rows"),
        vec!["One".to_string()]
    );

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_TWO],
            )?;
            Ok(())
        })
        .await
        .expect("commit row outside key predicates");

    assert_does_not_wake(&mut selected).await;
}

#[tokio::test]
async fn count_star_ignores_updates_but_wakes_for_row_insertions() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let mut count = handle.subscribe(|sql| {
        sql.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, i64>(0))
            .map_err(CovenError::from)
    });
    assert_eq!(count.next().await.expect("initial count"), 1);

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit update without cardinality change");
    assert_does_not_wake(&mut count).await;

    insert_note(&handle, NOTE_TWO, "Two").await;
    assert_eq!(count.next().await.expect("count after insertion"), 2);
}

#[tokio::test]
async fn tables_without_declared_primary_keys_publish_rowid_changes() {
    let (_temp, handle) = open_handle();
    let mut values = handle.subscribe(|sql| {
        sql.query("SELECT value FROM scratch ORDER BY rowid", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(
        values.next().await.expect("initial values"),
        Vec::<String>::new()
    );

    handle
        .write(|sql| {
            sql.execute("INSERT INTO scratch (value) VALUES ('captured')", [])?;
            Ok(())
        })
        .await
        .expect("commit rowid table insertion");

    assert_eq!(
        values.next().await.expect("values after insertion"),
        vec!["captured".to_string()]
    );
}

#[tokio::test]
async fn trigger_side_effects_publish_the_changed_target_row() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let mut body = handle.subscribe(|sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(body.next().await.expect("initial body"), "One");

    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO labels (id, name) VALUES ('update-note-one', ?1)",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit triggering insert");

    assert_eq!(body.next().await.expect("body after trigger"), NOTE_ONE);
}

#[tokio::test]
async fn cancellation_during_a_rerun_preserves_the_pending_change() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let invocations = Arc::new(AtomicUsize::new(0));
    let second_started = Arc::new(tokio::sync::Notify::new());
    let release_second = Arc::new((Mutex::new(false), Condvar::new()));
    let query_invocations = invocations.clone();
    let query_started = second_started.clone();
    let query_release = release_second.clone();
    let mut body = handle.subscribe(move |sql| {
        if query_invocations.fetch_add(1, Ordering::AcqRel) == 1 {
            query_started.notify_one();
            let (released, wake) = &*query_release;
            let mut released = released.lock().expect("release mutex poisoned");
            while !*released {
                released = wake.wait(released).expect("release mutex poisoned");
            }
        }
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(body.next().await.expect("initial body"), "One");

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit matching row");

    let mut cancelled = Box::pin(body.next());
    tokio::select! {
        () = second_started.notified() => {}
        result = &mut cancelled => panic!("rerun completed before cancellation: {result:?}"),
    }
    drop(cancelled);
    let (released, wake) = &*release_second;
    *released.lock().expect("release mutex poisoned") = true;
    wake.notify_one();

    assert_eq!(
        body.next().await.expect("rerun after cancellation"),
        "Changed"
    );
    assert_eq!(invocations.load(Ordering::Acquire), 3);
}

#[tokio::test]
async fn cancellation_while_waiting_does_not_consume_the_next_change() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let mut body = handle.subscribe(|sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(body.next().await.expect("initial body"), "One");

    assert!(
        tokio::time::timeout(Duration::from_millis(10), body.next())
            .await
            .is_err(),
        "the subscription has no pending relevant change"
    );

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit matching row");
    assert_eq!(
        body.next().await.expect("body after cancellation"),
        "Changed"
    );
}

#[tokio::test]
async fn reconfigurable_query_delivers_the_latest_absolute_request() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let mut notes = handle.subscribe_reconfigurable(Vec::<String>::new(), |request, sql| {
        let mut bodies = Vec::new();
        for id in request {
            bodies.push(
                sql.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
                    row.get::<_, String>(0)
                })?,
            );
        }
        Ok(bodies)
    });

    let initial = notes.next().await;
    assert_eq!(initial.cause(), ReconfigurableLiveQueryCause::Initial);
    assert_eq!(initial.revision().get(), 0);
    assert!(initial.request().is_empty());
    assert_eq!(
        initial.into_result().expect("initial query"),
        Vec::<String>::new()
    );

    let requested = notes
        .requests()
        .set(vec![NOTE_ONE.to_string()])
        .expect("subscription open");
    let delivered = notes.next().await;
    assert_eq!(
        delivered.cause(),
        ReconfigurableLiveQueryCause::RequestChanged
    );
    assert_eq!(delivered.revision(), requested);
    assert_eq!(delivered.request(), &[NOTE_ONE.to_string()]);
    assert_eq!(
        delivered.into_result().expect("requested query"),
        vec!["One"]
    );

    let requests = notes.requests();
    requests
        .set(vec![NOTE_ONE.to_string()])
        .expect("same request stays open");
    let latest = requests
        .set(vec![NOTE_TWO.to_string()])
        .expect("replace request");
    let delivered = notes.next().await;
    assert_eq!(
        delivered.cause(),
        ReconfigurableLiveQueryCause::RequestChanged
    );
    assert_eq!(delivered.revision(), latest);
    assert_eq!(delivered.request(), &[NOTE_TWO.to_string()]);
    assert_eq!(delivered.into_result().expect("latest query"), vec!["Two"]);
}

#[tokio::test]
async fn reconfigurable_query_serializes_request_changes_and_commits() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let mut notes = handle.subscribe_reconfigurable(vec![NOTE_ONE.to_string()], |request, sql| {
        sql.query_row(
            "SELECT body FROM notes WHERE id = ?1",
            [&request[0]],
            |row| row.get::<_, String>(0),
        )
        .map_err(CovenError::from)
    });
    assert_eq!(
        notes.next().await.into_result().expect("initial query"),
        "One"
    );

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit matching row");
    let revision = notes
        .requests()
        .set(vec![NOTE_TWO.to_string()])
        .expect("replace request");

    let delivered = notes.next().await;
    assert_eq!(
        delivered.cause(),
        ReconfigurableLiveQueryCause::RequestAndDatabaseChanged
    );
    assert_eq!(delivered.revision(), revision);
    assert_eq!(delivered.into_result().expect("latest query"), "Two");
}

#[tokio::test]
async fn reconfigurable_query_reports_a_relevant_commit_without_a_request_change() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let mut note = handle.subscribe_reconfigurable(NOTE_ONE.to_string(), |id, sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(
        note.next().await.cause(),
        ReconfigurableLiveQueryCause::Initial
    );

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Changed' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit matching row");

    let delivered = note.next().await;
    assert_eq!(
        delivered.cause(),
        ReconfigurableLiveQueryCause::DatabaseChanged
    );
    assert_eq!(delivered.into_result().expect("changed note"), "Changed");
}

#[tokio::test]
async fn reconfigurable_query_ignores_an_irrelevant_commit_coalesced_with_a_request() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let mut note = handle.subscribe_reconfigurable(NOTE_ONE.to_string(), |id, sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(
        note.next().await.cause(),
        ReconfigurableLiveQueryCause::Initial
    );

    note.requests()
        .set(NOTE_TWO.to_string())
        .expect("replace request");
    handle
        .write(|sql| {
            sql.execute(
                "INSERT INTO labels (id, name) VALUES ('unrelated', 'Unrelated')",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("commit unrelated row");

    let delivered = note.next().await;
    assert_eq!(
        delivered.cause(),
        ReconfigurableLiveQueryCause::RequestChanged
    );
    assert_eq!(delivered.into_result().expect("requested note"), "Two");
}

#[tokio::test]
async fn reconfigurable_query_reports_lag_as_a_database_change() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let mut note = handle.subscribe_reconfigurable(NOTE_ONE.to_string(), |id, sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(
        note.next().await.cause(),
        ReconfigurableLiveQueryCause::Initial
    );

    // Enough unread-column commits to lag the change receiver, with the
    // body edited inside the burst so the lag-forced rerun has something new
    // to deliver; a rerun that repeats the delivered value is withheld.
    for rank in 1..=257 {
        handle
            .write(move |sql| {
                sql.execute("UPDATE notes SET rank = ?1 WHERE id = ?2", (rank, NOTE_ONE))?;
                if rank == 200 {
                    sql.execute("UPDATE notes SET body = 'Lagged' WHERE id = ?1", [NOTE_ONE])?;
                }
                Ok(())
            })
            .await
            .expect("commit rank");
    }

    let lagged = note.next().await;
    assert_eq!(
        lagged.cause(),
        ReconfigurableLiveQueryCause::DatabaseChanged
    );
    assert_eq!(lagged.into_result().expect("body after lag"), "Lagged");
}

#[tokio::test]
async fn reconfigurable_query_request_handle_reports_subscription_drop() {
    let (_temp, handle) = open_handle();
    let notes = handle.subscribe_reconfigurable(0usize, |limit, sql| {
        sql.query(
            "SELECT body FROM notes ORDER BY id LIMIT ?1",
            [*limit as i64],
            |row| row.get::<_, String>(0),
        )
        .map_err(CovenError::from)
    });
    let requests = notes.requests();
    drop(notes);

    assert!(requests.set(1).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_changed_during_a_query_discards_the_superseded_result() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let invocations = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new((Mutex::new(false), Condvar::new()));
    let query_invocations = invocations.clone();
    let query_started = first_started.clone();
    let query_release = release_first.clone();
    let mut notes =
        handle.subscribe_reconfigurable(vec![NOTE_ONE.to_string()], move |request, sql| {
            if query_invocations.fetch_add(1, Ordering::AcqRel) == 0 {
                query_started.notify_one();
                let (released, wake) = &*query_release;
                let mut released = released.lock().expect("release mutex poisoned");
                while !*released {
                    released = wake.wait(released).expect("release mutex poisoned");
                }
            }
            sql.query_row(
                "SELECT body FROM notes WHERE id = ?1",
                [&request[0]],
                |row| row.get::<_, String>(0),
            )
            .map_err(CovenError::from)
        });
    let requests = notes.requests();

    let delivery = tokio::spawn(async move { notes.next().await });
    first_started.notified().await;
    let revision = requests
        .set(vec![NOTE_TWO.to_string()])
        .expect("replace in-progress request");
    let (released, wake) = &*release_first;
    *released.lock().expect("release mutex poisoned") = true;
    wake.notify_one();

    let delivered = delivery.await.expect("delivery task");
    assert_eq!(
        delivered.cause(),
        ReconfigurableLiveQueryCause::RequestChanged
    );
    assert_eq!(delivered.revision(), revision);
    assert_eq!(delivered.request(), &[NOTE_TWO.to_string()]);
    assert_eq!(delivered.into_result().expect("latest result"), "Two");
    assert_eq!(invocations.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn a_rerun_that_repeats_the_delivered_value_is_not_delivered() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    insert_note(&handle, NOTE_TWO, "Two").await;
    let mut first_page = handle.subscribe(|sql| {
        sql.query("SELECT body FROM notes ORDER BY id LIMIT 1", [], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    assert_eq!(first_page.next().await.expect("initial page"), vec!["One"]);

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'Two again' WHERE id = ?1",
                [NOTE_TWO],
            )?;
            Ok(())
        })
        .await
        .expect("commit a row outside the page");
    assert_does_not_wake(&mut first_page).await;

    handle
        .write(|sql| {
            sql.execute(
                "UPDATE notes SET body = 'One again' WHERE id = ?1",
                [NOTE_ONE],
            )?;
            Ok(())
        })
        .await
        .expect("commit the row inside the page");
    assert_eq!(
        first_page.next().await.expect("page after change"),
        vec!["One again"]
    );
}

#[tokio::test]
async fn a_request_change_delivers_even_when_the_value_repeats() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "Same").await;
    insert_note(&handle, NOTE_TWO, "Same").await;
    let mut body = handle.subscribe_reconfigurable(NOTE_ONE.to_string(), |id, sql| {
        sql.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
            row.get::<_, String>(0)
        })
        .map_err(CovenError::from)
    });
    let initial = body.next().await;
    assert_eq!(initial.cause(), ReconfigurableLiveQueryCause::Initial);
    assert_eq!(initial.into_result().expect("initial body"), "Same");

    body.requests()
        .set(NOTE_TWO.to_string())
        .expect("set request");
    let switched = body.next().await;
    assert_eq!(
        switched.cause(),
        ReconfigurableLiveQueryCause::RequestChanged
    );
    assert_eq!(switched.request(), NOTE_TWO);
    assert_eq!(switched.into_result().expect("body after request"), "Same");
}

#[tokio::test]
async fn the_first_value_after_an_error_is_delivered_even_when_it_repeats() {
    let (_temp, handle) = open_handle();
    insert_note(&handle, NOTE_ONE, "One").await;
    let fail = Arc::new(AtomicBool::new(false));
    let mut body = handle.subscribe({
        let fail = fail.clone();
        move |sql| {
            if fail.load(Ordering::SeqCst) {
                return Err(CovenError::from(DbError::Message("forced".to_string())));
            }
            sql.query_row("SELECT body FROM notes WHERE id = ?1", [NOTE_ONE], |row| {
                row.get::<_, String>(0)
            })
            .map_err(CovenError::from)
        }
    });
    assert_eq!(body.next().await.expect("initial body"), "One");

    fail.store(true, Ordering::SeqCst);
    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET body = 'Two' WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        })
        .await
        .expect("commit while failing");
    assert!(body.next().await.is_err(), "the failing run is delivered");

    fail.store(false, Ordering::SeqCst);
    handle
        .write(|sql| {
            sql.execute("UPDATE notes SET body = 'One' WHERE id = ?1", [NOTE_ONE])?;
            Ok(())
        })
        .await
        .expect("commit the original body back");
    assert_eq!(
        body.next().await.expect("body after error"),
        "One",
        "an error clears the remembered value, so the repeat is delivered"
    );
}
