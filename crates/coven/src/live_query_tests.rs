use crate::{Coven, CovenError, DbError, Migration, RowIdentity, StoreDir, SyncedTable};
use coven_foundation::config::Config;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::time::Duration;

const NOTE_ONE: &str = "018f4e91-bb24-7ed6-a9be-6b8a4c248551";
const NOTE_TWO: &str = "018f4e91-bb24-7ed6-a9be-6b8a4c248552";

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

async fn assert_does_not_wake<T: Send + 'static>(query: &mut crate::LiveQuery<T>) {
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
            return Err(CovenError::Database(DbError::Message(
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
            Err::<(), _>(CovenError::Database(DbError::Message(
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
