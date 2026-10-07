use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
    CovenError, DbError,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc,
};
use std::time::Duration;

#[tokio::test]
async fn read_is_lazy_and_readonly_opens_use_the_same_api() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('f47ac10b-58cc-4372-a567-0e02b2c3d479','Plan','body')")
            .unwrap()
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let read = db.read(move |sql| {
        count.fetch_add(1, Ordering::SeqCst);
        Ok(sql.query_row(
            "SELECT title FROM notes WHERE id=?1",
            ["f47ac10b-58cc-4372-a567-0e02b2c3d479"],
            |r| r.get::<_, String>(0),
        )?)
    });
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(read.await.unwrap(), "Plan");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let ro = store
        .builder(notes_tables(), notes_migrations())
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(
        ro.read(|sql| Ok(sql.query_row("SELECT body FROM notes", [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap(),
        "body"
    );
    ro.close().await.unwrap();
    assert!(matches!(
        ro.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::StoreClosed))
    ));
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_is_fixed_before_the_closure_and_across_its_statements() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    db.commit_writer(|sql| {
        sql.batch("INSERT INTO notes VALUES ('a','old','')")
            .unwrap()
    });
    let (entered, entry) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let clone = db.clone();
    let task = tokio::spawn(async move {
        clone
            .read(move |sql| {
                entered.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(5)).unwrap();
                let first =
                    sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?;
                let second =
                    sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?;
                Ok((first, second))
            })
            .await
    });
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    db.commit_writer(|sql| sql.batch("UPDATE notes SET title='new'").unwrap());
    release.send(()).unwrap();
    assert_eq!(task.await.unwrap().unwrap(), ("old".into(), "old".into()));
    assert_eq!(
        db.read(|sql| Ok(sql.query_row("SELECT title FROM notes", [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap(),
        "new"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn read_context_refuses_writes_and_internal_tables_with_typed_errors() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    for statement in [
        "INSERT INTO notes VALUES ('a','b','c') RETURNING title",
        "UPDATE notes SET title='changed' RETURNING title",
        "DELETE FROM notes RETURNING title",
        "CREATE TEMP TABLE temp_write (x)",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "PRAGMA user_version=9",
        "PRAGMA data_version",
        "ATTACH ':memory:' AS other",
    ] {
        let error = db
            .read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                CovenError::Database(DbError::StatementForbidden { .. })
            ),
            "{statement}: {error:?}"
        );
    }
    for statement in [
        "SELECT * FROM _coven_lost",
        "SELECT * FROM _CoVeN_rows",
        "SELECT * FROM _coven_columns",
    ] {
        let error = db
            .read(move |sql| Ok(sql.query(statement, [], |_| Ok(()))?))
            .await
            .unwrap_err();
        assert!(
            matches!(error, CovenError::Database(DbError::InternalTable { .. })),
            "{error:?}"
        );
    }
    assert_eq!(
        db.read(|sql| Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        0
    );
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_releases_all_reader_leases_and_store_borrows() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let (entered, entry) = mpsc::channel();
    let mut releases = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let clone = db.clone();
        let entered = entered.clone();
        let (release, released) = mpsc::channel();
        releases.push(release);
        tasks.push(tokio::spawn(async move {
            clone
                .read(|sql| {
                    Ok(sql.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))?)
                })
                .process(move |value| {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(value + 1)
                })
                .await
        }));
    }
    for _ in 0..4 {
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), db.read(|_| Ok(42)))
            .await
            .unwrap()
            .unwrap(),
        42
    );
    tokio::time::timeout(Duration::from_secs(2), db.close())
        .await
        .unwrap()
        .unwrap();
    for release in releases {
        release.send(()).unwrap();
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap(), 1);
    }
}

#[tokio::test]
async fn a_panicking_read_releases_its_snapshot_and_keeps_the_reader_usable() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let clone = db.clone();
    let panic = tokio::spawn(async move {
        clone
            .read(|_| -> crate::CovenResult<()> { panic!("app read panic") })
            .await
    })
    .await
    .unwrap_err();
    assert_eq!(
        *panic.into_panic().downcast::<&str>().unwrap(),
        "app read panic"
    );
    assert_eq!(
        db.read(|sql| Ok(sql.query_row("SELECT 42", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        42
    );
    db.close().await.unwrap();

    let ro = store
        .builder(vec![], vec![])
        .open_read_only()
        .await
        .unwrap();
    let clone = ro.clone();
    let panic = tokio::spawn(async move {
        clone
            .read(|_| -> crate::CovenResult<()> { panic!("read-only panic") })
            .await
    })
    .await
    .unwrap_err();
    assert_eq!(
        *panic.into_panic().downcast::<&str>().unwrap(),
        "read-only panic"
    );
    assert_eq!(
        ro.read(|sql| Ok(sql.query_row("SELECT 42", [], |r| r.get::<_, i64>(0))?))
            .await
            .unwrap(),
        42
    );
    ro.close().await.unwrap();
}
