use super::*;
use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::TestStore;
use coven_foundation::files::{SettingsError, StoreLockError};

impl Database {
    pub(crate) fn observe_next_reader_wait(&self) -> tokio::sync::oneshot::Receiver<()> {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().access.readers.observe_next_wait()
    }

    pub(crate) fn commit_writer<T>(&self, run: impl FnOnce(&DatabaseConnection) -> T) -> T {
        self.inspect_writer(|writer| writer.transaction(|writer| Ok(run(writer))).unwrap())
    }

    pub(crate) fn inspect_writer_schema<T>(
        &self,
        inspect: impl FnOnce(&DatabaseConnection, &crate::write_schema::WriteSchema) -> T,
    ) -> T {
        let slot = self.inner.read().unwrap();
        let inner = slot.as_ref().unwrap();
        let writer = inner.access.writer.lock().unwrap();
        inspect(&writer, &inner.access.write_schema)
    }

    pub(crate) fn inspect_writer<T>(&self, inspect: impl FnOnce(&DatabaseConnection) -> T) -> T {
        let inner = self.inner.read().unwrap();
        let writer = inner.as_ref().unwrap().access.writer.lock().unwrap();
        inspect(&writer)
    }

    pub(crate) fn inspect_readers(&self, inspect: impl Fn(&DatabaseConnection)) {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().access.readers.inspect_each(inspect);
    }
}

impl DatabaseReadHandle {
    pub(crate) fn inspect_readers(&self, inspect: impl Fn(&DatabaseConnection)) {
        let inner = self.inner.read().unwrap();
        inner.as_ref().unwrap().readers.inspect_each(inspect);
    }

    pub(crate) fn inspect_writer<T>(&self, inspect: impl FnOnce(&DatabaseConnection) -> T) -> T {
        let inner = self.inner.read().unwrap();
        let writer = inner.as_ref().unwrap().writer.lock().unwrap();
        inspect(&writer)
    }
}

#[tokio::test]
async fn writer_is_exclusive_readers_coexist_and_clones_close_together() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let clone = database.clone();
    assert_eq!(database.schema_version().await.unwrap(), 1);
    assert!(
        matches!(store.builder(notes_tables(), notes_migrations()).open().await.err().unwrap(), CovenError::Lock(StoreLockError::AlreadyOpen(id)) if id == store.id())
    );
    let reader = store
        .builder(notes_tables(), notes_migrations())
        .open_read_only()
        .await
        .unwrap();
    assert_eq!(reader.schema_version().await.unwrap(), 1);
    database.inspect_writer(|sql| {
        assert_eq!(
            sql.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        assert!(!sql
            .query_row("SELECT coven_applying()", [], |r| r.get::<_, bool>(0))
            .unwrap());
    });
    database.close().await.unwrap();
    assert!(matches!(database.close().await, Err(DbError::StoreClosed)));
    assert!(matches!(
        clone.schema_version().await,
        Err(DbError::StoreClosed)
    ));
    assert!(matches!(
        clone.applied_migrations(),
        Err(DbError::StoreClosed)
    ));
    assert_eq!(reader.schema_version().await.unwrap(), 1);
    let reopened = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    assert!(reopened.applied_migrations().unwrap().is_empty());
    reader.close().await.unwrap();
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_connections_are_read_only_and_run_concurrently() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    {
        let slot = database.inner.read().unwrap();
        slot.as_ref().unwrap().access.readers.assert_read_only();
    }
    let hold_reader = |database: Database| {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held = std::thread::spawn(move || {
            let slot = database.inner.read().unwrap();
            let _reader = slot.as_ref().unwrap().access.readers.acquire_reader();
            entered_tx.send(()).unwrap();
            release_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        });
        entered_rx.recv().unwrap();
        (release_tx, held)
    };
    let mut held = vec![hold_reader(database.clone())];
    // With one reservation held, even the fourth successive call must finish.
    // Cycling through reader mutexes would block one of these calls.
    for _ in 0..4 {
        let (tx, rx) = std::sync::mpsc::channel();
        let clone = database.clone();
        let call = tokio::spawn(async move {
            tx.send(clone.schema_version().await).unwrap();
        });
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
                .unwrap(),
            1
        );
        call.await.unwrap();
    }
    for _ in 0..3 {
        held.push(hold_reader(database.clone()));
    }
    let (tx, rx) = std::sync::mpsc::channel();
    let clone = database.clone();
    let fifth = tokio::spawn(async move {
        tx.send(clone.schema_version().await).unwrap();
    });
    assert!(matches!(
        rx.recv_timeout(std::time::Duration::from_millis(200)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    let (release, worker) = held.pop().unwrap();
    release.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        1
    );
    fifth.await.unwrap();
    for (release, worker) in held {
        release.send(()).unwrap();
        worker.join().unwrap();
    }
    database.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_waits_for_a_borrowed_connection_before_releasing_the_lock() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let clone = database.clone();
    let worker = std::thread::spawn(move || {
        let slot = clone.inner.read().unwrap();
        let reader = slot.as_ref().unwrap().access.readers.acquire_reader();
        entered_tx.send(()).unwrap();
        release_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        reader.schema_version().unwrap()
    });
    entered_rx.recv().unwrap();
    let clone = database.clone();
    let (closing_tx, closing_rx) = std::sync::mpsc::channel();
    let closing = tokio::spawn(async move {
        closing_tx.send(()).unwrap();
        clone.close().await
    });
    closing_rx.recv().unwrap();
    store.assert_writer_locked();
    assert!(!closing.is_finished());
    release_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), 1);
    closing.await.unwrap().unwrap();
    store.assert_writer_unlocked();
}

#[tokio::test]
async fn settings_failure_remains_distinct() {
    let store = TestStore::new();
    let settings = store
        .database_path()
        .parent()
        .unwrap()
        .join("settings.json");
    std::fs::remove_file(settings).unwrap();
    assert!(matches!(
        store.builder(vec![], vec![]).open().await.err().unwrap(),
        CovenError::Settings(SettingsError::Missing(_))
    ));
}

#[tokio::test]
async fn dropped_bootstrap_reopen_waits_for_the_active_read_to_finish() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let temp = tempfile::tempdir().unwrap();
    let layout = coven_foundation::files::StoreLayout::new(temp.path().to_owned());
    let id = coven_foundation::id_source::StoreId(uuid::Uuid::from_u128(1));
    let pending = layout
        .begin_bootstrap(id, "Household", &UuidIds)
        .await
        .unwrap();
    let database = DatabaseBuilder::new(pending.directory())
        .synced_tables(vec![])
        .migrations(vec![])
        .open()
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut read = Box::pin(database.read(move |_| {
        entered.send(()).unwrap();
        released
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        Ok(())
    }));
    tokio::select! {
        result = &mut read => panic!("held read returned: {result:?}"),
        result = entry => result.unwrap(),
    }
    drop(read);
    drop(database);
    drop(pending);
    let mut reopen = Box::pin(layout.begin_bootstrap(id, "Household", &UuidIds));
    poll_fn(|cx| {
        assert!(
            reopen.as_mut().poll(cx).is_pending(),
            "reopen must await the held read"
        );
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    let resumed = reopen.await.unwrap();
    let database = DatabaseBuilder::new(resumed.directory())
        .synced_tables(vec![])
        .migrations(vec![])
        .open()
        .await
        .unwrap();
    database.read(|_| Ok(())).await.unwrap();
    database.close().await.unwrap();
    resumed.cancel().unwrap();
}

#[tokio::test]
async fn reopening_a_dropped_or_closing_database_waits_for_its_active_read() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    for close in [false, true] {
        let store = TestStore::new();
        let database = store
            .builder(notes_tables(), notes_migrations())
            .open()
            .await
            .unwrap();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let read = database.start_read(move |_| {
            entered.send(()).unwrap();
            released
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            Ok(())
        });
        entry.await.unwrap();
        if close {
            let mut closing = Box::pin(database.close());
            poll_fn(|cx| {
                assert!(closing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            // The close continues even when its caller stops awaiting it.
            drop(closing);
        }
        drop(database);
        let mut reopen = Box::pin(store.builder(notes_tables(), notes_migrations()).open());
        poll_fn(|cx| {
            assert!(reopen.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_other_process_refuses_writer(&store);
        release.send(()).unwrap();
        read.await.unwrap().unwrap();
        let reopened = reopen.await.unwrap();
        assert_eq!(reopened.schema_version().await.unwrap(), 1);
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_dropped_open_waits_for_its_blocking_migration_before_reopening() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let store = TestStore::new();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let mut opening = Box::pin(
        store
            .builder(
                vec![],
                vec![Migration::run(1, "held", move |_| {
                    entered.lock().unwrap().take().unwrap().send(()).unwrap();
                    released
                        .lock()
                        .unwrap()
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                    Ok(())
                })],
            )
            .open(),
    );
    tokio::select! {
        result = &mut opening => panic!("held migration returned: {:?}", result.err()),
        result = entry => result.unwrap(),
    }
    drop(opening);
    let mut reopen = Box::pin(
        store
            .builder(
                vec![],
                vec![Migration::run(1, "held", |_| {
                    panic!("committed migration must not run again")
                })],
            )
            .open(),
    );
    poll_fn(|cx| {
        assert!(reopen.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    let reopened = reopen.await.unwrap();
    assert_eq!(reopened.schema_version().await.unwrap(), 1);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn dropping_a_write_caller_makes_reopen_wait_for_its_commit() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let mut write = Box::pin(database.write(move |sql| {
        sql.execute(
            "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','Title','Body')",
            [],
        )?;
        entered.send(()).unwrap();
        released
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        Ok(())
    }));
    tokio::select! {
        result = &mut write => panic!("held write returned: {result:?}"),
        result = entry => result.unwrap(),
    }
    drop(write);
    drop(database);
    let mut reopen = Box::pin(store.builder(notes_tables(), notes_migrations()).open());
    let waiting = poll_fn(|cx| Poll::Ready(reopen.as_mut().poll(cx).is_pending())).await;
    release.send(()).unwrap();
    assert!(
        waiting,
        "blocking work must not keep its dropped caller live"
    );
    let reopened = reopen.await.unwrap();
    assert_eq!(
        reopened
            .read(|sql| Ok(sql.query_row(
                "SELECT title FROM notes WHERE id='00000000-0000-4000-8000-000000000001'",
                [],
                |row| row.get::<_, String>(0)
            )?))
            .await
            .unwrap(),
        "Title"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_close_while_file_work_is_held_does_not_cancel_release() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let store = TestStore::new();
    let database = store.builder(vec![], vec![]).open().await.unwrap();
    let lease = database.file_tasks.clone().read_owned().await;
    let mut closing = Box::pin(database.close());
    poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    let mut reopen = Box::pin(store.builder(vec![], vec![]).open());
    poll_fn(|cx| {
        assert!(reopen.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(lease);
    let reopened = reopen.await.unwrap();
    assert!(matches!(
        database.schema_version().await,
        Err(DbError::StoreClosed)
    ));
    reopened.close().await.unwrap();
}

#[test]
fn another_process_can_read_while_the_writer_is_open() {
    let store = TestStore::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = runtime
        .block_on(store.builder(notes_tables(), notes_migrations()).open())
        .unwrap();
    assert_other_process_refuses_writer(&store);
    runtime.block_on(database.close()).unwrap();
}

fn assert_other_process_refuses_writer(store: &TestStore) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "database::tests::read_only_process",
            "--nocapture",
        ])
        .env("COVEN_DATABASE_TEST_FILE", store.database_path())
        .env("COVEN_DATABASE_TEST_STORE", store.id().to_string())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn read_only_process() {
    let Ok(path) = std::env::var("COVEN_DATABASE_TEST_FILE") else {
        return;
    };
    let store_id = coven_foundation::id_source::StoreId(
        uuid::Uuid::parse_str(&std::env::var("COVEN_DATABASE_TEST_STORE").unwrap()).unwrap(),
    );
    let path = std::path::Path::new(&path);
    let layout = coven_foundation::files::StoreLayout::new(
        path.parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned(),
    );
    let directory = layout.store_dir(&store_id);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    assert!(matches!(
        runtime.block_on(DatabaseBuilder::new(directory.clone())
            .synced_tables(notes_tables()).migrations(notes_migrations()).open()),
        Err(CovenError::Lock(StoreLockError::AlreadyOpen(id))) if id == store_id
    ));
    assert!(matches!(
        runtime.block_on(layout.begin_bootstrap(store_id, "Store", &UuidIds)),
        Err(coven_foundation::files::BootstrapDirectoryError::Lock(
            StoreLockError::AlreadyOpen(id)
        )) if id == store_id
    ));
    let db = runtime
        .block_on(
            DatabaseBuilder::new(directory)
                .synced_tables(notes_tables())
                .migrations(notes_migrations())
                .open_read_only(),
        )
        .unwrap();
    assert_eq!(runtime.block_on(db.schema_version()).unwrap(), 1);
    runtime.block_on(db.close()).unwrap();
    std::process::exit(86);
}

#[tokio::test]
async fn a_panicking_migration_preserves_its_panic_payload() {
    let call = tokio::spawn(async {
        let store = TestStore::new();
        let _ = store
            .builder(
                vec![],
                vec![Migration::run(1, "panic", |_| {
                    std::panic::panic_any(123_u64);
                })],
            )
            .open()
            .await;
    });
    let panic = call
        .await
        .expect_err("opening must propagate the migration panic");
    assert_eq!(*panic.into_panic().downcast::<u64>().unwrap(), 123);
}

#[tokio::test]
async fn opening_checks_integrity_once_for_all_its_connections() {
    let store = TestStore::new();
    let database = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let reader = store
        .builder(notes_tables(), notes_migrations())
        .open_read_only()
        .await
        .unwrap();
    {
        let slot = database.inner.read().unwrap();
        let inner = slot.as_ref().unwrap();
        let checks = inner.access.readers.integrity_checks()
            + inner.access.writer.lock().unwrap().integrity_checks();
        assert_eq!(checks, 1);
        let slot = reader.inner.read().unwrap();
        assert_eq!(slot.as_ref().unwrap().readers.integrity_checks(), 1);
    }
    reader.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn callback_panics_release_the_writer_and_reserve_nothing() {
    for work in [
        crate::ClaimWork::Writes(1),
        crate::ClaimWork::Operation(crate::OperationId(1)),
    ] {
        let store = TestStore::new();
        let db = store.builder(vec![], vec![]).open().await.unwrap();
        let call = db.clone();
        let panic = tokio::spawn(async move {
            call.begin_claim(
                work,
                |_| -> Result<crate::ClaimAttempt<std::io::Empty>, DbError> {
                    std::panic::panic_any(123u64)
                },
            )
            .await
            .unwrap();
        })
        .await
        .unwrap_err();
        assert_eq!(*panic.into_panic().downcast::<u64>().unwrap(), 123);
        assert!(db.current_claim().await.unwrap().is_none());
        let claim = db
            .begin_claim(crate::ClaimWork::Writes(1), |_| {
                Ok(crate::ClaimAttempt {
                    path: coven_format::path::ObjectPath::slot(
                        crate::tests::store_id(),
                        1u64.try_into().unwrap(),
                    ),
                    bytes: Box::new(std::io::Cursor::new(vec![7])),
                })
            })
            .await
            .unwrap();
        assert_eq!(claim.number, 1);
        db.write(|_| Ok(())).await.unwrap();
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn journal_lookups_read_current_steps_and_keep_failed_rows_in_order() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let mut ids = Vec::new();
    for kind in ["invite", "reload-snapshots", "reload-snapshots"] {
        ids.push(
            db.start_operation(crate::NewOperation {
                kind: kind.into(),
                data: vec![1],
                started_by: "coven".into(),
                pauses_writes: false,
            })
            .await
            .unwrap(),
        );
    }
    assert!(db.operation(crate::OperationId(0)).await.unwrap().is_none());
    assert!(db.first_operation("retention").await.unwrap().is_none());
    db.advance_operation(crate::OperationUpdate {
        id: ids[1],
        previous: 0,
        last_step: 2,
        data: vec![2, 3],
    })
    .await
    .unwrap();
    db.record_pending(crate::pending::tests::operation_record(&db, ids[1]).await)
        .await
        .unwrap();
    let first = db
        .first_operation("reload-snapshots")
        .await
        .unwrap()
        .unwrap();
    let exact = db.operation(ids[1]).await.unwrap().unwrap();
    for record in [first, exact] {
        assert_eq!(record.id, ids[1]);
        assert_eq!(record.kind, "reload-snapshots");
        assert_eq!(record.last_step, 2);
        assert_eq!(record.data, [2, 3]);
        assert_eq!(record.started_by, "coven");
        assert!(db
            .operation_pending::<crate::pending::tests::Record>(record.id)
            .await
            .unwrap()
            .is_some());
    }
    assert_eq!(db.operation(ids[0]).await.unwrap().unwrap().data, [1]);
    db.finish_operation(ids[1]).await.unwrap();
    assert!(db.operation(ids[1]).await.unwrap().is_none());
    assert_eq!(
        db.first_operation("reload-snapshots")
            .await
            .unwrap()
            .unwrap()
            .id,
        ids[2]
    );
    db.finish_operation(ids[2]).await.unwrap();
    assert!(db
        .first_operation("reload-snapshots")
        .await
        .unwrap()
        .is_none());
    db.close().await.unwrap();
}

#[tokio::test]
async fn finished_operation_ids_cannot_address_a_new_operation_after_reopening() {
    fn operation() -> crate::NewOperation {
        crate::NewOperation {
            kind: "reload-snapshots".into(),
            data: vec![1],
            started_by: "coven".into(),
            pauses_writes: false,
        }
    }
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let old = db.start_operation(operation()).await.unwrap();
    db.finish_operation(old).await.unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let new = db.start_operation(operation()).await.unwrap();
    assert!(new > old);
    assert!(matches!(
        db.advance_operation(crate::OperationUpdate {
            id: old,
            previous: 0,
            last_step: 1,
            data: vec![2],
        })
        .await,
        Err(DbError::OperationChanged(id)) if id == old
    ));
    db.finish_operation(old).await.unwrap();
    let current = db.operation(new).await.unwrap().unwrap();
    assert_eq!(current.last_step, 0);
    assert_eq!(current.data, [1]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_panicking_write_whose_rollback_fails_still_panics_and_fails_the_handle() {
    let store = TestStore::new();
    let open = || store.builder(notes_tables(), notes_migrations()).open();
    let db = open().await.unwrap();
    db.inspect_writer(|writer| writer.refuse_transaction_end(true));
    let clone = db.clone();
    let panic = tokio::spawn(async move {
        clone
            .write(|sql| -> Result<(), DbError> {
                sql.execute("INSERT INTO notes VALUES('n','held','')", [])?;
                panic!("app write panic")
            })
            .await
    })
    .await
    .unwrap_err();
    assert_eq!(
        *panic.into_panic().downcast::<&str>().unwrap(),
        "app write panic"
    );
    let held = |db: &Database| {
        db.inspect_writer(|writer| {
            writer.query_row("SELECT count(*) FROM notes", [], |r| r.get::<_, i64>(0))
        })
        .unwrap()
    };
    assert_eq!(held(&db), 1, "the writer still holds the uncommitted row");
    assert!(matches!(
        db.write(|_| Ok(())).await,
        Err(DbError::RollbackFailed)
    ));
    assert!(matches!(
        db.read(|_| Ok(())).await,
        Err(CovenError::Database(DbError::RollbackFailed))
    ));
    db.close().await.unwrap();
    let db = open().await.unwrap();
    assert_eq!(held(&db), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_rollback_after_snapshot_checks_fails_the_handle() {
    // Checks that pass report the failed ROLLBACK; checks that fail report both.
    for valid in [true, false] {
        let store = TestStore::new();
        let open = || store.builder(notes_tables(), notes_migrations()).open();
        let db = open().await.unwrap();
        db.write(|sql| {
            sql.execute(
                "INSERT INTO notes VALUES('00000000-0000-4000-8000-000000000001','kept','')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let snapshot =
            crate::snapshot_write::tests::frames(&db, coven_merge::Audience::Store).await;
        let (id, prefix, input, author) = snapshot.input();
        db.validate_snapshot(
            id,
            prefix,
            input,
            author,
            std::sync::Arc::new(crate::test_utils::TestAuthorship),
        )
        .await
        .unwrap();
        let (id, prefix, input, author) = snapshot.input();
        let input = if valid {
            input
        } else {
            // Framing is checked before acquiring the writer. This failure
            // belongs to installation: synced values disagree with merge state.
            let (header, mut records) = crate::snapshot_write::tests::decode(&snapshot);
            let row = records
                .iter_mut()
                .find_map(|record| match record {
                    coven_format::snapshot::SnapshotRecord::Synced(row) => Some(row),
                    _ => None,
                })
                .expect("snapshot carries the inserted row");
            row.columns.get_mut("title").unwrap().value =
                coven_format::value::Value::Text("changed".into());
            let (mut encoder, mut bytes) =
                coven_format::snapshot::SnapshotEncoder::start(header).unwrap();
            for record in records {
                bytes.extend(encoder.record(record).unwrap());
            }
            bytes.extend(encoder.finish().unwrap());
            std::io::Cursor::new(bytes)
        };
        db.inspect_writer(|writer| writer.refuse_transaction_end(true));
        let error = db
            .validate_snapshot(
                id,
                prefix,
                input,
                author,
                std::sync::Arc::new(crate::test_utils::TestAuthorship),
            )
            .await
            .unwrap_err();
        if valid {
            assert!(matches!(error, DbError::Sqlite(_)), "{error:?}");
        } else {
            assert!(
                matches!(&error, DbError::Rollback { operation, .. } if !matches!(**operation, DbError::Sqlite(_) | DbError::RollbackFailed)),
                "{error:?}"
            );
        }
        let scratch = db.inspect_writer(|writer| {
            writer
                .query_row(
                    "SELECT count(*) FROM temp.sqlite_schema WHERE name='_coven_snapshot_values'",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap()
        });
        assert_eq!(
            scratch, 1,
            "the writer is still inside the checks' transaction"
        );
        assert!(matches!(
            db.write(|_| Ok(())).await,
            Err(DbError::RollbackFailed)
        ));
        db.close().await.unwrap();
        let db = open().await.unwrap();
        db.write(|_| Ok(())).await.unwrap();
        db.close().await.unwrap();
    }
}

/// A self-replacement's operation pauses app writes from its insertion until
/// it finishes; finishing it removes the pause in the same transaction (§10).
#[tokio::test]
async fn a_pausing_operation_refuses_app_writes_until_it_finishes() {
    let store = TestStore::new();
    let db = store
        .builder(notes_tables(), notes_migrations())
        .open()
        .await
        .unwrap();
    let insert = |n: u32| {
        let db = db.clone();
        async move {
            db.write(move |sql| -> Result<(), DbError> {
                sql.execute(
                    "INSERT INTO notes VALUES(?1,'title','')",
                    [format!("00000000-0000-4000-8000-{n:012}")],
                )?;
                Ok(())
            })
            .await
        }
    };
    insert(1).await.unwrap();
    let operation = |pauses_writes| crate::NewOperation {
        kind: "replace-member-keys".into(),
        data: vec![0],
        started_by: "test".into(),
        pauses_writes,
    };
    let other = db.start_operation(operation(false)).await.unwrap();
    insert(2).await.unwrap();
    let paused = db.start_operation(operation(true)).await.unwrap();
    assert!(matches!(insert(3).await, Err(DbError::MemberKeysChanging)));
    // A write that changes no synced row queues nothing and is not refused.
    db.write(|_| Ok(())).await.unwrap();
    db.finish_operation(other).await.unwrap();
    assert!(matches!(insert(4).await, Err(DbError::MemberKeysChanging)));
    db.finish_operation(paused).await.unwrap();
    insert(5).await.unwrap();
    let ids: Vec<String> = db
        .read(|sql| Ok(sql.query("SELECT id FROM notes ORDER BY id", [], |r| r.get(0))?))
        .await
        .unwrap();
    let expected: Vec<String> = [1, 2, 5]
        .map(|n| format!("00000000-0000-4000-8000-{n:012}"))
        .into();
    assert_eq!(ids, expected);
    db.close().await.unwrap();
}
