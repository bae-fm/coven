use super::*;
use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::TestStore;
use coven_foundation::files::{SettingsError, StoreLockError};

impl Database {
    pub(crate) fn commit_writer<T>(&self, run: impl FnOnce(&DatabaseConnection) -> T) -> T {
        self.inspect_writer(|writer| writer.transaction(|writer| Ok(run(writer))).unwrap())
    }

    pub(crate) fn inspect_writer_schema<T>(
        &self,
        inspect: impl FnOnce(&DatabaseConnection, &crate::write_schema::WriteSchema) -> T,
    ) -> T {
        let slot = self.inner.read().unwrap();
        let inner = slot.as_ref().unwrap();
        let writer = inner.writer.lock().unwrap();
        inspect(&writer, &inner.write_schema)
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
        for reader in &slot.as_ref().unwrap().readers.readers {
            let error = reader
                .lock()
                .unwrap()
                .internal_execute("INSERT INTO notes VALUES ('x', 'y', '')", [])
                .unwrap_err();
            assert!(
                matches!(error, DbError::Sqlite(rusqlite::Error::SqliteFailure(code, _)) if code.code == rusqlite::ErrorCode::ReadOnly)
            );
        }
    }
    let hold_reader = |database: Database| {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held = std::thread::spawn(move || {
            let slot = database.inner.read().unwrap();
            let _reader = slot.as_ref().unwrap().readers.acquire_reader();
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
        let reader = slot.as_ref().unwrap().readers.acquire_reader();
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

#[test]
fn another_process_can_read_while_the_writer_is_open() {
    let store = TestStore::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = runtime
        .block_on(store.builder(notes_tables(), notes_migrations()).open())
        .unwrap();
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
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    runtime.block_on(database.close()).unwrap();
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
        let checks = inner
            .readers
            .readers
            .iter()
            .map(|r| r.lock().unwrap().integrity_checks())
            .sum::<usize>()
            + inner.writer.lock().unwrap().integrity_checks();
        assert_eq!(checks, 1);
        let slot = reader.inner.read().unwrap();
        assert_eq!(
            slot.as_ref()
                .unwrap()
                .readers
                .iter()
                .map(|r| r.lock().unwrap().integrity_checks())
                .sum::<usize>(),
            1
        );
    }
    reader.close().await.unwrap();
    database.close().await.unwrap();
}
