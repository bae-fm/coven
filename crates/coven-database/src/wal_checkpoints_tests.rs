use crate::tests::TestStore;
use crate::{
    CheckpointMode, CovenError, DbError, Migration, SqliteSettings, WalAutocheckpoint,
    WalCheckpoint,
};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

impl super::WalCheckpointer {
    /// Visit the checkpointer's connection, after any checkpoint in progress,
    /// unless it has stopped.
    pub(crate) fn with_connection(
        &self,
        visit: impl FnOnce(&crate::sqlite::DatabaseConnection) -> Result<(), crate::DbError>,
    ) -> Result<(), crate::DbError> {
        self.shared
            .connection
            .lock()
            .expect("checkpointer lock poisoned")
            .as_ref()
            .map_or(Ok(()), visit)
    }

    /// Run one wake's checkpoint now, on the calling thread.
    pub(crate) fn run_now(&self) -> Result<(), crate::DbError> {
        self.shared.run()
    }

    /// Run `fault` as each checkpoint takes the writer.
    pub(crate) fn fault(&self, fault: impl FnMut() -> Result<(), crate::DbError> + Send + 'static) {
        *self.shared.fault.lock().unwrap() = Some(Box::new(fault));
    }
}

fn blobs(store: &TestStore, settings: SqliteSettings) -> crate::DatabaseBuilder {
    store
        .builder(
            Vec::new(),
            vec![Migration::addition(
                1,
                "blobs",
                "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
            )],
        )
        .sqlite_settings(settings)
}

fn wal(store: &TestStore) -> std::path::PathBuf {
    let mut path = store.database_path().into_os_string();
    path.push("-wal");
    path.into()
}

/// Write four rows of 4,000 bytes: some 5 frames of the WAL.
async fn insert(db: &crate::Database) -> Result<(), DbError> {
    db.write(|sql| {
        sql.execute(
            "INSERT INTO blobs(body) SELECT randomblob(4000) FROM (VALUES(1),(2),(3),(4))",
            [],
        )?;
        Ok(())
    })
    .await
}

async fn add(db: &crate::Database) {
    insert(db).await.unwrap();
}

/// Frames the WAL holds now.
async fn frames(db: &crate::Database) -> u32 {
    match db.checkpoint(CheckpointMode::Noop).await.unwrap() {
        WalCheckpoint::Ran { log_frames, .. } => log_frames,
        WalCheckpoint::NotStarted => panic!("a NOOP checkpoint between writes reads the WAL"),
    }
}

/// The writer never checkpoints as it commits; coven's checkpointer does,
/// off the commit path, once the WAL passes the threshold, and the WAL then
/// starts over from its beginning, a live query rerunning or not (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coven_checkpoints_the_wal_past_its_threshold() {
    let store = TestStore::new();
    let settings = SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Pages(NonZeroU32::new(100).unwrap())),
        ..SqliteSettings::default()
    };
    let db = blobs(&store, settings).open().await.unwrap();
    db.inspect_writer(|writer| {
        let pages: i64 = writer.query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0)).unwrap();
        assert_eq!(pages, 0, "the writer never checkpoints as it commits");
    });
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?)
    });
    let reader = tokio::spawn(async move { while let Some(Ok(_)) = query.next().await {} });
    let mut largest = 0;
    for _ in 0..1000 {
        add(&db).await;
        largest = largest.max(frames(&db).await);
    }
    reader.abort();
    // 1,000 commits of about 5 frames each would be 5,000 frames without
    // it. Commits go on while each checkpoint runs, so the WAL passes the
    // threshold by what they add meanwhile.
    assert!(largest < 500, "the WAL grew to {largest} frames");
    db.close().await.unwrap();
}

/// With `Off`, coven never checkpoints: the WAL grows until the app does.
#[tokio::test]
async fn off_leaves_checkpoints_to_the_app() {
    let store = TestStore::new();
    let settings = SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        ..SqliteSettings::default()
    };
    let db = blobs(&store, settings).open().await.unwrap();
    for _ in 0..100 {
        add(&db).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(frames(&db).await >= 400);
    let done = db.checkpoint(CheckpointMode::Truncate).await.unwrap();
    assert!(matches!(done, WalCheckpoint::Ran { busy: false, .. }), "{done:?}");
    assert_eq!(frames(&db).await, 0);
    db.close().await.unwrap();
}

/// Closing the store still checkpoints the whole WAL and removes its file,
/// as the last connection to close does: the checkpointer's closes first.
#[tokio::test]
async fn closing_checkpoints_and_removes_the_wal() {
    let store = TestStore::new();
    let db = blobs(&store, SqliteSettings::default()).open().await.unwrap();
    for _ in 0..50 {
        add(&db).await;
    }
    assert!(wal(&store).exists());
    db.close().await.unwrap();
    assert!(!wal(&store).exists(), "closing removes the WAL");
    let db = blobs(&store, SqliteSettings::default()).open().await.unwrap();
    let rows: i64 = db
        .read(|sql| Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(rows, 200);
    db.close().await.unwrap();
}

/// Settings under which coven checkpoints once the WAL holds 100 pages
/// not yet copied, which some 20 of `add`'s commits pass.
fn checkpointed_at_100_pages() -> SqliteSettings {
    SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Pages(NonZeroU32::new(100).unwrap())),
        ..SqliteSettings::default()
    }
}

fn sqlite_error(code: std::ffi::c_int) -> DbError {
    DbError::Sqlite(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None))
}

/// Write past the threshold until the checkpointer's failure refuses a
/// write; then the error a live query reports, after which it ends.
async fn checkpointer_failure(db: &crate::Database) -> CovenError {
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?)
    });
    let mut writes = 0;
    while insert(db).await.is_ok() {
        writes += 1;
        assert!(writes < 1000, "no checkpoint failed the handle");
    }
    let failure = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match query.next().await {
                Some(Ok(_)) => (),
                Some(Err(error)) => return error,
                None => panic!("the query ended without the failure"),
            }
        }
    })
    .await
    .expect("the live query reports the failure");
    assert!(query.next().await.is_none(), "the live query ends after the failure");
    failure
}

/// A panic in a checkpoint is a bug in coven, and its checkpointing stops:
/// the handle fails with it rather than stay open while the WAL grows
/// without bound. The panic leaves the writer it held unpoisoned, so later
/// calls report the failure and closing still closes every connection (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_checkpoint_fails_the_handle() {
    let store = TestStore::new();
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    db.fault_checkpoints(|| panic!("the checkpoint broke"));
    let failure = checkpointer_failure(&db).await;
    assert!(
        matches!(
            &failure,
            CovenError::Database(DbError::CheckpointPanicked { message })
                if message == "the checkpoint broke"
        ),
        "{failure:?}"
    );
    let read = db.read(|sql| Ok(sql.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?)).await;
    assert!(matches!(read, Err(CovenError::Database(DbError::CheckpointPanicked { .. }))));
    db.close().await.unwrap();
}

/// A checkpoint SQLite fails other than on a busy or locked database stops
/// checkpointing too, and fails the handle with SQLite's cause (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_checkpoint_fails_the_handle_with_its_cause() {
    let store = TestStore::new();
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    db.fault_checkpoints(|| Err(sqlite_error(rusqlite::ffi::SQLITE_IOERR)));
    let failure = checkpointer_failure(&db).await;
    let CovenError::Database(DbError::CheckpointFailed(cause)) = &failure else {
        panic!("{failure:?}");
    };
    assert!(
        matches!(
            cause.as_ref(),
            DbError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == rusqlite::ErrorCode::SystemIoFailure
        ),
        "{cause:?}"
    );
    db.close().await.unwrap();
}

/// A database busy or locked past the busy timeout is expected: the
/// checkpoint is tried again at the next commit's wake, and the handle
/// goes on (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_checkpoint_is_tried_again() {
    let store = TestStore::new();
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    let tries = Arc::new(AtomicUsize::new(0));
    let counted = tries.clone();
    db.fault_checkpoints(move || match counted.fetch_add(1, Ordering::SeqCst) {
        0 => Err(sqlite_error(rusqlite::ffi::SQLITE_BUSY)),
        1 => Err(sqlite_error(rusqlite::ffi::SQLITE_LOCKED)),
        _ => Ok(()),
    });
    let mut writes = 0;
    while tries.load(Ordering::SeqCst) < 3 {
        add(&db).await;
        writes += 1;
        assert!(writes < 1000, "the checkpoint was not tried again");
    }
    db.read(|sql| Ok(sql.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?)).await.unwrap();
    db.close().await.unwrap();
}

/// Set once the connection holding SQLite's checkpoint lock waits in its
/// busy handler; the test sets the second to let it give up.
static HOLDING: AtomicBool = AtomicBool::new(false);
static RELEASED: AtomicBool = AtomicBool::new(false);

/// A busy handler that waits until the test releases it, then gives up.
fn wait_for_release(_tries: i32) -> bool {
    HOLDING.store(true, Ordering::SeqCst);
    while !RELEASED.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    false
}

/// Start another connection's RESTART checkpoint, which takes SQLite's
/// checkpoint lock and holds it while it waits for a reader, until the test
/// releases it. It returns SQLite's busy column. A try that finds coven's
/// own checkpointer holding the lock gives up at once, so it tries again.
async fn hold_the_checkpoint_lock(store: &TestStore) -> std::thread::JoinHandle<i64> {
    loop {
        let path = store.database_path();
        let holder = std::thread::spawn(move || {
            let connection = rusqlite::Connection::open(path).unwrap();
            connection.busy_handler(Some(wait_for_release)).unwrap();
            connection.query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| r.get(0)).unwrap()
        });
        while !HOLDING.load(Ordering::SeqCst) && !holder.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        if HOLDING.load(Ordering::SeqCst) {
            return holder;
        }
        holder.join().unwrap();
    }
}

/// While another connection checkpoints the WAL, it holds SQLite's
/// checkpoint lock, and a checkpoint that finds the lock held reads nothing
/// of the WAL: SQLite reports busy with no frames. That is the database
/// busy, not a failure: coven's checkpointer tries again at the next wake,
/// and the handle goes on (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_another_connection_holds_off_is_tried_again() {
    let store = TestStore::new();
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    // A reader whose snapshot keeps every later frame in the WAL, so the WAL
    // stays past the threshold however often coven checkpoints.
    let reader = rusqlite::Connection::open(store.database_path()).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader.query_row("SELECT count(*) FROM blobs", [], |r| r.get(0)).unwrap();
    for _ in 0..40 {
        add(&db).await;
    }
    let holder = hold_the_checkpoint_lock(&store).await;

    let checkpoint = db.checkpoint(CheckpointMode::Passive).await.unwrap();
    assert_eq!(checkpoint, WalCheckpoint::NotStarted);
    // The WAL is past the threshold, so this goes on past reading it to
    // copying it, which finds the lock held.
    db.checkpoint_now().unwrap();

    RELEASED.store(true, Ordering::SeqCst);
    assert_eq!(holder.join().unwrap(), 1, "the other checkpoint gave up waiting for the reader");
    reader.execute_batch("COMMIT").unwrap();
    drop(reader);
    let mut writes = 0;
    while frames(&db).await >= 100 {
        add(&db).await;
        writes += 1;
        assert!(writes < 100, "coven's checkpoints stopped");
    }
    db.close().await.unwrap();
}

/// A runtime shutting down drops the checkpoint's blocking work unstarted.
/// That is the process ending the store, not a bug: checkpointing stops
/// and the handle is left as it was.
#[test]
fn a_checkpoint_its_runtime_dropped_leaves_the_handle_as_it_was() {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let handle = runtime.handle().clone();
    drop(runtime);
    let observer = crate::observation::CommitObserver::new();
    let serving = super::Serving {
        shared: Arc::new(super::Checkpoints {
            wake: tokio::sync::Notify::new(),
            closing: std::sync::atomic::AtomicBool::new(false),
            connection: std::sync::Mutex::new(None),
            writer: std::sync::Weak::new(),
            threshold: NonZeroU32::new(100).unwrap(),
            fault: std::sync::Mutex::new(None),
        }),
        handle: observer.clone(),
    };
    let flow = tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
        let _entered = handle.enter();
        serving.checkpoint().await
    });
    assert!(flow.is_break(), "checkpointing stops");
    assert!(observer.error().is_none(), "the handle is left as it was");
}
