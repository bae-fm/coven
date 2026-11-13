use crate::tests::TestStore;
use crate::{CheckpointMode, DbError, Migration, SqliteSettings, WalAutocheckpoint, WalCheckpoint};
use coven_foundation::clock::HANG_GUARD;
use std::num::NonZeroU32;
use std::sync::{Condvar, Mutex};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

impl super::WalCheckpointer {
    /// Visit the checkpointer's connection, after any checkpoint in progress.
    pub(crate) fn with_connection(
        &self,
        visit: impl FnOnce(&crate::sqlite::DatabaseConnection) -> Result<(), crate::DbError>,
    ) -> Result<(), crate::DbError> {
        visit(&self.upkeep().connection.lock().expect("checkpointer lock poisoned"))
    }

    /// Run one wake's checkpoint now, on the calling thread.
    pub(crate) fn run_now(&self) -> Result<(), crate::DbError> {
        self.upkeep().checkpoint()
    }

    /// Run `fault` as each checkpoint takes the writer.
    pub(crate) fn fault(&self, fault: impl FnMut() -> Result<(), crate::DbError> + Send + 'static) {
        *self.upkeep().fault.lock().unwrap() = Some(Box::new(fault));
    }

    /// Run `step` as each pass's copy returns, before its sync.
    pub(crate) fn after_copy(&self, step: impl FnMut() + Send + 'static) {
        *self.upkeep().copied.lock().unwrap() = Some(Box::new(step));
    }

    /// What each checkpoint's last step does from now on, told while the
    /// checkpoint still holds the writer.
    pub(crate) fn report_restarts(&self) -> UnboundedReceiver<WalCheckpoint> {
        let (restarts, reported) = unbounded_channel();
        *self.upkeep().restarts.lock().unwrap() = Some(restarts);
        reported
    }
}

fn blobs(store: &TestStore, settings: SqliteSettings) -> crate::DatabaseBuilder {
    blobs_in(store.directory(), settings)
}

fn blobs_in(
    directory: coven_foundation::files::StoreDir,
    settings: SqliteSettings,
) -> crate::DatabaseBuilder {
    crate::DatabaseBuilder::new(directory)
        .synced_tables(Vec::new())
        .migrations(vec![Migration::addition(
            1,
            "blobs",
            "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
        )])
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

/// The frames the WAL holds not yet copied, read on the writer between
/// writes.
fn not_copied(writer: &crate::sqlite::DatabaseConnection) -> u32 {
    let WalCheckpoint::Ran { log_frames, checkpointed_frames, .. } =
        writer.wal_checkpoint(CheckpointMode::Noop).unwrap()
    else {
        panic!("a NOOP checkpoint between writes reads the WAL");
    };
    log_frames - checkpointed_frames
}

/// The frames `checkpointed_at_100_pages` lets the WAL hold not yet copied
/// before coven checkpoints it.
const THRESHOLD: u32 = 100;

/// Commit until the WAL holds `THRESHOLD` frames not yet copied, then wait
/// for what coven's checkpointer tells `told` as it checkpoints them, off
/// the commit path. The checkpointer tells it while holding the writer, so
/// whatever `told` holds while the test holds the writer came from an earlier
/// checkpoint, and the checkpoint the last commit woke tells it next.
async fn checkpointed<T>(db: &crate::Database, told: &mut UnboundedReceiver<T>) -> T {
    loop {
        add(db).await;
        let past = db.inspect_writer(|writer| {
            let past = not_copied(writer) >= THRESHOLD;
            while told.try_recv().is_ok() {}
            past
        });
        if past {
            break;
        }
    }
    tokio::time::timeout(HANG_GUARD, told.recv())
        .await
        .expect("the checkpointer checkpoints a WAL past its threshold")
        .expect("the checkpointer runs while the store is open")
}

/// The writer never checkpoints as it commits; coven's checkpointer does,
/// off the commit path, once the WAL passes the threshold, and the WAL then
/// starts over from its beginning, a live query rerunning or not (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coven_checkpoints_the_wal_past_its_threshold() {
    let store = TestStore::new();
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    db.inspect_writer(|writer| {
        let pages: i64 = writer.query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0)).unwrap();
        assert_eq!(pages, 0, "the writer never checkpoints as it commits");
    });
    let mut restarts = db.report_checkpoints();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?)
    });
    let reader = tokio::spawn(async move { while let Some(Ok(_)) = query.next().await {} });
    tokio::time::timeout(HANG_GUARD, async {
        let mut started_over = 0;
        while started_over < 3 {
            let restarted = checkpointed(&db, &mut restarts).await;
            let WalCheckpoint::Ran { busy, log_frames, .. } = restarted else {
                panic!("holding the writer, the checkpoint reads the WAL: {restarted:?}");
            };
            assert!(log_frames >= THRESHOLD, "checkpointed at {log_frames} frames");
            // A rerun that held its snapshot past the readers' wait leaves
            // the WAL copied, to start over once it grows by the threshold
            // again.
            if busy {
                continue;
            }
            add(&db).await;
            assert!(frames(&db).await < THRESHOLD, "the next commit starts the WAL over");
            started_over += 1;
        }
    })
    .await
    .expect("the WAL starts over");
    reader.abort();
    db.close().await.unwrap();
}

/// A commit that lands while the checkpoint copies the WAL is copied and
/// synced too before the checkpoint holds the writer: holding it, the
/// checkpoint finds every frame already copied, so coven's writes don't wait
/// while it copies or syncs them (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_during_the_copy_is_copied_before_the_checkpoint_holds_the_writer() {
    let store = TestStore::new();
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    let (copied, mut copies) = unbounded_channel();
    let (go, going) = std::sync::mpsc::channel::<()>();
    let mut first = true;
    db.after_checkpoint_copies(move || {
        if std::mem::take(&mut first) {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a test that stopped listening lets the checkpoint go on"
            )]
            let _ = copied.send(()).map(|()| going.recv());
        }
    });
    // A connection of the test's own reads the WAL's counts as the
    // checkpoint takes the writer.
    let wal = rusqlite::Connection::open(store.database_path()).unwrap();
    let (uncopied, mut told) = unbounded_channel();
    db.fault_checkpoints(move || {
        let (log, copied): (i64, i64) = wal
            .query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| Ok((r.get(1)?, r.get(2)?)))
            .unwrap();
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a test that stopped listening has no use for it"
        )]
        let _ = uncopied.send(log - copied);
        Ok(())
    });
    // Commit until the WAL holds the threshold's frames not yet copied, and
    // no more, so the first pass copies them all.
    loop {
        add(&db).await;
        if db.inspect_writer(|writer| not_copied(writer)) >= THRESHOLD {
            break;
        }
    }
    tokio::time::timeout(HANG_GUARD, copies.recv())
        .await
        .expect("the checkpointer copies a WAL past its threshold")
        .unwrap();
    // The first pass's copy has returned, and this commit lands after it.
    add(&db).await;
    go.send(()).unwrap();
    let uncopied = tokio::time::timeout(HANG_GUARD, told.recv()).await.unwrap().unwrap();
    assert_eq!(uncopied, 0, "holding the writer, the checkpoint finds every frame copied");
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
    assert!(!db.checkpoints_its_database(), "no checkpointer runs for the store's database");
    for _ in 0..100 {
        add(&db).await;
    }
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
        .read(|sql| {
            Ok::<_, DbError>(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get(0))?)
        })
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

/// Run `fault` as each checkpoint takes the writer, telling the returned
/// receiver as it does.
fn faulted(
    db: &crate::Database,
    mut fault: impl FnMut() -> Result<(), DbError> + Send + 'static,
) -> UnboundedReceiver<()> {
    let (tried, tries) = unbounded_channel();
    db.fault_checkpoints(move || {
        #[expect(
            clippy::let_underscore_must_use,
            reason = "a test that stopped listening has no use for it"
        )]
        let _ = tried.send(());
        fault()
    });
    tries
}

/// Write past the threshold until the checkpoint `fault` fails; then the
/// error a live query reports, after which it ends, and the write the
/// failure refuses.
async fn checkpointer_failure(
    db: &crate::Database,
    fault: impl FnMut() -> Result<(), DbError> + Send + 'static,
) -> DbError {
    let mut tries = faulted(db, fault);
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?)
    });
    checkpointed(db, &mut tries).await;
    let failure = tokio::time::timeout(HANG_GUARD, async {
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
    assert!(insert(db).await.is_err(), "the failed handle refuses a write");
    failure
}

/// A panic in a checkpoint is a bug in coven. The store can't stay open with
/// no checkpointer while its WAL grows without bound, so the handle fails with
/// the bug: the bug sink hears it once, every later call and live query fails
/// with `StoreFailed` carrying it, and the process lives on. Closing the handle
/// and opening the store again starts a checkpointer that checkpoints again
/// (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_checkpoint_fails_the_handle_with_its_bug_and_a_reopen_checkpoints_again() {
    let bugs = coven_foundation::task::HeardBugs::shared();
    let store = TestStore::reporting_to(bugs.clone());
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    let failure = checkpointer_failure(&db, || panic!("the checkpoint broke")).await;
    let DbError::StoreFailed(bug) = &failure else {
        panic!("{failure:?}");
    };
    assert_eq!(bug.operation, "coven WAL checkpoint");
    assert_eq!(bug.message, "the checkpoint broke");
    assert!(matches!(insert(&db).await, Err(DbError::StoreFailed(later)) if later == *bug));
    assert!(matches!(db.read(|_| Ok(())).await, Err(DbError::StoreFailed(later)) if later == *bug));
    assert_eq!(bugs.take(), std::slice::from_ref(bug), "the sink hears the bug once");
    db.close().await.unwrap();

    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    let mut restarts = db.report_checkpoints();
    checkpointed(&db, &mut restarts).await;
    add(&db).await;
    db.close().await.unwrap();
}

/// A checkpoint SQLite fails other than on a busy or locked database stops
/// checkpointing too, and fails the handle with SQLite's cause (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_checkpoint_fails_the_handle_with_its_cause() {
    let store = TestStore::new();
    let db = blobs(&store, checkpointed_at_100_pages()).open().await.unwrap();
    let failure =
        checkpointer_failure(&db, || Err(sqlite_error(rusqlite::ffi::SQLITE_IOERR))).await;
    let DbError::CheckpointFailed(cause) = &failure else {
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
    let mut tried = 0;
    let mut tries = faulted(&db, move || {
        tried += 1;
        match tried {
            1 => Err(sqlite_error(rusqlite::ffi::SQLITE_BUSY)),
            2 => Err(sqlite_error(rusqlite::ffi::SQLITE_LOCKED)),
            _ => Ok(()),
        }
    });
    // Each try after the first is the checkpoint tried again.
    for _ in 0..3 {
        checkpointed(&db, &mut tries).await;
    }
    db.read(|sql| Ok::<_, DbError>(sql.query_row("SELECT 1", [], |r| r.get::<_, i64>(0))?))
        .await
        .unwrap();
    db.close().await.unwrap();
}

/// Where the connection holding SQLite's checkpoint lock stands, as its
/// busy handler, its thread and the test tell each other through `HOLDER`
/// and `HOLDER_MOVED`.
struct Holder {
    /// Its busy handler is waiting: it holds the lock.
    holding: bool,
    /// Its checkpoint returned.
    finished: bool,
    /// The test lets its busy handler give up.
    released: bool,
}

static HOLDER: Mutex<Holder> =
    Mutex::new(Holder { holding: false, finished: false, released: false });
static HOLDER_MOVED: Condvar = Condvar::new();

/// Change where the holder stands, and wake whoever waits on it.
fn move_holder(change: impl FnOnce(&mut Holder)) {
    change(&mut HOLDER.lock().unwrap());
    HOLDER_MOVED.notify_all();
}

/// A busy handler that waits until the test releases it, then gives up.
fn wait_for_release(_tries: i32) -> bool {
    move_holder(|holder| holder.holding = true);
    drop(HOLDER_MOVED.wait_while(HOLDER.lock().unwrap(), |holder| !holder.released).unwrap());
    false
}

/// Start another connection's RESTART checkpoint, which takes SQLite's
/// checkpoint lock and holds it while it waits for a reader, until the test
/// releases it. It returns SQLite's busy column. A try that finds coven's
/// own checkpointer holding the lock gives up at once, so it tries again.
async fn hold_the_checkpoint_lock(store: &TestStore) -> std::thread::JoinHandle<i64> {
    loop {
        move_holder(|holder| holder.finished = false);
        let path = store.database_path();
        let holder = std::thread::spawn(move || {
            let mut connection = rusqlite::Connection::open(path).unwrap();
            connection.busy_handler(Some(wait_for_release)).unwrap();
            let busy =
                connection.query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| r.get(0)).unwrap();
            move_holder(|holder| holder.finished = true);
            busy
        });
        let holding = tokio::task::spawn_blocking(|| {
            HOLDER_MOVED
                .wait_while(HOLDER.lock().unwrap(), |holder| !holder.holding && !holder.finished)
                .unwrap()
                .holding
        })
        .await
        .unwrap();
        if holding {
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
    let mut restarts = db.report_checkpoints();
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

    move_holder(|holder| holder.released = true);
    assert_eq!(holder.join().unwrap(), 1, "the other checkpoint gave up waiting for the reader");
    reader.execute_batch("COMMIT").unwrap();
    drop(reader);
    let restarted = checkpointed(&db, &mut restarts).await;
    assert!(matches!(restarted, WalCheckpoint::Ran { busy: false, .. }), "{restarted:?}");
    add(&db).await;
    assert!(frames(&db).await < THRESHOLD, "coven's checkpoints go on");
    db.close().await.unwrap();
}

/// The checkpointer lives as long as the store: a store opened on a runtime
/// that then shuts down, and kept open and written on another, still has
/// its WAL checkpointed (§4).
#[test]
fn a_store_outliving_its_opening_runtime_still_checkpoints() {
    let store = TestStore::new();
    let opening = tokio::runtime::Runtime::new().unwrap();
    let db = opening.block_on(blobs(&store, checkpointed_at_100_pages()).open()).unwrap();
    let mut restarts = db.report_checkpoints();
    drop(opening);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let restarted = checkpointed(&db, &mut restarts).await;
        assert!(matches!(restarted, WalCheckpoint::Ran { busy: false, .. }), "{restarted:?}");
        add(&db).await;
        assert!(frames(&db).await < THRESHOLD, "the next commit starts the WAL over");
        db.close().await.unwrap();
    });
}
