use crate::tests::{QueryError, TestDatabase, TestError, TestOptions, HANG_GUARD};
use crate::{CheckpointMode, SqliteSettings, WalAutocheckpoint, WalCheckpoint};
use std::convert::Infallible;
use std::num::NonZeroU32;
use std::sync::{Condvar, Mutex};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

/// A table of blobs, under `settings`, checkpointed on a thread of its own
/// as they ask.
fn blobs(settings: SqliteSettings) -> TestDatabase {
    TestDatabase::open_with(
        "CREATE TABLE blobs(id INTEGER PRIMARY KEY, body BLOB NOT NULL)",
        options(settings),
    )
}

/// The options of a database under `settings`, checkpointed as they ask.
fn options(settings: SqliteSettings) -> TestOptions {
    TestOptions {
        checkpoint_pages: settings.checkpoint_pages(),
        settings,
        ..TestOptions::default()
    }
}

fn wal(db: &TestDatabase) -> std::path::PathBuf {
    let mut path = db.path().to_owned().into_os_string();
    path.push("-wal");
    path.into()
}

/// Write four rows of 4,000 bytes: some 5 frames of the WAL.
fn insert(db: &TestDatabase) -> Result<(), TestError> {
    db.write("INSERT INTO blobs(body) SELECT randomblob(4000) FROM (VALUES(1),(2),(3),(4))")
}

fn add(db: &TestDatabase) {
    insert(db).unwrap();
}

/// Frames the WAL holds now.
fn frames(db: &TestDatabase) -> u32 {
    match db.host(|writer| writer.wal_checkpoint(CheckpointMode::Noop)).unwrap() {
        WalCheckpoint::Ran { log_frames, .. } => log_frames,
        WalCheckpoint::NotStarted => panic!("a NOOP checkpoint between writes reads the WAL"),
    }
}

/// The frames the WAL holds not yet copied, read on the writer between
/// writes.
fn not_copied(db: &TestDatabase) -> u32 {
    let WalCheckpoint::Ran { log_frames, checkpointed_frames, .. } =
        db.host(|writer| writer.wal_checkpoint(CheckpointMode::Noop)).unwrap()
    else {
        panic!("a NOOP checkpoint between writes reads the WAL");
    };
    log_frames - checkpointed_frames
}

/// The frames `checkpointed_at_100_pages` lets the WAL hold not yet copied
/// before sqlive checkpoints it.
const THRESHOLD: u32 = 100;

/// Commit until the WAL holds `THRESHOLD` frames not yet copied, then wait
/// for what the checkpointer tells `told` as it checkpoints them, off the
/// commit path. The checkpointer tells it while holding the writer, so
/// whatever `told` holds while the test holds the writer came from an earlier
/// checkpoint, and the checkpoint the last commit woke tells it next.
async fn checkpointed<T>(db: &TestDatabase, told: &mut UnboundedReceiver<T>) -> T {
    loop {
        add(db);
        let past = db.host(|writer| {
            let WalCheckpoint::Ran { log_frames, checkpointed_frames, .. } =
                writer.wal_checkpoint(CheckpointMode::Noop).unwrap()
            else {
                panic!("a NOOP checkpoint between writes reads the WAL");
            };
            while told.try_recv().is_ok() {}
            log_frames - checkpointed_frames >= THRESHOLD
        });
        if past {
            break;
        }
    }
    tokio::time::timeout(HANG_GUARD, told.recv())
        .await
        .expect("the checkpointer checkpoints a WAL past its threshold")
        .expect("the checkpointer runs while the database is open")
}

/// The writer never checkpoints as it commits; sqlive's checkpointer does,
/// off the commit path, once the WAL passes the threshold, and the WAL then
/// starts over from its beginning, a live query rerunning or not (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlive_checkpoints_the_wal_past_its_threshold() {
    let db = blobs(checkpointed_at_100_pages());
    let pages: i64 = db.value("PRAGMA wal_autocheckpoint");
    assert_eq!(pages, 0, "the writer never checkpoints as it commits");
    let mut restarts = db.with_database(|database| database.report_checkpoints());
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?)
    });
    let reader = tokio::spawn(async move { while let Some(Ok(_)) = query.next_inline().await {} });
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
            add(&db);
            assert!(frames(&db) < THRESHOLD, "the next commit starts the WAL over");
            started_over += 1;
        }
    })
    .await
    .expect("the WAL starts over");
    reader.abort();
    db.close();
}

/// The checkpoint also copies and syncs a commit made while it copies the
/// WAL, before it holds the writer. Once the checkpoint holds the writer, it
/// finds every frame already copied, so writes don't wait while it copies or
/// syncs them (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_during_the_copy_is_copied_before_the_checkpoint_holds_the_writer() {
    let db = blobs(checkpointed_at_100_pages());
    let (copied, mut copies) = unbounded_channel();
    let (go, going) = std::sync::mpsc::channel::<()>();
    let mut first = true;
    db.with_database(|database| {
        database.after_checkpoint_copies(move || {
            if std::mem::take(&mut first) {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "a test that stopped listening lets the checkpoint go on"
                )]
                let _ = copied.send(()).map(|()| going.recv());
            }
        })
    });
    // A connection of the test's own reads the WAL's counts as the
    // checkpoint takes the writer.
    let wal = rusqlite::Connection::open(db.path()).unwrap();
    let (uncopied, mut told) = unbounded_channel();
    db.with_database(|database| {
        database.fault_checkpoints(move || {
            let (log, copied): (i64, i64) = wal
                .query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| Ok((r.get(1)?, r.get(2)?)))
                .unwrap();
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a test that stopped listening has no use for it"
            )]
            let _ = uncopied.send(log - copied);
            Ok(())
        })
    });
    // Commit until the WAL holds the threshold's frames not yet copied, and
    // no more, so the first pass copies them all.
    loop {
        add(&db);
        if not_copied(&db) >= THRESHOLD {
            break;
        }
    }
    tokio::time::timeout(HANG_GUARD, copies.recv())
        .await
        .expect("the checkpointer copies a WAL past its threshold")
        .unwrap();
    // The first pass's copy has returned, and this commit lands after it.
    add(&db);
    go.send(()).unwrap();
    let uncopied = tokio::time::timeout(HANG_GUARD, told.recv()).await.unwrap().unwrap();
    assert_eq!(uncopied, 0, "holding the writer, the checkpoint finds every frame copied");
    db.close();
}

/// With `Off`, sqlive never checkpoints: the WAL grows until the host does.
#[test]
fn off_leaves_checkpoints_to_the_host() {
    let settings = SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Off),
        ..SqliteSettings::default()
    };
    let db = blobs(settings);
    assert!(
        !db.with_database(|database| database.checkpoints()),
        "no checkpointer runs for the database"
    );
    for _ in 0..100 {
        add(&db);
    }
    assert!(frames(&db) >= 400);
    let done = db.host(|writer| writer.wal_checkpoint(CheckpointMode::Truncate)).unwrap();
    assert!(matches!(done, WalCheckpoint::Ran { busy: false, .. }), "{done:?}");
    assert_eq!(frames(&db), 0);
    db.close();
}

/// Closing the database still checkpoints the whole WAL and removes its
/// file, as the last connection to close does: the checkpointer's connection
/// closes first.
#[test]
fn closing_checkpoints_and_removes_the_wal() {
    let db = blobs(SqliteSettings::default());
    for _ in 0..50 {
        add(&db);
    }
    let file = wal(&db);
    assert!(file.exists());
    let db = db.reopen(options(SqliteSettings::default()));
    // Opening the database again makes a WAL of its own, empty.
    assert_eq!(frames(&db), 0, "closing emptied the WAL");
    let rows: i64 = db.value("SELECT count(*) FROM blobs");
    assert_eq!(rows, 200);
    db.close();
}

/// Settings under which sqlive checkpoints once the WAL holds 100 pages
/// not yet copied, which some 20 of `add`'s commits pass.
fn checkpointed_at_100_pages() -> SqliteSettings {
    SqliteSettings {
        wal_autocheckpoint: Some(WalAutocheckpoint::Pages(NonZeroU32::new(100).unwrap())),
        ..SqliteSettings::default()
    }
}

fn sqlite_error(code: std::ffi::c_int) -> TestError {
    crate::Error::Sqlite(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None))
}

/// Run `fault` as each checkpoint takes the writer, telling the returned
/// receiver as it does.
fn faulted(
    db: &TestDatabase,
    mut fault: impl FnMut() -> Result<(), TestError> + Send + 'static,
) -> UnboundedReceiver<()> {
    let (tried, tries) = unbounded_channel();
    db.with_database(|database| {
        database.fault_checkpoints(move || {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a test that stopped listening has no use for it"
            )]
            let _ = tried.send(());
            fault()
        })
    });
    tries
}

/// Write past the threshold until `fault` fails the checkpoint. Return the
/// error a live query reports before it ends, which is the error the handle
/// holds for every later call.
async fn checkpointer_failure(
    db: &TestDatabase,
    fault: impl FnMut() -> Result<(), TestError> + Send + 'static,
) -> TestError {
    let mut tries = faulted(db, fault);
    let mut query = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, i64>(0))?)
    });
    checkpointed(db, &mut tries).await;
    let QueryError(failure) = tokio::time::timeout(HANG_GUARD, async {
        loop {
            match query.next_inline().await {
                Some(Ok(_)) => (),
                Some(Err(error)) => return error,
                None => panic!("the query ended without the failure"),
            }
        }
    })
    .await
    .expect("the live query reports the failure");
    assert!(query.next_inline().await.is_none(), "the live query ends after the failure");
    let held = db.handle().error::<Infallible>().expect("the handle failed");
    assert_eq!(held.to_string(), failure.to_string(), "the handle holds the live query's failure");
    failure
}

/// A panic in a checkpoint is a bug in sqlive. The database can't stay open
/// with no checkpointer while its WAL grows without bound, so the handle
/// fails with the bug: the bug sink receives it once, every live query and
/// the handle's error after it hold `HandleFailed` carrying it, and the
/// process keeps running.
/// Closing the handle and opening the database again starts a checkpointer
/// that checkpoints again (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_checkpoint_fails_the_handle_with_its_bug_and_a_reopen_checkpoints_again() {
    let db = blobs(checkpointed_at_100_pages());
    let failure = checkpointer_failure(&db, || panic!("the checkpoint broke")).await;
    let crate::Error::HandleFailed(bug) = &failure else {
        panic!("{failure:?}");
    };
    assert_eq!(bug.operation, "sqlive WAL checkpoint");
    assert_eq!(bug.message, "the checkpoint broke");
    assert!(matches!(
        db.handle().error::<Infallible>(),
        Some(crate::Error::HandleFailed(later)) if later == *bug
    ));
    assert_eq!(db.take_bugs(), std::slice::from_ref(bug), "the sink hears the bug once");

    let db = db.reopen(options(checkpointed_at_100_pages()));
    let mut restarts = db.with_database(|database| database.report_checkpoints());
    checkpointed(&db, &mut restarts).await;
    add(&db);
    db.close();
}

/// When SQLite fails a checkpoint for a reason other than a busy or locked
/// database, the checkpointer stops too, and fails the handle with SQLite's
/// cause (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_checkpoint_fails_the_handle_with_its_cause() {
    let db = blobs(checkpointed_at_100_pages());
    let failure =
        checkpointer_failure(&db, || Err(sqlite_error(rusqlite::ffi::SQLITE_IOERR))).await;
    let crate::Error::Host(handle) = &failure else {
        panic!("{failure:?}");
    };
    let crate::EngineFailure::Checkpoint(cause) = handle.engine() else {
        panic!("{failure:?}");
    };
    assert!(
        matches!(
            cause,
            crate::Error::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == rusqlite::ErrorCode::SystemIoFailure
        ),
        "{cause:?}"
    );
    db.close();
}

/// A database busy or locked past the busy timeout is no failure: the
/// checkpointer tries again at the next commit's wake, and the handle keeps
/// serving calls (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_checkpoint_is_tried_again() {
    let db = blobs(checkpointed_at_100_pages());
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
    let one: i64 = db.read_app(|sql| Ok(sql.query_row("SELECT 1", [], |r| r.get(0))?)).unwrap();
    assert_eq!(one, 1);
    db.close();
}

/// The state of the connection that holds SQLite's checkpoint lock, which
/// its busy handler, its thread and the test share through `HOLDER` and wait
/// on through `HOLDER_MOVED`.
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

/// Change the holder's state, and wake every thread that waits on it.
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
/// releases it. The checkpoint's thread returns SQLite's busy column. A
/// checkpoint that finds sqlive's own checkpointer holding the lock gives up
/// at once, so this function then starts another.
async fn hold_the_checkpoint_lock(db: &TestDatabase) -> std::thread::JoinHandle<i64> {
    loop {
        move_holder(|holder| holder.finished = false);
        let path = db.path().to_owned();
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
/// of the WAL: SQLite reports busy with no frames. SQLite reports a busy
/// database, not a failure, so sqlive's checkpointer tries again at the next
/// wake, and the handle keeps serving calls (§4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_another_connection_holds_off_is_tried_again() {
    let db = blobs(checkpointed_at_100_pages());
    let mut restarts = db.with_database(|database| database.report_checkpoints());
    // A reader whose snapshot keeps every later frame in the WAL, so the WAL
    // stays past the threshold however often sqlive checkpoints.
    let reader = rusqlite::Connection::open(db.path()).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader.query_row("SELECT count(*) FROM blobs", [], |r| r.get(0)).unwrap();
    for _ in 0..40 {
        add(&db);
    }
    let holder = hold_the_checkpoint_lock(&db).await;

    let checkpoint = db.host(|writer| writer.wal_checkpoint(CheckpointMode::Passive)).unwrap();
    assert_eq!(checkpoint, WalCheckpoint::NotStarted);
    // The WAL is past the threshold, so `checkpoint_now` reads the WAL and
    // then tries to copy it, and the copy finds the lock held.
    db.with_database(|database| database.checkpoint_now()).unwrap();

    move_holder(|holder| holder.released = true);
    assert_eq!(holder.join().unwrap(), 1, "the other checkpoint gave up waiting for the reader");
    reader.execute_batch("COMMIT").unwrap();
    drop(reader);
    let restarted = checkpointed(&db, &mut restarts).await;
    assert!(matches!(restarted, WalCheckpoint::Ran { busy: false, .. }), "{restarted:?}");
    add(&db);
    assert!(frames(&db) < THRESHOLD, "sqlive's checkpoints go on");
    db.close();
}
