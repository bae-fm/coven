//! Coven's upkeep of a store's databases, off the commit path (§4). Each
//! kind runs on a thread of the store's own, woken by the writer's commits,
//! on a connection of its own: checkpointing a database's WAL
//! (wal_checkpoints.rs), and gathering the query planner's statistics
//! (planner_statistics.rs).
//!
//! The thread starts as the store opens and is stopped and joined as it
//! closes, so it lives exactly as long as the store: no runtime shutting
//! down while the store stays open can stop it.
//!
//! A run that finds the database busy or locked waited out its busy
//! timeout; the next wake tries again. Any other failure stops the upkeep
//! for good, so the thread fails the handle with it: every later call and
//! live query reports it until the store is reopened, rather than the store
//! staying open without its upkeep. A panic in a run is a bug in coven, and
//! ends the process.

use crate::observation::{CommitObserver, HandleFailure};
use crate::DbError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// One kind of upkeep, run at each wake of its thread.
pub(crate) trait Upkeep: Send + Sync + 'static {
    /// The thread's name.
    const THREAD: &'static str;

    /// One wake's upkeep. A wake may come with no commit.
    fn run(&self) -> Result<(), DbError>;

    /// What the handle fails with once a run fails other than on a busy or
    /// locked database.
    fn failure(error: Arc<DbError>) -> HandleFailure;
}

/// Wakes an upkeep thread. Waking only leaves the thread's permit, which
/// wakes after many commits coalesce into; it never waits for the upkeep.
pub(crate) struct Wake {
    thread: std::thread::Thread,
}

impl Wake {
    pub(crate) fn wake(&self) {
        self.thread.unpark();
    }
}

struct Shared<U> {
    /// Set as the store closes, before the thread is woken to see it.
    closing: AtomicBool,
    upkeep: U,
}

/// A thread of the store's own running `U` at each wake. It stops as the
/// store closes, before the writer.
pub(crate) struct UpkeepThread<U: Upkeep> {
    shared: Arc<Shared<U>>,
    /// Taken as the thread stops: when the store closes, or as it drops
    /// when the open failed.
    thread: Option<std::thread::JoinHandle<()>>,
}

impl<U: Upkeep> UpkeepThread<U> {
    /// Run `upkeep` at each wake from now on, failing `handle` once a run
    /// fails.
    pub(crate) fn spawn(upkeep: U, handle: CommitObserver) -> Result<Self, DbError> {
        let shared = Arc::new(Shared { closing: AtomicBool::new(false), upkeep });
        let serving = Serving { shared: shared.clone(), handle };
        let thread = std::thread::Builder::new()
            .name(U::THREAD.into())
            .spawn(move || serving.serve())
            .map_err(DbError::UpkeepThread)?;
        Ok(Self { shared, thread: Some(thread) })
    }

    /// What wakes the thread.
    pub(crate) fn wake(&self) -> Wake {
        Wake { thread: self.running().thread().clone() }
    }

    #[cfg(test)]
    pub(crate) fn upkeep(&self) -> &U {
        &self.shared.upkeep
    }

    /// Stop, after any run in progress, and hand the upkeep back.
    pub(crate) fn join(mut self) -> U {
        self.halt();
        let shared = self.shared.clone();
        drop(self);
        // A panic on the thread ends the process (`serve`), so it always
        // returned, and let go of its part.
        Arc::into_inner(shared)
            .expect("only the upkeep holds its state once its thread ends")
            .upkeep
    }

    fn running(&self) -> &std::thread::JoinHandle<()> {
        self.thread.as_ref().expect("the upkeep's thread runs until it stops")
    }

    /// Tell the thread to stop, and wait for it to end, after any run in
    /// progress.
    fn halt(&mut self) {
        let Some(thread) = self.thread.take() else {
            return;
        };
        self.shared.closing.store(true, Ordering::Release);
        thread.thread().unpark();
        // A panic on the thread ends the process, so it always returns.
        thread.join().expect("the upkeep's thread returns");
    }
}

/// A store whose open failed after its upkeep started stops it too; its
/// connection closes as the last of the upkeep goes.
impl<U: Upkeep> Drop for UpkeepThread<U> {
    fn drop(&mut self) {
        self.halt();
    }
}

/// The upkeep's thread: the upkeep it runs, and the handle it fails once it
/// can't.
struct Serving<U> {
    shared: Arc<Shared<U>>,
    handle: CommitObserver,
}

impl<U: Upkeep> Serving<U> {
    /// Run at each wake until the store closes or a run fails. A panic is a
    /// bug in coven: rather than leave the store open without its upkeep,
    /// it ends the process, whose crash report carries the panic.
    fn serve(self) {
        let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
            std::thread::park();
            if self.shared.closing.load(Ordering::Acquire) {
                return;
            }
            match self.shared.upkeep.run() {
                Ok(()) => (),
                Err(error) if busy(&error) => {
                    // Nothing is wrong; the upkeep waits for the next wake.
                    tracing::debug!(?error, thread = U::THREAD, "upkeep found the database busy");
                }
                Err(error) => {
                    self.handle.fail(U::failure(Arc::new(error)));
                    return;
                }
            }
        }));
        if let Err(panic) = served {
            let message = coven_foundation::task::panic_message(panic.as_ref());
            tracing::error!(%message, thread = U::THREAD, "coven's upkeep panicked, a bug; ending the process");
            std::process::abort()
        }
    }
}

/// Whether `error` is SQLite finding the database busy or locked: another
/// connection held on past the busy timeout.
fn busy(error: &DbError) -> bool {
    matches!(
        error,
        DbError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}
