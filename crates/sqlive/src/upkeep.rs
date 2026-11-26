//! sqlive's upkeep of a database, off the commit path (SPEC.md section 10).
//! Each kind runs on a thread of the database's own, woken by the writer's
//! commits, on a connection of its own: checkpointing a database's WAL
//! (wal_checkpoints.rs), and gathering the query planner's statistics
//! (planner_statistics.rs).
//!
//! The thread starts as the database opens, and the database stops and joins
//! it as the database closes, so the thread lives exactly as long as the
//! database: no runtime shutting down while the database stays open can stop
//! it.
//!
//! A run that finds the database busy or locked waited out its busy
//! timeout; the next wake tries again. Any other failure stops the upkeep
//! for good, so the thread fails the handle with it: every later call and
//! live query reports it until the host opens the database again, rather
//! than the database staying open without its upkeep. A panic in a run is a
//! bug in sqlive: the thread hands it to the handle's bug sink and fails the
//! handle the same way, as `crate::Error::HandleFailed` carrying it, so every
//! way the thread ends reaches the handle or the bug sink.

use crate::authorization::{Policy, PolicyError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// One kind of upkeep, run at each wake of its thread.
pub(crate) trait Upkeep: Send + Sync + 'static {
    /// The policy of the connection the upkeep runs on.
    type Policy: Policy;

    /// The thread's name.
    const THREAD: &'static str;

    /// One wake's upkeep. A wake may come with no commit.
    fn run(&self) -> Result<(), PolicyError<Self::Policy>>;

    /// What the handle fails with once a run fails with `error` other than
    /// on a busy or locked database.
    fn failure(
        error: PolicyError<Self::Policy>,
    ) -> crate::EngineFailure<<Self::Policy as Policy>::Failure, <Self::Policy as Policy>::Refusal>;
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
    /// `halt` sets this as the database closes, then wakes the thread, which
    /// reads it and stops.
    closing: AtomicBool,
    upkeep: U,
}

/// One of the database's own threads, which runs `U` at each wake. The thread
/// stops as the database closes, before the writer.
pub(crate) struct UpkeepThread<U: Upkeep> {
    shared: Arc<Shared<U>>,
    /// `halt` takes this as the thread stops: when the database closes, or
    /// when the `UpkeepThread` drops because the open failed.
    thread: Option<std::thread::JoinHandle<()>>,
}

impl<U: Upkeep> UpkeepThread<U> {
    /// Run `upkeep` at each wake from now on, failing `handle` once a run
    /// fails.
    pub(crate) fn spawn(
        upkeep: U,
        handle: crate::Handle<<U::Policy as Policy>::Failure>,
    ) -> Result<Self, PolicyError<U::Policy>> {
        #[cfg(any(test, feature = "test-utils"))]
        if handle.upkeep_thread_refused() {
            return Err(crate::Error::UpkeepThread(std::io::ErrorKind::WouldBlock.into()));
        }
        let shared = Arc::new(Shared { closing: AtomicBool::new(false), upkeep });
        let serving = Serving { shared: shared.clone(), handle };
        let thread = std::thread::Builder::new()
            .name(U::THREAD.into())
            .spawn(move || serving.serve())
            .map_err(crate::Error::UpkeepThread)?;
        Ok(Self { shared, thread: Some(thread) })
    }

    /// What wakes the thread.
    pub(crate) fn wake(&self) -> Wake {
        Wake { thread: self.running().thread().clone() }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn upkeep(&self) -> &U {
        &self.shared.upkeep
    }

    /// Stop, after any run in progress, and hand the upkeep back.
    pub(crate) fn join(mut self) -> U {
        self.halt();
        let shared = self.shared.clone();
        drop(self);
        // A panic on the thread is caught (`serve`), so it always returned,
        // and let go of its part.
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
        // A panic on the thread is caught, so it always returns.
        thread.join().expect("the upkeep's thread returns");
    }
}

/// A database whose open failed after its upkeep started stops it too; its
/// connection closes as the last of the upkeep goes.
impl<U: Upkeep> Drop for UpkeepThread<U> {
    fn drop(&mut self) {
        self.halt();
    }
}

/// The upkeep's thread: the upkeep it runs, and the handle it fails once it
/// can't.
struct Serving<U: Upkeep> {
    shared: Arc<Shared<U>>,
    handle: crate::Handle<<U::Policy as Policy>::Failure>,
}

impl<U: Upkeep> Serving<U> {
    /// Run at each wake until the database closes or a run fails. A panic is
    /// a bug in sqlive: rather than leave the database open without its
    /// upkeep, the thread fails the handle with it and hands it to the
    /// handle's bug sink (E15).
    fn serve(self) {
        let served = panic_as_error::caught_sync(U::THREAD, || loop {
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
                    self.handle.fail_engine(U::failure(error));
                    return;
                }
            }
        });
        if let Err(bug) = served {
            tracing::error!(%bug, thread = U::THREAD, "sqlive's upkeep panicked, a bug; failing the handle");
            self.handle.fail_with_bug(bug);
        }
    }
}

/// Whether `error` is SQLite finding the database busy or locked: another
/// connection held on past the busy timeout.
fn busy<F, R>(error: &crate::Error<F, R>) -> bool {
    matches!(
        error,
        crate::Error::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}
