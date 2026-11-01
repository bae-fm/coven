//! Coven's own WAL checkpoints, off the commit path (§4).
//!
//! A checkpoint run inside a commit, as SQLite's automatic one is, makes the
//! commit wait for it, `F_FULLFSYNC` included on macOS and iOS. While a live
//! query keeps a reader's snapshot open nearly all the time, that checkpoint
//! can't finish, so every later commit runs another and the WAL never starts
//! over. So the writer never checkpoints as it commits. Each commit only
//! wakes a task of the store's own, which reads how many frames the WAL
//! holds past what was checkpointed and, past the threshold, checkpoints on a
//! connection of its own: first copying what it can while writes go on, then,
//! holding coven's writer so no write starts, copying the rest and waiting
//! briefly for the readers to leave the WAL, so the next commit writes it
//! from its start.
//!
//! A checkpoint that finds the database busy or locked waited out its busy
//! timeout; the next wake tries again. So does one SQLite couldn't start,
//! reading nothing of the WAL, because another connection was checkpointing
//! it or committing to it right then. Any other failure, or a panic in a
//! checkpoint, stops checkpointing for good, so the task fails the handle
//! with it: every later call and live query reports it until the store is
//! reopened, rather than the store staying open while its WAL grows without
//! bound. A runtime shutting down ends the task quietly, as the store goes
//! with it.

use crate::database::with_locked;
use crate::observation::{CommitObserver, HandleFailure};
use crate::sqlite::DatabaseConnection;
use crate::{CheckpointMode, DbError, WalCheckpoint};
use coven_foundation::task::TaskFailed;
use std::num::NonZeroU32;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// How long the checkpoint that lets the WAL start over waits for readers
/// to leave it, while coven's writes wait: a reader that started before it
/// finishes within this, and later ones read the database file alone. If one
/// still holds on, the WAL starts over once it grows by the threshold again.
const READER_WAIT: Duration = Duration::from_millis(100);

/// Wakes the checkpointer after each of the writer's commits. Waking only
/// leaves a permit, which wakes after many commits coalesce into; it never
/// waits for the checkpointer.
pub(crate) struct CommitSignal {
    shared: Arc<Checkpoints>,
}

struct Checkpoints {
    wake: tokio::sync::Notify,
    closing: std::sync::atomic::AtomicBool,
    /// The checkpointer's own connection, held through each checkpoint and
    /// taken as the store closes.
    connection: Mutex<Option<DatabaseConnection>>,
    /// The writer, held only during a checkpoint, so the store's close can
    /// take it back once the checkpointer stops.
    writer: Weak<Mutex<DatabaseConnection>>,
    /// The frames not yet checkpointed past which a wake checkpoints.
    threshold: NonZeroU32,
    /// A test's fault, run as each checkpoint's last step begins.
    #[cfg(test)]
    fault: Mutex<Option<Fault>>,
}

#[cfg(test)]
type Fault = Box<dyn FnMut() -> Result<(), DbError> + Send>;

impl CommitSignal {
    pub(crate) fn committed(&self) {
        self.shared.wake.notify_one();
    }
}

impl Checkpoints {
    fn close(&self) {
        self.closing.store(true, std::sync::atomic::Ordering::Release);
        self.wake.notify_one();
    }
}

/// A database's checkpointer: a task woken by the writer's commits, which
/// runs each checkpoint as blocking work on its own connection. It holds no
/// thread while it waits, and stops as the store closes, before the writer.
pub(crate) struct WalCheckpointer {
    shared: Arc<Checkpoints>,
}

impl WalCheckpointer {
    /// Take checkpointing `writer`'s database over from its commits, once
    /// the WAL holds `threshold` frames not yet checkpointed, on
    /// `connection`, a connection of its own to the same database, failing
    /// `handle` if checkpointing fails. Coven's checkpoints read and write
    /// none of the app's tables, so it needs none of the app's functions.
    pub(crate) fn start(
        connection: DatabaseConnection,
        writer: &Arc<Mutex<DatabaseConnection>>,
        threshold: NonZeroU32,
        handle: CommitObserver,
    ) -> Result<Self, DbError> {
        connection.batch(&format!("PRAGMA busy_timeout = {}", READER_WAIT.as_millis()))?;
        let shared = Arc::new(Checkpoints {
            wake: tokio::sync::Notify::new(),
            closing: std::sync::atomic::AtomicBool::new(false),
            connection: Mutex::new(Some(connection)),
            writer: Arc::downgrade(writer),
            threshold,
            #[cfg(test)]
            fault: Mutex::new(None),
        });
        {
            let mut writer = writer.lock().expect("database connection lock poisoned");
            writer.batch("PRAGMA wal_autocheckpoint = 0")?;
            writer.signal_commits(CommitSignal { shared: shared.clone() });
        }
        drop(tokio::spawn(Serving { shared: shared.clone(), handle }.serve()));
        Ok(Self { shared })
    }

    /// Stop, after any checkpoint in progress, and close the connection.
    pub(crate) fn stop(self) -> Result<(), DbError> {
        self.shared.close();
        let connection = self.shared.connection.lock().expect("checkpointer lock poisoned").take();
        connection.map_or(Ok(()), DatabaseConnection::close)
    }
}

/// A store whose open failed after its checkpointer started stops it too;
/// its connection closes as the last of the checkpointer goes.
impl Drop for WalCheckpointer {
    fn drop(&mut self) {
        self.shared.close();
    }
}

/// The checkpointer's task: the database it checkpoints, and the handle it
/// fails once it can't.
struct Serving {
    shared: Arc<Checkpoints>,
    handle: CommitObserver,
}

impl Serving {
    async fn serve(self) {
        loop {
            self.shared.wake.notified().await;
            if self.shared.closing.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            if self.checkpoint().await.is_break() {
                return;
            }
        }
    }

    /// One wake's checkpoint, on the blocking pool. A database busy or
    /// locked past the busy timeout is tried again at the next wake. Any
    /// other failure, or a panic, a bug, fails the handle and stops
    /// checkpointing; a runtime shutting down stops it without one.
    async fn checkpoint(&self) -> ControlFlow<()> {
        let shared = self.shared.clone();
        let ran = coven_foundation::task::blocking("WAL checkpoint", move || shared.run()).await;
        let failure = match ran {
            Ok(Ok(())) => return ControlFlow::Continue(()),
            Ok(Err(error)) if busy(&error) => {
                // The data stays whole; only the WAL stays as large as it
                // grew until the next commit's wake.
                tracing::debug!(?error, "WAL checkpoint found the database busy");
                return ControlFlow::Continue(());
            }
            Ok(Err(error)) => HandleFailure::Checkpoint(Arc::new(error)),
            Err(TaskFailed::Panicked { message, .. }) => HandleFailure::CheckpointPanicked(message),
            // Not a bug: the process is ending the runtime, and the store
            // with it.
            Err(TaskFailed::Shutdown(_)) => return ControlFlow::Break(()),
        };
        self.handle.fail(failure);
        ControlFlow::Break(())
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

impl Checkpoints {
    /// One wake's checkpoint, on the checkpointer's connection while it is
    /// still open, once the WAL holds `threshold` frames not yet in the
    /// database file. A checkpoint the readers held up leaves the WAL copied
    /// but not started over; it is tried again once the WAL grows by the
    /// threshold.
    ///
    /// The writer is dropped before the connection is let go, so a close
    /// that took the connection finds the writer free. A panic lets go of
    /// both unpoisoned: the handle it fails still closes them, and its later
    /// calls report the failure rather than a poisoned lock.
    fn run(&self) -> Result<(), DbError> {
        with_locked(&self.connection, |connection| {
            let Some(connection) = connection.as_ref() else {
                return Ok(());
            };
            let Some(writer) = self.writer.upgrade() else {
                return Ok(());
            };
            // A WAL SQLite couldn't read now is read again at the next wake.
            let WalCheckpoint::Ran { log_frames, checkpointed_frames, .. } =
                connection.wal_checkpoint(CheckpointMode::Noop)?
            else {
                return Ok(());
            };
            if log_frames.saturating_sub(checkpointed_frames) < self.threshold.get() {
                return Ok(());
            }
            // Copy what no reader needs while coven's writes go on.
            connection.wal_checkpoint(CheckpointMode::Passive)?;
            // No write of coven's starts meanwhile, so the checkpoint takes
            // SQLite's writer lock at once, copies the few frames committed
            // since, and waits only for readers.
            let restarted = with_locked(&writer, |_writes| {
                #[cfg(test)]
                self.fault()?;
                connection.wal_checkpoint(CheckpointMode::Restart)
            })?;
            tracing::debug!(frames = log_frames, ?restarted, "checkpointed the WAL");
            Ok(())
        })
    }

    /// Run the fault a test set, as the checkpoint's last step begins.
    #[cfg(test)]
    fn fault(&self) -> Result<(), DbError> {
        self.fault.lock().expect("checkpoint fault lock poisoned").as_mut().map_or(Ok(()), |f| f())
    }
}

#[cfg(test)]
#[path = "wal_checkpoints_tests.rs"]
mod tests;
