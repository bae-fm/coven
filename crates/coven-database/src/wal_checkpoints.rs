//! Coven's own WAL checkpoints, off the commit path (§4).
//!
//! A checkpoint run inside a commit, as SQLite's automatic one is, makes the
//! commit wait for it, `F_FULLFSYNC` included on macOS and iOS. While a live
//! query keeps a reader's snapshot open nearly all the time, that checkpoint
//! can't finish, so every later commit runs another and the WAL never starts
//! over. So the writer never checkpoints as it commits. Each commit only
//! wakes a thread of the store's own, which reads how many frames the WAL
//! holds past what was checkpointed and, past the threshold, checkpoints on a
//! connection of its own: first copying what it can while writes go on, then,
//! holding coven's writer so no write starts, copying the rest and waiting
//! briefly for the readers to leave the WAL, so the next commit writes it
//! from its start.
//!
//! The thread starts as the store opens and is stopped and joined as it
//! closes, so it lives exactly as long as the store: no runtime shutting
//! down while the store stays open can stop it.
//!
//! A checkpoint that finds the database busy or locked waited out its busy
//! timeout; the next wake tries again. So does one SQLite couldn't start,
//! reading nothing of the WAL, because another connection was checkpointing
//! it or committing to it right then. Any other failure stops checkpointing
//! for good, so the thread fails the handle with it: every later call and
//! live query reports it until the store is reopened, rather than the store
//! staying open while its WAL grows without bound. A panic in a checkpoint
//! is a bug in coven, and ends the process.

use crate::database::with_locked;
use crate::observation::{CommitObserver, HandleFailure};
use crate::sqlite::DatabaseConnection;
use crate::{CheckpointMode, DbError, WalCheckpoint};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// How long the checkpoint that lets the WAL start over waits for readers
/// to leave it, while coven's writes wait: a reader that started before it
/// finishes within this, and later ones read the database file alone. If one
/// still holds on, the WAL starts over once it grows by the threshold again.
const READER_WAIT: Duration = Duration::from_millis(100);

/// Wakes the checkpointer after each of the writer's commits. Waking only
/// leaves the thread's permit, which wakes after many commits coalesce
/// into; it never waits for the checkpointer.
pub(crate) struct CommitSignal {
    thread: std::thread::Thread,
}

impl CommitSignal {
    pub(crate) fn committed(&self) {
        self.thread.unpark();
    }
}

struct Checkpoints {
    /// Set as the store closes, before the thread is woken to see it.
    closing: AtomicBool,
    /// The checkpointer's own connection, held through each checkpoint.
    connection: Mutex<DatabaseConnection>,
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

/// A database's checkpointer: a thread of the store's own, woken by the
/// writer's commits, which runs each checkpoint on its own connection. It
/// stops as the store closes, before the writer.
pub(crate) struct WalCheckpointer {
    shared: Arc<Checkpoints>,
    /// Taken as the checkpointer stops: when the store closes, or as it
    /// drops when the open failed.
    thread: Option<std::thread::JoinHandle<()>>,
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
            closing: AtomicBool::new(false),
            connection: Mutex::new(connection),
            writer: Arc::downgrade(writer),
            threshold,
            #[cfg(test)]
            fault: Mutex::new(None),
        });
        let serving = Serving { shared: shared.clone(), handle };
        let thread = std::thread::Builder::new()
            .name("coven WAL checkpoint".into())
            .spawn(move || serving.serve())
            .map_err(DbError::CheckpointerThread)?;
        // Made before the writer's steps, so a step that fails stops the
        // thread as this drops.
        let checkpointer = Self { shared, thread: Some(thread) };
        let signal = CommitSignal { thread: checkpointer.running().thread().clone() };
        {
            let mut writer = writer.lock().expect("database connection lock poisoned");
            writer.batch("PRAGMA wal_autocheckpoint = 0")?;
            writer.signal_commits(signal);
        }
        Ok(checkpointer)
    }

    /// Stop, after any checkpoint in progress, and close the connection.
    pub(crate) fn stop(mut self) -> Result<(), DbError> {
        self.halt();
        let shared = self.shared.clone();
        drop(self);
        Arc::into_inner(shared)
            .expect("only the checkpointer holds its state once its thread ends")
            .connection
            .into_inner()
            .expect("checkpointer lock poisoned")
            .close()
    }

    fn running(&self) -> &std::thread::JoinHandle<()> {
        self.thread.as_ref().expect("the checkpointer's thread runs until it stops")
    }

    /// Tell the thread to stop, and wait for it to end, after any checkpoint
    /// in progress.
    fn halt(&mut self) {
        let Some(thread) = self.thread.take() else {
            return;
        };
        self.shared.closing.store(true, Ordering::Release);
        thread.thread().unpark();
        // A panic on the thread ends the process, so it always returns.
        thread.join().expect("the checkpointer's thread returns");
    }
}

/// A store whose open failed after its checkpointer started stops it too;
/// its connection closes as the last of the checkpointer goes.
impl Drop for WalCheckpointer {
    fn drop(&mut self) {
        self.halt();
    }
}

/// The checkpointer's thread: the database it checkpoints, and the handle it
/// fails once it can't.
struct Serving {
    shared: Arc<Checkpoints>,
    handle: CommitObserver,
}

impl Serving {
    /// Checkpoint at each wake until the store closes or a checkpoint
    /// fails. The thread's permit may also wake it with no commit, and that
    /// wake only reads how large the WAL is. A panic is a bug in coven:
    /// rather than leave the store open with no checkpointer while its WAL
    /// grows without bound, it ends the process, whose crash report carries
    /// the panic.
    fn serve(self) {
        let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
            std::thread::park();
            if self.shared.closing.load(Ordering::Acquire) {
                return;
            }
            if let Err(error) = self.checkpoint() {
                self.handle.fail(HandleFailure::Checkpoint(Arc::new(error)));
                return;
            }
        }));
        if let Err(panic) = served {
            let message = coven_foundation::task::panic_message(panic.as_ref());
            tracing::error!(%message, "coven's WAL checkpoint panicked, a bug; ending the process");
            std::process::abort()
        }
    }

    /// One wake's checkpoint. A database busy or locked past the busy
    /// timeout is tried again at the next wake; any other failure stops
    /// checkpointing.
    fn checkpoint(&self) -> Result<(), DbError> {
        match self.shared.run() {
            Err(error) if busy(&error) => {
                // The data stays whole; only the WAL stays as large as it
                // grew until the next commit's wake.
                tracing::debug!(?error, "WAL checkpoint found the database busy");
                Ok(())
            }
            ran => ran,
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

impl Checkpoints {
    /// One wake's checkpoint, on the checkpointer's connection, once the WAL
    /// holds `threshold` frames not yet in the database file. A checkpoint
    /// the readers held up leaves the WAL copied but not started over; it is
    /// tried again once the WAL grows by the threshold. A store whose open
    /// failed may let its writer go before its checkpointer stops; there is
    /// then nothing to checkpoint.
    fn run(&self) -> Result<(), DbError> {
        with_locked(&self.connection, |connection| {
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
