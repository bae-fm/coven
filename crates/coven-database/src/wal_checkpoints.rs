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

use crate::sqlite::DatabaseConnection;
use crate::{CheckpointMode, DbError};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
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
}

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
    /// `connection`, a connection of its own to the same database. Coven's
    /// checkpoints read and write none of the app's tables, so it needs none
    /// of the app's functions.
    pub(crate) fn start(
        connection: DatabaseConnection,
        writer: &Arc<Mutex<DatabaseConnection>>,
        threshold: NonZeroU32,
    ) -> Result<Self, DbError> {
        connection.batch(&format!("PRAGMA busy_timeout = {}", READER_WAIT.as_millis()))?;
        let shared = Arc::new(Checkpoints {
            wake: tokio::sync::Notify::new(),
            closing: std::sync::atomic::AtomicBool::new(false),
            connection: Mutex::new(Some(connection)),
        });
        {
            let mut writer = writer.lock().expect("database connection lock poisoned");
            writer.batch("PRAGMA wal_autocheckpoint = 0")?;
            writer.signal_commits(CommitSignal { shared: shared.clone() });
        }
        // The task holds the writer only during a checkpoint, so the store's
        // close can take it back once the checkpointer stops.
        let serving = shared.clone();
        let writer = Arc::downgrade(writer);
        drop(tokio::spawn(async move {
            loop {
                serving.wake.notified().await;
                if serving.closing.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                let running = serving.clone();
                let writer = writer.clone();
                let run = tokio::task::spawn_blocking(move || {
                    run(&running, &writer, threshold.get());
                });
                if run.await.is_err() {
                    return;
                }
            }
        }));
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

/// One wake's checkpoint, on the checkpointer's connection while it is
/// still open. The writer is dropped before the connection is let go, so a
/// close that took the connection finds the writer free.
fn run(shared: &Checkpoints, writer: &std::sync::Weak<Mutex<DatabaseConnection>>, threshold: u32) {
    let connection = shared.connection.lock().expect("checkpointer lock poisoned");
    let Some(connection) = connection.as_ref() else {
        return;
    };
    let Some(writer) = writer.upgrade() else {
        return;
    };
    if let Err(error) = checkpoint(connection, &writer, threshold) {
        // The data stays whole; only the WAL stays as large as it grew.
        // The next commit tries again.
        tracing::error!(?error, "WAL checkpoint failed");
    }
}

/// Checkpoint the WAL once it holds `threshold` frames not yet in the
/// database file. A checkpoint the readers held up leaves the WAL copied but
/// not started over; it is tried again once the WAL grows by the threshold.
fn checkpoint(
    connection: &DatabaseConnection,
    writer: &Arc<Mutex<DatabaseConnection>>,
    threshold: u32,
) -> Result<(), DbError> {
    let held = connection.wal_checkpoint(CheckpointMode::Noop)?;
    if held.log_frames.saturating_sub(held.checkpointed_frames) < threshold {
        return Ok(());
    }
    // Copy what no reader needs while coven's writes go on.
    connection.wal_checkpoint(CheckpointMode::Passive)?;
    // No write of coven's starts meanwhile, so the checkpoint takes SQLite's
    // writer lock at once, copies the few frames committed since, and waits
    // only for readers.
    let restarted = {
        let _writes = writer.lock().expect("database connection lock poisoned");
        connection.wal_checkpoint(CheckpointMode::Restart)?
    };
    tracing::debug!(frames = held.log_frames, busy = restarted.busy, "checkpointed the WAL");
    Ok(())
}

#[cfg(test)]
#[path = "wal_checkpoints_tests.rs"]
mod tests;
