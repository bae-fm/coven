//! Coven's own WAL checkpoints, off the commit path (§4).
//!
//! A checkpoint run inside a commit, as SQLite's automatic one is, makes the
//! commit wait for it, `F_FULLFSYNC` included on macOS and iOS. While a live
//! query keeps a reader's snapshot open nearly all the time, that checkpoint
//! can't finish, so every later commit runs another and the WAL never starts
//! over. So the writer never checkpoints as it commits. Each commit only
//! wakes a thread of the store's own, which reads how many frames the WAL
//! holds past what was checkpointed and, past the threshold, checkpoints on a
//! connection of its own: first copying what it can while writes go on and
//! syncing the copy, again for the frames committed meanwhile while each
//! pass leaves fewer, then, holding coven's writer so no write starts,
//! copying the rest, syncing it and waiting briefly for the readers to leave
//! the WAL, so the next commit writes it from its start.
//!
//! The thread is one of the store's upkeep (upkeep.rs). A checkpoint
//! SQLite couldn't start, reading nothing of the WAL, because another
//! connection was checkpointing it or committing to it right then, is tried
//! again at the next wake, as one that finds the database busy is. Any
//! other failure fails the handle rather than leave the store open while
//! its WAL grows without bound.

use crate::database::with_locked;
use crate::observation::{CommitObserver, HandleFailure};
use crate::sqlite::DatabaseConnection;
use crate::upkeep::{Upkeep, UpkeepThread};
use crate::{CheckpointMode, DbError, WalCheckpoint};
use coven_foundation::clock::ClockRef;
use coven_foundation::locks::lock_unpoisoned;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// How long the checkpoint that lets the WAL start over waits for readers
/// to leave it, while coven's writes wait: a reader that started before it
/// finishes within this, and later ones read the database file alone. If one
/// still holds on, the WAL starts over once it grows by the threshold again.
const READER_WAIT: Duration = Duration::from_millis(100);

pub(crate) struct Checkpoints {
    /// The checkpointer's own connection, held through each checkpoint.
    connection: Mutex<DatabaseConnection>,
    /// The writer, held only during a checkpoint, so the store's close can
    /// take it back once the checkpointer stops.
    writer: Weak<Mutex<DatabaseConnection>>,
    /// The frames not yet checkpointed past which a wake checkpoints.
    threshold: NonZeroU32,
    /// Times how long each checkpoint holds the writer.
    clock: ClockRef,
    /// A test's fault, run as each checkpoint's last step begins.
    #[cfg(test)]
    fault: Mutex<Option<Fault>>,
    /// A test's step, run as each pass's copy returns, before its sync.
    #[cfg(test)]
    copied: Mutex<Option<Box<dyn FnMut() + Send>>>,
    /// Where a test hears what each checkpoint's last step did, told while
    /// the checkpoint still holds the writer.
    #[cfg(test)]
    restarts: Mutex<Option<tokio::sync::mpsc::UnboundedSender<WalCheckpoint>>>,
}

#[cfg(test)]
type Fault = Box<dyn FnMut() -> Result<(), DbError> + Send>;

/// A database's checkpointer: a thread of the store's own, woken by the
/// writer's commits, which runs each checkpoint on its own connection. It
/// stops as the store closes, before the writer.
pub(crate) type WalCheckpointer = UpkeepThread<Checkpoints>;

impl UpkeepThread<Checkpoints> {
    /// Take checkpointing `writer`'s database over from its commits, once
    /// the WAL holds `threshold` frames not yet checkpointed, on
    /// `connection`, a connection of its own to the same database, failing
    /// `handle` if checkpointing fails. Coven's checkpoints read and write
    /// none of the app's tables, so it needs none of the app's functions.
    pub(crate) fn start(
        connection: DatabaseConnection,
        writer: &Arc<Mutex<DatabaseConnection>>,
        threshold: NonZeroU32,
        clock: ClockRef,
        handle: CommitObserver,
    ) -> Result<Self, DbError> {
        connection.batch(&format!("PRAGMA busy_timeout = {}", READER_WAIT.as_millis()))?;
        // Made before the writer's steps, so a step that fails stops the
        // thread as this drops.
        let checkpointer = Self::spawn(
            Checkpoints {
                connection: Mutex::new(connection),
                writer: Arc::downgrade(writer),
                threshold,
                clock,
                #[cfg(test)]
                fault: Mutex::new(None),
                #[cfg(test)]
                copied: Mutex::new(None),
                #[cfg(test)]
                restarts: Mutex::new(None),
            },
            handle,
        )?;
        let wake = checkpointer.wake();
        {
            let mut writer = lock_unpoisoned(writer);
            writer.batch("PRAGMA wal_autocheckpoint = 0")?;
            writer.signal_commits(wake);
        }
        Ok(checkpointer)
    }

    /// Stop, after any checkpoint in progress, and close the connection.
    pub(crate) fn stop(self) -> Result<(), DbError> {
        coven_foundation::locks::into_unpoisoned(self.join().connection).close()
    }
}

impl Upkeep for Checkpoints {
    const THREAD: &'static str = "coven WAL checkpoint";

    fn run(&self) -> Result<(), DbError> {
        self.checkpoint()
    }

    fn failure(error: Arc<DbError>) -> HandleFailure {
        HandleFailure::Checkpoint(error)
    }
}

impl Checkpoints {
    /// One wake's checkpoint, on the checkpointer's connection, once the WAL
    /// holds `threshold` frames not yet in the database file. A checkpoint
    /// the readers held up leaves the WAL copied but not started over; it is
    /// tried again once the WAL grows by the threshold. A store whose open
    /// failed may let its writer go before its checkpointer stops; there is
    /// then nothing to checkpoint.
    fn checkpoint(&self) -> Result<(), DbError> {
        with_locked(&self.connection, |connection| {
            let Some(writer) = self.writer.upgrade() else {
                return Ok(());
            };
            // A WAL SQLite couldn't read now is read again at the next wake.
            let Some(uncopied) = uncopied_frames(connection)? else {
                return Ok(());
            };
            if uncopied < self.threshold.get() {
                return Ok(());
            }
            // Copy what no reader needs while coven's writes go on, and sync
            // it. SQLite syncs the database file only when a checkpoint
            // copies the whole WAL, and a commit landing during the copy
            // leaves it short; without a sync of its own, the checkpoint that
            // holds the writer would sync every page copied here. The frames
            // committed during a pass get a pass of their own while each pass
            // leaves fewer, so the checkpoint holding the writer copies and
            // syncs only the frames committed during the last pass and since,
            // none when no commit landed.
            let (mut passes, mut left) = (0, uncopied);
            loop {
                // A copy SQLite couldn't start now, as another connection was
                // checkpointing the WAL or committing to it, is tried again at
                // the next wake rather than made whole while writes wait.
                if let WalCheckpoint::NotStarted =
                    connection.wal_checkpoint(CheckpointMode::Passive)?
                {
                    return Ok(());
                }
                #[cfg(test)]
                self.copied();
                connection.sync_database_file()?;
                passes += 1;
                match uncopied_frames(connection)? {
                    Some(now) if now > 0 && now < left => left = now,
                    _ => break,
                }
            }
            // No write of coven's starts meanwhile, so the checkpoint takes
            // SQLite's writer lock at once, copies and syncs the frames
            // committed since the last pass, and waits only for readers.
            let (restarted, held) = with_locked(&writer, |_writes| {
                let held = self.clock.elapsed();
                #[cfg(test)]
                self.fault()?;
                let restarted = connection.wal_checkpoint(CheckpointMode::Restart)?;
                #[cfg(test)]
                self.report(restarted);
                Ok::<_, DbError>((restarted, self.clock.elapsed() - held))
            })?;
            tracing::debug!(frames = uncopied, passes, ?restarted, ?held, "checkpointed the WAL");
            Ok(())
        })
    }

    /// Run the fault a test set, as the checkpoint's last step begins.
    #[cfg(test)]
    fn fault(&self) -> Result<(), DbError> {
        lock_unpoisoned(&self.fault).as_mut().map_or(Ok(()), |f| f())
    }

    /// Run the step a test set, as a pass's copy returns.
    #[cfg(test)]
    fn copied(&self) {
        if let Some(step) = lock_unpoisoned(&self.copied).as_mut() {
            step();
        }
    }

    /// Tell a test that asked what the checkpoint's last step did.
    #[cfg(test)]
    fn report(&self, restarted: WalCheckpoint) {
        if let Some(restarts) = lock_unpoisoned(&self.restarts).as_ref() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a test that stopped listening has no use for it"
            )]
            let _ = restarts.send(restarted);
        }
    }
}

/// The frames the WAL holds that the database file doesn't, or None when
/// SQLite couldn't read the WAL now: another connection was checkpointing it
/// or committing to it.
fn uncopied_frames(connection: &DatabaseConnection) -> Result<Option<u32>, DbError> {
    Ok(match connection.wal_checkpoint(CheckpointMode::Noop)? {
        WalCheckpoint::Ran { log_frames, checkpointed_frames, .. } => {
            Some(log_frames.saturating_sub(checkpointed_frames))
        }
        WalCheckpoint::NotStarted => None,
    })
}

#[cfg(test)]
#[path = "wal_checkpoints_tests.rs"]
mod tests;
