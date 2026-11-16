//! sqlive's own WAL checkpoints, off the commit path (§4).
//!
//! A checkpoint run inside a commit, as SQLite's automatic one is, makes the
//! commit wait for it, `F_FULLFSYNC` included on macOS and iOS. While a live
//! query keeps a reader's snapshot open nearly all the time, that checkpoint
//! can't finish, so every later commit runs another and the WAL never starts
//! over. So none of the database's connections runs a checkpoint as it
//! commits: not the writer, and not the statistics thread's connection,
//! whose commits hold the writer. Each of the writer's commits only wakes a thread of the
//! database's own, which reads how many frames the WAL holds past what was
//! checkpointed and, past the threshold, checkpoints on a
//! connection of its own: first copying what it can while writes go on and
//! syncing the copy, again for the frames committed meanwhile while each
//! pass leaves fewer, then, holding the writer so no write starts,
//! copying the rest, syncing it and waiting briefly for the readers to leave
//! the WAL, so the next commit writes it from its start.
//!
//! The thread is one of the database's upkeep (upkeep.rs). A checkpoint
//! SQLite couldn't start, reading nothing of the WAL, because another
//! connection was checkpointing it or committing to it right then, is tried
//! again at the next wake, as one that finds the database busy is. Any
//! other failure fails the handle rather than leave the database open while
//! its WAL grows without bound.
//!
//! Every checkpoint then goes through `HostSql::wal_checkpoint`, the
//! checkpointer's and the host's alike, which sends the main file's size to
//! the database's watch (`Database::checkpointed_size`).

use crate::authorization::{Policy, PolicyError};
use crate::connection::HostSql;
use crate::upkeep::{Upkeep, UpkeepThread};
use crate::{CheckpointMode, WalCheckpoint};
use caught::locks::lock_unpoisoned;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// How long the checkpoint that lets the WAL start over waits for readers
/// to leave it, while the writer's writes wait: a reader that started before it
/// finishes within this, and later ones read the database file alone. If one
/// still holds on, the WAL starts over once it grows by the threshold again.
const READER_WAIT: Duration = Duration::from_millis(100);

pub(crate) struct Checkpoints<P: Policy> {
    /// The checkpointer's own connection, held through each checkpoint.
    connection: Mutex<HostSql<P>>,
    /// The writer, held only during a checkpoint, so the database's close can
    /// take it back once the checkpointer stops.
    writer: Weak<Mutex<HostSql<P>>>,
    /// The frames not yet checkpointed past which a wake checkpoints.
    threshold: NonZeroU32,
    /// A test's fault, run as each checkpoint's last step begins.
    #[cfg(any(test, feature = "test-utils"))]
    fault: Mutex<Option<Fault<P>>>,
    /// A test's step, run as each pass's copy returns, before its sync.
    #[cfg(any(test, feature = "test-utils"))]
    copied: Mutex<Option<Box<dyn FnMut() + Send>>>,
    /// Where a test hears what each checkpoint's last step did, told while
    /// the checkpoint still holds the writer.
    #[cfg(any(test, feature = "test-utils"))]
    restarts: Mutex<Option<tokio::sync::mpsc::UnboundedSender<WalCheckpoint>>>,
}

#[cfg(any(test, feature = "test-utils"))]
type Fault<P> = Box<dyn FnMut() -> Result<(), PolicyError<P>> + Send>;

/// A database's checkpointer: a thread of the database's own, woken by the
/// writer's commits, which runs each checkpoint on its own connection. It
/// stops as the database closes, before the writer.
pub(crate) type WalCheckpointer<P> = UpkeepThread<Checkpoints<P>>;

impl<P: Policy> UpkeepThread<Checkpoints<P>> {
    /// Take checkpointing `writer`'s database over from its commits, once
    /// the WAL holds `threshold` frames not yet checkpointed, on
    /// `connection`, a connection of its own to the same database, failing
    /// `handle` if checkpointing fails.
    pub(crate) fn start(
        connection: HostSql<P>,
        writer: &Arc<Mutex<HostSql<P>>>,
        threshold: NonZeroU32,
        handle: crate::Handle<P::Failure>,
    ) -> Result<Self, PolicyError<P>> {
        connection.batch(&format!("PRAGMA busy_timeout = {}", READER_WAIT.as_millis()))?;
        // Made before the writer's steps, so a step that fails stops the
        // thread as this drops.
        let checkpointer = Self::spawn(
            Checkpoints {
                connection: Mutex::new(connection),
                writer: Arc::downgrade(writer),
                threshold,
                #[cfg(any(test, feature = "test-utils"))]
                fault: Mutex::new(None),
                #[cfg(any(test, feature = "test-utils"))]
                copied: Mutex::new(None),
                #[cfg(any(test, feature = "test-utils"))]
                restarts: Mutex::new(None),
            },
            handle,
        )?;
        let wake = checkpointer.wake();
        {
            let mut writer = lock_unpoisoned(writer);
            leave_checkpoints_to_checkpointer(&writer)?;
            writer.signal_commits(wake);
        }
        Ok(checkpointer)
    }

    /// Stop, after any checkpoint in progress, and close the connection.
    pub(crate) fn stop(self) -> Result<(), PolicyError<P>> {
        caught::locks::into_unpoisoned(self.join().connection).close()
    }
}

impl<P: Policy> Upkeep for Checkpoints<P> {
    type Policy = P;

    const THREAD: &'static str = "sqlive WAL checkpoint";

    fn run(&self) -> Result<(), PolicyError<P>> {
        self.checkpoint()
    }

    fn failure(error: PolicyError<P>) -> crate::EngineFailure<P::Failure, P::Refusal> {
        crate::EngineFailure::Checkpoint(error)
    }
}

impl<P: Policy> Checkpoints<P> {
    /// One wake's checkpoint, on the checkpointer's connection, once the WAL
    /// holds `threshold` frames not yet in the database file. A checkpoint
    /// the readers held up leaves the WAL copied but not started over; the
    /// checkpointer tries again once the WAL grows by the threshold. A
    /// database whose open failed may let its writer go before its
    /// checkpointer stops; there is then nothing to checkpoint.
    fn checkpoint(&self) -> Result<(), PolicyError<P>> {
        let connection = lock_unpoisoned(&self.connection);
        let Some(writer) = self.writer.upgrade() else {
            return Ok(());
        };
        // When SQLite can't read the WAL now, the checkpointer reads it at the
        // next wake.
        let Some(uncopied) = uncopied_frames(&connection)? else {
            return Ok(());
        };
        if uncopied < self.threshold.get() {
            return Ok(());
        }
        // Copy what no reader needs while the writer's writes go on, and sync
        // it. SQLite syncs the database file only when a checkpoint
        // copies the whole WAL, and a commit made during the copy
        // leaves it short; without a sync of its own, the checkpoint that
        // holds the writer would sync every page copied here. The frames
        // committed during a pass get a pass of their own while each pass
        // leaves fewer, so the checkpoint holding the writer copies and
        // syncs only the frames committed during the last pass and since,
        // none when no commit was made.
        let (mut passes, mut left) = (0, uncopied);
        loop {
            // When SQLite can't start a copy now, because another connection
            // is checkpointing the WAL or committing to it, the checkpointer
            // tries the copy again at the next wake instead of finishing it
            // while writes wait.
            if let WalCheckpoint::NotStarted = connection.wal_checkpoint(CheckpointMode::Passive)? {
                return Ok(());
            }
            #[cfg(any(test, feature = "test-utils"))]
            self.copied();
            connection.sync_database_file()?;
            passes += 1;
            match uncopied_frames(&connection)? {
                Some(now) if now > 0 && now < left => left = now,
                _ => break,
            }
        }
        // No write on the writer starts meanwhile, so the checkpoint takes
        // SQLite's writer lock at once, copies and syncs the frames
        // committed since the last pass, and waits only for readers.
        let (restarted, held) = {
            let _writes = lock_unpoisoned(&writer);
            let started = Instant::now();
            #[cfg(any(test, feature = "test-utils"))]
            self.fault()?;
            let restarted = connection.wal_checkpoint(CheckpointMode::Restart)?;
            #[cfg(any(test, feature = "test-utils"))]
            self.report(restarted);
            (restarted, started.elapsed())
        };
        tracing::debug!(frames = uncopied, passes, ?restarted, ?held, "checkpointed the WAL");
        Ok(())
    }

    /// Run the fault a test set, as the checkpoint's last step begins.
    #[cfg(any(test, feature = "test-utils"))]
    fn fault(&self) -> Result<(), PolicyError<P>> {
        lock_unpoisoned(&self.fault).as_mut().map_or(Ok(()), |f| f())
    }

    /// Run the step a test set, as a pass's copy returns.
    #[cfg(any(test, feature = "test-utils"))]
    fn copied(&self) {
        if let Some(step) = lock_unpoisoned(&self.copied).as_mut() {
            step();
        }
    }

    /// Tell a test that asked what the checkpoint's last step did.
    #[cfg(any(test, feature = "test-utils"))]
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

/// Turn SQLite's own automatic checkpoint off on `connection`, a connection
/// that commits to a database with a checkpointer, so its commits run no
/// checkpoint while the database stays open.
pub(crate) fn leave_checkpoints_to_checkpointer<P: Policy>(
    connection: &HostSql<P>,
) -> Result<(), PolicyError<P>> {
    connection.batch("PRAGMA wal_autocheckpoint = 0")
}

/// The frames the WAL holds that the database file doesn't, or None when
/// SQLite couldn't read the WAL now: another connection was checkpointing it
/// or committing to it.
fn uncopied_frames<P: Policy>(connection: &HostSql<P>) -> Result<Option<u32>, PolicyError<P>> {
    Ok(match connection.wal_checkpoint(CheckpointMode::Noop)? {
        WalCheckpoint::Ran { log_frames, checkpointed_frames, .. } => {
            Some(log_frames.saturating_sub(checkpointed_frames))
        }
        WalCheckpoint::NotStarted => None,
    })
}

#[cfg(any(test, feature = "test-utils"))]
impl<P: Policy> WalCheckpointer<P> {
    /// Pass the checkpointer's connection to `visit`, after any checkpoint in
    /// progress.
    pub(crate) fn with_connection(&self, visit: impl FnOnce(&HostSql<P>)) {
        visit(&lock_unpoisoned(&self.upkeep().connection));
    }

    /// Run one wake's checkpoint now, on the calling thread.
    pub(crate) fn run_now(&self) -> Result<(), PolicyError<P>> {
        self.upkeep().checkpoint()
    }

    /// Run `fault` as each checkpoint takes the writer.
    pub(crate) fn fault(&self, fault: impl FnMut() -> Result<(), PolicyError<P>> + Send + 'static) {
        *lock_unpoisoned(&self.upkeep().fault) = Some(Box::new(fault));
    }

    /// Run `step` as each pass's copy returns, before its sync.
    pub(crate) fn after_copy(&self, step: impl FnMut() + Send + 'static) {
        *lock_unpoisoned(&self.upkeep().copied) = Some(Box::new(step));
    }

    /// Return a receiver that gets what each later checkpoint's last step
    /// does. The checkpointer sends it while the checkpoint still holds the
    /// writer.
    pub(crate) fn report_restarts(&self) -> tokio::sync::mpsc::UnboundedReceiver<WalCheckpoint> {
        let (restarts, reported) = tokio::sync::mpsc::unbounded_channel();
        *lock_unpoisoned(&self.upkeep().restarts) = Some(restarts);
        reported
    }
}

#[cfg(test)]
#[path = "wal_checkpoints_tests.rs"]
mod tests;
