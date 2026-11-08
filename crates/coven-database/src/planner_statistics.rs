//! The query planner's statistics while a store stays open (§4).
//!
//! Coven gathers the statistics as a writable store opens and as it closes
//! (`optimize`, sqlite_settings.rs). A table can fill from empty within one
//! session, as a library's first scan fills its candidates, and SQLite
//! would plan every statement on it without statistics until the next open.
//! So each time the writer's commits have changed [`CHECKED_CHANGES`] rows
//! since the last check, they wake a thread of the store's own (upkeep.rs),
//! which asks SQLite, on a connection of its own, which tables
//! `PRAGMA optimize` would analyze: one whose size changed tenfold since its
//! statistics, or one with an index that has none. A table it names that
//! holds fewer than `SMALL_TABLE` rows would keep none, so only a larger one
//! starts a run. A run gathers the statistics as an open does, holding
//! coven's writer so no write of coven's finds the database locked meanwhile.

use crate::database::with_locked;
use crate::observation::{CommitObserver, HandleFailure};
use crate::sqlite::DatabaseConnection;
use crate::upkeep::{Upkeep, UpkeepThread, Wake};
use crate::DbError;
use coven_foundation::locks::lock_unpoisoned;
use std::cell::Cell;
use std::sync::{Arc, Mutex, Weak};

/// The rows the writer's commits change between two checks. A check took
/// about 80 µs on a store of four 20,000-row tables beside coven's own,
/// under 0.1 µs for each row changed since the last; and a table has
/// statistics within this many changed rows of reaching the size that
/// calls for them.
pub(crate) const CHECKED_CHANGES: u64 = 1_000;

/// The upkeep of the planner's statistics of a store's database.
pub(crate) struct PlannerStatistics {
    /// The thread's own connection, held through each check and run.
    connection: Mutex<DatabaseConnection>,
    /// The writer, held only during a run, so the store's close can take it
    /// back once the thread stops.
    writer: Weak<Mutex<DatabaseConnection>>,
    /// Where a test hears of each run, told once it committed.
    #[cfg(test)]
    runs: Mutex<Option<tokio::sync::mpsc::UnboundedSender<()>>>,
}

impl UpkeepThread<PlannerStatistics> {
    /// Keep the planner's statistics of `writer`'s database from its commits
    /// on, on `connection`, a writable connection of its own to the same
    /// database with the app's functions, as an open's run has, failing
    /// `handle` if a check or run fails.
    pub(crate) fn start(
        connection: DatabaseConnection,
        writer: &Arc<Mutex<DatabaseConnection>>,
        handle: CommitObserver,
    ) -> Result<Self, DbError> {
        let statistics = Self::spawn(
            PlannerStatistics {
                connection: Mutex::new(connection),
                writer: Arc::downgrade(writer),
                #[cfg(test)]
                runs: Mutex::new(None),
            },
            handle,
        )?;
        {
            let mut writer = lock_unpoisoned(writer);
            let wake = ChangeWake::new(statistics.wake(), &writer);
            writer.signal_changes(wake);
        }
        Ok(statistics)
    }

    /// Stop, after any check or run in progress, and close the connection.
    pub(crate) fn stop(self) -> Result<(), DbError> {
        coven_foundation::locks::into_unpoisoned(self.join().connection).close()
    }
}

impl Upkeep for PlannerStatistics {
    const THREAD: &'static str = "coven planner statistics";

    /// One wake's check, and the run it calls for. A store whose open failed
    /// may let its writer go before the thread stops; there is then nothing
    /// to keep statistics for.
    fn run(&self) -> Result<(), DbError> {
        with_locked(&self.connection, |connection| {
            if !connection.statistics_wanted()? {
                return Ok(());
            }
            let Some(writer) = self.writer.upgrade() else {
                return Ok(());
            };
            // The run moves the schema cookie without changing a table, so
            // the writer keeps the shapes it read: the next write reads none.
            with_locked(&writer, |writes| {
                writes.keeping_table_shapes(|| connection.gather_statistics_for_every_connection())
            })?;
            tracing::debug!("gathered the planner's statistics");
            #[cfg(test)]
            self.report();
            Ok(())
        })
    }

    fn failure(error: Arc<DbError>) -> HandleFailure {
        HandleFailure::Statistics(error)
    }
}

impl PlannerStatistics {
    /// Tell a test that asked of each run.
    #[cfg(test)]
    fn report(&self) {
        if let Some(runs) = lock_unpoisoned(&self.runs).as_ref() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a test that stopped listening has no use for it"
            )]
            let _ = runs.send(());
        }
    }
}

/// Wakes the statistics thread from the writer's commits, once they have
/// changed [`CHECKED_CHANGES`] rows since the last wake.
pub(crate) struct ChangeWake {
    wake: Wake,
    /// The writer's count of changed rows at the last wake.
    woken_at: Cell<u64>,
}

impl ChangeWake {
    fn new(wake: Wake, writer: &DatabaseConnection) -> Self {
        Self { wake, woken_at: Cell::new(writer.changed_rows()) }
    }

    /// The writer committed, having changed `changed` rows since it opened.
    pub(crate) fn committed(&self, changed: u64) {
        if changed.saturating_sub(self.woken_at.get()) >= CHECKED_CHANGES {
            self.woken_at.set(changed);
            self.wake.wake();
        }
    }
}

#[cfg(test)]
#[path = "planner_statistics_tests.rs"]
mod tests;
