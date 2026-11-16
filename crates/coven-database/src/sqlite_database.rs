//! A database's connections: one writer, the readers beside it, and the
//! upkeep threads that checkpoint its WAL and keep its planner's statistics.
//! No connection leaves it: the host runs its SQL through `host` on the
//! writer and through a `Reader` on a reader.

#[path = "read_pool.rs"]
mod read_pool;

use crate::authorization::{Policy, PolicyError};
use crate::connection_setup::Setup;
use crate::planner_statistics::PlannerStatistics;
use crate::sqlite::DatabaseConnection;
use crate::upkeep::UpkeepThread;
use crate::wal_checkpoints::WalCheckpointer;
use coven_foundation::locks::{into_unpoisoned, lock_unpoisoned};
use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub(crate) use read_pool::Reader;

/// How to open a database's connections. Every connection gets a policy of
/// its own from `policy`, and the same `setup`.
pub(crate) struct Options<'a, P: Policy> {
    pub(crate) settings: crate::SqliteSettings,
    /// The read-only connections beside the writer.
    pub(crate) readers: NonZeroUsize,
    /// Run `prepare` on the first reader, and open the writer only once it
    /// passed, leaving the journal as it is: a database whose checks must
    /// not write, whose writer would create the file or the schema it
    /// checks. Otherwise the writer switches the database to WAL and runs
    /// `prepare`.
    pub(crate) prepare_on_reader: bool,
    /// The views and tables a live query reads whole.
    pub(crate) whole_views: Arc<sqlive::WholeViews>,
    /// Each connection's policy, made as the connection opens.
    pub(crate) policy: &'a dyn Fn() -> P,
    /// What each connection registers, given that connection's policy,
    /// before any SQL runs on it.
    pub(crate) setup: &'a dyn Fn(&Setup<'_, P>, &P) -> rusqlite::Result<()>,
    /// Capture the writer's commits for the handle's live queries, keeping
    /// a write's operations on these tables, lowercase, whole.
    pub(crate) capture: Option<BTreeSet<String>>,
    /// Checkpoint the WAL on a thread of its own once it holds this many
    /// frames not yet in the database file (§4).
    pub(crate) checkpoint_pages: Option<NonZeroU32>,
    /// Gather the planner's statistics as the database opens, keep them on
    /// a thread of their own while it stays open, and gather them again as
    /// it closes (§4).
    pub(crate) statistics: bool,
}

/// One database's connections and upkeep threads.
pub(crate) struct Database<P: Policy> {
    /// Shared with the upkeep threads, which hold it while they run.
    writer: Arc<Mutex<DatabaseConnection<P>>>,
    readers: read_pool::ReadPool<P>,
    /// The handle the connections serve.
    handle: sqlive::Handle<P::Failure>,
    checkpoints: Option<WalCheckpointer<P>>,
    statistics: Option<UpkeepThread<PlannerStatistics<P>>>,
}

impl<P: Policy> Database<P> {
    /// Open the database at `path`: its writer, switched to WAL, then
    /// `prepare` on the writer (or on the first reader, as `options` asks)
    /// before the database serves anything; then the statistics, commit
    /// capture, upkeep threads and readers `options` asks for. Every
    /// connection then serves `handle`, which their failures fail.
    pub(crate) fn open_with<T, E: From<PolicyError<P>>>(
        path: &Path,
        handle: sqlive::Handle<P::Failure>,
        options: Options<'_, P>,
        prepare: impl FnOnce(&mut DatabaseConnection<P>) -> Result<T, E>,
    ) -> Result<(Self, T), E> {
        let open = |read_only| {
            DatabaseConnection::open(
                path,
                read_only,
                (options.policy)(),
                Arc::clone(&options.whole_views),
                &options.settings,
                options.setup,
            )
        };
        let statistics = options.statistics;
        let mut readers = Vec::with_capacity(options.readers.get());
        let (mut writer, prepared) = if options.prepare_on_reader {
            assert!(!statistics, "a database prepared on a reader gathers no statistics");
            let mut first = open(true)?;
            let prepared = first.with_profile(prepare)?;
            readers.push(first);
            (open(false)?, prepared)
        } else {
            let mut writer = open(false)?;
            let prepared = writer.with_profile(|writer| -> Result<T, E> {
                writer.enable_wal()?;
                let prepared = prepare(writer)?;
                if statistics {
                    // On a connection of its own: one that analyzes a small
                    // table loads its count, and forgetting the statistics
                    // afterwards doesn't unload it. The writer then reads
                    // what that connection kept; SQLite reads its statistics
                    // again only when told to.
                    open(false)?.gather_statistics()?;
                    writer.batch("ANALYZE sqlite_schema")?;
                }
                Ok(prepared)
            })?;
            (writer, prepared)
        };
        match options.capture {
            Some(recorded) => writer.observe_commits(handle.clone(), recorded),
            None => writer.serve(handle.clone()),
        }
        let mut database = Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: read_pool::ReadPool::new(Vec::new()),
            handle: handle.clone(),
            checkpoints: None,
            statistics: None,
        };
        // Each upkeep thread stops as `database` drops, should a later step
        // fail.
        if let Some(pages) = options.checkpoint_pages {
            database.checkpoints = Some(WalCheckpointer::start(
                open(false)?,
                &database.writer,
                pages,
                handle.clone(),
            )?);
        }
        if statistics {
            database.statistics = Some(UpkeepThread::<PlannerStatistics<P>>::start(
                open(false)?,
                &database.writer,
                handle.clone(),
            )?);
        }
        while readers.len() < options.readers.get() {
            readers.push(open(true)?);
        }
        for reader in &mut readers {
            reader.serve(handle.clone());
        }
        database.readers = read_pool::ReadPool::new(readers.into_iter().map(Mutex::new).collect());
        Ok((database, prepared))
    }

    /// Run `run` on the writer, once no other call holds it.
    pub(crate) fn host<T>(&self, run: impl FnOnce(&mut DatabaseConnection<P>) -> T) -> T {
        crate::database::with_locked(&self.writer, run)
    }

    /// An idle reader, once one is free; it stays the caller's until the
    /// lease drops.
    pub(crate) fn reader(&self) -> Reader<'_, P> {
        self.readers.acquire_reader()
    }

    /// Run `visit` on each reader in turn, under its own lock.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn each_reader(&self, visit: impl FnMut(&mut DatabaseConnection<P>)) {
        self.readers.each(visit);
    }

    /// Stop the upkeep threads; gather the planner's statistics again if
    /// the database keeps them and its handle hasn't failed; then close every
    /// connection. Closing goes on past each failure, and returns them all.
    pub(crate) fn close(self) -> Vec<PolicyError<P>> {
        let mut failures = Vec::new();
        if let Some(checkpoints) = self.checkpoints {
            failures.extend(checkpoints.stop().err());
        }
        if let Some(statistics) = self.statistics {
            failures.extend(statistics.stop().err());
            // The statistics follow this session's writes, outside any
            // captured transaction. A failed handle's writer may still hold
            // a transaction, which closing rolls back; a handle its host
            // closed first leaves none.
            if !self.handle.has_failed() {
                failures.extend(lock_unpoisoned(&self.writer).optimize().err());
            }
        }
        failures.extend(self.readers.close());
        // Each upkeep thread stopped before, and let go of the writer.
        match Arc::try_unwrap(self.writer) {
            Ok(writer) => failures.extend(into_unpoisoned(writer).close().err()),
            Err(_held) => failures.push(sqlive::Error::Invariant {
                what: "a writer closes after its upkeep threads stopped",
            }),
        }
        failures
    }
}

#[cfg(test)]
#[path = "sqlite_database_tests.rs"]
mod tests;
