//! A database's connections: one writer, the readers beside it, and the
//! upkeep threads that checkpoint its WAL and keep its planner's statistics.
//! No connection leaves it: the host runs its SQL through `host` on the
//! writer and through a `Reader` on a reader.

#[path = "read_pool.rs"]
mod read_pool;

use crate::authorization::{Policy, PolicyError};
use crate::connection::HostSql;
use crate::connection_setup::Setup;
use crate::host_judge::HostAttachment;
use crate::planner_statistics::PlannerStatistics;
use crate::upkeep::UpkeepThread;
use crate::wal_checkpoints::WalCheckpointer;
use panic_as_error::locks::{into_unpoisoned, lock_unpoisoned};
use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub use read_pool::Reader;

/// How to open a database's connections. Every connection gets a policy of
/// its own from `policy`, and the same `setup`.
pub struct Options<'a, P: Policy> {
    /// The SQLite settings of every connection.
    pub settings: crate::SqliteSettings,
    /// The read-only connections beside the writer.
    pub readers: NonZeroUsize,
    /// Run `prepare` on the first reader, and open the writer only once
    /// `prepare` passes, leaving the journal mode as it is. Set this for a
    /// database whose checks must not write, because its writer would create
    /// the file or the schema the checks look for. Otherwise the writer
    /// switches the database to WAL and runs `prepare`.
    pub prepare_on_reader: bool,
    /// Each connection's policy, made as the connection opens.
    pub policy: &'a dyn Fn() -> P,
    /// What each connection registers, given that connection's policy,
    /// before any SQL runs on it.
    pub setup: &'a dyn Fn(&Setup<'_, P>, &P) -> rusqlite::Result<()>,
    /// Capture the writer's commits for the handle's live queries, and record
    /// each operation of a write on these tables, named in lowercase, with
    /// every column's value before and after it.
    pub capture: Option<BTreeSet<String>>,
    /// Checkpoint the WAL on a thread of its own once it holds this many
    /// frames not yet in the database file (SPEC.md section 10).
    pub checkpoint_pages: Option<NonZeroU32>,
    /// Gather the planner's statistics as the database opens, keep them on
    /// a thread of their own while it stays open, and gather them again as
    /// it closes (SPEC.md section 10).
    pub statistics: bool,
}

/// One database's connections and upkeep threads.
pub struct Database<P: Policy> {
    /// Shared with the upkeep threads, which hold it while they run.
    writer: Arc<Mutex<HostSql<P>>>,
    readers: read_pool::ReadPool<P>,
    /// The handle the connections serve.
    handle: crate::Handle<P::Failure>,
    /// The host tables' judge, on a connection of its own, while the
    /// database declares any.
    host_judge: Option<HostAttachment<P>>,
    checkpoints: Option<WalCheckpointer<P>>,
    statistics: Option<UpkeepThread<PlannerStatistics<P>>>,
    /// The main file's size after the latest checkpoint. The database keeps
    /// this receiver unread, so each view it hands out sees every change
    /// since the open.
    checkpointed_size: watch::Receiver<u64>,
}

impl<P: Policy> Database<P> {
    /// Open the database at `path`: its writer, switched to WAL, then
    /// `prepare` on the writer (or on the first reader, as `options` asks)
    /// before the database serves anything; then the statistics, commit
    /// capture, upkeep threads and readers `options` asks for. Every
    /// connection then serves `handle`, which their failures fail.
    pub fn open_with<T, E: From<PolicyError<P>>>(
        path: &Path,
        handle: crate::Handle<P::Failure>,
        options: Options<'_, P>,
        prepare: impl FnOnce(&mut HostSql<P>) -> Result<T, E>,
    ) -> Result<(Self, T), E> {
        // Every connection orders its commits and snapshots with the others
        // (`snapshot_values.rs`).
        let order = Arc::new(crate::snapshot_values::CommitOrder::new());
        // A statement's scope plan depends on its SQL, the schema, and the SQL
        // functions and whole views of its connection. `open` gives every
        // connection the same functions and views, so they share the plans
        // they build (SPEC.md section 4).
        let plans = Arc::new(crate::connection::ScopePlans::new());
        let open = |read_only| {
            let mut connection = HostSql::open_sharing(
                path,
                read_only,
                (options.policy)(),
                &options.settings,
                options.setup,
                Arc::clone(&plans),
            )?;
            connection.share_commit_order(Arc::clone(&order));
            Ok::<_, PolicyError<P>>(connection)
        };
        // The watch starts at the main file's size as the writer opens. The
        // writer holds the watch from then on, so the watch gets the size
        // after each checkpoint the open runs; the checkpointer holds it too
        // once it starts.
        let open_writer = || -> Result<(HostSql<P>, watch::Sender<u64>), PolicyError<P>> {
            let mut writer = open(false)?;
            let sizes = watch::Sender::new(writer.database_file_size()?);
            writer.send_checkpointed_sizes(sizes.clone());
            Ok((writer, sizes))
        };
        let statistics = options.statistics;
        let mut readers = Vec::with_capacity(options.readers.get());
        let (mut writer, sizes, prepared) = if options.prepare_on_reader {
            assert!(!statistics, "a database prepared on a reader gathers no statistics");
            let mut first = open(true)?;
            let prepared = first.with_profile(prepare)?;
            readers.push(first);
            let (writer, sizes) = open_writer()?;
            (writer, sizes, prepared)
        } else {
            let (mut writer, sizes) = open_writer()?;
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
            (writer, sizes, prepared)
        };
        // Switching a new database to WAL writes the main file's first page,
        // and the commits `open_with` ran so far may have run SQLite's own
        // automatic checkpoint, which sends no size. So the writer sends the
        // file's size here, and no commit runs between this send and the
        // checkpointer's start.
        writer.send_checkpointed_size()?;
        match options.capture {
            Some(recorded) => writer.observe_commits(handle.clone(), recorded),
            None => writer.serve(handle.clone()),
        }
        let mut database = Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: read_pool::ReadPool::new(Vec::new()),
            handle: handle.clone(),
            host_judge: None,
            checkpoints: None,
            statistics: None,
            checkpointed_size: sizes.subscribe(),
        };
        // Each upkeep thread stops as `database` drops, should a later step
        // fail.
        if let Some(pages) = options.checkpoint_pages {
            let mut checkpointer = open(false)?;
            checkpointer.send_checkpointed_sizes(sizes);
            database.checkpoints = Some(WalCheckpointer::start(
                checkpointer,
                &database.writer,
                pages,
                handle.clone(),
            )?);
        }
        if statistics {
            let connection = open(false)?;
            // A run's commit holds the writer, so it must not checkpoint
            // either.
            if database.checkpoints.is_some() {
                crate::wal_checkpoints::leave_checkpoints_to_checkpointer(&connection)?;
            }
            database.statistics = Some(UpkeepThread::<PlannerStatistics<P>>::start(
                connection,
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
        let host_tables = lock_unpoisoned(&database.writer).reads_host_tables();
        if host_tables {
            let mut judge = open(true)?;
            judge.serve(handle);
            database.host_judge = Some(judge.judge_host_changes());
        }
        Ok((database, prepared))
    }

    /// Run `run` on the writer, once no other call holds it.
    pub fn host<T>(&self, run: impl FnOnce(&mut HostSql<P>) -> T) -> T {
        with_locked(&self.writer, run)
    }

    /// An idle reader, once one is free; it stays the caller's until the
    /// lease drops.
    pub fn reader(&self) -> Reader<'_, P> {
        self.readers.acquire_reader()
    }

    /// A view of the main database file's size in bytes after the latest
    /// checkpoint, which wakes when a later checkpoint changes the size.
    /// Before the first checkpoint, the watch holds the file's size once the
    /// commits `open_with` ran ended. The writer and the checkpointer send the
    /// size after each checkpoint they run through
    /// `HostSql::wal_checkpoint`: the checkpointer's own, and the host's on
    /// the writer.
    ///
    /// Two kinds of checkpoint send nothing. SQLite checkpoints the WAL as
    /// the database closes, and nothing reads the watch after that, because
    /// the watch closes with the database. SQLite's own automatic
    /// checkpoint, which runs inside a commit, sends nothing either; while
    /// sqlive's checkpointer runs, no connection of the database's runs one.
    /// Without the checkpointer, the writer runs one as its
    /// `wal_autocheckpoint` asks.
    pub fn checkpointed_size(&self) -> crate::CheckpointedSize {
        crate::CheckpointedSize::new(self.checkpointed_size.clone())
    }

    /// Stop the upkeep threads; gather the planner's statistics again if
    /// the database keeps them and its handle hasn't failed; then close every
    /// connection. Closing goes on past each failure, and returns them all.
    pub fn close(self) -> Vec<PolicyError<P>> {
        let mut failures = Vec::new();
        if let Some(judge) = self.host_judge {
            failures.extend(judge.close().err());
        }
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
            Err(_held) => failures.push(crate::Error::Invariant {
                what: "a writer closes after its upkeep threads stopped",
            }),
        }
        failures
    }
}

/// Run `run` on `connection`, one of a database's connections. A panic while
/// a caller holds the connection leaves it usable: its open transaction rolls
/// back as the panic unwinds (`write_transaction.rs`), and a failed rollback
/// fails the handle. So `with_locked` takes the lock even when a panic
/// poisoned it.
pub(crate) fn with_locked<T, R>(connection: &Mutex<T>, run: impl FnOnce(&mut T) -> R) -> R {
    run(&mut lock_unpoisoned(connection))
}

#[cfg(any(test, feature = "test-utils"))]
#[path = "database_test_hooks.rs"]
mod test_hooks;
