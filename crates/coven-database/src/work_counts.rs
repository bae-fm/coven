//! Test-only accounting shared by a database's writer and readers.

use coven_foundation::locks::lock_unpoisoned;
use std::sync::{Arc, Mutex};

/// Local database work observed at its execution boundaries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkCounts {
    /// Successful acquisitions of the writer connection, including read-only work.
    pub writer_holds: usize,
    /// Successful durable transaction commits, including unchanged transactions.
    pub commits: usize,
    /// Snapshot bodies the loader parsed, for loading, verification or
    /// file-reference checks.
    pub snapshot_reads: usize,
    /// Rows whose published state an applied slot kept or put back beside
    /// this device's unsent writes (§6.2).
    pub published_rows: usize,
    /// Rows a removal view evaluated: its present rows staged, with their
    /// CHECKs, unique values and references read (§9.3).
    pub evaluated_rows: usize,
    /// Reads of what a transaction can't change: the schema version, SQLite's
    /// schema cookie, and synced columns' and foreign keys' registered ids.
    pub constant_reads: usize,
    /// SQLite virtual machine steps the app's statements ran in `query` and
    /// `query_row`, in reads, live queries' runs, writes and migrations.
    pub app_read_steps: usize,
    /// The app's statements that return rows, run in `query` and `query_row`,
    /// one per run.
    pub app_read_statements: usize,
    /// SQLite virtual machine steps the app's statements ran in `execute` and
    /// `execute_batch`, triggers' included, in writes and migrations.
    pub app_write_steps: usize,
    /// Steps of coven's lookups judging whether a write changed rows a live
    /// query joins: each asks whether a partner row in its scope holds a
    /// changed row's value (§4).
    pub lookup_steps: usize,
}

/// Injected counter; clones observe the same database, never other test databases.
#[derive(Clone)]
pub struct WorkCounter(Arc<Mutex<WorkCounts>>);

impl WorkCounter {
    /// A counter no database has counted on yet, for a test to give one.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(WorkCounts::default())))
    }

    /// Start another measurement after the preceding work has completed.
    pub fn take(&self) -> WorkCounts {
        std::mem::take(&mut lock_unpoisoned(&self.0))
    }

    pub(crate) fn record(&self, update: impl FnOnce(&mut WorkCounts)) {
        update(&mut lock_unpoisoned(&self.0));
    }
}
