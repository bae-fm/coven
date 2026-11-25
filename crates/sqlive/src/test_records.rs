//! What a test reads back of a connection's work: the work it counts, the
//! statements its reads run, and the points a write transaction passes; and
//! the costs a measurement prints for the performance suite.

use panic_as_error::locks::lock_unpoisoned;
use std::sync::{Arc, Mutex};

/// The work a connection counts: its commits, its reads of the schema
/// cookie, the steps of the statements it runs for the app and for live
/// queries' lookups, and the scope plans it builds, with their size.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionWork {
    /// Successful durable transaction commits, unchanged transactions'
    /// included.
    pub commits: usize,
    /// Reads of SQLite's schema cookie.
    pub cookie_reads: usize,
    /// SQLite virtual machine steps the app's statements that return rows
    /// ran.
    pub app_read_steps: usize,
    /// The app's statements that return rows, one per run.
    pub app_read_statements: usize,
    /// SQLite virtual machine steps the app's statements that write ran,
    /// triggers' included.
    pub app_write_steps: usize,
    /// Steps of the lookups judging whether a write changed rows a live
    /// query joins.
    pub lookup_steps: usize,
    /// Live queries that a write judged its changes against, one for each
    /// write that judged each: those it could affect.
    pub judged_queries: usize,
    /// Questions about a changed row that ran past `LOOKUP_STEPS` and
    /// stopped, each of which reran a live query.
    pub stopped_questions: usize,
    /// Scope plans built for a live query's statement (`scope_plan.rs`).
    pub scope_plans_built: usize,
    /// How much of their statements those plans read, all together.
    pub scope_plan_size: crate::ScopePlanSize,
}

/// Where connections count their work. Clones count into the same place.
#[derive(Clone, Default)]
pub struct ConnectionWorkCounter(Arc<Mutex<ConnectionWork>>);

impl ConnectionWorkCounter {
    /// A counter that no connection has counted into yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The work counted since the last take, starting the count again.
    pub fn take(&self) -> ConnectionWork {
        std::mem::take(&mut *lock_unpoisoned(&self.0))
    }

    /// Count work a connection did.
    pub(crate) fn record(&self, update: impl FnOnce(&mut ConnectionWork)) {
        update(&mut lock_unpoisoned(&self.0));
    }
}

/// The statements that reads ran on the connections this log was given to,
/// in order, from when each connection got it. Clones share one log.
#[derive(Clone, Default)]
pub struct StatementLog(Arc<Mutex<Vec<String>>>);

impl StatementLog {
    /// A log no connection has recorded into yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The recorded statements so far, each as SQLite's text with the bound
    /// values written in.
    pub fn statements(&self) -> Vec<String> {
        lock_unpoisoned(&self.0).clone()
    }

    /// The recorded statements so far, emptying the log.
    pub fn take(&self) -> Vec<String> {
        std::mem::take(&mut lock_unpoisoned(&self.0))
    }

    pub(crate) fn push(&self, statement: String) {
        lock_unpoisoned(&self.0).push(statement);
    }
}

/// A point a write transaction passes as it commits, for tests that stop or
/// end the process there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitPoint {
    /// Live queries have judged the write, before SQLite commits it.
    Judged,
    /// SQLite has committed, before the handle's subscribers hear of it.
    Committed,
    /// The handle's subscribers have heard of the commit.
    Observed,
}

/// What a connection calls at each commit point.
pub(crate) type CommitPointObserver = Box<dyn Fn(CommitPoint) + Send>;

/// One measured number, as a line `scripts/perf.sh` reads:
/// `COST metric=<name> value=<n> unit=<ms|ratio|count>`. Lower is better in
/// every unit. A character in `metric` outside letters, digits and `_./-`
/// becomes `_`, so a caller can pass a label unchanged.
pub fn report_cost(metric: &str, value: f64, unit: &str) {
    let metric: String = metric
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || "_./-".contains(c) { c } else { '_' })
        .collect();
    eprintln!("COST metric={metric} value={value} unit={unit}");
}

/// [`report_cost`] for a time, in milliseconds.
pub fn report_time(metric: &str, time: std::time::Duration) {
    report_cost(metric, time.as_secs_f64() * 1000.0, "ms");
}
