//! Wall-clock timing for the named stages a run is made of.
//!
//! A sync loop iteration, a sync cycle, a Store pull, and a database open are
//! each a sequence of named stages — probe the provider, drain uploads,
//! discover streams, verify commits, migrate, install a snapshot image,
//! materialize. When one of them takes twenty seconds the only useful question
//! is which of those stages it spent them in, and the logs have to answer it
//! without a second round of instrumentation.
//!
//! This lives in the foundation rather than beside the sync loop because the
//! slow stages are not all in one crate: a device join spends most of its time
//! inside a database open, and timing that from the caller only ever reports
//! one opaque total.
//!
//! Each run holds one [`StageTimings`], times every stage through it, and
//! reports one line naming each stage's total. Stages repeat — a pull discovers
//! once per device stream and applies once per commit — so a stage's entry
//! accumulates across the run instead of being replaced, and only stages that
//! actually ran appear (a stage skipped because a key rotation is pending is
//! absent, not zero). Nested runs report their own line, so the cycle's `pull`
//! stage and the pull's own line describe the same span at two levels of detail.
//!
//! The reported total is the run's whole wall time, so time spent outside every
//! named stage stays visible as the difference rather than disappearing.
//! Timing reads a [`Stopwatch`], not the injected
//! [`Clock`](crate::clock::Clock): this measures how long real work
//! took, not what the store stamps its commits with.
//!
//! A time alone does not say what a stage waited on, so a run whose storage can
//! count what it asks of the provider is started with
//! [`StageTimings::counting`] and reports each stage's operation count beside
//! its time. That is the form the round-trip budget is written in: a join is
//! one snapshot download and a handful of small operations, and a stage that
//! exceeds that says so in its own line.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::Stopwatch;
use tracing::info;

/// The running total of provider operations the storage behind a run has
/// issued.
///
/// A stage's wall time says how long it waited, not what it waited on, and the
/// two shapes want opposite fixes: one slow transfer is a size problem, two
/// hundred fast ones are a round-trip problem no faster network will help.
/// Counting them apart is what lets a stage convict itself instead of inviting
/// another round of instrumentation.
///
/// The foundation cannot see the cloud layer, so a run that wants counts is
/// handed something that can read the total. What counts as one operation is
/// the storage's to define — a call that pages internally is one operation here
/// and several round trips underneath, so a stage whose count is small while
/// its time is large is a paging or streaming call, which is worth telling
/// apart rather than hiding in a total.
pub trait ProviderRequests: Send + Sync {
    fn issued(&self) -> u64;
}

/// One named stage's accumulated cost.
///
/// `requests` is meaningful only for a run started with
/// [`StageTimings::counting`]. An uncounted run leaves it zero and prints no
/// counts at all, because zero operations and "nobody was counting" must not
/// read the same way.
struct Stage {
    name: &'static str,
    elapsed: Duration,
    requests: u64,
}

pub struct StageTimings {
    run: &'static str,
    started: Stopwatch,
    stages: Vec<Stage>,
    /// `None` for a run nobody handed a counter, which reports times alone.
    requests: Option<Arc<dyn ProviderRequests>>,
    /// The counter's reading when this run began. The total is the difference,
    /// so a run sharing a home with runs before it starts from its own zero.
    started_requests: u64,
    reported: bool,
}

impl StageTimings {
    /// Begins timing a run. `run` names it in the reported line.
    pub fn start(run: &'static str) -> Self {
        Self {
            run,
            started: Stopwatch::start(),
            stages: Vec::new(),
            requests: None,
            started_requests: 0,
            reported: false,
        }
    }

    /// Begins timing a run that asks its storage to count the provider
    /// operations each stage issues.
    ///
    /// `requests` is what the storage answered: `None` from storage nobody
    /// wrapped for counting, and such a run reports the line
    /// [`start`](Self::start) would have. Every run with storage in reach comes
    /// through here rather than choosing between the two constructors itself,
    /// because whether counting is on is the storage's answer, not the run's.
    ///
    /// The reported line carries the run's own total beside its wall time, so
    /// operations made outside every named stage stay visible as the difference
    /// — the same way unnamed time already does.
    ///
    /// The counter belongs to the provider, not to this run, so a second run
    /// working the same home at the same time is counted into whichever stage
    /// is open — exactly the way it is already counted into that stage's wall
    /// time. Both numbers describe what the home did while the stage was open,
    /// which is the honest reading of a shared home.
    pub fn counting(run: &'static str, requests: Option<Arc<dyn ProviderRequests>>) -> Self {
        let started_requests = requests.as_ref().map_or(0, |counter| counter.issued());
        Self {
            run,
            started: Stopwatch::start(),
            stages: Vec::new(),
            requests,
            started_requests,
            reported: false,
        }
    }

    fn issued(&self) -> u64 {
        self.requests.as_ref().map_or(0, |counter| counter.issued())
    }

    /// Awaits `work` as the named stage, adding its elapsed time and the
    /// operations it issued to that stage's totals for this run.
    pub async fn stage<T>(&mut self, stage: &'static str, work: impl Future<Output = T>) -> T {
        let started = Stopwatch::start();
        let before = self.issued();
        let outcome = work.await;
        let issued = self.issued().saturating_sub(before);
        self.add(stage, started.elapsed(), issued);
        outcome
    }

    /// Time one blocking step. [`stage`](Self::stage) covers work that awaits;
    /// plenty of what dominates a run — parsing and signature checks over a
    /// carried history — never awaits, and is invisible without this.
    pub fn mark<T>(&mut self, stage: &'static str, work: impl FnOnce() -> T) -> T {
        let started = Stopwatch::start();
        let before = self.issued();
        let outcome = work();
        let issued = self.issued().saturating_sub(before);
        self.add(stage, started.elapsed(), issued);
        outcome
    }

    /// Adds time and operations a caller measured itself, for work whose split
    /// is only visible from inside it — a stream walk knows how much of its wait
    /// was head slots and how much was the commits behind them, and how many
    /// reads each took; the pull only sees the total.
    pub fn record(&mut self, stage: &'static str, elapsed: Duration, requests: u64) {
        self.add(stage, elapsed, requests);
    }

    fn add(&mut self, stage: &'static str, elapsed: Duration, requests: u64) {
        match self.stages.iter_mut().find(|held| held.name == stage) {
            Some(held) => {
                held.elapsed = held.elapsed.saturating_add(elapsed);
                held.requests = held.requests.saturating_add(requests);
            }
            None => self.stages.push(Stage {
                name: stage,
                elapsed,
                requests,
            }),
        }
    }

    /// Each stage this run has accumulated and the operations charged to it, in
    /// first-seen order — the same content the reported line renders, for a test
    /// that has to see where a choreography's operations landed rather than
    /// read them back off a log.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn counted_stages(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        self.stages.iter().map(|stage| (stage.name, stage.requests))
    }

    /// Reports the breakdown. Called on every exit path, including failures —
    /// a cycle that died halfway is exactly the one whose stage timings matter.
    pub fn report(mut self) {
        self.emit(false);
    }

    /// Reports once. Returns whether this call was the one that reported, so a
    /// test can hold the "exactly once, whichever path gets there first" rule.
    fn emit(&mut self, cancelled: bool) -> bool {
        if std::mem::replace(&mut self.reported, true) {
            return false;
        }
        let counted = self.requests.is_some();
        info!(
            run = self.run,
            total_ms = self.started.elapsed().as_millis() as u64,
            total_requests = counted.then(|| self.issued().saturating_sub(self.started_requests)),
            stages = %StageBreakdown(&self.stages, counted),
            cancelled,
            "Stage timings"
        );
        true
    }
}

/// A run that is dropped without reporting was cancelled — its future was
/// abandoned partway, which no `?` and no explicit call at the end of a function
/// can catch. A device join whose pairing code expires mid-install is exactly
/// that, and it used to leave no trace at all: the stages it had reached died
/// with the future. Reporting from the drop makes the abandoned run say how far
/// it got.
impl Drop for StageTimings {
    fn drop(&mut self) {
        self.emit(true);
    }
}

/// Renders the stages of one run. `counted` decides whether the counts print at
/// all: an uncounted run's zeroes would claim a measurement nobody took.
struct StageBreakdown<'stages>(&'stages [Stage], bool);

impl fmt::Display for StageBreakdown<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let StageBreakdown(stages, counted) = self;
        if stages.is_empty() {
            return formatter.write_str("none");
        }
        for (index, stage) in stages.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{} {}ms", stage.name, stage.elapsed.as_millis())?;
            if *counted {
                write!(formatter, "/{}req", stage.requests)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A counter a test drives by hand, standing in for the storage's.
    #[derive(Default)]
    struct FakeRequests(AtomicU64);

    impl FakeRequests {
        fn issue(&self, operations: u64) {
            self.0.fetch_add(operations, Ordering::Relaxed);
        }
    }

    impl ProviderRequests for FakeRequests {
        fn issued(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn repeated_stages_accumulate_in_first_seen_order() {
        let mut timings = StageTimings::start("test run");
        timings.add("discover streams", Duration::from_millis(30), 3);
        timings.add("materialize", Duration::from_millis(5), 0);
        timings.add("discover streams", Duration::from_millis(12), 1);

        assert_eq!(
            StageBreakdown(&timings.stages, true).to_string(),
            "discover streams 42ms/4req, materialize 5ms/0req"
        );
    }

    /// A run nobody handed a counter reports the line it always did. Printing
    /// `0req` would claim every stage was measured and found free.
    #[test]
    fn an_uncounted_run_reports_times_alone() {
        let mut timings = StageTimings::start("test run");
        timings.add("discover streams", Duration::from_millis(30), 0);
        timings.add("materialize", Duration::from_millis(5), 0);

        assert_eq!(
            StageBreakdown(&timings.stages, false).to_string(),
            "discover streams 30ms, materialize 5ms"
        );
    }

    #[test]
    fn a_run_with_no_stages_reports_none() {
        let timings = StageTimings::start("test run");

        assert_eq!(StageBreakdown(&timings.stages, true).to_string(), "none");
    }

    #[tokio::test]
    async fn a_timed_stage_is_recorded_once_it_completes() {
        let mut timings = StageTimings::start("test run");

        let outcome = timings.stage("verify commits", async { 7_u32 }).await;

        assert_eq!(outcome, 7);
        assert_eq!(
            timings
                .stages
                .iter()
                .map(|stage| stage.name)
                .collect::<Vec<_>>(),
            vec!["verify commits"]
        );
    }

    /// The point of the whole mechanism: a known choreography's operations land
    /// on the stage that issued them, and no other. The stage boundaries are the
    /// only thing dividing one shared running total, so a stage that issues
    /// nothing must not inherit its neighbour's count.
    #[tokio::test]
    async fn each_stage_reports_the_operations_it_issued() {
        let counter = Arc::new(FakeRequests::default());
        counter.issue(9); // A run before this one, on the same home.
        let mut timings = StageTimings::counting("device join", Some(counter.clone()));

        timings
            .stage("discover streams", async { counter.issue(4) })
            .await;
        timings.mark("verify commits", || {});
        timings
            .stage("download the snapshot", async { counter.issue(1) })
            .await;
        timings
            .stage("discover streams", async { counter.issue(2) })
            .await;

        assert_eq!(
            StageBreakdown(&timings.stages, true).to_string(),
            "discover streams 0ms/6req, verify commits 0ms/0req, \
             download the snapshot 0ms/1req"
        );
    }

    /// The run's own total counts from where it began, not from the home's whole
    /// lifetime, and it exceeds the sum of the stages by whatever was issued
    /// between them — the same way unnamed time already exceeds the stage times.
    #[tokio::test]
    async fn a_counted_run_totals_only_its_own_operations() {
        let counter = Arc::new(FakeRequests::default());
        counter.issue(9);
        let mut timings = StageTimings::counting("device join", Some(counter.clone()));

        timings
            .stage("probe the provider", async { counter.issue(1) })
            .await;
        counter.issue(3); // Issued between stages, named by no stage.

        assert_eq!(timings.issued() - timings.started_requests, 4);
    }

    /// Storage nobody wrapped for counting answers `None`, and the run it was
    /// asked for reports the line it always did rather than a column of zeroes
    /// standing in for a measurement nobody took.
    #[tokio::test]
    async fn a_run_over_uncounted_storage_reports_times_alone() {
        let mut timings = StageTimings::counting("device join", None);

        timings.stage("download the snapshot", async {}).await;

        assert_eq!(
            StageBreakdown(&timings.stages, timings.requests.is_some()).to_string(),
            "download the snapshot 0ms"
        );
    }

    /// Times recorded by a caller that counted its own reads carry those counts
    /// through, for splits only the callee can see.
    #[test]
    fn recorded_stages_carry_the_counts_their_caller_measured() {
        let mut timings =
            StageTimings::counting("Store pull", Some(Arc::new(FakeRequests::default())));
        timings.record("fetch heads", Duration::from_millis(120), 6);
        timings.record("fetch commits", Duration::from_millis(80), 14);

        assert_eq!(
            StageBreakdown(&timings.stages, true).to_string(),
            "fetch heads 120ms/6req, fetch commits 80ms/14req"
        );
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    /// A run whose future is abandoned partway never reaches its `report()` —
    /// the live case is a device join whose pairing code expires mid-install.
    /// Its drop reports instead, so the stages it did reach still say so.
    #[test]
    fn an_abandoned_run_reports_from_its_drop() {
        let mut timings = StageTimings::start("abandoned run");
        timings.add("first", Duration::from_millis(3), 0);

        assert!(
            timings.emit(true),
            "a run that never reported reports when it is dropped",
        );
    }

    /// And a run that did report stays quiet when it drops, so an ordinary
    /// completion still logs one line.
    #[test]
    fn a_reported_run_does_not_report_again_when_it_drops() {
        let mut timings = StageTimings::start("reported run");
        timings.add("only", Duration::from_millis(1), 0);

        assert!(timings.emit(false), "the first report is the one that logs");
        assert!(
            !timings.emit(true),
            "its drop finds the run already reported",
        );
    }
}
