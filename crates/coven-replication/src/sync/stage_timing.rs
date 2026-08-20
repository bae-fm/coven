//! Wall-clock timing for the named stages a sync run is made of.
//!
//! A sync loop iteration, a sync cycle, and a Store pull are each a sequence of
//! named stages — probe the provider, drain uploads, discover streams, verify
//! commits, download blobs, materialize. When a cycle takes twenty seconds the
//! only useful question is which of those stages it spent them in, and the logs
//! have to answer it without a second round of instrumentation.
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
//! [`Clock`](coven_foundation::clock::Clock): this measures how long real work
//! took, not what the store stamps its commits with.

use std::fmt;
use std::future::Future;
use std::time::Duration;

use coven_foundation::clock::Stopwatch;
use tracing::info;

pub struct StageTimings {
    run: &'static str,
    started: Stopwatch,
    stages: Vec<(&'static str, Duration)>,
}

impl StageTimings {
    /// Begins timing a run. `run` names it in the reported line.
    pub fn start(run: &'static str) -> Self {
        Self {
            run,
            started: Stopwatch::start(),
            stages: Vec::new(),
        }
    }

    /// Awaits `work` as the named stage, adding its elapsed time to that
    /// stage's total for this run.
    pub async fn stage<T>(&mut self, stage: &'static str, work: impl Future<Output = T>) -> T {
        let started = Stopwatch::start();
        let outcome = work.await;
        self.add(stage, started.elapsed());
        outcome
    }

    /// Adds time a caller measured itself, for work whose split is only visible
    /// from inside it — a stream walk knows how much of its wait was head slots
    /// and how much was the commits behind them; the pull only sees the total.
    pub fn record(&mut self, stage: &'static str, elapsed: Duration) {
        self.add(stage, elapsed);
    }

    fn add(&mut self, stage: &'static str, elapsed: Duration) {
        match self.stages.iter_mut().find(|(name, _)| *name == stage) {
            Some((_, total)) => *total = total.saturating_add(elapsed),
            None => self.stages.push((stage, elapsed)),
        }
    }

    /// Reports the breakdown. Called on every exit path, including failures —
    /// a cycle that died halfway is exactly the one whose stage timings matter.
    pub fn report(self) {
        info!(
            run = self.run,
            total_ms = self.started.elapsed().as_millis() as u64,
            stages = %StageBreakdown(&self.stages),
            "Sync stage timings"
        );
    }
}

struct StageBreakdown<'stages>(&'stages [(&'static str, Duration)]);

impl fmt::Display for StageBreakdown<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return formatter.write_str("none");
        }
        for (index, (stage, elapsed)) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "{stage} {}ms", elapsed.as_millis())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_stages_accumulate_in_first_seen_order() {
        let mut timings = StageTimings::start("test run");
        timings.add("discover streams", Duration::from_millis(30));
        timings.add("materialize", Duration::from_millis(5));
        timings.add("discover streams", Duration::from_millis(12));

        assert_eq!(
            StageBreakdown(&timings.stages).to_string(),
            "discover streams 42ms, materialize 5ms"
        );
    }

    #[test]
    fn a_run_with_no_stages_reports_none() {
        let timings = StageTimings::start("test run");

        assert_eq!(StageBreakdown(&timings.stages).to_string(), "none");
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
                .map(|(name, _)| *name)
                .collect::<Vec<_>>(),
            vec!["verify commits"]
        );
    }
}
