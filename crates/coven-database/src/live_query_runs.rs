//! How often a live query has run, counted for tests and benchmarks (§4).

use crate::LiveQueryCause;
use std::sync::Arc;

/// How often a live query has run, for tests and benchmarks that measure
/// how many of its runs writes caused, and how many found nothing new.
#[derive(Clone, Default)]
pub struct LiveQueryRuns {
    counts: Arc<RunCounts>,
}

#[derive(Default)]
struct RunCounts {
    runs: std::sync::atomic::AtomicU64,
    writes: std::sync::atomic::AtomicU64,
    equal: std::sync::atomic::AtomicU64,
}

impl LiveQueryRuns {
    /// The runs that have finished.
    pub fn runs(&self) -> u64 {
        self.counts.runs.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Of those, the runs a write caused, with no new request.
    pub fn writes(&self) -> u64 {
        self.counts.writes.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Of those, the runs whose result equaled the one before, which the
    /// query didn't return.
    pub fn equal(&self) -> u64 {
        self.counts.equal.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Count a finished run, caused by `cause`, whose result was `equal`
    /// to the one before.
    pub(crate) fn count(&self, cause: LiveQueryCause, equal: bool) {
        use std::sync::atomic::Ordering::SeqCst;
        self.counts.runs.fetch_add(1, SeqCst);
        if cause == LiveQueryCause::Write {
            self.counts.writes.fetch_add(1, SeqCst);
            if equal {
                self.counts.equal.fetch_add(1, SeqCst);
            }
        }
    }
}

#[cfg(test)]
#[path = "live_query_runs_tests.rs"]
mod tests;
