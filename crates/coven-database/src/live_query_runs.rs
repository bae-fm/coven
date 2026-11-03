//! How often a live query has run, counted for tests and benchmarks (§4).

use crate::LiveQueryCause;

/// How often a live query had run when asked, for tests and benchmarks
/// that measure how many of its runs writes caused, and how many found
/// nothing new.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveQueryRuns {
    runs: u64,
    writes: u64,
    equal: u64,
}

impl LiveQueryRuns {
    /// The runs that have finished.
    pub fn runs(&self) -> u64 {
        self.runs
    }

    /// Of those, the runs a write caused, with no new request.
    pub fn writes(&self) -> u64 {
        self.writes
    }

    /// Of those, the runs whose result equaled the one before, which the
    /// query didn't return.
    pub fn equal(&self) -> u64 {
        self.equal
    }

    /// Count a finished run, caused by `cause`, whose result was `equal`
    /// to the one before.
    pub(crate) fn count(&mut self, cause: LiveQueryCause, equal: bool) {
        self.runs += 1;
        if cause == LiveQueryCause::Write {
            self.writes += 1;
            if equal {
                self.equal += 1;
            }
        }
    }
}

#[cfg(test)]
#[path = "live_query_runs_tests.rs"]
mod tests;
