//! Commit notifications for work that waits on what the database holds.

use crate::handle::{CommitSubscription, Failed};

/// A live commit observation. `DatabaseChanges` merges several commits into
/// one change and ends when the writer closes. Its methods convert the
/// handle's failure into `E` and return that.
pub struct DatabaseChanges<E> {
    commits: CommitSubscription,
    reads: crate::ReadSet,
    first: bool,
    failed: Failed<E>,
}

impl<E> DatabaseChanges<E> {
    /// Report each commit `commits` sees that changes what `reads` read, and
    /// turn the handle's failure into `E` with `failed`. With `first`, the
    /// first report is the state now.
    pub(crate) fn new(
        commits: CommitSubscription,
        reads: crate::ReadSet,
        first: bool,
        failed: Failed<E>,
    ) -> Self {
        Self { commits, reads, first, failed }
    }

    /// Whether a relevant commit happened since the previous observation,
    /// without waiting; the first call reports the initial state.
    pub fn take_changed(&mut self) -> Result<bool, E> {
        let state = self.commits.state();
        if let Some(stop) = state.stopped_by() {
            return Err((self.failed)(stop));
        }
        if self.first || state.changed {
            self.first = false;
            self.commits.begin();
            self.commits.finish(self.reads.clone());
            return Ok(true);
        }
        Ok(false)
    }

    /// Resolves once a relevant commit happened since the previous
    /// observation, or the handle failed or closed. The future owns a clone
    /// of the subscription, so a worker can wait on it beside other futures;
    /// `take_changed` then consumes the change.
    pub fn changed(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut commits = self.commits.clone();
        let first = self.first;
        async move {
            loop {
                let state = commits.state();
                if first || state.changed || state.stopped() {
                    return;
                }
                commits.changed().await;
            }
        }
    }

    /// The initial state, then each relevant commit. `next` waits on the
    /// commit subscription, with no timer or polling.
    pub async fn next(&mut self) -> Result<(), E> {
        loop {
            let state = self.commits.state();
            if let Some(stop) = state.stopped_by() {
                return Err((self.failed)(stop));
            }
            if self.first || state.changed {
                self.first = false;
                self.commits.begin();
                self.commits.finish(self.reads.clone());
                return Ok(());
            }
            self.commits.changed().await;
        }
    }
}
