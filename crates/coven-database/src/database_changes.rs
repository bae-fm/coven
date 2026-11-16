//! Commit notifications shared by file work and synchronization.

use crate::engine_error::EngineError;
use crate::store_failure::StoreFailure;
use crate::DbError;

/// A live commit observation. `DatabaseChanges` merges several commits into
/// one change and ends when the writer closes. Its methods convert the
/// handle's failure into `E` and return that.
pub struct DatabaseChanges<E> {
    commits: crate::observation::CommitSubscription,
    reads: sqlive::ReadSet,
    first: bool,
    failed: crate::live_query::Failed<E>,
}

impl<E> DatabaseChanges<E> {
    /// What `commits`, waiting on `reads`, reports; `first` reports the
    /// state now as their first change.
    pub(crate) fn new(
        commits: crate::observation::CommitSubscription,
        reads: sqlive::ReadSet,
        first: bool,
    ) -> Self
    where
        E: From<EngineError<StoreFailure, DbError>>,
    {
        Self { commits, reads, first, failed: E::from }
    }

    /// Whether a relevant commit happened since the previous observation,
    /// without waiting; the first call reports the initial state.
    pub fn take_changed(&mut self) -> Result<bool, E> {
        let state = self.commits.state();
        if let Some(error) = state.error() {
            return Err((self.failed)(error));
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
    /// observation, or the handle failed or closed. It owns its subscription,
    /// so a worker can wait on it beside other wakes; `take_changed` then
    /// consumes the change.
    pub fn changed(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut commits = self.commits.clone();
        let first = self.first;
        async move {
            loop {
                let state = commits.state();
                if first || state.changed || state.error().is_some() {
                    return;
                }
                commits.changed().await;
            }
        }
    }

    /// The initial state, then each relevant commit. No timer or polling is used.
    pub async fn next(&mut self) -> Result<(), E> {
        loop {
            let state = self.commits.state();
            if let Some(error) = state.error() {
                return Err((self.failed)(error));
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

#[cfg(test)]
#[path = "database_changes_tests.rs"]
pub(crate) mod tests;
