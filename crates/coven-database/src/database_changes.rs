//! Commit notifications shared by file work and synchronization.
use crate::DbError;

/// A live commit observation; coalesces changes and ends when the writer closes.
pub struct DatabaseChanges {
    pub(crate) commits: crate::observation::CommitSubscription,
    pub(crate) reads: crate::observation::ReadSet,
    pub(crate) first: bool,
}
impl DatabaseChanges {
    /// Whether a relevant commit happened since the previous observation,
    /// without waiting; the first call reports the initial state.
    pub fn take_changed(&mut self) -> Result<bool, DbError> {
        let state = self.commits.state();
        if let Some(error) = state.error() {
            return Err(error);
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
    pub async fn next(&mut self) -> Result<(), DbError> {
        loop {
            let state = self.commits.state();
            if let Some(error) = state.error() {
                return Err(error);
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
mod tests;
