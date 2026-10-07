//! Commit notifications shared by file work and synchronization.
use crate::DbError;

/// A live commit observation; coalesces changes and ends when the writer closes.
pub struct DatabaseChanges {
    pub(crate) commits: crate::observation::CommitSubscription,
    pub(crate) reads: crate::observation::ReadSet,
    pub(crate) first: bool,
}
impl DatabaseChanges {
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
