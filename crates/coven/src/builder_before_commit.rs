//! The app's before-commit function on the builder (E1).

use super::*;

/// The app's before-commit function, as the builder keeps it.
pub(super) type BeforeCommit = dyn Fn(&CommitSql<'_>) -> Result<(), DbError> + Send + Sync;

impl CovenBuilder {
    /// The app's before-commit function (§9.4): coven runs it last in each
    /// transaction that changes the app's rows, so the app stores what its
    /// own rules compute from them in the same transaction. A local write, a
    /// bulk write, an applied slot, a snapshot load and a migration as the
    /// store opens each run it once, after every other change of the
    /// transaction; coven's own transactions, such as one that pins a file,
    /// don't. It reads any table app SQL reads and writes device-local
    /// tables only: a statement that would write a synced table fails with
    /// `SyncedTableBeforeCommit`. An error, or a statement of it that
    /// failed, fails the transaction: a write returns the error, an applied
    /// slot or a load doesn't commit, the pass reports the error and the next
    /// pass tries again, and a migration fails the open. Without one,
    /// nothing runs.
    pub fn before_commit<F>(mut self, function: F) -> Self
    where
        F: Fn(&CommitSql<'_>) -> Result<(), DbError> + Send + Sync + 'static,
    {
        self.before_commit = Some(Arc::new(function));
        self
    }
}
