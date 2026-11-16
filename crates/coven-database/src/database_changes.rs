//! The store's change watches: what wakes a worker waiting on the rows it
//! names (§4, §6.7).

use super::StoreDatabase;
use crate::DbError;
use std::collections::BTreeSet;

impl StoreDatabase {
    /// Observe new local writes: rows inserted into the outgoing write queue.
    /// Upload confirmation removing a row is this device's own bookkeeping
    /// and wakes nothing (§6.7). Register before the first sync to
    /// include writes committed while a pass is running.
    pub fn sync_changes(&self) -> crate::DatabaseChanges<DbError> {
        self.table_changes(&["_coven_uploads"], true, true)
    }

    /// Observe operations as they start: rows inserted into the journal,
    /// such as an invite, whose expiry the handle wakes to write (§13.2).
    pub fn started_operations(&self) -> crate::DatabaseChanges<DbError> {
        self.table_changes(&["_coven_operations"], true, true)
    }

    /// `first` reports the initial state as a change; `inserted_only`
    /// ignores updates and deletions.
    fn table_changes(
        &self,
        tables: &[&str],
        first: bool,
        inserted_only: bool,
    ) -> crate::DatabaseChanges<DbError> {
        let reads: Vec<_> = tables
            .iter()
            .map(|table| sqlive::TableRead {
                table: (*table).into(),
                columns: BTreeSet::new(),
                scope: sqlive::RowScope::All,
                inserted_only,
            })
            .collect();
        self.observer.changes(reads, first, DbError::from)
    }

    /// Observe, from now, every commit made outside `attempt`: an operation
    /// attempt's own commits are no evidence against the wait it ends in,
    /// and the attempt learns which rows matter only when it ends (§6.7).
    pub fn attempt_commits(&self, attempt: crate::CommitScope) -> crate::AttemptCommits {
        self.observer.attempt(attempt)
    }

    /// Observe `rows` from here on, after `attempt`. The result first reports
    /// whether a commit since the attempt began changed one of them: such a
    /// commit is evidence against the wait the attempt ends in (IO
    /// `racing_commit_wakes`).
    pub fn watch_attempt(
        &self,
        attempt: crate::AttemptCommits,
        rows: &[crate::PrerequisiteRows],
    ) -> crate::DatabaseChanges<DbError> {
        let reads = rows.iter().flat_map(crate::PrerequisiteRows::reads).collect();
        self.observer.watch_attempt(attempt, reads, DbError::from)
    }
}

#[cfg(test)]
#[path = "database_changes_tests.rs"]
mod tests;
