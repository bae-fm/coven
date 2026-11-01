//! The app's write: its SQL and coven's steps in one transaction (§4, E3).

use crate::*;
use coven_database::SqlContext;

/// One write's SQL and coven's steps, in one transaction (§4, E3). A step
/// runs where it is called: the write's later SQL sees what it changed, and
/// it commits or rolls back with the app's statements. A failed step fails
/// the write, even if the closure goes on and returns success. Returning an
/// error rolls back every statement and step.
pub struct CovenWrite<'write> {
    sql: SqlContext<'write, 'write>,
    /// The store's operation owner, whose steps run in this write.
    storage: &'write crate::storage::StorageConnections,
}

impl<'write> CovenWrite<'write> {
    pub(crate) fn new(
        sql: SqlContext<'write, 'write>,
        storage: &'write crate::storage::StorageConnections,
    ) -> Self {
        Self { sql, storage }
    }

    /// Records a move of these files to the cloud (§17.3).
    pub fn move_to_cloud(&self, files: Vec<MoveFile>) -> Result<MoveId, DbError> {
        self.sql.move_to_cloud(files)
    }

    /// Cancels a move: every file stays on this device only (§17.3).
    pub fn cancel_move(&self, id: MoveId) -> Result<(), DbError> {
        self.sql.cancel_move(id)
    }

    /// Releases the pins on these files' stored bytes, then evicts each of
    /// their namespaces down to its budget (§17.8). A file this device has
    /// no record of, or one on one device only, has no pin to release. A
    /// pin coven keeps for a switch to the cloud stays (§17.8). The evicted
    /// bytes go once the write has committed.
    pub fn unpin(&self, files: &[FileId]) -> Result<(), DbError> {
        self.sql.unpin(files)
    }

    /// Removes a stored file's cached copies, pinned or not, without
    /// changing storage or a local original (§17.8). A file this device has
    /// no record of, or one on one device only, has none. The bytes go once
    /// the write has committed.
    pub fn evict_file(&self, file: FileId) -> Result<(), DbError> {
        self.sql.evict_file(file)
    }

    /// Makes an operation's retry or undo due now, from its recorded
    /// progress (E6): its pending record and retry wait end in this write.
    /// It fails with `NotPending` for an operation that has finished or
    /// never existed, and with `Pending` and the record for one whose
    /// record never retries, or only after an update not installed yet. It
    /// can't bypass a provider cooldown. Once the write commits, a pass runs
    /// the operation as soon as storage is connected.
    pub fn retry_pending_operation(&self, operation: OperationId) -> Result<(), OperationError> {
        self.sql.journal_step(|journal| self.storage.retry_pending_operation(journal, operation))
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> SqliteResult<usize> {
        self.sql.execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> SqliteResult<()> {
        self.sql.execute_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqliteResult<T>,
    {
        self.sql.query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> SqliteResult<T>,
    {
        self.sql.query(sql, params, map)
    }
}

impl SqlRead for CovenWrite<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqliteResult<T>,
    {
        CovenWrite::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> SqliteResult<T>,
    {
        CovenWrite::query(self, sql, params, map)
    }
}

#[cfg(test)]
#[path = "coven_write_tests.rs"]
mod tests;
