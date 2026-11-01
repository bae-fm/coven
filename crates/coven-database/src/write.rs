//! Transaction-scoped app SQL. The connection remains inside its owner.

use rusqlite::{Params, Row};

/// App SQL inside one local write, and coven's steps beside it (§4).
/// Returning an error rolls back every statement and step. This borrowed
/// context cannot change the transaction or access coven's tables.
pub struct SqlContext<'connection, 'write> {
    files: &'write crate::file_write::FileWrite<'connection>,
}

impl<'connection, 'write> SqlContext<'connection, 'write> {
    pub(crate) fn new(files: &'write crate::file_write::FileWrite<'connection>) -> Self {
        Self { files }
    }

    /// Record a move of these files of this device, on it only, to the
    /// cloud (§17.3). It survives restarts; once every file is stored, one
    /// write switches them all to the cloud, changing no app row.
    pub fn move_to_cloud(
        &self,
        files: Vec<crate::MoveFile>,
    ) -> Result<crate::MoveId, crate::DbError> {
        self.files.step(|| self.files.move_to_cloud(files))
    }

    /// Cancel a recorded move: every file stays on this device only, and
    /// the move's record ends (§17.3).
    pub fn cancel_move(&self, id: crate::MoveId) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.cancel_move(id))
    }

    /// Release the pins on these files' stored bytes and evict each of their
    /// namespaces down to its budget, in this write (§17.8). A file this
    /// device has no record of, or one on one device only, has no pin to
    /// release. The evicted bytes go once the write has committed.
    pub fn unpin(
        &self,
        files: &[coven_foundation::id_source::FileId],
    ) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.unpin(files))
    }

    /// Remove a stored file's cached copies, pinned or not, in this write,
    /// with no storage operation (§17.8). A file this device has no record
    /// of, or one on one device only, has none. The bytes go once the write
    /// has committed.
    pub fn evict_file(
        &self,
        file: coven_foundation::id_source::FileId,
    ) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.evict(file))
    }

    /// Set a declared namespace's cache budget and evict its unpinned
    /// entries down to it, in this write (§17.8). An undeclared namespace
    /// fails with `UndeclaredNamespace`. The evicted bytes go once the write
    /// has committed.
    pub fn set_cache_budget(&self, namespace: &str, bytes: u64) -> Result<(), crate::DbError> {
        self.files.step(|| self.files.set_cache_budget(namespace, bytes))
    }

    /// Run one of sync's steps on the journal in this write (§4, §11). A
    /// failed step fails the write, even if the closure goes on and returns
    /// success, since it may have made only part of its change.
    pub fn journal_step<T, E>(
        &self,
        step: impl FnOnce(&crate::Journal<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        self.files.step(|| step(&crate::Journal::new(self.files)))
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.files.execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.files.execute_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.files.query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.files.query(sql, params, map)
    }
}

/// App SQL inside one bulk write (§4): it changes device-local tables
/// alone, and may read any table app SQL reads. Returning an error rolls back
/// every statement. This borrowed context cannot change the transaction,
/// access coven's tables or write a synced one.
pub struct BulkSqlContext<'connection> {
    database: &'connection crate::sqlite::DatabaseConnection,
}

impl<'connection> BulkSqlContext<'connection> {
    pub(crate) fn new(database: &'connection crate::sqlite::DatabaseConnection) -> Self {
        Self { database }
    }

    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.database.app_execute(sql, params)
    }

    /// Execute app statements within this write's transaction.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.database.app_batch(sql)
    }

    /// Map one app row, including changes already made by this write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query_row(sql, params, map)
    }

    /// Map all app rows produced by a query within this write.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.database.app_query(sql, params, map)
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
pub(crate) mod tests;
