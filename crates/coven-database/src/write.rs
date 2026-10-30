//! Transaction-scoped app SQL. The connection remains inside its owner.

use rusqlite::{Params, Row};

/// App SQL inside one local write. Returning an error rolls back every statement.
/// This borrowed context cannot change the transaction or access coven's tables.
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
        self.files.move_to_cloud(files)
    }

    /// Cancel a recorded move: every file stays on this device only, and
    /// the move's record ends (§17.3).
    pub fn cancel_move(&self, id: crate::MoveId) -> Result<(), crate::DbError> {
        self.files.cancel_move(id)
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

#[cfg(test)]
#[path = "write_tests.rs"]
pub(crate) mod tests;
