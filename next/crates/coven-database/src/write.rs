//! Transaction-scoped app SQL. The connection remains inside its owner.

use rusqlite::{Params, Row};

use crate::sqlite::DatabaseConnection;

/// App SQL inside one local write. Returning an error rolls back every statement.
/// This borrowed context cannot change the transaction or access coven's tables.
pub struct SqlContext<'connection> {
    database: &'connection DatabaseConnection,
}

impl<'connection> SqlContext<'connection> {
    pub(crate) fn new(database: &'connection DatabaseConnection) -> Self {
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
