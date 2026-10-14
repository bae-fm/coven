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

    /// Insert a row and attach its already checked original in the same write.
    pub fn insert_user_file(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
        prepared: crate::PreparedUserFile,
        insert_sql: &str,
        params: &[(&str, &dyn rusqlite::ToSql)],
    ) -> Result<(), crate::DbError> {
        self.execute(insert_sql, params)?;
        self.register_user_file(table, key, prepared)
    }

    /// Attach an original, checking its recorded facts and the row's size.
    pub fn register_user_file(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
        prepared: crate::PreparedUserFile,
    ) -> Result<(), crate::DbError> {
        self.files.register(table, key.into(), prepared)
    }

    /// Detach the original by clearing hash and location; never touch its bytes.
    pub fn clear_user_file(
        &self,
        table: &str,
        key: impl Into<crate::RowKey>,
    ) -> Result<(), crate::DbError> {
        self.files.clear(table, key.into())
    }

    /// Refuse this write if an earlier reference no longer names the row's file.
    /// Call before changing or deleting that row.
    pub fn validate_file_ref(&self, reference: &crate::FileRef) -> Result<(), crate::DbError> {
        self.files.validate_file_ref(reference)
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
