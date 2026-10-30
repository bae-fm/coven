//! The reads every app SQL context offers.

use crate::{BulkSqlContext, SqlContext, SqlReadContext};
use rusqlite::{Params, Row};

/// Ordinary SQL reads through a read context or a write's, so code that
/// reads the same way inside a write and outside one is written once. Inside
/// a write it sees the write's own changes.
pub trait SqlRead {
    /// Map the first result row.
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>;

    /// Map all result rows.
    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>;
}

impl SqlRead for SqlReadContext<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        SqlReadContext::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        SqlReadContext::query(self, sql, params, map)
    }
}

impl SqlRead for SqlContext<'_, '_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        SqlContext::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        SqlContext::query(self, sql, params, map)
    }
}

impl SqlRead for BulkSqlContext<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        BulkSqlContext::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        BulkSqlContext::query(self, sql, params, map)
    }
}

#[cfg(test)]
#[path = "sql_read_tests.rs"]
mod tests;
