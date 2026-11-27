//! The reads every app SQL context offers.

use crate::{BulkSqlContext, SqlContext, SqlReadContext};
use sqlive::rusqlite::{Params, Row};
use sqlive::CompiledStatement;

/// Ordinary SQL reads through a read context or a write's, so code that
/// reads the same way inside a write and outside one is written once. Inside
/// a write it sees the write's own changes.
pub trait SqlRead {
    /// Map the first result row.
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> sqlive::rusqlite::Result<T>;

    /// Map all result rows.
    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>;

    /// Map all result rows of a compiled read, which brings the scope plan
    /// its compiler built (sqlive's spec, section 4). It fails with
    /// `CompiledSchemaDiffers` when the store's schema shows an object the
    /// plan read otherwise than the schema the read was compiled against.
    fn query_compiled<T, P, F>(
        &self,
        statement: CompiledStatement,
        params: P,
        map: F,
    ) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>;
}

impl SqlRead for SqlReadContext<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        SqlReadContext::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        SqlReadContext::query(self, sql, params, map)
    }

    fn query_compiled<T, P, F>(
        &self,
        statement: CompiledStatement,
        params: P,
        map: F,
    ) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        SqlReadContext::query_compiled(self, statement, params, map)
    }
}

impl SqlRead for SqlContext<'_, '_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        SqlContext::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        SqlContext::query(self, sql, params, map)
    }

    fn query_compiled<T, P, F>(
        &self,
        statement: CompiledStatement,
        params: P,
        map: F,
    ) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        SqlContext::query_compiled(self, statement, params, map)
    }
}

impl SqlRead for BulkSqlContext<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        BulkSqlContext::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        BulkSqlContext::query(self, sql, params, map)
    }

    fn query_compiled<T, P, F>(
        &self,
        statement: CompiledStatement,
        params: P,
        map: F,
    ) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        BulkSqlContext::query_compiled(self, statement, params, map)
    }
}

#[cfg(test)]
#[path = "sql_read_tests.rs"]
mod tests;
