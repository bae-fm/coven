//! The reads every app SQL context offers.

use crate::{BulkSqlContext, Mode, SqlContext, SqlReadContext, Writer};
use sqlive::rusqlite::{Params, Row};
use sqlive::CompiledStatement;
use sqlive_compose::Read;

/// Ordinary SQL reads through a read context or a write's, so code that
/// reads the same way inside a write and outside one is written once. Inside
/// a write it sees the write's own changes.
pub trait SqlRead {
    /// The kind of handle the context reads on, whose compiled reads alone
    /// it runs.
    type Handle: Mode;

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

    /// Every row of a read the app's build compiled (`read!`), run with the
    /// scope plan it brings, as `query_compiled` runs it.
    fn fetch<Q>(&self, statement: &Q) -> sqlive::rusqlite::Result<Vec<Q::Row>>
    where
        Q: Read<Handle = Self::Handle>,
    {
        self.query_compiled(Q::compiled(), statement.params().as_slice(), Q::read)
    }

    /// The first row of a read the app's build compiled, as `fetch` reads
    /// it. It fails with `QueryReturnedNoRows` when there is none.
    fn fetch_row<Q>(&self, statement: &Q) -> sqlive::rusqlite::Result<Q::Row>
    where
        Q: Read<Handle = Self::Handle>,
    {
        let first = self.fetch(statement)?.into_iter().next();
        first.ok_or(sqlive::rusqlite::Error::QueryReturnedNoRows)
    }

    /// Map all result rows of a compiled read, which brings the scope plan
    /// its compiler built (sqlive's spec, section 4). It fails with
    /// `CompiledSchemaDiffers` when the store's schema shows an object the
    /// plan read otherwise than the schema the read was compiled against.
    fn query_compiled<T, P, F>(
        &self,
        statement: CompiledStatement<Self::Handle>,
        params: P,
        map: F,
    ) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>;
}

impl<M: Mode> SqlRead for SqlReadContext<'_, M> {
    type Handle = M;

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
        statement: CompiledStatement<M>,
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
    type Handle = Writer;

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
        statement: CompiledStatement<Writer>,
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
    type Handle = Writer;

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
        statement: CompiledStatement<Writer>,
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
