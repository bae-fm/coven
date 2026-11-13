//! The app's write as a member begins: the creator's first write (§10.3, E1).

use crate::*;
use coven_database::SqlContext;

/// The app's SQL as a member begins, in one write with the membership. The
/// creator's first write runs on it, inside `create_store`, after slot 1 has
/// registered the creator and their device and made the circles the app
/// asked for, and before the store is visible (§10.3). It offers the app's
/// SQL only: no operation, file or administrative step exists on it, so what
/// it commits is the member's own rows and nothing in the store's
/// membership.
pub struct MemberWrite<'write> {
    sql: SqlContext<'write, 'write>,
}

impl<'write> MemberWrite<'write> {
    pub(crate) fn new(sql: SqlContext<'write, 'write>) -> Self {
        Self { sql }
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

impl SqlRead for MemberWrite<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> SqliteResult<T>,
    {
        MemberWrite::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> SqliteResult<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> SqliteResult<T>,
    {
        MemberWrite::query(self, sql, params, map)
    }
}
