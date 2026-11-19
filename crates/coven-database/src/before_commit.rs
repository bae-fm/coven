//! The app's before-commit function (§9.4): app Rust that coven runs last in
//! each transaction that changes the app's rows, whether this device wrote
//! them, applied another device's or loaded a snapshot, so the app can store
//! what its own rules compute from them in the same transaction. Like a local
//! trigger, it writes device-local tables only.

use crate::sqlite::StoreSql;
use crate::DbError;
use sqlive::rusqlite::{Params, Row};
use std::cell::Cell;

/// App SQL inside the app's before-commit function: it reads any table app
/// SQL reads, the transaction's changes included, and writes device-local
/// tables only. A statement that would write a synced table, itself or
/// through a trigger or foreign-key action, fails with
/// `SyncedTableBeforeCommit`. Returning an error rolls back the transaction
/// the function runs in.
pub struct CommitSql<'a> {
    database: &'a StoreSql,
    /// Whether one of the function's statements failed, which fails the
    /// transaction even if the function goes on (§4).
    failed: Cell<bool>,
}

impl CommitSql<'_> {
    /// Execute one app statement with parameters.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> sqlive::rusqlite::Result<usize> {
        self.statement(|| self.database.app_execute(sql, params))
    }

    /// Execute app statements within the transaction.
    pub fn execute_batch(&self, sql: &str) -> sqlive::rusqlite::Result<()> {
        self.statement(|| self.database.app_batch(sql))
    }

    /// Map one app row, including the transaction's changes.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        self.statement(|| self.database.app_query_row(sqlive::Recorded::No, sql, params, map))
    }

    /// Map all app rows a query produces, including the transaction's
    /// changes.
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        self.statement(|| self.database.app_query(sqlive::Recorded::No, sql, params, map))
    }

    /// Run one statement. One SQLite fails may have kept part of its change,
    /// so it fails the transaction even if the function goes on. An answer
    /// with no row, or the function's own mapping of a row, is no failure of
    /// the statement.
    fn statement<T>(
        &self,
        run: impl FnOnce() -> sqlive::rusqlite::Result<T>,
    ) -> sqlive::rusqlite::Result<T> {
        let result = run();
        if matches!(result, Err(sqlive::rusqlite::Error::SqliteFailure(..))) {
            self.failed.set(true);
        }
        result
    }
}

impl crate::SqlRead for CommitSql<'_> {
    fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        CommitSql::query_row(self, sql, params, map)
    }

    fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> sqlive::rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> sqlive::rusqlite::Result<T>,
    {
        CommitSql::query(self, sql, params, map)
    }
}

/// The app's before-commit function, as the database keeps it.
pub(crate) type BeforeCommit = dyn Fn(&CommitSql<'_>) -> Result<(), DbError> + Send + Sync;

/// Run the app's before-commit function, if it registered one, as the last
/// step of the transaction `database` holds. Its error, or a statement of it
/// that failed, fails the transaction.
pub(crate) fn run(database: &StoreSql, function: Option<&BeforeCommit>) -> Result<(), DbError> {
    let Some(function) = function else {
        return Ok(());
    };
    let _scope = database.local_scope();
    let sql = CommitSql { database, failed: Cell::new(false) };
    function(&sql)?;
    // SQLite ending the transaction says more than the statement that
    // ended it.
    database.require_transaction().map_err(DbError::from)?;
    if sql.failed.get() {
        return Err(DbError::StatementFailed);
    }
    Ok(())
}

#[cfg(test)]
#[path = "before_commit_tests.rs"]
mod tests;
