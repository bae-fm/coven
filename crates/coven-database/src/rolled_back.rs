//! A connection's transactions that end by rolling back: a read of one
//! snapshot, and checks whose scratch writes are discarded.

use super::DatabaseConnection;
use crate::{DbError, SqlReadContext};

impl DatabaseConnection {
    /// A one-shot read of one snapshot. Nothing runs it again, so it records
    /// nothing of what it reads, and narrows nothing (§4).
    pub(crate) fn read_snapshot<F, R, E>(&self, read: F) -> Result<R, E>
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E>,
        E: From<DbError>,
    {
        self.read_transaction(|| {
            let _reading = self.authorization.reading();
            read(SqlReadContext::new(self))
        })
    }

    pub(crate) fn read_transaction<R, E: From<DbError>>(
        &self,
        read: impl FnOnce() -> Result<R, E>,
    ) -> Result<R, E> {
        self.batch("BEGIN DEFERRED")?;
        let mut guard = SqlTransaction { database: self, active: true };
        // A read runs no migration and no DDL.
        let constants = self.keep_constants();
        // BEGIN alone does not fix a WAL snapshot. Pin it before invoking a
        // consumer that can wait while a writer commits. Reading the schema
        // table also brings this connection's copy of the schema, and the
        // planner's statistics SQLite loads with it, up to the snapshot's,
        // before the read prepares a statement.
        let result = (|| {
            self.query_row("SELECT count(*) FROM main.sqlite_schema", [], |r| r.get::<_, i64>(0))?;
            read()
        })();
        drop(constants);
        guard.active = false;
        match self.roll_back() {
            Ok(()) => result,
            // A failed read keeps its own cause; the handle now refuses calls.
            Err(rollback) => Err(match result {
                Ok(_) => DbError::from(rollback).into(),
                Err(error) => {
                    tracing::error!(%rollback, "ending a failed read's snapshot failed");
                    error
                }
            }),
        }
    }

    /// Run checks that write scratch state, such as temporary tables, in a
    /// transaction that always rolls back, so nothing they write is kept or
    /// observed. It takes no write lock until something writes the main schema.
    pub(crate) fn discarded_transaction<T>(
        &self,
        run: impl FnOnce(&Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        use crate::WriteFailure;
        self.batch("BEGIN DEFERRED")?;
        let mut guard = SqlTransaction { database: self, active: true };
        let result = run(self);
        guard.active = false;
        match self.roll_back() {
            Ok(()) => result,
            Err(rollback) => Err(match result {
                Ok(_) => DbError::from(rollback),
                Err(error) => error.with_rollback(rollback),
            }),
        }
    }

    /// Roll back the open transaction unless SQLite already ended it. When
    /// ROLLBACK fails, this connection may still be inside the transaction: a
    /// writer would treat its uncommitted changes as committed and a reader
    /// would keep reading its old snapshot, so the handle refuses every later
    /// call until the store is reopened (§4).
    pub(super) fn roll_back(&self) -> rusqlite::Result<()> {
        if self.connection.is_autocommit() {
            return Ok(());
        }
        let result = {
            let _scope = self.authorization.internal();
            self.connection.execute_batch("ROLLBACK")
        };
        if result.is_err() {
            if let Some(handle) = &self.handle {
                handle.fail(crate::observation::HandleFailure::Rollback);
            }
        }
        result
    }
}

/// A read or a discarded transaction, rolled back if its callback panics.
struct SqlTransaction<'a> {
    database: &'a DatabaseConnection,
    active: bool,
}

// Still active only while a callback's panic unwinds, when a second panic
// would abort the process. The panic propagates; a failed rollback is logged
// and fails the handle, so every later call reports it.
impl Drop for SqlTransaction<'_> {
    fn drop(&mut self) {
        if self.active {
            if let Err(rollback) = self.database.roll_back() {
                tracing::error!(%rollback, "rolling back a panicking transaction failed");
            }
        }
    }
}
