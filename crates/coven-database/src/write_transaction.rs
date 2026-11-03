//! A write transaction on the writer. It holds the connection exclusively,
//! from `BEGIN IMMEDIATE` until it commits or rolls back, so it alone sets
//! and removes the hooks that observe its changes: SQLite keeps one of each
//! hook per connection, and replacing one while another statement runs
//! would free what that statement still calls.

use super::DatabaseConnection;
use crate::observation::RowChange;
use crate::DbError;
use rusqlite::hooks::{Action, PreUpdateCase};
use std::cell::Cell;

pub(super) struct WriteTransaction<'a> {
    database: &'a mut DatabaseConnection,
    /// Whether it has yet to commit or roll back.
    open: Cell<bool>,
    /// Whether its pre-update hook is set.
    operations: bool,
    /// Whether its update hook is set.
    rows: bool,
    /// Whether it keeps coven's constants once read (`write_constants.rs`).
    constants: bool,
}

impl<'a> WriteTransaction<'a> {
    /// Begin a write on `database`. A streamed one keeps its page cache
    /// bounded as it goes (`stream_capture.rs`).
    pub(super) fn begin(
        database: &'a mut DatabaseConnection,
        streaming: bool,
    ) -> Result<Self, DbError> {
        database.batch("BEGIN IMMEDIATE")?;
        database.streaming.set(streaming);
        Ok(Self {
            database,
            open: Cell::new(true),
            operations: false,
            rows: false,
            constants: false,
        })
    }

    /// Read each of coven's constants once in this transaction, which runs
    /// no migration and no DDL (`write_constants.rs`).
    pub(super) fn keep_constants(&mut self) {
        self.database.constants.keep();
        self.constants = true;
    }

    /// Observe each row operation, before SQLite makes it, until
    /// [`Self::stop_observing_operations`] or the transaction ends. A
    /// session takes the same hook, so none may begin meanwhile.
    pub(super) fn observe_operations(
        &mut self,
        hook: impl FnMut(Action, &str, &str, &PreUpdateCase) + Send + 'static,
    ) -> Result<(), DbError> {
        self.database.connection.preupdate_hook(Some(hook))?;
        self.operations = true;
        Ok(())
    }

    pub(super) fn stop_observing_operations(&mut self) -> Result<(), DbError> {
        if self.operations {
            self.database
                .connection
                .preupdate_hook(None::<fn(Action, &str, &str, &PreUpdateCase)>)?;
            self.operations = false;
        }
        Ok(())
    }

    /// Observe which rowid each changed row of a rowid table had, until the
    /// transaction ends.
    pub(super) fn observe_rows(
        &mut self,
        hook: impl FnMut(Action, &str, &str, i64) + Send + 'static,
    ) -> Result<(), DbError> {
        self.database.connection.update_hook(Some(hook))?;
        self.rows = true;
        Ok(())
    }

    /// Run `run` on the transaction's connection, which it keeps; `run`
    /// can't change the transaction's hooks.
    pub(super) fn within<T>(&self, run: impl FnOnce(&DatabaseConnection) -> T) -> T {
        run(self.database)
    }

    /// Judge `outcome`'s changes for the handle's live queries, commit and
    /// report them; or roll back, when `outcome` or any of that fails.
    pub(super) fn commit<T, E: crate::WriteFailure>(
        &self,
        outcome: Result<(T, Vec<RowChange>), E>,
    ) -> Result<T, E> {
        let database = &*self.database;
        let result = outcome.and_then(|(result, changes)| {
            let judged = match &database.handle {
                Some(handle) => Some(database.judge(handle, changes)?),
                None => None,
            };
            #[cfg(any(test, feature = "test-utils"))]
            database.checkpoint(crate::test_utils::WriteCheckpoint::Judged);
            database.batch("COMMIT")?;
            if let Some(checkpoints) = &database.checkpoints {
                checkpoints.committed();
            }
            #[cfg(any(test, feature = "test-utils"))]
            database.record_work(|work| work.commits += 1);
            #[cfg(any(test, feature = "test-utils"))]
            database.checkpoint(crate::test_utils::WriteCheckpoint::Committed);
            if let (Some(handle), Some(judged)) = (&database.handle, judged) {
                handle.commit(judged, crate::commit_scope::committing());
            }
            #[cfg(any(test, feature = "test-utils"))]
            database.checkpoint(crate::test_utils::WriteCheckpoint::Observed);
            Ok(result)
        });
        self.open.set(false);
        match result {
            Ok(result) => Ok(result),
            Err(operation) => match database.roll_back() {
                Ok(()) => Err(operation),
                Err(rollback) => Err(operation.with_rollback(rollback)),
            },
        }
    }
}

/// A transaction still open as it drops is one whose callback panicked,
/// while a second panic would abort the process: it rolls back, and a
/// failed rollback is logged and fails the handle, so every later call
/// reports it. Its hooks go either way.
impl Drop for WriteTransaction<'_> {
    fn drop(&mut self) {
        self.database.streaming.set(false);
        if self.constants {
            self.database.constants.forget();
        }
        if self.open.get() {
            if let Err(rollback) = self.database.roll_back() {
                tracing::error!(%rollback, "rolling back a panicking transaction failed");
            }
        }
        if self.operations {
            self.database
                .connection
                .preupdate_hook(None::<fn(Action, &str, &str, &PreUpdateCase)>)
                .expect("remove a transaction's pre-update hook");
        }
        if self.rows {
            self.database
                .connection
                .update_hook(None::<fn(Action, &str, &str, i64)>)
                .expect("remove a transaction's update hook");
        }
    }
}
