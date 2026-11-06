//! A write transaction on the writer. It holds the connection exclusively,
//! from `BEGIN IMMEDIATE` until it commits or rolls back, so it alone sets
//! and removes the hooks that observe its changes: SQLite keeps one of each
//! hook per connection, and replacing one while another statement runs
//! would free what that statement still calls.

use super::DatabaseConnection;
use crate::observation::RowChange;
use crate::DbError;
use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::task::{caught_sync, Bug};
use rusqlite::hooks::{Action, PreUpdateCase};
use std::cell::Cell;
use std::sync::{Arc, Mutex};

pub(super) struct WriteTransaction<'a> {
    database: &'a mut DatabaseConnection,
    /// Whether it has yet to commit or roll back.
    open: Cell<bool>,
    /// Whether its pre-update hook is set.
    operations: bool,
    /// Whether its update hook is set.
    rows: bool,
    /// Whether it keeps coven's constants once read (`transaction_constants.rs`).
    constants: bool,
    /// The first failure of one of its hooks: a read of the row that
    /// failed, or a panic. SQLite's hooks can't fail, and rusqlite drops a
    /// hook's panic, so a hook call that failed left what it observes
    /// incomplete: the transaction rolls back with it rather than committing
    /// what its observers missed (E15).
    hook_failure: Arc<Mutex<Option<DbError>>>,
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
            hook_failure: Arc::new(Mutex::new(None)),
        })
    }

    /// Read each of coven's constants once in this transaction, which runs
    /// no migration and no DDL (`transaction_constants.rs`).
    pub(super) fn keep_constants(&mut self) {
        self.database.constants.keep();
        self.constants = true;
    }

    /// Observe each row operation, before SQLite makes it, until
    /// [`Self::stop_observing_operations`] or the transaction ends. A
    /// session takes the same hook, so none may begin meanwhile.
    pub(super) fn observe_operations(
        &mut self,
        mut hook: impl FnMut(Action, &str, &str, &PreUpdateCase) -> Result<(), DbError> + Send + 'static,
    ) -> Result<(), DbError> {
        let failed = self.hook_failure.clone();
        #[cfg(test)]
        let fault = self.database.hook_panics.clone();
        self.database.connection.preupdate_hook(Some(
            move |action, schema: &str, table: &str, case: &PreUpdateCase| {
                let observed =
                    coven_foundation::task::caught_sync("observe a row operation", || {
                        #[cfg(test)]
                        assert!(
                            !fault.swap(false, std::sync::atomic::Ordering::SeqCst),
                            "the capture lost its place"
                        );
                        hook(action, schema, table, case)
                    });
                keep_failure(&failed, observed);
            },
        ))?;
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
        mut hook: impl FnMut(Action, &str, &str, i64) -> Result<(), DbError> + Send + 'static,
    ) -> Result<(), DbError> {
        let failed = self.hook_failure.clone();
        self.database.connection.update_hook(Some(
            move |action, schema: &str, table: &str, rowid| {
                let observed = coven_foundation::task::caught_sync("observe a changed row", || {
                    hook(action, schema, table, rowid)
                });
                keep_failure(&failed, observed);
            },
        ))?;
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
        let failed = lock_unpoisoned(&self.hook_failure).take();
        let outcome = match failed {
            Some(error) => Err(E::from(error)),
            None => outcome,
        };
        let result = outcome.and_then(|(result, changes)| {
            let judged = match &database.handle {
                Some(handle) => Some(database.judge(handle, changes)?),
                None => None,
            };
            #[cfg(any(test, feature = "test-utils"))]
            database.checkpoint(crate::test_utils::WriteCheckpoint::Judged);
            database.batch("COMMIT")?;
            // The write took effect, whatever follows, so it returns as
            // committed. A panic telling the handle's live queries may have
            // left one untold of it, so it fails the handle, and the call
            // running the write returns the bug (E15).
            let told = caught_sync("tell the live queries of a commit", || {
                if let Some(checkpoints) = &database.checkpoints {
                    checkpoints.wake();
                }
                if let Some(statistics) = &database.statistics {
                    statistics.committed(database.changed_rows());
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
            });
            if let (Err(bug), Some(handle)) = (told, &database.handle) {
                handle.fail_with_bug(bug);
            }
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

/// Keep a hook call's failure, the first one, for the commit to fail with.
fn keep_failure(failed: &Mutex<Option<DbError>>, observed: Result<Result<(), DbError>, Bug>) {
    let failure = match observed {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(bug) => DbError::Bug(bug),
    };
    lock_unpoisoned(failed).get_or_insert(failure);
}

/// A transaction still open as it drops is one whose callback panicked,
/// while a second panic would abort the process: it rolls back, and a
/// failed rollback is logged and fails the handle, so every later call
/// reports it. Its hooks go either way; one SQLite won't let go fails the
/// handle too, since the connection's hooks are then unknown.
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
            let removed = self
                .database
                .connection
                .preupdate_hook(None::<fn(Action, &str, &str, &PreUpdateCase)>);
            self.database.removed_hook("the pre-update hook", removed);
        }
        if self.rows {
            let removed = self.database.connection.update_hook(None::<fn(Action, &str, &str, i64)>);
            self.database.removed_hook("the update hook", removed);
        }
    }
}
