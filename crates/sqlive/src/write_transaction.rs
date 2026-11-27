//! A write transaction on the writer. It holds the connection exclusively,
//! from `BEGIN IMMEDIATE` until it commits or rolls back, so it alone sets
//! and removes the hooks that observe its changes: SQLite keeps one of each
//! hook per connection, and replacing one while another statement runs
//! would free what that statement still calls.

use super::HostSql;
use crate::authorization::Policy;
use crate::authorization::PolicyError;
use crate::net_changes::Captured;
use panic_as_error::locks::lock_unpoisoned;
use panic_as_error::{caught_sync, Bug};
use rusqlite::hooks::{Action, PreUpdateCase};
use std::cell::Cell;
use std::sync::{Arc, Mutex};

pub(super) struct WriteTransaction<'a, P: Policy> {
    database: &'a mut HostSql<P>,
    /// Whether it has yet to commit or roll back.
    open: Cell<bool>,
    /// Whether its pre-update hook is set.
    operations: bool,
    /// Whether it keeps the schema cookie once read (`schema_cookie.rs`).
    cookie: bool,
    /// The first failure of one of its hooks: a read of the row that
    /// failed, or a panic. SQLite's hooks can't fail, and rusqlite drops a
    /// hook's panic, so a hook call that failed left what it observes
    /// incomplete: the transaction rolls back with it rather than committing
    /// what its observers missed (E15).
    hook_failure: Arc<Mutex<Option<PolicyError<P>>>>,
}

impl<'a, P: Policy> WriteTransaction<'a, P> {
    /// Begin a write on `database`. A streamed one keeps its page cache
    /// bounded as it goes (`stream_capture.rs`).
    pub(super) fn begin(
        database: &'a mut HostSql<P>,
        streaming: bool,
    ) -> Result<Self, PolicyError<P>> {
        database.batch("BEGIN IMMEDIATE")?;
        database.streaming.set(streaming);
        let transaction = Self {
            database,
            open: Cell::new(true),
            operations: false,
            cookie: false,
            hook_failure: Arc::new(Mutex::new(None)),
        };
        // The write holds the database's write lock from here, so no commit
        // comes between its snapshot and the host's values it takes
        // (`snapshot_values.rs`).
        transaction.database.snapshot_values.take();
        Ok(transaction)
    }

    /// Read the schema cookie once in this transaction, which runs no DDL
    /// (`schema_cookie.rs`).
    pub(super) fn keep_schema_cookie(&mut self) {
        if !self.cookie {
            self.database.cookie.hold();
            self.cookie = true;
        }
    }

    /// Observe each row operation, before SQLite makes it, until
    /// [`Self::stop_observing_operations`] or the transaction ends. A
    /// session takes the same hook, so none may begin meanwhile.
    pub(super) fn observe_operations(
        &mut self,
        mut hook: impl FnMut(Action, &str, &str, &PreUpdateCase) -> Result<(), PolicyError<P>>
            + Send
            + 'static,
    ) -> Result<(), PolicyError<P>> {
        let failed = self.hook_failure.clone();
        #[cfg(any(test, feature = "test-utils"))]
        let fault = self.database.hook_panics.clone();
        self.database.connection.preupdate_hook(Some(
            move |action, schema: &str, table: &str, case: &PreUpdateCase| {
                let observed = panic_as_error::caught_sync("observe a row operation", || {
                    #[cfg(any(test, feature = "test-utils"))]
                    assert!(
                        !fault.swap(false, std::sync::atomic::Ordering::SeqCst),
                        "the capture lost its place"
                    );
                    hook(action, schema, table, case)
                });
                keep_failure::<P>(&failed, observed);
            },
        ))?;
        self.operations = true;
        Ok(())
    }

    pub(super) fn stop_observing_operations(&mut self) -> Result<(), PolicyError<P>> {
        if self.operations {
            self.database
                .connection
                .preupdate_hook(None::<fn(Action, &str, &str, &PreUpdateCase)>)?;
            self.operations = false;
        }
        Ok(())
    }

    /// Run `run` on the transaction's connection, which it keeps; `run`
    /// can't change the transaction's hooks.
    pub(super) fn within<T>(&self, run: impl FnOnce(&HostSql<P>) -> T) -> T {
        run(self.database)
    }

    /// Judge `outcome`'s changes, each row's net change over the
    /// transaction, for the handle's live queries, commit and report them;
    /// or roll back, when `outcome` or any of that fails.
    pub(super) fn commit<T, E: crate::RollbackJoin<P::Failure, P::Refusal>>(
        &self,
        outcome: Result<(T, Captured), E>,
    ) -> Result<T, E> {
        let database = &*self.database;
        let failed = lock_unpoisoned(&self.hook_failure).take();
        let outcome = match failed {
            Some(error) => Err(E::from(error)),
            None => outcome,
        };
        let result = outcome.and_then(|(result, changes)| {
            // No change to the host tables comes between this write's
            // judging, its commit and its report (`host_tables.rs`). The
            // write's statements read the rows as it began; its lookups read
            // them as they are now, since the write commits after every
            // change made so far.
            let _held = database.host_tables.as_ref().map(crate::HostTables::hold);
            database.snapshot_values.take();
            let judged = match &database.handle {
                Some(handle) => Some(database.judge(handle, changes.finish())?),
                None => None,
            };
            #[cfg(any(test, feature = "test-utils"))]
            database.commit_point(crate::test_records::CommitPoint::Judged);
            {
                // No read fixes its snapshot while the commit runs
                // (`snapshot_values.rs`).
                let _ordered = database.commit_order.committing();
                database.batch("COMMIT")?;
            }
            database.snapshot_values.forget();
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
                database.commit_point(crate::test_records::CommitPoint::Committed);
                if let (Some(handle), Some(judged)) = (&database.handle, judged) {
                    handle.commit(judged, crate::committing());
                }
                #[cfg(any(test, feature = "test-utils"))]
                database.commit_point(crate::test_records::CommitPoint::Observed);
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
fn keep_failure<P: Policy>(
    failed: &Mutex<Option<PolicyError<P>>>,
    observed: Result<Result<(), PolicyError<P>>, Bug>,
) {
    let failure = match observed {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(bug) => crate::Error::Bug(bug),
    };
    lock_unpoisoned(failed).get_or_insert(failure);
}

/// A transaction still open as it drops is one whose callback panicked,
/// while a second panic would abort the process: it rolls back, and a
/// failed rollback is logged and fails the handle, so every later call
/// reports it. Its hooks go either way; one SQLite won't let go fails the
/// handle too, since the connection's hooks are then unknown.
impl<P: Policy> Drop for WriteTransaction<'_, P> {
    fn drop(&mut self) {
        self.database.streaming.set(false);
        if self.cookie {
            self.database.cookie.release();
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
    }
}
