//! A connection's part in live queries' judging of writes (§4): a live
//! query's run, which judges the writes that committed while it ran, a
//! write's judging before it commits, and the lookups both run in their own
//! transaction.

use super::DatabaseConnection;
use crate::observation::{CommitObserver, CommitSubscription, ReadSet};
use crate::{DbError, SqlReadContext};

impl DatabaseConnection {
    /// A live query's run on one snapshot. What its statements read goes
    /// to `subscription` before the snapshot ends, which judges the writes
    /// that committed during the run by lookups in that snapshot (§4).
    pub(crate) fn live_read<F, R, E>(
        &self,
        subscription: &CommitSubscription,
        read: F,
    ) -> Result<R, E>
    where
        F: FnOnce(SqlReadContext<'_>) -> Result<R, E>,
        E: From<DbError>,
    {
        let mut finished = false;
        let result = self.read_transaction(|| {
            let _reading = self.authorization.live_reading();
            let result = read(SqlReadContext::new(self));
            let reads = self.authorization.reads();
            finished = true;
            let judged = subscription.finish_judging(reads, &mut |lookup, value| {
                self.authorization
                    .judged_unrecorded(|| self.lookup(lookup, value))
            });
            let value = result?;
            judged.map_err(DbError::from)?;
            Ok(value)
        });
        // A run that never began read nothing.
        if !finished {
            subscription.finish(ReadSet::new());
        }
        result
    }

    /// Judge a write's changes for every waiting live query, in the
    /// write's own transaction before it commits: a lookup reads the rows
    /// the write leaves (§4).
    pub(super) fn judge(
        &self,
        handle: &CommitObserver,
        changes: Vec<crate::observation::RowChange>,
    ) -> Result<crate::observation::Judged, DbError> {
        let _reading = self.authorization.reading();
        handle
            .judge(
                changes,
                crate::commit_scope::committing(),
                &mut |lookup, value| {
                    self.authorization
                        .judged_unrecorded(|| self.lookup(lookup, value))
                },
            )
            .map_err(Into::into)
    }

    /// Whether some partner row in `lookup`'s scope holds `value`, read in
    /// this connection's transaction: a write's own, or a live query's
    /// run. A lookup that runs past `LOOKUP_STEPS` stops and counts as a
    /// match (§4). The caller sets how the authorizer judges it.
    fn lookup(
        &self,
        lookup: &crate::scope_lookup::Lookup,
        value: &crate::sql_value::SqlValue,
    ) -> rusqlite::Result<bool> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let (sql, parameters) = lookup.sql(value);
        let mut statement = self.connection.prepare_cached(sql)?;
        for (index, value) in parameters.iter().enumerate() {
            statement.raw_bind_parameter(index + 1, value)?;
        }
        // SQLite calls the handler as a statement's count of steps passes
        // each multiple of the limit, so the count starts at zero.
        statement.reset_status(rusqlite::StatementStatus::VmStep);
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopped);
        self.connection.progress_handler(
            crate::scope_lookup::LOOKUP_STEPS,
            Some(move || {
                stop.store(true, Ordering::Relaxed);
                true
            }),
        )?;
        let found = self.counting_steps(&mut statement, super::ReadSteps::Lookup, |statement| {
            statement.raw_query().next().map(|row| row.is_some())
        });
        self.connection.progress_handler(0, None::<fn() -> bool>)?;
        match found {
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::OperationInterrupted
                    && stopped.load(Ordering::Relaxed) =>
            {
                Ok(true)
            }
            found => found,
        }
    }
}
