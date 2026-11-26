//! A connection's part in live queries' judging of writes (SPEC.md section 7):
//! a live query's run, which judges the writes that committed while it ran, a
//! write's judging before it commits, and the lookups both run in their own
//! transaction.

use super::HostSql;
use crate::authorization::Policy;
use crate::authorization::PolicyError;
use crate::{Judging, Lookups, NoAnswer, ReadSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

impl<P: Policy> HostSql<P> {
    /// A live query's run on one snapshot. What its statements read goes to
    /// `judge` before the snapshot ends, which judges the writes that committed
    /// during the run by lookups in that snapshot (SPEC.md section 3). A schema
    /// with a table that has no primary key refuses the run.
    pub fn live_read<F, R, E>(
        &self,
        judge: &mut dyn FnMut(ReadSet, &mut Lookups<'_>) -> rusqlite::Result<()>,
        read: F,
    ) -> Result<R, E>
    where
        F: FnOnce(&Self) -> Result<R, E>,
        E: From<PolicyError<P>>,
    {
        let mut finished = false;
        let result = self.read_transaction(|| {
            let _reading = self.authorization.live_reading();
            // A table with no primary key refuses the read before it starts.
            // The shapes are metadata, so the run doesn't record reading them.
            let shapes = self.authorization.unrecorded(|| self.table_shapes());
            let result = shapes.map_err(E::from).and_then(|_| read(self));
            let reads = self.authorization.reads();
            finished = true;
            let judged = judge(reads, &mut |judging| self.ask_judged(judging));
            let value = result?;
            judged.map_err(crate::Error::from)?;
            Ok(value)
        });
        // A run that never began read nothing.
        if !finished {
            judge(ReadSet::new(), &mut |_| unreachable!("a run that read nothing reads no join"))
                .expect("judging without lookups can't fail");
        }
        result
    }

    /// Judge a write's changes for every waiting live query, in the
    /// write's own transaction before it commits: a lookup reads the rows
    /// the write leaves (SPEC.md section 7).
    pub(super) fn judge(
        &self,
        handle: &crate::Handle<P::Failure>,
        changes: Vec<crate::RowChange>,
    ) -> Result<crate::Judged, PolicyError<P>> {
        let _reading = self.authorization.reading();
        let judged =
            handle.judge(changes, crate::committing(), &mut |judging| self.ask_judged(judging))?;
        #[cfg(any(test, feature = "test-utils"))]
        self.record_work(|work| work.judged_queries += judged.judgments());
        Ok(judged)
    }

    /// Answer one question about a changed row, read in this connection's
    /// transaction, judged as app SQL is without recording its reads.
    fn ask_judged(&self, judging: Judging<'_>) -> Result<bool, NoAnswer> {
        match judging {
            // A host statement's read can name tables the app's SQL can't, so
            // its questions run as the host's own SQL.
            Judging::Host(question) => self.authorization.unrecorded(|| self.ask(*question)),
            question => self.authorization.judged_unrecorded(|| Ok(self.ask(question)))?,
        }
    }

    /// Answer one question about a changed row, read in this connection's
    /// transaction: a write's own, or a live query's run. The caller sets
    /// how the authorizer judges it.
    fn ask(&self, judging: Judging<'_>) -> Result<bool, NoAnswer> {
        match judging {
            Judging::Host(question) => self.ask(*question),
            Judging::Partner(lookup, value) => {
                let (sql, parameters) = lookup.sql(value);
                self.run_limited(sql, &parameters)
            }
            // An error SQLite gives for this question, as for malformed JSON
            // a condition reads or an overflow, says nothing of whether the
            // row passes: it counts as a match.
            Judging::Row(sql, parameters) => match self.run_limited(sql, parameters) {
                Err(NoAnswer::Failed(
                    rusqlite::Error::SqliteFailure(..) | rusqlite::Error::SqlInputError { .. },
                )) => Ok(true),
                found => found,
            },
        }
    }

    /// Whether `sql` finds a row. A question that runs past `LOOKUP_STEPS`
    /// stops with no answer (SPEC.md section 7).
    fn run_limited(&self, sql: &str, parameters: &[crate::SqlValue]) -> Result<bool, NoAnswer> {
        let mut statement = self.connection.prepare_cached(sql)?;
        for (index, value) in parameters.iter().enumerate() {
            statement.raw_bind_parameter(index + 1, value)?;
        }
        // SQLite calls the handler as a statement's count of steps passes
        // each multiple of the limit, so the count starts at zero.
        statement.reset_status(rusqlite::StatementStatus::VmStep);
        self.lookups.arm();
        let found = self.counting_steps(&mut statement, super::StepsOf::Lookup, |statement| {
            statement.raw_query().next().map(|row| row.is_some())
        });
        let stopped = self.lookups.disarm();
        match found {
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::OperationInterrupted && stopped =>
            {
                #[cfg(any(test, feature = "test-utils"))]
                self.record_work(|work| work.stopped_questions += 1);
                Err(NoAnswer::Stopped)
            }
            found => found.map_err(NoAnswer::Failed),
        }
    }
}

/// What stops a lookup that runs past `LOOKUP_STEPS`. The connection's
/// progress handler, set once as it opens, runs every `LOOKUP_STEPS` steps
/// of any statement and interrupts one only while a lookup has armed it.
pub(super) struct LookupLimit(Arc<LookupState>);

struct LookupState {
    armed: AtomicBool,
    stopped: AtomicBool,
}

impl LookupLimit {
    pub(super) fn install(connection: &mut rusqlite::Connection) -> rusqlite::Result<Self> {
        let state = Arc::new(LookupState {
            armed: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        });
        let handler = Arc::clone(&state);
        connection.progress_handler(
            crate::LOOKUP_STEPS,
            Some(move || {
                let stop = handler.armed.load(Ordering::Relaxed);
                if stop {
                    handler.stopped.store(true, Ordering::Relaxed);
                }
                stop
            }),
        )?;
        Ok(Self(state))
    }

    fn arm(&self) {
        self.0.stopped.store(false, Ordering::Relaxed);
        self.0.armed.store(true, Ordering::Relaxed);
    }

    /// Whether the lookup armed since was stopped.
    fn disarm(&self) -> bool {
        self.0.armed.store(false, Ordering::Relaxed);
        self.0.stopped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[path = "live_judging_tests.rs"]
mod tests;
