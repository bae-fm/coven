//! A local write: the app's SQL recorded one row operation at a time, then
//! coven's capture, merge and queueing in the same transaction (§5, §8.8).

use super::event_capture::{Event, EventCapture};
use super::transaction_capture::ChangeCapture;
use super::{DatabaseConnection, SqlTransaction};
use crate::DbError;
use coven_foundation::id_source::DeviceId;
use std::collections::BTreeSet;
use std::time::SystemTime;

impl DatabaseConnection {
    pub(crate) fn local_write<F, R, E>(
        &self,
        schema: &crate::write_schema::WriteSchema,
        device: DeviceId,
        now: SystemTime,
        files: &crate::file_write::FileWrite<'_>,
        sql: F,
    ) -> Result<R, E>
    where
        E: crate::WriteFailure,
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, E>,
    {
        #[cfg(test)]
        let _profile = self.profile_statements();
        self.local_transaction(|database, phase| {
            let deleted_circles = crate::store_log_tables::deleted_circles(database)?;
            let result = {
                let _scope = database.authorization.write();
                let result = sql(crate::SqlContext::new(files))?;
                database.authorization.take_file_violation()?;
                files.attach_app_files()?;
                result
            };
            database.require_transaction()?;
            let captured = crate::write_capture::capture(schema, &phase.events())?;
            {
                let _scope = database.authorization.write();
                files.clear_null_ids(&captured)?;
            }
            let events = phase.finish_app()?;
            let captured = crate::write_capture::capture(schema, &events)?;
            files.validate(&captured)?;
            let before = crate::write_rows::AppView::before(database, schema, &captured)?;
            let after = crate::write_rows::AppView::after(database, schema);
            let store = crate::merge_store::MergeStore::new(database, &before);
            let steps =
                crate::execution_capture::capture(database, schema, &before, &after, &events)?;
            let changes = crate::write_record::changes(
                database,
                schema,
                &before,
                &after,
                &store,
                &before.changes(&after, captured.keys().cloned())?,
                &deleted_circles,
            )?;
            let record =
                crate::write_record::record(database, device, now, changes, steps, &store)?;
            let affected = crate::write_apply::WriteApply::new(
                database,
                schema,
                &store,
                &before,
                &after,
                &deleted_circles,
            )
            .apply(record.as_ref(), BTreeSet::new())
            .map_err(|error| match error {
                // This device authored the record; rejecting it means our own
                // authoring or removal machinery broke its invariants.
                DbError::InvalidWrite { write, error } => {
                    panic!("locally authored write {write:?} violates merge invariant: {error}")
                }
                error => error,
            })?;
            files.retain_rows(affected)?;
            if let Some(record) = &record {
                crate::operation::check_write_pause(database)?;
                crate::write_commit::queue(database, schema, record)?;
            }
            files.queue_attached(now)?;
            files.before_commit()?;
            Ok(result)
        })
    }

    /// A local write's transaction. Its app phase records each row operation
    /// (§8.8); a session observes the coven phase after it, as for every
    /// other transaction.
    fn local_transaction<T, E: crate::WriteFailure>(
        &self,
        run: impl FnOnce(&Self, &mut LocalPhase<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        self.batch("BEGIN IMMEDIATE")?;
        self.streaming.set(false);
        let mut guard = SqlTransaction {
            database: self,
            active: true,
        };
        let result = (|| {
            let mut phase = LocalPhase {
                database: self,
                recording: Some(EventCapture::begin(self)?),
                app: Vec::new(),
                capture: None,
            };
            let result = run(self, &mut phase)?;
            self.require_transaction()?;
            let changes = phase.changes()?;
            drop(phase);
            self.batch("COMMIT")?;
            #[cfg(any(test, feature = "test-utils"))]
            self.record_work(|work| work.commits += 1);
            #[cfg(any(test, feature = "test-utils"))]
            self.checkpoint(crate::test_utils::WriteCheckpoint::Committed);
            if let Some(handle) = &self.handle {
                handle.commit(changes, crate::commit_scope::committing());
            }
            #[cfg(any(test, feature = "test-utils"))]
            self.checkpoint(crate::test_utils::WriteCheckpoint::Observed);
            Ok(result)
        })();
        guard.active = false;
        match result {
            Ok(result) => Ok(result),
            Err(operation) => match self.roll_back() {
                Ok(()) => Err(operation),
                Err(rollback) => Err(operation.with_rollback(rollback)),
            },
        }
    }
}

/// The phases of a local write: the app's own SQL, recorded one row
/// operation at a time, then coven's merge and materialization.
pub(crate) struct LocalPhase<'a> {
    database: &'a DatabaseConnection,
    recording: Option<EventCapture<'a>>,
    app: Vec<Event>,
    capture: Option<ChangeCapture<'a>>,
}

impl LocalPhase<'_> {
    /// The app phase's row operations so far, in order.
    pub(crate) fn events(&self) -> Vec<Event> {
        self.recording
            .as_ref()
            .expect("the app phase is recording")
            .events()
    }

    /// End the app phase: return its row operations and observe the rest of
    /// the transaction with a session.
    pub(crate) fn finish_app(&mut self) -> Result<Vec<Event>, DbError> {
        let recording = self.recording.take().expect("the app phase ends once");
        self.app = recording.events();
        drop(recording);
        if let Some(observation) = &self.database.observation {
            self.capture = Some(ChangeCapture::begin(self.database, observation)?);
        }
        Ok(self.app.clone())
    }

    fn changes(&mut self) -> Result<Vec<crate::observation::RowChange>, DbError> {
        let Some(observation) = &self.database.observation else {
            return Ok(Vec::new());
        };
        let mut changes = crate::change_capture::event_changes(
            self.database,
            &self.app,
            &observation.supplemental,
        )?;
        if let Some(capture) = &mut self.capture {
            changes.extend(capture.take_changes()?);
        }
        Ok(changes)
    }
}
