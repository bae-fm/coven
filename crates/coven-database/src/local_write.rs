//! A local write: the app's SQL recorded one row operation at a time, then
//! coven's capture, merge and queueing in the same transaction (§4, §9.6).

use super::event_capture::{Event, EventCapture};
use super::transaction_capture::ChangeCapture;
use super::DatabaseConnection;
use crate::DbError;
use coven_foundation::id_source::DeviceId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

impl DatabaseConnection {
    pub(crate) fn local_write<F, R, E>(
        &mut self,
        schema: &crate::write_schema::WriteSchema,
        device: DeviceId,
        now: SystemTime,
        files: &crate::file_write::WriteFiles<'_>,
        sql: F,
    ) -> Result<R, E>
    where
        E: crate::WriteFailure,
        F: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, E>,
    {
        #[cfg(test)]
        self.start_profile();
        let written = self.local_transaction(
            |database, recording| {
                let files = &crate::file_write::FileWrite::new(database, files);
                let deleted_circles = crate::store_state_tables::deleted_circles(database)?;
                let _scope = database.authorization.write();
                let result = sql(crate::SqlContext::new(files))?;
                files.check_steps()?;
                files.finish_app(|| recording.events())?;
                Ok((result, deleted_circles))
            },
            |database, events, (result, deleted_circles)| {
                let files = &crate::file_write::FileWrite::new(database, files);
                let captured = crate::write_capture::capture(schema, events)?;
                let before = crate::write_rows::AppView::before(database, schema, &captured)?;
                let after = crate::write_rows::AppView::after(database, schema);
                let store = crate::merge_store::MergeStore::new(database, &before);
                let mut steps =
                    crate::execution_capture::capture(database, schema, &before, &after, events)?;
                let changed = before.changes(&after, captured.keys().cloned())?;
                // The write's own moves already named their shown rows' new
                // files; only a taken-out row's insert names one (§17.2).
                let mut changes = crate::write_record::changes(
                    database,
                    schema,
                    &before,
                    &after,
                    &store,
                    &changed,
                    &deleted_circles,
                    &files.taken_out_copies(),
                )?;
                // The write's dismissals follow the app's changes (§8.4).
                let acknowledgements = crate::dismissal::merge(
                    database,
                    schema,
                    &store,
                    device,
                    &files.take_dismissals(),
                    &mut changes,
                    &mut steps,
                )?;
                let record =
                    files.delete_unreferenced(&store, changes, steps, |changes, steps| {
                        crate::dismissal::recorded(
                            crate::write_record::record(
                                database, device, now, changes, steps, &store,
                            )?,
                            &acknowledgements,
                            || crate::write_record::changed(database, device, now, BTreeMap::new()),
                        )
                    })?;
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
                    // The write completing a bound upload frees the write it
                    // holds, which a self-replacement waits to send (§17.4).
                    if !files.completes_bound() {
                        crate::operation::check_write_pause(database)?;
                    }
                    crate::write_commit::queue(database, schema, record)?;
                }
                files.queue_uploads(now, record.map(|record| record.header.position.number))?;
                files.before_commit()?;
                Ok(result)
            },
        );
        #[cfg(test)]
        self.end_profile();
        written
    }

    /// A local write's transaction, in two phases with a borrow of the
    /// connection each. The app phase's row operations are recorded one at a
    /// time (§9.6); a session observes the coven phase after it, as for
    /// every other transaction. `app` gives what `coven` goes on from.
    fn local_transaction<A, T, E: crate::WriteFailure>(
        &self,
        app: impl FnOnce(&Self, &EventCapture<'_>) -> Result<A, E>,
        coven: impl FnOnce(&Self, &[Event], A) -> Result<T, E>,
    ) -> Result<T, E> {
        self.write_transaction(false, || {
            let _constants = self.write_constants();
            let recording = EventCapture::begin(self)?;
            let begun = app(self, &recording)?;
            self.require_transaction()?;
            let events = recording.events();
            let mut changes = recording.local_changes();
            drop(recording);
            let mut capture = match &self.observation {
                Some(observation) => Some(ChangeCapture::begin(self, observation)?),
                None => None,
            };
            let result = coven(self, &events, begun)?;
            self.require_transaction()?;
            if let Some(observation) = &self.observation {
                let scoped = self
                    .handle
                    .as_ref()
                    .map(crate::observation::CommitObserver::scoped_columns)
                    .unwrap_or_default();
                let mut app = crate::change_capture::event_changes(
                    self,
                    &events,
                    &observation.supplemental,
                    &scoped,
                )?;
                app.append(&mut changes);
                changes = app;
            } else {
                changes.clear();
            }
            if let Some(capture) = &mut capture {
                changes.extend(capture.take_changes()?);
            }
            drop(capture);
            Ok(((), result, changes))
        })
    }
}
