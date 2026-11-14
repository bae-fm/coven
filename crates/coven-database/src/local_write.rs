//! A local write: the app's SQL recorded one row operation at a time, then
//! coven's capture, merge and queueing in the same transaction (§4, §9.6).

use super::event_capture::{Event, EventCapture};
use super::stream_capture::{StreamCapture, StreamChanges};
use super::transaction_capture::{ChangeCapture, SupplementalChanges};
use super::write_transaction::WriteTransaction;
use super::DatabaseConnection;
use crate::observation::RowChange;
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
        F: FnOnce(&crate::file_write::FileWrite<'_>) -> Result<R, E>,
    {
        self.with_profile(|db| {
            let mut transaction = WriteTransaction::begin(db, false)?;
            transaction.keep_constants();
            let outcome = write_phases(&mut transaction, schema, device, now, files, sql);
            transaction.commit(outcome)
        })
    }

    /// A streamed transaction, as [`Self::stream_transaction`] runs one, in
    /// which `run` applies what registers this device and says whether it
    /// did. When it did, `registering`'s write follows in the same
    /// transaction, as a local write made right after `run` (§13.2): both
    /// commit, or neither. Without one, or when `run` registered nothing,
    /// this is `stream_transaction`.
    pub(crate) fn stream_registering<T>(
        &mut self,
        registering: Option<crate::joining_write::Registering<'_>>,
        run: impl FnOnce(&Self) -> Result<(T, bool), DbError>,
    ) -> Result<T, DbError> {
        let Some(registering) = registering else {
            return self.stream_transaction(|db| run(db).map(|(result, _)| result));
        };
        let mut transaction = WriteTransaction::begin(self, true)?;
        let outcome = StreamCapture::begin(&mut transaction).and_then(|stream| {
            let (result, registered) = transaction.within(|database| {
                let ran = run(database)?;
                database.require_transaction()?;
                Ok::<_, DbError>(ran)
            })?;
            let mut changes = match stream.changes() {
                StreamChanges::Rows(rows) => rows,
                StreamChanges::Tables(tables) => transaction
                    .within(|database| crate::change_capture::table_changes(database, tables))?,
            };
            if registered {
                // The write's app phase takes the hook the stream's capture
                // held, which has seen every change of `run`.
                transaction.stop_observing_operations()?;
                transaction.keep_constants();
                let (_, written) = registering.make(|schema, device, now, files, app| {
                    write_phases(&mut transaction, schema, device, now, files, app)
                })?;
                changes.extend(written);
            }
            Ok((result, changes))
        });
        transaction.commit(outcome)
    }
}

/// A local write's phases on `transaction`, whose connection holds the
/// transaction and whose hooks this sets: the app's SQL, each row operation
/// of it recorded (§9.6), then coven's capture, merge and queueing. Returns
/// the result and the row changes live queries are told of; `transaction`
/// commits or rolls back with them.
fn write_phases<F, R, E>(
    transaction: &mut WriteTransaction<'_>,
    schema: &crate::write_schema::WriteSchema,
    device: DeviceId,
    now: SystemTime,
    files: &crate::file_write::WriteFiles<'_>,
    sql: F,
) -> Result<(R, Vec<RowChange>), E>
where
    E: crate::WriteFailure,
    F: FnOnce(&crate::file_write::FileWrite<'_>) -> Result<R, E>,
{
    local_phases(
        transaction,
        |database, recording| {
            let files = &crate::file_write::FileWrite::new(database, files);
            let deleted_circles = crate::store_state_tables::deleted_circles(database)?;
            let _scope = database.authorization.write();
            let result = sql(files)?;
            // SQLite ending the transaction says more than the
            // statement that ended it.
            database.require_transaction()?;
            files.check_parts()?;
            files.finish_app(|| recording.events())?;
            Ok((result, deleted_circles))
        },
        |database, events, (result, deleted_circles)| {
            let files = &crate::file_write::FileWrite::new(database, files);
            let captured = crate::write_capture::capture(schema, events)?;
            let before = crate::write_rows::AppView::before(database, schema, &captured)?;
            let after = crate::write_rows::AppView::after(database, schema)
                .with_rows_read(files.take_rows_read());
            let store = crate::merge_store::MergeStore::new(database, &before);
            // Capturing and building the record write no app row: a
            // parent many rows name is read once.
            let parents = (before.remembering_parents(), after.remembering_parents());
            let mut steps = crate::execution_capture::capture(
                database, schema, &store, &before, &after, events,
            )?;
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
                &files.taken_out_copies(),
            )?;
            drop(parents);
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
            let record = files.delete_unreferenced(&store, changes, steps, |changes, steps| {
                crate::dismissal::recorded(
                    crate::write_record::record(database, device, now, changes, steps, &store)?,
                    &acknowledgements,
                    || crate::write_record::changed(database, device, now, BTreeMap::new()),
                )
            })?;
            if record.is_some()
                && crate::store_state_tables::awaiting_registration(database, device)?
            {
                return Err(DbError::NotRegistered.into());
            }
            // The paths its record updates move off, read before
            // the write applies.
            let replaced = match &record {
                Some(record) => crate::file_protection::replaced(&store, record)?,
                None => BTreeSet::new(),
            };
            let applied = crate::write_apply::WriteApply::local(
                database,
                schema,
                &store,
                &before,
                &after,
                &deleted_circles,
            )
            .apply_merged(record.as_ref(), BTreeSet::new())
            .map_err(|error| match error {
                // This device authored the record; rejecting it means our own
                // authoring or removal machinery broke its invariants.
                DbError::InvalidWrite { write, error } => {
                    panic!("locally authored write {write:?} violates merge invariant: {error}")
                }
                error => error,
            })?;
            files.retain_rows(applied.affected, &applied.merged)?;
            if let Some(record) = &record {
                // The write completing a bound upload frees the write it
                // holds, which a self-replacement waits to send (§17.4).
                if !files.completes_bound() {
                    crate::operation::check_write_pause(database)?;
                }
                crate::write_commit::queue(database, schema, record, replaced)?;
            }
            files.queue_uploads(now, record.map(|record| record.header.position.number))?;
            files.before_commit()?;
            Ok(result)
        },
    )
}

/// A local write's transaction, in two phases with a borrow of the
/// connection each. The app phase's row operations are recorded one at a
/// time (§9.6); a session observes the coven phase after it, as for
/// every other transaction. `app` gives what `coven` goes on from.
fn local_phases<A, T, E: crate::WriteFailure>(
    transaction: &mut WriteTransaction<'_>,
    app: impl FnOnce(&DatabaseConnection, &EventCapture) -> Result<A, E>,
    coven: impl FnOnce(&DatabaseConnection, &[Event], A) -> Result<T, E>,
) -> Result<(T, Vec<RowChange>), E> {
    let recording = EventCapture::begin(transaction);
    let begun = recording.map_err(E::from).and_then(|recording| {
        transaction.within(|database| {
            let begun = app(database, &recording)?;
            database.require_transaction()?;
            Ok((begun, recording))
        })
    });
    // The app phase ends: a session observes the rest of the transaction.
    let switched = match begun {
        Ok(begun) => transaction
            .stop_observing_operations()
            .and_then(|()| SupplementalChanges::observe(transaction))
            .map(|supplemental| (begun, supplemental))
            .map_err(E::from),
        Err(error) => Err(error),
    };
    switched.and_then(|((begun, recording), supplemental)| {
        transaction.within(|database| {
            let events = recording.events();
            let mut changes = recording.local_changes();
            let mut capture = match &database.observation {
                Some(_) => Some(ChangeCapture::begin(database, supplemental)?),
                None => None,
            };
            let result = coven(database, &events, begun)?;
            database.require_transaction()?;
            if database.observation.is_some() {
                let scoped = database
                    .handle
                    .as_ref()
                    .map(crate::observation::CommitObserver::scoped_columns)
                    .unwrap_or_default();
                let mut app = crate::change_capture::event_changes(database, &events, &scoped)?;
                app.append(&mut changes);
                changes = app;
            } else {
                changes.clear();
            }
            if let Some(capture) = &mut capture {
                changes.extend(capture.take_changes()?);
            }
            Ok((result, changes))
        })
    })
}
