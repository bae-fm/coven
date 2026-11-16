//! A local write: the app's SQL recorded one row operation at a time, then
//! coven's capture, merge and queueing in the same transaction (§4, §9.6).

use crate::sqlite::{DatabaseConnection, Operation};
use crate::store_call::StoreCall;
use crate::write_failure::{hosted, Hosted};
use crate::DbError;
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::Slot;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

pub(crate) fn local_write<F, R, E>(
    sql: &mut DatabaseConnection,
    call: &StoreCall<'_>,
    schema: &crate::write_schema::WriteSchema,
    device: DeviceId,
    now: SystemTime,
    files: &crate::file_write::WriteFiles<'_>,
    app: F,
) -> Result<R, E>
where
    E: crate::WriteFailure,
    F: FnOnce(&crate::file_write::FileWrite<'_>) -> Result<R, E>,
{
    sql.with_profile(|db| {
        db.write_with(
            |database, operations| hosted(|| app_phase(database, call, operations, files, app)),
            |database, operations, applied| {
                hosted(|| {
                    host_phase(database, call, operations, applied, schema, device, now, files)
                })
            },
        )
        .map_err(Hosted::into_inner)
    })
}

/// A streamed transaction on `sql`, as `stream_transaction` runs one, in
/// which `run` applies what registers this device and says whether it
/// did. When it did, `registering`'s write follows in the same
/// transaction, as a local write made right after `run` (§13.2): both
/// commit, or neither. Without one, or when `run` registered nothing,
/// this is `stream_transaction`.
pub(crate) fn stream_registering<T>(
    sql: &mut DatabaseConnection,
    call: &StoreCall<'_>,
    registering: Option<crate::joining_write::Registering<'_>>,
    run: impl FnOnce(&DatabaseConnection) -> Result<(T, bool), DbError>,
) -> Result<T, DbError> {
    let Some(registering) = registering else {
        return sql.stream_transaction(|db| run(db).map(|(result, _)| result));
    };
    registering.make(|schema, device, now, files, app| {
        sql.stream_then_write(
            run,
            |database, operations| app_phase(database, call, operations, files, app),
            |database, operations, applied| {
                host_phase(database, call, operations, applied, schema, device, now, files)
            },
        )
    })
}

type DeletedCircles = BTreeMap<CircleId, Slot>;

/// A local write's app phase: coven runs the app's SQL and records each of
/// its row operations (§9.6). Returns the app's result and the circles
/// deleted before it.
fn app_phase<F, R, E>(
    database: &DatabaseConnection,
    call: &StoreCall<'_>,
    operations: &dyn Fn() -> Vec<Operation>,
    files: &crate::file_write::WriteFiles<'_>,
    sql: F,
) -> Result<(R, DeletedCircles), E>
where
    E: crate::WriteFailure,
    F: FnOnce(&crate::file_write::FileWrite<'_>) -> Result<R, E>,
{
    // A write runs no migration and no DDL.
    let _constants = crate::transaction_constants::keep_constants(database, call);
    let files = &crate::file_write::FileWrite::new(database, call, files);
    let deleted_circles = crate::store_state_tables::deleted_circles(database)?;
    let _scope = database.write_scope();
    let result = sql(files)?;
    // SQLite ending the transaction says more than the
    // statement that ended it.
    database.require_transaction().map_err(DbError::from)?;
    files.check_parts()?;
    files.finish_app(operations)?;
    Ok((result, deleted_circles))
}

/// A local write's host phase: coven's capture, merge, record, materializing
/// and queueing of the app phase's operations.
fn host_phase<R, E: crate::WriteFailure>(
    database: &DatabaseConnection,
    call: &StoreCall<'_>,
    events: &[Operation],
    (result, deleted_circles): (R, DeletedCircles),
    schema: &crate::write_schema::WriteSchema,
    device: DeviceId,
    now: SystemTime,
    files: &crate::file_write::WriteFiles<'_>,
) -> Result<R, E> {
    let _constants = crate::transaction_constants::keep_constants(database, call);
    let files = &crate::file_write::FileWrite::new(database, call, files);
    let captured = crate::write_capture::capture(schema, events)?;
    let before = crate::write_rows::AppView::before(database, schema, &captured)?;
    let after =
        crate::write_rows::AppView::after(database, schema).with_rows_read(files.take_rows_read());
    let store = crate::merge_store::MergeStore::new(database, call, &before);
    // Capturing and building the record write no app row, so coven reads a
    // parent that many rows name only once.
    let parents = (before.remembering_parents(), after.remembering_parents());
    let mut steps =
        crate::execution_capture::capture(database, schema, &store, &before, &after, events)?;
    let changed = before.changes(&after, captured.keys().cloned())?;
    // The write's own moves already named their shown rows' new
    // files; only a taken-out row's insert names one (§17.2).
    let mut changes = crate::write_record::changes(
        database,
        call,
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
        call,
        schema,
        &store,
        device,
        &files.take_dismissals(),
        &mut changes,
        &mut steps,
    )?;
    let record = files.delete_unreferenced(&store, changes, steps, |changes, steps| {
        crate::dismissal::recorded(
            crate::write_record::record(database, call, device, now, changes, steps, &store)?,
            &acknowledgements,
            || crate::write_record::changed(database, call, device, now, BTreeMap::new()),
        )
    })?;
    if record.is_some() && crate::store_state_tables::awaiting_registration(database, device)? {
        return Err(DbError::NotRegistered.into());
    }
    // Read the paths that the record's updates move off before the write
    // applies.
    let replaced = match &record {
        Some(record) => crate::file_protection::replaced(&store, record)?,
        None => BTreeSet::new(),
    };
    let applied = crate::write_apply::WriteApply::local(
        database,
        call,
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
        crate::write_commit::queue(database, call, schema, record, replaced)?;
    }
    files.queue_uploads(now, record.map(|record| record.header.position.number))?;
    files.before_commit()?;
    Ok(result)
}
