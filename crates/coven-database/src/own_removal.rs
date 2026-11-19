//! A device's stop at its own removal, or its member's, that a reload found
//! in history past the slots this device applied (§14.1, §16.5).

use crate::sqlite::StoreSql;
use crate::store_call::StoreCall;
use crate::{write_schema::WriteSchema, DbError, StoreState};

/// Install `state`, what this device keeps at the removal, over its own rows,
/// and end what its app journaled, in one transaction. The reload installs
/// none of the snapshots or slots it read: the device keeps its copy, and
/// its applied slot stays the last one it applied. The rows of each circle
/// `state` deletes are taken out, as at any slot that deletes a circle.
pub(crate) fn stop(
    db: &mut StoreSql,
    call: &StoreCall<'_>,
    before: &StoreSql,
    before_call: &StoreCall<'_>,
    schema: &WriteSchema,
    state: &StoreState,
    files: &crate::file_write::WriteFiles<'_>,
) -> Result<(), DbError> {
    crate::local_write::stream_registering(db, call, files, None, |db| {
        let files = &crate::file_write::FileWrite::new(db, call, files);
        // The stop runs no migration and no DDL.
        let _constants = crate::transaction_constants::keep_constants(db, call);
        crate::download_stage::begin(db)?;
        // The state changes how every queued write applies: every kept row
        // is put back and the whole queue folds again, as at an admin slot.
        let mut touched = crate::published::restore(db, call, schema)?;
        let old = crate::store_state_tables::deleted_circles(db)?;
        crate::store_state_tables::replace(db, state)?;
        touched.extend(crate::sequence_apply::newly_deleted_rows(db, &old, state)?);
        crate::queued_writes::overlay(
            db,
            call,
            schema,
            &mut touched,
            crate::queued_writes::Refold::Whole,
        )?;
        let deleted = crate::store_state_tables::deleted_circles(db)?;
        crate::download_stage::materialize(
            db,
            call,
            before,
            before_call,
            schema,
            &deleted,
            touched,
            files,
        )?;
        files.before_commit()?;
        crate::operation::end_on_stop(db)?;
        Ok(((), false))
    })
}
