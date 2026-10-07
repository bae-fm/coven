//! A circle deletion’s explicit causal row write (§14.7).

use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::write::RowChange;
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Change, Operation, WriteId};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

pub(crate) fn write(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    device: DeviceId,
    now: SystemTime,
    circle: CircleId,
    files: &crate::file_write::FileWrite<'_>,
) -> Result<Option<WriteId>, DbError> {
    let rows = db.query(
        "SELECT DISTINCT table_name,key,audience FROM coven_rows WHERE audience=?1",
        [circle.to_string()],
        crate::row_queries::read_identity,
    )?;
    let visible = crate::write_rows::AppView::after(db, schema);
    let store = crate::merge_store::MergeStore::new(db, &visible);
    let mut changes = BTreeMap::new();
    for row in rows {
        let current = store.row(&row)?;
        if !current.state.present() {
            continue;
        }
        changes.insert(
            row.clone(),
            RowChange {
                row,
                change: Change {
                    generation: current.state.generation(),
                    operation: Operation::Delete,
                },
                old: current
                    .state
                    .cells()
                    .iter()
                    .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
                    .collect(),
            },
        );
    }
    if changes.is_empty() {
        return Ok(None);
    }
    let record = crate::write_record::record(db, device, now, changes)?;
    let deleted = crate::store_log_tables::deleted_circles(db)?;
    let affected =
        crate::write_apply::WriteApply::new(db, schema, &store, &visible, &visible, &deleted)
            .apply(Some(&record), BTreeSet::new())?;
    files.retain_rows(affected, &deleted)?;
    files.before_commit()?;
    crate::write_commit::queue(db, &record)?;
    Ok(Some(record.header.position))
}
