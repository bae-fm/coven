//! Acknowledging cells and deleting removed rows in a causal write.

use crate::lost::LossTarget;
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::audience_text;
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::{DbError, LostValue};
use coven_format::{
    dismissal::Dismissal,
    write::{WritePart, WriteRecord},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Change, Operation, RowId};
use rusqlite::params;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

pub(crate) fn dismiss(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    device: DeviceId,
    now: SystemTime,
    values: &[LostValue],
    files: &crate::file_write::FileWrite<'_>,
) -> Result<(), DbError> {
    database.transaction(|database| {
        let visible = AppView::after(database, schema);
        let store = MergeStore::new(database, &visible);
        let mut changes = BTreeMap::new();
        let mut cells = BTreeSet::new();
        for value in values {
            match &value.target {
                LossTarget::Removed { row, generation } => {
                    let old = store.row(row)?;
                    // A stale query must not delete a restored or re-created row.
                    // Only an owned row's owner may delete it; queueing judges that (§9.6).
                    if old.loss.is_some() && old.state.generation() == *generation {
                        changes.insert(
                            row.clone(),
                            crate::write_sources::authored(
                                database,
                                &store,
                                row.clone(),
                                Change {
                                    generation: *generation,
                                    operation: Operation::Delete,
                                },
                                old.state
                                    .cells()
                                    .iter()
                                    .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
                                    .collect(),
                            )?,
                        );
                    }
                }
                LossTarget::Cells(dismissals) => {
                    for dismissal in dismissals {
                        if contains(database, dismissal)? {
                            cells.insert(dismissal.clone());
                        }
                    }
                }
            }
        }
        if changes.is_empty() && cells.is_empty() {
            return Ok(());
        }
        let mut record = crate::write_record::changed(database, device, now, changes)?;
        for dismissal in cells {
            let audience = dismissal.row.audience.clone();
            match record
                .parts
                .binary_search_by(|part| part.audience.cmp(&audience))
            {
                Ok(index) => record.parts[index].dismissals.push(dismissal),
                Err(index) => record.parts.insert(
                    index,
                    WritePart {
                        audience,
                        rows: Vec::new(),
                        executions: Vec::new(),
                        dismissals: vec![dismissal],
                    },
                ),
            }
        }
        let deleted = crate::store_state_tables::deleted_circles(database)?;
        let affected = crate::write_apply::WriteApply::new(
            database, schema, &store, &visible, &visible, &deleted,
        )
        .apply(Some(&record), BTreeSet::new())?;
        files.retain_rows(affected)?;
        files.before_commit()?;
        crate::operation::check_write_pause(database)?;
        crate::write_commit::queue(database, schema, &record)
    })
}

fn contains(database: &DatabaseConnection, dismissal: &Dismissal) -> Result<bool, DbError> {
    let row = &dismissal.row;
    let records = database.query(
        &format!(
            "{} WHERE l.table_name=?1 AND l.key=?2 AND l.audience=?3",
            crate::loss_record::SELECT
        ),
        params![row.table, row.key, audience_text(&row.audience)],
        crate::loss_record::read,
    )?;
    let losses = records
        .into_iter()
        .map(LostValue::from_record)
        .collect::<Result<Vec<_>, DbError>>()?;
    Ok(losses.iter().any(|loss| match &loss.target {
        LossTarget::Cells(cells) => cells.contains(dismissal),
        LossTarget::Removed { .. } => false,
    }))
}

pub(crate) fn apply(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    record: &WriteRecord,
) -> Result<BTreeSet<RowId>, DbError> {
    let mut affected = BTreeSet::new();
    for dismissal in record.parts.iter().flat_map(|part| &part.dismissals) {
        let row = &dismissal.row;
        let ids = database.query(
            "SELECT id FROM _coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3",
            params![row.table, row.key, audience_text(&row.audience)],
            |r| r.get::<_, i64>(0),
        )?;
        for id in ids {
            use coven_format::loss::LossValues;
            let mut loss = database.query_row(
                &format!("{} WHERE l.id=?1", crate::loss_record::SELECT),
                [id],
                crate::loss_record::read,
            )?;
            let matches = match &loss.values {
                LossValues::Cell { column, cell } => {
                    *column == dismissal.column && cell.write == dismissal.write
                }
                LossValues::Row(cells) => {
                    loss.frozen
                        && cells
                            .get(&dismissal.column)
                            .is_some_and(|cell| cell.write == dismissal.write)
                }
            };
            if !matches {
                continue;
            }
            let keep = if let LossValues::Row(cells) = &mut loss.values {
                cells.remove(&dismissal.column);
                !cells.is_empty()
            } else {
                false
            };
            if keep {
                crate::loss_record::put(database, &loss, Some(id))?;
            } else {
                database.internal_execute("DELETE FROM _coven_lost WHERE id=?1", [id])?;
            }
            if !loss.frozen {
                affected.insert(row.clone());
            }
        }
    }
    for key in record
        .parts
        .iter()
        .flat_map(|part| &part.dismissals)
        .map(|d| (d.row.table.clone(), d.row.key.clone()))
        .collect::<BTreeSet<_>>()
    {
        crate::file_protection::refresh(database, schema, &key)?;
    }
    Ok(affected)
}

pub(crate) fn validate_past(
    database: &DatabaseConnection,
    header: &coven_format::write::WriteHeader,
    dismissal: &Dismissal,
) -> Result<(), DbError> {
    if !coven_merge::had_read(
        header.position,
        header.read_through,
        dismissal.write,
        crate::sequence::write_slot(database, dismissal.write)?,
    ) {
        return Err(crate::snapshot_error::invalid(
            "dismissal names a write outside authored past",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "dismissal_tests.rs"]
mod tests;
