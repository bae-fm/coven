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
    write::{RowChange, WritePart, WriteRecord},
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
                    if old.loss.is_some() && old.state.generation() == *generation {
                        changes.insert(
                            row.clone(),
                            RowChange {
                                row: row.clone(),
                                change: Change {
                                    generation: *generation,
                                    operation: Operation::Delete,
                                },
                                old: old
                                    .state
                                    .cells()
                                    .iter()
                                    .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
                                    .collect(),
                            },
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
        let mut record = crate::write_record::record(database, device, now, changes)?;
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
                        dismissals: vec![dismissal],
                    },
                ),
            }
        }
        let deleted = crate::store_log_tables::deleted_circles(database)?;
        let affected = crate::write_apply::WriteApply::new(
            database, schema, &store, &visible, &visible, &deleted,
        )
        .apply(Some(&record), BTreeSet::new())?;
        files.retain_rows(affected, &deleted)?;
        files.before_commit()?;
        crate::write_commit::queue(database, &record)
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
                    loss.retired
                        && cells
                            .get(&dismissal.column)
                            .is_some_and(|cell| cell.write == dismissal.write)
                }
            };
            if !matches {
                continue;
            }
            if loss.retired {
                crate::fingerprint::forget_loss(database, &loss)?;
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
            if !loss.retired {
                affected.insert(row.clone());
            }
        }
    }
    Ok(affected)
}

#[cfg(test)]
#[path = "dismissal_tests.rs"]
mod tests;
