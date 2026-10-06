//! Acknowledging cells and deleting removed rows in a causal write.

use crate::lost::LossTarget;
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::write_rows::AppView;
use crate::write_schema::WriteSchema;
use crate::{DbError, LostValue};
use coven_format::{
    dismissal::Dismissal,
    merge_fields,
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
    let records = database.query("SELECT l.table_name,l.key,l.column_id,c.table_name,c.column_name,l.value,l.set_by,l.replacement_kind,l.replaced_by,l.audience,l.generation,l.retired FROM coven_lost l LEFT JOIN coven_columns c ON c.id=l.column_id WHERE l.table_name=?1 AND l.key=?2 AND l.audience=?3", params![row.table,row.key,audience_text(&row.audience)], crate::lost::LostRecord::read)?;
    let losses = records
        .into_iter()
        .map(|record| record.decode())
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
        let setter = encoded(merge_fields::encode_write_id(&dismissal.write))?;
        let matches = database.query("SELECT l.id,l.generation,c.column_name,l.value,l.set_by,l.replacement_kind,l.replaced_by,l.retired FROM coven_lost l LEFT JOIN coven_columns c ON c.id=l.column_id WHERE l.table_name=?1 AND l.key=?2 AND l.audience=?3 AND (c.column_name=?4 OR l.column_id IS NULL)", params![row.table,row.key,audience_text(&row.audience),dismissal.column], |r| Ok((r.get::<_,i64>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Vec<u8>>(3)?,r.get::<_,Vec<u8>>(4)?,r.get::<_,String>(5)?,r.get::<_,Vec<u8>>(6)?,r.get::<_,bool>(7)?)))?;
        for (id, generation, column, value, setters, kind, cause, retired) in matches {
            if column.is_some() {
                if setters != setter {
                    continue;
                }
                if retired {
                    crate::fingerprint::forget_retired(
                        database,
                        row,
                        &generation,
                        column.as_deref().expect("cell column"),
                        &setters,
                    )?;
                }
                database.internal_execute("DELETE FROM coven_lost WHERE id=?1", [id])?;
            } else if retired || kind == "excluded" {
                let mut columns = decoded(merge_fields::decode_columns(&value))?;
                let mut writes = decoded(merge_fields::decode_setters(&setters))?;
                if writes.get(&dismissal.column) != Some(&dismissal.write) {
                    continue;
                }
                if retired {
                    crate::fingerprint::forget_retired(database, row, &generation, "", &setters)?;
                } else {
                    crate::fingerprint::forget_excluded(database, row, &setter)?;
                }
                columns.remove(&dismissal.column);
                writes.remove(&dismissal.column);
                if !retired && kind == "excluded" {
                    crate::excluded_write::dismiss(database, dismissal)?;
                }
                if columns.is_empty() {
                    database.internal_execute("DELETE FROM coven_lost WHERE id=?1", [id])?;
                } else {
                    let value = encoded(merge_fields::encode_columns(&columns))?;
                    let setters = encoded(merge_fields::encode_setters(&writes))?;
                    database.internal_execute(
                        "UPDATE coven_lost SET value=?1,set_by=?2 WHERE id=?3",
                        params![value, setters, id],
                    )?;
                    if retired {
                        crate::fingerprint::retired(
                            database,
                            row,
                            &generation,
                            "",
                            &setters,
                            &[&value, &setters, kind.as_bytes(), &cause],
                        )?;
                    } else {
                        crate::fingerprint::excluded(
                            database,
                            row,
                            &setter,
                            &[&generation, &value, &setters, &cause],
                        )?;
                    }
                }
            } else {
                continue;
            }
            if !retired && kind == "write" {
                affected.insert(row.clone());
            }
        }
    }
    Ok(affected)
}

#[cfg(test)]
#[path = "dismissal_tests.rs"]
mod tests;
