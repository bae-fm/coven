//! Acknowledging lost values and deleting removed rows in a causal write.

use crate::lost::LossTarget;
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::audience_text;
use crate::write_schema::WriteSchema;
use crate::{DbError, LostValue};
use coven_format::{
    dismissal::{Dismissal, DismissalTarget},
    value::Value,
    write::{Half, RowChange, Step, WritePart, WriteRecord},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Change, Operation, RowId, RowState};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// The deletes and acknowledgements a local write's dismissals add to the
/// app's own changes (§8.4). Each removed row still lost as it was gets its
/// delete, with the deletes its CASCADE children need, as steps after the
/// app's; each acknowledgement still naming a lost value is returned for the
/// record's parts. A dismissal no longer matching what is lost, as a stale
/// query's, is left out. A write that also changes a row a dismissal
/// deletes fails with `DismissedRowChanged`: the delete is for good, and the
/// app's change would make it something else.
pub(crate) fn merge(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    store: &MergeStore<'_>,
    device: DeviceId,
    values: &[LostValue],
    changes: &mut BTreeMap<RowId, RowChange>,
    steps: &mut BTreeMap<RowId, Vec<Step>>,
) -> Result<BTreeSet<Dismissal>, DbError> {
    let mut deletes = BTreeMap::new();
    let mut acknowledgements = BTreeSet::new();
    for value in values {
        match &value.target {
            LossTarget::Removed { row, generation } => {
                let old = store.row(row)?;
                // A stale query must not delete a restored or re-created row.
                // Only an owned row's owner may delete it; queueing judges that (§9.6).
                if old.loss.is_some() && old.state.generation() == *generation {
                    deletes.insert(row.clone(), deletion(database, store, &old.state)?);
                }
            }
            LossTarget::Acknowledgements(dismissals) => {
                for dismissal in dismissals {
                    if contains(database, dismissal)? {
                        acknowledgements.insert(dismissal.clone());
                    }
                }
            }
        }
    }
    cascade_deletes(database, schema, store, device, &mut deletes)?;
    let after = coven_format::write::Step::after(steps.values().flatten());
    for (row, mut change) in deletes {
        if changes.contains_key(&row) || steps.contains_key(&row) {
            return Err(DbError::DismissedRowChanged {
                table: row.table.clone(),
                key: crate::RowKey(
                    crate::write_rows::key_parameters(&row.key)?.into_iter().collect(),
                ),
            });
        }
        let mut deleted = std::mem::take(&mut change.execution.steps);
        for step in &mut deleted {
            step.sequence = step
                .sequence
                .checked_add(after)
                .ok_or(DbError::Invariant { what: "a write has fewer operations" })?;
        }
        steps.insert(row.clone(), deleted);
        changes.insert(row, change);
    }
    Ok(acknowledgements)
}

/// A local write's record with its acknowledgements: `record`, as its
/// changes make it, with each acknowledgement in its row's audience part,
/// or, for a write of acknowledgements alone, one `empty` makes for them.
pub(crate) fn recorded(
    record: Option<WriteRecord>,
    acknowledgements: &BTreeSet<Dismissal>,
    empty: impl FnOnce() -> Result<WriteRecord, DbError>,
) -> Result<Option<WriteRecord>, DbError> {
    let mut record = match record {
        Some(record) => record,
        None if acknowledgements.is_empty() => return Ok(None),
        None => empty()?,
    };
    dismissing(&mut record, acknowledgements);
    Ok(Some(record))
}

/// Add each of `acknowledgements` to the part of `record` in its row's
/// audience.
fn dismissing(record: &mut WriteRecord, acknowledgements: &BTreeSet<Dismissal>) {
    for dismissal in acknowledgements.iter().cloned() {
        let audience = dismissal.row.audience.clone();
        match record.parts.binary_search_by(|part| part.audience.cmp(&audience)) {
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
}

/// Whether the dismissal names the lost cell of `row`'s `column` that
/// `write` set: a lost cell, or a cell of a frozen row's lost values, as
/// applying it removes.
pub(crate) fn dismisses_cell(
    dismissal: &Dismissal,
    row: &RowId,
    column: &str,
    write: coven_merge::WriteId,
) -> bool {
    dismissal.row == *row
        && dismissal.write == write
        && matches!(&dismissal.target, DismissalTarget::Cell(target) if target == column)
}

/// Whether the dismissal names this lost value, as applying it does.
fn dismisses(dismissal: &Dismissal, loss: &coven_format::loss::Loss) -> bool {
    use coven_format::loss::LossValues;
    if loss.row != dismissal.row {
        return false;
    }
    match (&dismissal.target, &loss.values) {
        (DismissalTarget::Cell(_), LossValues::Cell { column, cell }) => {
            dismisses_cell(dismissal, &loss.row, column, cell.write)
        }
        (DismissalTarget::Cell(column), LossValues::Row(cells)) => {
            loss.frozen
                && cells
                    .get(column)
                    .is_some_and(|cell| dismisses_cell(dismissal, &loss.row, column, cell.write))
        }
        (DismissalTarget::EmptyRow, LossValues::Row(cells)) => {
            loss.frozen && cells.is_empty() && loss.cause.change_write() == Some(dismissal.write)
        }
        (DismissalTarget::EmptyRow, LossValues::Cell { .. }) => false,
    }
}

/// The delete of a row, as this device authors it from its merged state.
pub(crate) fn deletion(
    database: &DatabaseConnection,
    store: &MergeStore<'_>,
    state: &RowState<Value>,
) -> Result<RowChange, DbError> {
    crate::write_sources::authored(
        database,
        store,
        state.row().clone(),
        Change { generation: state.generation(), operation: Operation::Delete },
        state.cells().iter().map(|(name, cell)| (name.clone(), cell.value.value.clone())).collect(),
    )
}

/// Removed rows are absent from SQLite, so its CASCADE cannot capture their
/// children. Record the same deletes, parent before child, with the written
/// references that let every receiver verify the actions (§9.6).
fn cascade_deletes(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    store: &MergeStore<'_>,
    device: DeviceId,
    changes: &mut BTreeMap<RowId, RowChange>,
) -> Result<(), DbError> {
    let mut owners =
        crate::judge::Owners::new(database, schema, device, database.schema_version()?)?;
    let mut roots = Vec::new();
    for row in changes.keys() {
        roots.push((owners.foreign_owner(row)?.is_some(), row.clone()));
    }
    // Selecting a child as well must not turn an authorized cascade into a
    // direct delete of someone else's row. Start every permitted root first.
    roots.sort();
    let mut pending: VecDeque<_> = roots.into_iter().map(|(_, row)| row).collect();
    let mut sequence: u32 = 0;
    while let Some(row) = pending.pop_front() {
        let change = changes.get_mut(&row).expect("scheduled delete");
        // A selected row may also have been reached through a parent or cycle.
        if !change.execution.steps.is_empty() {
            continue;
        }
        let generation = change.change.generation;
        change.execution.steps.push(Step {
            sequence,
            change: change.change.clone(),
            half: Half::Whole,
        });
        sequence = sequence.checked_add(1).expect("a write has fewer operations");
        for declaration in &schema.declarations {
            let table = schema.table(&declaration.name);
            for foreign in table.foreign_keys.iter().filter(|key| {
                key.target.eq_ignore_ascii_case(&row.table) && key.on_delete == "CASCADE"
            }) {
                let key = schema.foreign_key(table, foreign);
                for child in crate::row_queries::children(database, table, &key, &row)? {
                    if changes.get(&child).is_some_and(|c| !c.execution.steps.is_empty()) {
                        continue;
                    }
                    let state = store.row(&child)?.state;
                    let names_parent = state
                        .cells()
                        .get(&key.columns.0[0])
                        .and_then(|cell| cell.value.parents.get(&key))
                        .is_some_and(|parent| parent.row == row && parent.generation == generation);
                    if state.present() && names_parent {
                        if let std::collections::btree_map::Entry::Vacant(entry) =
                            changes.entry(child.clone())
                        {
                            entry.insert(deletion(database, store, &state)?);
                        }
                        // Finish this root's cascade before trying another
                        // selected row as a direct target.
                        pending.push_front(child);
                    }
                }
            }
        }
    }
    Ok(())
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
    let losses =
        records.into_iter().map(LostValue::from_record).collect::<Result<Vec<_>, DbError>>()?;
    Ok(losses.iter().any(|loss| match &loss.target {
        LossTarget::Acknowledgements(values) => values.contains(dismissal),
        LossTarget::Removed { .. } => false,
    }))
}

/// Remove the lost values the write's dismissals name. Returns the rows
/// whose losses it changed, whose file protection the caller refreshes.
pub(crate) fn apply(
    database: &DatabaseConnection,
    record: &WriteRecord,
) -> Result<BTreeSet<crate::write_rows::AppKey>, DbError> {
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
            if !dismisses(dismissal, &loss) {
                continue;
            }
            let keep = match (&dismissal.target, &mut loss.values) {
                (DismissalTarget::Cell(column), LossValues::Row(cells)) => {
                    cells.remove(column);
                    !cells.is_empty()
                }
                _ => false,
            };
            if keep {
                crate::loss_record::put(database, &loss, Some(id))?;
            } else {
                crate::loss_record::delete(database, id)?;
            }
            affected.insert((row.table.clone(), row.key.clone()));
        }
    }
    Ok(affected)
}

pub(crate) fn validate_past(
    database: &DatabaseConnection,
    header: &coven_format::write::WriteHeader,
    dismissal: &Dismissal,
) -> Result<(), DbError> {
    if !crate::write_sources::had_read(database, header, dismissal.write)? {
        return Err(crate::snapshot_error::invalid(
            "dismissal names a write outside authored past",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "dismissal_tests.rs"]
pub(crate) mod tests;
