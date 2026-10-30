//! Session changes, audience moves, and one causal write record.
use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, counter, decoded, timestamp};
use crate::write_rows::{column_name, reference_error, row_id, AppKey, AppValues, AppView};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    merge_fields,
    value::Value,
    write::{RowChange, RowExecution, WriteDisposition, WriteHeader, WritePart, WriteRecord},
};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, Change, ColumnValue, Operation, Parent, RowId, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

/// The rows a write moves to another audience (§15.4): each row whose
/// audience its own values changed, and, through any chain, each child that
/// inherits its audience from a moved parent, whether shown or taken out.
/// Returns them by their old and new identities, and the keys of the
/// inheriting children it added.
pub(crate) fn moved(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    before: &AppView<'_>,
    after: &AppView<'_>,
    stored: &MergeStore<'_>,
    changed: impl IntoIterator<Item = AppKey>,
) -> Result<(BTreeMap<RowId, RowId>, BTreeSet<AppKey>), DbError> {
    let mut moved = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for key in changed {
        // A row the write inserted had no audience to leave, so its shown
        // values aren't read.
        let Some(old) = before.row(&key)? else {
            continue;
        };
        if let Some(new) = after.row(&key)? {
            if old.audience != new.audience {
                moved.insert(row_id(&key, &old), row_id(&key, &new));
            }
        }
    }
    let mut pending: Vec<_> = moved.keys().cloned().collect();
    while let Some(parent) = pending.pop() {
        for declaration in &schema.declarations {
            let crate::declaration::AudienceSource::ForeignKey(column) = &declaration.audience
            else {
                continue;
            };
            let table = schema.table(&declaration.name);
            let fk = table
                .foreign_keys
                .iter()
                .find(|fk| fk.columns.iter().any(|c| c.eq_ignore_ascii_case(column)))
                .expect("audience foreign key");
            if !fk.target.eq_ignore_ascii_case(&parent.table) {
                continue;
            }
            for child in crate::row_queries::children(
                database,
                table,
                &schema.foreign_key(table, fk),
                &parent,
            )? {
                if moved.contains_key(&child) {
                    continue;
                }
                let state = stored.row(&child)?;
                let reference = &state.state.cells()[column_name(table, column)]
                    .value
                    .parents[&schema.foreign_key(table, fk)];
                if reference.row != parent || reference.generation != generation(database, &parent)?
                {
                    continue;
                }
                let key = (child.table.clone(), child.key.clone());
                let audience = if state.loss.is_some() {
                    moved[&parent].audience.clone()
                } else {
                    let Some(new) = after.row(&key)? else {
                        continue;
                    };
                    new.audience
                };
                if audience == child.audience {
                    continue;
                }
                moved.insert(
                    child.clone(),
                    RowId {
                        audience,
                        ..child.clone()
                    },
                );
                keys.insert(key);
                pending.push(child);
            }
        }
    }
    Ok((moved, keys))
}

/// The write's row changes. `copies` names, for each file a moved row
/// taken out by removal rules references, the new file the move made for it
/// in the row's new audience (§17.2); a shown row's file the write's SQL
/// already changed.
pub(crate) fn changes(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    before: &AppView<'_>,
    after: &AppView<'_>,
    stored: &MergeStore<'_>,
    changed: &BTreeMap<AppKey, BTreeSet<String>>,
    deleted: &BTreeMap<CircleId, crate::Slot>,
    copies: &crate::file_write::Copied,
) -> Result<BTreeMap<RowId, RowChange>, DbError> {
    let (moved, inherited) = moved(
        database,
        schema,
        before,
        after,
        stored,
        changed.keys().cloned(),
    )?;
    let mut keys: BTreeSet<_> = changed.keys().cloned().collect();
    keys.extend(inherited);
    let mut changes = BTreeMap::new();
    let mut validate = keys.clone();
    for key in &keys {
        let old = before.row(key)?;
        let new = after.row(key)?;
        let moving = old
            .as_ref()
            .zip(new.as_ref())
            .is_some_and(|(a, b)| a.audience != b.audience);
        if let Some(old) = &old {
            let row = row_id(key, old);
            if moving {
                // Changing a parent's audience can invalidate an unchanged child's
                // ordinary reference. Reverse indexes locate exactly those children.
                for declaration in &schema.declarations {
                    let table = schema.table(&declaration.name);
                    for fk in table
                        .foreign_keys
                        .iter()
                        .filter(|fk| fk.target.eq_ignore_ascii_case(&row.table))
                    {
                        validate.extend(
                            crate::row_queries::children(
                                database,
                                table,
                                &schema.foreign_key(table, fk),
                                &row,
                            )?
                            .into_iter()
                            .map(|r| (r.table, r.key)),
                        );
                    }
                }
            }
            if moving || new.is_none() {
                let generation = generation(database, &row)?;
                assert_eq!(generation % 2, 1, "visible row has no present generation");
                // A delete's old values are the cells it replaces, as their
                // setters wrote them: what receivers can check (D5), not a
                // display substitution such as a stale reference's NULL.
                let cells = stored
                    .row(&row)?
                    .state
                    .cells()
                    .iter()
                    .map(|(name, cell)| (name.clone(), cell.value.value.clone()))
                    .collect();
                changes.insert(
                    row.clone(),
                    crate::write_sources::authored(
                        database,
                        stored,
                        row,
                        Change {
                            generation,
                            operation: Operation::Delete,
                        },
                        cells,
                    )?,
                );
            }
        }
        if let Some(new) = new {
            if !moving && !changed.contains_key(key) {
                continue;
            }
            let row = row_id(key, &new);
            let generation = generation(database, &row)?;
            let (operation, old_values) = if let Some(old) = old.filter(|_| !moving) {
                let mut new_values: AppValues = new
                    .values
                    .iter()
                    .filter(|(c, _)| changed[key].contains(*c))
                    .map(|(c, v)| (c.clone(), v.clone()))
                    .collect();
                if new_values.is_empty() {
                    continue;
                }
                // Capture every column that must share this setter. Expanding a
                // reference can touch another reference or the file tuple.
                loop {
                    let before = new_values.len();
                    // A record's coordinates and content hash move together
                    // (§17.2), so a file's place always comes whole from one write.
                    if crate::file_table::is_files(&key.0)
                        && crate::file_table::PLACE
                            .iter()
                            .any(|column| new_values.contains_key(*column))
                    {
                        for column in crate::file_table::PLACE {
                            new_values.insert(column.to_owned(), new.values[column].clone());
                        }
                    }
                    for group in &schema.rules[&key.0].reference_groups {
                        if group.iter().any(|column| new_values.contains_key(column)) {
                            for column in group {
                                new_values.insert(column.clone(), new.values[column].clone());
                            }
                        }
                    }
                    if new_values.len() == before {
                        break;
                    }
                }
                let state = stored.row(&row)?.state;
                let old_values = new_values
                    .keys()
                    .map(|c| {
                        (
                            c.clone(),
                            match (state.cells().get(c), old.values.get(c)) {
                                (Some(cell), _) => cell.value.value.clone(),
                                (None, Some(value)) => value.clone(),
                                // A column introduced by this migration has no
                                // old cell; the update's old-value field is NULL.
                                (None, None) => Value::Null,
                            },
                        )
                    })
                    .collect();
                (Operation::Update(columns(&new_values)), old_values)
            } else if generation % 2 == 1 {
                (
                    Operation::Update(columns(&new.values)),
                    removed_values(database, &row, generation)?,
                )
            } else {
                (Operation::Insert(columns(&new.values)), BTreeMap::new())
            };
            changes.insert(
                row.clone(),
                crate::write_sources::authored(
                    database,
                    stored,
                    row,
                    Change {
                        generation,
                        operation,
                    },
                    old_values,
                )?,
            );
        }
    }
    for (old, new) in &moved {
        let state = stored.row(old)?;
        if state.loss.is_none() {
            continue;
        }
        changes.insert(
            old.clone(),
            crate::write_sources::authored(
                database,
                stored,
                old.clone(),
                Change {
                    generation: state.state.generation(),
                    operation: Operation::Delete,
                },
                state
                    .state
                    .cells()
                    .iter()
                    .map(|(n, c)| (n.clone(), c.value.value.clone()))
                    .collect(),
            )?,
        );
        let generation = generation(database, new)?;
        let values = state
            .state
            .cells()
            .iter()
            .map(|(n, c)| {
                (
                    n.clone(),
                    copied(
                        schema,
                        &old.table,
                        n,
                        &new.audience,
                        c.value.clone(),
                        copies,
                    ),
                )
            })
            .collect();
        let (operation, old_values) = if generation % 2 == 1 {
            (
                Operation::Update(values),
                removed_values(database, new, generation)?,
            )
        } else {
            (Operation::Insert(values), BTreeMap::new())
        };
        if let std::collections::btree_map::Entry::Vacant(entry) = changes.entry(new.clone()) {
            entry.insert(crate::write_sources::authored(
                database,
                stored,
                new.clone(),
                Change {
                    generation,
                    operation,
                },
                old_values,
            )?);
        }
    }
    for key in validate {
        if let Some(row) = after.row(&key)? {
            let table = schema.table(&key.0);
            for fk in &table.foreign_keys {
                if let Some(parent) = row.parents.get(&schema.foreign_key(table, fk)) {
                    let parent = after.row(parent)?.expect("reference parent");
                    // A file lives in one audience, and only its rows
                    // reference it (§17.2).
                    if fk.is_file() && parent.audience != row.audience {
                        return Err(crate::file_write::unknown(
                            table,
                            &row.values,
                            &fk.columns[0],
                        ));
                    }
                    if parent.audience != Audience::Store && parent.audience != row.audience {
                        return Err(reference_error(table, &row.values, &fk.columns[0]));
                    }
                }
            }
        }
    }
    // A copy into another audience keeps the row's original creation (§9.6).
    for (old, new) in &moved {
        let Some(change) = changes.get_mut(new) else {
            continue;
        };
        if change.change.generation == 0 && matches!(change.change.operation, Operation::Insert(_))
        {
            let creation =
                crate::ownership::creation(database, old)?.ok_or(DbError::DamagedDatabase)?;
            change.creator = Some(creation.creator);
        }
    }
    let generations: BTreeMap<_, _> = changes
        .iter()
        .map(|(row, c)| {
            (
                row.clone(),
                match c.change.operation {
                    Operation::Update(_) => c.change.generation,
                    _ => c
                        .change
                        .generation
                        .checked_add(1)
                        .expect("generation exhausted"),
                },
            )
        })
        .collect();
    for (row, change) in &mut changes {
        if let Audience::Circle(circle) = row.audience {
            if deleted.contains_key(&circle) {
                return Err(DbError::DeletedCircle(circle));
            }
        }
        let columns = match &mut change.change.operation {
            Operation::Insert(c) | Operation::Update(c) => c,
            Operation::Delete => continue,
        };
        let table = schema.table(&row.table);
        let app = after
            .row(&(row.table.clone(), row.key.clone()))?
            .filter(|a| a.audience == row.audience);
        if let Some(app) = app {
            for fk in &table.foreign_keys {
                let name = schema.foreign_key(table, fk);
                let Some(key) = app.parents.get(&name) else {
                    // A reference names a file this device has a record of
                    // (§17.2).
                    if fk.is_file()
                        && columns
                            .get(column_name(table, &fk.columns[0]))
                            .is_some_and(|value| value.value != Value::Null)
                    {
                        return Err(crate::file_write::unknown(
                            table,
                            &app.values,
                            &fk.columns[0],
                        ));
                    }
                    continue;
                };
                let parent = row_id(key, &after.row(key)?.expect("reference parent"));
                let generation = match generations.get(&parent) {
                    Some(g) => *g,
                    None => generation(database, &parent)?,
                };
                assert_eq!(generation % 2, 1, "reference has no present parent");
                for column in &fk.columns {
                    if let Some(value) = columns.get_mut(column_name(table, column)) {
                        value.parents.insert(
                            name.clone(),
                            Parent {
                                row: parent.clone(),
                                generation,
                            },
                        );
                    }
                }
            }
        } else {
            let values: AppValues = columns
                .iter()
                .map(|(n, c)| (n.clone(), c.value.clone()))
                .collect();
            // A moved row's new file is made in this write, in the row's new
            // audience (§17.2).
            for fk in table.foreign_keys.iter().filter(|fk| fk.is_file()) {
                let name = schema.foreign_key(table, fk);
                let column = column_name(table, &fk.columns[0]);
                let Some(value) = columns.get_mut(column) else {
                    continue;
                };
                if value.parents.contains_key(&name) {
                    continue;
                }
                if let Some(file) = crate::file_table::id(&value.value) {
                    let parent = crate::file_table::row(file, row.audience.clone())?;
                    let generation = match generations.get(&parent) {
                        Some(g) => *g,
                        None => generation(database, &parent)?,
                    };
                    if generation % 2 != 1 {
                        return Err(crate::file_write::unknown(table, &values, column));
                    }
                    value.parents.insert(
                        name,
                        Parent {
                            row: parent,
                            generation,
                        },
                    );
                }
            }
            for value in columns.values_mut() {
                for (name, parent) in &mut value.parents {
                    if parent.generation == generation(database, &parent.row)? {
                        if let Some(destination) = moved.get(&parent.row) {
                            parent.row = destination.clone();
                            parent.generation = generations[destination];
                        }
                    }
                    if crate::file_table::is_files(&parent.row.table)
                        && parent.row.audience != row.audience
                    {
                        return Err(crate::file_write::unknown(
                            table,
                            &values,
                            &name.columns.0[0],
                        ));
                    }
                    if parent.row.audience != Audience::Store && parent.row.audience != row.audience
                    {
                        return Err(reference_error(table, &values, &name.columns.0[0]));
                    }
                }
            }
        }
    }
    Ok(changes)
}
/// The write record of a local write's net changes and the SQL operations
/// that produced them, or nothing when no row changed. Rows whose operations
/// left no net change keep those operations as executions (D5).
pub(crate) fn record(
    database: &DatabaseConnection,
    device: DeviceId,
    now: SystemTime,
    changes: BTreeMap<RowId, RowChange>,
    steps: BTreeMap<RowId, Vec<coven_format::write::Step>>,
    store: &MergeStore<'_>,
) -> Result<Option<WriteRecord>, DbError> {
    if changes.is_empty() {
        return Ok(None);
    }
    let mut changes = changes;
    let mut executions = BTreeMap::<Audience, Vec<RowExecution>>::new();
    for (row, steps) in steps {
        match changes.get_mut(&row) {
            Some(change) => {
                change.execution = crate::write_sources::executed(
                    database,
                    store,
                    &row,
                    steps,
                    Some(&change.sources),
                )?;
            }
            None => {
                let execution = crate::write_sources::executed(database, store, &row, steps, None)?;
                executions
                    .entry(row.audience.clone())
                    .or_default()
                    .push(RowExecution { row, execution });
            }
        }
    }
    unordered(database, device, now, changes, executions).map(Some)
}

/// The record of these changes, for writes made without SQL operations.
pub(crate) fn changed(
    database: &DatabaseConnection,
    device: DeviceId,
    now: SystemTime,
    changes: BTreeMap<RowId, RowChange>,
) -> Result<WriteRecord, DbError> {
    unordered(database, device, now, changes, BTreeMap::new())
}

fn unordered(
    database: &DatabaseConnection,
    device: DeviceId,
    now: SystemTime,
    changes: BTreeMap<RowId, RowChange>,
    mut executions: BTreeMap<Audience, Vec<RowExecution>>,
) -> Result<WriteRecord, DbError> {
    let latest = crate::write_encoding::latest_timestamp(database)?;
    let timestamp = timestamp(latest, now, device)?;
    let previous: Option<Vec<u8>> = database.query_row(
        &format!("SELECT max(number) FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1"),
        [device.0.to_be_bytes().as_slice()],
        |r| r.get(0),
    )?;
    let number = previous
        .map(counter)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(DbError::WriteNumberExhausted)?;
    let header = WriteHeader {
        position: WriteId { device, number },
        timestamp,
        read_through: crate::sequence::applied(database)?,
        schema_version: database.schema_version()?,
        disposition: WriteDisposition::Apply,
    };
    let mut parts = BTreeMap::<Audience, Vec<RowChange>>::new();
    for change in changes.into_values() {
        parts
            .entry(change.row.audience.clone())
            .or_default()
            .push(change);
    }
    for audience in executions.keys() {
        parts.entry(audience.clone()).or_default();
    }
    Ok(WriteRecord {
        header,
        parts: parts
            .into_iter()
            .map(|(audience, rows)| WritePart {
                executions: executions.remove(&audience).unwrap_or_default(),
                audience,
                rows,
                dismissals: Vec::new(),
            })
            .collect(),
    })
}

pub(crate) fn generation(database: &DatabaseConnection, row: &RowId) -> Result<u64, DbError> {
    let generation: Option<Vec<u8>> = database.query_row(
        "SELECT max(generation) FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3",
        params![row.table, row.key, audience_text(&row.audience)],
        |r| r.get(0),
    )?;
    Ok(match generation {
        Some(generation) => counter(generation),
        None => 0,
    })
}

fn removed_values(
    database: &DatabaseConnection,
    row: &RowId,
    generation: u64,
) -> Result<AppValues, DbError> {
    let rows = database.query(
        "SELECT value FROM _coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4 AND column_id IS NULL AND replacement_kind='rules' AND frozen=0",
        params![row.table, row.key, audience_text(&row.audience), generation.to_be_bytes().as_slice()],
        |r| { let bytes: Vec<u8> = r.get(0)?; decoded(merge_fields::decode_columns(&bytes)) },
    )?;
    assert!(
        rows.len() == 1,
        "removed row must have exactly one stored value: {row:?}"
    );
    Ok(rows
        .into_iter()
        .next()
        .expect("one removed row")
        .into_iter()
        .map(|(column, value)| (column, value.value))
        .collect())
}

/// A moved row's cell, naming the new file the move made in the row's new
/// audience in place of the file it named in the old one (§17.2).
fn copied(
    schema: &WriteSchema,
    table: &str,
    column: &str,
    audience: &Audience,
    value: ColumnValue<Value>,
    copies: &crate::file_write::Copied,
) -> ColumnValue<Value> {
    if schema.file_column(table, column).is_none() {
        return value;
    }
    match crate::file_table::id(&value.value).and_then(|file| copies.get(&(file, audience.clone())))
    {
        Some(copy) => ColumnValue {
            value: Value::Text(copy.0.to_string()),
            parents: BTreeMap::new(),
        },
        None => value,
    }
}

fn columns(values: &AppValues) -> BTreeMap<String, ColumnValue<Value>> {
    values
        .iter()
        .map(|(column, value)| {
            (
                column.clone(),
                ColumnValue {
                    value: value.clone(),
                    parents: BTreeMap::new(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
#[path = "write_record_tests.rs"]
mod tests;
