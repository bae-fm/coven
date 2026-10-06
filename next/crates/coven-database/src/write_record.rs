//! Session changes, audience moves, and one causal write record.
use crate::merge_store::MergeStore;
use crate::sqlite::DatabaseConnection;
use crate::write_capture::CapturedRow;
use crate::write_encoding::{audience_text, counter, decoded, timestamp};
use crate::write_rows::{column_name, reference_error, row_id, AppKey, AppValues, AppView};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    merge_fields,
    value::{Value, WritePositions},
    write::{RowChange, WriteDisposition, WriteHeader, WritePart, WriteRecord},
};
use coven_foundation::id_source::{CircleId, DeviceId};
use coven_merge::{Audience, Change, ColumnValue, Operation, Parent, RowId, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

pub(crate) fn changes(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    before: &AppView<'_>,
    after: &AppView<'_>,
    stored: &MergeStore<'_>,
    captured: &BTreeMap<AppKey, CapturedRow>,
    deleted: &BTreeSet<CircleId>,
) -> Result<BTreeMap<RowId, RowChange>, DbError> {
    let mut moved = BTreeMap::new();
    let mut keys: BTreeSet<_> = captured.keys().cloned().collect();
    for key in captured.keys() {
        if let (Some(old), Some(new)) = (before.row(key)?, after.row(key)?) {
            if old.audience != new.audience {
                moved.insert(row_id(key, &old), row_id(key, &new));
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
                changes.insert(
                    row.clone(),
                    RowChange {
                        row,
                        change: Change {
                            generation,
                            operation: Operation::Delete,
                        },
                        old: old.values.clone(),
                    },
                );
            }
        }
        if let Some(new) = new {
            if !moving && !captured.contains_key(key) {
                continue;
            }
            let row = row_id(key, &new);
            let generation = generation(database, &row)?;
            let (operation, old_values) = if let Some(old) = old.filter(|_| !moving) {
                let new_values: AppValues = new
                    .values
                    .iter()
                    .filter(|(c, v)| old.values[*c] != **v)
                    .map(|(c, v)| (c.clone(), v.clone()))
                    .collect();
                if new_values.is_empty() {
                    continue;
                }
                let old_values = new_values
                    .keys()
                    .map(|c| (c.clone(), old.values[c].clone()))
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
                RowChange {
                    row,
                    change: Change {
                        generation,
                        operation,
                    },
                    old: old_values,
                },
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
            RowChange {
                row: old.clone(),
                change: Change {
                    generation: state.state.generation(),
                    operation: Operation::Delete,
                },
                old: state
                    .state
                    .cells()
                    .iter()
                    .map(|(n, c)| (n.clone(), c.value.value.clone()))
                    .collect(),
            },
        );
        let generation = generation(database, new)?;
        let values = state
            .state
            .cells()
            .iter()
            .map(|(n, c)| (n.clone(), c.value.clone()))
            .collect();
        let (operation, old_values) = if generation % 2 == 1 {
            (
                Operation::Update(values),
                removed_values(database, new, generation)?,
            )
        } else {
            (Operation::Insert(values), BTreeMap::new())
        };
        changes.entry(new.clone()).or_insert(RowChange {
            row: new.clone(),
            change: Change {
                generation,
                operation,
            },
            old: old_values,
        });
    }
    for key in validate {
        if let Some(row) = after.row(&key)? {
            let table = schema.table(&key.0);
            for fk in &table.foreign_keys {
                if let Some(parent) = row.parents.get(&schema.foreign_key(table, fk)) {
                    let parent = after.row(parent)?.expect("reference parent");
                    if parent.audience != Audience::Store && parent.audience != row.audience {
                        return Err(reference_error(table, &row.values, &fk.columns[0]));
                    }
                }
            }
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
            if deleted.contains(&circle) {
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
            let values = columns
                .iter()
                .map(|(n, c)| (n.clone(), c.value.clone()))
                .collect();
            for value in columns.values_mut() {
                for (name, parent) in &mut value.parents {
                    if parent.generation == generation(database, &parent.row)? {
                        if let Some(destination) = moved.get(&parent.row) {
                            parent.row = destination.clone();
                            parent.generation = generations[destination];
                        }
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
pub(crate) fn record(
    database: &DatabaseConnection,
    device: DeviceId,
    now: SystemTime,
    changes: BTreeMap<RowId, RowChange>,
) -> Result<WriteRecord, DbError> {
    let latest = database.query_row("SELECT max(timestamp) FROM coven_writes", [], |r| {
        let applied: Option<Vec<u8>> = r.get(0)?;
        applied
            .map(|b| decoded(merge_fields::decode_timestamp(&b)))
            .transpose()
    })?;
    let timestamp = timestamp(latest, now, device)?;
    let mut positions: BTreeMap<DeviceId,u64> = database.query(
        "SELECT device,number FROM coven_positions WHERE device>=x'0000000000000000' ORDER BY device", [],
        |r| Ok((DeviceId(counter(r.get(0)?)),counter(r.get(1)?))),
    )?.into_iter().collect();
    let number = match positions.remove(&device) {
        Some(number) => number
            .checked_add(1)
            .expect("write numbers exhausted before timestamps"),
        None => 1,
    };
    let header = WriteHeader {
        position: WriteId { device, number },
        timestamp,
        had_read: WritePositions(
            positions
                .into_iter()
                .map(|(device, number)| WriteId { device, number })
                .collect(),
        ),
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
    Ok(WriteRecord {
        header,
        parts: parts
            .into_iter()
            .map(|(audience, rows)| WritePart { audience, rows })
            .collect(),
    })
}

pub(crate) fn generation(database: &DatabaseConnection, row: &RowId) -> Result<u64, DbError> {
    let generation: Option<Vec<u8>> = database.query_row(
        "SELECT max(generation) FROM coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3",
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
        "SELECT value FROM coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4 AND column_id IS NULL AND replacement_kind='rules'",
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
