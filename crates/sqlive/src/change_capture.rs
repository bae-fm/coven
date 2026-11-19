//! Convert transaction changes into read dependencies before commit: each
//! operation on a row a key names, which `net_changes.rs` folds into the
//! row's net change, and the changes no key names.

use crate::authorization::HostError;
use crate::authorization::Policy;
use crate::authorization::PolicyError;
use crate::connection::table_shapes::{Shadow, TableShape, TableShapes};
use crate::connection::HostSql;
use crate::net_changes::{Captured, ColumnChange, RowOperation};
use crate::{RowChange, RowImage, SqlValue};
use rusqlite::{
    fallible_streaming_iterator::FallibleStreamingIterator, hooks::Action, session::ChangesetIter,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// The columns of each table live queries read or test, whose values a
/// write keeps of the rows it changes, by table, lowercase
/// (`Handle::kept_columns`).
pub(crate) type KeptColumns = BTreeMap<String, BTreeSet<String>>;

/// The columns a table's row images hold: its key's, then those `kept`
/// names, each with its position among `names`.
fn image_columns(
    names: &[&str],
    key: impl Fn(usize) -> bool,
    kept: Option<&BTreeSet<String>>,
) -> (Arc<[String]>, Vec<usize>) {
    let positions: Vec<usize> =
        (0..names.len())
            .filter(|index| key(*index))
            .chain((0..names.len()).filter(|index| {
                !key(*index) && kept.is_some_and(|kept| kept.contains(names[*index]))
            }))
            .collect();
    let columns = positions.iter().map(|index| names[*index].to_owned()).collect();
    (columns, positions)
}

/// The shapes a change to `table`, as SQLite names it in one of the
/// transaction's changes, is a change under: the table's own, and for a
/// contentful FTS5 table's content table the FTS5 table's
/// (`Shadow::Rows`). Notes the change of the virtual table that each change
/// of a shadow table changes whole (`Shadow::Whole`).
fn changed_shapes<'a, F: HostError, R: HostError>(
    shapes: &'a TableShapes,
    table: &str,
    changes: &mut Captured,
) -> Result<Vec<&'a TableShape>, crate::Error<F, R>> {
    let shape = shapes
        .position(table)
        .map(|position| shapes.shape(position))
        .ok_or(crate::Error::Invariant { what: "every table a transaction changes has a shape" })?;
    let mut found = vec![shape];
    match &shape.shadow {
        Some(Shadow::Whole(parent)) => {
            changes.push(RowChange { table: parent.clone(), column: String::new(), rows: None });
        }
        Some(Shadow::Rows(position)) => found.push(shapes.shape(*position)),
        Some(Shadow::Index) | None => {}
    }
    Ok(found)
}

/// A whole-table change to each of `tables`, as a transaction that records
/// only which tables it changed reports them: a shadow table's change is its
/// virtual table's too.
pub(crate) fn table_changes<P: Policy>(
    database: &HostSql<P>,
    mut tables: BTreeSet<String>,
) -> Result<Captured, PolicyError<P>> {
    let shapes = database.table_shapes()?;
    for table in tables.clone() {
        if let Some(parent) = shapes.parent(&table) {
            tables.insert(parent.to_owned());
        }
    }
    Ok(Captured::unkeyed(
        tables
            .into_iter()
            .map(|table| RowChange { table, column: String::new(), rows: None })
            .collect(),
    ))
}

/// What a session's changeset `bytes` holds, with the values of the
/// columns `kept` names: one operation for each row it changed, which the
/// session already gives as the row's net change since the session began.
pub(crate) fn changes<P: Policy>(
    database: &HostSql<P>,
    bytes: &[u8],
    kept: &KeptColumns,
) -> Result<Captured, PolicyError<P>> {
    let mut input = bytes;
    let input: &mut dyn std::io::Read = &mut input;
    let mut items = ChangesetIter::start_strm(&input)?;
    let shapes = database.table_shapes()?;
    let mut images = BTreeMap::<String, (Arc<[String]>, Vec<usize>)>::new();
    let mut changes = Captured::default();
    while let Some(item) = items.next()? {
        let operation = item.op()?;
        for columns in changed_shapes(&shapes, operation.table_name(), &mut changes)? {
            let table = columns.name.clone();
            let first = !images.contains_key(&table);
            // SQLITE_SESSION_OBJCONFIG_ROWID adds the implicit key first. Session
            // positions otherwise omit generated columns; event positions don't.
            let names: Vec<&str> = columns
                .whole
                .then_some("rowid")
                .into_iter()
                .chain(
                    columns
                        .columns
                        .iter()
                        .filter(|(_, _, hidden)| !hidden)
                        .map(|(name, _, _)| name.as_str()),
                )
                .collect();
            if first {
                assert_eq!(
                    names.len(),
                    operation.number_of_columns() as usize,
                    "session columns match table metadata"
                );
            }
            let pk = item.pk()?;
            let (image, positions) = images
                .entry(table.clone())
                .or_insert_with(|| image_columns(&names, |index| pk[index] != 0, kept.get(&table)))
                .clone();
            let mut before = Vec::with_capacity(names.len());
            let mut after = Vec::with_capacity(names.len());
            let mut changed = Vec::new();
            for (index, key) in pk.iter().enumerate() {
                let old = if operation.code() == Action::SQLITE_INSERT {
                    None
                } else {
                    value(item.old_value(index), index)?
                };
                let new = if operation.code() == Action::SQLITE_DELETE {
                    None
                } else {
                    value(item.new_value(index), index)?
                };
                if *key != 0 {
                    // An UPDATE includes the old key and omits unchanged new keys.
                    let new = match (operation.code(), new) {
                        (Action::SQLITE_UPDATE, None) => old.clone(),
                        (_, new) => new,
                    };
                    before.push(Some(old.unwrap_or(SqlValue::Null)));
                    after.push(Some(new.unwrap_or(SqlValue::Null)));
                } else {
                    // An UPDATE holds only the columns it changed; an insert
                    // or a delete changes every column.
                    if operation.code() != Action::SQLITE_UPDATE || old.is_some() || new.is_some() {
                        changed.push(index);
                    }
                    before.push(old);
                    after.push(new);
                }
            }
            let key: Vec<SqlValue> = (0..names.len())
                .filter(|index| pk[*index] != 0)
                .map(|index| {
                    let side =
                        if operation.code() == Action::SQLITE_INSERT { &after } else { &before };
                    side[index].clone().unwrap_or(SqlValue::Null)
                })
                .collect();
            let column_changes = changed
                .iter()
                .map(|index| {
                    ColumnChange::new(
                        names[*index].to_owned(),
                        positions.contains(index),
                        before[*index].clone(),
                        after[*index].clone(),
                    )
                })
                .collect();
            let row = |values: &[Option<SqlValue>]| {
                RowImage::new(
                    Arc::clone(&image),
                    positions.iter().map(|index| values[*index].clone()).collect(),
                )
            };
            let (old, new) = match operation.code() {
                Action::SQLITE_INSERT => (None, Some(row(&after))),
                Action::SQLITE_DELETE => (Some(row(&before)), None),
                _ => {
                    // A column an UPDATE leaves alone is absent from its changes:
                    // its value is the row's still, so read it there.
                    let unchanged: Vec<usize> = positions
                        .iter()
                        .copied()
                        .filter(|index| pk[*index] == 0 && before[*index].is_none())
                        .collect();
                    if !unchanged.is_empty() {
                        let found = current(database, &table, &names, pk, &before, &unchanged)?;
                        for (index, value) in unchanged.into_iter().zip(found) {
                            before[index] = value.clone();
                            after[index] = value;
                        }
                    }
                    (Some(row(&before)), Some(row(&after)))
                }
            };
            changes.record(RowOperation {
                table,
                key,
                before: old,
                after: new,
                columns: column_changes,
                // Sessions omit generated columns; they can change with their inputs.
                generated: columns.generated(),
            });
        }
    }
    Ok(changes)
}

/// The current values of the `wanted` columns of the row whose key `before`
/// holds; `None` each when no row has that key, as one with a NULL in it.
fn current<P: Policy>(
    database: &HostSql<P>,
    table: &str,
    names: &[&str],
    pk: &[u8],
    before: &[Option<SqlValue>],
    wanted: &[usize],
) -> Result<Vec<Option<SqlValue>>, PolicyError<P>> {
    let keys: Vec<usize> = (0..names.len()).filter(|index| pk[*index] != 0).collect();
    let sql = format!(
        "SELECT {} FROM main.{} WHERE {}",
        wanted
            .iter()
            .map(|index| crate::sql::identifier(names[*index]))
            .collect::<Vec<_>>()
            .join(","),
        crate::sql::identifier(table),
        keys.iter()
            .enumerate()
            .map(|(slot, index)| format!("{}=?{}", crate::sql::identifier(names[*index]), slot + 1))
            .collect::<Vec<_>>()
            .join(" AND "),
    );
    let key: Vec<&SqlValue> = keys
        .iter()
        .map(|index| before[*index].as_ref().expect("a change's key is recorded"))
        .collect();
    let found = database.query(&sql, rusqlite::params_from_iter(key), |row| {
        (0..wanted.len())
            .map(|index| row.get::<_, SqlValue>(index).map(Some))
            .collect::<rusqlite::Result<Vec<_>>>()
    })?;
    Ok(found.into_iter().next().unwrap_or_else(|| vec![None; wanted.len()]))
}

fn value(
    value: rusqlite::Result<rusqlite::types::ValueRef<'_>>,
    index: usize,
) -> rusqlite::Result<Option<SqlValue>> {
    match value {
        Ok(value) => Ok(Some(value.into())),
        Err(rusqlite::Error::InvalidColumnIndex(i)) if i == index => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read dependencies of a local write's app phase, from its row operations.
/// Each operation counts as a session change of its row would: inserts and
/// deletes change the whole row, updates each column whose value changed,
/// and a primary-key update deletes the old key's row and inserts the new.
/// Each operation on a row a key names folds into the row's net change;
/// tables with no primary key report table-wide changes.
pub(crate) fn event_changes<P: Policy>(
    database: &HostSql<P>,
    events: &[crate::connection::Operation],
    kept: &KeptColumns,
) -> Result<Captured, PolicyError<P>> {
    let shapes = database.table_shapes()?;
    let mut images = BTreeMap::<String, (Arc<[String]>, Vec<usize>)>::new();
    let mut changes = Captured::default();
    for event in events {
        for fields in changed_shapes(&shapes, &event.table, &mut changes)? {
            let table = fields.name.clone();
            if fields.whole {
                changes.push(RowChange { table, column: String::new(), rows: None });
                continue;
            }
            let (image, positions) = images
                .entry(table.clone())
                .or_insert_with(|| {
                    let names: Vec<&str> =
                        fields.columns.iter().map(|(name, _, _)| name.as_str()).collect();
                    image_columns(&names, |index| fields.columns[index].1, kept.get(&table))
                })
                .clone();
            let at =
                |values: &[Option<SqlValue>], index: usize| values.get(index).cloned().flatten();
            let key = |values: &[Option<SqlValue>]| {
                (0..fields.columns.len())
                    .filter(|index| fields.columns[*index].1)
                    .map(|index| at(values, index).unwrap_or(SqlValue::Null))
                    .collect::<Vec<_>>()
            };
            let row = |values: &[Option<SqlValue>]| {
                RowImage::new(
                    Arc::clone(&image),
                    positions.iter().map(|index| at(values, *index)).collect(),
                )
            };
            // The stored columns outside the key `changed` picks, with the
            // values the transaction keeps of them.
            let columns = |old: &[Option<SqlValue>],
                           new: &[Option<SqlValue>],
                           changed: &dyn Fn(usize) -> bool| {
                fields
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(index, (_, pk, hidden))| !pk && !hidden && changed(*index))
                    .map(|(index, (name, _, _))| {
                        ColumnChange::new(
                            name.clone(),
                            positions.contains(&index),
                            at(old, index),
                            at(new, index),
                        )
                    })
                    .collect()
            };
            let operation = |key, before, after, columns| RowOperation {
                table: table.clone(),
                key,
                before,
                after,
                columns,
                // A generated column changes with the stored columns it reads.
                generated: fields.generated(),
            };
            let inserted = |new: &[Option<SqlValue>]| {
                operation(key(new), None, Some(row(new)), columns(&[], new, &|_| true))
            };
            let deleted = |old: &[Option<SqlValue>]| {
                operation(key(old), Some(row(old)), None, columns(old, &[], &|_| true))
            };
            match event.action {
                Action::SQLITE_INSERT => changes.record(inserted(&event.new)),
                Action::SQLITE_DELETE => changes.record(deleted(&event.old)),
                _ if key(&event.old) != key(&event.new) => {
                    changes.record(deleted(&event.old));
                    changes.record(inserted(&event.new));
                }
                _ => changes.record(operation(
                    key(&event.old),
                    Some(row(&event.old)),
                    Some(row(&event.new)),
                    columns(&event.old, &event.new, &|index| {
                        event.old.get(index) != event.new.get(index)
                    }),
                )),
            }
        }
    }
    Ok(changes)
}
