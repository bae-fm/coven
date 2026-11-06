//! Convert transaction changes into read dependencies before commit.

use crate::sqlite::table_shapes::{TableShape, TableShapes};
use crate::{
    observation::RowChange, row_scope::RowImage, sql_value::SqlValue, sqlite::DatabaseConnection,
    DbError,
};
use rusqlite::{
    fallible_streaming_iterator::FallibleStreamingIterator, hooks::Action, session::ChangesetIter,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// The columns waiting live queries' scopes test, by table, lowercase.
pub(crate) type ScopedColumns = BTreeMap<String, BTreeSet<String>>;

/// The columns a table's row images hold: its key's, then those `scoped`
/// names, each with its position among `names`.
fn image_columns(
    names: &[&str],
    key: impl Fn(usize) -> bool,
    scoped: Option<&BTreeSet<String>>,
) -> (Arc<[String]>, Vec<usize>) {
    let positions: Vec<usize> = (0..names.len())
        .filter(|index| key(*index))
        .chain((0..names.len()).filter(|index| {
            !key(*index) && scoped.is_some_and(|scoped| scoped.contains(names[*index]))
        }))
        .collect();
    let columns = positions.iter().map(|index| names[*index].to_owned()).collect();
    (columns, positions)
}

/// The shape of `table`, as SQLite names it in one of the transaction's
/// changes, noting the change of the virtual table a shadow table belongs to.
fn changed_table<'a>(
    shapes: &'a TableShapes,
    table: &str,
    changes: &mut Vec<RowChange>,
) -> Result<&'a TableShape, DbError> {
    let shape = shapes
        .position(table)
        .map(|position| shapes.shape(position))
        .ok_or(DbError::Invariant { what: "every table a transaction changes has a shape" })?;
    if let Some(parent) = &shape.parent {
        changes.push(RowChange { table: parent.clone(), column: String::new(), rows: None });
    }
    Ok(shape)
}

/// A whole-table change to each of `tables`, as a transaction that records
/// only which tables it changed reports them: a shadow table's change is its
/// virtual table's too.
pub(crate) fn table_changes(
    database: &DatabaseConnection,
    mut tables: BTreeSet<String>,
) -> Result<Vec<RowChange>, DbError> {
    let shapes = database.table_shapes()?;
    for table in tables.clone() {
        if let Some(parent) = shapes.parent(&table) {
            tables.insert(parent.to_owned());
        }
    }
    Ok(tables
        .into_iter()
        .map(|table| RowChange { table, column: String::new(), rows: None })
        .collect())
}

pub(crate) fn changes(
    database: &DatabaseConnection,
    bytes: &[u8],
    scoped: &ScopedColumns,
) -> Result<Vec<RowChange>, DbError> {
    let mut input = bytes;
    let input: &mut dyn std::io::Read = &mut input;
    let mut items = ChangesetIter::start_strm(&input)?;
    let shapes = database.table_shapes()?;
    let mut images = BTreeMap::<String, (Arc<[String]>, Vec<usize>)>::new();
    let mut changes = Vec::new();
    while let Some(item) = items.next()? {
        let operation = item.op()?;
        let columns = changed_table(&shapes, operation.table_name(), &mut changes)?;
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
            .or_insert_with(|| image_columns(&names, |index| pk[index] != 0, scoped.get(&table)))
            .clone();
        let mut before = Vec::with_capacity(names.len());
        let mut after = Vec::with_capacity(names.len());
        let mut changed = Vec::new();
        for (index, name) in names.iter().enumerate() {
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
            if pk[index] != 0 {
                // An UPDATE includes the old key and omits unchanged new keys.
                let new = match (operation.code(), new) {
                    (Action::SQLITE_UPDATE, None) => old.clone(),
                    (_, new) => new,
                };
                before.push(Some(old.unwrap_or(SqlValue::Null)));
                after.push(Some(new.unwrap_or(SqlValue::Null)));
            } else {
                if old.is_some() || new.is_some() {
                    changed.push((*name).to_owned());
                }
                before.push(old);
                after.push(new);
            }
        }
        let row = |values: &[Option<SqlValue>]| {
            RowImage::new(
                Arc::clone(&image),
                positions.iter().map(|index| values[*index].clone()).collect(),
            )
        };
        let rows = match operation.code() {
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
        if operation.code() == Action::SQLITE_UPDATE {
            // Sessions omit generated columns; they can change with their inputs.
            changed.extend(
                columns
                    .columns
                    .iter()
                    .filter(|(_, _, hidden)| *hidden)
                    .map(|(name, _, _)| name.clone()),
            );
        } else {
            changed = vec![String::new()];
        }
        for column in changed {
            changes.push(RowChange { table: table.clone(), column, rows: Some(rows.clone()) });
        }
    }
    Ok(changes)
}

/// The current values of the `wanted` columns of the row whose key `before`
/// holds; `None` each when no row has that key, as one with a NULL in it.
fn current(
    database: &DatabaseConnection,
    table: &str,
    names: &[&str],
    pk: &[u8],
    before: &[Option<SqlValue>],
    wanted: &[usize],
) -> Result<Vec<Option<SqlValue>>, DbError> {
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
/// Tables with no primary key report table-wide changes.
pub(crate) fn event_changes(
    database: &DatabaseConnection,
    events: &[crate::sqlite::Event],
    scoped: &ScopedColumns,
) -> Result<Vec<RowChange>, DbError> {
    let shapes = database.table_shapes()?;
    let mut images = BTreeMap::<String, (Arc<[String]>, Vec<usize>)>::new();
    let mut changes = Vec::new();
    for event in events {
        let fields = changed_table(&shapes, &event.table, &mut changes)?;
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
                image_columns(&names, |index| fields.columns[index].1, scoped.get(&table))
            })
            .clone();
        let key = |values: &[Option<SqlValue>]| {
            fields
                .columns
                .iter()
                .zip(values.iter().map(Some).chain(std::iter::repeat(None)))
                .filter(|((_, pk, _), _)| *pk)
                .map(|(_, value)| value.and_then(Option::clone).unwrap_or(SqlValue::Null))
                .collect::<Vec<_>>()
        };
        let row = |values: &[Option<SqlValue>]| {
            RowImage::new(
                Arc::clone(&image),
                positions.iter().map(|index| values.get(*index).cloned().flatten()).collect(),
            )
        };
        let whole =
            |rows| RowChange { table: table.clone(), column: String::new(), rows: Some(rows) };
        match event.action {
            Action::SQLITE_INSERT => changes.push(whole((None, Some(row(&event.new))))),
            Action::SQLITE_DELETE => changes.push(whole((Some(row(&event.old)), None))),
            _ if key(&event.old) != key(&event.new) => {
                changes.push(whole((Some(row(&event.old)), None)));
                changes.push(whole((None, Some(row(&event.new)))));
            }
            _ => {
                let rows = (Some(row(&event.old)), Some(row(&event.new)));
                for (index, (name, pk, hidden)) in fields.columns.iter().enumerate() {
                    // Sessions omit generated columns; they can change with their inputs.
                    if *pk || (!*hidden && event.old.get(index) == event.new.get(index)) {
                        continue;
                    }
                    changes.push(RowChange {
                        table: table.clone(),
                        column: name.clone(),
                        rows: Some(rows.clone()),
                    });
                }
            }
        }
    }
    Ok(changes)
}
