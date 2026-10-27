//! What a local write's row operations on device-local tables mean to live
//! queries (§4.1). A device-local table never syncs, so coven keeps of its
//! rows only what wakes a live query: the table and the columns changed, and
//! the changed rows' keys only where a live query reads that table by key.

use super::DatabaseConnection;
use crate::observation::RowChange;
use crate::sql_value::SqlValue;
use crate::DbError;
use rusqlite::hooks::{PreUpdateCase, PreUpdateNewValueAccessor, PreUpdateOldValueAccessor};
use rusqlite::types::ValueRef;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A whole row's change, beside the column positions an update names.
const ROW: usize = usize::MAX;

/// One table of the store's database as a local write's capture reads it.
struct TableShape {
    /// Lowercase, as live queries record what they read.
    name: String,
    synced: bool,
    /// Each column at its pre-update position: lowercase name, whether it is
    /// part of the key, and whether it is hidden or generated.
    columns: Vec<(String, bool, bool)>,
    /// No key can name its rows' changes, as for change capture's tables
    /// without a primary key, or with one that can hold NULL.
    whole: bool,
    /// The virtual table a shadow table belongs to.
    parent: Option<String>,
}

/// Every table of the store's database, read once as the writer starts
/// serving: no write creates or changes one.
pub(crate) struct TableShapes {
    positions: HashMap<String, usize>,
    shapes: Vec<TableShape>,
}

impl TableShapes {
    pub(crate) fn read(
        database: &DatabaseConnection,
        synced: &[crate::SyncedTable],
        supplemental: &BTreeMap<String, Option<String>>,
    ) -> Result<Self, DbError> {
        let synced: BTreeSet<String> = synced
            .iter()
            .map(|table| table.name.to_ascii_lowercase())
            .collect();
        let names = database.query(
            "SELECT name FROM pragma_table_list WHERE schema='main' AND type IN ('table','shadow') AND name NOT GLOB 'sqlite_*'",
            [],
            |r| r.get::<_, String>(0),
        )?;
        let mut positions = HashMap::new();
        let mut shapes = Vec::new();
        for table in names {
            let name = table.to_ascii_lowercase();
            let columns = database.query(
                "SELECT name,pk,hidden FROM pragma_table_xinfo(?1,'main') ORDER BY cid",
                [&table],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?.to_ascii_lowercase(),
                        r.get::<_, bool>(1)?,
                        r.get::<_, u32>(2)? != 0,
                    ))
                },
            )?;
            let keyed = columns.iter().any(|(_, key, _)| *key);
            positions.insert(table.clone(), shapes.len());
            shapes.push(TableShape {
                synced: synced.contains(&name),
                whole: !keyed || supplemental.contains_key(&name),
                parent: crate::change_capture::shadow_parent(database, &name)?,
                name,
                columns,
            });
        }
        Ok(Self { positions, shapes })
    }

    /// The position of `table`, as SQLite names it to a pre-update hook.
    pub(crate) fn position(&self, table: &str) -> Option<usize> {
        self.positions.get(table).copied()
    }

    pub(crate) fn synced(&self, position: usize) -> bool {
        self.shapes[position].synced
    }

    /// The positions of the device-local tables named `tables`, lowercase.
    pub(crate) fn local(&self, tables: &BTreeSet<String>) -> BTreeSet<usize> {
        self.shapes
            .iter()
            .enumerate()
            .filter(|(_, shape)| !shape.synced && tables.contains(&shape.name))
            .map(|(position, _)| position)
            .collect()
    }
}

/// One write's changes to device-local tables, as its pre-update hook
/// reports them.
#[derive(Default)]
pub(crate) struct LocalChanges {
    /// Tables whose changes a live query reads by key.
    keyed: BTreeSet<usize>,
    /// A table and a column changed, or `ROW` for a whole row.
    columns: BTreeSet<(usize, usize)>,
    /// Changes with their rows' keys, for the tables in `keyed`.
    rows: Vec<RowChange>,
    /// Shadow tables changed: each change is its virtual table's too, in
    /// every column.
    shadows: BTreeSet<usize>,
    /// Tables made after the writer read the shapes, lowercase. Every synced
    /// table was there by then, so these are device-local, and change whole.
    unshaped: BTreeSet<String>,
}

impl LocalChanges {
    pub(crate) fn new(keyed: BTreeSet<usize>) -> Self {
        Self {
            keyed,
            ..Self::default()
        }
    }

    /// Record an operation on a table made after the shapes were read.
    pub(crate) fn unshaped(&mut self, table: &str) {
        if !self
            .unshaped
            .iter()
            .any(|known| known.eq_ignore_ascii_case(table))
        {
            self.unshaped.insert(table.to_ascii_lowercase());
        }
    }

    /// Record one row operation on the device-local table at `position`.
    pub(crate) fn record(&mut self, shapes: &TableShapes, position: usize, case: &PreUpdateCase) {
        let shape = &shapes.shapes[position];
        let keyed = !shape.whole && self.keyed.contains(&position);
        match case {
            PreUpdateCase::Insert(new) => {
                if keyed {
                    self.rows
                        .push(whole(shape, absent(shape), new_key(shape, new)));
                } else {
                    self.columns.insert((position, ROW));
                }
            }
            PreUpdateCase::Delete(old) => {
                if keyed {
                    self.rows
                        .push(whole(shape, old_key(shape, old), absent(shape)));
                } else {
                    self.columns.insert((position, ROW));
                }
            }
            PreUpdateCase::Update {
                old_value_accessor: old,
                new_value_accessor: new,
            } => {
                let value = |index: usize| {
                    (
                        old.get_old_column_value(at(index)).ok(),
                        new.get_new_column_value(at(index)).ok(),
                    )
                };
                let key_changed = shape
                    .columns
                    .iter()
                    .enumerate()
                    .any(|(index, (_, key, _))| {
                        *key && {
                            let (before, after) = value(index);
                            before != after
                        }
                    });
                if shape.whole || key_changed {
                    if keyed {
                        let (before, after) = (old_key(shape, old), new_key(shape, new));
                        self.rows.push(whole(shape, before, absent(shape)));
                        self.rows.push(whole(shape, absent(shape), after));
                    } else {
                        self.columns.insert((position, ROW));
                    }
                    return;
                }
                let keys = keyed.then(|| (old_key(shape, old), new_key(shape, new)));
                for (index, (name, key, hidden)) in shape.columns.iter().enumerate() {
                    // Sessions omit generated columns; they can change with their inputs.
                    if *key
                        || (!*hidden && {
                            let (before, after) = value(index);
                            before == after
                        })
                    {
                        continue;
                    }
                    match &keys {
                        Some(keys) => self.rows.push(RowChange {
                            table: shape.name.clone(),
                            column: name.clone(),
                            keys: Some(keys.clone()),
                        }),
                        None => {
                            self.columns.insert((position, index));
                        }
                    }
                }
            }
            PreUpdateCase::Unknown => {}
        }
        if shape.parent.is_some() {
            self.shadows.insert(position);
        }
    }

    /// The live-query changes the write's device-local operations made.
    pub(crate) fn finish(self, shapes: &TableShapes) -> Vec<RowChange> {
        let mut changes = self.rows;
        let parents: BTreeSet<&String> = self
            .shadows
            .iter()
            .filter_map(|position| shapes.shapes[*position].parent.as_ref())
            .collect();
        for (position, column) in self.columns {
            let shape = &shapes.shapes[position];
            changes.push(RowChange {
                table: shape.name.clone(),
                column: if column == ROW {
                    String::new()
                } else {
                    shape.columns[column].0.clone()
                },
                keys: None,
            });
        }
        changes.extend(
            parents
                .into_iter()
                .cloned()
                .chain(self.unshaped)
                .map(|table| RowChange {
                    table,
                    column: String::new(),
                    keys: None,
                }),
        );
        changes
    }
}

type Key = BTreeMap<String, SqlValue>;

fn whole(shape: &TableShape, old: Key, new: Key) -> RowChange {
    RowChange {
        table: shape.name.clone(),
        column: String::new(),
        keys: Some((old, new)),
    }
}

/// The key of a row that isn't there: every key column NULL.
fn absent(shape: &TableShape) -> Key {
    key(shape, |_| None)
}

fn old_key(shape: &TableShape, old: &PreUpdateOldValueAccessor) -> Key {
    key(shape, |index| old.get_old_column_value(at(index)).ok())
}

fn new_key(shape: &TableShape, new: &PreUpdateNewValueAccessor) -> Key {
    key(shape, |index| new.get_new_column_value(at(index)).ok())
}

fn key<'a>(shape: &TableShape, value: impl Fn(usize) -> Option<ValueRef<'a>>) -> Key {
    shape
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (_, key, _))| *key)
        .map(|(index, (name, _, _))| {
            (
                name.clone(),
                value(index).map_or(SqlValue::Null, SqlValue::from),
            )
        })
        .collect()
}

/// A column's position as SQLite's pre-update accessors take it.
fn at(index: usize) -> i32 {
    i32::try_from(index).expect("SQLite's column positions fit an int")
}

#[cfg(test)]
#[path = "local_changes_tests.rs"]
mod tests;
