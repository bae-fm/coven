//! What row operations a pre-update hook reports mean to live queries
//! (§4.1): a local write's on device-local tables, which never sync, and a
//! streamed transaction's on any table, as applying another device's slot.
//! Coven keeps of the rows only what wakes a live query: the table and the
//! columns changed, and the changed rows' keys and the values a live query's
//! scope tests, only where a waiting live query narrows that table.

use super::DatabaseConnection;
use crate::observation::RowChange;
use crate::row_scope::RowImage;
use crate::sql_value::SqlValue;
use crate::DbError;
use rusqlite::hooks::{PreUpdateCase, PreUpdateNewValueAccessor, PreUpdateOldValueAccessor};
use rusqlite::types::ValueRef;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// A whole row's change, beside the column positions an update names.
const ROW: usize = usize::MAX;

/// Past this many row changes to one table, a write keeps that the table
/// changed instead of each row, so its memory stays bounded; every live
/// query that reads the table then runs again.
pub(crate) const ROWS_PER_TABLE: usize = 10_000;

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
        let synced: BTreeSet<String> =
            synced.iter().map(|table| table.name.to_ascii_lowercase()).collect();
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

    /// The row images of the tables `scoped` names, by position: each one's
    /// key and the scoped columns it has. A table no key can name keeps none.
    pub(crate) fn scoped(
        &self,
        scoped: &crate::change_capture::ScopedColumns,
    ) -> HashMap<usize, ImageColumns> {
        self.shapes
            .iter()
            .enumerate()
            .filter(|(_, shape)| !shape.whole)
            .filter_map(|(position, shape)| {
                let columns = scoped.get(&shape.name)?;
                let positions: Vec<usize> = (0..shape.columns.len())
                    .filter(|index| shape.columns[*index].1)
                    .chain((0..shape.columns.len()).filter(|index| {
                        let (name, key, hidden) = &shape.columns[*index];
                        !key && !hidden && columns.contains(name)
                    }))
                    .collect();
                let names = positions.iter().map(|index| shape.columns[*index].0.clone()).collect();
                Some((position, ImageColumns { names, positions }))
            })
            .collect()
    }
}

/// The columns a table's row images hold, with their pre-update positions.
pub(crate) struct ImageColumns {
    names: Arc<[String]>,
    positions: Vec<usize>,
}

impl ImageColumns {
    fn before(&self, old: &PreUpdateOldValueAccessor) -> RowImage {
        self.image(|index| old.get_old_column_value(at(index)).ok())
    }

    fn after(&self, new: &PreUpdateNewValueAccessor) -> RowImage {
        self.image(|index| new.get_new_column_value(at(index)).ok())
    }

    fn image<'a>(&self, value: impl Fn(usize) -> Option<ValueRef<'a>>) -> RowImage {
        RowImage::new(
            Arc::clone(&self.names),
            self.positions.iter().map(|index| value(*index).map(SqlValue::from)).collect(),
        )
    }
}

/// One transaction's row changes, as its pre-update hook reports them.
#[derive(Default)]
pub(crate) struct PreUpdateChanges {
    /// Tables a waiting live query narrows, and their images' columns.
    scoped: HashMap<usize, ImageColumns>,
    /// A table and a column changed, or `ROW` for a whole row.
    columns: BTreeSet<(usize, usize)>,
    /// Changes with their rows' images, for the tables in `scoped`.
    rows: Vec<RowChange>,
    /// Shadow tables changed: each change is its virtual table's too, in
    /// every column.
    shadows: BTreeSet<usize>,
    /// Tables made after the writer read the shapes, lowercase. Every synced
    /// table was there by then, so these are device-local, and change whole.
    unshaped: BTreeSet<String>,
    /// Row changes kept so far, by table position.
    kept: HashMap<usize, usize>,
}

impl PreUpdateChanges {
    pub(crate) fn new(scoped: HashMap<usize, ImageColumns>) -> Self {
        Self { scoped, ..Self::default() }
    }

    /// Record an operation on a table made after the shapes were read.
    pub(crate) fn unshaped(&mut self, table: &str) {
        if !self.unshaped.iter().any(|known| known.eq_ignore_ascii_case(table)) {
            self.unshaped.insert(table.to_ascii_lowercase());
        }
    }

    /// Record one row operation on the table at `position`.
    pub(crate) fn record(&mut self, shapes: &TableShapes, position: usize, case: &PreUpdateCase) {
        let shape = &shapes.shapes[position];
        let before = self.rows.len();
        self.record_rows(shape, position, case);
        let kept = self.kept.entry(position).or_default();
        *kept += self.rows.len() - before;
        if *kept > ROWS_PER_TABLE && self.scoped.remove(&position).is_some() {
            self.rows.retain(|change| change.table != shape.name);
            self.columns.insert((position, ROW));
        }
        if shape.parent.is_some() {
            self.shadows.insert(position);
        }
    }

    fn record_rows(&mut self, shape: &TableShape, position: usize, case: &PreUpdateCase) {
        let image = self.scoped.get(&position);
        match case {
            PreUpdateCase::Insert(new) => match image {
                Some(image) => self.rows.push(whole(shape, None, Some(image.after(new)))),
                None => {
                    self.columns.insert((position, ROW));
                }
            },
            PreUpdateCase::Delete(old) => match image {
                Some(image) => self.rows.push(whole(shape, Some(image.before(old)), None)),
                None => {
                    self.columns.insert((position, ROW));
                }
            },
            PreUpdateCase::Update { old_value_accessor: old, new_value_accessor: new } => {
                let value = |index: usize| {
                    (
                        old.get_old_column_value(at(index)).ok(),
                        new.get_new_column_value(at(index)).ok(),
                    )
                };
                let key_changed = shape.columns.iter().enumerate().any(|(index, (_, key, _))| {
                    *key && {
                        let (before, after) = value(index);
                        before != after
                    }
                });
                if shape.whole || key_changed {
                    match image {
                        Some(image) => {
                            let (before, after) = (image.before(old), image.after(new));
                            self.rows.push(whole(shape, Some(before), None));
                            self.rows.push(whole(shape, None, Some(after)));
                        }
                        None => {
                            self.columns.insert((position, ROW));
                        }
                    }
                    return;
                }
                let rows = image.map(|image| (Some(image.before(old)), Some(image.after(new))));
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
                    match &rows {
                        Some(rows) => self.rows.push(RowChange {
                            table: shape.name.clone(),
                            column: name.clone(),
                            rows: Some(rows.clone()),
                        }),
                        None => {
                            self.columns.insert((position, index));
                        }
                    }
                }
            }
            PreUpdateCase::Unknown => {}
        }
    }

    /// The live-query changes the operations recorded made.
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
                column: if column == ROW { String::new() } else { shape.columns[column].0.clone() },
                rows: None,
            });
        }
        changes.extend(parents.into_iter().cloned().chain(self.unshaped).map(|table| RowChange {
            table,
            column: String::new(),
            rows: None,
        }));
        changes
    }
}

fn whole(shape: &TableShape, old: Option<RowImage>, new: Option<RowImage>) -> RowChange {
    RowChange { table: shape.name.clone(), column: String::new(), rows: Some((old, new)) }
}

/// A column's position as SQLite's pre-update accessors take it.
fn at(index: usize) -> i32 {
    i32::try_from(index).expect("SQLite's column positions fit an int")
}

#[cfg(test)]
#[path = "pre_update_changes_tests.rs"]
mod tests;
