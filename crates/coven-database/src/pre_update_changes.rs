//! What row operations a pre-update hook reports mean to live queries
//! (§4.1): a local write's on device-local tables, which never sync, and a
//! streamed transaction's on any table, as applying another device's slot.
//! Coven keeps of the rows only what wakes a live query: the table and the
//! columns changed, and the changed rows' keys and the values a live query's
//! scope tests, only where a waiting live query narrows that table.

use super::table_shapes::{TableShape, TableShapes};
use crate::observation::RowChange;
use crate::row_scope::RowImage;
use crate::sql_value::SqlValue;
use crate::DbError;
use rusqlite::hooks::{PreUpdateCase, PreUpdateNewValueAccessor, PreUpdateOldValueAccessor};
use rusqlite::types::ValueRef;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// A whole row's change, beside the column positions an update names.
const ROW: usize = usize::MAX;

/// Past this many row changes to one table, a write keeps that the table
/// changed instead of each row, so its memory stays bounded; every live
/// query that reads the table then runs again.
pub(crate) const ROWS_PER_TABLE: usize = 10_000;

/// The columns a table's row images hold, with their pre-update positions.
pub(crate) struct ImageColumns {
    names: Arc<[String]>,
    positions: Vec<usize>,
}

impl ImageColumns {
    /// The row images of the tables `scoped` names, by position: each one's
    /// key and the scoped columns it has. A table no key can name keeps none.
    pub(crate) fn scoped(
        shapes: &TableShapes,
        scoped: &crate::change_capture::ScopedColumns,
    ) -> HashMap<usize, ImageColumns> {
        shapes
            .iter()
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

    fn before(&self, old: &PreUpdateOldValueAccessor) -> Result<RowImage, DbError> {
        self.image(|index| old_value(old, index))
    }

    fn after(&self, new: &PreUpdateNewValueAccessor) -> Result<RowImage, DbError> {
        self.image(|index| new_value(new, index))
    }

    fn image<'a>(
        &self,
        value: impl Fn(usize) -> Result<Option<ValueRef<'a>>, DbError>,
    ) -> Result<RowImage, DbError> {
        let values = self
            .positions
            .iter()
            .map(|index| Ok(value(*index)?.map(SqlValue::from)))
            .collect::<Result<_, DbError>>()?;
        Ok(RowImage::new(Arc::clone(&self.names), values))
    }
}

/// The value a pre-update read gives: `None` for a virtual generated column,
/// which keeps no value and for which SQLite answers `SQLITE_RANGE`. Any
/// other failure is a failed read, which fails the write, so no capture
/// takes it for a value that isn't there.
pub(crate) fn column_value(
    read: rusqlite::Result<ValueRef<'_>>,
) -> Result<Option<ValueRef<'_>>, DbError> {
    match read {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::SqliteFailure(error, _))
            if error.code == rusqlite::ErrorCode::ParameterOutOfRange =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

/// The row's value in column `index` before the operation.
pub(crate) fn old_value(
    old: &PreUpdateOldValueAccessor,
    index: usize,
) -> Result<Option<ValueRef<'_>>, DbError> {
    column_value(old.get_old_column_value(at(index)))
}

/// The row's value in column `index` after the operation.
pub(crate) fn new_value(
    new: &PreUpdateNewValueAccessor,
    index: usize,
) -> Result<Option<ValueRef<'_>>, DbError> {
    column_value(new.get_new_column_value(at(index)))
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
    /// Tables the transaction's shapes don't hold, lowercase: made since
    /// they were read, as only a migration makes a synced table, so these
    /// are device-local, and change whole.
    unshaped: BTreeSet<String>,
    /// Tables a row was inserted into without an image of it: a read of
    /// insertions alone still learns of it.
    inserted: BTreeSet<usize>,
    /// Row changes kept so far, by table position.
    kept: HashMap<usize, usize>,
}

impl PreUpdateChanges {
    pub(crate) fn new(scoped: HashMap<usize, ImageColumns>) -> Self {
        Self { scoped, ..Self::default() }
    }

    /// Record an operation on a table the transaction's shapes don't hold.
    pub(crate) fn unshaped(&mut self, table: &str) {
        if !self.unshaped.iter().any(|known| known.eq_ignore_ascii_case(table)) {
            self.unshaped.insert(table.to_ascii_lowercase());
        }
    }

    /// Record one row operation on the table at `position`.
    pub(crate) fn record(
        &mut self,
        shapes: &TableShapes,
        position: usize,
        case: &PreUpdateCase,
    ) -> Result<(), DbError> {
        let shape = shapes.shape(position);
        let before = self.rows.len();
        self.record_rows(shape, position, case)?;
        let kept = self.kept.entry(position).or_default();
        *kept += self.rows.len() - before;
        if *kept > ROWS_PER_TABLE && self.scoped.remove(&position).is_some() {
            if self.rows.iter().any(|change| {
                change.table == shape.name
                    && change.rows.as_ref().is_some_and(|(old, _)| old.is_none())
            }) {
                self.inserted.insert(position);
            }
            self.rows.retain(|change| change.table != shape.name);
            self.columns.insert((position, ROW));
        }
        if shape.parent.is_some() {
            self.shadows.insert(position);
        }
        Ok(())
    }

    fn record_rows(
        &mut self,
        shape: &TableShape,
        position: usize,
        case: &PreUpdateCase,
    ) -> Result<(), DbError> {
        let image = self.scoped.get(&position);
        match case {
            PreUpdateCase::Insert(new) => match image {
                Some(image) => self.rows.push(whole(shape, None, Some(image.after(new)?))),
                None => {
                    self.columns.insert((position, ROW));
                    self.inserted.insert(position);
                }
            },
            PreUpdateCase::Delete(old) => match image {
                Some(image) => self.rows.push(whole(shape, Some(image.before(old)?), None)),
                None => {
                    self.columns.insert((position, ROW));
                }
            },
            PreUpdateCase::Update { old_value_accessor: old, new_value_accessor: new } => {
                let changed = |index: usize| -> Result<bool, DbError> {
                    Ok(old_value(old, index)? != new_value(new, index)?)
                };
                let mut key_changed = false;
                for (index, (_, key, _)) in shape.columns.iter().enumerate() {
                    if *key && changed(index)? {
                        key_changed = true;
                        break;
                    }
                }
                if shape.whole || key_changed {
                    match image {
                        Some(image) => {
                            let (before, after) = (image.before(old)?, image.after(new)?);
                            self.rows.push(whole(shape, Some(before), None));
                            self.rows.push(whole(shape, None, Some(after)));
                        }
                        None => {
                            self.columns.insert((position, ROW));
                        }
                    }
                    return Ok(());
                }
                let rows = match image {
                    Some(image) => Some((Some(image.before(old)?), Some(image.after(new)?))),
                    None => None,
                };
                for (index, (name, key, hidden)) in shape.columns.iter().enumerate() {
                    // Sessions omit generated columns; they can change with their inputs.
                    if *key || (!*hidden && !changed(index)?) {
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
        Ok(())
    }

    /// The live-query changes the operations recorded made.
    pub(crate) fn finish(self, shapes: &TableShapes) -> Vec<RowChange> {
        let mut changes = self.rows;
        let parents: BTreeSet<&String> = self
            .shadows
            .iter()
            .filter_map(|position| shapes.shape(*position).parent.as_ref())
            .collect();
        for (position, column) in self.columns {
            let shape = shapes.shape(position);
            changes.push(RowChange {
                table: shape.name.clone(),
                column: if column == ROW { String::new() } else { shape.columns[column].0.clone() },
                rows: None,
            });
        }
        // An insertion with no image: no column of the row is known, so it
        // falls within every scope.
        changes.extend(self.inserted.into_iter().map(|position| {
            let unknown = RowImage::new(Arc::from([]), Vec::new());
            whole(shapes.shape(position), None, Some(unknown))
        }));
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
