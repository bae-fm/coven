//! What row operations a pre-update hook reports mean to live queries
//! (§4.1): a local write's on device-local tables, which never sync, and a
//! streamed transaction's on any table, as applying another device's slot.
//! sqlive keeps of the rows only what wakes a live query: the table and the
//! columns changed, and, where a waiting live query reads the table, each
//! changed row's key and the values live queries read or test, folded into
//! the row's net change over the transaction (`net_changes.rs`).

use super::table_shapes::{Shadow, TableShape, TableShapes};
use crate::authorization::HostError;
use crate::net_changes::{Captured, ColumnChange, NetRows, RowOperation};
use crate::{RowChange, RowImage, SqlValue, ROWS_PER_TABLE};
use rusqlite::hooks::{PreUpdateCase, PreUpdateNewValueAccessor, PreUpdateOldValueAccessor};
use rusqlite::types::ValueRef;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// A whole row's change, beside the column positions an update names.
const ROW: usize = usize::MAX;

/// The columns a table's row images hold, with their pre-update positions.
pub(crate) struct ImageColumns {
    names: Arc<[String]>,
    positions: Vec<usize>,
    /// The table's generated columns.
    generated: Arc<[String]>,
}

impl ImageColumns {
    /// The row images of the tables `kept` names, by position: each one's
    /// key and the kept columns it has. A table no key can name keeps none.
    pub(crate) fn kept(
        shapes: &TableShapes,
        kept: &crate::change_capture::KeptColumns,
    ) -> HashMap<usize, ImageColumns> {
        shapes
            .iter()
            .filter(|(_, shape)| !shape.whole)
            .filter_map(|(position, shape)| {
                let columns = kept.get(&shape.name)?;
                let positions: Vec<usize> = (0..shape.columns.len())
                    .filter(|index| shape.columns[*index].1)
                    .chain((0..shape.columns.len()).filter(|index| {
                        let (name, key, hidden) = &shape.columns[*index];
                        !key && !hidden && columns.contains(name)
                    }))
                    .collect();
                let names = positions.iter().map(|index| shape.columns[*index].0.clone()).collect();
                Some((position, ImageColumns { names, positions, generated: shape.generated() }))
            })
            .collect()
    }

    fn before<F: HostError, R: HostError>(
        &self,
        old: &PreUpdateOldValueAccessor,
    ) -> Result<RowImage, crate::Error<F, R>> {
        self.image(|index| old_value(old, index))
    }

    fn after<F: HostError, R: HostError>(
        &self,
        new: &PreUpdateNewValueAccessor,
    ) -> Result<RowImage, crate::Error<F, R>> {
        self.image(|index| new_value(new, index))
    }

    fn image<'a, F: HostError, R: HostError>(
        &self,
        value: impl Fn(usize) -> Result<Option<ValueRef<'a>>, crate::Error<F, R>>,
    ) -> Result<RowImage, crate::Error<F, R>> {
        let values = self
            .positions
            .iter()
            .map(|index| Ok(value(*index)?.map(SqlValue::from)))
            .collect::<Result<_, crate::Error<F, R>>>()?;
        Ok(RowImage::new(Arc::clone(&self.names), values))
    }

    /// One operation on a row of `shape`, whose images these are: an insert
    /// with only `new`, a delete with only `old`, or an update of the row's
    /// columns outside its key with both. It changes each stored column an
    /// insert or a delete changes, or whose value an update changes, and
    /// keeps the values of the columns the images hold.
    fn operation<F: HostError, R: HostError>(
        &self,
        shape: &TableShape,
        old: Option<&PreUpdateOldValueAccessor>,
        new: Option<&PreUpdateNewValueAccessor>,
    ) -> Result<RowOperation, crate::Error<F, R>> {
        let value = |index: usize| -> Result<_, crate::Error<F, R>> {
            Ok((
                old.map(|old| old_value(old, index)).transpose()?.flatten(),
                new.map(|new| new_value(new, index)).transpose()?.flatten(),
            ))
        };
        let mut key = Vec::new();
        let mut columns = Vec::new();
        for (index, (name, is_key, hidden)) in shape.columns.iter().enumerate() {
            if *hidden {
                continue;
            }
            let (before, after) = value(index)?;
            if *is_key {
                key.push(before.or(after).map_or(SqlValue::Null, SqlValue::from));
                continue;
            }
            if old.is_some() && new.is_some() && before == after {
                continue;
            }
            // Only a kept column's values are copied.
            let kept = self.positions.contains(&index);
            let copy = |value: Option<ValueRef<'_>>| value.filter(|_| kept).map(SqlValue::from);
            columns.push(ColumnChange::new(name.clone(), kept, copy(before), copy(after)));
        }
        Ok(RowOperation {
            table: shape.name.clone(),
            key,
            before: old.map(|old| self.before(old)).transpose()?,
            after: new.map(|new| self.after(new)).transpose()?,
            columns,
            generated: Arc::clone(&self.generated),
        })
    }
}

/// The value a pre-update read gives: `None` for a virtual generated column,
/// which keeps no value and for which SQLite answers `SQLITE_RANGE`. Any
/// other failure is a failed read, which fails the write, so no capture
/// takes it for a value that isn't there.
pub(crate) fn column_value<F: HostError, R: HostError>(
    read: rusqlite::Result<ValueRef<'_>>,
) -> Result<Option<ValueRef<'_>>, crate::Error<F, R>> {
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
pub(crate) fn old_value<F: HostError, R: HostError>(
    old: &PreUpdateOldValueAccessor,
    index: usize,
) -> Result<Option<ValueRef<'_>>, crate::Error<F, R>> {
    column_value(old.get_old_column_value(at(index)))
}

/// The row's value in column `index` after the operation.
pub(crate) fn new_value<F: HostError, R: HostError>(
    new: &PreUpdateNewValueAccessor,
    index: usize,
) -> Result<Option<ValueRef<'_>>, crate::Error<F, R>> {
    column_value(new.get_new_column_value(at(index)))
}

/// One transaction's row changes, as its pre-update hook reports them.
#[derive(Default)]
pub(crate) struct PreUpdateChanges {
    /// Tables a waiting live query reads, and their images' columns: the
    /// capture names their changed rows by key and folds each row's
    /// operations into its net change, up to `ROWS_PER_TABLE` rows a table.
    kept: HashMap<usize, ImageColumns>,
    /// A table and a column changed, or `ROW` for a whole row, for the
    /// tables whose rows the capture doesn't keep.
    columns: BTreeSet<(usize, usize)>,
    /// The net change of each row of the tables in `kept`.
    rows: NetRows,
    /// Shadow tables changed whose every change is their virtual table's
    /// too, in every column (`Shadow::Whole`).
    shadows: BTreeSet<usize>,
    /// Tables the transaction's shapes don't hold, lowercase. These tables
    /// appeared after the transaction read its shapes, and only a migration
    /// makes a table whose changes the host records, so these tables are
    /// device-local, and sqlive counts any change to them as a change to the
    /// whole table.
    unshaped: BTreeSet<String>,
    /// Tables a row was inserted into without an image of it: a read of
    /// insertions alone still learns of it.
    inserted: BTreeSet<usize>,
    /// Tables in `kept` a row was inserted into, which join `inserted` if
    /// the capture stops keeping their rows.
    inserts: BTreeSet<usize>,
}

impl PreUpdateChanges {
    pub(crate) fn new(kept: HashMap<usize, ImageColumns>) -> Self {
        Self { kept, ..Self::default() }
    }

    /// Record an operation on a table the transaction's shapes don't hold.
    pub(crate) fn unshaped(&mut self, table: &str) {
        if !self.unshaped.iter().any(|known| known.eq_ignore_ascii_case(table)) {
            self.unshaped.insert(table.to_ascii_lowercase());
        }
    }

    /// Record one row operation on the table at `position`.
    pub(crate) fn record<F: HostError, R: HostError>(
        &mut self,
        shapes: &TableShapes,
        position: usize,
        case: &PreUpdateCase,
    ) -> Result<(), crate::Error<F, R>> {
        let shape = shapes.shape(position);
        self.record_rows(shape, position, case)?;
        // Past the bound, the table's changes count whole, so the
        // transaction's memory stays bounded.
        if self.kept.contains_key(&position) && self.rows.count(&shape.name) > ROWS_PER_TABLE {
            self.kept.remove(&position);
            self.rows.forget(&shape.name);
            if self.inserts.contains(&position) {
                self.inserted.insert(position);
            }
            self.columns.insert((position, ROW));
        }
        match shape.shadow {
            Some(Shadow::Whole(_)) => {
                self.shadows.insert(position);
            }
            // The same operation on the virtual table's row, whose columns
            // sit where the content table's do.
            Some(Shadow::Rows(rows)) => self.record(shapes, rows, case)?,
            Some(Shadow::Index) | None => {}
        }
        Ok(())
    }

    fn record_rows<F: HostError, R: HostError>(
        &mut self,
        shape: &TableShape,
        position: usize,
        case: &PreUpdateCase,
    ) -> Result<(), crate::Error<F, R>> {
        let Some(image) = self.kept.get(&position) else {
            return self.record_columns(shape, position, case);
        };
        match case {
            PreUpdateCase::Insert(new) => {
                self.inserts.insert(position);
                self.rows.record(image.operation(shape, None, Some(new))?);
            }
            PreUpdateCase::Delete(old) => {
                self.rows.record(image.operation(shape, Some(old), None)?);
            }
            PreUpdateCase::Update { old_value_accessor: old, new_value_accessor: new } => {
                if key_changed(shape, old, new)? {
                    self.rows.record(image.operation(shape, Some(old), None)?);
                    self.inserts.insert(position);
                    self.rows.record(image.operation(shape, None, Some(new))?);
                } else {
                    self.rows.record(image.operation(shape, Some(old), Some(new))?);
                }
            }
            PreUpdateCase::Unknown => {}
        }
        Ok(())
    }

    /// Record an operation on a table whose rows the capture doesn't keep:
    /// the columns it changed, or the whole row.
    fn record_columns<F: HostError, R: HostError>(
        &mut self,
        shape: &TableShape,
        position: usize,
        case: &PreUpdateCase,
    ) -> Result<(), crate::Error<F, R>> {
        match case {
            PreUpdateCase::Insert(_) => {
                self.columns.insert((position, ROW));
                self.inserted.insert(position);
            }
            PreUpdateCase::Delete(_) => {
                self.columns.insert((position, ROW));
            }
            PreUpdateCase::Update { old_value_accessor: old, new_value_accessor: new } => {
                if shape.whole || key_changed(shape, old, new)? {
                    self.columns.insert((position, ROW));
                    return Ok(());
                }
                for (index, (_, key, hidden)) in shape.columns.iter().enumerate() {
                    // Sessions omit generated columns; they can change with their inputs.
                    if *key || (!*hidden && old_value(old, index)? == new_value(new, index)?) {
                        continue;
                    }
                    self.columns.insert((position, index));
                }
            }
            PreUpdateCase::Unknown => {}
        }
        Ok(())
    }

    /// The live-query changes the operations recorded made.
    pub(crate) fn finish(self, shapes: &TableShapes) -> Captured {
        let mut changes = Vec::new();
        let parents: BTreeSet<String> = self
            .shadows
            .iter()
            .filter_map(|position| shapes.shape(*position).parent(shapes))
            .map(str::to_owned)
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
        changes.extend(parents.into_iter().chain(self.unshaped).map(|table| RowChange {
            table,
            column: String::new(),
            rows: None,
        }));
        Captured::new(self.rows, changes)
    }
}

/// Whether an update changes the row's key, which makes it a delete of the
/// old key's row and an insert of the new key's.
fn key_changed<F: HostError, R: HostError>(
    shape: &TableShape,
    old: &PreUpdateOldValueAccessor,
    new: &PreUpdateNewValueAccessor,
) -> Result<bool, crate::Error<F, R>> {
    for (index, (_, key, _)) in shape.columns.iter().enumerate() {
        if *key && old_value(old, index)? != new_value(new, index)? {
            return Ok(true);
        }
    }
    Ok(false)
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
