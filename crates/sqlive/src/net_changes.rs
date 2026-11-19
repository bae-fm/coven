//! A transaction's net change to each row it changes (§4): the row as it was
//! before the transaction and as it is at its end. A transaction can change
//! one row more than once, as an insert and a delete, an update and an update
//! back, or a delete and an insert of the same key, as `INSERT OR REPLACE`
//! and FTS5's rewrite of an updated row make. The captures report each
//! operation on a row that a key names (`RowOperation`), and `NetRows` folds
//! them into the row's first image before and its last image after, with
//! each column some operation changed and its value before the first change
//! and after the last, where the capture kept them. A row that ends as it
//! began changes nothing, and a column whose kept value ends as it began
//! changed nothing, so a live query judges the net change alone. The Lean
//! model folds a write's changes the same way (`net`, `net_sound`).

use crate::read_set::{RowChange, ValueKey};
use crate::row_scope::RowImage;
use crate::sql_value::SqlValue;
use std::collections::{hash_map::Entry, BTreeMap, HashMap};
use std::sync::Arc;

/// One operation on a row that a key names, as a capture saw it.
pub(crate) struct RowOperation {
    /// The table, lowercase.
    pub(crate) table: String,
    /// The row's key: the values of the table's key columns, in order.
    pub(crate) key: Vec<SqlValue>,
    /// The row before the operation, or `None` where there was none.
    pub(crate) before: Option<RowImage>,
    /// The row after the operation, or `None` where none is left.
    pub(crate) after: Option<RowImage>,
    /// Each stored column outside the key that the operation changed: every
    /// one for an insert or a delete.
    pub(crate) columns: Vec<ColumnChange>,
    /// The table's generated columns, which change with the stored columns
    /// they read.
    pub(crate) generated: Arc<[String]>,
}

/// One column an operation changed, with its value before and after where
/// the capture kept it: the columns live queries read or test.
pub(crate) struct ColumnChange {
    /// The column, lowercase.
    pub(crate) column: String,
    /// Its value before the operation, if kept.
    pub(crate) before: Option<SqlValue>,
    /// Its value after the operation, if kept.
    pub(crate) after: Option<SqlValue>,
}

impl ColumnChange {
    /// `column`'s change from `before` to `after`, with those values when
    /// the capture keeps them and none when it doesn't.
    pub(crate) fn new(
        column: String,
        kept: bool,
        before: Option<SqlValue>,
        after: Option<SqlValue>,
    ) -> Self {
        if kept {
            Self { column, before, after }
        } else {
            Self { column, before: None, after: None }
        }
    }
}

/// One row's operations so far, folded.
struct NetRow {
    table: String,
    key: Vec<SqlValue>,
    /// The row before the transaction's first operation on it.
    before: Option<RowImage>,
    /// The row after the last.
    after: Option<RowImage>,
    /// Each column an operation changed, with its kept value before the
    /// first operation that changed it and after the last.
    columns: BTreeMap<String, (Option<SqlValue>, Option<SqlValue>)>,
    generated: Arc<[String]>,
}

/// Rows a transaction changed, each folded to its net change, in the order
/// the transaction first changed them.
#[derive(Default)]
pub(crate) struct NetRows {
    /// Each row's place in `rows`, by its table and key.
    places: HashMap<(String, Vec<ValueKey>), usize>,
    rows: Vec<NetRow>,
    /// How many rows of each table `rows` holds.
    counts: HashMap<String, usize>,
}

impl NetRows {
    /// Fold `operation` into its row's net change.
    pub(crate) fn record(&mut self, operation: RowOperation) {
        let RowOperation { table, key, before, after, columns, generated } = operation;
        let place = (table.clone(), key.iter().map(ValueKey::of).collect());
        match self.places.entry(place) {
            Entry::Occupied(place) => {
                let row = &mut self.rows[*place.get()];
                row.after = after;
                for change in columns {
                    // A column no earlier operation changed held, before this
                    // one, the value it held before the transaction.
                    row.columns.entry(change.column).or_insert((change.before, None)).1 =
                        change.after;
                }
            }
            Entry::Vacant(place) => {
                place.insert(self.rows.len());
                *self.counts.entry(table.clone()).or_default() += 1;
                let columns = columns
                    .into_iter()
                    .map(|change| (change.column, (change.before, change.after)))
                    .collect();
                self.rows.push(NetRow { table, key, before, after, columns, generated });
            }
        }
    }

    /// How many rows of `table` the transaction changed so far.
    pub(crate) fn count(&self, table: &str) -> usize {
        self.counts.get(table).copied().unwrap_or(0)
    }

    /// Forget every row of `table`, whose changes the caller records whole
    /// instead.
    pub(crate) fn forget(&mut self, table: &str) {
        if self.counts.remove(table).is_none() {
            return;
        }
        self.rows.retain(|row| row.table != table);
        self.places.clear();
        for (place, row) in self.rows.iter().enumerate() {
            self.places
                .insert((row.table.clone(), row.key.iter().map(ValueKey::of).collect()), place);
        }
    }

    /// Each row's net change as one operation, which a later capture's
    /// `NetRows` folds on.
    fn into_operations(self) -> impl Iterator<Item = RowOperation> {
        self.rows.into_iter().map(|row| RowOperation {
            table: row.table,
            key: row.key,
            before: row.before,
            after: row.after,
            columns: row
                .columns
                .into_iter()
                .map(|(column, (before, after))| ColumnChange { column, before, after })
                .collect(),
            generated: row.generated,
        })
    }

    /// The row changes live queries judge: a whole row inserted or deleted,
    /// or each column that changed, for each row that doesn't end as it
    /// began. A column whose value the transaction didn't keep counts as
    /// changed once an operation changed it.
    fn finish(self, into: &mut Vec<RowChange>) {
        for row in self.rows {
            let whole = |rows| RowChange { table: row.table.clone(), column: String::new(), rows };
            match (row.before, row.after) {
                (None, None) => {}
                (before, None) => into.push(whole(Some((before, None)))),
                (None, after) => into.push(whole(Some((None, after)))),
                (Some(before), Some(after)) => {
                    let mut changed = row
                        .columns
                        .into_iter()
                        .filter(|(_, ends)| !matches!(ends, (Some(first), Some(last)) if same(first, last)))
                        .map(|(column, _)| column)
                        .peekable();
                    if changed.peek().is_none() {
                        continue;
                    }
                    let rows = (Some(before), Some(after));
                    for column in changed.chain(row.generated.iter().cloned()) {
                        into.push(RowChange {
                            table: row.table.clone(),
                            column,
                            rows: Some(rows.clone()),
                        });
                    }
                }
            }
        }
    }
}

/// What a transaction's captures recorded for live queries: the net change
/// of each row a key names, and the changes no key names, as a table's that
/// has no key or that changed past the bound on rows (`ROWS_PER_TABLE`).
#[derive(Default)]
pub(crate) struct Captured {
    rows: NetRows,
    changes: Vec<RowChange>,
}

impl Captured {
    /// Changes no key names.
    pub(crate) fn unkeyed(changes: Vec<RowChange>) -> Self {
        Self { rows: NetRows::default(), changes }
    }

    /// Keyed rows' net changes, and changes no key names.
    pub(crate) fn new(rows: NetRows, changes: Vec<RowChange>) -> Self {
        Self { rows, changes }
    }

    /// Fold `operation` into its row's net change.
    pub(crate) fn record(&mut self, operation: RowOperation) {
        self.rows.record(operation);
    }

    /// Add `change`, which no key names.
    pub(crate) fn push(&mut self, change: RowChange) {
        self.changes.push(change);
    }

    /// Add what a capture recorded after these: each of its rows' net change
    /// folds onto the same row's here.
    pub(crate) fn append(&mut self, later: Self) {
        for operation in later.rows.into_operations() {
            self.rows.record(operation);
        }
        self.changes.extend(later.changes);
    }

    /// The row changes live queries judge.
    pub(crate) fn finish(self) -> Vec<RowChange> {
        let mut changes = Vec::new();
        self.rows.finish(&mut changes);
        changes.extend(self.changes);
        changes
    }
}

/// Whether two values are the same value: of one type, with the same bytes.
/// An integer and a real that compare equal are not, since a query can tell
/// them apart.
fn same(left: &SqlValue, right: &SqlValue) -> bool {
    match (left, right) {
        (SqlValue::Null, SqlValue::Null) => true,
        (SqlValue::Integer(left), SqlValue::Integer(right)) => left == right,
        (SqlValue::Real(left), SqlValue::Real(right)) => left.to_bits() == right.to_bits(),
        (SqlValue::Text(left), SqlValue::Text(right))
        | (SqlValue::Blob(left), SqlValue::Blob(right)) => left == right,
        _ => false,
    }
}

#[cfg(test)]
#[path = "net_changes_tests.rs"]
mod tests;
