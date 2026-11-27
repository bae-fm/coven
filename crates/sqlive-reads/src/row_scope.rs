//! Which rows of a table a live query depends on, and the values of a changed
//! row that decide whether it is one of them (SPEC.md section 5). A join's
//! values come from a lookup, and a condition on the row's own columns from a
//! question to SQLite (`scope_lookup.rs`, `scope_row.rs`).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::scope_lookup::{GroupLookup, GroupQuestion, Judging, Lookup, Lookups, NoAnswer};
use crate::scope_row::RowCondition;
use crate::scope_window::{RowWindow, WindowQuestion};
use crate::sql_value::SqlValue as Value;

/// A predicate over a table's rows. A change reruns a query when its row
/// passes in its old or its new image.
#[derive(Clone, Debug)]
pub enum RowScope {
    /// Every row.
    All,
    /// Rows whose `column` compares with `value` as `ordering`, or equal too
    /// when `inclusive`.
    Compare {
        /// The tested column, lowercase.
        column: String,
        /// The value the scope compares the column with.
        value: Value,
        /// The order the column's value has to `value`.
        ordering: Ordering,
        /// Whether a value equal to `value` also passes.
        inclusive: bool,
    },
    /// Rows whose `column` is NULL, or is not NULL when `not`.
    Null {
        /// The tested column, lowercase.
        column: String,
        /// Whether rows whose column is not NULL pass instead.
        not: bool,
    },
    /// Rows whose `column` equals one of `values`, sorted in SQLite's order
    /// with NULLs and repeats left out: made by [`RowScope::equal`].
    In {
        /// The tested column, lowercase.
        column: String,
        /// The values that pass, in SQLite's order.
        values: Vec<Value>,
    },
    /// Rows a condition on their columns holds for, which a write judges by
    /// asking SQLite (`scope_row.rs`).
    Row(Arc<RowCondition>),
    /// Rows whose `column` equals a value some row of another source holds
    /// in its own scope: a join, which a write judges by running `lookup`.
    Joined {
        /// The joined column, lowercase.
        column: String,
        /// The question that finds partner rows holding a value.
        lookup: Arc<Lookup>,
    },
    /// Rows whose columns equal the values of one row of another source in
    /// its own scope: the equalities of one join, which a write judges by
    /// running `group`.
    JoinedAll(Arc<GroupLookup>),
    /// Rows that fewer than a subquery's LIMIT rows of their window sort
    /// before, which a write judges by asking SQLite (`scope_window.rs`).
    Window(Arc<RowWindow>),
    /// Rows both scopes pass.
    And(Box<Self>, Box<Self>),
    /// Rows either scope passes.
    Or(Box<Self>, Box<Self>),
}

impl RowScope {
    /// Whether `row` passes, running `lookups` for a join's values. A
    /// column the image doesn't hold passes: the write that made it didn't
    /// know this scope would test it. A question that stops ends the test
    /// with no answer.
    pub(crate) fn matches(
        &self,
        row: &RowImage,
        lookups: &mut Lookups<'_>,
    ) -> Result<bool, NoAnswer> {
        Ok(match self {
            Self::All => true,
            Self::Compare { column, value, ordering, inclusive } => {
                row.get(column).is_none_or(|actual| {
                    compare(actual, value)
                        .is_some_and(|found| found == *ordering || (*inclusive && found.is_eq()))
                })
            }
            Self::Null { column, not } => {
                row.get(column).is_none_or(|actual| matches!(actual, Value::Null) != *not)
            }
            Self::In { column, values } => row.get(column).is_none_or(|actual| {
                !matches!(actual, Value::Null)
                    && values
                        .binary_search_by(|value| {
                            compare(value, actual).expect("neither value is NULL")
                        })
                        .is_ok()
            }),
            Self::Joined { column, lookup } => match row.get(column) {
                None => true,
                // NULL equals nothing, so no join matches it, except one on
                // `IS`.
                Some(Value::Null) if !lookup.nulls() => false,
                Some(value) => lookups(Judging::Partner(lookup, value))?,
            },
            Self::JoinedAll(group) => match group.question(row) {
                GroupQuestion::Answered(passes) => passes,
                GroupQuestion::Ask(sql, parameters) => lookups(Judging::Row(&sql, &parameters))?,
            },
            // A value the question can't give its column's type, or one the
            // write didn't record, passes.
            Self::Row(condition) => match condition.question(row) {
                None => true,
                Some((sql, parameters)) => lookups(Judging::Row(&sql, &parameters))?,
            },
            Self::Window(window) => match window.question(row) {
                WindowQuestion::Answered(passes) => passes,
                WindowQuestion::Ask(sql, parameters) => lookups(Judging::Row(&sql, &parameters))?,
            },
            Self::And(left, right) => left.matches(row, lookups)? && right.matches(row, lookups)?,
            Self::Or(left, right) => left.matches(row, lookups)? || right.matches(row, lookups)?,
        })
    }

    /// Rows whose `column` equals one of `values`.
    pub fn equal(column: String, mut values: Vec<Value>) -> Self {
        values.retain(|value| !matches!(value, Value::Null));
        values.sort_by(|left, right| compare(left, right).expect("NULLs were left out"));
        values.dedup_by(|left, right| compare(left, right).is_some_and(Ordering::is_eq));
        Self::In { column, values }
    }

    /// The values every row in this scope holds in `column`, when the scope
    /// says: an equality or IN list on it, alone or among conditions it
    /// joins with AND.
    pub(crate) fn equal_values(&self, column: &str) -> Option<Vec<Value>> {
        match self {
            Self::In { column: tested, values } if tested == column => Some(values.clone()),
            Self::Compare {
                column: tested,
                value,
                ordering: Ordering::Equal,
                inclusive: false,
            } if tested == column => Some(vec![value.clone()]),
            Self::And(left, right) => {
                left.equal_values(column).or_else(|| right.equal_values(column))
            }
            Self::Or(left, right) => {
                let mut values = left.equal_values(column)?;
                values.extend(right.equal_values(column)?);
                Some(values)
            }
            _ => None,
        }
    }

    /// A column in which every row in this scope holds one of a few values,
    /// with those values: the first column its equalities and IN lists test
    /// for which `equal_values` gives them. A handle files the scope's read
    /// under them, so a change to a row that holds none of them is never
    /// judged against the read (`read_index.rs`).
    pub fn key(&self) -> Option<(&str, Vec<Value>)> {
        let mut columns = Vec::new();
        self.equality_columns(&mut columns);
        columns.into_iter().find_map(|column| Some((column, self.equal_values(column)?)))
    }

    /// The columns this scope's equalities and IN lists test, in order.
    fn equality_columns<'a>(&'a self, into: &mut Vec<&'a str>) {
        match self {
            Self::In { column, .. }
            | Self::Compare { column, ordering: Ordering::Equal, inclusive: false, .. } => {
                into.push(column);
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.equality_columns(into);
                right.equality_columns(into);
            }
            Self::All
            | Self::Compare { .. }
            | Self::Null { .. }
            | Self::Row(_)
            | Self::Joined { .. }
            | Self::JoinedAll(_)
            | Self::Window(_) => {}
        }
    }

    /// The columns this scope tests, lowercase.
    pub fn columns(&self, into: &mut std::collections::BTreeSet<String>) {
        match self {
            Self::All => {}
            Self::Compare { column, .. }
            | Self::Null { column, .. }
            | Self::In { column, .. }
            | Self::Joined { column, .. } => {
                into.insert(column.clone());
            }
            Self::JoinedAll(group) => into.extend(group.columns().cloned()),
            Self::Row(condition) => {
                into.extend(condition.term.columns.iter().map(|read| read.column.clone()));
            }
            Self::Window(window) => into.extend(window.columns().cloned()),
            Self::And(left, right) | Self::Or(left, right) => {
                left.columns(into);
                right.columns(into);
            }
        }
    }

    /// Both scopes: either alone when the other is every row.
    pub fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, scope) | (scope, Self::All) => scope,
            (left, right) => Self::And(Box::new(left), Box::new(right)),
        }
    }

    /// Either scope: every row when either is.
    pub(crate) fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (left, right) => Self::Or(Box::new(left), Box::new(right)),
        }
    }
}

/// What a run depends on through one reference to a table, one of the
/// statement's sources that reads it.
pub(crate) struct Reference {
    /// The rows it depends on.
    pub(crate) scope: RowScope,
    /// The columns the statement reads through it
    /// (`reference_columns.rs`).
    pub(crate) columns: Arc<BTreeSet<String>>,
}

/// What one run of a statement depends on: each reference to each table it
/// read as one of its sources, and the app's views and the CTEs it read as
/// sources of their own, by the names SQLite reports reads through them by,
/// whose tables' rows those scopes cover. A table read otherwise, as
/// through a view read whole, depends on every row, but for a view read whole
/// in a column nothing reads, whose reads give no row. Empty when the
/// statement's shape keeps every table whole.
pub struct Scopes {
    /// The references to each table, by its name, lowercase.
    pub(crate) tables: BTreeMap<String, Vec<Reference>>,
    pub(crate) inlined: BTreeSet<String>,
    /// The shadow tables of the contentful FTS5 tables the statement reads,
    /// whose reads count for nothing (`Sources::fts5_shadows`).
    pub(crate) fts5_shadows: BTreeSet<String>,
    /// What the statement reads only in columns nothing reads
    /// (`unread_columns.rs`).
    pub(crate) unread: crate::unread_columns::UnreadReads,
    /// The columns of the tables in `tables` that only what nothing reads
    /// names, which no reference reads though SQLite reports them.
    pub(crate) unread_columns: crate::read_set::ColumnSet,
    /// Columns the statement reads that SQLite's authorizer doesn't report:
    /// those its USING and NATURAL joins compare and every column of a table
    /// that a query whose rows SQLite picks reads, or every column when the
    /// planner couldn't tell which (`unreported_columns.rs`).
    pub(crate) unreported: std::sync::Arc<crate::unreported_columns::UnreportedColumns>,
}

impl Default for Scopes {
    /// Every table whole, with no column beyond those SQLite reports.
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            inlined: BTreeSet::new(),
            fts5_shadows: BTreeSet::new(),
            unread: Default::default(),
            unread_columns: crate::read_set::ColumnSet::new(),
            unreported: std::sync::Arc::new(crate::unreported_columns::UnreportedColumns::Known(
                crate::read_set::ColumnSet::new(),
            )),
        }
    }
}

/// The values of some of a changed row's columns, as a write recorded them:
/// its key's, and those a waiting live query reads or its scope tests.
#[derive(Clone, Debug, PartialEq)]
pub struct RowImage {
    /// The columns, lowercase, shared by every image of one table in a write.
    columns: Arc<[String]>,
    /// Each column's value, or `None` where the write couldn't record it.
    values: Vec<Option<Value>>,
}

impl RowImage {
    /// The image of a row that holds `values` in `columns`.
    pub fn new(columns: Arc<[String]>, values: Vec<Option<Value>>) -> Self {
        assert_eq!(columns.len(), values.len(), "one value per column");
        Self { columns, values }
    }

    /// The value of `column`, lowercase, if the write recorded it.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|name| name == column)
            .and_then(|index| self.values[index].as_ref())
    }
}

/// SQLite's order between two values of a column with BINARY collation and
/// no conversion between them; `None` when either is NULL, which equals and
/// orders against nothing.
pub fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Integer(_) | Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
        }
    }
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Blob(left), Value::Blob(right)) => Some(left.cmp(right)),
        (Value::Integer(left), Value::Integer(right)) => Some(left.cmp(right)),
        (Value::Real(left), Value::Real(right)) => Some(compare_reals(*left, *right)),
        (Value::Integer(left), Value::Real(right)) => Some(compare_integer_real(*left, *right)),
        (Value::Real(left), Value::Integer(right)) => {
            Some(compare_integer_real(*right, *left).reverse())
        }
        _ => Some(rank(left).cmp(&rank(right))),
    }
}

/// SQLite stores no NaN, and -0.0 equals 0.0.
fn compare_reals(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).expect("SQLite stores no NaN")
}

/// Compares an integer with a real exactly as SQLite's `sqlite3IntFloatCompare`
/// does, because converting the integer to `f64` would round it.
fn compare_integer_real(integer: i64, real: f64) -> Ordering {
    // 2^63 is the first real above every integer.
    if real < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    if real >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    // The real's whole part is an integer in range; its fraction decides a tie.
    let whole = real as i64;
    integer.cmp(&whole).then_with(|| compare_reals(whole as f64, real))
}

#[cfg(test)]
#[path = "row_scope_tests.rs"]
mod tests;
