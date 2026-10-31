//! Which rows of a table a live query depends on, and the values of a changed
//! row that decide whether it is one of them (§4).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::scope_lookup::{Lookup, Lookups};
use crate::sql_value::SqlValue as Value;

/// A predicate over a table's rows. A change reruns a query when its row
/// passes in its old or its new image.
#[derive(Clone, Debug)]
pub(crate) enum RowScope {
    /// Every row.
    All,
    /// Rows whose `column` compares with `value` as `ordering`, or equal too
    /// when `inclusive`.
    Compare {
        column: String,
        value: Value,
        ordering: Ordering,
        inclusive: bool,
    },
    /// Rows whose `column` equals one of `values`, sorted in SQLite's order
    /// with NULLs and repeats left out: made by [`RowScope::equal`].
    In {
        column: String,
        values: Vec<Value>,
    },
    /// Rows whose `column` equals a value some row of another source holds
    /// in its own scope: a join, which a write judges by running `lookup`.
    Joined {
        column: String,
        lookup: Arc<Lookup>,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl RowScope {
    /// Whether `row` passes, running `lookups` for a join's values. A
    /// column the image doesn't hold passes: the write that made it didn't
    /// know this scope would test it.
    pub(crate) fn matches(
        &self,
        row: &RowImage,
        lookups: &mut Lookups<'_>,
    ) -> rusqlite::Result<bool> {
        Ok(match self {
            Self::All => true,
            Self::Compare { column, value, ordering, inclusive } => {
                row.get(column).is_none_or(|actual| {
                    compare(actual, value)
                        .is_some_and(|found| found == *ordering || (*inclusive && found.is_eq()))
                })
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
                // NULL equals nothing, so no join matches it.
                Some(Value::Null) => false,
                Some(value) => lookups(lookup, value)?,
            },
            Self::And(left, right) => left.matches(row, lookups)? && right.matches(row, lookups)?,
            Self::Or(left, right) => left.matches(row, lookups)? || right.matches(row, lookups)?,
        })
    }

    /// Rows whose `column` equals one of `values`.
    pub(crate) fn equal(column: String, mut values: Vec<Value>) -> Self {
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

    /// The columns this scope tests, lowercase.
    pub(crate) fn columns(&self, into: &mut std::collections::BTreeSet<String>) {
        match self {
            Self::All => {}
            Self::Compare { column, .. }
            | Self::In { column, .. }
            | Self::Joined { column, .. } => {
                into.insert(column.clone());
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.columns(into);
                right.columns(into);
            }
        }
    }

    /// Both scopes: either alone when the other is every row.
    pub(crate) fn and(self, other: Self) -> Self {
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

/// What one run of a statement depends on: the scope of each table it read
/// as one of its sources, and the app's views and the CTEs it read as
/// sources of their own, by the names SQLite reports reads through them by,
/// whose tables' rows those scopes cover. A table read otherwise, as
/// through one of coven's views, depends on every row. Empty when the
/// statement's shape keeps every table whole.
#[derive(Default)]
pub(crate) struct Scopes {
    pub(crate) tables: BTreeMap<String, RowScope>,
    pub(crate) inlined: BTreeSet<String>,
}

/// The values of some of a changed row's columns, as a write recorded them:
/// its key's, and those a waiting live query's scope tests.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowImage {
    /// The columns, lowercase, shared by every image of one table in a write.
    columns: Arc<[String]>,
    /// Each column's value, or `None` where the write couldn't record it.
    values: Vec<Option<Value>>,
}

impl RowImage {
    pub(crate) fn new(columns: Arc<[String]>, values: Vec<Option<Value>>) -> Self {
        assert_eq!(columns.len(), values.len(), "one value per column");
        Self { columns, values }
    }

    /// The value of `column`, lowercase, if the write recorded it.
    pub(crate) fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|name| name == column)
            .and_then(|index| self.values[index].as_ref())
    }
}

/// SQLite's order between two values of a column with BINARY collation and
/// no conversion between them; `None` when either is NULL, which equals and
/// orders against nothing.
pub(crate) fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    fn rank(value: &Value) -> u8 {
        match value {
            Value::Null => 0,
            Value::Integer(_) | Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
        }
    }
    fn number(value: &Value) -> Vec<u8> {
        let value = match value {
            Value::Integer(value) => coven_format::value::Value::Integer(*value),
            Value::Real(value) => {
                coven_format::value::Value::Real(if *value == 0.0 { 0 } else { value.to_bits() })
            }
            _ => unreachable!("numeric comparison"),
        };
        coven_format::key::encode_key(&[value])
            .expect("one SQLite number has a canonical key encoding")
    }
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Blob(left), Value::Blob(right)) => Some(left.cmp(right)),
        (Value::Integer(_) | Value::Real(_), Value::Integer(_) | Value::Real(_)) => {
            Some(number(left).cmp(&number(right)))
        }
        _ => Some(rank(left).cmp(&rank(right))),
    }
}

#[cfg(test)]
#[path = "row_scope_tests.rs"]
mod tests;
