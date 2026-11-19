//! Forced reruns (database-boundary): a live query runs again because what
//! it read changed, so no column counts up to make it run again. A counter
//! is a column some statement sets to itself plus a positive integer
//! (`UPDATE t SET c = c + 1`, an upsert's `DO UPDATE SET c = c + 1`) and
//! that no statement anywhere sets to anything else; one that is also reset
//! to zero or `NULL` is a counter with a start. Count tables that decrement,
//! upserts that copy `excluded.c` and columns set to bound values are none.
//! The SQL read is that of every `.sql` file and every production string
//! literal, as the database rule reads it. The policy's `database.counters`
//! lists the columns that may count up, each with why.

use std::collections::BTreeMap;

use crate::crate_code::CrateCode;
use crate::database_boundary::production_sql;
use crate::sql_statements::statements;

/// What the SQL does to one column.
#[derive(Default)]
struct ColumnSets {
    /// Where it is increased, the first by path and line.
    first_increment: Option<(String, usize)>,
    /// Whether any statement sets it to anything else.
    set_otherwise: bool,
    /// Whether any statement resets it to zero or `NULL`.
    reset: bool,
}

fn column_sets(code: &CrateCode) -> BTreeMap<String, ColumnSets> {
    let mut columns: BTreeMap<String, ColumnSets> = BTreeMap::new();
    for text in production_sql(code) {
        for statement in statements(&text.text) {
            for assignment in statement.assignments {
                let sets = columns
                    .entry(format!("{}.{}", assignment.table, assignment.column))
                    .or_default();
                if assignment.increment {
                    let place = (text.path.clone(), text.line_at(assignment.at));
                    if sets.first_increment.as_ref().is_none_or(|first| place < *first) {
                        sets.first_increment = Some(place);
                    }
                } else if assignment.reset {
                    sets.reset = true;
                } else {
                    sets.set_otherwise = true;
                }
            }
        }
    }
    columns
}

/// A column only ever counted up.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Counter {
    /// Where a statement first increases it, by path and line.
    pub(crate) path: String,
    pub(crate) line: usize,
    /// The column, as `table.column`.
    pub(crate) column: String,
    /// Whether a statement also resets it to zero or `NULL`.
    pub(crate) reset: bool,
}

/// Each column only ever counted up, at its first increment.
pub(crate) fn counters(code: &CrateCode) -> Vec<Counter> {
    column_sets(code)
        .into_iter()
        .filter(|(_, sets)| !sets.set_otherwise)
        .filter_map(|(column, sets)| {
            let (path, line) = sets.first_increment?;
            Some(Counter { path, line, column, reset: sets.reset })
        })
        .collect()
}

#[cfg(test)]
#[path = "counters_tests.rs"]
mod tests;
