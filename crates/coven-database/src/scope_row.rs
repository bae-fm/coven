//! A condition on one table's row that coven's comparison handling can't
//! read, judged by asking SQLite (§4).
//!
//! A WHERE or ON conjunct whose every name is a stored, BINARY column of one
//! table of its own SELECT, and that holds no subquery, no clock and no
//! function that isn't deterministic, narrows that table to the rows it holds
//! for: a function of a column, a CASE, LIKE and GLOB, `<>`, a row value, a
//! COLLATE. Coven compares none of this itself. A write asks SQLite whether
//! the changed row's old and new values pass: a SELECT over a row of those
//! values, each with its column's type, so the condition compares as it does
//! in the table. A value coven can't give its column's type, one the write
//! didn't record, an error and a question past the lookups' step limit count
//! as a match.
//!
//! The condition may be true of the NULLs a missing right row gives, so it
//! never makes a LEFT JOIN inner, and it isn't passed into a view's or
//! subquery's rows.

use std::sync::Arc;

use crate::row_scope::RowImage;
use crate::scope_plan::Planner;
use crate::scope_term::{Affinity, ScopeTerm};
use crate::select_syntax::{deterministic, is_clock_function, Part};
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::Expr;

/// The condition of one conjunct and what it names, before a run's values.
#[derive(Debug)]
pub(crate) struct RowTerm {
    /// The conjunct as SQL, its parameters numbered as the plan's statement
    /// numbers them.
    pub(crate) condition: String,
    /// How the statement names the table's rows.
    pub(crate) qualifier: String,
    /// The columns it names, lowercase, with their affinities.
    pub(crate) columns: Vec<(String, Affinity)>,
}

/// The term `condition` gives the table item `item` of `core`, when it is a
/// condition SQLite can judge row by row; `parts` are its parts.
pub(crate) fn row_term(
    connection: &Connection,
    planner: &Planner,
    core: usize,
    item: usize,
    condition: &Expr<'_>,
    parts: &[Part<'_>],
) -> rusqlite::Result<Option<ScopeTerm>> {
    let from = &planner.items[item];
    if from.source.table.is_none() {
        return Ok(None);
    }
    let mut columns: Vec<(String, Affinity)> = Vec::new();
    let mut functions: Vec<String> = Vec::new();
    for part in parts {
        match part {
            Part::Name(name) => {
                // A name qualified by a database isn't one the judging
                // statement could resolve.
                if matches!(name, Expr::DoublyQualified(..)) {
                    return Ok(None);
                }
                let Some((_, column)) = planner.resolve(core, name) else {
                    return Ok(None);
                };
                let Some(affinity) = from.columns.affinity(&column) else {
                    return Ok(None);
                };
                if !columns.iter().any(|(named, _)| *named == column) {
                    columns.push((column, affinity));
                }
            }
            Part::Function(name, window) => {
                let name = name.to_ascii_lowercase();
                if *window || planner.aggregates.contains(&name) || is_clock_function(&name) {
                    return Ok(None);
                }
                functions.push(name);
            }
            Part::Collation => {}
            Part::Select(_) | Part::Table | Part::Clock => return Ok(None),
        }
    }
    if columns.is_empty() || !deterministic(connection, &functions)? {
        return Ok(None);
    }
    Ok(Some(ScopeTerm::Row(Arc::new(RowTerm {
        condition: condition.to_string(),
        qualifier: from.source.qualifier.clone(),
        columns,
    }))))
}

/// A run's condition: the term bound to the run's parameters.
#[derive(Debug)]
pub(crate) struct RowCondition {
    pub(crate) term: Arc<RowTerm>,
    pub(crate) parameters: Vec<Value>,
}

impl RowCondition {
    /// The SQL that asks whether the row `image` passes, and its parameters:
    /// the run's, then the image's values. `None` when coven can't ask, which
    /// counts as a match: the image lacks a column, or holds a value no
    /// cast would keep as it is in the column's type.
    pub(crate) fn question(&self, image: &RowImage) -> Option<(String, Vec<Value>)> {
        let term = &self.term;
        let mut bound = self.parameters.clone();
        let mut row = Vec::with_capacity(term.columns.len());
        for (column, affinity) in &term.columns {
            let value = image.get(column)?;
            let placeholder = format!("?{}", bound.len() + 1);
            // SQLite gives a subquery's column the affinity of its result
            // expression, so a cast gives the column its own.
            let typed = match (affinity, value) {
                (_, Value::Null) | (Affinity::Blob, _) => placeholder,
                (Affinity::Text, Value::Text(_)) => format!("CAST({placeholder} AS TEXT)"),
                (Affinity::Numeric, Value::Integer(_)) => {
                    format!("CAST({placeholder} AS INTEGER)")
                }
                (Affinity::Numeric, Value::Real(_)) => format!("CAST({placeholder} AS REAL)"),
                _ => return None,
            };
            row.push(format!("{typed} AS {}", crate::sql::identifier(column)));
            bound.push(value.clone());
        }
        let sql = format!(
            "SELECT 1 FROM (SELECT {}) AS {} WHERE {} LIMIT 1",
            row.join(","),
            term.qualifier,
            term.condition
        );
        Some((sql, bound))
    }
}

#[cfg(test)]
#[path = "scope_row_tests.rs"]
mod tests;
