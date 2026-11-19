//! A condition on one table's row that sqlive's own comparisons can't read,
//! and an ORDER BY term's expression over one table's row, judged by asking
//! SQLite (§4).
//!
//! A WHERE or ON conjunct whose every name is a stored, BINARY column of one
//! table of its own SELECT, and that holds no subquery, no clock and no
//! function that isn't deterministic, narrows that table to the rows it holds
//! for: a function of a column, a CASE, LIKE and GLOB, `<>`, a row value, a
//! COLLATE. sqlive compares none of this itself. A write asks SQLite whether
//! the changed row's old and new values pass: a SELECT over a row of those
//! values, each with its column's type, so the condition compares as it does
//! in the table. A value sqlive can't give its column's type, one the write
//! didn't record, an error and a question past the lookups' step limit count
//! as a match.
//!
//! The condition may be true of the NULLs a missing right row gives, so it
//! never makes a LEFT JOIN inner, and it isn't passed into a view's or
//! subquery's rows.
//!
//! An ORDER BY term's expression over such columns, or a generated column's,
//! bounds a full run's rows by the last row's value (`scope_order.rs`): the
//! question tests whether the changed row's value is NULL, or how it compares
//! with that value.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::row_scope::RowImage;
use crate::scope_plan::Planner;
use crate::scope_term::Affinity;
use crate::select_syntax::{Functions, Part};
use crate::sql_value::SqlValue as Value;
use sqlite3_parser::ast::Expr;

/// An expression over one table's row, a conjunct or an ORDER BY term's
/// value, and what it names, before a run's values.
#[derive(Debug)]
pub(crate) struct RowTerm {
    /// The expression as SQL, its parameters numbered as the plan's
    /// statement numbers them.
    pub(crate) sql: String,
    /// How the statement names the table's rows.
    pub(crate) qualifier: String,
    /// The columns it names, lowercase, with their affinities.
    pub(crate) columns: Vec<(String, Affinity)>,
}

/// An expression over one table's row, with the affinity SQLite gives its
/// value: an ORDER BY term's, which has none, or a generated column's own.
#[derive(Clone, Debug)]
pub(crate) struct RowExpression {
    pub(crate) term: Arc<RowTerm>,
    pub(crate) affinity: Affinity,
}

/// The term `expr`, a condition or a value whose names resolve to the table
/// item `item` of `core`, gives, when SQLite can evaluate it row by row;
/// `parts` are its parts.
pub(crate) fn item_row_term(
    planner: &Planner,
    core: usize,
    item: usize,
    expr: &Expr<'_>,
    parts: &[Part<'_>],
) -> Option<Arc<RowTerm>> {
    let from = &planner.items[item];
    from.source.table.as_ref()?;
    row_term(planner.functions, &from.source.qualifier, expr, parts, &|name| {
        let (_, column) = planner.resolve(core, name)?;
        let affinity = from.columns.affinity(&column)?;
        Some((column, affinity))
    })
}

/// The term `expr` gives a table's rows, which the statement names by
/// `qualifier`, when SQLite can evaluate it row by row: `column` gives the
/// column, lowercase, and the affinity of each name in it, when sqlive
/// compares that column's values, and the connection's `functions` tell
/// whether each call repeats; `parts` are its parts.
pub(crate) fn row_term(
    functions: &Functions,
    qualifier: &str,
    expr: &Expr<'_>,
    parts: &[Part<'_>],
    column: &dyn Fn(&Expr<'_>) -> Option<(String, Affinity)>,
) -> Option<Arc<RowTerm>> {
    let mut columns: Vec<(String, Affinity)> = Vec::new();
    for part in parts {
        match part {
            Part::Name(name) => {
                // A name qualified by a database isn't one the judging
                // statement could resolve.
                if matches!(name, Expr::DoublyQualified(..)) {
                    return None;
                }
                let (column, affinity) = column(name)?;
                if !columns.iter().any(|(named, _)| *named == column) {
                    columns.push((column, affinity));
                }
            }
            Part::Function(name, arguments, window) => {
                if *window || !functions.repeats(name, *arguments) {
                    return None;
                }
            }
            Part::Collation => {}
            Part::Select(_) | Part::Subquery(_) | Part::Table | Part::Clock => return None,
        }
    }
    if columns.is_empty() {
        return None;
    }
    Some(Arc::new(RowTerm { sql: expr.to_string(), qualifier: qualifier.to_owned(), columns }))
}

/// A run's condition: what it tests of a term, with the run's parameters.
#[derive(Debug)]
pub struct RowCondition {
    pub(crate) term: Arc<RowTerm>,
    pub(crate) parameters: Vec<Value>,
    pub(crate) test: RowTest,
}

/// What a run's condition tests of its term.
#[derive(Debug)]
pub(crate) enum RowTest {
    /// That the term, a condition, holds.
    Holds,
    /// That the term's value is NULL, or isn't when `not`.
    Null { not: bool },
    /// That the term's value, given `affinity`, compares with `value`, which
    /// isn't NULL, as `ordering`, as an ORDER BY compares two values.
    Compare { ordering: Ordering, value: Value, affinity: Affinity },
}

impl RowCondition {
    /// The condition as SQL over the row the term's qualifier names, with
    /// the value it compares with appended to `bound` as a parameter.
    /// `None` when no cast gives that value the term's affinity and keeps it
    /// as it is.
    pub(crate) fn sql(&self, bound: &mut Vec<Value>) -> Option<String> {
        let sql = &self.term.sql;
        Some(match &self.test {
            RowTest::Holds => format!("({sql})"),
            RowTest::Null { not } => {
                format!("({sql}) IS {}NULL", if *not { "NOT " } else { "" })
            }
            RowTest::Compare { ordering, value, affinity } => {
                let typed = typed(&format!("?{}", bound.len() + 1), *affinity, value)?;
                bound.push(value.clone());
                let operator = match ordering {
                    Ordering::Less => "<",
                    Ordering::Equal => "=",
                    Ordering::Greater => ">",
                };
                // A unary plus takes away the expression's affinity, so the
                // comparison converts its value only to the affinity the
                // typed value carries, as a generated column's value has it.
                format!("+({sql}) {operator} {typed}")
            }
        })
    }

    /// The SQL that asks whether the row `image` passes, and its parameters:
    /// the run's, then any value the condition compares with, then the
    /// image's values. `None` when sqlive can't ask, which counts as a
    /// match: the image lacks a column, or a value is one no cast would keep
    /// as it is in its column's type.
    pub(crate) fn question(&self, image: &RowImage) -> Option<(String, Vec<Value>)> {
        let term = &self.term;
        let mut bound = self.parameters.clone();
        let condition = self.sql(&mut bound)?;
        let mut row = Vec::with_capacity(term.columns.len());
        for (column, affinity) in &term.columns {
            let value = image.get(column)?;
            let typed = typed(&format!("?{}", bound.len() + 1), *affinity, value)?;
            row.push(format!("{typed} AS {}", crate::sql::identifier(column)));
            bound.push(value.clone());
        }
        let sql = format!(
            "SELECT 1 FROM (SELECT {}) AS {} WHERE {condition} LIMIT 1",
            row.join(","),
            term.qualifier,
        );
        Some((sql, bound))
    }
}

/// The parameter `placeholder`, bound to `value`, as SQL of `affinity`: a
/// subquery's column, and a comparison's operand, take the affinity of their
/// expression, so a cast gives the value its own. `None` for a value no cast
/// keeps as it is.
fn typed(placeholder: &str, affinity: Affinity, value: &Value) -> Option<String> {
    Some(match (affinity, value) {
        (_, Value::Null) | (Affinity::Blob, _) => placeholder.to_owned(),
        (Affinity::Text, Value::Text(_)) => format!("CAST({placeholder} AS TEXT)"),
        (Affinity::Numeric, Value::Integer(_)) => format!("CAST({placeholder} AS INTEGER)"),
        (Affinity::Numeric, Value::Real(_)) => format!("CAST({placeholder} AS REAL)"),
        _ => return None,
    })
}
