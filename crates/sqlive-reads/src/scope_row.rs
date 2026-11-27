//! A condition on one table's row that sqlive's own comparisons can't read,
//! an expression over one table's row compared with another source's value,
//! and an ORDER BY term's expression over one table's row, judged by asking
//! SQLite (SPEC.md section 5).
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
//! never makes a LEFT JOIN inner. A condition on a view's or subquery's
//! columns passes into the table those columns come from, as a condition on
//! that table's own columns (`RowTerm::over`).
//!
//! An expression over such columns that an equality compares with another
//! source's value narrows the table to the rows whose value some row of that
//! source gives in its scope: the question asks whether the expression's
//! value in the changed row is IN the values the other source's rows give
//! (`scope_lookup::joined`), as the statement's equality compares them. The
//! expression is NULL wherever its columns are, or its table is no LEFT
//! JOIN's (`Planner::member_expression`), so the equality, as an edge, can
//! make a LEFT JOIN inner.
//!
//! An ORDER BY term's expression over such columns, or a generated column's,
//! bounds a full run's rows by the last row's value (`scope_order.rs`): the
//! question tests whether the changed row's value is NULL, or how it compares
//! with that value.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::row_scope::RowImage;
use crate::scope_lookup::Lookup;
use crate::scope_plan::Planner;
use crate::scope_term::Affinity;
use crate::select_syntax::{Functions, Part};
use crate::sql_value::SqlValue as Value;
use sqlite3_parser::ast::Expr;

/// An expression over one source's row, a conjunct or an ORDER BY term's
/// value, and what it names, before a run's values.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RowTerm {
    /// The expression as SQL, its parameters numbered as the plan's
    /// statement numbers them.
    pub(crate) sql: String,
    /// The name the SQL gives the row it reads.
    pub(crate) qualifier: String,
    /// The columns the SQL names.
    pub(crate) columns: Vec<RowColumn>,
    /// How the statement names the rows the term is of, when they are not the
    /// ones the SQL was written over: those of a source a derived item passes
    /// its columns on from.
    pub(crate) passed_to: Option<String>,
}

/// A column a row term's SQL names.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RowColumn {
    /// The name the SQL gives it, lowercase.
    pub(crate) name: String,
    /// The column of the term's rows that holds its value, lowercase.
    pub(crate) column: String,
    pub(crate) affinity: Affinity,
}

impl RowColumn {
    /// The column `column` of the rows the SQL is written over.
    pub(crate) fn written(column: String, affinity: Affinity) -> Self {
        Self { name: column.clone(), column, affinity }
    }
}

impl RowTerm {
    /// This term over the rows the statement names `rows`, of a source whose
    /// column `column` gives for each of this one's rows' columns holds the
    /// same value, or `None` when it gives none for one of them.
    pub(crate) fn over(
        &self,
        rows: &str,
        column: &dyn Fn(&str) -> Option<String>,
    ) -> Option<Arc<Self>> {
        let columns = self
            .columns
            .iter()
            .map(|read| Some(RowColumn { column: column(&read.column)?, ..read.clone() }))
            .collect::<Option<_>>()?;
        Some(Arc::new(Self {
            sql: self.sql.clone(),
            qualifier: self.qualifier.clone(),
            columns,
            passed_to: Some(rows.to_owned()),
        }))
    }

    /// The expression as SQL over the rows the term is of, as a lookup that
    /// reads them writes it. Over rows the SQL was not written over, it reads
    /// each name from them. SQLite keeps a column's type through such a
    /// SELECT over a table, though not over a row of cast values, so a
    /// question about a changed row writes its values under the SQL's own
    /// names instead (`RowCondition::question`).
    fn over_rows(&self) -> String {
        let Some(rows) = &self.passed_to else {
            return self.sql.clone();
        };
        let columns: Vec<String> = self
            .columns
            .iter()
            .map(|read| {
                format!(
                    "{rows}.{} AS {}",
                    crate::sql::identifier(&read.column),
                    crate::sql::identifier(&read.name)
                )
            })
            .collect();
        format!("(SELECT ({}) FROM (SELECT {}) AS {})", self.sql, columns.join(","), self.qualifier)
    }
}

/// An expression over one table's row, with the affinity SQLite gives its
/// value: an ORDER BY term's, which has none, or a generated column's own.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RowExpression {
    pub(crate) term: Arc<RowTerm>,
    pub(crate) affinity: Affinity,
}

/// The term `expr`, a condition or a value whose names resolve to item `item`
/// of `core`, a table or a derived item, gives, when SQLite can evaluate it
/// row by row; `parts` are its parts.
pub(crate) fn item_row_term(
    planner: &Planner,
    core: usize,
    item: usize,
    expr: &Expr<'_>,
    parts: &[Part<'_>],
) -> Option<Arc<RowTerm>> {
    let from = &planner.items[item];
    if from.source.table().is_none() && from.derived.is_none() {
        return None;
    }
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
    let mut columns: Vec<RowColumn> = Vec::new();
    for part in parts {
        match part {
            Part::Name(name) => {
                // A name qualified by a database isn't one the judging
                // statement could resolve.
                if matches!(name, Expr::DoublyQualified(..)) {
                    return None;
                }
                let (column, affinity) = column(name)?;
                if !columns.iter().any(|read| read.name == column) {
                    columns.push(RowColumn::written(column, affinity));
                }
            }
            Part::Function(name, arguments, window) => {
                if *window || !functions.repeats(name, *arguments) {
                    return None;
                }
            }
            Part::Collation => {}
            Part::Select(_) | Part::Subquery(_) | Part::Table(_) | Part::Clock => return None,
        }
    }
    if columns.is_empty() {
        return None;
    }
    Some(Arc::new(RowTerm {
        sql: expr.to_string(),
        qualifier: qualifier.to_owned(),
        columns,
        passed_to: None,
    }))
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
    /// isn't NULL, as `ordering`, as an ORDER BY compares two values: text
    /// by `collation` when the ORDER BY's isn't BINARY.
    Compare { ordering: Ordering, value: Value, affinity: Affinity, collation: Option<String> },
    /// That the term's value equals a value some row of the lookup's
    /// partner gives in its scope, as the statement's equality compares
    /// them: a member's expression joined to its partner.
    Joined(Arc<Lookup>),
}

impl RowCondition {
    /// The condition as SQL over the row the term's qualifier names, with
    /// the value it compares with appended to `bound` as a parameter.
    /// `None` when no cast gives that value the term's affinity and keeps it
    /// as it is.
    pub(crate) fn sql(&self, bound: &mut Vec<Value>) -> Option<String> {
        self.testing(&self.term.over_rows(), bound)
    }

    /// What the condition tests of `sql`, its term's expression.
    fn testing(&self, sql: &str, bound: &mut Vec<Value>) -> Option<String> {
        Some(match &self.test {
            RowTest::Holds => format!("({sql})"),
            RowTest::Null { not } => {
                format!("({sql}) IS {}NULL", if *not { "NOT " } else { "" })
            }
            RowTest::Compare { ordering, value, affinity, collation } => {
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
                let collate = collation.as_ref().map_or_else(String::new, |name| {
                    format!(" COLLATE {}", crate::sql::identifier(name))
                });
                format!("+({sql}){collate} {operator} {typed}")
            }
            RowTest::Joined(lookup) => {
                crate::scope_lookup::joined(&format!("({sql})"), lookup, bound)
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
        let condition = self.testing(&term.sql, &mut bound)?;
        let mut row = Vec::with_capacity(term.columns.len());
        for read in &term.columns {
            let value = image.get(&read.column)?;
            let typed = typed(&format!("?{}", bound.len() + 1), read.affinity, value)?;
            row.push(format!("{typed} AS {}", crate::sql::identifier(&read.name)));
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
pub(crate) fn typed(placeholder: &str, affinity: Affinity, value: &Value) -> Option<String> {
    Some(match (affinity, value) {
        (_, Value::Null) | (Affinity::Blob, _) => placeholder.to_owned(),
        (Affinity::Text, Value::Text(_)) => format!("CAST({placeholder} AS TEXT)"),
        (Affinity::Numeric, Value::Integer(_)) => format!("CAST({placeholder} AS INTEGER)"),
        (Affinity::Numeric, Value::Real(_)) => format!("CAST({placeholder} AS REAL)"),
        _ => return None,
    })
}
