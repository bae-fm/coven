//! A statement's conditions on one source's rows, read from its parsed SQL and
//! bound to each run's values (SPEC.md section 5). A condition it can't read,
//! on one table's columns, becomes a `Row` term that a write judges by asking
//! SQLite (`scope_row.rs`).

use std::cmp::Ordering;
use std::sync::Arc;

use crate::row_scope::{compare, RowScope};
use crate::scope_row::{RowCondition, RowTerm, RowTest};
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Literal, Operator, UnaryOperator};

/// A condition on one source's rows, with values from the SQL or its
/// parameters.
#[derive(Clone)]
pub(crate) enum ScopeTerm {
    Compare {
        column: String,
        affinity: Affinity,
        operand: Operand,
        ordering: Ordering,
        inclusive: bool,
    },
    In {
        column: String,
        affinity: Affinity,
        operands: Vec<Operand>,
    },
    /// `column IS NULL`, or `IS NOT NULL` when `not`.
    Null {
        column: String,
        not: bool,
    },
    /// `(condition) op operand`: the truth of a condition compared with a
    /// value, which is where the condition holds, where it doesn't, or where
    /// it is NULL, whichever of those the comparison keeps.
    Truth {
        condition: Truth,
        /// How the comparison orders the truth with the value, and whether
        /// equal passes; `is` for `IS`, which NULL equals.
        ordering: Ordering,
        inclusive: bool,
        is: bool,
        operand: Operand,
    },
    /// A condition on the columns of one row that sqlive asks SQLite to judge
    /// (`scope_row.rs`).
    Row(Arc<RowTerm>),
    /// `column IS ?`: a NULL test when the run's value is NULL, an equality
    /// otherwise.
    Is {
        column: String,
        affinity: Affinity,
        operand: Operand,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl ScopeTerm {
    /// The scope this run's values give, or `None` when a value would need a
    /// conversion SQLite applies and sqlive doesn't.
    pub(crate) fn bind(&self, values: RunValues<'_>) -> Option<RowScope> {
        match self {
            Self::Compare { column, affinity, operand, ordering, inclusive } => {
                let value = operand.value(values, *affinity);
                affinity.compares(&value).then(|| RowScope::Compare {
                    column: column.clone(),
                    value,
                    ordering: *ordering,
                    inclusive: *inclusive,
                })
            }
            Self::In { column, affinity, operands } => {
                let found: Vec<Value> =
                    operands.iter().map(|operand| operand.value(values, *affinity)).collect();
                found
                    .iter()
                    .all(|value| affinity.compares(value))
                    .then(|| RowScope::equal(column.clone(), found))
            }
            Self::Null { column, not } => {
                Some(RowScope::Null { column: column.clone(), not: *not })
            }
            Self::Row(term) => Some(RowScope::Row(Arc::new(RowCondition {
                term: Arc::clone(term),
                parameters: values.parameters().to_vec(),
                test: RowTest::Holds,
            }))),
            Self::Truth { condition, ordering, inclusive, is, operand } => {
                let value = operand.known(values);
                let passes = |truth: Option<i64>| match (truth, &value) {
                    // Which truths pass depends on the run's value, so each
                    // can.
                    (_, None) => true,
                    (None, Some(value)) => *is && matches!(value, Value::Null),
                    (Some(_), Some(Value::Null)) => false,
                    (Some(truth), Some(value)) => compare(&Value::Integer(truth), value)
                        .is_some_and(|found| {
                            if *is {
                                found.is_eq()
                            } else {
                                found == *ordering || (*inclusive && found.is_eq())
                            }
                        }),
                };
                let mut scope = None;
                for (truth, holds) in [(Some(1), true), (Some(0), false), (None, false)] {
                    if !passes(truth) {
                        continue;
                    }
                    let found = match truth {
                        None => condition.when_null(values),
                        _ => condition.when(holds, values),
                    }?;
                    scope = Some(match scope {
                        Some(other) => RowScope::or(other, found),
                        None => found,
                    });
                }
                Some(
                    scope.unwrap_or_else(|| RowScope::equal(condition.column().to_owned(), vec![])),
                )
            }
            Self::Is { column, affinity, operand } => {
                let value = operand.value(values, *affinity);
                if matches!(value, Value::Null) {
                    Some(RowScope::Null { column: column.clone(), not: false })
                } else {
                    affinity.compares(&value).then(|| RowScope::Compare {
                        column: column.clone(),
                        value,
                        ordering: Ordering::Equal,
                        inclusive: false,
                    })
                }
            }
            Self::And(left, right) => match (left.bind(values), right.bind(values)) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (scope, None) | (None, scope) => scope,
            },
            Self::Or(left, right) => Some(left.bind(values)?.or(right.bind(values)?)),
        }
    }

    /// Whether a row whose columns are all NULL can pass it: a NULL test
    /// does, as does `IS` a parameter, whose value isn't known until a run.
    /// Every other term is false where its column is NULL. An AND passes
    /// only where both parts do, an OR where either does.
    pub(crate) fn may_hold_on_nulls(&self) -> bool {
        match self {
            Self::Compare { .. } | Self::In { .. } => false,
            Self::Null { not, .. } => !not,
            Self::Is { .. } | Self::Truth { .. } | Self::Row(_) => true,
            Self::And(left, right) => left.may_hold_on_nulls() && right.may_hold_on_nulls(),
            Self::Or(left, right) => left.may_hold_on_nulls() || right.may_hold_on_nulls(),
        }
    }

    /// Whether it compares a column with a value sqlive reads: it does where
    /// either side of an AND does, or each side of an OR. A condition SQLite
    /// judges compares none.
    pub(crate) fn compares(&self) -> bool {
        match self {
            Self::Compare { .. }
            | Self::In { .. }
            | Self::Null { .. }
            | Self::Is { .. }
            | Self::Truth { .. } => true,
            Self::Row(_) => false,
            Self::And(left, right) => left.compares() || right.compares(),
            Self::Or(left, right) => left.compares() && right.compares(),
        }
    }

    /// Both terms.
    pub(crate) fn and(self, other: Self) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    /// Add `term` to the term `found` holds, with AND, or hold `term` alone
    /// when it holds none.
    pub(crate) fn add_to(found: &mut Option<Self>, term: Self) {
        *found = Some(match found.take() {
            Some(existing) => existing.and(term),
            None => term,
        });
    }

    /// The columns it tests.
    pub(crate) fn columns(&self, into: &mut Vec<String>) {
        match self {
            Self::Compare { column, .. }
            | Self::In { column, .. }
            | Self::Null { column, .. }
            | Self::Is { column, .. } => {
                if !into.contains(column) {
                    into.push(column.clone());
                }
            }
            Self::Truth { condition, .. } => {
                if !into.iter().any(|column| column == condition.column()) {
                    into.push(condition.column().to_owned());
                }
            }
            Self::Row(term) => {
                for read in &term.columns {
                    if !into.contains(&read.column) {
                        into.push(read.column.clone());
                    }
                }
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.columns(into);
                right.columns(into);
            }
        }
    }

    /// This term over another source's rows, whose column `column` gives
    /// for each of this one's holds the same value in each row: what holds
    /// of every row here holds of those. A part testing a column it gives
    /// none for is left out of an AND, and leaves an OR with nothing.
    /// A condition SQLite judges passes to rows the statement names `rows`,
    /// and is left out like the rest when there is no such name: a term that
    /// edges carry between columns, or that a derived item takes from its
    /// arms, holds none.
    pub(crate) fn renamed(
        &self,
        column: &dyn Fn(&str) -> Option<String>,
        rows: Option<&str>,
    ) -> Option<Self> {
        match self {
            Self::Compare { column: tested, affinity, operand, ordering, inclusive } => {
                Some(Self::Compare {
                    column: column(tested)?,
                    affinity: *affinity,
                    operand: operand.clone(),
                    ordering: *ordering,
                    inclusive: *inclusive,
                })
            }
            Self::In { column: tested, affinity, operands } => Some(Self::In {
                column: column(tested)?,
                affinity: *affinity,
                operands: operands.clone(),
            }),
            Self::Null { column: tested, not } => {
                Some(Self::Null { column: column(tested)?, not: *not })
            }
            Self::Row(term) => term.over(rows?, column).map(Self::Row),
            Self::Truth { condition, ordering, inclusive, is, operand } => Some(Self::Truth {
                condition: condition.renamed(column)?,
                ordering: *ordering,
                inclusive: *inclusive,
                is: *is,
                operand: operand.clone(),
            }),
            Self::Is { column: tested, affinity, operand } => Some(Self::Is {
                column: column(tested)?,
                affinity: *affinity,
                operand: operand.clone(),
            }),
            Self::And(left, right) => {
                match (left.renamed(column, rows), right.renamed(column, rows)) {
                    (Some(left), Some(right)) => Some(left.and(right)),
                    (term, None) | (None, term) => term,
                }
            }
            Self::Or(left, right) => Some(Self::Or(
                Box::new(left.renamed(column, rows)?),
                Box::new(right.renamed(column, rows)?),
            )),
        }
    }
}

/// The condition whose truth a term compares with a value: a NULL test, or
/// a column compared with a value. In each row it is 1, 0 or NULL; a NULL
/// test is never NULL, and a comparison is where its column is NULL.
#[derive(Clone)]
pub(crate) enum Truth {
    Null {
        column: String,
        not: bool,
    },
    Compare {
        column: String,
        affinity: Affinity,
        operand: Operand,
        ordering: Ordering,
        inclusive: bool,
    },
}

impl Truth {
    fn column(&self) -> &str {
        match self {
            Self::Null { column, .. } | Self::Compare { column, .. } => column,
        }
    }

    fn renamed(&self, rename: &dyn Fn(&str) -> Option<String>) -> Option<Self> {
        Some(match self {
            Self::Null { column, not } => Self::Null { column: rename(column)?, not: *not },
            Self::Compare { column, affinity, operand, ordering, inclusive } => Self::Compare {
                column: rename(column)?,
                affinity: *affinity,
                operand: operand.clone(),
                ordering: *ordering,
                inclusive: *inclusive,
            },
        })
    }

    /// The rows where the condition is true, or false when not `holds`, for
    /// a run bound to `values`; `None` when a value would be converted.
    fn when(&self, holds: bool, values: RunValues<'_>) -> Option<RowScope> {
        match self {
            Self::Null { column, not } => {
                Some(RowScope::Null { column: column.clone(), not: *not == holds })
            }
            Self::Compare { column, affinity, operand, ordering, inclusive } => {
                let value = operand.value(values, *affinity);
                if !affinity.compares(&value) {
                    return None;
                }
                // Compared with NULL it is NULL in every row.
                if matches!(value, Value::Null) {
                    return Some(RowScope::equal(column.clone(), vec![]));
                }
                let compare = |ordering, inclusive| RowScope::Compare {
                    column: column.clone(),
                    value: value.clone(),
                    ordering,
                    inclusive,
                };
                Some(match (holds, ordering) {
                    (true, _) => compare(*ordering, *inclusive),
                    // Where NULL fails both, as the comparison's negation does.
                    (false, Ordering::Equal) => RowScope::or(
                        compare(Ordering::Less, false),
                        compare(Ordering::Greater, false),
                    ),
                    (false, ordering) => compare(ordering.reverse(), !inclusive),
                })
            }
        }
    }

    /// The rows where the condition is NULL.
    fn when_null(&self, values: RunValues<'_>) -> Option<RowScope> {
        match self {
            // A NULL test is never NULL.
            Self::Null { column, .. } => Some(RowScope::equal(column.clone(), vec![])),
            Self::Compare { column, affinity, operand, .. } => {
                let value = operand.value(values, *affinity);
                if !affinity.compares(&value) {
                    return None;
                }
                Some(if matches!(value, Value::Null) {
                    RowScope::All
                } else {
                    RowScope::Null { column: column.clone(), not: false }
                })
            }
        }
    }
}

/// A value in a condition: one the SQL spells, or a bound parameter.
#[derive(Clone, Debug)]
pub(crate) enum Operand {
    Constant(Value),
    /// A parameter's position, from zero.
    Parameter(usize),
}

impl Operand {
    /// The value for a run bound to `values`, or `None` for a parameter that
    /// no run binds.
    fn known(&self, values: RunValues<'_>) -> Option<Value> {
        match (self, values) {
            (Self::Constant(value), _) => Some(value.clone()),
            (Self::Parameter(index), RunValues::Run(parameters)) => {
                Some(parameters[*index].clone())
            }
            (Self::Parameter(_), RunValues::Any) => None,
        }
    }

    /// The value for a run bound to `values`, where a column of `affinity`
    /// is compared with it: one the comparison binds when no run says which.
    pub(crate) fn value(&self, values: RunValues<'_>, affinity: Affinity) -> Value {
        self.known(values).unwrap_or_else(|| affinity.any_value())
    }
}

/// The values a statement's parameters hold as its plan's terms bind.
#[derive(Clone, Copy)]
pub(crate) enum RunValues<'a> {
    /// A run's parameters.
    Run(&'a [Value]),
    /// No run's: the scopes say which columns each term of the plan tests,
    /// whatever value a run gives it, and judge no row. A parameter a column
    /// is compared with stands for any value the comparison binds, a LIMIT's
    /// for any positive one, and one a condition's truth is compared with
    /// for any at all.
    Any,
}

impl<'a> RunValues<'a> {
    /// The run's parameters, which SQL that judges rows binds; none when no
    /// run binds them.
    pub(crate) fn parameters(self) -> &'a [Value] {
        match self {
            Self::Run(parameters) => parameters,
            Self::Any => &[],
        }
    }
}

/// A column's affinity, as SQLite derives it from the declared type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Affinity {
    /// INTEGER, REAL or NUMERIC: they compare their values alike.
    Numeric,
    /// CHAR, CLOB or TEXT.
    Text,
    /// BLOB, or no declared type: SQLite converts nothing.
    Blob,
}

impl Affinity {
    /// SQLite's rules for a declared column type (datatype3 §3.1).
    pub fn declared(declared: &str) -> Self {
        let declared = declared.to_ascii_uppercase();
        if declared.contains("INT") {
            Self::Numeric
        } else if ["CHAR", "CLOB", "TEXT"].iter().any(|word| declared.contains(word)) {
            Self::Text
        } else if declared.contains("BLOB") || declared.is_empty() {
            Self::Blob
        } else {
            Self::Numeric
        }
    }

    /// A value other than NULL that SQLite compares with a column of this
    /// affinity as it is.
    pub(crate) fn any_value(self) -> Value {
        match self {
            Self::Numeric | Self::Blob => Value::Integer(1),
            Self::Text => Value::Text(b"1".to_vec()),
        }
    }

    /// Whether comparing a column of this affinity with `value`, which has
    /// none, compares them as they are: SQLite would convert a text value
    /// for a numeric column, and a number for a text one.
    pub(crate) fn compares(self, value: &Value) -> bool {
        matches!(
            (self, value),
            (_, Value::Null | Value::Blob(_))
                | (Self::Blob, _)
                | (Self::Numeric, Value::Integer(_) | Value::Real(_))
                | (Self::Text, Value::Text(_))
        )
    }
}

/// The column `expr` names, lowercase, with its affinity, when it is one of
/// the source's whose conditions are being read, and a condition on it can
/// narrow.
pub(crate) type SourceColumn<'a> = dyn Fn(&Expr<'_>) -> Option<(String, Affinity)> + 'a;

/// The term `expr` gives the source `column` names columns of, if it
/// narrows that source.
pub(crate) fn term(
    connection: &Connection,
    column: &SourceColumn<'_>,
    expr: &Expr<'_>,
) -> rusqlite::Result<Option<ScopeTerm>> {
    let expr = crate::sql::unparenthesized(expr);
    if let Some((tested, not)) = null_test(expr) {
        if let Some((named, _)) = column(through_collation(tested)) {
            return Ok(Some(ScopeTerm::Null { column: named, not }));
        }
    }
    if let Expr::Binary(
        left,
        op @ (Operator::Equals
        | Operator::Less
        | Operator::LessEquals
        | Operator::Greater
        | Operator::GreaterEquals
        | Operator::Is),
        right,
    ) = expr
    {
        if let Some(truth) = truth(connection, column, left, *op, right)? {
            return Ok(Some(truth));
        }
    }
    let result = match expr {
        Expr::Binary(left, op @ (Operator::And | Operator::Or), right) => {
            match (term(connection, column, left)?, term(connection, column, right)?) {
                (Some(left), Some(right)) => Some(if *op == Operator::And {
                    left.and(right)
                } else {
                    ScopeTerm::Or(Box::new(left), Box::new(right))
                }),
                (Some(found), None) | (None, Some(found)) if *op == Operator::And => Some(found),
                _ => None,
            }
        }
        Expr::Binary(left, op, right) => {
            let (named, operand_expr, reverse) = if let Some(named) = column(left) {
                (named, right, false)
            } else if let Some(named) = column(right) {
                (named, left, true)
            } else {
                return Ok(None);
            };
            let (mut ordering, inclusive) = match op {
                Operator::Equals | Operator::Is => (Ordering::Equal, false),
                Operator::Less => (Ordering::Less, false),
                Operator::LessEquals => (Ordering::Less, true),
                Operator::Greater => (Ordering::Greater, false),
                Operator::GreaterEquals => (Ordering::Greater, true),
                _ => return Ok(None),
            };
            if reverse {
                ordering = ordering.reverse();
            }
            let (named, affinity) = named;
            operand(connection, operand_expr, affinity)?.map(|operand| {
                // `IS` a constant is an equality; `IS` a parameter is one or
                // a NULL test, by the run's value.
                if *op == Operator::Is && matches!(operand, Operand::Parameter(_)) {
                    ScopeTerm::Is { column: named, affinity, operand }
                } else {
                    ScopeTerm::Compare { column: named, affinity, operand, ordering, inclusive }
                }
            })
        }
        Expr::InList { lhs, not: false, rhs } => {
            let Some((named, affinity)) = column(lhs) else {
                return Ok(None);
            };
            let mut operands = Vec::new();
            for expr in rhs.iter().flat_map(|exprs| exprs.iter()) {
                let Some(operand) = operand(connection, expr, affinity)? else {
                    return Ok(None);
                };
                operands.push(operand);
            }
            Some(ScopeTerm::In { column: named, affinity, operands })
        }
        _ => None,
    };
    Ok(result)
}

/// The term `(condition) op value` gives, or `value op (condition)`, when
/// the condition is a NULL test or a column compared with a value, and the
/// other side is a constant or a parameter.
fn truth(
    connection: &Connection,
    column: &SourceColumn<'_>,
    left: &Expr<'_>,
    op: Operator,
    right: &Expr<'_>,
) -> rusqlite::Result<Option<ScopeTerm>> {
    let (condition_expr, value_expr, reversed) =
        match (condition(connection, column, left)?, condition(connection, column, right)?) {
            (Some(condition), None) => (condition, right, false),
            (None, Some(condition)) => (condition, left, true),
            _ => return Ok(None),
        };
    let (ordering, inclusive) = match op {
        Operator::Equals | Operator::Is => (Ordering::Equal, false),
        Operator::Less => (Ordering::Less, false),
        Operator::LessEquals => (Ordering::Less, true),
        Operator::Greater => (Ordering::Greater, false),
        _ => (Ordering::Greater, true),
    };
    let ordering = if reversed { ordering.reverse() } else { ordering };
    // The truth has no affinity, nor has the value: nothing converts.
    let operand = if matches!(crate::sql::unparenthesized(value_expr), Expr::Literal(Literal::Null))
    {
        Some(Operand::Constant(Value::Null))
    } else {
        operand(connection, value_expr, Affinity::Blob)?
    };
    Ok(operand.map(|operand| ScopeTerm::Truth {
        condition: condition_expr,
        ordering,
        inclusive,
        is: op == Operator::Is,
        operand,
    }))
}

/// The condition `expr` is, if it is one `Truth` holds.
fn condition(
    connection: &Connection,
    column: &SourceColumn<'_>,
    expr: &Expr<'_>,
) -> rusqlite::Result<Option<Truth>> {
    let expr = crate::sql::unparenthesized(expr);
    if let Some((tested, not)) = null_test(expr) {
        return Ok(
            column(through_collation(tested)).map(|(named, _)| Truth::Null { column: named, not })
        );
    }
    let Expr::Binary(left, op, right) = expr else {
        return Ok(None);
    };
    let (mut ordering, inclusive) = match op {
        Operator::Equals => (Ordering::Equal, false),
        Operator::Less => (Ordering::Less, false),
        Operator::LessEquals => (Ordering::Less, true),
        Operator::Greater => (Ordering::Greater, false),
        Operator::GreaterEquals => (Ordering::Greater, true),
        _ => return Ok(None),
    };
    let ((named, affinity), operand_expr) = if let Some(named) = column(left) {
        (named, *right)
    } else if let Some(named) = column(right) {
        ordering = ordering.reverse();
        (named, *left)
    } else {
        return Ok(None);
    };
    Ok(operand(connection, operand_expr, affinity)?.map(|operand| Truth::Compare {
        column: named,
        affinity,
        operand,
        ordering,
        inclusive,
    }))
}

/// The expression `expr` tests for NULL, and whether the test is `IS NOT
/// NULL`: `x IS NULL`, `x ISNULL`, `x IS NOT NULL`, `x NOTNULL` or `x NOT
/// NULL`.
pub(crate) fn null_test<'a>(expr: &'a Expr<'a>) -> Option<(&'a Expr<'a>, bool)> {
    let is_null =
        |expr: &Expr<'_>| matches!(crate::sql::unparenthesized(expr), Expr::Literal(Literal::Null));
    match expr {
        Expr::IsNull(tested) => Some((tested, false)),
        Expr::NotNull(tested) => Some((tested, true)),
        Expr::Binary(left, op @ (Operator::Is | Operator::IsNot), right) => {
            let not = *op == Operator::IsNot;
            if is_null(right) {
                Some((left, not))
            } else if is_null(left) {
                Some((right, not))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// `expr` without the COLLATEs on it: a collation doesn't change whether a
/// value is NULL.
fn through_collation<'a>(expr: &'a Expr<'a>) -> &'a Expr<'a> {
    match crate::sql::unparenthesized(expr) {
        Expr::Collate(inner, _) => through_collation(inner),
        expr => expr,
    }
}

/// The value `expr` stands for: a parameter, or a constant spelled in the
/// SQL whose type compares as it is with a column of `affinity`.
pub(crate) fn operand(
    connection: &Connection,
    expr: &Expr<'_>,
    affinity: Affinity,
) -> rusqlite::Result<Option<Operand>> {
    let expr = crate::sql::unparenthesized(expr);
    if let Expr::Variable(index) = expr {
        return Ok(Some(Operand::Parameter(
            index.parse::<usize>().expect("parameters were numbered before parsing") - 1,
        )));
    }
    match expr {
        Expr::Literal(Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_))
        | Expr::Unary(
            UnaryOperator::Negative | UnaryOperator::Positive,
            Expr::Literal(Literal::Numeric(_)),
        ) => {}
        _ => return Ok(None),
    }
    // Let SQLite interpret escapes, hex integers, exponents and i64::MIN.
    // Only AST-checked constants reach this evaluation, never app functions.
    let value =
        connection.query_row(&format!("SELECT {expr}"), [], |row| row.get::<_, Value>(0))?;
    Ok(affinity.compares(&value).then_some(Operand::Constant(value)))
}

#[cfg(test)]
#[path = "scope_term_tests.rs"]
mod tests;
