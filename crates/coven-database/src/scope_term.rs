//! A statement's conditions on one source's rows, read from its parsed SQL
//! and bound to each run's values (§4).

use std::cmp::Ordering;

use crate::row_scope::RowScope;
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Literal, Operator, UnaryOperator};

/// A condition on one source's rows, with values from the SQL or its
/// parameters.
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
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl ScopeTerm {
    /// The scope this run's values give, or `None` when a value would need a
    /// conversion SQLite applies and coven doesn't.
    pub(crate) fn bind(&self, parameters: &[Value]) -> Option<RowScope> {
        match self {
            Self::Compare {
                column,
                affinity,
                operand,
                ordering,
                inclusive,
            } => {
                let value = operand.value(parameters);
                affinity.compares(value).then(|| RowScope::Compare {
                    column: column.clone(),
                    value: value.clone(),
                    ordering: *ordering,
                    inclusive: *inclusive,
                })
            }
            Self::In {
                column,
                affinity,
                operands,
            } => {
                let values: Vec<Value> = operands
                    .iter()
                    .map(|operand| operand.value(parameters).clone())
                    .collect();
                values
                    .iter()
                    .all(|value| affinity.compares(value))
                    .then(|| RowScope::equal(column.clone(), values))
            }
            Self::And(left, right) => match (left.bind(parameters), right.bind(parameters)) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (scope, None) | (None, scope) => scope,
            },
            Self::Or(left, right) => Some(left.bind(parameters)?.or(right.bind(parameters)?)),
        }
    }

    /// Both terms.
    pub(crate) fn and(self, other: Self) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }
}

/// A value in a condition: one the SQL spells, or a bound parameter.
pub(crate) enum Operand {
    Constant(Value),
    /// A parameter's position, from zero.
    Parameter(usize),
}

impl Operand {
    fn value<'a>(&'a self, parameters: &'a [Value]) -> &'a Value {
        match self {
            Self::Constant(value) => value,
            Self::Parameter(index) => &parameters[*index],
        }
    }
}

/// A column's affinity, as SQLite derives it from the declared type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Affinity {
    /// INTEGER, REAL or NUMERIC: they compare their values alike.
    Numeric,
    Text,
    /// BLOB, or no declared type: SQLite converts nothing.
    Blob,
}

impl Affinity {
    /// SQLite's rules for a declared column type (datatype3 §3.1).
    pub(crate) fn declared(declared: &str) -> Self {
        let declared = declared.to_ascii_uppercase();
        if declared.contains("INT") {
            Self::Numeric
        } else if ["CHAR", "CLOB", "TEXT"]
            .iter()
            .any(|word| declared.contains(word))
        {
            Self::Text
        } else if declared.contains("BLOB") || declared.is_empty() {
            Self::Blob
        } else {
            Self::Numeric
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
    let result = match expr {
        Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            return term(connection, column, &exprs[0])
        }
        Expr::Binary(left, op @ (Operator::And | Operator::Or), right) => {
            match (
                term(connection, column, left)?,
                term(connection, column, right)?,
            ) {
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
                Operator::Equals => (Ordering::Equal, false),
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
            operand(connection, operand_expr, affinity)?.map(|operand| ScopeTerm::Compare {
                column: named,
                affinity,
                operand,
                ordering,
                inclusive,
            })
        }
        Expr::InList {
            lhs,
            not: false,
            rhs,
        } => {
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
            Some(ScopeTerm::In {
                column: named,
                affinity,
                operands,
            })
        }
        _ => None,
    };
    Ok(result)
}

/// The value `expr` stands for: a parameter, or a constant spelled in the
/// SQL whose type compares as it is with a column of `affinity`.
fn operand(
    connection: &Connection,
    expr: &Expr<'_>,
    affinity: Affinity,
) -> rusqlite::Result<Option<Operand>> {
    if let Expr::Variable(index) = expr {
        return Ok(Some(Operand::Parameter(
            index
                .parse::<usize>()
                .expect("parameters were numbered before parsing")
                - 1,
        )));
    }
    match expr {
        Expr::Literal(Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_))
        | Expr::Unary(
            UnaryOperator::Negative | UnaryOperator::Positive,
            Expr::Literal(Literal::Numeric(_)),
        ) => {}
        Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            return operand(connection, &exprs[0], affinity)
        }
        _ => return Ok(None),
    }
    // Let SQLite interpret escapes, hex integers, exponents and i64::MIN.
    // Only AST-checked constants reach this evaluation, never app functions.
    let value =
        connection.query_row(&format!("SELECT {expr}"), [], |row| row.get::<_, Value>(0))?;
    Ok(affinity
        .compares(&value)
        .then_some(Operand::Constant(value)))
}
