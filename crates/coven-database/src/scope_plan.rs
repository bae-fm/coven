//! What one app statement's SQL lets a live query narrow, read from SQLite's
//! parsed SELECT syntax once per statement and schema (§4). A shape this
//! doesn't understand depends on whole tables rather than guessing.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::row_scope::RowScope;
use crate::sql_value::SqlValue as Value;
use rusqlite::{Connection, Statement};
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, FunctionCallOrder, FunctionTail, Literal, Name, OneSelect, Operator, Over,
        ResultColumn, Select, SelectTable, Stmt, UnaryOperator, Window,
    },
    lexer::{
        sql::{Parser, TokenType, Tokenizer},
        Scanner,
    },
    Bump, FallibleIterator,
};

/// The rows of each table a statement can depend on, before its values are
/// bound.
pub(crate) struct ScopePlan {
    /// The one table a single-table statement reads, lowercase, with the
    /// conditions its WHERE puts on that table's rows; `None` when it has
    /// none coven can use.
    narrowed: Option<(String, Term)>,
}

impl ScopePlan {
    /// The plan for `sql`, prepared as `statement`.
    pub(crate) fn build(
        connection: &Connection,
        sql: &str,
        statement: &Statement<'_>,
    ) -> rusqlite::Result<Self> {
        Ok(Self {
            narrowed: narrowed(connection, sql, statement)?,
        })
    }

    /// The scope of `table`, lowercase, for a run bound to `parameters`,
    /// when `table` is the only one the statement read.
    pub(crate) fn scope(&self, table: &str, parameters: &[Value]) -> RowScope {
        match &self.narrowed {
            Some((narrowed, term)) if narrowed == table => {
                term.bind(parameters).unwrap_or(RowScope::All)
            }
            _ => RowScope::All,
        }
    }
}

/// A condition on one table's rows, with values from the SQL or its
/// parameters.
enum Term {
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

impl Term {
    /// The scope this run's values give, or `None` when a value would need a
    /// conversion SQLite applies and coven doesn't.
    fn bind(&self, parameters: &[Value]) -> Option<RowScope> {
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
                    .then(|| RowScope::In {
                        column: column.clone(),
                        values,
                    })
            }
            Self::And(left, right) => match (left.bind(parameters), right.bind(parameters)) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (scope, None) | (None, scope) => scope,
            },
            Self::Or(left, right) => Some(left.bind(parameters)?.or(right.bind(parameters)?)),
        }
    }
}

/// A value in a condition: one the SQL spells, or a bound parameter.
enum Operand {
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

/// A column a condition can narrow by: its affinity, or `None` when it can't
/// be compared outside SQLite, as a generated column or one with a collation
/// other than BINARY.
type Columns = BTreeMap<String, Option<Affinity>>;

fn narrowed(
    connection: &Connection,
    sql: &str,
    statement: &Statement<'_>,
) -> rusqlite::Result<Option<(String, Term)>> {
    let Some(sql) = number_parameters(sql, statement)? else {
        return Ok(None);
    };
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    // SQLite and the parser can accept different language versions. A SQL
    // form this parser cannot represent depends on whole tables.
    let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
        return Ok(None);
    };
    if !matches!(parser.next(), Ok(None))
        || select.with.is_some()
        || select.body.compounds.is_some()
        || nests(select)
    {
        return Ok(None);
    }
    let OneSelect::Select {
        from: Some(from),
        where_clause: Some(predicate),
        ..
    } = &select.body.select
    else {
        return Ok(None);
    };
    let Some(SelectTable::Table(name, alias, _)) = from.select else {
        return Ok(None);
    };
    if from.joins.is_some() || name.db_name.as_ref().is_some_and(|name| name != "main") {
        return Ok(None);
    }
    let Some((table, columns)) = table_columns(connection, &name.name)? else {
        return Ok(None);
    };
    let context = PredicateContext {
        connection,
        table: &table,
        alias: alias.as_ref().map(|alias| match alias {
            As::As(name) | As::Elided(name) => name.0.to_owned(),
        }),
        columns,
    };
    Ok(context.predicate(predicate)?.map(|term| (table, term)))
}

/// An ordinary table's name, lowercase, and its columns.
fn table_columns(
    connection: &Connection,
    name: &Name<'_>,
) -> rusqlite::Result<Option<(String, Columns)>> {
    let mut tables = connection.prepare_cached(
        "SELECT name FROM pragma_table_list WHERE schema='main' AND type='table'",
    )?;
    let table = tables
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .find(|table| *name == table.as_str());
    let Some(table) = table else {
        return Ok(None);
    };
    let mut metadata =
        connection.prepare_cached("SELECT name,type,hidden FROM pragma_table_xinfo(?1,'main')")?;
    let declared = metadata
        .query_map([&table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut columns = Columns::new();
    for (column, declared, hidden) in declared {
        let (_, collation, _, _, _) =
            connection.column_metadata(Some("main"), &table, column.as_str())?;
        let binary =
            collation.is_some_and(|collation| collation.to_bytes().eq_ignore_ascii_case(b"BINARY"));
        columns.insert(
            column.to_ascii_lowercase(),
            (binary && !hidden).then(|| Affinity::declared(&declared)),
        );
    }
    Ok(Some((table.to_ascii_lowercase(), columns)))
}

struct PredicateContext<'a> {
    connection: &'a Connection,
    table: &'a str,
    alias: Option<String>,
    columns: Columns,
}

impl PredicateContext<'_> {
    fn predicate(&self, expr: &Expr<'_>) -> rusqlite::Result<Option<Term>> {
        let result = match expr {
            Expr::Parenthesized(exprs) if exprs.len() == 1 => return self.predicate(&exprs[0]),
            Expr::Binary(left, op @ (Operator::And | Operator::Or), right) => {
                match (self.predicate(left)?, self.predicate(right)?) {
                    (Some(left), Some(right)) => Some(if *op == Operator::And {
                        Term::And(Box::new(left), Box::new(right))
                    } else {
                        Term::Or(Box::new(left), Box::new(right))
                    }),
                    (Some(term), None) | (None, Some(term)) if *op == Operator::And => Some(term),
                    _ => None,
                }
            }
            Expr::Binary(left, op, right) => {
                let (column, operand, reverse) = if let Some(column) = self.column(left) {
                    (column, right, false)
                } else if let Some(column) = self.column(right) {
                    (column, left, true)
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
                let (column, affinity) = column;
                self.operand(operand, affinity)?
                    .map(|operand| Term::Compare {
                        column,
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
                let Some((column, affinity)) = self.column(lhs) else {
                    return Ok(None);
                };
                let mut operands = Vec::new();
                for expr in rhs.iter().flat_map(|exprs| exprs.iter()) {
                    let Some(operand) = self.operand(expr, affinity)? else {
                        return Ok(None);
                    };
                    operands.push(operand);
                }
                Some(Term::In {
                    column,
                    affinity,
                    operands,
                })
            }
            _ => None,
        };
        Ok(result)
    }

    /// The column `expr` names, lowercase, and its affinity, when a
    /// condition on it can narrow.
    fn column(&self, expr: &Expr<'_>) -> Option<(String, Affinity)> {
        let qualifier = |name: &Name<'_>| {
            *name == self.table
                || self
                    .alias
                    .as_ref()
                    .is_some_and(|alias| *name == Name(alias))
        };
        let name = match expr {
            Expr::Id(name) => Name(name.0),
            Expr::Name(name) => name.clone(),
            Expr::Qualified(table, name) if qualifier(table) => name.clone(),
            Expr::DoublyQualified(db, table, name) if *db == "main" && qualifier(table) => {
                name.clone()
            }
            Expr::Parenthesized(exprs) if exprs.len() == 1 => return self.column(&exprs[0]),
            _ => return None,
        };
        self.columns
            .iter()
            .find(|(column, _)| name == column.as_str())
            .and_then(|(column, affinity)| Some((column.clone(), (*affinity)?)))
    }

    /// The value `expr` stands for: a parameter, or a constant spelled in the
    /// SQL whose type compares as it is with a column of `affinity`.
    fn operand(&self, expr: &Expr<'_>, affinity: Affinity) -> rusqlite::Result<Option<Operand>> {
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
                return self.operand(&exprs[0], affinity)
            }
            _ => return Ok(None),
        }
        // Let SQLite interpret escapes, hex integers, exponents and i64::MIN.
        // Only AST-checked constants reach this evaluation, never app functions.
        let value = self
            .connection
            .query_row(&format!("SELECT {expr}"), [], |row| row.get::<_, Value>(0))?;
        Ok(affinity
            .compares(&value)
            .then_some(Operand::Constant(value)))
    }
}

// Number every parameter in source order, before interpreting any predicate.
// The parser's AST normalizes LIMIT offset,count into count/OFFSET, so use its
// lexer to preserve source order. Only variables are rewritten; conditions are
// recognized from the parsed SELECT and expression nodes, never token matching.
fn number_parameters(sql: &str, statement: &Statement<'_>) -> rusqlite::Result<Option<String>> {
    let mut scanner = Scanner::new(Tokenizer::new());
    let mut next = 1;
    let mut end_of_previous = 0;
    let mut numbered = String::new();
    loop {
        let Ok((start, token, end)) = scanner.scan(sql.as_bytes()) else {
            return Ok(None);
        };
        let Some((_, kind)) = token else { break };
        if kind != TokenType::TK_VARIABLE {
            continue;
        }
        let name = &sql[start..end];
        let index = if name == "?" {
            next
        } else if let Some(digits) = name.strip_prefix('?') {
            digits
                .parse::<usize>()
                .expect("SQLite accepted this parameter index")
        } else {
            statement
                .parameter_index(name)?
                .expect("parsed parameter belongs to prepared statement")
        };
        next = next.max(index + 1);
        numbered.push_str(&sql[end_of_previous..start]);
        numbered.push_str(&format!("?{index}"));
        end_of_previous = end;
    }
    numbered.push_str(&sql[end_of_previous..]);
    Ok(Some(numbered))
}

/// Whether the SELECT holds another SELECT, or reads a table through `IN`,
/// anywhere: its rows aren't the outer statement's.
fn nests(select: &Select<'_>) -> bool {
    let OneSelect::Select {
        columns,
        from,
        where_clause,
        group_by,
        having,
        window_clause,
        ..
    } = &select.body.select
    else {
        return true;
    };
    let from_nests = from.as_ref().is_some_and(|from| {
        from.select
            .iter()
            .copied()
            .chain(from.joins.iter().flatten().map(|join| &join.table))
            .any(|table| !matches!(table, SelectTable::Table(..)))
            || from.joins.iter().flatten().any(|join| {
                matches!(&join.constraint, Some(sqlite3_parser::ast::JoinConstraint::On(expr)) if nested(expr))
            })
    });
    from_nests
        || columns
            .iter()
            .any(|column| matches!(column, ResultColumn::Expr(expr, _) if nested(expr)))
        || where_clause.is_some_and(nested)
        || group_by.is_some_and(|exprs| exprs.iter().any(nested))
        || having.is_some_and(nested)
        || window_clause.is_some_and(|windows| windows.iter().any(|def| window_nests(&def.window)))
        || select
            .order_by
            .is_some_and(|terms| terms.iter().any(|term| nested(&term.expr)))
        || select
            .limit
            .is_some_and(|limit| nested(&limit.expr) || limit.offset.as_ref().is_some_and(nested))
}

fn window_nests(window: &Window<'_>) -> bool {
    window
        .partition_by
        .is_some_and(|exprs| exprs.iter().any(nested))
        || window
            .order_by
            .is_some_and(|terms| terms.iter().any(|term| nested(&term.expr)))
        || window.frame_clause.as_ref().is_some_and(|frame| {
            [Some(&frame.start), frame.end.as_ref()]
                .into_iter()
                .flatten()
                .any(|bound| match bound {
                    sqlite3_parser::ast::FrameBound::Following(expr)
                    | sqlite3_parser::ast::FrameBound::Preceding(expr) => nested(expr),
                    _ => false,
                })
        })
}

fn tail_nests(tail: &FunctionTail<'_>) -> bool {
    tail.filter_clause.is_some_and(nested)
        || tail.over_clause.is_some_and(|over| match over {
            Over::Window(window) => window_nests(window),
            Over::Name(_) => false,
        })
}

/// Whether `expr` holds a SELECT or reads a table through `IN`.
fn nested(expr: &Expr<'_>) -> bool {
    match expr {
        Expr::Exists(_) | Expr::InSelect { .. } | Expr::Subquery(_) | Expr::InTable { .. } => true,
        Expr::Between {
            lhs, start, end, ..
        } => nested(lhs) || nested(start) || nested(end),
        Expr::Binary(left, _, right) => nested(left) || nested(right),
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            base.is_some_and(nested)
                || when_then_pairs
                    .iter()
                    .any(|(when, then)| nested(when) || nested(then))
                || else_expr.is_some_and(nested)
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => nested(expr),
        Expr::FunctionCall {
            args,
            order_by,
            filter_over,
            ..
        } => {
            args.is_some_and(|args| args.iter().any(nested))
                || order_by.as_ref().is_some_and(|order| match order {
                    FunctionCallOrder::SortList(terms) => {
                        terms.iter().any(|term| nested(&term.expr))
                    }
                })
                || filter_over.as_ref().is_some_and(tail_nests)
        }
        Expr::FunctionCallStar { filter_over, .. } => filter_over.as_ref().is_some_and(tail_nests),
        Expr::InList { lhs, rhs, .. } => {
            nested(lhs) || rhs.is_some_and(|exprs| exprs.iter().any(nested))
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => nested(lhs) || nested(rhs) || escape.is_some_and(nested),
        Expr::Parenthesized(exprs) => exprs.iter().any(nested),
        Expr::Raise(_, expr) => expr.is_some_and(nested),
        Expr::Variable(_)
        | Expr::DoublyQualified(..)
        | Expr::Id(_)
        | Expr::Literal(_)
        | Expr::Name(_)
        | Expr::Qualified(..) => false,
    }
}

#[cfg(test)]
#[path = "scope_plan_tests.rs"]
mod tests;
