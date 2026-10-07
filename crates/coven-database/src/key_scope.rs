//! Key predicates from SQLite's parsed SELECT syntax. Unsupported expressions
//! retain whole-table dependencies rather than guessing at their meaning.

use std::collections::BTreeMap;

use crate::sql_value::SqlValue as Value;
use rusqlite::{Connection, Statement};
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, Literal, Name, OneSelect, Operator, ResultColumn, SelectTable, Stmt,
        UnaryOperator,
    },
    lexer::{
        sql::{Parser, TokenType, Tokenizer},
        Scanner,
    },
    Bump, FallibleIterator,
};

#[derive(Clone)]
pub(crate) enum KeyScope {
    All,
    Compare {
        column: String,
        value: Value,
        ordering: std::cmp::Ordering,
        inclusive: bool,
    },
    In {
        column: String,
        values: Vec<Value>,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl KeyScope {
    pub(crate) fn matches(&self, key: &BTreeMap<String, Value>) -> bool {
        match self {
            Self::All => true,
            Self::Compare {
                column,
                value,
                ordering,
                inclusive,
            } => {
                let actual = key.get(column).expect("observed primary key column");
                compare(actual, value)
                    .is_some_and(|found| found == *ordering || (*inclusive && found.is_eq()))
            }
            Self::In { column, values } => {
                let actual = key.get(column).expect("observed primary key column");
                values
                    .iter()
                    .any(|value| compare(actual, value).is_some_and(|order| order.is_eq()))
            }
            Self::And(left, right) => left.matches(key) && right.matches(key),
            Self::Or(left, right) => left.matches(key) || right.matches(key),
        }
    }

    pub(crate) fn for_statement(
        connection: &Connection,
        sql: &str,
        table: &str,
        statement: &Statement<'_>,
        parameters: &[Value],
    ) -> rusqlite::Result<Self> {
        let Some(sql) = number_parameters(sql, statement)? else {
            return Ok(Self::All);
        };
        let bump = Bump::new();
        let mut parser = Parser::new(&bump, sql.as_bytes());
        // SQLite and the parser can accept different language versions. A SQL
        // form this parser cannot represent is explicitly a whole-table read.
        let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
            return Ok(Self::All);
        };
        if !matches!(parser.next(), Ok(None))
            || select.with.is_some()
            || select.body.compounds.is_some()
        {
            return Ok(Self::All);
        }
        let OneSelect::Select {
            columns,
            from: Some(from),
            where_clause: Some(predicate),
            group_by,
            having,
            window_clause,
            ..
        } = &select.body.select
        else {
            return Ok(Self::All);
        };
        if from.joins.is_some()
            || window_clause.is_some()
            || columns.iter().any(
                |column| matches!(column, ResultColumn::Expr(expr, _) if !row_expression(expr)),
            )
            || !row_expression(predicate)
            || group_by.is_some_and(|exprs| !exprs.iter().all(|expr| row_expression(expr)))
            || having.is_some_and(|expr| !row_expression(expr))
            || select
                .order_by
                .is_some_and(|terms| terms.iter().any(|term| !row_expression(&term.expr)))
            || select.limit.is_some_and(|limit| {
                !row_expression(&limit.expr)
                    || limit
                        .offset
                        .as_ref()
                        .is_some_and(|expr| !row_expression(expr))
            })
        {
            return Ok(Self::All);
        }
        let Some(SelectTable::Table(name, alias, _)) = from.select else {
            return Ok(Self::All);
        };
        if name.name != table || name.db_name.as_ref().is_some_and(|name| name != "main") {
            return Ok(Self::All);
        }
        let ordinary: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_list WHERE schema='main' AND name=?1 COLLATE NOCASE AND type='table')", [table], |row| row.get(0))?;
        if !ordinary {
            return Ok(Self::All);
        }
        let mut metadata = connection.prepare(
            "SELECT name,type FROM pragma_table_xinfo(?1,'main') WHERE pk>0 ORDER BY pk",
        )?;
        let key_columns = metadata
            .query_map([table], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut keys = BTreeMap::new();
        for (column, declared) in key_columns {
            let (_, collation, _, _, _) =
                connection.column_metadata(Some("main"), table, column.as_str())?;
            if !collation
                .is_some_and(|collation| collation.to_bytes().eq_ignore_ascii_case(b"BINARY"))
            {
                return Ok(Self::All);
            }
            let affinity = match declared.to_ascii_uppercase().as_str() {
                "INTEGER" => Affinity::Number,
                "TEXT" => Affinity::Text,
                "BLOB" | "" => Affinity::Blob,
                _ => return Ok(Self::All),
            };
            keys.insert(column.to_ascii_lowercase(), affinity);
        }
        let alias = alias.as_ref().map(|alias| match alias {
            As::As(name) | As::Elided(name) => name.0.to_owned(),
        });
        let context = PredicateContext {
            connection,
            table,
            alias,
            keys,
            parameters,
        };
        Ok(match context.predicate(predicate)? {
            Some(scope) => scope,
            None => Self::All,
        })
    }
}

#[derive(Clone, Copy)]
enum Affinity {
    Number,
    Text,
    Blob,
}

struct PredicateContext<'a> {
    connection: &'a Connection,
    table: &'a str,
    alias: Option<String>,
    keys: BTreeMap<String, Affinity>,
    parameters: &'a [Value],
}

impl PredicateContext<'_> {
    fn predicate(&self, expr: &Expr<'_>) -> rusqlite::Result<Option<KeyScope>> {
        let result = match expr {
            Expr::Parenthesized(exprs) if exprs.len() == 1 => return self.predicate(&exprs[0]),
            Expr::Binary(left, op @ (Operator::And | Operator::Or), right) => {
                match (self.predicate(left)?, self.predicate(right)?) {
                    (Some(left), Some(right)) => Some(if *op == Operator::And {
                        KeyScope::And(Box::new(left), Box::new(right))
                    } else {
                        KeyScope::Or(Box::new(left), Box::new(right))
                    }),
                    (Some(scope), None) | (None, Some(scope)) if *op == Operator::And => {
                        Some(scope)
                    }
                    _ => None,
                }
            }
            Expr::Binary(left, op, right) => {
                let (column, literal, reverse) = if let Some(column) = self.column(left) {
                    (column, right, false)
                } else if let Some(column) = self.column(right) {
                    (column, left, true)
                } else {
                    return Ok(None);
                };
                let (mut ordering, inclusive) = match op {
                    Operator::Equals => (std::cmp::Ordering::Equal, false),
                    Operator::Less => (std::cmp::Ordering::Less, false),
                    Operator::LessEquals => (std::cmp::Ordering::Less, true),
                    Operator::Greater => (std::cmp::Ordering::Greater, false),
                    Operator::GreaterEquals => (std::cmp::Ordering::Greater, true),
                    _ => return Ok(None),
                };
                if reverse {
                    ordering = ordering.reverse();
                }
                self.literal(literal, self.keys[&column])?
                    .map(|value| KeyScope::Compare {
                        column,
                        value,
                        ordering,
                        inclusive,
                    })
            }
            Expr::InList {
                lhs,
                not: false,
                rhs,
            } => {
                let Some(column) = self.column(lhs) else {
                    return Ok(None);
                };
                let mut values = Vec::new();
                if let Some(expressions) = rhs {
                    for expr in *expressions {
                        let Some(value) = self.literal(expr, self.keys[&column])? else {
                            return Ok(None);
                        };
                        values.push(value);
                    }
                }
                Some(KeyScope::In { column, values })
            }
            _ => None,
        };
        Ok(result)
    }

    fn column(&self, expr: &Expr<'_>) -> Option<String> {
        let qualifier = |name: &Name<'_>| {
            name == self.table
                || self
                    .alias
                    .as_ref()
                    .is_some_and(|alias| *name == Name(alias))
        };
        let name = match expr {
            Expr::Id(name) => Name(name.0),
            Expr::Name(name) => name.clone(),
            Expr::Qualified(table, name) if qualifier(table) => name.clone(),
            Expr::DoublyQualified(db, table, name) if db == "main" && qualifier(table) => {
                name.clone()
            }
            Expr::Parenthesized(exprs) if exprs.len() == 1 => return self.column(&exprs[0]),
            _ => return None,
        };
        self.keys.keys().find(|key| name == key.as_str()).cloned()
    }

    fn literal(&self, expr: &Expr<'_>, affinity: Affinity) -> rusqlite::Result<Option<Value>> {
        let value = if let Expr::Variable(index) = expr {
            self.parameters[index
                .parse::<usize>()
                .expect("parameters were numbered before parsing")
                - 1]
            .clone()
        } else {
            match expr {
                Expr::Literal(Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_))
                | Expr::Unary(
                    UnaryOperator::Negative | UnaryOperator::Positive,
                    Expr::Literal(Literal::Numeric(_)),
                ) => {}
                Expr::Parenthesized(exprs) if exprs.len() == 1 => {
                    return self.literal(&exprs[0], affinity)
                }
                _ => return Ok(None),
            }
            // Let SQLite interpret escapes, hex integers, exponents and i64::MIN.
            // Only AST-checked constants reach this evaluation, never app functions.
            self.connection
                .query_row(&format!("SELECT {expr}"), [], |row| row.get::<_, Value>(0))?
        };
        Ok(match (&value, affinity) {
            (Value::Integer(_) | Value::Real(_), Affinity::Number)
            | (Value::Text(_), Affinity::Text)
            | (Value::Blob(_), Affinity::Blob) => Some(value),
            _ => None, // Other affinities require SQLite coercion: whole-table read.
        })
    }
}

// Number every parameter in source order, before interpreting any predicate.
// The parser's AST normalizes LIMIT offset,count into count/OFFSET, so use its
// lexer to preserve source order. Only variables are rewritten; key recognition
// below uses the parsed SELECT and expression nodes, never token matching.
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

fn compare(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
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

fn row_expression(expr: &Expr<'_>) -> bool {
    match expr {
        Expr::Exists(_)
        | Expr::InSelect { .. }
        | Expr::Subquery(_)
        | Expr::InTable { .. }
        | Expr::FunctionCall { .. }
        | Expr::FunctionCallStar { .. }
        | Expr::Raise(..) => false,
        Expr::Between {
            lhs, start, end, ..
        } => row_expression(lhs) && row_expression(start) && row_expression(end),
        Expr::Binary(left, _, right) => row_expression(left) && row_expression(right),
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            base.is_none_or(|expr| row_expression(expr))
                && when_then_pairs
                    .iter()
                    .all(|(a, b)| row_expression(a) && row_expression(b))
                && else_expr.is_none_or(|expr| row_expression(expr))
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr) => row_expression(expr),
        Expr::InList { lhs, rhs, .. } => {
            row_expression(lhs)
                && rhs.is_none_or(|exprs| exprs.iter().all(|expr| row_expression(expr)))
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            row_expression(lhs)
                && row_expression(rhs)
                && escape.is_none_or(|expr| row_expression(expr))
        }
        Expr::Parenthesized(exprs) => exprs.iter().all(|expr| row_expression(expr)),
        Expr::Variable(_)
        | Expr::DoublyQualified(..)
        | Expr::Id(_)
        | Expr::Literal(_)
        | Expr::Name(_)
        | Expr::Qualified(..) => true,
    }
}

#[cfg(test)]
#[path = "key_scope_tests.rs"]
mod tests;
