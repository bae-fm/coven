//! SQL over coven's stored schema: statements, triggers and tables as SQLite
//! recorded them, read for migration and removal.

use crate::schema_source::SchemaSource;
use crate::sql::{normalized, unparenthesized, Token};
use sqlite3_parser::{
    ast::{Cmd, Expr, Name, Operator, Stmt, TriggerTime, UnaryOperator},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

/// One statement of SQLite's stored schema, which SQLite has already parsed
/// and accepted, as sqlite3-parser reads it.
pub(crate) fn stored_statement<'a>(bump: &'a Bump, sql: &'a str) -> Stmt<'a> {
    let mut parser = Parser::new(bump, sql.as_bytes());
    let Cmd::Stmt(stmt) = parser
        .next()
        .unwrap_or_else(|e| panic!("sqlite3-parser rejected SQLite's accepted schema {sql:?}: {e}"))
        .expect("stored schema statement")
    else {
        panic!("stored schema contains EXPLAIN")
    };
    assert!(parser.next().expect("end of stored schema").is_none());
    stmt
}

/// SQL that holds when `a` and `b` are the same value: the same storage
/// class and the same bytes, as `Value` compares them.
pub(crate) fn same_value(a: &str, b: &str) -> String {
    format!("(typeof({a})=typeof({b}) AND {a} IS {b} COLLATE BINARY)")
}

/// Whether a CREATE TRIGGER runs AFTER its change. One that names no time
/// runs BEFORE it.
pub(crate) fn after_trigger(sql: &str) -> bool {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let command = parser.next();
    matches!(
        command,
        Ok(Some(Cmd::Stmt(Stmt::CreateTrigger { time: Some(TriggerTime::After), .. })))
    )
}

pub(crate) fn guarded_trigger(sql: &str) -> bool {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let command = parser.next();
    match command {
        Ok(Some(Cmd::Stmt(Stmt::CreateTrigger { when_clause: Some(expr), .. }))) => {
            guarded_predicate(&expr)
        }
        _ => false,
    }
}

fn guarded_predicate(expr: &Expr<'_>) -> bool {
    match unparenthesized(expr) {
        Expr::Binary(left, Operator::And, right) => {
            guarded_predicate(left) || guarded_predicate(right)
        }
        Expr::Unary(UnaryOperator::Not, call) => matches!(unparenthesized(call),
            Expr::FunctionCall { name, distinctness: None, args: None, order_by: None, filter_over: None }
                if !name.0.starts_with('\'') && Name(name.0) == "coven_applying"),
        _ => false,
    }
}

/// Table clauses and suffix, retaining constraints and column definitions for
/// migration comparison. SQLite has already parsed and accepted this SQL.
pub(crate) fn table_parts(sql: &str) -> Option<(Vec<Vec<Token>>, Vec<Token>)> {
    let mut source = SchemaSource::new(sql);
    let body = source.try_body()?;
    Some((body.parts().iter().map(normalized).collect(), normalized(&source)))
}

#[cfg(test)]
#[path = "schema_sql_tests.rs"]
mod tests;
