//! SQLite tokens used to inspect CREATE statements without matching comments or strings.

use crate::schema_source::SchemaSource;
use sqlite3_parser::{
    ast::{Cmd, Expr, Name, Operator, Stmt, TriggerTime, UnaryOperator},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Token {
    Word(String),
    Quoted(String),
    String(String),
    Symbol(char),
}

impl Token {
    pub(crate) fn word(&self, word: &str) -> bool {
        matches!(self, Self::Word(value) if value.eq_ignore_ascii_case(word))
    }
}

pub(crate) fn tokens(sql: &str) -> rusqlite::Result<Vec<Token>> {
    Ok(normalized(&SchemaSource::try_new(sql)?))
}

// Keep migration comparison's spelling rules: quote contents retain case,
// unquoted words fold ASCII case, and numeric/operator punctuation stays separate.
fn normalized(source: &SchemaSource<'_>) -> Vec<Token> {
    let mut tokens = Vec::new();
    for mut text in source.tokens() {
        if text.starts_with(['x', 'X']) && text.as_bytes().get(1) == Some(&b'\'') {
            tokens.push(Token::Word("x".into()));
            text = &text[1..];
        }
        match text.as_bytes()[0] {
            quote @ (b'\'' | b'"' | b'`' | b'[') => {
                let value = name(text);
                tokens.push(if quote == b'\'' {
                    Token::String(value)
                } else {
                    Token::Quoted(value)
                });
            }
            _ => {
                let mut word = String::new();
                for ch in text.chars().chain(std::iter::once(' ')) {
                    if ch.is_alphanumeric() || ch == '_' || ch == '$' {
                        word.push(ch.to_ascii_lowercase());
                    } else {
                        if !word.is_empty() {
                            tokens.push(Token::Word(std::mem::take(&mut word)));
                        }
                        if !ch.is_whitespace() {
                            tokens.push(Token::Symbol(ch));
                        }
                    }
                }
            }
        }
    }
    tokens
}

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

/// Quote an identifier, never a value or SQL fragment.
pub(crate) fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn name(raw: &str) -> String {
    match raw.as_bytes()[0] {
        b'[' => raw[1..raw.len() - 1].to_owned(),
        quote @ (b'\'' | b'"' | b'`') => raw[1..raw.len() - 1].replace(
            &format!("{0}{0}", quote as char),
            &(quote as char).to_string(),
        ),
        _ => raw.to_owned(),
    }
}

/// Whether a CREATE TRIGGER runs AFTER its change. One that names no time
/// runs BEFORE it.
pub(crate) fn after_trigger(sql: &str) -> bool {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let command = parser.next();
    matches!(
        command,
        Ok(Some(Cmd::Stmt(Stmt::CreateTrigger {
            time: Some(TriggerTime::After),
            ..
        })))
    )
}

pub(crate) fn guarded_trigger(sql: &str) -> bool {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let command = parser.next();
    match command {
        Ok(Some(Cmd::Stmt(Stmt::CreateTrigger {
            when_clause: Some(expr),
            ..
        }))) => guarded_predicate(&expr),
        _ => false,
    }
}

fn unparenthesized<'a, 'b>(mut expr: &'a Expr<'b>) -> &'a Expr<'b> {
    while let Expr::Parenthesized(items) = expr {
        let [item] = items.as_slice() else { break };
        expr = item;
    }
    expr
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
    Some((
        body.parts().iter().map(normalized).collect(),
        normalized(&source),
    ))
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;
