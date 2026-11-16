//! SQLite tokens used to inspect CREATE statements without matching comments or strings.

use crate::schema_source::SchemaSource;
use sqlite3_parser::ast::Expr;

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
pub(crate) fn normalized(source: &SchemaSource<'_>) -> Vec<Token> {
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

/// Quote an identifier, never a value or SQL fragment.
pub(crate) fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A name the parser read, without the quotes SQLite allows around it:
/// double quotes, backticks, brackets, or single quotes where SQLite reads
/// a string as a name.
pub(crate) fn name(raw: &str) -> String {
    match raw.as_bytes()[0] {
        b'[' => raw[1..raw.len() - 1].to_owned(),
        quote @ (b'\'' | b'"' | b'`') => raw[1..raw.len() - 1]
            .replace(&format!("{0}{0}", quote as char), &(quote as char).to_string()),
        _ => raw.to_owned(),
    }
}

/// `expr` without the parentheses around it, each holding one expression.
pub(crate) fn unparenthesized<'a, 'b>(mut expr: &'a Expr<'b>) -> &'a Expr<'b> {
    while let Expr::Parenthesized(items) = expr {
        let [item] = items.as_slice() else { break };
        expr = item;
    }
    expr
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;
