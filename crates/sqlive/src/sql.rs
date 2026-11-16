//! SQLite tokens used to inspect CREATE statements without matching comments or strings.

use sqlite3_parser::ast::Expr;

pub use crate::token_scan::scan;

/// A token of SQL text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Token {
    /// An unquoted word, with ASCII letters lowercased.
    Word(String),
    /// A name in double quotes, backticks or brackets, without the quotes.
    Quoted(String),
    /// A string in single quotes, without the quotes.
    String(String),
    /// A punctuation character.
    Symbol(char),
}

impl Token {
    /// Whether this is the unquoted word `word`, in any letter case.
    pub fn word(&self, word: &str) -> bool {
        matches!(self, Self::Word(value) if value.eq_ignore_ascii_case(word))
    }
}

/// The tokens of `sql`, without its comments and whitespace.
pub fn tokens(sql: &str) -> rusqlite::Result<Vec<Token>> {
    let scanned = crate::token_scan::scan(sql)?;
    Ok(normalized(scanned.iter().map(|(_, range)| &sql[range.clone()])))
}

/// The tokens that the texts `scanned` hold. Quoted contents keep their case,
/// unquoted words fold ASCII case, and numeric and operator punctuation stay
/// separate.
fn normalized<'a>(scanned: impl Iterator<Item = &'a str>) -> Vec<Token> {
    let mut tokens = Vec::new();
    for mut text in scanned {
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
pub fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A name the parser read, without the quotes SQLite allows around it:
/// double quotes, backticks, brackets, or single quotes where SQLite reads
/// a string as a name.
pub fn name(raw: &str) -> String {
    match raw.as_bytes()[0] {
        b'[' => raw[1..raw.len() - 1].to_owned(),
        quote @ (b'\'' | b'"' | b'`') => raw[1..raw.len() - 1]
            .replace(&format!("{0}{0}", quote as char), &(quote as char).to_string()),
        _ => raw.to_owned(),
    }
}

/// `expr` without the parentheses around it, each holding one expression.
pub fn unparenthesized<'a, 'b>(mut expr: &'a Expr<'b>) -> &'a Expr<'b> {
    while let Expr::Parenthesized(items) = expr {
        let [item] = items.as_slice() else { break };
        expr = item;
    }
    expr
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;
