//! The structure of a CREATE statement's text: its parenthesized body and the
//! comma-separated parts of it, each with the spelling the app wrote.
//! sqlite3-parser's AST has no spans, so the token ranges from sqlive's scan
//! supply them.

use sqlite3_parser::ast::fmt::{ToTokens, TokenStream};
use sqlite3_parser::lexer::sql::TokenType;
use std::ops::Range;
use TokenType::{TK_COMMA, TK_LP, TK_RP};

/// The tokens of a CREATE statement's text, each with its range in that text.
pub(crate) struct SchemaSource<'a> {
    sql: &'a str,
    tokens: Vec<(TokenType, Range<usize>)>,
}

impl<'a> SchemaSource<'a> {
    /// The tokens of `sql`, which SQLite has already accepted.
    pub(crate) fn new(sql: &'a str) -> Self {
        let tokens = sqlive::sql::scan(sql).expect("accepted schema tokens");
        Self { sql, tokens }
    }

    /// The source between the parentheses of the first body, such as a
    /// table's column list. This source drops its tokens up to the closing
    /// parenthesis.
    // Only delimiters are interpreted here. The AST determines which nodes
    // exist, which expressions they own, and which clauses are constraints.
    pub(crate) fn body(&mut self) -> Self {
        self.try_body().expect("AST node body")
    }

    /// As [`body`](Self::body), or `None` when this source has no
    /// parenthesis.
    pub(crate) fn try_body(&mut self) -> Option<Self> {
        let start = self.tokens.iter().position(|(t, _)| *t == TK_LP)?;
        Some(self.body_at(start))
    }

    /// The source between the parentheses of the body of `node`, the next
    /// node this source holds. This source drops its tokens up to the
    /// closing parenthesis.
    pub(crate) fn node_body(&mut self, node: &impl ToTokens) -> Self {
        let mut prefix = NodePrefix { tokens: Vec::new(), finished: false };
        node.to_tokens(&mut prefix).unwrap();
        assert!(prefix.finished, "AST node has a parenthesized body");
        let mut depth = 0;
        let start = (0..self.tokens.len())
            .find(|&i| {
                let matches = depth == 0
                    && self.tokens[i..]
                        .iter()
                        .map(|(t, _)| *t)
                        .take(prefix.tokens.len())
                        .eq(prefix.tokens.iter().copied());
                match self.tokens[i].0 {
                    TK_LP => depth += 1,
                    TK_RP => depth -= 1,
                    _ => {}
                }
                matches
            })
            .expect("AST node source prefix");
        self.body_at(start + prefix.tokens.len() - 1)
    }

    /// The comma-separated parts at the top level of this source.
    pub(crate) fn parts(&self) -> Vec<Self> {
        let mut depth = 0;
        let mut start = 0;
        let mut parts = Vec::new();
        for (i, (kind, range)) in self.tokens.iter().enumerate() {
            match kind {
                TK_LP => depth += 1,
                TK_RP => depth -= 1,
                TK_COMMA if depth == 0 => {
                    parts.push(Self::new(&self.sql[start..range.start]));
                    start = self.tokens[i].1.end;
                }
                _ => {}
            }
        }
        parts.push(Self::new(&self.sql[start..]));
        parts
    }

    /// The SQL text from the first to the last token this source still holds:
    /// what follows the bodies it has given out.
    pub(crate) fn rest(&self) -> &'a str {
        match (self.tokens.first(), self.tokens.last()) {
            (Some((_, first)), Some((_, last))) => &self.sql[first.start..last.end],
            _ => "",
        }
    }

    /// The SQL text of this source.
    pub(crate) fn text(&self) -> &'a str {
        self.sql
    }

    /// The text from the first token, without the last `tokens` tokens.
    pub(crate) fn without_suffix(&self, tokens: usize) -> &'a str {
        let first = self.tokens.first().expect("AST expression").1.start;
        let last = self.tokens[self.tokens.len() - tokens - 1].1.end;
        &self.sql[first..last]
    }

    /// The text after the first token, without a trailing semicolon: the
    /// expression a leading keyword such as WHERE introduces.
    pub(crate) fn trailing_expression(&self) -> &'a str {
        let tokens = &self.tokens;
        let last = if tokens.last().expect("AST WHERE clause").0 == TokenType::TK_SEMI {
            tokens.len() - 2
        } else {
            tokens.len() - 1
        };
        self.sql[tokens[0].1.end..tokens[last].1.end].trim()
    }

    fn body_at(&mut self, start: usize) -> Self {
        let mut depth = 0;
        let end = (start..self.tokens.len())
            .find(|&i| {
                match self.tokens[i].0 {
                    TK_LP => depth += 1,
                    TK_RP => depth -= 1,
                    _ => {}
                }
                depth == 0
            })
            .expect("AST node closing delimiter");
        let body = Self::new(&self.sql[self.tokens[start].1.end..self.tokens[end].1.start]);
        self.tokens.drain(..=end);
        body
    }
}

struct NodePrefix {
    tokens: Vec<TokenType>,
    finished: bool,
}

impl TokenStream for NodePrefix {
    type Error = std::convert::Infallible;
    fn append(&mut self, kind: TokenType, _: Option<&str>) -> Result<(), Self::Error> {
        if !self.finished {
            self.tokens.push(kind);
            self.finished = kind == TK_LP;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "schema_source_tests.rs"]
mod tests;
