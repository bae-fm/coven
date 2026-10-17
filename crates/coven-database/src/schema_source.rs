//! Source ranges for parsed schema nodes. sqlite3-parser's AST has no spans;
//! its lexer supplies offsets so constraint identities keep the app's spelling.

use sqlite3_parser::ast::fmt::{ToTokens, TokenStream};
use sqlite3_parser::lexer::{
    sql::{TokenType, Tokenizer},
    Scanner, Splitter,
};
use std::ops::Range;
use TokenType::{TK_COMMA, TK_LP, TK_RP};

pub(crate) struct SchemaSource<'a> {
    sql: &'a str,
    tokens: Vec<(TokenType, Range<usize>)>,
}

impl<'a> SchemaSource<'a> {
    pub(crate) fn new(sql: &'a str) -> Self {
        Self::try_new(sql).expect("accepted schema tokens")
    }

    pub(crate) fn try_new(sql: &'a str) -> rusqlite::Result<Self> {
        let mut scanner = Scanner::new(SqliteTokenizer(Tokenizer::new()));
        let mut tokens = Vec::new();
        loop {
            let (start, token, end) = scanner
                .scan(sql.as_bytes())
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            let Some((_, kind)) = token else { break };
            tokens.push((kind, start..end));
        }
        Ok(Self { sql, tokens })
    }

    pub(crate) fn tokens(&self) -> impl Iterator<Item = &'a str> + '_ {
        self.tokens
            .iter()
            .map(|(_, range)| &self.sql[range.clone()])
    }

    // Only delimiters are interpreted here. The AST determines which nodes
    // exist, which expressions they own, and which clauses are constraints.
    pub(crate) fn body(&mut self) -> Self {
        self.try_body().expect("AST node body")
    }

    pub(crate) fn try_body(&mut self) -> Option<Self> {
        let start = self.tokens.iter().position(|(t, _)| *t == TK_LP)?;
        Some(self.body_at(start))
    }

    pub(crate) fn node_body(&mut self, node: &impl ToTokens) -> Self {
        let mut prefix = NodePrefix {
            tokens: Vec::new(),
            finished: false,
        };
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

    pub(crate) fn text(&self) -> &'a str {
        self.sql
    }

    pub(crate) fn without_suffix(&self, tokens: usize) -> &'a str {
        let first = self.tokens.first().expect("AST expression").1.start;
        let last = self.tokens[self.tokens.len() - tokens - 1].1.end;
        &self.sql[first..last]
    }

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

// sqlite3-parser omits SQLite's Tcl parameter suffixes and requires a closing
// block comment. Keep those accepted spellings in the shared token source.
struct SqliteTokenizer(Tokenizer);

impl Splitter for SqliteTokenizer {
    type Error = sqlite3_parser::lexer::sql::Error;
    type TokenType = TokenType;

    fn split<'a>(
        &mut self,
        data: &'a [u8],
    ) -> Result<(Option<(&'a [u8], TokenType)>, usize), Self::Error> {
        use sqlite3_parser::lexer::sql::Error;
        if !matches!(data[0], b'$' | b'@' | b':' | b'#') {
            return match self.0.split(data) {
                Err(Error::UnterminatedBlockComment(_)) if data.len() > 2 => Ok((None, data.len())),
                result => result,
            };
        }
        let mut end = 1;
        let mut named = false;
        while let Some(&byte) = data.get(end) {
            if byte.is_ascii_alphanumeric() || byte >= 0x80 || matches!(byte, b'_' | b'$') {
                named = true;
                end += 1;
            } else if data[end..].starts_with(b"::") {
                end += 2;
            } else if byte == b'(' && named {
                end += 1;
                while data
                    .get(end)
                    .is_some_and(|b| !b.is_ascii_whitespace() && *b != b')' && *b != 0)
                {
                    end += 1;
                }
                if data.get(end) != Some(&b')') {
                    return Err(Error::BadVariableName(None));
                }
                end += 1;
                break;
            } else {
                break;
            }
        }
        if !named {
            return Err(Error::BadVariableName(None));
        }
        Ok((Some((&data[..end], TokenType::TK_VARIABLE)), end))
    }
}
