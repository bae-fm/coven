//! The tokens of SQL text with their byte ranges. sqlite3-parser's AST has no
//! spans, so its lexer supplies the offsets that keep the app's spelling.

use sqlite3_parser::lexer::{
    sql::{TokenType, Tokenizer},
    Scanner, Splitter,
};
use std::ops::Range;

/// Each token of `sql`, without comments and whitespace, with its kind and
/// its byte range in `sql`. SQLite's parameter names and unterminated block
/// comments are accepted as SQLite accepts them.
pub fn scan(sql: &str) -> rusqlite::Result<Vec<(TokenType, Range<usize>)>> {
    let mut scanner = Scanner::new(SqliteTokenizer(Tokenizer::new()));
    let mut tokens = Vec::new();
    loop {
        let (start, token, end) = scanner
            .scan(sql.as_bytes())
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let Some((_, kind)) = token else { break };
        tokens.push((kind, start..end));
    }
    Ok(tokens)
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
