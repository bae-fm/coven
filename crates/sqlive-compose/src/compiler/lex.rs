//! The tokens of the SQL a macro call holds, with their byte ranges. The
//! compiler edits the text between tokens and keeps the rest as the app
//! wrote it, so the tokens carry ranges, not values.

use std::ops::Range;

/// What a token is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An unquoted word: a keyword or a name.
    Word,
    /// A name in double quotes, backticks or brackets.
    Quoted,
    /// A string or blob literal in single quotes.
    Text,
    /// A number.
    Number,
    /// A parameter: `:name`, `$name` or `?`, with or without a number.
    Parameter,
    /// A compiler marker: `@` and the path or name after it.
    Marker,
    /// Any other character.
    Symbol,
}

/// One token of the SQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) kind: Kind,
    pub(crate) range: Range<usize>,
}

impl Token {
    /// The token's text in `sql`.
    pub(crate) fn text<'a>(&self, sql: &'a str) -> &'a str {
        &sql[self.range.clone()]
    }

    /// Whether this is the unquoted word `word`, in any letter case.
    pub(crate) fn is_word(&self, sql: &str, word: &str) -> bool {
        self.kind == Kind::Word && self.text(sql).eq_ignore_ascii_case(word)
    }

    /// Whether this is the symbol `symbol`.
    pub(crate) fn is_symbol(&self, sql: &str, symbol: char) -> bool {
        self.kind == Kind::Symbol && self.text(sql).starts_with(symbol)
    }

    /// The name a word or a quoted name holds, lowercase and unquoted.
    pub(crate) fn name(&self, sql: &str) -> Option<String> {
        let text = self.text(sql);
        match self.kind {
            Kind::Word => Some(text.to_ascii_lowercase()),
            Kind::Quoted => {
                let quote = text.chars().next()?;
                let (open, close) = if quote == '[' { ('[', ']') } else { (quote, quote) };
                let inner = &text[open.len_utf8()..text.len() - close.len_utf8()];
                let doubled = format!("{close}{close}");
                Some(inner.replace(&doubled, &close.to_string()).to_ascii_lowercase())
            }
            _ => None,
        }
    }
}

fn starts_name(character: char) -> bool {
    character.is_alphabetic() || character == '_'
}

fn continues_name(character: char) -> bool {
    character.is_alphanumeric() || character == '_' || character == '$'
}

/// The tokens of `sql`, without whitespace and comments. An unterminated
/// quote or comment runs to the end of the text, as SQLite reads it.
pub(crate) fn tokens(sql: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = sql.char_indices().peekable();
    while let Some((start, character)) = chars.next() {
        let mut end = start + character.len_utf8();
        let kind = match character {
            _ if character.is_whitespace() => continue,
            '-' if sql[end..].starts_with('-') => {
                while let Some(&(_, next)) = chars.peek() {
                    if next == '\n' {
                        break;
                    }
                    chars.next();
                }
                continue;
            }
            '/' if sql[end..].starts_with('*') => {
                let close = sql[end + 1..].find("*/").map_or(sql.len(), |at| end + 1 + at + 2);
                while chars.peek().is_some_and(|&(position, _)| position < close) {
                    chars.next();
                }
                continue;
            }
            '\'' | '"' | '`' | '[' => {
                let close = if character == '[' { ']' } else { character };
                loop {
                    match chars.next() {
                        Some((position, next)) if next == close => {
                            end = position + next.len_utf8();
                            if close != ']'
                                && chars.peek().is_some_and(|&(_, after)| after == close)
                            {
                                chars.next();
                                continue;
                            }
                            break;
                        }
                        Some((position, next)) => end = position + next.len_utf8(),
                        None => break,
                    }
                }
                if character == '\'' {
                    Kind::Text
                } else {
                    Kind::Quoted
                }
            }
            ':' | '$' | '?' | '@'
                if character == '?'
                    || chars
                        .peek()
                        .is_some_and(|&(_, next)| starts_name(next) || next.is_ascii_digit()) =>
            {
                let marker = character == '@';
                while let Some(&(position, next)) = chars.peek() {
                    let in_path = marker && next == ':' && sql[position..].starts_with("::");
                    if in_path {
                        chars.next();
                        chars.next();
                        end = position + 2;
                    } else if continues_name(next) {
                        end = position + next.len_utf8();
                        chars.next();
                    } else {
                        break;
                    }
                }
                if marker {
                    Kind::Marker
                } else {
                    Kind::Parameter
                }
            }
            _ if starts_name(character) => {
                while let Some(&(position, next)) = chars.peek() {
                    if !continues_name(next) {
                        break;
                    }
                    end = position + next.len_utf8();
                    chars.next();
                }
                Kind::Word
            }
            _ if character.is_ascii_digit() => {
                while let Some(&(position, next)) = chars.peek() {
                    if !(next.is_ascii_alphanumeric() || next == '.') {
                        break;
                    }
                    end = position + next.len_utf8();
                    chars.next();
                }
                Kind::Number
            }
            _ => Kind::Symbol,
        };
        tokens.push(Token { kind, range: start..end });
    }
    tokens
}

/// Replace each range of `sql` that `edits` names, which don't overlap, in
/// order of position.
pub(crate) fn apply(sql: &str, edits: &[(Range<usize>, String)]) -> String {
    let mut sorted: Vec<&(Range<usize>, String)> = edits.iter().collect();
    sorted.sort_by_key(|(range, _)| range.start);
    let mut result = String::with_capacity(sql.len());
    let mut at = 0;
    for (range, text) in sorted {
        result.push_str(&sql[at..range.start]);
        result.push_str(text);
        at = range.end;
    }
    result.push_str(&sql[at..]);
    result
}

#[cfg(test)]
#[path = "lex_tests.rs"]
mod tests;
