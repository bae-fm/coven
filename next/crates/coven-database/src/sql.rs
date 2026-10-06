//! SQLite tokens used to inspect CREATE statements without matching comments or strings.

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

    pub(crate) fn identifier(&self, name: &str) -> bool {
        matches!(self, Self::Word(value) | Self::Quoted(value) if value.eq_ignore_ascii_case(name))
    }
}

pub(crate) fn tokens(sql: &str) -> Vec<Token> {
    spanned_tokens(sql)
        .into_iter()
        .map(|(token, _)| token)
        .collect()
}

/// Byte ranges preserve expressions exactly, including operators and literals.
pub(crate) fn spanned_tokens(sql: &str) -> Vec<(Token, std::ops::Range<usize>)> {
    let mut chars = sql.char_indices().peekable();
    let mut result = Vec::new();
    while let Some((start, ch)) = chars.next() {
        let token = match ch {
            ch if ch.is_whitespace() => continue,
            '-' if chars.peek().is_some_and(|(_, c)| *c == '-') => {
                chars.next();
                for (_, ch) in chars.by_ref() {
                    if ch == '\n' {
                        break;
                    }
                }
                continue;
            }
            '/' if chars.peek().is_some_and(|(_, c)| *c == '*') => {
                chars.next();
                while let Some((_, ch)) = chars.next() {
                    if ch == '*' && chars.peek().is_some_and(|(_, c)| *c == '/') {
                        chars.next();
                        break;
                    }
                }
                continue;
            }
            '\'' | '"' | '`' | '[' => {
                let end = if ch == '[' { ']' } else { ch };
                let mut value = String::new();
                while let Some((_, next)) = chars.next() {
                    if next == end {
                        if ch != '[' && chars.peek().is_some_and(|(_, c)| *c == end) {
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    value.push(next);
                }
                if ch == '\'' {
                    Token::String(value)
                } else {
                    Token::Quoted(value)
                }
            }
            ch if ch.is_alphanumeric() || ch == '_' || ch == '$' => {
                let mut word = String::from(ch);
                while chars
                    .peek()
                    .is_some_and(|(_, c)| c.is_alphanumeric() || *c == '_' || *c == '$')
                {
                    word.push(chars.next().expect("peeked character").1);
                }
                Token::Word(word.to_ascii_lowercase())
            }
            ch => Token::Symbol(ch),
        };
        let end = chars.peek().map_or(sql.len(), |(index, _)| *index);
        result.push((token, start..end));
    }
    result
}

/// Quote an identifier, never a value or SQL fragment.
pub(crate) fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn guarded_trigger(sql: &str) -> bool {
    let tokens = tokens(sql);
    let Some(begin) = tokens.iter().position(|t| t.word("begin")) else {
        return false;
    };
    let Some(when) = tokens[..begin].iter().position(|t| t.word("when")) else {
        return false;
    };
    guarded_predicate(&tokens[when + 1..begin])
}

fn guarded_predicate(mut expr: &[Token]) -> bool {
    while expr.first() == Some(&Token::Symbol('(')) && matching_paren(expr) == Some(expr.len() - 1)
    {
        expr = &expr[1..expr.len() - 1];
    }
    let mut depth = 0;
    let mut ands = Vec::new();
    let mut cases = 0;
    let mut between = 0;
    for (i, token) in expr.iter().enumerate() {
        match token {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth -= 1,
            token if token.word("case") => cases += 1,
            token if token.word("end") && cases > 0 => cases -= 1,
            token if depth == 0 && cases == 0 && token.word("between") => between += 1,
            token if depth == 0 && cases == 0 && token.word("or") => return false,
            token if depth == 0 && cases == 0 && token.word("and") => {
                if between > 0 {
                    between -= 1;
                } else {
                    ands.push(i);
                }
            }
            _ => {}
        }
    }
    if !ands.is_empty() {
        let mut start = 0;
        for end in ands.into_iter().chain(std::iter::once(expr.len())) {
            if guarded_predicate(&expr[start..end]) {
                return true;
            }
            start = end + 1;
        }
        return false;
    }
    if !expr.first().is_some_and(|t| t.word("not")) {
        return false;
    }
    let mut call = &expr[1..];
    while call.first() == Some(&Token::Symbol('(')) && matching_paren(call) == Some(call.len() - 1)
    {
        call = &call[1..call.len() - 1];
    }
    matches!(call, [name, Token::Symbol('('), Token::Symbol(')')] if name.identifier("coven_applying"))
}

fn matching_paren(tokens: &[Token]) -> Option<usize> {
    let mut depth = 0;
    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Table clauses and suffix, retaining constraints and column definitions for
/// migration comparison. SQLite has already parsed and accepted this SQL.
pub(crate) fn table_parts(sql: &str) -> Option<(Vec<Vec<Token>>, Vec<Token>)> {
    let tokens = tokens(sql);
    let start = tokens.iter().position(|t| *t == Token::Symbol('('))?;
    let end = start + matching_paren(&tokens[start..])?;
    let mut parts = Vec::new();
    let mut part = start + 1;
    let mut depth = 0;
    for (i, token) in tokens.iter().enumerate().take(end).skip(part) {
        match token {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => depth -= 1,
            Token::Symbol(',') if depth == 0 => {
                parts.push(tokens[part..i].to_vec());
                part = i + 1;
            }
            _ => {}
        }
    }
    parts.push(tokens[part..end].to_vec());
    Some((parts, tokens[end + 1..].to_vec()))
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;
