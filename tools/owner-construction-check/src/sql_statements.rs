//! What a piece of SQL text does to a schema's tables, statement by
//! statement: the table a `CREATE TABLE` makes, the table a `CREATE INDEX`
//! indexes or a `CREATE TRIGGER` fires on, and the tables each statement
//! writes.
//!
//! A write is the table after `INSERT INTO`, `REPLACE INTO`, `UPDATE` and
//! `DELETE FROM`, with SQLite's `OR <conflict>` forms, wherever they stand —
//! a statement on its own, a CTE's, or one in a `CREATE TRIGGER` body, which
//! is the trigger's write. A trigger's own `AFTER UPDATE ON t` names the
//! table it fires on, which it does not write. Reads (`SELECT`, `FROM`,
//! `JOIN`) are not writes. A temporary table, index or trigger belongs to the
//! connection that makes it, not to the schema, so its creation is none.
//!
//! Names compare as SQLite compares them: without case, quoted or not, and
//! without a schema prefix (`main.notes` is `notes`). String literals and
//! comments inside the SQL are skipped. Any text is read this way, so prose
//! that says "update the listing" names a table `the`; a rule asks only
//! about the tables a policy lists.

#[derive(Debug, PartialEq)]
enum Word {
    Name(String),
    Symbol(char),
}

/// A word and the byte offset where it starts in the text.
struct Located {
    word: Word,
    at: usize,
}

/// One statement: what it makes of the schema, and what it writes.
#[derive(Debug, PartialEq)]
pub(crate) struct Statement {
    /// The byte offsets in the text where the statement's first word starts
    /// and where the statement ends: at its `;`, or the text's end.
    pub(crate) at: usize,
    pub(crate) end: usize,
    pub(crate) creation: Option<Creation>,
    /// The tables the statement writes, each once, in the order they appear.
    pub(crate) writes: Vec<Write>,
}

/// What a `CREATE` statement adds to the schema, by the table it is about.
#[derive(Debug, PartialEq)]
pub(crate) enum Creation {
    /// `CREATE TABLE t` or `CREATE VIRTUAL TABLE t`.
    Table(String),
    /// `CREATE INDEX i ON t`.
    Index(String),
    /// `CREATE TRIGGER g … ON t`.
    Trigger(String),
}

/// A table a statement writes, lowercase, and the byte offset of its name in
/// the text.
#[derive(Debug, PartialEq)]
pub(crate) struct Write {
    pub(crate) table: String,
    pub(crate) at: usize,
}

/// The statements of `sql`, in order. A trigger's body, from its `BEGIN` to
/// its `END`, is part of the trigger's statement.
pub(crate) fn statements(sql: &str) -> Vec<Statement> {
    let words = words(sql);
    let mut statements = Vec::new();
    let mut start = 0;
    // Inside a trigger's body: how many `CASE … END` are open.
    let mut body: Option<usize> = None;
    let creates_trigger = |start: usize| {
        name_at(&words, start) == Some("create")
            && (start + 1..words.len())
                .map_while(|index| name_at(&words, index))
                .take_while(|name| matches!(*name, "temp" | "temporary" | "trigger"))
                .any(|name| name == "trigger")
    };
    for index in 0..words.len() {
        match (&words[index].word, &mut body) {
            (Word::Symbol(';'), None) => {
                statements.extend(statement(&words[start..index], words[index].at));
                start = index + 1;
            }
            (Word::Name(name), None) if name == "begin" && creates_trigger(start) => {
                body = Some(0);
            }
            (Word::Name(name), Some(depth)) if name == "case" => *depth += 1,
            (Word::Name(name), Some(depth)) if name == "end" => match depth.checked_sub(1) {
                Some(outer) => *depth = outer,
                None => body = None,
            },
            _ => {}
        }
    }
    statements.extend(statement(&words[start..], sql.len()));
    statements
}

fn statement(words: &[Located], end: usize) -> Option<Statement> {
    Some(Statement { at: words.first()?.at, end, creation: creation(words), writes: writes(words) })
}

fn name_at(words: &[Located], index: usize) -> Option<&str> {
    match words.get(index).map(|located| &located.word) {
        Some(Word::Name(name)) => Some(name.as_str()),
        _ => None,
    }
}

/// The object named at `index`, its schema prefix dropped; whether that
/// prefix is `temp`; and the index after the name.
fn object_at(words: &[Located], index: usize) -> Option<(String, bool, usize)> {
    let first = name_at(words, index)?;
    match (words.get(index + 1).map(|located| &located.word), name_at(words, index + 2)) {
        (Some(Word::Symbol('.')), Some(name)) => {
            Some((name.to_string(), matches!(first, "temp" | "temporary"), index + 3))
        }
        _ => Some((first.to_string(), false, index + 1)),
    }
}

fn creation(words: &[Located]) -> Option<Creation> {
    if name_at(words, 0) != Some("create") {
        return None;
    }
    let mut index = 1;
    let mut temporary = false;
    while let Some(modifier @ ("temp" | "temporary" | "unique" | "virtual")) = name_at(words, index)
    {
        temporary |= matches!(modifier, "temp" | "temporary");
        index += 1;
    }
    let kind = name_at(words, index)?;
    index += 1;
    // `IF NOT EXISTS`
    if name_at(words, index) == Some("if") {
        index += 3;
    }
    let (name, temporary_name, after) = object_at(words, index)?;
    if temporary || temporary_name {
        return None;
    }
    let on_table = || {
        let on = (after..words.len()).find(|&index| name_at(words, index) == Some("on"))?;
        object_at(words, on + 1).map(|(table, _, _)| table)
    };
    match kind {
        "table" => Some(Creation::Table(name)),
        "index" => on_table().map(Creation::Index),
        "trigger" => on_table().map(Creation::Trigger),
        _ => None,
    }
}

fn writes(words: &[Located]) -> Vec<Write> {
    // SQLite's `OR ROLLBACK`, `OR REPLACE`, … after INSERT and UPDATE.
    let after_conflict =
        |index: usize| if name_at(words, index) == Some("or") { index + 2 } else { index };
    let table_at = |index: usize| {
        object_at(words, index).map(|(table, _, _)| Write { table, at: words[index].at })
    };
    let mut writes: Vec<Write> = Vec::new();
    for index in 0..words.len() {
        let write = match name_at(words, index) {
            Some("insert" | "replace") => {
                let into = after_conflict(index + 1);
                (name_at(words, into) == Some("into")).then(|| table_at(into + 1)).flatten()
            }
            Some("update") => table_at(after_conflict(index + 1))
                .filter(|write| !matches!(write.table.as_str(), "set" | "of" | "on")),
            Some("delete") => {
                (name_at(words, index + 1) == Some("from")).then(|| table_at(index + 2)).flatten()
            }
            _ => None,
        };
        if let Some(write) = write {
            if !writes.iter().any(|known| known.table == write.table) {
                writes.push(write);
            }
        }
    }
    writes
}

fn words(sql: &str) -> Vec<Located> {
    let characters = sql.chars().collect::<Vec<_>>();
    let offsets = sql.char_indices().map(|(offset, _)| offset).collect::<Vec<_>>();
    let mut words = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];
        let next = characters.get(index + 1).copied();
        if character.is_whitespace() {
            index += 1;
        } else if character == '-' && next == Some('-') {
            while index < characters.len() && characters[index] != '\n' {
                index += 1;
            }
        } else if character == '/' && next == Some('*') {
            index += 2;
            while index < characters.len()
                && !(characters[index] == '*' && characters.get(index + 1) == Some(&'/'))
            {
                index += 1;
            }
            index += 2;
        } else if character == '\'' {
            index = quoted_end(&characters, index, '\'');
        } else if matches!(character, '"' | '`' | '[') {
            let close = if character == '[' { ']' } else { character };
            let end = quoted_end(&characters, index, close);
            let name = characters[index + 1..end.saturating_sub(1).max(index + 1)]
                .iter()
                .collect::<String>();
            words.push(Located { word: Word::Name(name.to_lowercase()), at: offsets[index] });
            index = end;
        } else if character.is_alphanumeric() || character == '_' {
            let start = index;
            while index < characters.len()
                && (characters[index].is_alphanumeric() || characters[index] == '_')
            {
                index += 1;
            }
            let name = characters[start..index].iter().collect::<String>().to_lowercase();
            words.push(Located { word: Word::Name(name), at: offsets[start] });
        } else {
            words.push(Located { word: Word::Symbol(character), at: offsets[index] });
            index += 1;
        }
    }
    words
}

/// The index after a quoted run opened at `start`, a doubled closing
/// character standing for itself.
fn quoted_end(characters: &[char], start: usize, close: char) -> usize {
    let mut index = start + 1;
    while index < characters.len() {
        if characters[index] == close {
            if characters.get(index + 1) == Some(&close) && close != ']' {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    index
}

/// The line, counted from 1, of byte offset `at` in `text`.
pub(crate) fn line_of(text: &str, at: usize) -> usize {
    1 + text[..at].matches('\n').count()
}

#[cfg(test)]
#[path = "sql_statements_tests.rs"]
mod tests;
