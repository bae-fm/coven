//! What a piece of SQL text does to a schema's tables, statement by
//! statement: the table a `CREATE TABLE` makes, the view a `CREATE VIEW`
//! makes, the table a `CREATE INDEX` indexes or a `CREATE TRIGGER` fires on,
//! with the operation an `INSTEAD OF` trigger stands in for, and the tables
//! each statement writes, with how.
//!
//! A write is the table after `INSERT INTO`, `REPLACE INTO`, `UPDATE` and
//! `DELETE FROM`, with SQLite's `OR <conflict>` forms, wherever they stand —
//! a statement on its own, a CTE's, or one in a `CREATE TRIGGER` body, which
//! is the trigger's write. A trigger's own `AFTER UPDATE ON t` names the
//! table it fires on, which it does not write. Reads (`SELECT`, `FROM`,
//! `JOIN`) are not writes. A temporary table, view, index or trigger belongs
//! to the connection that makes it, not to the schema, so its creation is
//! none.
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
    /// The tables the statement writes, each once for each way it writes
    /// it, in the order they appear.
    pub(crate) writes: Vec<Write>,
    /// The tables the statement reads, after `FROM` or `JOIN`, each once, in
    /// the order they appear.
    pub(crate) reads: Vec<Read>,
    /// The columns the statement sets with `UPDATE … SET` or an upsert's
    /// `DO UPDATE SET`, in the order they appear.
    pub(crate) assignments: Vec<Assignment>,
}

/// A column an `UPDATE` or an upsert sets, and whether it sets it to itself
/// plus a positive integer literal.
#[derive(Debug, PartialEq)]
pub(crate) struct Assignment {
    /// The table the statement writes, lowercase.
    pub(crate) table: String,
    pub(crate) column: String,
    pub(crate) increment: bool,
    /// The byte offset of the column's name in the text.
    pub(crate) at: usize,
}

/// A table a statement reads, lowercase, and the byte offset of its name in
/// the text.
#[derive(Debug, PartialEq)]
pub(crate) struct Read {
    pub(crate) table: String,
    pub(crate) at: usize,
}

/// What a `CREATE` statement adds to the schema, by the table it is about.
#[derive(Debug, PartialEq)]
pub(crate) enum Creation {
    /// `CREATE TABLE t` or `CREATE VIRTUAL TABLE t`.
    Table(String),
    /// `CREATE VIEW v`.
    View(String),
    /// `CREATE INDEX i ON t`.
    Index(String),
    /// `CREATE TRIGGER g … ON t`, and for `INSTEAD OF`, the operation on the
    /// view `t` the trigger runs instead of.
    Trigger { table: String, instead_of: Option<Operation> },
}

/// How a statement writes a table. `REPLACE INTO` inserts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Operation {
    Insert,
    Update,
    Delete,
}

/// A table a statement writes, lowercase, how it writes it, and the byte
/// offset of its name in the text.
#[derive(Debug, PartialEq)]
pub(crate) struct Write {
    pub(crate) table: String,
    pub(crate) operation: Operation,
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
    Some(Statement {
        at: words.first()?.at,
        end,
        creation: creation(words),
        writes: writes(words),
        reads: reads(words),
        assignments: assignments(words),
    })
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
        "view" => Some(Creation::View(name)),
        "index" => on_table().map(Creation::Index),
        "trigger" => {
            let table = on_table()?;
            // `INSTEAD OF INSERT ON v`, `INSTEAD OF UPDATE OF c ON v`.
            let instead_of = (name_at(words, after) == Some("instead")
                && name_at(words, after + 1) == Some("of"))
            .then(|| match name_at(words, after + 2) {
                Some("insert") => Some(Operation::Insert),
                Some("update") => Some(Operation::Update),
                Some("delete") => Some(Operation::Delete),
                _ => None,
            })
            .flatten();
            Some(Creation::Trigger { table, instead_of })
        }
        _ => None,
    }
}

fn writes(words: &[Located]) -> Vec<Write> {
    // SQLite's `OR ROLLBACK`, `OR REPLACE`, … after INSERT and UPDATE.
    let after_conflict =
        |index: usize| if name_at(words, index) == Some("or") { index + 2 } else { index };
    let table_at = |index: usize, operation| {
        object_at(words, index).map(|(table, _, _)| Write { table, operation, at: words[index].at })
    };
    let mut writes: Vec<Write> = Vec::new();
    for index in 0..words.len() {
        let write = match name_at(words, index) {
            Some("insert" | "replace") => {
                let into = after_conflict(index + 1);
                (name_at(words, into) == Some("into"))
                    .then(|| table_at(into + 1, Operation::Insert))
                    .flatten()
            }
            Some("update") => table_at(after_conflict(index + 1), Operation::Update)
                .filter(|write| !matches!(write.table.as_str(), "set" | "of" | "on")),
            Some("delete") => (name_at(words, index + 1) == Some("from"))
                .then(|| table_at(index + 2, Operation::Delete))
                .flatten(),
            _ => None,
        };
        if let Some(write) = write {
            if !writes
                .iter()
                .any(|known| known.table == write.table && known.operation == write.operation)
            {
                writes.push(write);
            }
        }
    }
    writes
}

/// The words that end an `UPDATE`'s `SET` list.
const SET_END_WORDS: &[&str] = &["where", "from", "returning", "end", "order", "limit"];

/// Each column set by `UPDATE t SET …` or by an upsert's `DO UPDATE SET …`,
/// wherever it stands in the statement, a trigger's body included. An
/// upsert writes the table of the nearest `INSERT INTO` before it.
fn assignments(words: &[Located]) -> Vec<Assignment> {
    let mut found = Vec::new();
    for index in 0..words.len() {
        if name_at(words, index) != Some("update") {
            continue;
        }
        let (table, set) = if index > 0 && name_at(words, index - 1) == Some("do") {
            let Some(table) = upsert_table(words, index) else { continue };
            (table, index + 1)
        } else {
            let first = if name_at(words, index + 1) == Some("or") { index + 3 } else { index + 1 };
            let Some((table, _, after)) = object_at(words, first) else { continue };
            (table, after)
        };
        if name_at(words, set) != Some("set") {
            continue;
        }
        set_list(words, set + 1, &table, &mut found);
    }
    found
}

/// The table of the `INSERT INTO` or `REPLACE INTO` nearest before `index`.
fn upsert_table(words: &[Located], index: usize) -> Option<String> {
    let into = (0..index).rev().find(|&at| {
        name_at(words, at) == Some("into")
            && matches!(
                name_at(words, at.wrapping_sub(1)),
                Some("insert" | "replace" | "rollback" | "abort" | "fail" | "ignore")
            )
    })?;
    object_at(words, into + 1).map(|(table, _, _)| table)
}

/// The assignments of the `SET` list starting at `start`.
fn set_list(words: &[Located], start: usize, table: &str, found: &mut Vec<Assignment>) {
    let mut index = start;
    loop {
        let Some((column, _, after)) = object_at(words, index) else { return };
        if !matches!(words.get(after).map(|located| &located.word), Some(Word::Symbol('='))) {
            return;
        }
        let value = after + 1;
        let mut depth = 0usize;
        let mut end = value;
        while let Some(located) = words.get(end) {
            match &located.word {
                Word::Symbol('(') => depth += 1,
                Word::Symbol(')') => match depth.checked_sub(1) {
                    Some(outer) => depth = outer,
                    None => break,
                },
                Word::Symbol(',' | ';') if depth == 0 => break,
                Word::Name(name) if depth == 0 && SET_END_WORDS.contains(&name.as_str()) => break,
                _ => {}
            }
            end += 1;
        }
        found.push(Assignment {
            table: table.to_string(),
            increment: is_increment(&words[value..end], &column),
            column,
            at: words[index].at,
        });
        match words.get(end).map(|located| &located.word) {
            Some(Word::Symbol(',')) => index = end + 1,
            _ => return,
        }
    }
}

/// Whether a value is the column plus a positive integer literal.
fn is_increment(value: &[Located], column: &str) -> bool {
    let Some((name, _, after)) = object_at(value, 0) else { return false };
    let number = |index: usize| match value.get(index).map(|located| &located.word) {
        Some(Word::Name(digits)) => {
            digits.chars().all(|digit| digit.is_ascii_digit())
                && digits.chars().any(|digit| digit != '0')
        }
        _ => false,
    };
    name == column
        && matches!(value.get(after).map(|located| &located.word), Some(Word::Symbol('+')))
        && number(after + 1)
        && value.len() == after + 2
}

/// The tables after `FROM` and `JOIN`, a `DELETE FROM`'s target aside, and
/// each further table of a `FROM a, b` list. A name followed by `(` is a
/// table-valued function, and `(` itself opens a subquery, whose reads are
/// read in turn.
fn reads(words: &[Located]) -> Vec<Read> {
    let mut reads: Vec<Read> = Vec::new();
    let push = |index: usize, reads: &mut Vec<Read>| -> Option<usize> {
        let (table, _, after) = object_at(words, index)?;
        if matches!(words.get(after).map(|located| &located.word), Some(Word::Symbol('('))) {
            return None;
        }
        if !reads.iter().any(|known| known.table == table) {
            reads.push(Read { table, at: words[index].at });
        }
        Some(after)
    };
    for index in 0..words.len() {
        let read = match name_at(words, index) {
            Some("from") => index == 0 || name_at(words, index - 1) != Some("delete"),
            Some("join") => true,
            _ => false,
        };
        if !read {
            continue;
        }
        let mut next = index + 1;
        while let Some(mut after) = push(next, &mut reads) {
            // An alias, with or without `AS`.
            if name_at(words, after) == Some("as") {
                after += 2;
            } else if name_at(words, after).is_some_and(|name| !CLAUSE_WORDS.contains(&name)) {
                after += 1;
            }
            match words.get(after).map(|located| &located.word) {
                Some(Word::Symbol(',')) => next = after + 1,
                _ => break,
            }
        }
    }
    reads
}

/// The words that can follow a table in a `FROM` list, so they are no alias.
const CLAUSE_WORDS: &[&str] = &[
    "where",
    "join",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "natural",
    "on",
    "using",
    "group",
    "order",
    "limit",
    "having",
    "window",
    "union",
    "except",
    "intersect",
    "indexed",
    "not",
    "returning",
    "set",
    "values",
    "select",
    "do",
    "end",
];

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
