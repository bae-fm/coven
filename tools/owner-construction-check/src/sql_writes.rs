//! The tables a piece of SQL text writes: the table after `INSERT INTO`,
//! `REPLACE INTO`, `UPDATE` and `DELETE FROM`, with SQLite's `OR <conflict>`
//! forms, wherever they stand — a statement on its own, a CTE's, or one in a
//! `CREATE TRIGGER` body. A trigger's own `AFTER UPDATE ON t` names the table
//! it watches, which it does not write. Reads (`SELECT`, `FROM`, `JOIN`) are
//! not writes.
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

/// The tables `sql` writes, lowercase, each once, in the order they appear.
pub(crate) fn written_tables(sql: &str) -> Vec<String> {
    let words = words(sql);
    let name = |index: usize| match words.get(index) {
        Some(Word::Name(name)) => Some(name.as_str()),
        _ => None,
    };
    // A table name, schema prefix dropped.
    let table_at = |index: usize| -> Option<String> {
        let first = name(index)?;
        match (words.get(index + 1), name(index + 2)) {
            (Some(Word::Symbol('.')), Some(table)) => Some(table.to_string()),
            _ => Some(first.to_string()),
        }
    };
    // SQLite's `OR ROLLBACK`, `OR REPLACE`, … after INSERT and UPDATE.
    let after_conflict = |index: usize| if name(index) == Some("or") { index + 2 } else { index };
    let mut tables = Vec::new();
    for index in 0..words.len() {
        let table = match name(index) {
            Some("insert" | "replace") => {
                let into = after_conflict(index + 1);
                (name(into) == Some("into")).then(|| table_at(into + 1)).flatten()
            }
            Some("update") => table_at(after_conflict(index + 1))
                .filter(|table| !matches!(table.as_str(), "set" | "of" | "on")),
            Some("delete") => {
                (name(index + 1) == Some("from")).then(|| table_at(index + 2)).flatten()
            }
            _ => None,
        };
        if let Some(table) = table {
            if !tables.contains(&table) {
                tables.push(table);
            }
        }
    }
    tables
}

fn words(sql: &str) -> Vec<Word> {
    let characters = sql.chars().collect::<Vec<_>>();
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
            words.push(Word::Name(name.to_lowercase()));
            index = end;
        } else if character.is_alphanumeric() || character == '_' {
            let start = index;
            while index < characters.len()
                && (characters[index].is_alphanumeric() || characters[index] == '_')
            {
                index += 1;
            }
            words.push(Word::Name(
                characters[start..index].iter().collect::<String>().to_lowercase(),
            ));
        } else {
            words.push(Word::Symbol(character));
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

#[cfg(test)]
#[path = "sql_writes_tests.rs"]
mod tests;
