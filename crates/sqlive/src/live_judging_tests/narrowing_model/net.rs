//! Writes that change one row more than once, which sqlive and the model
//! judge by each row's net change: a pair of statements that cancel out, as
//! an insert and a delete, an update and an update back, a delete and an
//! insert of the same row, or a key changed and changed back, and a row
//! changed and changed again. The statements are those of the term, edge,
//! LIMIT and mixed rules.

use super::{insert, pick, sql_value, value, Query, SEARCH, TABLES};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    match rng.below(4) {
        0 => super::term::statement(rng),
        1 => super::edge::statement(rng),
        2 => super::limit::statement(rng),
        _ => super::mixed::statement(rng),
    }
}

/// A write of a random statement, then two that change one row twice.
pub(super) fn write(rng: &mut Rng) -> String {
    let table = pick(rng, &TABLES);
    let id = rng.below(8);
    // An id no row holds.
    let free = 8 + rng.below(4);
    let column = pick(rng, &["a", "b", "c", "n"]);
    let other = pick(rng, &["a", "b", "c", "n"]);
    let key = |from: usize, to: usize| {
        let rowid = if table == SEARCH { format!("rowid = {to}, ") } else { String::new() };
        format!("UPDATE OR IGNORE {table} SET {rowid}id = {to} WHERE id = {from}")
    };
    let set = |column: &str, rng: &mut Rng, id: usize| {
        format!("UPDATE {table} SET {column} = {} WHERE id = {id}", sql_value(value(rng)))
    };
    let mut statements = Vec::new();
    if rng.chance(50) {
        statements.push(super::write(rng));
    }
    statements.extend(match rng.below(9) {
        0 => vec![
            insert("INSERT OR IGNORE", table, free, rng),
            format!("DELETE FROM {table} WHERE id = {free}"),
        ],
        1 => vec![
            format!("UPDATE {table} SET {column} = {column} + 1 WHERE id = {id}"),
            format!("UPDATE {table} SET {column} = {column} - 1 WHERE id = {id}"),
        ],
        // FTS5 deletes and inserts again each row an UPDATE names.
        2 if table == SEARCH => {
            vec![format!("UPDATE {table} SET {column} = {column} WHERE id = {id}")]
        }
        2 => vec![format!("REPLACE INTO {table} SELECT * FROM {table} WHERE id = {id}")],
        3 => vec![key(id, free), key(free, id)],
        4 => vec![set(column, rng, id), set(column, rng, id)],
        5 => vec![set(column, rng, id), set(other, rng, id)],
        6 => vec![insert("INSERT OR IGNORE", table, free, rng), set(column, rng, free)],
        7 => vec![set(column, rng, id), format!("DELETE FROM {table} WHERE id = {id}")],
        _ => vec![
            format!("DELETE FROM {table} WHERE id = {id}"),
            insert("INSERT OR IGNORE", table, id, rng),
        ],
    });
    statements.join("; ")
}
