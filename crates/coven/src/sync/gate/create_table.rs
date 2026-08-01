//! Rewrite a table's stored `CREATE TABLE` statement so it builds inside an
//! attached schema (the empty clone the full-state diff runs against), via a
//! small hand-rolled tokenizer over the leading `CREATE TABLE <name>` tokens.

use rusqlite::Connection;

use super::{query_row_optional, GateError};
use crate::database::quote_ident;

/// `CREATE TABLE` text for `table` from `sqlite_master`.
pub(super) fn create_table_sql(conn: &Connection, table: &str) -> Result<String, GateError> {
    let sql = "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?1";
    let create = query_row_optional(conn, sql, [table], |row| row.get::<_, Option<String>>(0))?;
    create
        .flatten()
        .ok_or_else(|| GateError::NoSchema(table.to_string()))
}

/// Qualify a `CREATE TABLE <name> ...` statement so it builds the table inside the attached
/// schema `alias`, replacing only the table-name token.
pub(super) fn rewrite_create_into_schema(
    create: &str,
    table: &str,
    alias: &str,
) -> Result<String, GateError> {
    let Some((name_start, name_end, parsed_table)) = create_table_name_token(create) else {
        return Err(GateError::BadCreateTableSql {
            table: table.to_string(),
            sql: create.to_string(),
        });
    };
    if parsed_table != table {
        return Err(GateError::BadCreateTableSql {
            table: table.to_string(),
            sql: create.to_string(),
        });
    }

    let qualified = format!("{alias}.{}", quote_ident(table));
    let mut out = String::with_capacity(create.len() + qualified.len());
    out.push_str(&create[..name_start]);
    out.push_str(&qualified);
    out.push_str(&create[name_end..]);
    Ok(out)
}

fn create_table_name_token(create: &str) -> Option<(usize, usize, String)> {
    let mut pos = consume_keyword_ws(create, skip_ascii_ws(create, 0), "CREATE")?;
    pos = consume_keyword_ws(create, pos, "TABLE")?;

    if keyword_at(create, pos, "IF") {
        pos = consume_keyword_ws(create, pos, "IF")?;
        pos = consume_keyword_ws(create, pos, "NOT")?;
        pos = consume_keyword_ws(create, pos, "EXISTS")?;
    }

    parse_identifier_token(create, pos)
}

fn skip_ascii_ws(sql: &str, mut pos: usize) -> usize {
    while sql.as_bytes().get(pos).is_some_and(u8::is_ascii_whitespace) {
        pos += 1;
    }
    pos
}

fn keyword_at(sql: &str, pos: usize, keyword: &str) -> bool {
    let Some(end) = pos.checked_add(keyword.len()) else {
        return false;
    };
    sql.get(pos..end)
        .is_some_and(|token| token.eq_ignore_ascii_case(keyword))
        && sql.as_bytes().get(end).is_some_and(u8::is_ascii_whitespace)
}

fn consume_keyword(sql: &str, pos: usize, keyword: &str) -> Option<usize> {
    keyword_at(sql, pos, keyword).then_some(pos + keyword.len())
}

fn consume_keyword_ws(sql: &str, pos: usize, keyword: &str) -> Option<usize> {
    Some(skip_ascii_ws(sql, consume_keyword(sql, pos, keyword)?))
}

fn parse_identifier_token(sql: &str, pos: usize) -> Option<(usize, usize, String)> {
    match sql.as_bytes().get(pos).copied()? {
        b'"' => parse_delimited_identifier(sql, pos, b'"'),
        _ => parse_bare_identifier(sql, pos),
    }
}

fn parse_delimited_identifier(
    sql: &str,
    start: usize,
    delimiter: u8,
) -> Option<(usize, usize, String)> {
    let bytes = sql.as_bytes();
    let mut pos = start + 1;
    let mut out = String::new();
    while pos < bytes.len() {
        if bytes[pos] == delimiter {
            if bytes.get(pos + 1).copied() == Some(delimiter) {
                out.push(delimiter as char);
                pos += 2;
            } else {
                return Some((start, pos + 1, out));
            }
        } else {
            let ch = sql[pos..].chars().next()?;
            out.push(ch);
            pos += ch.len_utf8();
        }
    }
    None
}

fn parse_bare_identifier(sql: &str, start: usize) -> Option<(usize, usize, String)> {
    let mut pos = start;
    while pos < sql.len() {
        let b = sql.as_bytes()[pos];
        if b.is_ascii_whitespace() || b == b'(' {
            break;
        }
        let ch = sql[pos..].chars().next()?;
        pos += ch.len_utf8();
    }
    (pos > start).then(|| (start, pos, sql[start..pos].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::gate::GateError;

    #[test]
    fn create_table_rewrite_qualifies_table_token() {
        let cases = [
            (
                "CREATE TABLE nodes (id TEXT PRIMARY KEY)",
                "CREATE TABLE empty.\"nodes\" (id TEXT PRIMARY KEY)",
            ),
            (
                "CREATE TABLE \"nodes\" (id TEXT PRIMARY KEY)",
                "CREATE TABLE empty.\"nodes\" (id TEXT PRIMARY KEY)",
            ),
            (
                "CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY)",
                "CREATE TABLE IF NOT EXISTS empty.\"nodes\" (id TEXT PRIMARY KEY)",
            ),
            (
                "CREATE TABLE nodes (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES \"nodes\" (id))",
                "CREATE TABLE empty.\"nodes\" (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES \"nodes\" (id))",
            ),
        ];

        for (create, expected) in cases {
            let rewritten = rewrite_create_into_schema(create, "nodes", "empty").expect("rewrite");
            assert_eq!(rewritten, expected);
        }
    }

    #[test]
    fn create_table_rewrite_rejects_mismatched_table_token() {
        let err = rewrite_create_into_schema(
            "CREATE TABLE other_nodes (id TEXT PRIMARY KEY)",
            "nodes",
            "empty",
        )
        .expect_err("mismatched table token must fail");
        assert!(
            matches!(
                err,
                GateError::BadCreateTableSql { ref table, ref sql }
                    if table == "nodes"
                        && sql == "CREATE TABLE other_nodes (id TEXT PRIMARY KEY)"
            ),
            "unexpected error: {err}"
        );
    }
}
