use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension};

#[derive(Debug, thiserror::Error)]
pub enum CreateTableSchemaError {
    #[error("read CREATE TABLE schema for {table:?} failed: {source}")]
    Read {
        table: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("no CREATE TABLE schema for {0}")]
    Missing(String),
    #[error("bad CREATE TABLE SQL for {table}: {sql}")]
    Malformed { table: String, sql: String },
}

/// `CREATE TABLE` text for `table` from `sqlite_master`.
pub(crate) fn create_table_sql(
    conn: &Connection,
    table: &str,
) -> Result<String, CreateTableSchemaError> {
    let create = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?1",
            [table],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(|source| CreateTableSchemaError::Read {
            table: table.to_string(),
            source,
        })?;
    create
        .flatten()
        .ok_or_else(|| CreateTableSchemaError::Missing(table.to_string()))
}

/// Qualify a `CREATE TABLE <name> ...` statement so it builds the table inside
/// the attached schema `alias`, replacing only the table-name token.
pub fn rewrite_create_into_schema(
    create: &str,
    table: &str,
    alias: &str,
) -> Result<String, CreateTableSchemaError> {
    let Some((name_start, name_end, parsed_table)) = create_table_name_token(create) else {
        return Err(CreateTableSchemaError::Malformed {
            table: table.to_string(),
            sql: create.to_string(),
        });
    };
    if parsed_table != table {
        return Err(CreateTableSchemaError::Malformed {
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

/// Column names of `table`, in declared order, via `PRAGMA table_info`. The
/// index of a name here is the index SQLite session changesets report for that
/// column.
pub(crate) fn table_columns(conn: &Connection, table: &str) -> rusqlite::Result<Vec<String>> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let mut stmt = conn.prepare(&sql)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns)
}

/// Quote an SQL identifier (table/column name), doubling any embedded quote, so
/// a trusted-but-unbindable name interpolates safely. Identifiers cannot be
/// passed as bound parameters; this is the safe interpolation path for them.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[derive(Debug, thiserror::Error)]
pub enum ForeignKeySchemaError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("foreign key on {child_table:?} is malformed: {reason}")]
    Malformed { child_table: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForeignKeyColumn {
    pub child: String,
    pub parent: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForeignKeyEdge {
    pub parent_table: String,
    pub columns: Vec<ForeignKeyColumn>,
    pub on_update: String,
    pub on_delete: String,
    pub match_clause: String,
}

struct ForeignKeyRow {
    sequence: i64,
    parent_table: String,
    child_column: String,
    parent_column: Option<String>,
    on_update: String,
    on_delete: String,
    match_clause: String,
}

/// Every outgoing foreign key on `child_table`, grouped by constraint and
/// sorted by its complete parent/column/action shape. SQLite's constraint ids
/// and PRAGMA listing order are deliberately discarded.
pub(crate) fn foreign_key_edges(
    conn: &Connection,
    child_table: &str,
) -> Result<Vec<ForeignKeyEdge>, ForeignKeySchemaError> {
    let sql = format!("PRAGMA foreign_key_list({})", quote_ident(child_table));
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            ForeignKeyRow {
                sequence: row.get(1)?,
                parent_table: row.get(2)?,
                child_column: row.get(3)?,
                parent_column: row.get(4)?,
                on_update: row.get::<_, String>(5)?.to_ascii_uppercase(),
                on_delete: row.get::<_, String>(6)?.to_ascii_uppercase(),
                match_clause: row.get::<_, String>(7)?.to_ascii_uppercase(),
            },
        ))
    })?;
    let mut grouped: BTreeMap<i64, Vec<ForeignKeyRow>> = BTreeMap::new();
    for row in rows {
        let (id, row) = row?;
        grouped.entry(id).or_default().push(row);
    }

    let mut edges = Vec::with_capacity(grouped.len());
    for mut rows in grouped.into_values() {
        rows.sort_by_key(|row| row.sequence);
        let first = rows
            .first()
            .ok_or_else(|| ForeignKeySchemaError::Malformed {
                child_table: child_table.to_string(),
                reason: "constraint has no columns".to_string(),
            })?;
        if rows.iter().any(|row| {
            row.parent_table != first.parent_table
                || row.on_update != first.on_update
                || row.on_delete != first.on_delete
                || row.match_clause != first.match_clause
        }) {
            return Err(ForeignKeySchemaError::Malformed {
                child_table: child_table.to_string(),
                reason: "one constraint reports inconsistent parent or actions".to_string(),
            });
        }
        let omitted_parent_columns = rows.iter().all(|row| row.parent_column.is_none());
        if !omitted_parent_columns && rows.iter().any(|row| row.parent_column.is_none()) {
            return Err(ForeignKeySchemaError::Malformed {
                child_table: child_table.to_string(),
                reason: "one constraint mixes named and omitted parent columns".to_string(),
            });
        }
        let inferred_parent_columns = if omitted_parent_columns {
            primary_key_columns(conn, &first.parent_table)?
        } else {
            Vec::new()
        };
        if omitted_parent_columns && inferred_parent_columns.len() != rows.len() {
            return Err(ForeignKeySchemaError::Malformed {
                child_table: child_table.to_string(),
                reason: format!(
                    "{} child columns reference {} primary-key columns",
                    rows.len(),
                    inferred_parent_columns.len(),
                ),
            });
        }
        let columns = rows
            .iter()
            .enumerate()
            .map(|(position, row)| ForeignKeyColumn {
                child: row.child_column.clone(),
                parent: row
                    .parent_column
                    .clone()
                    .unwrap_or_else(|| inferred_parent_columns[position].clone()),
            })
            .collect();
        edges.push(ForeignKeyEdge {
            parent_table: first.parent_table.clone(),
            columns,
            on_update: first.on_update.clone(),
            on_delete: first.on_delete.clone(),
            match_clause: first.match_clause.clone(),
        });
    }
    edges.sort();
    Ok(edges)
}

fn primary_key_columns(
    conn: &Connection,
    table: &str,
) -> Result<Vec<String>, ForeignKeySchemaError> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, i64>(5)?, row.get::<_, String>(1)?))
    })?;
    let mut columns = rows
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(rank, _)| *rank > 0)
        .collect::<Vec<_>>();
    columns.sort_by_key(|(rank, _)| *rank);
    Ok(columns.into_iter().map(|(_, name)| name).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

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
                CreateTableSchemaError::Malformed { ref table, ref sql }
                    if table == "nodes"
                        && sql == "CREATE TABLE other_nodes (id TEXT PRIMARY KEY)"
            ),
            "unexpected error: {err}"
        );
    }
}
