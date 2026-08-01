use std::collections::BTreeMap;

use rusqlite::Connection;

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
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ForeignKeySchemaError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("foreign key on {child_table:?} is malformed: {reason}")]
    Malformed { child_table: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ForeignKeyColumn {
    pub(crate) child: String,
    pub(crate) parent: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ForeignKeyEdge {
    pub(crate) parent_table: String,
    pub(crate) columns: Vec<ForeignKeyColumn>,
    pub(crate) on_update: String,
    pub(crate) on_delete: String,
    pub(crate) match_clause: String,
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
