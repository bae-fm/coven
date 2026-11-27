//! A table's unique keys, which tell its rows apart, and the columns of them
//! that hold no NULL.

use std::collections::BTreeSet;

use crate::comparison::BINARY;
use crate::schema_objects::{declared_columns, ROWID_NAMES};
use rusqlite::Connection;

/// The name a key holds for a table's rowid, whichever name a statement
/// gives it: no column's name holds a NUL.
pub(crate) const ROWID: &str = "\0rowid";

/// Whether `column`, lowercase, names the rowid of `table`, a table of
/// the main schema: its INTEGER PRIMARY KEY column, or `rowid`, `oid` or
/// `_rowid_` where no column has that name. A WITHOUT ROWID table has no
/// rowid.
pub(crate) fn is_rowid(
    connection: &Connection,
    table: &str,
    column: &str,
) -> rusqlite::Result<bool> {
    let without_rowid: bool = connection
        .prepare_cached("SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?1")?
        .query_row([table], |row| row.get(0))?;
    if without_rowid {
        return Ok(false);
    }
    if ROWID_NAMES.contains(&column) {
        return Ok(!declared_columns(connection, table)?
            .iter()
            .any(|found| found.name.eq_ignore_ascii_case(column)));
    }
    Ok(rowid_column(connection, table)?.is_some_and(|key| key.eq_ignore_ascii_case(column)))
}

/// The column of a rowid table that holds its rowid: its single primary
/// key column, when SQLite keeps no index for the key: `INTEGER PRIMARY
/// KEY`, but not `INT PRIMARY KEY` or `INTEGER PRIMARY KEY DESC`.
fn rowid_column(connection: &Connection, table: &str) -> rusqlite::Result<Option<String>> {
    let mut keys = connection
        .prepare_cached("SELECT name FROM pragma_table_info(?1,'main') WHERE pk > 0")?
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let indexed: bool = connection
        .prepare_cached(
            "SELECT EXISTS (SELECT 1 FROM pragma_index_list(?1,'main') WHERE origin='pk')",
        )?
        .query_row([table], |row| row.get(0))?;
    Ok(if !indexed && keys.len() == 1 { keys.pop() } else { None })
}

/// The unique keys of `table`, each as its columns, lowercase, with the
/// collation that tells their values apart: its rowid, and each unique
/// index over columns, with no WHERE. A partial index and one over an
/// expression let two rows hold the same values of its columns.
pub(crate) fn unique_keys(
    connection: &Connection,
    table: &str,
) -> rusqlite::Result<Vec<Vec<(String, String)>>> {
    let without_rowid: bool = connection
        .prepare_cached("SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?1")?
        .query_row([table], |row| row.get(0))?;
    let mut keys = Vec::new();
    if !without_rowid {
        keys.push(vec![(ROWID.to_owned(), BINARY.to_owned())]);
    }
    let indexes = connection
        .prepare_cached(
            "SELECT name FROM pragma_index_list(?1,'main') WHERE \"unique\" AND NOT partial",
        )?
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for index in indexes {
        let columns = connection
            .prepare_cached(
                "SELECT name, coll FROM pragma_index_xinfo(?1,'main') WHERE key ORDER BY seqno",
            )?
            .query_map([index], |row| {
                Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let key: Option<Vec<(String, String)>> = columns
            .into_iter()
            .map(|(name, collation)| Some((name?.to_ascii_lowercase(), collation)))
            .collect();
        keys.extend(key);
    }
    Ok(keys)
}

/// The columns of `table`, lowercase, that hold no NULL.
pub(crate) fn not_null_columns(
    connection: &Connection,
    table: &str,
) -> rusqlite::Result<BTreeSet<String>> {
    connection
        .prepare_cached("SELECT name FROM pragma_table_xinfo(?1,'main') WHERE \"notnull\"")?
        .query_map([table], |row| Ok(row.get::<_, String>(0)?.to_ascii_lowercase()))?
        .collect()
}
