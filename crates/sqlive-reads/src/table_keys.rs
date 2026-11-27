//! A table's unique keys, which tell its rows apart, and the columns of them
//! that hold no NULL.

use std::collections::BTreeSet;

use crate::comparison::BINARY;
use crate::schema_objects::{SchemaLookups, ROWID_NAMES};

/// The name a key holds for a table's rowid, whichever name a statement
/// gives it: no column's name holds a NUL.
pub(crate) const ROWID: &str = "\0rowid";

/// Whether `column`, lowercase, names the rowid of `table`, a table of
/// the main schema: its INTEGER PRIMARY KEY column, or `rowid`, `oid` or
/// `_rowid_` where no column has that name. A WITHOUT ROWID table has no
/// rowid.
pub(crate) fn is_rowid(
    schema: &SchemaLookups<'_>,
    table: &str,
    column: &str,
) -> rusqlite::Result<bool> {
    if schema.without_rowid(table)? {
        return Ok(false);
    }
    if ROWID_NAMES.contains(&column) {
        return Ok(!schema
            .columns(table)?
            .iter()
            .any(|found| found.name.eq_ignore_ascii_case(column)));
    }
    Ok(rowid_column(schema, table)?.is_some_and(|key| key.eq_ignore_ascii_case(column)))
}

/// The column of a rowid table that holds its rowid: its single primary
/// key column, when SQLite keeps no index for the key: `INTEGER PRIMARY
/// KEY`, but not `INT PRIMARY KEY` or `INTEGER PRIMARY KEY DESC`.
pub(crate) fn rowid_column<'s>(
    schema: &'s SchemaLookups<'s>,
    table: &str,
) -> rusqlite::Result<Option<&'s str>> {
    let mut keys = schema.columns(table)?.iter().filter(|column| column.primary_key);
    let (Some(key), None) = (keys.next(), keys.next()) else { return Ok(None) };
    let indexed = schema.indexes(table)?.iter().any(|index| index.primary_key);
    Ok((!indexed).then_some(key.name.as_str()))
}

/// The unique keys of `table`, each as its columns, lowercase, with the
/// collation that tells their values apart: its rowid, and each unique
/// index over columns, with no WHERE. A partial index and one over an
/// expression let two rows hold the same values of its columns.
pub(crate) fn unique_keys(
    schema: &SchemaLookups<'_>,
    table: &str,
) -> rusqlite::Result<Vec<Vec<(String, String)>>> {
    let mut keys = Vec::new();
    if !schema.without_rowid(table)? {
        keys.push(vec![(ROWID.to_owned(), BINARY.to_owned())]);
    }
    for index in schema.indexes(table)? {
        let key: Option<Vec<(String, String)>> = index.unique.as_ref().and_then(|columns| {
            columns
                .iter()
                .map(|(name, collation)| {
                    Some((name.as_ref()?.to_ascii_lowercase(), collation.clone()))
                })
                .collect()
        });
        keys.extend(key);
    }
    Ok(keys)
}

/// The columns of `table`, lowercase, that hold no NULL.
pub(crate) fn not_null_columns(
    schema: &SchemaLookups<'_>,
    table: &str,
) -> rusqlite::Result<BTreeSet<String>> {
    Ok(schema
        .columns(table)?
        .iter()
        .filter(|column| column.not_null)
        .map(|column| column.name.to_ascii_lowercase())
        .collect())
}
