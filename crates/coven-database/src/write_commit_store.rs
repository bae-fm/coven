//! A write's merge records in statements across its rows: the generations,
//! cells and references `persist` stores, each a multi-row statement for a
//! chunk.
use crate::sqlite::DatabaseConnection;
use crate::write_commit::StoredGeneration;
use crate::DbError;
use coven_merge::{Parent, RowId};
use rusqlite::params;
use std::collections::BTreeMap;

/// Store the generations `stores` names, each a row, its generation number
/// and the ordinal of the write that started it, in one statement for each
/// chunk. Returns each one's [`StoredGeneration`]: fresh when inserted, and
/// otherwise found stored and given the new writer.
pub(super) fn store_generations<'a>(
    database: &DatabaseConnection,
    stores: &[(&'a RowId, u64, i64)],
) -> Result<BTreeMap<(&'a RowId, u64), StoredGeneration>, DbError> {
    let mut stored = BTreeMap::new();
    for chunk in stores.chunks(GENERATION_CHUNK) {
        let values = vec!["(?,?,?,?,?)"; chunk.len()].join(",");
        let parameters = chunk.iter().flat_map(|(row, generation, writer)| {
            [
                rusqlite::types::Value::Text(row.table.clone()),
                rusqlite::types::Value::Blob(row.key.clone()),
                rusqlite::types::Value::from(&row.audience),
                rusqlite::types::Value::Blob(generation.to_be_bytes().to_vec()),
                rusqlite::types::Value::Integer(*writer),
            ]
        });
        let inserted: Vec<(RowId, Vec<u8>, i64)> = database.query(
            &format!("INSERT INTO _coven_rows(table_name,key,audience,generation,write_id) VALUES{values} ON CONFLICT(table_name,key,audience,generation) DO NOTHING RETURNING table_name,key,audience,generation,id"),
            rusqlite::params_from_iter(parameters),
            |r| Ok((crate::row_queries::read_identity(r)?, r.get(3)?, r.get(4)?)),
        )?;
        let inserted: BTreeMap<(RowId, Vec<u8>), i64> =
            inserted.into_iter().map(|(row, generation, id)| ((row, generation), id)).collect();
        for (row, generation, writer) in chunk {
            let id = match inserted.get(&((*row).clone(), generation.to_be_bytes().to_vec())) {
                Some(id) => StoredGeneration { ordinal: *id, fresh: true },
                None => StoredGeneration {
                    ordinal: database.query_row("UPDATE _coven_rows SET write_id=?5 WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4 RETURNING id",params![row.table,row.key,&row.audience,generation.to_be_bytes().as_slice(),writer],|r| r.get(0))?,
                    fresh: false,
                },
            };
            stored.insert((*row, *generation), id);
        }
    }
    Ok(stored)
}

/// The references the rows' cells now hold, each a row id, a column id, a
/// foreign key id and the parent generation named, as one upsert statement for
/// each chunk. Each statement is the one-row upsert for every row of its
/// chunk, in the order the rows were visited.
pub(super) fn store_references(
    database: &DatabaseConnection,
    references: &[(i64, i64, i64, &Parent)],
) -> Result<(), DbError> {
    for chunk in references.chunks(REFERENCE_CHUNK) {
        let values = vec!["(?,?,?,?,?,?,?)"; chunk.len()].join(",");
        let parameters = chunk.iter().flat_map(|(row, column, key, parent)| {
            [
                rusqlite::types::Value::Integer(*row),
                rusqlite::types::Value::Integer(*column),
                rusqlite::types::Value::Integer(*key),
                rusqlite::types::Value::Text(parent.row.table.clone()),
                rusqlite::types::Value::Blob(parent.row.key.clone()),
                rusqlite::types::Value::from(&parent.row.audience),
                rusqlite::types::Value::Blob(parent.generation.to_be_bytes().to_vec()),
            ]
        });
        database.internal_execute(
            &format!("INSERT INTO _coven_references(row_id,column_id,foreign_key_id,parent_table,parent_key,parent_audience,parent_generation) VALUES{values} ON CONFLICT(row_id,column_id,foreign_key_id) DO UPDATE SET parent_table=excluded.parent_table,parent_key=excluded.parent_key,parent_audience=excluded.parent_audience,parent_generation=excluded.parent_generation"),
            rusqlite::params_from_iter(parameters),
        )?;
    }
    Ok(())
}

/// How many references one `store_references` statement upserts.
const REFERENCE_CHUNK: usize = 1000;

/// How many generations one `store_generations` statement inserts.
const GENERATION_CHUNK: usize = 500;

/// The cells the rows' writes set, each a column id, a row id and the write's
/// ordinal, as one statement for each chunk. SQLite takes up to 32,766
/// parameters a statement.
pub(super) fn store_cells(
    database: &DatabaseConnection,
    cells: &[(i64, i64, i64)],
) -> Result<(), DbError> {
    for chunk in cells.chunks(10_000) {
        let values = vec!["(?,?,?)"; chunk.len()].join(",");
        database.internal_execute(
            &format!("INSERT INTO _coven_cells(column_id,row_id,write_id) VALUES{values} ON CONFLICT(column_id,row_id) DO UPDATE SET write_id=excluded.write_id"),
            rusqlite::params_from_iter(chunk.iter().flat_map(|(column, row, write)| [*column, *row, *write])),
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "write_commit_store_tests.rs"]
pub(crate) mod tests;
