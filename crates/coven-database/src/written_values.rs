//! Written values the app table displays differently. A reference read as
//! NULL after its parent's deletion is not a cell edit: merge state, losses
//! and snapshots keep what the cell's write set (Appendix B.1, §9.1). A
//! generated column has no cell, so it never has a written value.

use crate::sqlite::StoreSql;
use crate::store_call::StoreCall;
use crate::write_encoding::{sql_value, value};
use crate::write_rows::AppValues;
use crate::DbError;
use coven_format::value::Value;
use coven_merge::RowState;
use sqlive::rusqlite::params;

pub(crate) fn load(database: &StoreSql, row: i64) -> Result<AppValues, DbError> {
    Ok(database
        .query(
            "SELECT c.column_name,v.value FROM _coven_written_values v
         JOIN _coven_columns c ON c.id=v.column_id WHERE v.row_id=?1",
            [row],
            |r| Ok((r.get(0)?, value(r.get_ref(1)?)?)),
        )?
        .into_iter()
        .collect())
}

/// Store the written values of the generation with id `row`. A `fresh`
/// generation (see [`crate::write_commit::StoredGeneration`]) has none to clear.
pub(crate) fn store(
    database: &StoreSql,
    call: &StoreCall<'_>,
    row: i64,
    fresh: bool,
    state: &RowState<Value>,
    displayed: Option<&AppValues>,
) -> Result<(), DbError> {
    // Every column the table displays as written keeps no written value: one
    // statement clears them all, keeping the others, which are stored below.
    let mut kept = Vec::new();
    for (name, cell) in state.cells() {
        let shown = displayed.and_then(|values| values.get(name));
        if shown.is_some_and(|shown| *shown != cell.value.value) {
            let column =
                crate::transaction_constants::column_id(database, call, &state.row().table, name)?;
            database.execute(
                "INSERT INTO _coven_written_values(column_id,row_id,value) VALUES(?1,?2,?3)
                 ON CONFLICT(column_id,row_id) DO UPDATE SET value=excluded.value
                 WHERE value IS NOT excluded.value OR typeof(value)<>typeof(excluded.value)",
                params![column, row, sql_value(&cell.value.value)],
            )?;
            kept.push(column);
        }
    }
    if fresh {
        return Ok(());
    }
    let others = vec!["?"; kept.len()].join(",");
    database.execute(
        &format!(
            "DELETE FROM _coven_written_values WHERE row_id=? AND column_id NOT IN ({others})"
        ),
        sqlive::rusqlite::params_from_iter(std::iter::once(row).chain(kept)),
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "written_values_tests.rs"]
pub(crate) mod tests;
