//! Written values the app table displays differently. A reference read as
//! NULL after its parent's deletion, or a generated column SQLite recomputes
//! from merged inputs, is not a cell edit: merge state, losses and snapshots
//! keep what the cell's write set (Appendix B.1, §9.1).

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{sql_value, value};
use crate::write_rows::AppValues;
use crate::DbError;
use coven_format::value::Value;
use coven_merge::RowState;
use rusqlite::params;

pub(crate) fn load(database: &DatabaseConnection, row: i64) -> Result<AppValues, DbError> {
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

pub(crate) fn store(
    database: &DatabaseConnection,
    row: i64,
    state: &RowState<Value>,
    displayed: Option<&AppValues>,
) -> Result<(), DbError> {
    // Every column the table displays as written keeps no written value: one
    // statement clears them all, keeping the others, which are stored below.
    let mut kept = Vec::new();
    for (name, cell) in state.cells() {
        let shown = displayed.and_then(|values| values.get(name));
        if shown.is_some_and(|shown| *shown != cell.value.value) {
            let column = database.column_id(&state.row().table, name)?;
            database.internal_execute(
                "INSERT INTO _coven_written_values(column_id,row_id,value) VALUES(?1,?2,?3)
                 ON CONFLICT(column_id,row_id) DO UPDATE SET value=excluded.value
                 WHERE value IS NOT excluded.value OR typeof(value)<>typeof(excluded.value)",
                params![column, row, sql_value(&cell.value.value)],
            )?;
            kept.push(column);
        }
    }
    let others = vec!["?"; kept.len()].join(",");
    database.internal_execute(
        &format!(
            "DELETE FROM _coven_written_values WHERE row_id=? AND column_id NOT IN ({others})"
        ),
        rusqlite::params_from_iter(std::iter::once(row).chain(kept)),
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "written_values_tests.rs"]
mod tests;
