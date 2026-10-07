//! Written references hidden by a derived NULL are not cell edits.

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
            "SELECT c.column_name,v.value FROM _coven_reference_values v
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
    for (name, cell) in state.cells() {
        let column: i64 = database.query_row(
            "SELECT id FROM _coven_columns WHERE table_name=?1 AND column_name=?2",
            params![state.row().table, name],
            |r| r.get(0),
        )?;
        if !cell.value.parents.is_empty()
            && displayed.is_some_and(|values| cell.value.value != values[name])
        {
            database.internal_execute(
                "INSERT INTO _coven_reference_values(column_id,row_id,value) VALUES(?1,?2,?3)
                 ON CONFLICT(column_id,row_id) DO UPDATE SET value=excluded.value
                 WHERE value IS NOT excluded.value OR typeof(value)<>typeof(excluded.value)",
                params![column, row, sql_value(&cell.value.value)],
            )?;
        } else {
            database.internal_execute(
                "DELETE FROM _coven_reference_values WHERE column_id=?1 AND row_id=?2",
                params![column, row],
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "reference_values_tests.rs"]
mod tests;
