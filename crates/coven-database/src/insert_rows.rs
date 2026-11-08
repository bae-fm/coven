//! Statements that insert many rows at once, a chunk of rows to each.

use crate::{sqlite::DatabaseConnection, DbError};

/// The most parameters one statement takes.
pub(crate) const PARAMETERS: usize = 32_766;

/// The statement `statement` builds around one `(?,..)` group for each of
/// `rows`, each of `arity` values, as one statement for each chunk of rows,
/// and the rows each statement's `RETURNING`, if it has one, gives back.
pub(crate) fn insert_rows<T>(
    db: &DatabaseConnection,
    arity: usize,
    rows: &[Vec<rusqlite::types::Value>],
    statement: impl Fn(&str) -> String,
    returned: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>, DbError> {
    let mut results = Vec::new();
    for chunk in rows.chunks(PARAMETERS / arity) {
        let group = format!("({})", vec!["?"; arity].join(","));
        let values = vec![group; chunk.len()].join(",");
        results.extend(db.query(
            &statement(&values),
            rusqlite::params_from_iter(chunk.iter().flatten()),
            &returned,
        )?);
    }
    Ok(results)
}

pub(crate) fn value(parameter: &dyn rusqlite::ToSql) -> Result<rusqlite::types::Value, DbError> {
    match parameter.to_sql()? {
        rusqlite::types::ToSqlOutput::Owned(value) => Ok(value),
        rusqlite::types::ToSqlOutput::Borrowed(value) => {
            rusqlite::types::FromSql::column_result(value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, value.data_type(), Box::new(error))
                    .into()
            })
        }
        _ => Err(DbError::Invariant { what: "a queue parameter is a plain value" }),
    }
}
