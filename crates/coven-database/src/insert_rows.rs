//! Statements that insert many rows at once, a chunk of rows to each.

use crate::sqlite::StoreSql;
use crate::DbError;

/// The most parameters one statement takes.
pub(crate) const PARAMETERS: usize = 32_766;

/// The statement `statement` builds around one `(?,..)` group for each of
/// `rows`, each of `arity` values, as one statement for each chunk of rows,
/// and the rows each statement's `RETURNING`, if it has one, gives back.
pub(crate) fn insert_rows<T>(
    db: &StoreSql,
    arity: usize,
    rows: &[Vec<sqlive::rusqlite::types::Value>],
    statement: impl Fn(&str) -> String,
    returned: impl Fn(&sqlive::rusqlite::Row<'_>) -> sqlive::rusqlite::Result<T>,
) -> Result<Vec<T>, DbError> {
    let mut results = Vec::new();
    for chunk in rows.chunks(PARAMETERS / arity) {
        let group = format!("({})", vec!["?"; arity].join(","));
        let values = vec![group; chunk.len()].join(",");
        results.extend(db.query(
            &statement(&values),
            sqlive::rusqlite::params_from_iter(chunk.iter().flatten()),
            &returned,
        )?);
    }
    Ok(results)
}

pub(crate) fn value(
    parameter: &dyn sqlive::rusqlite::ToSql,
) -> Result<sqlive::rusqlite::types::Value, DbError> {
    match parameter.to_sql()? {
        sqlive::rusqlite::types::ToSqlOutput::Owned(value) => Ok(value),
        sqlive::rusqlite::types::ToSqlOutput::Borrowed(value) => {
            sqlive::rusqlite::types::FromSql::column_result(value).map_err(|error| {
                sqlive::rusqlite::Error::FromSqlConversionFailure(
                    0,
                    value.data_type(),
                    Box::new(error),
                )
                .into()
            })
        }
        _ => Err(DbError::Invariant { what: "a queue parameter is a plain value" }),
    }
}
