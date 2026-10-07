//! Owned SQLite storage values for observation. Unlike rusqlite's UTF-8 String
//! value, this representation also retains text containing invalid UTF-8.

use rusqlite::{
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
    ToSql,
};

#[derive(Clone)]
pub(crate) enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl FromSql for SqlValue {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(value.into())
    }
}

impl From<ValueRef<'_>> for SqlValue {
    fn from(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }
}

impl ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

pub(crate) fn parameters<P: rusqlite::Params>(
    connection: &rusqlite::Connection,
    statement: &rusqlite::Statement<'_>,
    params: P,
) -> rusqlite::Result<Vec<SqlValue>> {
    let count = statement.parameter_count();
    let sql = if count == 0 {
        "SELECT NULL WHERE 0".into()
    } else {
        let values = (1..=count)
            .map(|index| {
                // Walking every index preserves anonymous slots and holes, and
                // retains exactly the original names for named binding validation.
                let parameter = statement.parameter_name(index).unwrap_or("?");
                format!("({parameter})")
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("VALUES {values}")
    };
    let mut capture = connection.prepare(&sql)?;
    let values = capture.query_map(params, |row| row.get(0))?.collect();
    values
}
