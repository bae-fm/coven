//! Owned SQLite storage values for observation. Unlike rusqlite's UTF-8 String
//! value, this representation also retains text containing invalid UTF-8.

use rusqlite::{
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
    ToSql,
};

/// A value SQLite stores.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SqlValue {
    /// NULL.
    Null,
    /// An integer.
    Integer(i64),
    /// A floating-point number, encoded by its bits, which JSON keeps
    /// for every value, an infinity included.
    Real(#[serde(with = "crate::compiled_plan::float_bits")] f64),
    /// Text, as the bytes SQLite stores, which may not be valid UTF-8.
    Text(Vec<u8>),
    /// A blob.
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

impl SqlValue {
    /// The value as SQLite reports it.
    pub fn as_value_ref(&self) -> ValueRef<'_> {
        match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }
    }
}

impl ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(self.as_value_ref()))
    }
}

/// The values `params` binds to `statement`'s parameters, in parameter order,
/// with one entry for each index, including anonymous parameters and indices
/// that no parameter in the SQL uses.
pub fn parameters<P: rusqlite::Params>(
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
