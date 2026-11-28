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

/// The values a statement binds, by position or by name, as a host gives them
/// to a live query's run (SPEC.md section 3), which reads them to narrow what
/// the statement read.
#[derive(Clone, Debug, PartialEq)]
pub enum Values {
    /// The value of each parameter, in parameter order, one for each index.
    Positional(Vec<SqlValue>),
    /// Each parameter's value by its name, with its first character, as
    /// `:id`. A parameter the list doesn't name binds NULL.
    Named(Vec<(&'static str, SqlValue)>),
}

impl Values {
    /// The values `params` names, as rusqlite would bind them.
    ///
    /// # Errors
    ///
    /// Fails when a value's `ToSql` fails, or gives a value SQLite doesn't
    /// store, as an array.
    pub fn named(params: &[(&'static str, &dyn ToSql)]) -> rusqlite::Result<Self> {
        let values = params.iter().map(|(name, value)| Ok((*name, SqlValue::of(*value)?)));
        Ok(Self::Named(values.collect::<rusqlite::Result<_>>()?))
    }

    /// The values `params` binds in order.
    ///
    /// # Errors
    ///
    /// As `named`.
    pub fn positional(params: &[&dyn ToSql]) -> rusqlite::Result<Self> {
        let values = params.iter().map(|value| SqlValue::of(*value));
        Ok(Self::Positional(values.collect::<rusqlite::Result<_>>()?))
    }

    /// The value of each of `statement`'s parameters, in parameter order,
    /// with one entry for each index, including indices that no parameter
    /// in the SQL uses.
    ///
    /// # Errors
    ///
    /// Fails, as rusqlite's binding does, when the values are positional and
    /// their count isn't the statement's, or name a parameter the statement
    /// lacks.
    pub fn of_statement(
        &self,
        statement: &rusqlite::Statement<'_>,
    ) -> rusqlite::Result<Vec<SqlValue>> {
        let count = statement.parameter_count();
        match self {
            Self::Positional(values) if values.len() == count => Ok(values.clone()),
            Self::Positional(values) => {
                Err(rusqlite::Error::InvalidParameterCount(values.len(), count))
            }
            Self::Named(named) => {
                let mut values = vec![SqlValue::Null; count];
                for (name, value) in named {
                    let index = statement
                        .parameter_index(name)?
                        .ok_or_else(|| rusqlite::Error::InvalidParameterName((*name).to_owned()))?;
                    values[index - 1] = value.clone();
                }
                Ok(values)
            }
        }
    }
}

impl SqlValue {
    /// The value `value` binds.
    ///
    /// # Errors
    ///
    /// Fails when `value`'s `ToSql` fails, or gives a value SQLite doesn't
    /// store.
    pub fn of(value: &dyn ToSql) -> rusqlite::Result<Self> {
        Ok(match value.to_sql()? {
            ToSqlOutput::Borrowed(value) => value.into(),
            ToSqlOutput::Owned(value) => ValueRef::from(&value).into(),
            ToSqlOutput::ZeroBlob(length) => {
                Self::Blob(vec![0; usize::try_from(length).map_err(|_too_long| unstorable())?])
            }
            _ => return Err(unstorable()),
        })
    }
}

/// The error of a value SQLite doesn't store.
fn unstorable() -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure("a value SQLite doesn't store".into())
}
