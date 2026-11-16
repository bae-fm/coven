//! Primary-key values reported by database validation.

/// The primary-key values of a row, in the table's declared key order (api.md).
#[derive(Clone, Debug, PartialEq)]
pub struct RowKey(pub(crate) Vec<sqlive::rusqlite::types::Value>);

impl RowKey {
    /// A merge row's encoded key as SQL values.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    pub(crate) fn decode(key: &[u8]) -> Result<Self, crate::DbError> {
        Ok(Self(
            coven_format::key::decode_key(key)
                .map_err(|_| crate::DbError::DamagedDatabase)?
                .iter()
                .map(crate::write_encoding::sql_value)
                .collect(),
        ))
    }

    /// The SQL values identifying the row.
    pub fn values(&self) -> &[sqlive::rusqlite::types::Value] {
        &self.0
    }
}

/// Reads a key as `coven_key` gives it (api.md E4): coven's encoding of a
/// row's key (format.md D3). A value that isn't one is
/// `DamagedDatabase`.
impl sqlive::rusqlite::types::FromSql for RowKey {
    fn column_result(
        value: sqlive::rusqlite::types::ValueRef<'_>,
    ) -> sqlive::rusqlite::types::FromSqlResult<Self> {
        Self::decode(value.as_blob()?)
            .map_err(|error| sqlive::rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

impl From<&str> for RowKey {
    fn from(value: &str) -> Self {
        Self(vec![sqlive::rusqlite::types::Value::Text(value.into())])
    }
}

impl From<coven_foundation::uuid_text::UuidText> for RowKey {
    fn from(value: coven_foundation::uuid_text::UuidText) -> Self {
        Self(vec![value.into()])
    }
}

impl<A: Into<sqlive::rusqlite::types::Value>, B: Into<sqlive::rusqlite::types::Value>> From<(A, B)>
    for RowKey
{
    fn from((first, second): (A, B)) -> Self {
        Self(vec![first.into(), second.into()])
    }
}

impl From<Vec<sqlive::rusqlite::types::Value>> for RowKey {
    fn from(values: Vec<sqlive::rusqlite::types::Value>) -> Self {
        Self(values)
    }
}
