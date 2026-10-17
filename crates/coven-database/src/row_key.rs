//! Primary-key values reported by database validation.

/// The primary-key values of a row, in the table's declared key order (api.md).
#[derive(Clone, Debug, PartialEq)]
pub struct RowKey(pub(crate) Vec<rusqlite::types::Value>);

impl RowKey {
    /// A merge row's encoded key as SQL values.
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
    pub fn values(&self) -> &[rusqlite::types::Value] {
        &self.0
    }
}

impl From<&str> for RowKey {
    fn from(value: &str) -> Self {
        Self(vec![rusqlite::types::Value::Text(value.into())])
    }
}

impl From<coven_foundation::uuid_text::UuidText> for RowKey {
    fn from(value: coven_foundation::uuid_text::UuidText) -> Self {
        Self(vec![value.into()])
    }
}

impl<A: Into<rusqlite::types::Value>, B: Into<rusqlite::types::Value>> From<(A, B)> for RowKey {
    fn from((first, second): (A, B)) -> Self {
        Self(vec![first.into(), second.into()])
    }
}

impl From<Vec<rusqlite::types::Value>> for RowKey {
    fn from(values: Vec<rusqlite::types::Value>) -> Self {
        Self(values)
    }
}
