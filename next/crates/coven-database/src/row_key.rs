//! Primary-key values reported by database validation.

/// The primary-key values of a row, in the table's declared key order (§20).
#[derive(Clone, Debug, PartialEq)]
pub struct RowKey(pub(crate) Vec<rusqlite::types::Value>);

impl RowKey {
    /// The SQL values identifying the row.
    pub fn values(&self) -> &[rusqlite::types::Value] {
        &self.0
    }
}
