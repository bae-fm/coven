//! Row changes reported by synchronization operations.

/// The operation type for a changeset entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// One row change extracted from a changeset.
///
/// `columns` holds the row's column values in schema order. Inserts and updates
/// contain the resulting values; deletes contain the removed values. Unchanged
/// update columns are filled from the old side so primary keys and foreign keys
/// remain available.
///
/// `None` means SQL NULL or a column absent from the changeset.
#[derive(Debug, Clone)]
pub struct RowChange {
    pub table: String,
    pub op: ChangeOp,
    pub columns: Vec<Option<String>>,
}

impl RowChange {
    /// The primary key (column 0).
    pub fn pk(&self) -> Option<&str> {
        self.col(0)
    }

    /// A column value by index.
    pub fn col(&self, i: usize) -> Option<&str> {
        self.columns.get(i).and_then(|c| c.as_deref())
    }
}
