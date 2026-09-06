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
    changed_columns: Vec<bool>,
}

impl RowChange {
    /// Build a row change from equally sized column-value and changed-column
    /// vectors. The decoder is the sole producer; keeping the marker beside the
    /// decoded row preserves SQLite's distinction between an unchanged value
    /// copied from the old side and a value written by this UPDATE.
    pub fn new(
        table: String,
        op: ChangeOp,
        columns: Vec<Option<String>>,
        changed_columns: Vec<bool>,
    ) -> Self {
        assert_eq!(
            columns.len(),
            changed_columns.len(),
            "row change values and change markers must have equal lengths"
        );
        Self {
            table,
            op,
            columns,
            changed_columns,
        }
    }

    /// The primary key (column 0).
    pub fn pk(&self) -> Option<&str> {
        self.col(0)
    }

    /// A column value by index.
    pub fn col(&self, i: usize) -> Option<&str> {
        self.columns.get(i).and_then(|c| c.as_deref())
    }

    /// Whether this column was written by the change. Inserts and deletes affect
    /// every value in their row; updates mark only values present on SQLite's new
    /// side, even though [`Self::col`] fills unchanged values from the old side.
    pub fn column_changed(&self, i: usize) -> bool {
        self.changed_columns.get(i).copied().unwrap_or(false)
    }
}
