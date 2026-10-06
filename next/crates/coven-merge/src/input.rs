use crate::{MergeError, Timestamp};
use coven_foundation::id_source::{CircleId, DeviceId};
use std::collections::{BTreeMap, BTreeSet};

/// A foreign key's columns in declaration order (§8).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConstraintColumns(
    /// Ordered column names, retaining composite-constraint boundaries.
    pub Vec<String>,
);

impl<const N: usize> From<[&str; N]> for ConstraintColumns {
    fn from(columns: [&str; N]) -> Self {
        Self(columns.into_iter().map(str::to_owned).collect())
    }
}

/// A unique constraint's ordered terms and optional partial predicate (§8).
/// Column terms use their names; expression terms retain their SQL text.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UniqueConstraint {
    /// Column names or expression text, in declaration order.
    pub terms: Vec<String>,
    /// The WHERE expression as written for a partial unique index.
    pub partial: Option<String>,
}

impl<const N: usize> From<[&str; N]> for UniqueConstraint {
    fn from(terms: [&str; N]) -> Self {
        Self {
            terms: terms.into_iter().map(str::to_owned).collect(),
            partial: None,
        }
    }
}

/// A foreign key's stable identity, independent of SQLite's positional id (§8).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ForeignKey {
    /// Referencing columns in declaration order.
    pub columns: ConstraintColumns,
    /// Referenced table.
    pub parent: String,
    /// Referenced columns, paired in order with `columns`.
    pub parent_columns: ConstraintColumns,
}

impl ForeignKey {
    /// Name both sides of a declared reference, including implicit primary keys.
    pub fn new(
        columns: impl Into<ConstraintColumns>,
        parent: impl Into<String>,
        parent_columns: impl Into<ConstraintColumns>,
    ) -> Self {
        Self {
            columns: columns.into(),
            parent: parent.into(),
            parent_columns: parent_columns.into(),
        }
    }
}

/// One device's numbered write (§5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteId {
    /// The install's 64-bit device id.
    pub device: DeviceId,
    /// The write's number in that device's log.
    pub number: u64,
}

/// Every synced row reaches the store or one circle (§14).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Audience {
    /// Every member can read the row.
    Store,
    /// Only members of this circle can read the row.
    Circle(CircleId),
}

/// A row is one table, primary key and audience, with generations of its own.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId {
    /// The synced table's name.
    pub table: String,
    /// The primary key, encoded by the caller so byte order is key order.
    pub key: Vec<u8>,
    /// The store or circle that can read the row.
    pub audience: Audience,
}

/// The parent and generation a reference names (§8.4). Written references
/// carry odd incarnations; a resolved default carries the current generation,
/// including an even generation while that parent is absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parent {
    /// The referenced row, including its audience.
    pub row: RowId,
    /// The parent's generation.
    pub generation: u64,
}

impl Parent {
    /// Validate a written reference from `child`: it must name an odd
    /// incarnation in the child's audience or the store (§8.4, §14.1).
    /// Resolved default references may name even generations and are not
    /// written references; this check does not apply to those substitutions.
    pub fn validate_written(&self, child: &RowId) -> Result<(), MergeError> {
        if self.generation.is_multiple_of(2) {
            return Err(MergeError::ParentGeneration(self.generation));
        }
        validate_audience(child, &self.row)
    }
}

/// A column's value and the parent generations recorded with it.
/// The metadata follows the winning setter, including for values kept as lost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnValue<V> {
    /// The app's value; the merge does not interpret SQL values.
    pub value: V,
    /// Foreign-key names and their parents for this column's written value.
    /// For a composite reference the database combines the winning columns.
    pub parents: BTreeMap<ForeignKey, Parent>,
}

/// One row's insert, update or delete. A delete cannot set columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation<V> {
    /// Start the next incarnation with these columns.
    Insert(BTreeMap<String, ColumnValue<V>>),
    /// Set these columns in the existing incarnation.
    Update(BTreeMap<String, ColumnValue<V>>),
    /// End the existing incarnation.
    Delete,
}

impl<V> Operation<V> {
    pub(crate) fn columns(&self) -> Option<&BTreeMap<String, ColumnValue<V>>> {
        match self {
            Self::Insert(c) | Self::Update(c) => Some(c),
            Self::Delete => None,
        }
    }
    pub(crate) fn advances(&self) -> bool {
        !matches!(self, Self::Update(_))
    }
}

/// A row change carries the generation on the authoring device (§8.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change<V> {
    /// Generation at which the change was made.
    pub generation: u64,
    /// The change and the values it sets.
    pub operation: Operation<V>,
}

impl<V> Change<V> {
    /// Validate generation parity, advancement without overflow, and every
    /// written parent's generation and audience for this row (§8.3–§8.4, §14).
    /// Values and their encodings are opaque here. Generation witnesses and
    /// causal history require [`crate::History`] or [`crate::apply`].
    pub fn validate(&self, row: &RowId) -> Result<(), MergeError> {
        self.incarnation()?;
        if self.operation.advances() {
            self.generation
                .checked_add(1)
                .ok_or(MergeError::GenerationExhausted)?;
        }
        if let Some(columns) = self.operation.columns() {
            for value in columns.values() {
                for parent in value.parents.values() {
                    parent.validate_written(row)?;
                }
            }
        }
        Ok(())
    }

    /// The incarnation this change belongs to: generation + 1 for an insert,
    /// generation for an update or delete. Invalid parity is a typed error.
    pub fn incarnation(&self) -> Result<u64, MergeError> {
        if matches!(self.operation, Operation::Insert(_)) != self.generation.is_multiple_of(2) {
            return Err(MergeError::GenerationParity(self.generation));
        }
        if matches!(self.operation, Operation::Insert(_)) {
            self.generation
                .checked_add(1)
                .ok_or(MergeError::GenerationExhausted)
        } else {
            Ok(self.generation)
        }
    }
}

/// A decoded write's merge inputs, independent of its storage encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Write<V, P = BTreeSet<WriteId>> {
    /// Device and log number.
    pub id: WriteId,
    /// Unique total-order timestamp (§7.2).
    pub timestamp: Timestamp,
    /// Every write the author had applied, including its own earlier writes.
    /// A position frontier can represent the same relation without expansion.
    pub had_read: P,
    /// At most one change for each table, key and audience.
    pub changes: BTreeMap<RowId, Change<V>>,
}

/// A causally closed past, represented explicitly or by device log positions.
/// A frontier contains maximal writes covering the set: all covered writes
/// have timestamps at most a frontier timestamp. Device logs are contiguous.
pub trait WritePast {
    /// Whether the writer had applied this write.
    fn contains(&self, write: &WriteId) -> bool;
    /// Writes whose causal pasts cover this entire past, including themselves.
    fn frontier(&self) -> impl Iterator<Item = &WriteId>;
}

impl WritePast for BTreeSet<WriteId> {
    fn contains(&self, write: &WriteId) -> bool {
        self.contains(write)
    }
    fn frontier(&self) -> impl Iterator<Item = &WriteId> {
        self.iter()
    }
}

/// Pure access to applied write metadata. The arriving write is not yet in
/// this view. Implementations must report missing metadata, never invent it.
/// No applied row-change history is required by the incremental step.
pub trait WriteOracle {
    /// The timestamp of an applied write, or `None` if it is not applied.
    fn timestamp(&self, write: WriteId) -> Option<Timestamp>;
    /// Whether an applied write had read another write.
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, MergeError>;
}

pub(crate) fn validate_audience(row: &RowId, parent: &RowId) -> Result<(), MergeError> {
    if parent.audience != Audience::Store && parent.audience != row.audience {
        return Err(MergeError::ReferenceAudience(row.clone()));
    }
    Ok(())
}

#[cfg(test)]
#[path = "input_tests.rs"]
mod tests;
