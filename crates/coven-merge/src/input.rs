use crate::{MergeError, Timestamp};
use coven_foundation::id_source::{CircleId, DeviceId};
use std::collections::{BTreeMap, BTreeSet};

/// A foreign key's columns in declaration order (Appendix B.5).
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

/// A unique constraint's ordered terms and optional partial predicate (Appendix B.5).
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
        Self { terms: terms.into_iter().map(str::to_owned).collect(), partial: None }
    }
}

/// A foreign key's stable identity, independent of SQLite's positional id (Appendix B.5).
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

/// One device's numbered write (§4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteId {
    /// The install's 64-bit device id.
    pub device: DeviceId,
    /// The device's original write number.
    pub number: u64,
}

/// A positive position in the store's shared sequence (§5). Slot 0 names
/// nothing: a prefix applied through 0 has applied no slot, and that count
/// stays a plain `u64`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slot(std::num::NonZeroU64);

impl Slot {
    /// The first slot of a sequence.
    pub const FIRST: Self = Self(std::num::NonZeroU64::MIN);

    /// The slot numbered `number`, or `None` for 0.
    pub fn new(number: u64) -> Option<Self> {
        std::num::NonZeroU64::new(number).map(Self)
    }

    /// The slot's number, at least 1.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// The slots from a first one through a last number, in order: empty when
/// the first lies past the last. The last stays a plain number, since 0
/// (nothing applied) ends an empty range.
#[derive(Clone, Copy, Debug)]
pub struct SlotRange {
    next: Option<Slot>,
    last: u64,
}

impl SlotRange {
    /// The slots from `first` through slot number `last`.
    pub fn new(first: Slot, last: u64) -> Self {
        Self { next: Some(first).filter(|first| first.get() <= last), last }
    }
}

impl Iterator for SlotRange {
    type Item = Slot;

    fn next(&mut self) -> Option<Slot> {
        let slot = self.next?;
        self.next = slot.get().checked_add(1).filter(|next| *next <= self.last).and_then(Slot::new);
        Some(slot)
    }
}

impl From<std::num::NonZeroU64> for Slot {
    fn from(number: std::num::NonZeroU64) -> Self {
        Self(number)
    }
}

impl From<Slot> for std::num::NonZeroU64 {
    fn from(slot: Slot) -> Self {
        slot.0
    }
}

/// Test support: the slot numbered `number`, which must be positive.
#[cfg(any(test, feature = "test-utils"))]
pub fn slot(number: u64) -> Slot {
    Slot::new(number).expect("a test slot number is positive")
}

/// Every synced row reaches the store or one circle (§15).
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

/// The parent and odd incarnation a reference names (§9.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parent {
    /// The referenced row, including its audience.
    pub row: RowId,
    /// The parent's generation.
    pub generation: u64,
}

impl Parent {
    /// Validate a written reference from `child`: it must name an odd
    /// incarnation in the child's audience or the store (§9.1, §15.3).
    pub fn validate_written(&self, child: &RowId) -> Result<(), MergeError> {
        if !crate::generation_present(self.generation) {
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
    /// Each cell retains the complete parent identity recorded by its setter.
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
    /// written parent's generation and audience for this row (§8.3–§9.1, §15).
    /// Values and their encodings are opaque here. The authoring database
    /// establishes generation witnesses; [`crate::apply`] checks the received
    /// change against applied metadata.
    pub fn validate(&self, row: &RowId) -> Result<(), MergeError> {
        self.incarnation()?;
        self.after()?;
        if let Some(columns) = self.operation.columns() {
            for value in columns.values() {
                for parent in value.parents.values() {
                    parent.validate_written(row)?;
                }
            }
        }
        Ok(())
    }

    /// The row's generation after this change: an insert or a delete advances
    /// it, an update keeps it (§8.3).
    pub fn after(&self) -> Result<u64, MergeError> {
        if self.operation.advances() {
            self.generation.checked_add(1).ok_or(MergeError::GenerationExhausted)
        } else {
            Ok(self.generation)
        }
    }

    /// The incarnation this change belongs to: generation + 1 for an insert,
    /// generation for an update or delete. Invalid parity is a typed error.
    pub fn incarnation(&self) -> Result<u64, MergeError> {
        if matches!(self.operation, Operation::Insert(_))
            == crate::generation_present(self.generation)
        {
            return Err(MergeError::GenerationParity(self.generation));
        }
        if matches!(self.operation, Operation::Insert(_)) {
            self.generation.checked_add(1).ok_or(MergeError::GenerationExhausted)
        } else {
            Ok(self.generation)
        }
    }
}

/// A decoded write's merge inputs, independent of its storage encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Write<V, P = BTreeSet<WriteId>> {
    /// Device and original write number.
    pub id: WriteId,
    /// Unique total-order timestamp (§7.2).
    pub timestamp: Timestamp,
    /// The writes in the author's `read_through` prefix and its own earlier
    /// writes (§7.1). Consuming a write does not imply retaining its row changes.
    /// The sequence resolves this relation from the prefix and own predecessors.
    pub had_read: P,
    /// At most one change for each table, key and audience.
    pub changes: BTreeMap<RowId, Change<V>>,
}

impl<V, P: WritePast> Write<V, P> {
    /// Check the write's identity and causal timestamp against the applied set.
    /// This also validates writes with no row changes available to the receiver.
    pub fn validate_metadata<O: WriteOracle>(&self, oracle: &O) -> Result<(), O::Error> {
        if oracle.timestamp(self.id)?.is_some() {
            return Err(MergeError::DuplicateWrite(self.id).into());
        }
        if self.timestamp.device() != self.id.device {
            return Err(MergeError::TimestampDevice(self.id).into());
        }
        for past in self.had_read.frontier() {
            if crate::state::timestamp(oracle, *past)? >= self.timestamp {
                return Err(MergeError::CausalTimestamp(self.id).into());
            }
        }
        Ok(())
    }
}

/// A causally closed authored past. Its frontier bounds the timestamps of
/// every covered write; sequence order itself is not timestamp order.
pub trait WritePast {
    /// Whether the writer had consumed this write's position.
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

/// Access to applied write metadata. The arriving write is not yet in this
/// view. Implementations must report missing metadata, never invent it, and
/// a failure to read it is their own error, never a missing write.
/// No applied row-change history is required by the incremental step.
pub trait WriteOracle {
    /// Reading the metadata failed, or a merge check on it did.
    type Error: From<MergeError>;
    /// The timestamp of an applied write, or `None` if it is not applied.
    fn timestamp(&self, write: WriteId) -> Result<Option<Timestamp>, Self::Error>;
    /// Whether an applied write had read another write.
    fn had_read(&self, reader: WriteId, earlier: WriteId) -> Result<bool, Self::Error>;
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
