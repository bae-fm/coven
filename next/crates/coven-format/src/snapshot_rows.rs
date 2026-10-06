//! Snapshot rows use merge's state; writes excluded by schema changes or resets
//! have a separate record because they were never applied to that state.

use crate::error::{require, Error, Rule as FormatRule};
use crate::value::{positive, row, EntryId, Value, WritePositions};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use crate::write::{RowChange, WriteDisposition, WriteHeader};
use coven_merge::{ColumnValue, RowId, RowState, Rule, Timestamp, WriteId, WriteOracle};
use std::collections::{BTreeMap, BTreeSet};

/// One app-visible row, with its original reference metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncedRow {
    /// Table, encoded primary key and audience.
    pub row: RowId,
    /// Columns in name order, carrying merge's value and parent metadata.
    pub columns: BTreeMap<String, ColumnValue<Value>>,
}
wire_struct!(SyncedRow, row, columns);
impl SyncedRow {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        require(
            !self.columns.is_empty(),
            "synced columns",
            FormatRule::Required,
        )?;
        crate::merge_wire::columns(&self.columns)?;
        for value in self.columns.values() {
            for parent in value.parents.values() {
                parent.validate_written(&self.row).map_err(Error::Merge)?;
            }
        }
        Ok(())
    }
}

/// Applied write metadata needed to reconstruct merge's state without write bodies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedWrite {
    /// The applied write.
    pub id: WriteId,
    /// Its timestamp.
    pub timestamp: Timestamp,
    /// Other devices' read positions; its own earlier writes are implicit.
    pub had_read: WritePositions,
}
wire_struct!(AppliedWrite, id, timestamp, had_read);
impl AppliedWrite {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        positive(self.id.number)?;
        require(
            self.timestamp.device() == self.id.device,
            "applied write timestamp",
            FormatRule::TimestampDevice,
        )?;
        self.had_read.own_before(self.id, false)
    }
}

/// A synced column, named independently of device-local numeric ids.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SyncedColumn {
    /// The table.
    pub table: String,
    /// The column.
    pub column: String,
}
wire_struct!(SyncedColumn, table, column);

/// A row's merge state, retained even while removal rules hide it from the app.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeRow {
    /// Merge owns the generations, winning cells and concurrently lost values.
    pub state: RowState<Value>,
    /// Every removal rule holding for this row, in merge's rule order.
    pub removed: BTreeSet<Rule>,
}
impl MergeRow {
    pub(crate) fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.state.row().put(out)?;
        self.state.generations().put(out)?;
        self.state.cells().put(out)?;
        self.state.lost().put(out)?;
        self.removed.put(out)
    }
    pub(crate) fn get(input: &mut Decoder<'_>, oracle: &impl WriteOracle) -> Result<Self, Error> {
        let state = RowState::from_parts(
            Wire::get(input)?,
            Wire::get(input)?,
            Wire::get(input)?,
            Wire::get(input)?,
            oracle,
        )
        .map_err(Error::Merge)?;
        Ok(Self {
            state,
            removed: Wire::get(input)?,
        })
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::merge_wire::state(&self.state)?;
        require(
            self.removed.is_empty() || self.state.present(),
            "removed row",
            FormatRule::Generation,
        )?;
        crate::merge_wire::rules(&self.removed)?;
        for rule in &self.removed {
            match rule {
                Rule::ForeignKey(_) | Rule::Check(_) | Rule::Unique(_) => {}
                Rule::DeletedCircle | Rule::OtherAudience => require(
                    matches!(self.state.row().audience, coven_merge::Audience::Circle(_)),
                    "removed audience",
                    FormatRule::Audience,
                )?,
            }
        }
        Ok(())
    }
}

/// Why an entire write was excluded from the merged state (§17.1, §19.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LostWriteCause {
    /// The schema version reached by a breaking change, including author-marked loss.
    SchemaChange(u32),
    /// A reset whose snapshot dropped one of the write's causes.
    Reset(EntryId),
}
impl LostWriteCause {
    pub(crate) fn validate(self) -> Result<(), Error> {
        match self {
            Self::SchemaChange(version) => {
                require(version > 0, "breaking schema version", FormatRule::Required)
            }
            Self::Reset(entry) => entry.validate(),
        }
    }
}
impl Wire for LostWriteCause {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::SchemaChange(e) => {
                0u8.put(out)?;
                e.put(out)
            }
            Self::Reset(e) => {
                1u8.put(out)?;
                e.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::SchemaChange(Wire::get(input)?)),
            1 => Ok(Self::Reset(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "lost write cause",
                tag,
            }),
        }
    }
}

/// Header of an excluded write, followed by its declared row-change records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LostWrite {
    /// The original write header.
    pub header: WriteHeader,
    /// The snapshot audience whose excluded rows follow.
    pub audience: coven_merge::Audience,
    /// Number of following row records, without a per-write collection bound.
    pub row_count: u64,
    /// The breaking schema version or reset entry that excluded it.
    pub cause: LostWriteCause,
}
wire_struct!(LostWrite, header, audience, row_count, cause);
impl LostWrite {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.header.validate()?;
        self.header.disposition.validate_parts(1)?;
        require(self.row_count > 0, "lost write rows", FormatRule::Required)?;
        self.cause.validate()?;
        if let WriteDisposition::Lost(version) = self.header.disposition {
            require(
                self.cause == LostWriteCause::SchemaChange(version),
                "lost write disposition",
                FormatRule::LostWriteCause,
            )?;
        }
        Ok(())
    }
}

/// One row belonging to the immediately preceding lost-write header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LostWriteRow {
    /// Its next row change in strictly increasing row-identity order.
    pub change: RowChange,
}
wire_struct!(LostWriteRow, change);
