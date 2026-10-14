//! Snapshot rows carry app values and merge generations; losses travel separately.

use crate::error::{require, Error, Rule as FormatRule};
use crate::value::{positive, row, EntryId, Value, WritePositions};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_merge::{ColumnValue, RowId, RowState, Timestamp, WriteId, WriteOracle};
use std::collections::BTreeMap;

/// One app-visible row, with its original reference metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncedRow {
    /// Table, encoded primary key and audience.
    pub row: RowId,
    /// Columns in name order, carrying merge's value and parent metadata.
    pub columns: BTreeMap<String, ColumnValue<Value>>,
}
wire_struct!(SyncedRow, row, columns => crate::wire::get_name_map);
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
        self.had_read.without_own_device(self.id.device)
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
wire_struct!(SyncedColumn, table => crate::wire::get_name, column => crate::wire::get_name);

/// A row's merge state, retained even while removal rules hide it from the app.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeRow {
    /// Generations and winning cells only; losses occupy the loss section.
    pub state: RowState<Value>,
}
impl MergeRow {
    pub(crate) fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.state.row().put(out)?;
        self.state.generations().put(out)?;
        self.state.cells().put(out)
    }
    pub(crate) fn get(input: &mut Decoder<'_>, oracle: &impl WriteOracle) -> Result<Self, Error> {
        let state = RowState::from_parts(
            Wire::get(input)?,
            Wire::get(input)?,
            crate::wire::get_name_map(input)?,
            BTreeMap::new(),
            oracle,
        )
        .map_err(Error::Merge)?;
        Ok(Self { state })
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::merge_wire::state(&self.state)?;
        require(
            self.state.lost().is_empty(),
            "merge losses belong in the loss section",
            FormatRule::Required,
        )?;
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
            Self::Reset(entry) => positive(entry.number),
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

/// How a device interpreted one original row change (§8, §10, §17.1, §19.3).
/// It decides whether the change survives as a source for later changes,
/// independently of dismissible losses and consumed positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeVerdict {
    /// Never interpreted here: a loaded snapshot already carries its effects.
    Unjudged,
    /// It entered the merge.
    Applied,
    /// A schema change or reset excluded its part.
    Excluded(LostWriteCause),
    /// Its write landed outside a retirement window and has no effects.
    Dropped,
    /// It used data that does not survive: these sources, in role order.
    Dependent(Vec<crate::write_sources::ChangeId>),
}
impl ChangeVerdict {
    /// Whether the change survives as data for later changes.
    pub fn retained(&self) -> bool {
        matches!(self, Self::Unjudged | Self::Applied)
    }
}
impl Wire for ChangeVerdict {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Unjudged => 0u8.put(out),
            Self::Applied => 1u8.put(out),
            Self::Excluded(cause) => {
                2u8.put(out)?;
                cause.put(out)
            }
            Self::Dropped => 3u8.put(out),
            Self::Dependent(sources) => {
                4u8.put(out)?;
                sources.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Unjudged),
            1 => Ok(Self::Applied),
            2 => Ok(Self::Excluded(Wire::get(input)?)),
            3 => Ok(Self::Dropped),
            4 => Ok(Self::Dependent(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "change verdict",
                tag,
            }),
        }
    }
}

/// One original row change kept as source evidence (D7 section 5): the
/// authored header of its part, whether that part arrived whole, the original
/// change with its sources, and its author's verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoredChange {
    /// The original write header, before any conversion.
    pub header: crate::write::WriteHeader,
    /// Whether every original change of this part is known, so absence of a
    /// named row is proof rather than a wait.
    pub complete: bool,
    /// The original change, with its generation source and replaced setters.
    pub change: crate::write::RowChange,
    /// Its interpretation by the snapshot's author.
    pub verdict: ChangeVerdict,
}
impl Wire for AuthoredChange {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.header.put(out)?;
        u8::from(self.complete).put(out)?;
        self.change.put(out)?;
        self.verdict.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let header = Wire::get(input)?;
        let complete = match u8::get(input)? {
            0 => false,
            1 => true,
            tag => {
                return Err(Error::UnknownTag {
                    field: "authored part completeness",
                    tag,
                })
            }
        };
        Ok(Self {
            header,
            complete,
            change: Wire::get(input)?,
            verdict: Wire::get(input)?,
        })
    }
}
impl AuthoredChange {
    /// The original change's identity.
    pub fn id(&self) -> crate::write_sources::ChangeId {
        crate::write_sources::ChangeId {
            write: self.header.position,
            row: self.change.row.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.header.validate()?;
        self.change.validate()?;
        match &self.verdict {
            ChangeVerdict::Excluded(cause) => cause.validate(),
            ChangeVerdict::Dependent(sources) => {
                require(
                    !sources.is_empty(),
                    "dependency sources",
                    FormatRule::Required,
                )?;
                for source in sources {
                    positive(source.write.number)?;
                    require(
                        source.row.key == self.change.row.key
                            && source.row.audience == self.change.row.audience,
                        "dependency source row",
                        FormatRule::Required,
                    )?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
