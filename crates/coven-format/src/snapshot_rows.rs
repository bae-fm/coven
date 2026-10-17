//! Snapshot rows carry app values and merge generations; losses travel separately.

use crate::error::{require, Error, Rule as FormatRule};
use crate::value::{positive, row, Slot, Value};
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
    /// Slot containing this published write.
    pub slot: Slot,
    /// Its timestamp.
    pub timestamp: Timestamp,
    /// Last applied slot when authored; own earlier writes are implicit.
    pub read_through: u64,
}
wire_struct!(AppliedWrite, id, slot, timestamp, read_through);
impl AppliedWrite {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        positive(self.id.number)?;
        require(
            self.timestamp.device() == self.id.device,
            "applied write timestamp",
            FormatRule::TimestampDevice,
        )?;
        require(
            self.read_through < self.slot.0,
            "applied write read-through",
            FormatRule::Coverage,
        )
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

/// How a device interpreted one original row change (§8, §10, §17.1, §19.3).
/// It decides whether the change survives as a source for later changes,
/// independently of dismissible losses and consumed slots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeVerdict {
    /// Never interpreted here: a loaded snapshot already carries its effects.
    Unjudged,
    /// It entered the merge.
    Applied,
    /// A breaking change to this schema version excluded its part (§17.1).
    Excluded(u32),
    /// An earlier removal or credential replacement cut off this write.
    Refused,
    /// It used data that does not survive: these sources, in role order.
    Dependent(Vec<crate::write_sources::ChangeId>),
    /// A kept reset ignored its part: it had not read that entry (§19.3).
    Ignored(Slot),
    /// It lacked permission: it directly changed another member's owned row,
    /// or was not the exact foreign-key effect of authorized changes (§8.8).
    NotAuthorized(crate::loss::AuthorizationFailure),
    /// It entered the merge as the exact foreign-key effect of these changes
    /// in its own write: each action's root and intermediate causes, in
    /// execution order (§8.8).
    Derived(Vec<crate::write_sources::ChangeId>),
}
impl ChangeVerdict {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Excluded(version) => require(
                *version > 0,
                "breaking schema version",
                FormatRule::Required,
            ),
            Self::Ignored(entry) => positive(entry.0),
            Self::Dependent(sources) | Self::Derived(sources) => {
                require(!sources.is_empty(), "verdict sources", FormatRule::Required)?;
                sources.iter().try_for_each(|s| positive(s.write.number))
            }
            Self::Unjudged | Self::Applied | Self::Refused | Self::NotAuthorized(_) => Ok(()),
        }
    }

    /// Whether the change survives as data for later changes.
    pub fn retained(&self) -> bool {
        matches!(self, Self::Unjudged | Self::Applied | Self::Derived(_))
    }
}
impl Wire for ChangeVerdict {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Unjudged => 0u8.put(out),
            Self::Applied => 1u8.put(out),
            Self::Excluded(version) => {
                2u8.put(out)?;
                version.put(out)
            }
            Self::Refused => 3u8.put(out),
            Self::Dependent(sources) => {
                4u8.put(out)?;
                sources.put(out)
            }
            Self::Ignored(entry) => {
                5u8.put(out)?;
                entry.put(out)
            }
            Self::NotAuthorized(reason) => {
                6u8.put(out)?;
                reason.put(out)
            }
            Self::Derived(causes) => {
                7u8.put(out)?;
                causes.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Unjudged),
            1 => Ok(Self::Applied),
            2 => Ok(Self::Excluded(Wire::get(input)?)),
            3 => Ok(Self::Refused),
            4 => Ok(Self::Dependent(Wire::get(input)?)),
            5 => Ok(Self::Ignored(Wire::get(input)?)),
            6 => Ok(Self::NotAuthorized(Wire::get(input)?)),
            7 => Ok(Self::Derived(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "change verdict",
                tag,
            }),
        }
    }
}

/// One row's original record in a write part (D5): its net change, or the
/// SQL operations that left it with none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OriginalRow {
    /// A net change, with its operations.
    Change(crate::write::RowChange),
    /// Operations with no net change.
    Execution(crate::write::RowExecution),
}
impl Wire for OriginalRow {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Change(change) => {
                0u8.put(out)?;
                change.put(out)
            }
            Self::Execution(execution) => {
                1u8.put(out)?;
                execution.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Change(Wire::get(input)?)),
            1 => Ok(Self::Execution(Wire::get(input)?)),
            tag => Err(Error::UnknownTag {
                field: "original row",
                tag,
            }),
        }
    }
}
impl OriginalRow {
    /// The changed row.
    pub fn row(&self) -> &RowId {
        match self {
            Self::Change(change) => &change.row,
            Self::Execution(execution) => &execution.row,
        }
    }

    /// The record as a part record.
    pub fn record(&self) -> crate::write::PartRecord<'_> {
        match self {
            Self::Change(change) => crate::write::PartRecord::Change(change),
            Self::Execution(execution) => crate::write::PartRecord::Execution(execution),
        }
    }

    /// The row's operations and the sources they used.
    pub fn execution(&self) -> &crate::write::Execution {
        match self {
            Self::Change(change) => &change.execution,
            Self::Execution(execution) => &execution.execution,
        }
    }

    /// The record's own frame: kind 2 for a change, 13 for an execution.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        match self {
            Self::Change(change) => change.encode(),
            Self::Execution(execution) => execution.encode(),
        }
    }

    /// Decode either frame kind.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        match crate::dismissal::WriteFrame::decode(bytes)? {
            crate::dismissal::WriteFrame::Change(change) => Ok(Self::Change(change)),
            crate::dismissal::WriteFrame::Execution(execution) => Ok(Self::Execution(execution)),
            crate::dismissal::WriteFrame::Dismissal(_) => Err(Error::UnknownTag {
                field: "original row kind",
                tag: 3,
            }),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Change(change) => change.validate(),
            Self::Execution(execution) => execution.validate(),
        }
    }

    pub(crate) fn sources(&self) -> impl Iterator<Item = &crate::write_sources::ChangeId> {
        let (net, execution) = match self {
            Self::Change(change) => (Some(&change.sources), &change.execution),
            Self::Execution(execution) => (None, &execution.execution),
        };
        net.into_iter()
            .flat_map(|sources| sources.changes())
            .chain(execution.sources.changes())
            .chain(match self {
                Self::Change(change) => change.creator.as_ref(),
                Self::Execution(_) => None,
            })
    }
}

/// One original row record kept as source evidence (D7 section 5): the
/// authored header of its part, whether that part arrived whole, the original
/// change or execution with its sources, and its author's verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoredChange {
    /// The original write header, before any conversion.
    pub header: crate::write::WriteHeader,
    /// Whether every original change of this part is known, so absence of a
    /// named row is proof rather than a wait.
    pub complete: bool,
    /// The original record, with its generation source, replaced setters and
    /// operations.
    pub change: OriginalRow,
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
            row: self.change.row().clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.header.validate()?;
        self.change.validate()?;
        self.verdict.validate()?;
        match &self.verdict {
            ChangeVerdict::Dependent(sources) => {
                for source in sources {
                    require(
                        crate::loss::dependency_source(
                            self.change.row(),
                            self.header.position,
                            source,
                        ),
                        "dependency source row",
                        FormatRule::Required,
                    )?;
                }
                Ok(())
            }
            ChangeVerdict::Derived(causes) => require(
                causes
                    .iter()
                    .all(|cause| cause.write == self.header.position),
                "derivation cause write",
                FormatRule::Required,
            ),
            _ => Ok(()),
        }
    }
}
