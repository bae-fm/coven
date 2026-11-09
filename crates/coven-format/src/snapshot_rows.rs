//! Snapshot merge rows carry app values and generations; losses travel separately.

use crate::error::{require, Error, Rule as FormatRule};
use crate::value::{positive, Slot, Value};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_merge::{
    Change, ColumnValue, Operation, RowId, RowState, Timestamp, WriteId, WriteOracle,
};
use std::collections::BTreeMap;

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
            self.read_through < self.slot.get(),
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
    pub(crate) fn get<O: WriteOracle>(input: &mut Decoder<'_>, oracle: &O) -> Result<Self, O::Error>
    where
        O::Error: From<Error>,
    {
        let state = RowState::from_parts(
            Wire::get(input)?,
            Wire::get(input)?,
            crate::wire::get_name_map(input)?,
            BTreeMap::new(),
            oracle,
        )?;
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

/// How a device interpreted one original row change (§9.5, §14.1, §18.6, §19.4).
/// It decides whether the change survives as a source for later changes,
/// independently of dismissible losses and consumed slots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeVerdict {
    /// Never interpreted here: a loaded snapshot already carries its effects.
    Unjudged,
    /// It entered the merge.
    Applied,
    /// A breaking change to this schema version excluded its part (§18.6).
    Excluded(u32),
    /// An earlier removal or credential replacement cut off this write.
    Refused,
    /// It used data that does not survive: these sources, in role order.
    Dependent(Vec<crate::write_sources::ChangeId>),
    /// A kept reset ignored its part: it had not read that entry (§19.4).
    Ignored(Slot),
    /// It lacked permission: it directly changed another member's owned row,
    /// or was not the exact foreign-key effect of authorized changes (§9.6).
    NotAuthorized(crate::loss::AuthorizationFailure),
    /// It entered the merge as the exact foreign-key effect of these changes
    /// in its own write: each action's root and intermediate causes, in
    /// execution order (§9.6).
    Derived(Vec<crate::write_sources::ChangeId>),
}
impl ChangeVerdict {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Excluded(version) => {
                require(*version > 0, "breaking schema version", FormatRule::Required)
            }
            Self::Ignored(_) => Ok(()),
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
            tag => Err(Error::UnknownTag { field: "change verdict", tag }),
        }
    }
}

/// A value an accepted change wrote, as its kept record holds it (D7.3): in
/// full when the column was a reference under the change's schema, since a
/// later write's foreign-key actions read it and the parents it named; else
/// only its digest, which tells whether a later delete shows the same value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeptValue {
    /// The value itself.
    Value(Value),
    /// [`coven_crypto::kept_value_digest`] of the value's encoding.
    Digest([u8; 32]),
}
impl KeptValue {
    /// The digest a kept record holds for `value`.
    pub fn digest(value: &Value) -> Result<[u8; 32], Error> {
        let mut out = Encoder::new();
        value.put(&mut out)?;
        Ok(coven_crypto::kept_value_digest(&out.bytes))
    }

    /// Whether `value` is the value this one keeps.
    pub fn matches(&self, value: &Value) -> Result<bool, Error> {
        Ok(match self {
            Self::Value(kept) => kept == value,
            Self::Digest(digest) => Self::digest(value)? == *digest,
        })
    }
}

/// A column a kept change set: its value and parents in full, or a digest,
/// which names no parents.
impl Wire for ColumnValue<KeptValue> {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match &self.value {
            KeptValue::Value(value) => {
                0u8.put(out)?;
                value.put(out)?;
                self.parents.put(out)
            }
            KeptValue::Digest(digest) => {
                1u8.put(out)?;
                digest.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => {
                Ok(Self { value: KeptValue::Value(Wire::get(input)?), parents: Wire::get(input)? })
            }
            1 => Ok(Self { value: KeptValue::Digest(Wire::get(input)?), parents: BTreeMap::new() }),
            tag => Err(Error::UnknownTag { field: "kept value", tag }),
        }
    }
}

/// One row's record of an accepted write part, as a device keeps it once the
/// write is judged (D7.3): what later writes are judged against and what
/// reopens the part's leaves, without the values it replaced, the sources it
/// built on or its SQL operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeptRecord {
    /// A net change.
    Change(KeptChange),
    /// Operations with no net change: only their count, the leaves they take.
    Execution {
        /// The row.
        row: RowId,
        /// How many operations the record listed, at least one.
        steps: u32,
    },
}

/// A net change's kept record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeptChange {
    /// Table, encoded primary key and audience.
    pub row: RowId,
    /// The generation before the change, and the columns it set: each in full
    /// when it was a reference under the write's schema, else its digest.
    pub change: Change<KeptValue>,
    /// For an audience copy, the original creation it copies (D5.7).
    pub creator: Option<crate::write_sources::ChangeId>,
    /// How many operations the record listed.
    pub steps: u32,
}

impl Wire for Change<KeptValue> {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.generation.put(out)?;
        match &self.operation {
            Operation::Insert(columns) => {
                0u8.put(out)?;
                columns.put(out)
            }
            Operation::Update(columns) => {
                1u8.put(out)?;
                columns.put(out)
            }
            Operation::Delete => 2u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let generation = Wire::get(input)?;
        let operation = match u8::get(input)? {
            0 => Operation::Insert(crate::wire::get_name_map(input)?),
            1 => Operation::Update(crate::wire::get_name_map(input)?),
            2 => Operation::Delete,
            tag => return Err(Error::UnknownTag { field: "row operation", tag }),
        };
        Ok(Self { generation, operation })
    }
}

impl Wire for KeptRecord {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Change(change) => {
                0u8.put(out)?;
                change.row.put(out)?;
                change.change.put(out)?;
                change.creator.put(out)?;
                change.steps.put(out)
            }
            Self::Execution { row, steps } => {
                1u8.put(out)?;
                row.put(out)?;
                steps.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Change(KeptChange {
                row: Wire::get(input)?,
                change: Wire::get(input)?,
                creator: Wire::get(input)?,
                steps: Wire::get(input)?,
            })),
            1 => Ok(Self::Execution { row: Wire::get(input)?, steps: Wire::get(input)? }),
            tag => Err(Error::UnknownTag { field: "kept record", tag }),
        }
    }
}

impl KeptRecord {
    /// The kept record of `record`, whose `reference` says which of its
    /// columns were references under its write's schema.
    pub fn keep(
        record: crate::write::PartRecord<'_>,
        reference: impl Fn(&str) -> bool,
    ) -> Result<Self, Error> {
        // A record's steps are a bounded collection, so their count fits.
        let steps = |execution: &crate::write::Execution| {
            crate::error::bound(execution.steps.len(), crate::wire::MAX_ITEMS, "collection")
                .map(|()| execution.steps.len() as u32)
        };
        Ok(match record {
            crate::write::PartRecord::Change(change) => {
                let kept = |columns: &BTreeMap<String, ColumnValue<Value>>| {
                    columns
                        .iter()
                        .map(|(name, cell)| {
                            let kept = if reference(name) {
                                ColumnValue {
                                    value: KeptValue::Value(cell.value.clone()),
                                    parents: cell.parents.clone(),
                                }
                            } else {
                                ColumnValue {
                                    value: KeptValue::Digest(KeptValue::digest(&cell.value)?),
                                    parents: BTreeMap::new(),
                                }
                            };
                            Ok((name.clone(), kept))
                        })
                        .collect::<Result<BTreeMap<_, _>, Error>>()
                };
                let operation = match &change.change.operation {
                    Operation::Insert(columns) => Operation::Insert(kept(columns)?),
                    Operation::Update(columns) => Operation::Update(kept(columns)?),
                    Operation::Delete => Operation::Delete,
                };
                Self::Change(KeptChange {
                    row: change.row.clone(),
                    change: Change { generation: change.change.generation, operation },
                    creator: change.creator.clone(),
                    steps: steps(&change.execution)?,
                })
            }
            crate::write::PartRecord::Execution(execution) => {
                Self::Execution { row: execution.row.clone(), steps: steps(&execution.execution)? }
            }
        })
    }

    /// Whether this record keeps in full exactly the columns `reference`
    /// names, as [`KeptRecord::keep`] would.
    pub fn conforms(&self, reference: impl Fn(&str) -> bool) -> bool {
        match self {
            Self::Change(change) => match &change.change.operation {
                Operation::Insert(columns) | Operation::Update(columns) => {
                    columns.iter().all(|(name, cell)| {
                        matches!(cell.value, KeptValue::Value(_)) == reference(name)
                    })
                }
                Operation::Delete => true,
            },
            Self::Execution { .. } => true,
        }
    }

    /// The changed row.
    pub fn row(&self) -> &RowId {
        match self {
            Self::Change(change) => &change.row,
            Self::Execution { row, .. } => row,
        }
    }

    /// The record's bytes as a device keeps them.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut out = Encoder::new();
        self.put(&mut out)?;
        Ok(out.bytes)
    }

    /// Read back kept bytes, checking them again.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut input = Decoder::new(bytes)?;
        let record = Self::get(&mut input)?;
        input.finish()?;
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::value::row(self.row())?;
        match self {
            Self::Execution { steps, .. } => {
                require(*steps > 0, "execution steps", FormatRule::Required)
            }
            Self::Change(change) => {
                change.change.validate(&change.row).map_err(Error::Merge)?;
                if let Operation::Insert(columns) | Operation::Update(columns) =
                    &change.change.operation
                {
                    require(!columns.is_empty(), "changed columns", FormatRule::Required)?;
                    for (name, cell) in columns {
                        crate::value::name(name)?;
                        if let KeptValue::Value(value) = &cell.value {
                            value.validate()?;
                        }
                        crate::merge_wire::parents(&cell.parents)?;
                    }
                }
                if let Some(creator) = &change.creator {
                    // Only a generation-zero insert in another audience copies a row.
                    require(
                        matches!(change.change.operation, Operation::Insert(_))
                            && change.change.generation == 0
                            && creator.row.table == change.row.table
                            && creator.row.key == change.row.key
                            && creator.row.audience != change.row.audience,
                        "copied creation",
                        FormatRule::ColumnOperation,
                    )?;
                    positive(creator.write.number)?;
                }
                Ok(())
            }
        }
    }
}

/// One accepted write part's shared details in a snapshot's section 4 (D7.3),
/// before its changes: the authored header, and whether every change of the
/// part is known, so absence of a named row is proof rather than a wait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoredPart {
    /// The original write header, before any conversion.
    pub header: crate::write::WriteHeader,
    /// Whether the part arrived whole.
    pub complete: bool,
}
impl Wire for AuthoredPart {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.header.put(out)?;
        u8::from(self.complete).put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let header = Wire::get(input)?;
        let complete = match u8::get(input)? {
            0 => false,
            1 => true,
            tag => return Err(Error::UnknownTag { field: "authored part completeness", tag }),
        };
        Ok(Self { header, complete })
    }
}

/// One kept change of the part before it in section 4, and its verdict by
/// the snapshot's author (D7.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoredChange {
    /// The change's kept record.
    pub record: KeptRecord,
    /// Its interpretation by the snapshot's author.
    pub verdict: ChangeVerdict,
}
wire_struct!(AuthoredChange, record, verdict);
impl AuthoredChange {
    /// The change's identity in the part `header` heads.
    pub fn id(&self, header: &crate::write::WriteHeader) -> crate::write_sources::ChangeId {
        crate::write_sources::ChangeId { write: header.position, row: self.record.row().clone() }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.record.validate()?;
        self.verdict.validate()
    }

    /// The checks that need the part's header: a dependency names this row or
    /// its own write, and a derivation's causes are its own write's changes.
    pub(crate) fn validate_in(&self, header: &crate::write::WriteHeader) -> Result<(), Error> {
        match &self.verdict {
            ChangeVerdict::Dependent(sources) => {
                for source in sources {
                    require(
                        crate::loss::dependency_source(self.record.row(), header.position, source),
                        "dependency source row",
                        FormatRule::Required,
                    )?;
                }
                Ok(())
            }
            ChangeVerdict::Derived(causes) => require(
                causes.iter().all(|cause| cause.write == header.position),
                "derivation cause write",
                FormatRule::Required,
            ),
            _ => Ok(()),
        }
    }
}

/// A record of section 4: a part's shared details, or one of its changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthoredRecord {
    /// A part, before its changes.
    Part(AuthoredPart),
    /// A change of the part before it.
    Change(AuthoredChange),
}
impl Wire for AuthoredRecord {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Part(part) => {
                0u8.put(out)?;
                part.put(out)
            }
            Self::Change(change) => {
                1u8.put(out)?;
                change.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Part(Wire::get(input)?)),
            1 => Ok(Self::Change(Wire::get(input)?)),
            tag => Err(Error::UnknownTag { field: "authored record", tag }),
        }
    }
}
