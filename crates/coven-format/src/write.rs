//! A write's old values and merge-owned changes, split into audience parts (§4, §15.6).
//!
//! `WriteRecord` and `WritePart` are in-memory values, not additional encodings.
//! `write_stream::WriteEncoder` emits the header and per-audience frame streams.

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, row, Value};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use crate::write_sources::ChangeId;
use coven_merge::{Audience, Change, Operation, RowId, Timestamp, WriteId};
use std::collections::BTreeMap;

/// Identity, timestamp, read-through slot and schema of a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteHeader {
    /// Which device wrote it and its original local write number.
    pub position: WriteId,
    /// The author's timestamp, whose device matches the write identity.
    pub timestamp: Timestamp,
    /// Last applied slot when authored; own earlier writes are implicit reads.
    pub read_through: u64,
    /// The app schema version (§18).
    pub schema_version: u32,
    /// Whether to apply rows, retain them as lost, or consume a migration write.
    pub disposition: WriteDisposition,
}
wire_struct!(
    WriteHeader,
    position,
    timestamp,
    read_through,
    schema_version,
    disposition
);
impl WriteHeader {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        positive(self.position.number)?;
        require(
            self.timestamp.device() == self.position.device,
            "write timestamp",
            Rule::TimestampDevice,
        )?;
        Ok(())
    }
}

/// An ordinary write or a breaking migration's empty marker (D5). Schema
/// exclusion is a received interpretation, never transmitted (§18.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteDisposition {
    /// Apply its changes under the merge rules.
    Apply,
    /// Its changes travel only in the breaking change's snapshot; the log has no parts.
    Migration,
}
impl WriteDisposition {
    pub(crate) fn validate_parts(self, count: usize) -> Result<(), Error> {
        match self {
            Self::Migration => require(count == 0, "migration write parts", Rule::StreamLength),
            Self::Apply => require(count > 0, "write parts", Rule::Required),
        }
    }
}
impl Wire for WriteDisposition {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Apply => 0u8.put(out),
            Self::Migration => 2u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Apply),
            2 => Ok(Self::Migration),
            tag => Err(Error::UnknownTag {
                field: "write disposition",
                tag,
            }),
        }
    }
}

impl Wire for Operation<Value> {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Insert(values) => {
                0u8.put(out)?;
                values.put(out)
            }
            Self::Update(values) => {
                1u8.put(out)?;
                values.put(out)
            }
            Self::Delete => 2u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Insert(crate::wire::get_name_map(input)?)),
            1 => Ok(Self::Update(crate::wire::get_name_map(input)?)),
            2 => Ok(Self::Delete),
            tag => Err(Error::UnknownTag {
                field: "row operation",
                tag,
            }),
        }
    }
}
wire_struct!(Change<Value>, generation, operation);

/// Merge's change to one row, together with SQLite's old column values (§4),
/// the original creation it copies and the SQL operations that produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowChange {
    /// Table, encoded primary key and audience.
    pub row: RowId,
    /// The pre-change generation and merge operation, including parent metadata.
    pub change: Change<Value>,
    /// Previous values: empty on insert, matching updated columns on update,
    /// and on delete every replaced cell with the value its setter wrote. SQL
    /// NULL is a value, not absence.
    pub old: BTreeMap<String, Value>,
    /// Authenticated original generation and cell sources, independent of scalars.
    pub sources: crate::write_sources::WriteSources,
    /// For an insert copying a row from another audience (§15.4), the copied
    /// row's original creation, whose author keeps owning it (§9.6).
    pub creator: Option<ChangeId>,
    /// The statement, shared-trigger and foreign-key operations on this row,
    /// in execution order, including a dismissal's deletes. Audience moves
    /// and migration differences have no steps.
    pub execution: Execution,
}
impl Wire for RowChange {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.row.put(out)?;
        self.change.put(out)?;
        self.old.put(out)?;
        self.sources.put(out)?;
        self.creator.put(out)?;
        self.execution.put_against(out, Some(&self.change))
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let row = Wire::get(input)?;
        let change = Wire::get(input)?;
        let old = crate::wire::get_name_map(input)?;
        let sources = Wire::get(input)?;
        let creator = Wire::get(input)?;
        let execution = Execution::get_against(input, Some(&change))?;
        Ok(Self {
            row,
            change,
            old,
            sources,
            creator,
            execution,
        })
    }
}
impl RowChange {
    /// Validate and encode one bounded row-change frame (kind 2).
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        crate::encode_frame(2, self)
    }

    /// Decode exactly one row-change frame.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = crate::wire::decode_frame(bytes)?;
        require(kind == 2, "row change kind", Rule::Kind)?;
        let row = Self::get(&mut input)?;
        input.finish()?;
        row.validate()?;
        Ok(row)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        self.change.validate(&self.row).map_err(Error::Merge)?;
        match &self.change.operation {
            Operation::Insert(values) | Operation::Update(values) => {
                require(!values.is_empty(), "changed columns", Rule::Required)?;
                crate::merge_wire::columns(values)?;
                let valid = if matches!(self.change.operation, Operation::Insert(_)) {
                    self.old.is_empty()
                } else {
                    self.old.keys().eq(values.keys())
                };
                require(valid, "old columns", Rule::ColumnOperation)?;
            }
            Operation::Delete => {}
        }
        for (n, value) in &self.old {
            name(n)?;
            value.validate()?;
        }
        self.sources.validate(self.change.generation)?;
        let valid = match &self.change.operation {
            Operation::Insert(_) => self.sources.setters.is_empty(),
            Operation::Update(columns) => {
                self.sources.setters.keys().all(|n| columns.contains_key(n))
            }
            Operation::Delete => true,
        };
        require(valid, "replaced setters", Rule::ColumnOperation)?;
        if let Some(creator) = &self.creator {
            // Only a generation-zero insert in another audience copies a row.
            require(
                matches!(self.change.operation, Operation::Insert(_))
                    && self.change.generation == 0
                    && creator.row.table == self.row.table
                    && creator.row.key == self.row.key
                    && creator.row.audience != self.row.audience,
                "copied creation",
                Rule::ColumnOperation,
            )?;
            positive(creator.write.number)?;
        }
        self.execution.validate(&self.row)
    }

    /// Check source reads against the consumer's resolved authored past.
    pub fn validate_past(&self, past: &impl coven_merge::WritePast) -> Result<(), Error> {
        self.sources.validate_past(past)?;
        self.execution.sources.validate_past(past)?;
        if let Some(creator) = &self.creator {
            require(
                past.contains(&creator.write),
                "copied creation read",
                Rule::Coverage,
            )?;
        }
        Ok(())
    }
}

/// One row operation at its place in the write's execution (§9.6, D5).
/// Statements, shared triggers, foreign-key actions and dismissal deletes
/// record steps; no step says which of them produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    /// The operation's position in the write. Only the two halves of one
    /// primary-key update share a position.
    pub sequence: u32,
    /// The row's generation before the operation, and the columns it set,
    /// with the parents its references named at that moment.
    pub change: Change<Value>,
    /// A delete that is the old half of a primary-key update: its foreign
    /// keys follow their ON UPDATE actions, not ON DELETE (§9.2).
    pub key_update: bool,
}
impl crate::wire::Wire for Step {
    fn put(&self, out: &mut crate::wire::Encoder) -> Result<(), Error> {
        self.sequence.put(out)?;
        self.change.put(out)?;
        u8::from(self.key_update).put(out)
    }
    fn get(input: &mut crate::wire::Decoder<'_>) -> Result<Self, Error> {
        let sequence = Wire::get(input)?;
        let change = Wire::get(input)?;
        let key_update = match u8::get(input)? {
            0 => false,
            1 => true,
            tag => {
                return Err(Error::UnknownTag {
                    field: "key update",
                    tag,
                })
            }
        };
        Ok(Self {
            sequence,
            change,
            key_update,
        })
    }
}
impl Step {
    /// The generation after this step: a delete or insert advances it.
    pub fn after(&self) -> Option<u64> {
        match self.change.operation {
            Operation::Update(_) => Some(self.change.generation),
            _ => self.change.generation.checked_add(1),
        }
    }
}

/// A row's operations in execution order, with the sources they replaced
/// that its net change, if any, does not already name (§9.5, D5). An
/// operation after the row moved into this audience in the same write
/// starts from the generation the move began, not the net change's.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Execution {
    /// In increasing sequence order, each starting at the previous one's
    /// resulting generation.
    pub steps: Vec<Step>,
    /// The generation source and replaced setters the steps used beyond the
    /// net change's own sources, in the same role order.
    pub sources: crate::write_sources::WriteSources,
}
impl Execution {
    /// Each step is `sequence | change | key_update`, with `change` as
    /// `0` for exactly the row change's own change, written once, or as
    /// `1 | change` otherwise (D5). An execution record has no own change.
    fn put_against(&self, out: &mut Encoder, own: Option<&Change<Value>>) -> Result<(), Error> {
        crate::error::bound(self.steps.len(), crate::wire::MAX_ITEMS, "collection")?;
        out.charge_items(self.steps.len())?;
        (self.steps.len() as u32).put(out)?;
        for step in &self.steps {
            step.sequence.put(out)?;
            if own == Some(&step.change) {
                0u8.put(out)?;
            } else {
                1u8.put(out)?;
                step.change.put(out)?;
            }
            u8::from(step.key_update).put(out)?;
        }
        self.sources.put(out)
    }

    fn get_against(input: &mut Decoder<'_>, own: Option<&Change<Value>>) -> Result<Self, Error> {
        let steps = crate::wire::get_sequence(input, |input| {
            let sequence = Wire::get(input)?;
            let change = match (u8::get(input)?, own) {
                (0, Some(own)) => own.clone(),
                (1, own) => {
                    let change: Change<Value> = Wire::get(input)?;
                    // The row change's own change is only ever written once.
                    require(own != Some(&change), "step change", Rule::Required)?;
                    change
                }
                (tag, _) => {
                    return Err(Error::UnknownTag {
                        field: "step change",
                        tag,
                    })
                }
            };
            let key_update = match u8::get(input)? {
                0 => false,
                1 => true,
                tag => {
                    return Err(Error::UnknownTag {
                        field: "key update",
                        tag,
                    })
                }
            };
            Ok(Step {
                sequence,
                change,
                key_update,
            })
        })?;
        Ok(Self {
            steps,
            sources: Wire::get(input)?,
        })
    }

    pub(crate) fn validate(&self, row_id: &RowId) -> Result<(), Error> {
        for step in &self.steps {
            step.change.validate(row_id).map_err(Error::Merge)?;
            if let Operation::Insert(values) | Operation::Update(values) = &step.change.operation {
                crate::merge_wire::columns(values)?;
            }
            require(
                !step.key_update || matches!(step.change.operation, Operation::Delete),
                "key update step",
                Rule::ColumnOperation,
            )?;
        }
        for pair in self.steps.windows(2) {
            require(
                pair[0].sequence < pair[1].sequence,
                "execution steps",
                Rule::Order,
            )?;
            require(
                pair[0].after() == Some(pair[1].change.generation),
                "execution step generation",
                Rule::Generation,
            )?;
        }
        for source in self.sources.changes() {
            positive(source.write.number)?;
            row(&source.row)?;
        }
        for (column, source) in &self.sources.setters {
            name(column)?;
            name(&source.column)?;
        }
        Ok(())
    }
}

/// A row the write's SQL changed with no net change: it was inserted and
/// deleted again, or ends with the values it began with (kind 13). Its
/// operations can still cause foreign-key actions on other rows (§9.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowExecution {
    /// Table, encoded primary key and audience.
    pub row: RowId,
    /// At least one operation, with every source the operations replaced.
    pub execution: Execution,
}
impl Wire for RowExecution {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.row.put(out)?;
        self.execution.put_against(out, None)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        Ok(Self {
            row: Wire::get(input)?,
            execution: Execution::get_against(input, None)?,
        })
    }
}
impl RowExecution {
    /// Validate and encode one bounded execution frame (kind 13).
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        crate::encode_frame(13, self)
    }

    /// Decode exactly one execution frame.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = crate::wire::decode_frame(bytes)?;
        require(kind == 13, "row execution kind", Rule::Kind)?;
        let row = Self::get(&mut input)?;
        input.finish()?;
        row.validate()?;
        Ok(row)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        require(
            !self.execution.steps.is_empty(),
            "execution steps",
            Rule::Required,
        )?;
        self.execution.validate(&self.row)
    }
}

/// All changes for one audience, sealed together (§15.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritePart {
    /// The audience of every row in this part.
    pub audience: Audience,
    /// One change per row, in increasing merge row-identity order.
    pub rows: Vec<RowChange>,
    /// Rows the SQL changed with no net change, in increasing row order; no
    /// row also has a change.
    pub executions: Vec<RowExecution>,
    /// Lost values acknowledged by this write, ordered by row, target and write (D5.10).
    pub dismissals: Vec<crate::dismissal::Dismissal>,
}
impl WritePart {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(
            !self.rows.is_empty() || !self.executions.is_empty() || !self.dismissals.is_empty(),
            "write part rows",
            Rule::Required,
        )?;
        require(
            self.dismissals.windows(2).all(|pair| pair[0] < pair[1]),
            "write part dismissals",
            Rule::Order,
        )?;
        for dismissal in &self.dismissals {
            dismissal.validate()?;
            require(
                dismissal.row.audience == self.audience,
                "write part audience",
                Rule::Audience,
            )?;
        }
        require(
            self.rows.windows(2).all(|r| r[0].row < r[1].row),
            "write part rows",
            Rule::Order,
        )?;
        require(
            self.executions.windows(2).all(|r| r[0].row < r[1].row),
            "write part executions",
            Rule::Order,
        )?;
        for change in &self.rows {
            change.validate()?;
            require(
                change.row.audience == self.audience,
                "write part audience",
                Rule::Audience,
            )?;
        }
        for execution in &self.executions {
            execution.validate()?;
            require(
                execution.row.audience == self.audience
                    && self
                        .rows
                        .binary_search_by(|change| change.row.cmp(&execution.row))
                        .is_err(),
                "write part executions",
                Rule::Audience,
            )?;
        }
        Ok(())
    }

    /// Each row's record, change or execution, in row order.
    pub fn records(&self) -> impl Iterator<Item = PartRecord<'_>> + '_ {
        let mut rows = self.rows.iter().peekable();
        let mut executions = self.executions.iter().peekable();
        std::iter::from_fn(move || match (rows.peek(), executions.peek()) {
            (Some(row), Some(execution)) if row.row < execution.row => {
                rows.next().map(PartRecord::Change)
            }
            (_, Some(_)) => executions.next().map(PartRecord::Execution),
            (Some(_), None) => rows.next().map(PartRecord::Change),
            (None, None) => None,
        })
    }

    pub(crate) fn frames(&self) -> impl Iterator<Item = Result<Vec<u8>, Error>> + '_ {
        let mut records = self.records().peekable();
        let mut dismissals = self.dismissals.iter().peekable();
        std::iter::from_fn(move || match (records.peek(), dismissals.peek()) {
            (Some(record), Some(dismissal)) if record.row() <= &dismissal.row => {
                records.next().map(|record| record.encode())
            }
            (_, Some(_)) => dismissals.next().map(crate::dismissal::Dismissal::encode),
            (Some(_), None) => records.next().map(|record| record.encode()),
            (None, None) => None,
        })
    }
}

/// One row's record in a part: its net change or its execution alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartRecord<'a> {
    /// A net row change with its execution.
    Change(&'a RowChange),
    /// Operations with no net change.
    Execution(&'a RowExecution),
}
impl<'a> PartRecord<'a> {
    /// The row this record changes.
    pub fn row(&self) -> &RowId {
        match self {
            Self::Change(change) => &change.row,
            Self::Execution(execution) => &execution.row,
        }
    }

    /// The row's operations.
    pub fn execution(&self) -> &'a Execution {
        match self {
            Self::Change(change) => &change.execution,
            Self::Execution(execution) => &execution.execution,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, Error> {
        match self {
            Self::Change(change) => change.encode(),
            Self::Execution(execution) => execution.encode(),
        }
    }
}

/// A committed transaction, with one header and a part per audience (§4, §15.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteRecord {
    /// Who wrote it, when, what it had read and its schema.
    pub header: WriteHeader,
    /// Strictly increasing audience parts, store before circles. Empty only for a migration.
    pub parts: Vec<WritePart>,
}
impl WriteRecord {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.header.validate()?;
        self.header.disposition.validate_parts(self.parts.len())?;
        require(
            self.parts.windows(2).all(|p| p[0].audience < p[1].audience),
            "write parts",
            Rule::Order,
        )?;
        for part in &self.parts {
            part.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
