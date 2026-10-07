//! A write's old values and merge-owned changes, split into audience parts (§5, §14.4).
//!
//! `WriteRecord` and `WritePart` are in-memory values, not additional encodings.
//! `write_stream::WriteEncoder` emits the header and per-audience frame streams.

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, row, EntryPositions, Value, WritePositions};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_merge::{Audience, Change, Operation, RowId, Timestamp, WriteId};
use std::collections::BTreeMap;

/// Identity, timestamp, causal positions and schema of a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteHeader {
    /// Which device wrote it, and its number in that device's log.
    pub position: WriteId,
    /// The author's timestamp, whose device matches the write identity.
    pub timestamp: Timestamp,
    /// Other devices' read positions; its own earlier writes are implicit.
    pub had_read: WritePositions,
    /// Applied store-log positions, including this device's entries (§7.1, D5).
    pub store_log_read: EntryPositions,
    /// The app schema version (§17.1).
    pub schema_version: u32,
    /// Whether to apply rows, retain them as lost, or consume a migration write.
    pub disposition: WriteDisposition,
}
wire_struct!(
    WriteHeader,
    position,
    timestamp,
    had_read,
    store_log_read,
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
        self.had_read.without_own_device(self.position.device)?;
        self.store_log_read.validate()?;
        if let WriteDisposition::Lost(version) = self.disposition {
            require(version > 0, "breaking schema version", Rule::Required)?;
        }
        Ok(())
    }
}

/// How an uploaded write is treated after an app schema change (§17.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteDisposition {
    /// Apply its changes under the merge rules.
    Apply,
    /// Keep the whole write as lost, naming the version reached by the breaking change.
    Lost(u32),
    /// Its changes travel only in the breaking change's snapshot; the log has no parts.
    Migration,
}
impl WriteDisposition {
    pub(crate) fn validate_parts(self, count: usize) -> Result<(), Error> {
        match self {
            Self::Migration => require(count == 0, "migration write parts", Rule::StreamLength),
            Self::Apply | Self::Lost(_) => require(count > 0, "write parts", Rule::Required),
        }
    }
}
impl Wire for WriteDisposition {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Apply => 0u8.put(out),
            Self::Lost(p) => {
                1u8.put(out)?;
                p.put(out)
            }
            Self::Migration => 2u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Apply),
            1 => Ok(Self::Lost(Wire::get(input)?)),
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

/// Merge's change to one row, together with SQLite's old column values (§5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowChange {
    /// Table, encoded primary key and audience.
    pub row: RowId,
    /// The pre-change generation and merge operation, including parent metadata.
    pub change: Change<Value>,
    /// Previous SQL values: empty on insert, matching updated columns on update,
    /// and optionally populated on delete. SQL NULL is a value, not absence.
    pub old: BTreeMap<String, Value>,
}
wire_struct!(RowChange, row, change, old => crate::wire::get_name_map);
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
        Ok(())
    }
}

/// All changes for one audience, sealed together (§14.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritePart {
    /// The audience of every row in this part.
    pub audience: Audience,
    /// One change per row, in increasing merge row-identity order.
    pub rows: Vec<RowChange>,
    /// Lost cells acknowledged by this write, ordered by row, column and setter.
    pub dismissals: Vec<crate::dismissal::Dismissal>,
}
impl WritePart {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(
            !self.rows.is_empty() || !self.dismissals.is_empty(),
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
        for change in &self.rows {
            change.validate()?;
            require(
                change.row.audience == self.audience,
                "write part audience",
                Rule::Audience,
            )?;
        }
        Ok(())
    }

    pub(crate) fn frames(&self) -> impl Iterator<Item = Result<Vec<u8>, Error>> + '_ {
        let mut rows = self.rows.iter().peekable();
        let mut dismissals = self.dismissals.iter().peekable();
        std::iter::from_fn(move || match (rows.peek(), dismissals.peek()) {
            (Some(row), Some(dismissal)) if row.row <= dismissal.row => {
                rows.next().map(RowChange::encode)
            }
            (_, Some(_)) => dismissals.next().map(crate::dismissal::Dismissal::encode),
            (Some(_), None) => rows.next().map(RowChange::encode),
            (None, None) => None,
        })
    }
}

/// A committed transaction, with one header and a part per audience (§5, §14.4).
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
            for dismissal in &part.dismissals {
                dismissal.validate_past(&self.header)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
