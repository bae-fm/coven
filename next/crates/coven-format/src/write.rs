//! A write's old values and merge-owned changes, split into audience parts (§5, §14.4).

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, row, Value, WritePositions};
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
    /// The app schema version (§17.1).
    pub schema_version: u32,
    /// Whether the author has marked the write lost after a breaking change.
    pub disposition: WriteDisposition,
}
wire_struct!(
    WriteHeader,
    position,
    timestamp,
    had_read,
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
        self.had_read.own_before(self.position, false)?;
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
}
impl Wire for WriteDisposition {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Apply => 0u8.put(out),
            Self::Lost(p) => {
                1u8.put(out)?;
                p.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Apply),
            1 => Ok(Self::Lost(Wire::get(input)?)),
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
            0 => Ok(Self::Insert(Wire::get(input)?)),
            1 => Ok(Self::Update(Wire::get(input)?)),
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
wire_struct!(RowChange, row, change, old);
impl RowChange {
    /// Validate and encode one bounded row-change frame (kind 13).
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        crate::encode_frame(13, self)
    }

    /// Decode exactly one row-change frame.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = crate::wire::decode_frame(bytes)?;
        require(kind == 13, "row change kind", Rule::Kind)?;
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
}
impl WritePart {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        require(!self.rows.is_empty(), "write part rows", Rule::Required)?;
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
}

/// A committed transaction, with one header and a part per audience (§5, §14.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteRecord {
    /// Who wrote it, when, what it had read and its schema.
    pub header: WriteHeader,
    /// Nonempty, strictly increasing audience parts, store before circles.
    pub parts: Vec<WritePart>,
}
impl WriteRecord {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.header.validate()?;
        require(!self.parts.is_empty(), "write parts", Rule::Required)?;
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
