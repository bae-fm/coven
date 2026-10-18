//! Lost-value acknowledgements in ordinary write streams (format.md D5).

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, row};
use crate::wire::{decode_frame, wire_struct, Decoder, Encoder, Wire};
use coven_merge::{RowId, WriteId, WritePast};

/// What the author acknowledged. Empty frozen rows have no cell to name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DismissalTarget {
    /// A column containing a lost value.
    Cell(String),
    /// A frozen original change with no retained values.
    EmptyRow,
}

impl Wire for DismissalTarget {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Cell(column) => {
                0u8.put(out)?;
                column.put(out)
            }
            Self::EmptyRow => 1u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => crate::wire::get_name(input).map(Self::Cell),
            1 => Ok(Self::EmptyRow),
            tag => Err(Error::UnknownTag {
                field: "dismissal target",
                tag,
            }),
        }
    }
}

/// One lost value the author has dealt with. Removed rows use ordinary deletes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dismissal {
    /// The table, key and audience containing the loss.
    pub row: RowId,
    /// A lost cell or an empty frozen row.
    pub target: DismissalTarget,
    /// The cell's setter, or the empty row's original write.
    pub write: WriteId,
}
wire_struct!(Dismissal, row, target, write);

impl Dismissal {
    /// Encode a bounded kind-3 dismissal frame.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        crate::encode_frame(3, self)
    }

    /// Decode exactly one dismissal, checking names and write identity.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = decode_frame(bytes)?;
        require(kind == 3, "dismissal kind", Rule::Kind)?;
        let dismissal = Self::get(&mut input)?;
        input.finish()?;
        dismissal.validate()?;
        Ok(dismissal)
    }

    /// A dismissal can acknowledge only a value its author had read.
    pub fn validate_past(&self, past: &impl WritePast) -> Result<(), Error> {
        require(
            past.contains(&self.write),
            "dismissed write",
            Rule::Coverage,
        )
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        if let DismissalTarget::Cell(column) = &self.target {
            name(column)?;
        }
        positive(self.write.number)
    }
}

/// One decoded write-part frame; a row's change or execution precedes
/// dismissals of that row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteFrame {
    /// An insert, update or delete of a row.
    Change(crate::write::RowChange),
    /// SQL operations on a row with no net change.
    Execution(crate::write::RowExecution),
    /// An acknowledgement of a lost cell or an empty frozen row.
    Dismissal(Dismissal),
}

impl WriteFrame {
    /// Encode this record with its kind and bounded payload.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        match self {
            Self::Change(row) => row.encode(),
            Self::Execution(execution) => execution.encode(),
            Self::Dismissal(dismissal) => dismissal.encode(),
        }
    }

    /// Decode a row change, execution or dismissal, refusing other frame kinds.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, _) = decode_frame(bytes)?;
        match kind {
            2 => crate::write::RowChange::decode(bytes).map(Self::Change),
            3 => Dismissal::decode(bytes).map(Self::Dismissal),
            13 => crate::write::RowExecution::decode(bytes).map(Self::Execution),
            tag => Err(Error::UnknownTag {
                field: "write record kind",
                tag,
            }),
        }
    }

    pub(crate) fn key(&self) -> (RowId, Option<(DismissalTarget, WriteId)>) {
        match self {
            Self::Change(row) => (row.row.clone(), None),
            Self::Execution(execution) => (execution.row.clone(), None),
            Self::Dismissal(dismissal) => (
                dismissal.row.clone(),
                Some((dismissal.target.clone(), dismissal.write)),
            ),
        }
    }
}

#[cfg(test)]
#[path = "dismissal_tests.rs"]
mod tests;
