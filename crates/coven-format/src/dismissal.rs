//! Lost-cell acknowledgements in ordinary write streams (Appendix D5).

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, row};
use crate::wire::{decode_frame, wire_struct, Wire};
use coven_merge::{RowId, WriteId, WritePast};

/// One lost cell the author has dealt with. Removed rows use ordinary deletes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dismissal {
    /// The table, key and audience containing the lost cell.
    pub row: RowId,
    /// The column containing the lost value.
    pub column: String,
    /// The write that set that value.
    pub write: WriteId,
}
wire_struct!(Dismissal, row, column => crate::wire::get_name, write);

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
    pub fn validate_past(&self, header: &crate::write::WriteHeader) -> Result<(), Error> {
        require(
            header
                .had_read
                .causal_past(header.position)
                .contains(&self.write),
            "dismissed write",
            Rule::Coverage,
        )
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        name(&self.column)?;
        positive(self.write.number)
    }
}

/// One decoded write-part frame; row changes precede dismissals of that row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteFrame {
    /// An insert, update or delete of a row.
    Change(crate::write::RowChange),
    /// An acknowledgement of a lost cell.
    Dismissal(Dismissal),
}

impl WriteFrame {
    /// Encode this record with its kind and bounded payload.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        match self {
            Self::Change(row) => row.encode(),
            Self::Dismissal(dismissal) => dismissal.encode(),
        }
    }

    /// Decode a row change or dismissal, refusing other frame kinds.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, _) = decode_frame(bytes)?;
        match kind {
            2 => crate::write::RowChange::decode(bytes).map(Self::Change),
            3 => Dismissal::decode(bytes).map(Self::Dismissal),
            tag => Err(Error::UnknownTag {
                field: "write record kind",
                tag,
            }),
        }
    }

    pub(crate) fn key(&self) -> (RowId, Option<(String, WriteId)>) {
        match self {
            Self::Change(row) => (row.row.clone(), None),
            Self::Dismissal(dismissal) => (
                dismissal.row.clone(),
                Some((dismissal.column.clone(), dismissal.write)),
            ),
        }
    }
}

#[cfg(test)]
#[path = "dismissal_tests.rs"]
mod tests;
