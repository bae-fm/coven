//! Losses kept after a breaking migration discards their row's merge records.

use crate::error::{require, Error, Rule as FormatRule};
use crate::value::{name, positive, row, Value, WritePositions};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_merge::{Cell, ColumnValue, LostKey, LostValue, RowId, Rule, WriteId};
use std::collections::{BTreeMap, BTreeSet};

/// One `coven_lost` record whose merge history was discarded (§17.1).
/// These values remain losses; loading them never restores a merged row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedLoss {
    /// The original table, primary key and audience.
    pub row: RowId,
    /// A displaced cell or removed row, including its original replacement.
    pub values: RetainedValues,
}
wire_struct!(RetainedLoss, row, values);

/// The two forms of loss retained when a row's merge records are discarded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetainedValues {
    /// A concurrent cell loss, retaining its incarnation and replacing write.
    Cell {
        /// Column and the write that set the lost value.
        key: LostKey,
        /// Frozen value, incarnation and replacing write; its parent map is empty.
        value: LostValue<Value>,
    },
    /// A removed row, with each cell's own setter and every removal reason.
    Row {
        /// The incarnation recorded by `coven_lost`.
        generation: u64,
        /// Column names, frozen values and setters; every parent map is empty.
        cells: BTreeMap<String, Cell<Value>>,
        /// The rules that removed this row before its history was discarded.
        replaced_by: BTreeSet<Rule>,
    },
}

impl Wire for RetainedValues {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Cell { key, value } => {
                0u8.put(out)?;
                key.put(out)?;
                value.put(out)
            }
            Self::Row {
                generation,
                cells,
                replaced_by,
            } => {
                1u8.put(out)?;
                generation.put(out)?;
                cells.put(out)?;
                replaced_by.put(out)
            }
        }
    }

    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Cell {
                key: Wire::get(input)?,
                value: Wire::get(input)?,
            }),
            1 => Ok(Self::Row {
                generation: Wire::get(input)?,
                cells: crate::wire::get_name_map(input)?,
                replaced_by: Wire::get(input)?,
            }),
            tag => Err(Error::UnknownTag {
                field: "retained loss",
                tag,
            }),
        }
    }
}

/// Stable identities within one row and incarnation; no local loss ids travel.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LossIdentity {
    Cell(LostKey),
    Row(BTreeMap<String, WriteId>),
}

impl RetainedLoss {
    pub(crate) fn key(&self) -> (RowId, u64, LossIdentity) {
        let (generation, identity) = match &self.values {
            RetainedValues::Cell { key, value } => {
                (value.incarnation, LossIdentity::Cell(key.clone()))
            }
            RetainedValues::Row {
                generation, cells, ..
            } => (
                *generation,
                LossIdentity::Row(
                    cells
                        .iter()
                        .map(|(name, cell)| (name.clone(), cell.write))
                        .collect(),
                ),
            ),
        };
        (self.row.clone(), generation, identity)
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        let generation = match &self.values {
            RetainedValues::Cell { key, value } => {
                name(&key.column)?;
                positive(key.write.number)?;
                positive(value.replaced_by.number)?;
                column(&value.value)?;
                value.incarnation
            }
            RetainedValues::Row {
                generation,
                cells,
                replaced_by,
            } => {
                require(
                    !cells.is_empty() && !replaced_by.is_empty(),
                    "retained row",
                    FormatRule::Required,
                )?;
                for (name_, cell) in cells {
                    name(name_)?;
                    positive(cell.write.number)?;
                    column(&cell.value)?;
                }
                crate::merge_wire::rules(replaced_by)?;
                for rule in replaced_by {
                    if matches!(rule, Rule::DeletedCircle | Rule::OtherAudience) {
                        require(
                            matches!(self.row.audience, coven_merge::Audience::Circle(_)),
                            "retained audience",
                            FormatRule::Audience,
                        )?;
                    }
                }
                *generation
            }
        };
        require(
            !generation.is_multiple_of(2),
            "retained incarnation",
            FormatRule::Generation,
        )
    }

    pub(crate) fn covered(&self, positions: &WritePositions) -> bool {
        match &self.values {
            RetainedValues::Cell { key, value } => {
                positions.covers(key.write) && positions.covers(value.replaced_by)
            }
            RetainedValues::Row { cells, .. } => {
                cells.values().all(|cell| positions.covers(cell.write))
            }
        }
    }
}

fn column(value: &ColumnValue<Value>) -> Result<(), Error> {
    require(
        value.parents.is_empty(),
        "retained references",
        FormatRule::Required,
    )?;
    crate::merge_wire::column(value)
}

#[cfg(test)]
#[path = "retained_loss_tests.rs"]
mod tests;
