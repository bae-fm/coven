//! One loss record for displaced cells, removed rows and excluded writes.

use crate::error::{require, Error, Rule as FormatRule};
use crate::snapshot_rows::LostWriteCause;
use crate::value::{name, positive, row, Value, WritePositions};
use crate::wire::{Decoder, Encoder, Wire};
use coven_merge::{Cell, LostKey, LostValue, RowId, Rule, WriteId};
use std::collections::{BTreeMap, BTreeSet};

/// A loss independent of local SQLite identities. Active losses follow merge;
/// retired losses keep their values and cause after their merge history is gone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loss {
    /// Original table, key and audience.
    pub row: RowId,
    /// Cell incarnation, removed-row generation, or excluded change's generation.
    pub generation: u64,
    /// Whether this loss is frozen outside merge.
    pub retired: bool,
    /// Values as written, with each value's setter.
    pub values: LossValues,
    /// What displaced these values.
    pub cause: LossCause,
}
impl Wire for Loss {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.row.put(out)?;
        self.generation.put(out)?;
        u8::from(self.retired).put(out)?;
        self.values.put(out)?;
        self.cause.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let row = Wire::get(input)?;
        let generation = Wire::get(input)?;
        let retired = match u8::get(input)? {
            0 => false,
            1 => true,
            tag => {
                return Err(Error::UnknownTag {
                    field: "loss retirement",
                    tag,
                })
            }
        };
        Ok(Self {
            row,
            generation,
            retired,
            values: Wire::get(input)?,
            cause: Wire::get(input)?,
        })
    }
}

/// The cell or whole row retained by a loss.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LossValues {
    /// One concurrently displaced cell.
    Cell {
        /// The displaced column.
        column: String,
        /// Its written value and setter.
        cell: Cell<Value>,
    },
    /// Every retained column of a removed or excluded row.
    Row(BTreeMap<String, Cell<Value>>),
}
impl Wire for LossValues {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Cell { column, cell } => {
                0u8.put(out)?;
                column.put(out)?;
                cell.put(out)
            }
            Self::Row(cells) => {
                1u8.put(out)?;
                cells.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Cell {
                column: crate::wire::get_name(input)?,
                cell: Wire::get(input)?,
            }),
            1 => Ok(Self::Row(crate::wire::get_name_map(input)?)),
            tag => Err(Error::UnknownTag {
                field: "loss values",
                tag,
            }),
        }
    }
}

/// The replacing write, removal rules, or boundary that excluded a write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LossCause {
    /// A replacing write that had not read the lost setter.
    Write(WriteId),
    /// Every rule holding when the row was removed.
    Rules(BTreeSet<Rule>),
    /// A schema change or reset that excluded the write.
    Excluded {
        /// The excluded write, including deletions with no retained cells.
        write: WriteId,
        /// The boundary that excluded it.
        cause: LostWriteCause,
    },
}
impl Wire for LossCause {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Write(write) => {
                0u8.put(out)?;
                write.put(out)
            }
            Self::Rules(rules) => {
                1u8.put(out)?;
                rules.put(out)
            }
            Self::Excluded { write, cause } => {
                2u8.put(out)?;
                write.put(out)?;
                cause.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Write(Wire::get(input)?)),
            1 => Ok(Self::Rules(Wire::get(input)?)),
            2 => Ok(Self::Excluded {
                write: Wire::get(input)?,
                cause: Wire::get(input)?,
            }),
            tag => Err(Error::UnknownTag {
                field: "loss cause",
                tag,
            }),
        }
    }
}

/// Stable identity within a row and generation; values and causes may change.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LossIdentity {
    Cell(LostKey),
    Row(BTreeMap<String, WriteId>),
}
impl Loss {
    /// Project a concurrent merge loss into the common record.
    pub fn cell(row: RowId, key: LostKey, value: LostValue<Value>) -> Self {
        Self {
            row,
            generation: value.incarnation,
            retired: false,
            values: LossValues::Cell {
                column: key.column,
                cell: Cell {
                    write: key.write,
                    value: value.value,
                },
            },
            cause: LossCause::Write(value.replaced_by),
        }
    }

    /// Project a row removed by rules into the common record.
    pub fn removed(state: &coven_merge::RowState<Value>, rules: BTreeSet<Rule>) -> Self {
        Self {
            row: state.row().clone(),
            generation: state.generation(),
            retired: false,
            values: LossValues::Row(state.cells().clone()),
            cause: LossCause::Rules(rules),
        }
    }

    pub(crate) fn key(&self) -> (RowId, u64, LossIdentity, bool, u8, Option<WriteId>) {
        let identity = match &self.values {
            LossValues::Cell { column, cell } => LossIdentity::Cell(LostKey {
                column: column.clone(),
                write: cell.write,
            }),
            LossValues::Row(cells) => {
                LossIdentity::Row(cells.iter().map(|(n, c)| (n.clone(), c.write)).collect())
            }
        };
        let cause = match self.cause {
            LossCause::Write(_) => 0,
            LossCause::Rules(_) => 1,
            LossCause::Excluded { .. } => 2,
        };
        (
            self.row.clone(),
            self.generation,
            identity,
            self.retired,
            cause,
            match self.cause {
                LossCause::Excluded { write, .. } => Some(write),
                _ => None,
            },
        )
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        row(&self.row)?;
        match (&self.values, &self.cause) {
            (LossValues::Cell { .. }, LossCause::Write(write)) => positive(write.number)?,
            (LossValues::Row(cells), LossCause::Rules(rules)) => {
                require(
                    !cells.is_empty() && !rules.is_empty(),
                    "removed loss",
                    FormatRule::Required,
                )?;
                crate::merge_wire::rules(rules)?;
                for rule in rules {
                    if matches!(rule, Rule::DeletedCircle | Rule::OtherAudience) {
                        require(
                            matches!(self.row.audience, coven_merge::Audience::Circle(_)),
                            "loss audience",
                            FormatRule::Audience,
                        )?;
                    }
                }
            }
            (LossValues::Row(cells), LossCause::Excluded { write, cause }) => {
                positive(write.number)?;
                require(
                    cells.values().all(|cell| cell.write == *write),
                    "excluded setters",
                    FormatRule::Required,
                )?;
                require(self.retired, "excluded loss", FormatRule::Required)?;
                cause.validate()?;
            }
            _ => {
                return Err(Error::Invalid {
                    field: "loss cause",
                    rule: FormatRule::ColumnOperation,
                })
            }
        }
        if !matches!(self.cause, LossCause::Excluded { .. }) {
            require(
                !self.generation.is_multiple_of(2),
                "loss incarnation",
                FormatRule::Generation,
            )?;
        }
        let cell = |n: &str, cell: &Cell<Value>| {
            name(n)?;
            positive(cell.write.number)?;
            crate::merge_wire::column(&cell.value)?;
            if self.retired && !matches!(self.cause, LossCause::Excluded { .. }) {
                require(
                    cell.value.parents.is_empty(),
                    "frozen references",
                    FormatRule::Required,
                )?;
            } else {
                for parent in cell.value.parents.values() {
                    parent.validate_written(&self.row).map_err(Error::Merge)?;
                }
            }
            Ok(())
        };
        match &self.values {
            LossValues::Cell {
                column,
                cell: value,
            } => cell(column, value),
            LossValues::Row(cells) => cells.iter().try_for_each(|(n, c)| cell(n, c)),
        }
    }

    pub(crate) fn covered(&self, positions: &WritePositions) -> bool {
        let values = match &self.values {
            LossValues::Cell { cell, .. } => positions.covers(cell.write),
            LossValues::Row(cells) => cells.values().all(|cell| positions.covers(cell.write)),
        };
        values
            && match self.cause {
                LossCause::Write(write) | LossCause::Excluded { write, .. } => {
                    positions.covers(write)
                }
                _ => true,
            }
    }
}

#[cfg(test)]
#[path = "loss_tests.rs"]
mod tests;
