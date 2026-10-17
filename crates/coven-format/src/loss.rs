//! One loss record for displaced cells, removed rows and excluded writes.

use crate::error::{require, Error, Rule as FormatRule};
use crate::value::{name, positive, row, Value};
use crate::wire::{Decoder, Encoder, Wire};
use crate::write_sources::ChangeId;
use coven_foundation::id_source::MemberId;
use coven_merge::{Cell, LostKey, LostValue, RowId, Rule, WriteId};
use std::collections::{BTreeMap, BTreeSet};

/// A loss independent of local SQLite identities. Active losses follow merge;
/// frozen losses keep their values and cause after their merge history is gone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loss {
    /// Original table, key and audience.
    pub row: RowId,
    /// Cell incarnation, removed-row generation, or excluded change's generation.
    pub generation: u64,
    /// Whether this loss is frozen outside merge.
    pub frozen: bool,
    /// Values as written, with each value's setter.
    pub values: LossValues,
    /// What displaced these values.
    pub cause: LossCause,
}
impl Wire for Loss {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        self.row.put(out)?;
        self.generation.put(out)?;
        u8::from(self.frozen).put(out)?;
        self.values.put(out)?;
        self.cause.put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        let row = Wire::get(input)?;
        let generation = Wire::get(input)?;
        let frozen = match u8::get(input)? {
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
            frozen,
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
    /// A breaking schema change that excluded the write, or whose migration
    /// write deleted a hidden row. Resets create no losses (§19.3).
    Excluded {
        /// The excluded write, including deletions with no retained cells.
        write: WriteId,
        /// The schema version reached by the breaking change.
        version: u32,
    },
    /// An admitted row change that used data which does not survive (§8):
    /// its generation source, a cell setter it replaced, a source its
    /// operations used, or a foreign-key cause in its own write.
    Dependency {
        /// The write whose row change this is, even for an empty delete.
        write: WriteId,
        /// Failed sources in role order, with repeats: generation, replaced
        /// setters, additional execution sources, then foreign-key causes.
        sources: Vec<ChangeId>,
    },
    /// A member's row change without permission: a direct change of another
    /// member's owned row, or a foreign-key effect the receiver could not
    /// derive exactly from an authorized direct change (§8.8).
    NotAuthorized {
        /// The write whose row change this is, even for an empty delete.
        write: WriteId,
        /// Why the change was refused.
        reason: AuthorizationFailure,
    },
}

/// Why a row change lacked permission (§8.8).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizationFailure {
    /// The row belongs to this member, and the change was not an exact
    /// foreign-key effect of an authorized change.
    NotOwner {
        /// The member whose write created the row.
        owner: MemberId,
    },
    /// A derived change differs from the schema's effect of authorized
    /// direct changes in its write, or has no authorized root.
    InvalidForeignKeyEffect,
}
impl Wire for AuthorizationFailure {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::NotOwner { owner } => {
                0u8.put(out)?;
                owner.put(out)
            }
            Self::InvalidForeignKeyEffect => 1u8.put(out),
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::NotOwner {
                owner: Wire::get(input)?,
            }),
            1 => Ok(Self::InvalidForeignKeyEffect),
            tag => Err(Error::UnknownTag {
                field: "authorization failure",
                tag,
            }),
        }
    }
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
            Self::Excluded { write, version } => {
                2u8.put(out)?;
                write.put(out)?;
                version.put(out)
            }
            Self::Dependency { write, sources } => {
                3u8.put(out)?;
                write.put(out)?;
                sources.put(out)
            }
            Self::NotAuthorized { write, reason } => {
                4u8.put(out)?;
                write.put(out)?;
                reason.put(out)
            }
        }
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Write(Wire::get(input)?)),
            1 => Ok(Self::Rules(Wire::get(input)?)),
            2 => Ok(Self::Excluded {
                write: Wire::get(input)?,
                version: Wire::get(input)?,
            }),
            3 => Ok(Self::Dependency {
                write: Wire::get(input)?,
                sources: Wire::get(input)?,
            }),
            4 => Ok(Self::NotAuthorized {
                write: Wire::get(input)?,
                reason: Wire::get(input)?,
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
impl LossCause {
    /// The original change's own write, for a frozen change loss.
    pub fn change_write(&self) -> Option<WriteId> {
        match self {
            Self::Excluded { write, .. }
            | Self::Dependency { write, .. }
            | Self::NotAuthorized { write, .. } => Some(*write),
            Self::Write(_) | Self::Rules(_) => None,
        }
    }

    pub(crate) fn tag(&self) -> u8 {
        match self {
            Self::Write(_) => 0,
            Self::Rules(_) => 1,
            Self::Excluded { .. } => 2,
            Self::Dependency { .. } => 3,
            Self::NotAuthorized { .. } => 4,
        }
    }

    /// A frozen original row change: excluded, dependent or unauthorized.
    pub fn freezes_change(&self) -> bool {
        self.change_write().is_some()
    }
}

impl Loss {
    /// Project a concurrent merge loss into the common record.
    pub fn cell(row: RowId, key: LostKey, value: LostValue<Value>) -> Self {
        Self {
            row,
            generation: value.incarnation,
            frozen: false,
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
            frozen: false,
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
        (
            self.row.clone(),
            self.generation,
            identity,
            self.frozen,
            self.cause.tag(),
            self.cause.change_write(),
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
                    if matches!(rule, Rule::DeletedCircle(_) | Rule::OtherAudience) {
                        require(
                            matches!(self.row.audience, coven_merge::Audience::Circle(_)),
                            "loss audience",
                            FormatRule::Audience,
                        )?;
                    }
                }
            }
            (LossValues::Row(cells), LossCause::Excluded { write, version }) => {
                positive(write.number)?;
                // An excluded write names itself as setter; a row a migration
                // write deleted through its generation keeps each original
                // setter (D7).
                for cell in cells.values() {
                    positive(cell.write.number)?;
                }
                require(self.frozen, "excluded loss", FormatRule::Required)?;
                require(
                    *version > 0,
                    "breaking schema version",
                    FormatRule::Required,
                )?;
            }
            (LossValues::Row(cells), LossCause::Dependency { write, sources }) => {
                positive(write.number)?;
                require(
                    cells.values().all(|cell| cell.write == *write),
                    "dependent setters",
                    FormatRule::Required,
                )?;
                require(self.frozen, "dependent loss", FormatRule::Required)?;
                require(
                    !sources.is_empty(),
                    "dependency sources",
                    FormatRule::Required,
                )?;
                for source in sources {
                    positive(source.write.number)?;
                    require(
                        dependency_source(&self.row, *write, source),
                        "dependency source row",
                        FormatRule::Required,
                    )?;
                }
            }
            (LossValues::Row(cells), LossCause::NotAuthorized { write, .. }) => {
                positive(write.number)?;
                require(
                    cells.values().all(|cell| cell.write == *write),
                    "unauthorized setters",
                    FormatRule::Required,
                )?;
                require(self.frozen, "unauthorized loss", FormatRule::Required)?;
            }
            _ => {
                return Err(Error::Invalid {
                    field: "loss cause",
                    rule: FormatRule::ColumnOperation,
                })
            }
        }
        if !self.cause.freezes_change() {
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
            if self.frozen && !self.cause.freezes_change() {
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

    pub(crate) fn covered(&self, covered: impl Fn(WriteId) -> bool) -> bool {
        let values = match &self.values {
            LossValues::Cell { cell, .. } => covered(cell.write),
            LossValues::Row(cells) => cells.values().all(|cell| covered(cell.write)),
        };
        values
            && match self.cause {
                LossCause::Write(write)
                | LossCause::Excluded { write, .. }
                | LossCause::NotAuthorized { write, .. } => covered(write),
                LossCause::Dependency { write, ref sources } => {
                    covered(write) && sources.iter().all(|s| covered(s.write))
                }
                _ => true,
            }
    }
}

/// A dependent change's source: a change at the same row (its generation,
/// setters and execution sources), or a foreign-key cause in its own write.
pub(crate) fn dependency_source(row: &RowId, write: WriteId, source: &ChangeId) -> bool {
    source.write == write || (source.row.key == row.key && source.row.audience == row.audience)
}

#[cfg(test)]
#[path = "loss_tests.rs"]
mod tests;
