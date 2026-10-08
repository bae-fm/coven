//! Decoding the durable lost-value fields into the read API.

use crate::{DbError, EntryId, RowKey, WriteId};
use coven_format::{
    loss::{Loss, LossCause, LossValues},
    snapshot_rows::LostWriteCause,
};
use coven_merge::Rule;
use rusqlite::types::Value;

/// One durable lost cell or removed row.
#[derive(Clone, Debug, PartialEq)]
pub struct LostValue {
    /// The application's table.
    pub table: String,
    /// The row's primary key.
    pub key: RowKey,
    /// The displaced cell or removed row's cells.
    pub lost: Lost,
    /// What replaced these values.
    pub replaced_by: Replacement,
    pub(crate) target: LossTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LossTarget {
    Removed {
        row: coven_merge::RowId,
        generation: u64,
    },
    Cells(Vec<coven_format::dismissal::Dismissal>),
}

/// Values retained when a cell loses or a row is removed.
#[derive(Clone, Debug, PartialEq)]
pub enum Lost {
    /// One displaced cell.
    Cell(LostCell),
    /// Every retained column, with its own setter.
    Row(Vec<LostCell>),
}

/// One retained value and the write that set it.
#[derive(Clone, Debug, PartialEq)]
pub struct LostCell {
    /// The column's name.
    pub column: String,
    /// The SQLite value as written, without foreign-key null substitution.
    pub value: Value,
    /// The write that set this cell.
    pub set_by: WriteId,
}

/// The write, rules, breaking schema version or reset entry responsible for a loss.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Replacement {
    /// A write that had not read the displaced value.
    Write(WriteId),
    /// All removal rules holding for the row.
    Rules(Vec<RemovalRule>),
    /// A breaking schema change the write had not read.
    SchemaChange {
        /// The schema version reached by the breaking change.
        version: u32,
    },
    /// A reset the write had not read.
    Reset(EntryId),
}

/// A rule responsible for a removed row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RemovalRule {
    /// A reference to an absent, stale or removed parent.
    ForeignKey {
        /// The referencing columns, in constraint order.
        columns: Vec<String>,
        /// The referenced table.
        parent: String,
        /// The referenced columns, in constraint order.
        parent_columns: Vec<String>,
    },
    /// A failed CHECK.
    Check {
        /// The constraint's name.
        constraint: String,
    },
    /// The row belongs to a deleted circle.
    DeletedCircle,
    /// The same key is shown from another audience.
    OtherAudience,
    /// A competing row won the unique values.
    Unique {
        /// Column names or expression text, in constraint order.
        terms: Vec<String>,
        /// The WHERE expression of a partial constraint.
        partial: Option<String>,
    },
}

impl LostValue {
    pub(crate) fn from_record(record: Loss) -> Result<Self, DbError> {
        let key = RowKey(
            coven_format::key::decode_key(&record.row.key)
                .map_err(|_| DbError::DamagedDatabase)?
                .iter()
                .map(crate::write_encoding::sql_value)
                .collect(),
        );
        let cell = |column, cell: coven_merge::Cell<coven_format::value::Value>| LostCell {
            column,
            value: crate::write_encoding::sql_value(&cell.value.value),
            set_by: cell.write,
        };
        let lost = match record.values {
            LossValues::Cell {
                column,
                cell: value,
            } => Lost::Cell(cell(column, value)),
            LossValues::Row(cells) => Lost::Row(
                cells
                    .into_iter()
                    .map(|(column, value)| cell(column, value))
                    .collect(),
            ),
        };
        let replaced_by = match record.cause {
            LossCause::Write(write) => Replacement::Write(write),
            LossCause::Rules(rules) => Replacement::Rules(
                rules
                    .into_iter()
                    .map(|rule| match rule {
                        Rule::ForeignKey(key) => RemovalRule::ForeignKey {
                            columns: key.columns.0,
                            parent: key.parent,
                            parent_columns: key.parent_columns.0,
                        },
                        Rule::Unique(constraint) => RemovalRule::Unique {
                            terms: constraint.terms,
                            partial: constraint.partial,
                        },
                        Rule::Check(constraint) => RemovalRule::Check { constraint },
                        Rule::DeletedCircle => RemovalRule::DeletedCircle,
                        Rule::OtherAudience => RemovalRule::OtherAudience,
                    })
                    .collect(),
            ),
            LossCause::Excluded {
                cause: LostWriteCause::SchemaChange(version),
                ..
            } => Replacement::SchemaChange { version },
            LossCause::Excluded {
                cause: LostWriteCause::Reset(entry),
                ..
            } => Replacement::Reset(entry),
        };
        let row = record.row;
        let target = if matches!(replaced_by, Replacement::Rules(_)) && !record.retired {
            LossTarget::Removed {
                row: row.clone(),
                generation: record.generation,
            }
        } else {
            let cells = match &lost {
                Lost::Cell(cell) => std::slice::from_ref(cell),
                Lost::Row(cells) => cells,
            };
            LossTarget::Cells(
                cells
                    .iter()
                    .map(|cell| coven_format::dismissal::Dismissal {
                        row: row.clone(),
                        column: cell.column.clone(),
                        write: cell.set_by,
                    })
                    .collect(),
            )
        };
        Ok(LostValue {
            table: row.table,
            key,
            lost,
            replaced_by,
            target,
        })
    }
}

#[cfg(test)]
#[path = "lost_tests.rs"]
mod tests;
