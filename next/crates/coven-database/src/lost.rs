//! Decoding the durable lost-value fields into the read API.

use crate::{CovenResult, DbError, EntryId, RowKey, WriteId};
use coven_format::{merge_fields::*, snapshot_rows::LostWriteCause};
use coven_merge::Rule;
use rusqlite::{types::Value, Row};

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
    /// The original SQLite value, preserving its storage class.
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

pub(crate) struct LostRecord {
    table: String,
    key: Vec<u8>,
    column_id: Option<i64>,
    column_table: Option<String>,
    column_name: Option<String>,
    value: Vec<u8>,
    setters: Vec<u8>,
    kind: String,
    replacement: Vec<u8>,
}

impl LostRecord {
    pub(crate) fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            table: row.get(0)?,
            key: row.get(1)?,
            column_id: row.get(2)?,
            column_table: row.get(3)?,
            column_name: row.get(4)?,
            value: row.get(5)?,
            setters: row.get(6)?,
            kind: row.get(7)?,
            replacement: row.get(8)?,
        })
    }

    pub(crate) fn decode(self) -> CovenResult<LostValue> {
        let key = RowKey(
            decoded(coven_format::key::decode_key(&self.key))?
                .iter()
                .map(crate::write_encoding::sql_value)
                .collect(),
        );
        let lost = match self.column_id {
            Some(_) => {
                let column = self
                    .column_name
                    .filter(|_| self.column_table.as_ref() == Some(&self.table))
                    .ok_or(DbError::DamagedDatabase)?;
                Lost::Cell(LostCell {
                    column,
                    value: crate::write_encoding::sql_value(
                        &decoded(decode_column_value(&self.value))?.value,
                    ),
                    set_by: decoded(decode_write_id(&self.setters))?,
                })
            }
            None => {
                let values = decoded(decode_columns(&self.value))?;
                let setters = decoded(decode_setters(&self.setters))?;
                if !values.keys().eq(setters.keys()) {
                    return Err(DbError::DamagedDatabase.into());
                }
                Lost::Row(
                    values
                        .into_iter()
                        .map(|(column, value)| LostCell {
                            set_by: setters[&column],
                            column,
                            value: crate::write_encoding::sql_value(&value.value),
                        })
                        .collect(),
                )
            }
        };
        let replaced_by = match self.kind.as_str() {
            "write" if self.column_id.is_some() => {
                Replacement::Write(decoded(decode_write_id(&self.replacement))?)
            }
            "rules" if self.column_id.is_none() => Replacement::Rules(
                decoded(decode_rules(&self.replacement))?
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
            "excluded" => match decoded(decode_lost_write_cause(&self.replacement))? {
                LostWriteCause::SchemaChange(version) => Replacement::SchemaChange { version },
                LostWriteCause::Reset(entry) => Replacement::Reset(entry),
            },
            _ => return Err(DbError::DamagedDatabase.into()),
        };
        Ok(LostValue {
            table: self.table,
            key,
            lost,
            replaced_by,
        })
    }
}

fn decoded<T>(result: Result<T, coven_format::Error>) -> Result<T, DbError> {
    result.map_err(|_| DbError::DamagedDatabase)
}

#[cfg(test)]
#[path = "lost_tests.rs"]
mod tests;
