//! What a live query's run read, and whether a commit's row changes could
//! change what it returned.

use crate::row_scope::{RowImage, RowScope};
use crate::scope_lookup::{Judging, Lookups, NoAnswer};
use crate::sql_value::SqlValue;
use std::collections::{BTreeSet, HashMap};

/// Past this many row changes to one table, a write records only that the
/// table changed, not each row, so its memory stays bounded; every live
/// query that reads the table then runs again.
pub const ROWS_PER_TABLE: usize = 10_000;

/// The table and column names a run read, as pairs.
pub type ColumnSet = BTreeSet<(String, String)>;
/// The tables a run read, with the rows and columns of each.
pub type ReadSet = Vec<TableRead>;

/// What a run read from one table.
#[derive(Clone)]
pub struct TableRead {
    /// The table's name.
    pub table: String,
    /// The columns the run read.
    pub columns: BTreeSet<String>,
    /// The rows whose changes matter.
    pub scope: RowScope,
    /// Only inserted rows affect this read, not updates or deletions.
    pub inserted_only: bool,
}

/// One change a commit made to a table's column or row.
pub struct RowChange {
    /// The table's name.
    pub table: String,
    /// The changed column, or empty for a change to a whole row.
    pub column: String,
    /// The row before and after, `None` on the side where it isn't there;
    /// `None` altogether for a change no row names, as a virtual table's or
    /// one only the update hook observed, which every scope covers.
    pub rows: Option<(Option<RowImage>, Option<RowImage>)>,
}

impl TableRead {
    /// Whether `change` could change what this read returned, running
    /// `lookups` for a join's values.
    fn affected(&self, change: &RowChange, lookups: &mut Lookups<'_>) -> Result<bool, NoAnswer> {
        if self.table != change.table
            || !(change.column.is_empty() || self.columns.contains(&change.column))
            || (self.inserted_only && !inserted(change))
        {
            return Ok(false);
        }
        let Some((old, new)) = &change.rows else {
            return Ok(true);
        };
        for row in [old, new].into_iter().flatten() {
            if self.scope.matches(row, lookups)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// An insertion: a whole-row change with no row before it.
fn inserted(change: &RowChange) -> bool {
    change.column.is_empty() && change.rows.as_ref().is_some_and(|(old, _)| old.is_none())
}

/// Whether `changes` could change what `reads` returned. Past
/// `ROWS_PER_TABLE` row changes to one table, a join's values in that table
/// count as matched without a lookup, as a table changed whole would, so a
/// write's lookups stay bounded. A question that runs past `LOOKUP_STEPS`
/// and stops leaves sqlive unable to tell, so `reads` count as changed
/// whatever the other questions answer (§4).
pub fn affected(
    reads: &ReadSet,
    changes: &[RowChange],
    answers: &mut Answers,
    lookups: &mut Lookups<'_>,
) -> rusqlite::Result<bool> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for change in changes.iter().filter(|change| change.rows.is_some()) {
        *counts.entry(&change.table).or_default() += 1;
    }
    for change in changes {
        let bounded =
            counts.get(change.table.as_str()).is_some_and(|count| *count > ROWS_PER_TABLE);
        for read in reads {
            let found = if bounded {
                read.affected(change, &mut |_| Ok(true))
            } else {
                read.affected(change, &mut |judging| answers.answer(judging, lookups))
            };
            match found {
                Ok(false) => {}
                Ok(true) | Err(NoAnswer::Stopped) => return Ok(true),
                Err(NoAnswer::Failed(error)) => return Err(error),
            }
        }
    }
    Ok(false)
}

/// The answers to the lookups `affected` has run, keyed by lookup and value
/// or by condition and values. Rows that share a value, and a row's old and
/// new images that agree on the columns a condition names, share one run of
/// the lookup.
#[derive(Default)]
pub struct Answers {
    partners: HashMap<(usize, ValueKey), bool>,
    rows: HashMap<(String, Vec<ValueKey>), bool>,
}

impl Answers {
    fn answer(
        &mut self,
        judging: Judging<'_>,
        lookups: &mut Lookups<'_>,
    ) -> Result<bool, NoAnswer> {
        match judging {
            Judging::Partner(lookup, value) => {
                let key = (std::ptr::from_ref(lookup) as usize, ValueKey::of(value));
                if let Some(found) = self.partners.get(&key) {
                    return Ok(*found);
                }
                let found = lookups(judging)?;
                self.partners.insert(key, found);
                Ok(found)
            }
            Judging::Row(sql, parameters) => {
                let key = (sql.to_owned(), parameters.iter().map(ValueKey::of).collect());
                if let Some(found) = self.rows.get(&key) {
                    return Ok(*found);
                }
                let found = lookups(judging)?;
                self.rows.insert(key, found);
                Ok(found)
            }
        }
    }
}

/// A value as a hash key, by its type and bytes.
#[derive(Hash, PartialEq, Eq)]
enum ValueKey {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl ValueKey {
    fn of(value: &SqlValue) -> Self {
        match value {
            SqlValue::Null => Self::Null,
            SqlValue::Integer(value) => Self::Integer(*value),
            SqlValue::Real(value) => Self::Real(value.to_bits()),
            SqlValue::Text(value) => Self::Text(value.clone()),
            SqlValue::Blob(value) => Self::Blob(value.clone()),
        }
    }
}
