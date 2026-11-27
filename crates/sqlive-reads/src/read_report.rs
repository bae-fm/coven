//! What a live query's statement depends on in each table and view it reads, as
//! its plan narrows them before a run binds its values (SPEC.md section 12):
//! the rows a value fixes, the rows a lookup or another question finds, or
//! every row, with the reason for each read of every row.

use std::collections::BTreeMap;
use std::fmt;

use crate::row_scope::RowScope;
use crate::scope_lookup::{source_scopes, table_scopes};
use crate::scope_plan::{Object, ScopePlan, Source, Sources};
use crate::scope_term::RunValues;

/// What a live query's statement depends on, as its plan narrows what it
/// reads. It also depends on every row of each table a view read whole reads,
/// whatever that table's own entry says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadReport {
    /// The planner can't read the statement, so it depends on every row of
    /// each table it reads.
    Whole {
        /// Why the planner can't read it.
        why: String,
    },
    /// One entry for each table the plan reads as a source and each view it
    /// reads whole, by name.
    Entries(Vec<ReadEntry>),
}

/// What a statement depends on in one table, or one view read whole.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadEntry {
    /// The table or view, lowercase.
    pub object: String,
    /// The rows the statement depends on.
    pub rows: ReadRows,
}

/// Which rows of one table or view a statement depends on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadRows {
    /// Those that hold one of a few values in `column`: each read of the
    /// table compares the column with a value by `=`, `IN` or `IS`, or with
    /// another table's column that holds such values.
    Keyed {
        /// The column, lowercase.
        column: String,
    },
    /// Those each read of the table narrows it to with no one column that
    /// every read fixes: by a join, which a write looks up, a condition or a
    /// LIMIT that a write asks SQLite about, an OR or a range.
    LookedUp,
    /// Every row.
    Whole {
        /// Why, for each read of the table or view that nothing narrows.
        why: Vec<String>,
    },
}

impl ReadReport {
    /// What `plan` narrows.
    pub fn of(plan: &ScopePlan) -> Self {
        let sources = match &plan.sources {
            Ok(sources) => sources,
            Err(why) => return Self::Whole { why: why.clone() },
        };
        let last = sources
            .sorted
            .as_ref()
            .and_then(|sorted| sorted.last_row(RunValues::Any))
            .map(|last| last.any_values());
        let scopes = source_scopes(sources, RunValues::Any, last.as_deref());
        // Why each table is read whole, when it is, and each view is.
        let mut whole: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        let read = sources.sources.iter().zip(&scopes).filter(|(source, _)| !source.unread);
        for (source, scope) in read {
            let (object, why) = match &source.object {
                Object::Table { name, within } => {
                    (name, why_whole(sources, source, within.as_ref(), scope))
                }
                Object::View { name, why } => (name, Some(why.0.to_owned())),
                Object::Function | Object::Derived => continue,
            };
            let reasons = whole.entry(object).or_default();
            reasons.extend(why.filter(|why| !reasons.contains(why)));
        }
        let tables = table_scopes(sources, scopes);
        let entries = whole.into_iter().map(|(object, why)| {
            let rows = if why.is_empty() {
                match tables[object].key() {
                    Some((column, _)) => ReadRows::Keyed { column: column.to_owned() },
                    None => ReadRows::LookedUp,
                }
            } else {
                ReadRows::Whole { why }
            };
            ReadEntry { object: object.to_owned(), rows }
        });
        Self::Entries(entries.collect())
    }

    /// The entries whose every row the statement depends on, as text, when
    /// it has any.
    pub fn whole(&self) -> Option<String> {
        match self {
            Self::Whole { .. } => Some(self.to_string()),
            Self::Entries(entries) => {
                let whole: Vec<String> = entries
                    .iter()
                    .filter(|entry| matches!(entry.rows, ReadRows::Whole { .. }))
                    .map(ReadEntry::to_string)
                    .collect();
                (!whole.is_empty()).then(|| whole.join("; "))
            }
        }
    }
}

impl fmt::Display for ReadReport {
    /// One line for each entry.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Whole { why } => write!(formatter, "every table: whole: {why}"),
            Self::Entries(entries) => {
                for (position, entry) in entries.iter().enumerate() {
                    if position > 0 {
                        writeln!(formatter)?;
                    }
                    write!(formatter, "{entry}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for ReadEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let object = &self.object;
        match &self.rows {
            ReadRows::Keyed { column } => write!(formatter, "{object}: keyed by {column}"),
            ReadRows::LookedUp => write!(formatter, "{object}: looked up"),
            ReadRows::Whole { why } => write!(formatter, "{object}: whole: {}", why.join(", ")),
        }
    }
}

/// Why `source` reads every row of its table, in a scope of `scope`, when it
/// does; `within` is the CTE or view it sits in.
fn why_whole(
    sources: &Sources,
    source: &Source,
    within: Option<&String>,
    scope: &RowScope,
) -> Option<String> {
    // A read through such a name may be the view's, whose tables no source
    // narrows (`Sources::inlined`).
    if let Some(name) = within {
        match &sources.inlined {
            None => return Some("a view read whole that the parser can't read".to_owned()),
            Some(inlined) if !inlined.contains(name) => {
                return Some(format!("read through `{name}`, which a view read whole also names"))
            }
            Some(_) => {}
        }
    }
    matches!(scope, RowScope::All).then(|| format!("nothing narrows `{}`", source.qualifier))
}

/// What the live query that reads `whole` whole depends on when it runs
/// `sql` on `connection`.
#[cfg(any(test, feature = "test-utils"))]
pub fn read_report(
    connection: &rusqlite::Connection,
    sql: &str,
    whole: &crate::read_capture::WholeViews,
) -> rusqlite::Result<ReadReport> {
    let sql = crate::sql::up_to_nul(sql);
    let statement = connection.prepare(sql)?;
    Ok(ReadReport::of(&ScopePlan::build(connection, sql, &statement, whole)?))
}

#[cfg(test)]
#[path = "read_report_tests.rs"]
mod tests;
