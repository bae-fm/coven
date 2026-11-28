//! What a live query's statement depends on in each table and view it reads, as
//! its plan narrows them before a run binds its values (SPEC.md section 12):
//! the rows a value fixes, the rows a lookup or another question finds, or
//! every row, with the reason for each read of every row.

use std::collections::BTreeMap;
use std::fmt;

use crate::row_scope::RowScope;
use crate::scope_lookup::source_scopes;
use crate::scope_plan::{Object, ScopePlan, Source, Sources};
use crate::scope_term::RunValues;
#[cfg(any(test, feature = "test-utils"))]
use crate::table_places::Places;
use crate::table_places::SqlPosition;

/// What a live query's statement depends on, as its plan narrows what it
/// reads. It also depends on every row of each table a view read whole reads,
/// whatever that table's own entry says, and so of each table a recursive
/// CTE reads, which has an entry of its own at each place that names the
/// CTE.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadReport {
    /// The planner can't read the statement, so it depends on every row of
    /// each table it reads.
    Whole {
        /// Why the planner can't read it.
        why: String,
    },
    /// One entry for each place the statement reads a table or a view it
    /// reads whole, and for each table of a recursive CTE at each place that
    /// names the CTE, by the object's name and then the place.
    Entries(Vec<ReadEntry>),
}

/// What a statement depends on through one place that reads a table, or a
/// view read whole.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadEntry {
    /// The table or view, lowercase.
    pub object: String,
    /// The place that reads it. A plan built to run a statement finds no
    /// place in the statement's own SQL, so its report has none for a name
    /// written there outside every CTE.
    pub place: Option<ReadPlace>,
    /// The rows the statement depends on.
    pub rows: ReadRows,
}

/// Where SQL names a table or view a statement reads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum ReadPlace {
    /// Here in the statement's own SQL.
    Statement(SqlPosition),
    /// In the SQL of this view, or of this CTE that a view declares.
    Within(String),
}

/// Which rows of one table or view a statement depends on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadRows {
    /// Those that hold one of a few values in `column`: each read the place
    /// gives compares the column with a value by `=`, `IN` or `IS`, or with
    /// another table's column that holds such values.
    Keyed {
        /// The column, lowercase.
        column: String,
    },
    /// Those the place narrows the table to with no one column that every
    /// read it gives fixes: by a join, which a write looks up, a condition
    /// or a LIMIT that a write asks SQLite about, an OR or a range.
    LookedUp,
    /// Every row.
    Whole {
        /// Why, for each read the place gives that nothing narrows.
        why: Vec<String>,
    },
}

/// Why a CTE whose SQL names itself reads each of its tables whole.
const RECURSIVE: &str = "a recursive CTE";

/// A table or view, a place that reads it and the name the place reads it
/// by, which tells apart two places within one view.
type PlaceKey<'a> = (&'a str, Option<&'a ReadPlace>, &'a str);

/// What the reads one place gives depend on.
#[derive(Default)]
struct PlaceReads {
    why: Vec<String>,
    scope: Option<RowScope>,
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
        // The reads of each place, and why each is whole when it is: the
        // planner reads a CTE's SQL once for each name that reads the CTE.
        let mut places: BTreeMap<PlaceKey<'_>, PlaceReads> = BTreeMap::new();
        for (source, scope) in sources.sources.iter().zip(scopes) {
            if source.unread {
                continue;
            }
            let read: Vec<(&String, Option<String>)> = match &source.object {
                Object::Table { name, within } => {
                    vec![(name, why_whole(sources, source, within.as_ref(), &scope))]
                }
                Object::View { name, why, .. } => vec![(name, Some(why.clone()))],
                Object::Recursive { tables, .. } => {
                    tables.iter().map(|table| (table, Some(RECURSIVE.to_owned()))).collect()
                }
                Object::Function | Object::Derived => continue,
            };
            for (object, why) in read {
                let key = (object.as_str(), source.place.as_ref(), source.qualifier.as_str());
                let reads = places.entry(key).or_default();
                reads.why.extend(why.filter(|why| !reads.why.contains(why)));
                reads.scope = Some(match reads.scope.take() {
                    Some(other) => other.or(scope.clone()),
                    None => scope.clone(),
                });
            }
        }
        let entries = places.into_iter().map(|((object, place, _), PlaceReads { why, scope })| {
            let rows = if why.is_empty() {
                match scope.as_ref().and_then(RowScope::key) {
                    Some((column, _)) => ReadRows::Keyed { column: column.to_owned() },
                    None => ReadRows::LookedUp,
                }
            } else {
                ReadRows::Whole { why }
            };
            ReadEntry { object: object.to_owned(), place: place.cloned(), rows }
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
        write!(formatter, "{object}")?;
        if let Some(place) = &self.place {
            write!(formatter, " {place}")?;
        }
        match &self.rows {
            ReadRows::Keyed { column } => write!(formatter, ": keyed by {column}"),
            ReadRows::LookedUp => write!(formatter, ": looked up"),
            ReadRows::Whole { why } => write!(formatter, ": whole: {}", why.join(", ")),
        }
    }
}

impl fmt::Display for ReadPlace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Statement(position) => write!(formatter, "{position}"),
            Self::Within(name) => write!(formatter, "in `{name}`"),
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
                return Some(format!(
                    "read through `{name}`, which a view or CTE read whole also names"
                ))
            }
            Some(_) => {}
        }
    }
    matches!(scope, RowScope::All).then(|| format!("nothing narrows `{}`", source.qualifier))
}

/// What a live query depends on when it runs `sql` on `connection`.
#[cfg(any(test, feature = "test-utils"))]
pub fn read_report(connection: &rusqlite::Connection, sql: &str) -> rusqlite::Result<ReadReport> {
    let sql = crate::sql::up_to_nul(sql);
    let statement = connection.prepare(sql)?;
    let schema = crate::schema_objects::MainSchema::read(connection)?;
    let (plan, _) = ScopePlan::read(connection, &schema, sql, &statement, Places::Find)?;
    Ok(ReadReport::of(&plan))
}

#[cfg(test)]
#[path = "read_report_tests.rs"]
mod tests;
