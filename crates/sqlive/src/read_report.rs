//! What a live query's statement depends on in each table and view it reads,
//! as its plan narrows them before a run binds its values (§4): the rows a
//! value fixes, the rows a lookup or another question finds, or every row,
//! with the reason for each read of every row.

use std::collections::BTreeMap;
use std::fmt;

use crate::scope_edge::MemberValue;
use crate::scope_plan::{Object, PartnerValue, ScopePlan, Source, Sources};
use crate::scope_term::{add_columns, shared_columns, ScopeTerm};

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
    pub(crate) fn of(plan: &ScopePlan) -> Self {
        let sources = match &plan.sources {
            Ok(sources) => sources,
            Err(why) => return Self::Whole { why: why.clone() },
        };
        let narrowed = narrowed(sources);
        let mut read: BTreeMap<&str, Vec<SourceRows>> = BTreeMap::new();
        for (source, keys) in sources.sources.iter().zip(narrowed) {
            let (object, rows) = match &source.object {
                Object::Table { name, within } => (name, table_rows(sources, source, within, keys)),
                Object::View { name, why } => (name, SourceRows::Whole(why.0.to_owned())),
                Object::Function | Object::Derived => continue,
            };
            read.entry(object).or_default().push(rows);
        }
        Self::Entries(
            read.into_iter()
                .map(|(object, rows)| ReadEntry { object: object.to_owned(), rows: merged(rows) })
                .collect(),
        )
    }

    /// The entries whose every row the statement depends on, as text, when
    /// it has any.
    pub(crate) fn whole(&self) -> Option<String> {
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

/// The rows of its table one source depends on.
enum SourceRows {
    /// Every row, and why.
    Whole(String),
    /// Those that hold one of a few values in each of these columns, or some
    /// other narrowing's when there are none.
    Narrowed(Vec<String>),
}

/// The rows of its table that `source`, read within the CTE or view
/// `within`, depends on; `keys` are its own, if anything narrows it.
fn table_rows(
    sources: &Sources,
    source: &Source,
    within: &Option<String>,
    keys: Option<Vec<String>>,
) -> SourceRows {
    // A read through such a name may be the view's, whose tables no source
    // narrows (`Sources::inlined`).
    if let Some(name) = within {
        match &sources.inlined {
            None => {
                return SourceRows::Whole("a view read whole that the parser can't read".to_owned())
            }
            Some(inlined) if !inlined.contains(name) => {
                return SourceRows::Whole(format!(
                    "read through `{name}`, which a view read whole also names"
                ))
            }
            Some(_) => {}
        }
    }
    match keys {
        Some(keys) => SourceRows::Narrowed(keys),
        None => SourceRows::Whole(format!("nothing narrows `{}`", source.qualifier)),
    }
}

/// The rows that every source of one table, or one view read whole, depends
/// on together, as `scope_lookup::scopes` joins their scopes with OR.
fn merged(sources: Vec<SourceRows>) -> ReadRows {
    let mut why: Vec<String> = Vec::new();
    let mut keys: Option<Vec<String>> = None;
    for rows in sources {
        match rows {
            SourceRows::Whole(reason) => {
                if !why.contains(&reason) {
                    why.push(reason);
                }
            }
            SourceRows::Narrowed(found) => {
                keys = Some(match keys {
                    Some(keys) => shared_columns(keys, found),
                    None => found,
                });
            }
        }
    }
    if !why.is_empty() {
        return ReadRows::Whole { why };
    }
    match keys.into_iter().flatten().next() {
        Some(column) => ReadRows::Keyed { column },
        None => ReadRows::LookedUp,
    }
}

/// For each of `sources`, `None` when nothing narrows it, and otherwise the
/// columns its scope compares with values, in the order `RowScope::key` looks
/// for one in: the scope `scope_lookup::scopes` would bind for a run whose
/// values all bind.
fn narrowed(sources: &Sources) -> Vec<Option<Vec<String>>> {
    let mut found: Vec<Option<Vec<String>>> = sources.sources.iter().map(|_| None).collect();
    for &position in &sources.order {
        let source = &sources.sources[position];
        let mut keys: Option<Vec<String>> = None;
        for term in [&source.term, &source.carried].into_iter().flatten() {
            narrow(&mut keys, term.key_columns());
        }
        for membership in &source.memberships {
            let column = match (&membership.member, &membership.value) {
                (MemberValue::Column(member), PartnerValue::Column(column))
                    if found[membership.partner]
                        .as_ref()
                        .is_some_and(|keys| keys.contains(column)) =>
                {
                    Some(member.clone())
                }
                _ => None,
            };
            narrow(&mut keys, column.into_iter().collect());
        }
        for sides in &source.either {
            let sides = sides
                .iter()
                .map(|side| side.term.as_ref().map(ScopeTerm::key_columns).unwrap_or_default());
            if let Some(shared) = sides.reduce(shared_columns) {
                narrow(&mut keys, shared);
            }
        }
        if sources
            .sorted
            .as_ref()
            .is_some_and(|sorted| sorted.bounds(position, &source.memberships))
        {
            narrow(&mut keys, Vec::new());
        }
        found[position] = keys;
    }
    for window in &sources.windows {
        narrow(&mut found[window.item], Vec::new());
    }
    found
}

/// Add a narrowing that fixes `columns` to the columns of `keys`.
fn narrow(keys: &mut Option<Vec<String>>, columns: Vec<String>) {
    add_columns(keys.get_or_insert_default(), columns);
}

/// What the live query that reads `whole` whole depends on when it runs
/// `sql` on `connection`.
#[cfg(any(test, feature = "test-utils"))]
pub fn read_report(
    connection: &rusqlite::Connection,
    sql: &str,
    whole: &crate::WholeViews,
) -> rusqlite::Result<ReadReport> {
    let statement = connection.prepare(sql)?;
    Ok(ReadReport::of(&ScopePlan::build(connection, sql, &statement, whole)?))
}

#[cfg(test)]
#[path = "read_report_tests.rs"]
mod tests;
