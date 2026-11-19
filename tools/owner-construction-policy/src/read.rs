//! The read rule's rows: what the typed lints of the read rule read
//! (`read_rule`), and the structural checker's read rule (`read_shapes`),
//! with its shapes and the findings it freezes or allows.

use std::collections::BTreeSet;

use serde::Deserialize;

use crate::home_holds;

/// The read rule, which the typed lints hold: a read does no work SQL does
/// and keeps no state between reads. Each type is named by the path that
/// defines it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRule {
    /// Where the rule is written, which every finding names.
    pub rule: String,
    /// The types of what a read receives and of the receivers whose callbacks
    /// are reads: code whose inputs name one is read code.
    pub read_types: BTreeSet<String>,
    /// The live queries: a type holding one is a read's holder, and the
    /// callback a live query's `process` runs works on what it read.
    pub live_query_types: BTreeSet<String>,
    /// The row a statement hands its row mapper: what a function or closure
    /// taking one makes is a row type, unless it is one column's value.
    pub statement_rows: BTreeSet<String>,
    /// The traits of a value one column converts to, such as rusqlite's
    /// `FromSql`: a row mapper whose result implements one reads one column,
    /// and the rows of its read are the collections that flow from what the
    /// read returns.
    pub column_values: BTreeSet<String>,
    /// The read plumbing, which is no read code: source files, and
    /// directories (ending in `/`) of them.
    #[serde(default)]
    pub plumbing: Vec<String>,
}

impl ReadRule {
    /// Whether the workspace file `path` is read plumbing.
    pub fn is_plumbing(&self, path: &str) -> bool {
        names_file(&self.plumbing, path)
    }
}

/// Whether one of `entries`, files and directories (ending in `/`) of them,
/// holds the workspace file `path`.
fn names_file(entries: &[String], path: &str) -> bool {
    entries.iter().any(|entry| home_holds(entry, path))
}

/// The shapes `read-shapes` finds in read code, by the id findings and list
/// rows name them with.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Shape {
    /// Rust sorting rows.
    Sort,
    /// Rust filtering rows.
    Filter,
    /// Rust grouping rows into a map or set.
    Group,
    /// Rust counting, summing or folding rows.
    Aggregate,
    /// Rust picking a row out of rows.
    Pick,
    /// A statement run once for each row of another.
    QueryPerRow,
    /// A statement keyed by a value an earlier statement returned.
    KeyedStatement,
    /// Memory kept between reads.
    KeptState,
    /// A column only ever counted up, to run a live query again.
    Counter,
    /// A column only ever counted up or reset to zero, to run a live query again.
    ResetCounter,
    /// A request's integer field counted up by one, to run a live query again.
    RequestBump,
    /// A comparison with a fixed limit, which picks one read path over another.
    Cutoff,
    /// Rust joining one statement's rows to another's by a key or a place.
    Join,
    /// A branch on how many rows a slice holds, the count and the case done in
    /// Rust.
    LengthBranch,
    /// A row dropped in Rust because one of its optional columns is empty.
    DropRow,
}

impl Shape {
    /// Whether a function that only a live query's `process` runs may make a
    /// finding of this shape: one that sorts, filters, groups, counts or
    /// picks among the rows of the one result that `process` passes the
    /// function. Keeping state, running a statement and choosing a read path
    /// do none of these.
    pub fn shapes_display(self) -> bool {
        match self {
            Shape::Sort
            | Shape::Filter
            | Shape::Group
            | Shape::Aggregate
            | Shape::Pick
            | Shape::Join
            | Shape::LengthBranch
            | Shape::DropRow => true,
            Shape::QueryPerRow
            | Shape::KeyedStatement
            | Shape::KeptState
            | Shape::Counter
            | Shape::ResetCounter
            | Shape::RequestBump
            | Shape::Cutoff => false,
        }
    }

    /// The id findings and list rows use.
    pub fn id(self) -> &'static str {
        match self {
            Shape::Sort => "sort",
            Shape::Filter => "filter",
            Shape::Group => "group",
            Shape::Aggregate => "aggregate",
            Shape::Pick => "pick",
            Shape::QueryPerRow => "query-per-row",
            Shape::KeyedStatement => "keyed-statement",
            Shape::KeptState => "kept-state",
            Shape::Counter => "counter",
            Shape::ResetCounter => "reset-counter",
            Shape::RequestBump => "request-bump",
            Shape::Cutoff => "cutoff",
            Shape::Join => "join",
            Shape::LengthBranch => "length-branch",
            Shape::DropRow => "drop-row",
        }
    }
}

/// The read rule (`read_shapes.rs`): what read code is, where the read
/// plumbing is, and the findings the workspace has frozen or allowed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadShapes {
    /// The types of what a read receives and of the receivers whose callbacks
    /// are reads: a function or closure taking one is read code, and so is a
    /// closure passed to a callee that names one in its parameter's type or
    /// its generic bound, or to a method of one.
    #[serde(default)]
    pub read_types: BTreeSet<String>,
    /// The live queries: a type holding one is a read's holder, and keeps no
    /// state beside it.
    #[serde(default)]
    pub live_query_types: BTreeSet<String>,
    /// The keyed latest-value streams, which a holder of a live query may
    /// hold: the values that change many times a second travel on them.
    #[serde(default)]
    pub value_stream_types: BTreeSet<String>,
    /// The live queries that take a request as their first type argument: the
    /// request is the type of what the query is asked, and its integer fields
    /// are not counted up to run the query again.
    #[serde(default)]
    pub request_query_types: BTreeSet<String>,
    /// The types that keep memory between reads: locks, cells and atomics.
    #[serde(default)]
    pub kept_state_types: BTreeSet<String>,
    /// The read plumbing, which is no read code: source files, and
    /// directories (ending in `/`) of them.
    #[serde(default)]
    pub plumbing: Vec<String>,
    /// Where the rule is written, which every finding's remedy names.
    pub rule: String,
    /// The violations the workspace has today; they only shrink.
    #[serde(default)]
    pub frozen: Vec<FrozenRow>,
    /// Findings that are not a read's work.
    #[serde(default)]
    pub allowed: Vec<AllowedRow>,
}

/// A violation the workspace has today: `count` findings of `shape` in `item`
/// of `path`, and what the audit found there.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenRow {
    /// The source file.
    pub path: String,
    /// The function, method (`Type::method`), static or const that holds the
    /// findings.
    pub item: String,
    /// What the findings find.
    pub shape: Shape,
    /// How many findings the item holds.
    pub count: usize,
    /// What the audit found there.
    pub finding: String,
}

/// A finding that is not a read's work. The row lists either code that works
/// on values that are not rows, with `reason` saying what the values are, or
/// a function that only a live query's `process` runs to sort, filter,
/// group, count or pick among one result's rows for display, with `reason`
/// saying what the function does and `process` naming the query whose result
/// it takes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedRow {
    /// The source file.
    pub path: String,
    /// The function, method (`Type::method`), static or const that holds the
    /// findings.
    pub item: String,
    /// What the findings find.
    pub shape: Shape,
    /// How many findings the item holds.
    pub count: usize,
    /// What the values are, or what the function does for display.
    pub reason: String,
    /// The live query whose result the function takes, for a function that
    /// only that query's `process` runs.
    #[serde(default)]
    pub process: Option<String>,
}

/// The two lists of a policy's read rule.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ListKind {
    /// Violations the workspace has today.
    Frozen,
    /// Findings that are not a read's work.
    Allowed,
}

impl ListKind {
    /// The list's key in the policy file.
    pub fn name(self) -> &'static str {
        match self {
            ListKind::Frozen => "frozen",
            ListKind::Allowed => "allowed",
        }
    }
}

/// One row of either list.
pub struct ListedRow<'a> {
    /// The list that holds the row.
    pub list: ListKind,
    /// The source file.
    pub path: &'a str,
    /// The item that holds the findings.
    pub item: &'a str,
    /// What the findings find.
    pub shape: Shape,
    /// How many findings the item holds.
    pub count: usize,
    /// The audit's finding (`frozen`) or the reason (`allowed`).
    pub note: &'a str,
}

impl ReadShapes {
    /// Whether a file is read plumbing.
    pub fn is_plumbing(&self, path: &str) -> bool {
        names_file(&self.plumbing, path)
    }

    /// Every row of both lists.
    pub fn listed(&self) -> Vec<ListedRow<'_>> {
        let frozen = self.frozen.iter().map(|row| ListedRow {
            list: ListKind::Frozen,
            path: &row.path,
            item: &row.item,
            shape: row.shape,
            count: row.count,
            note: &row.finding,
        });
        let allowed = self.allowed.iter().map(|row| ListedRow {
            list: ListKind::Allowed,
            path: &row.path,
            item: &row.item,
            shape: row.shape,
            count: row.count,
            note: &row.reason,
        });
        frozen.chain(allowed).collect()
    }
}
