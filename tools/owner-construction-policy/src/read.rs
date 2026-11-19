//! The read rule's rows (`read_shapes`), which the typed lints of the read
//! rule read: the read rule's types and the read plumbing.

use std::collections::BTreeSet;

use serde::Deserialize;

use crate::home_holds;

/// The read rule: a read does no work SQL does and keeps no state between
/// reads. The typed lints hold it (`tools/typed-lints`, `read_rule.rs`).
/// Each type row names its type by path, the crate as Rust names it, its
/// modules and the type, as the owner rules' rows do.
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
    /// The row a statement hands its row mapper: what a function or closure
    /// taking one makes is a row type, unless it is one column's value.
    pub statement_rows: BTreeSet<String>,
    /// The traits of a value one column converts to, such as rusqlite's
    /// `FromSql`: a row mapper whose result implements one reads one column,
    /// and the rows of its read are the collections that flow from what the
    /// read returns.
    pub column_values: BTreeSet<String>,
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
    /// Where the rule is written, which every finding's help names.
    pub rule: String,
}

impl ReadShapes {
    /// Whether a file is read plumbing.
    pub fn is_plumbing(&self, path: &str) -> bool {
        self.plumbing.iter().any(|entry| home_holds(entry, path))
    }
}
