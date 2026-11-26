//! What the compiler reports of each call it compiled.

use std::collections::BTreeSet;

use super::header::{CallKind, Header};
use super::source::Call;
use super::Location;

/// One `fragment!` or `read!` call that compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledCall {
    /// Which macro the call is.
    pub kind: CallKind,
    /// The call's name.
    pub name: String,
    /// Where the call's macro path is.
    pub location: Location,
    /// The call's SQL as its author wrote it, with its spreads in place.
    pub sql: String,
    /// The tables and views the call's own SQL reads by name in FROM and
    /// JOIN, lowercase, at any depth, with a schema's prefix dropped. It
    /// leaves out the call's own CTEs, its relations, its table-valued
    /// functions, and what the fragments it spreads read.
    pub reads: BTreeSet<String>,
}

impl CompiledCall {
    /// The report of `call`, whose header is `header` and whose own SQL reads
    /// `reads`.
    pub(crate) fn of(call: &Call, header: &Header, reads: BTreeSet<String>) -> Self {
        Self {
            kind: header.kind(),
            name: header.name.clone(),
            location: call.location.clone(),
            sql: header.sql.clone(),
            reads,
        }
    }
}

#[cfg(test)]
#[path = "crate_report_tests.rs"]
mod tests;
