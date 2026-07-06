//! Production conflict resolution for changeset application.
//!
//! Row-level Last-Writer-Wins (LWW) on the `_updated_at` column: each side's
//! value is parsed as an HLC [`Timestamp`] and the greater one wins (the parsed
//! order equals the lexicographic order of the string form, but parsing also lets
//! the receiver reject a stamp it can't trust). The `_updated_at` column index is
//! looked up dynamically from the schema so adding columns to the end of a table
//! is safe.
//!
//! A member is trusted to author valid changesets, so this is robustness, not a
//! security boundary: a buggy client or a device with a grossly-wrong wall clock
//! can stamp a row far in the future. As a value that would beat every honest
//! stamp and win every conflict forever, so the receiver bounds an incoming stamp
//! to its own wall clock plus a generous offline allowance
//! ([`super::hlc::MAX_FUTURE_SKEW_MS`]) and refuses to let a grossly-future one win
//! (the matching refusal to let it ratchet the clock lives in the pull's HLC
//! advance — a rejected stamp never becomes an applied row there either).
//!
//! The logic runs inside the `apply_strm` conflict closure in [`super::apply`],
//! which is `Fn(ConflictType, ChangesetItem) -> ConflictAction + Send + 'static`.
//! This module provides the per-table column map (moved owned into the closure)
//! and the pure per-row decision.

use std::collections::HashMap;

use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetItem, ConflictAction, ConflictType};
use rusqlite::Connection;
use tracing::warn;

use super::hlc::Timestamp;
use super::session::table_columns;
use crate::changeset::value_ref_to_string;
use crate::database::DbError;

/// Schema info for all synced tables: maps table name to column indices. Built
/// once before an apply and moved (owned) into the conflict closure, which must
/// be `'static`.
pub struct TableSchema {
    updated_at_by_table: HashMap<String, usize>,
    columns_by_table: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LwwComparison {
    IncomingWins,
    LocalWins,
    IncomingGrossFuture,
}

impl TableSchema {
    /// Build schema info by querying `PRAGMA table_info` for each synced table.
    /// A registered table that has no `_updated_at` column is a host integration
    /// error and surfaces as `Err`.
    pub fn from_db(conn: &Connection, synced_tables: &[&str]) -> Result<Self, DbError> {
        let mut updated_at_by_table = HashMap::new();
        let mut columns_by_table = HashMap::new();

        for &table in synced_tables {
            let columns = table_columns(conn, table).map_err(DbError::from)?;
            let updated_at = columns.iter().position(|name| name == "_updated_at");
            let updated_at = updated_at.ok_or_else(|| {
                DbError(format!("synced table {table} has no _updated_at column"))
            })?;
            updated_at_by_table.insert(table.to_string(), updated_at);
            columns_by_table.insert(table.to_string(), columns);
        }

        Ok(TableSchema {
            updated_at_by_table,
            columns_by_table,
        })
    }

    /// The `_updated_at` column index for a table, or `None` if the table was not
    /// in the synced set passed to `from_db`. A remote changeset can carry a table
    /// this device hasn't declared (a newer peer added it); rather than panic mid-
    /// apply, the caller treats an unresolved table as a row to omit.
    pub fn updated_at(&self, table: &str) -> Option<usize> {
        self.updated_at_by_table.get(table).copied()
    }

    pub fn columns(&self, table: &str) -> Option<&[String]> {
        self.columns_by_table.get(table).map(Vec::as_slice)
    }
}

pub fn compare_lww_stamps(
    table: &str,
    incoming: Timestamp,
    local: Timestamp,
    receiver_wall_ms: u64,
) -> LwwComparison {
    if !incoming.is_within_future_bound(receiver_wall_ms) {
        warn!(
            table,
            incoming = %incoming,
            receiver_wall_ms,
            "incoming _updated_at is grossly beyond the offline-skew allowance, \
             refusing to let it win; keeping local"
        );
        LwwComparison::IncomingGrossFuture
    } else if incoming > local {
        LwwComparison::IncomingWins
    } else {
        LwwComparison::LocalWins
    }
}

/// The production LWW decision for one conflicting changeset row.
///
/// Rules:
/// - **DATA** (same row, both sides edited): incoming DELETE removes the row;
///   otherwise compare `_updated_at`. Newer wins.
/// - **NOTFOUND** (row deleted locally, incoming UPDATE): OMIT (delete wins).
/// - **CONFLICT** (row exists, incoming INSERT): compare `_updated_at`. Newer wins.
///
/// FOREIGN_KEY conflicts never reach here — [`super::apply`] resolves them before
/// calling this, because that conflict type's iterator does not expose the row.
/// CONSTRAINT conflicts are also handled in [`super::apply`] so the caller can
/// surface the table that hit a non-retryable uniqueness/constraint clash.
///
/// For DATA/CONFLICT, the incoming `_updated_at` is read from the side the op
/// records — `item.new_value(uat)` for an INSERT/UPDATE, `item.old_value(uat)` for
/// a DELETE (which has no "new" side) — and `item.conflict(uat)` is the existing
/// local one; either can be absent (an unchanged column in an UPDATE) → `None` →
/// OMIT (keep local). Incoming DELETE conflicts are remove-wins because a hard
/// delete carries only the row's pre-delete stamp and cannot be reconstructed
/// from a later partial UPDATE. Non-delete conflicts parse both stamps as HLC
/// [`Timestamp`]s; an unparseable value keeps local. A grossly-future incoming
/// stamp — beyond `receiver_wall_ms` + [`super::hlc::MAX_FUTURE_SKEW_MS`] — is
/// refused (kept local) so a broken clock can't win every conflict.
pub fn lww_conflict_handler(
    conflict_type: ConflictType,
    item: ChangesetItem,
    table: &str,
    schema: &TableSchema,
    receiver_wall_ms: u64,
) -> ConflictAction {
    match conflict_type {
        ConflictType::SQLITE_CHANGESET_DATA | ConflictType::SQLITE_CHANGESET_CONFLICT => {
            let Some(uat) = schema.updated_at(table) else {
                // The changeset carries a table this device doesn't declare (a
                // newer peer's schema). We can't resolve its `_updated_at`, so we
                // can't LWW it — omit rather than blindly apply a row we don't
                // understand.
                warn!(
                    table,
                    "conflict on a table not in this device's synced set, omitting the row"
                );
                return ConflictAction::SQLITE_CHANGESET_OMIT;
            };
            // Read each side's `_updated_at` and parse it to an HLC `Timestamp`. A
            // rusqlite error reading the column (an API failure on a known column,
            // genuinely exceptional) is logged distinctly from a value that is simply
            // absent or doesn't parse — the latter falls through to the `_` arm below.
            let read_stamp = |v: Result<rusqlite::types::ValueRef, rusqlite::Error>, side: &str| {
                match v {
                    Ok(value) => value_ref_to_string(value).and_then(|s| Timestamp::parse(&s)),
                    Err(e) => {
                        warn!(table, side, error = %e, "failed to read _updated_at column for conflict resolution");
                        None
                    }
                }
            };
            // The incoming `_updated_at` lives on whichever side the op records: a
            // DELETE carries only its old values (no "new" side), an INSERT/UPDATE
            // carry the new value. Reading `new_value` for a DELETE returns None,
            // which would keep local and leave a DELETE whose row diverges from the
            // peer's copy unapplied — a zombie (the exact strand gate retract fixes,
            // where the retracted root's flip bumped its gate column + `_updated_at`
            // so its synthetic DELETE no longer matches the peer's pre-flip row).
            let incoming_code = match item.op() {
                Ok(op) => op.code(),
                Err(error) => {
                    warn!(
                        table,
                        error = %error,
                        "failed to read changeset operation for conflict resolution; aborting apply"
                    );
                    return ConflictAction::SQLITE_CHANGESET_ABORT;
                }
            };
            let incoming_is_delete = incoming_code == Action::SQLITE_DELETE;
            if incoming_is_delete {
                return ConflictAction::SQLITE_CHANGESET_REPLACE;
            }
            let incoming_raw = item.new_value(uat);
            let incoming = read_stamp(incoming_raw, "incoming");
            let local = read_stamp(item.conflict(uat), "local");

            match (incoming, local) {
                (Some(inc), Some(loc)) => {
                    match compare_lww_stamps(table, inc, loc, receiver_wall_ms) {
                        LwwComparison::IncomingWins => ConflictAction::SQLITE_CHANGESET_REPLACE,
                        LwwComparison::LocalWins | LwwComparison::IncomingGrossFuture => {
                            ConflictAction::SQLITE_CHANGESET_OMIT
                        }
                    }
                }
                _ => {
                    warn!(
                        table,
                        "conflict without parseable _updated_at values, keeping local"
                    );
                    ConflictAction::SQLITE_CHANGESET_OMIT
                }
            }
        }

        // Row was deleted locally, incoming changeset has an UPDATE. Delete wins.
        ConflictType::SQLITE_CHANGESET_NOTFOUND => ConflictAction::SQLITE_CHANGESET_OMIT,

        // FOREIGN_KEY and CONSTRAINT are filtered out in `apply`; `ConflictType`
        // is also `#[non_exhaustive]` (an `UNKNOWN` sentinel for codes outside
        // the five SQLite documents). None reach a well-formed apply here, so
        // keep local.
        _ => {
            warn!(table, "unexpected changeset conflict type, keeping local");
            ConflictAction::SQLITE_CHANGESET_OMIT
        }
    }
}
