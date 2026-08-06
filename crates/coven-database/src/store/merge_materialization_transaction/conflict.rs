//! Row arbitration for changeset application: when an incoming row collides with
//! the local one, decide which whole row wins.
//!
//! The arbiter compares each side's `_updated_at`: both are parsed as HLC
//! [`Timestamp`]s and the greater one wins — the later writer of the row (the parsed
//! order equals the lexicographic order of the string form, but parsing also lets
//! the receiver reject a stamp it can't trust). An incoming DELETE is remove-wins:
//! a hard delete carries only the row's pre-delete stamp and cannot be
//! reconstructed from a later partial UPDATE, so the delete always wins and the row
//! stays gone. The `_updated_at` column index is looked up dynamically from the
//! schema so adding columns to the end of a table is safe.
//!
//! This is row-level: the losing row is dropped whole. Column-level survival of
//! concurrent edits to *different* columns of one row is handled upstream by the
//! premerge in [`crate::MergeMaterializationTransaction::apply_changeset`], before the changeset reaches this arbiter — the
//! arbiter only picks a winner for the collisions the premerge did not fold in.
//!
//! A member is trusted to author valid changesets, so this is robustness, not a
//! security boundary: a buggy client or a device with a grossly-wrong wall clock
//! can stamp a row far in the future — a value that would beat every honest stamp
//! and win every conflict forever — so the receiver bounds an incoming stamp to
//! its own wall clock plus an offline allowance
//! ([`coven_protocol::hlc::MAX_FUTURE_SKEW_MS`]) and refuses to let a grossly-future one win
//! (the matching refusal to let it ratchet the clock lives in the pull's HLC
//! advance — a rejected stamp never becomes an applied row there either).
//!
//! The decision runs inside the transaction owner's changeset application,
//! which is `Fn(ConflictType, ChangesetItem) -> ConflictAction + Send + 'static`.
//! This module provides the per-table column map (moved owned into the closure)
//! and the pure per-row decision.

use std::collections::HashMap;

use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetItem, ConflictAction, ConflictType};
use rusqlite::Connection;
use tracing::warn;

use crate::changeset::value_ref_to_string;
use crate::{table_columns, DbError};
use coven_protocol::hlc::Timestamp;
use coven_protocol::synced_schema::SyncedTable;

/// Schema info for all synced tables: maps table name to column indices. Built
/// once before an apply and moved (owned) into the conflict closure, which must
/// be `'static`.
pub struct TableSchema {
    updated_at_by_table: HashMap<String, usize>,
    columns_by_table: HashMap<String, Vec<String>>,
    synced_tables: Vec<SyncedTable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LwwComparison {
    IncomingWins,
    LocalWins,
    IncomingGrossFuture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncomingTimestampPolicy {
    Received { receiver_wall_ms: u64 },
    LocallyAuthored,
}

impl IncomingTimestampPolicy {
    pub fn received_wall_ms(self) -> Option<u64> {
        match self {
            Self::Received { receiver_wall_ms } => Some(receiver_wall_ms),
            Self::LocallyAuthored => None,
        }
    }
}

impl TableSchema {
    pub fn for_apply(
        conn: &Connection,
        synced_tables: &[SyncedTable],
        gates: &crate::Gates,
    ) -> Result<Self, DbError> {
        let mut tables = synced_tables.to_vec();
        if gates.has_scoped_graph() {
            for table in ["_coven_audience", "_coven_row_routes"] {
                tables.push(SyncedTable::new(
                    table,
                    coven_protocol::synced_schema::RowIdentity::SharedKey,
                ));
            }
        }
        Self::from_db(conn, &tables)
    }

    /// Build schema info by querying `PRAGMA table_info` for each synced table.
    /// A registered table that has no `_updated_at` column is a host integration
    /// error and surfaces as `Err`.
    pub fn from_db(conn: &Connection, synced_tables: &[SyncedTable]) -> Result<Self, DbError> {
        let mut updated_at_by_table = HashMap::new();
        let mut columns_by_table = HashMap::new();

        for synced_table in synced_tables {
            let table = synced_table.name();
            let columns = table_columns(conn, table).map_err(DbError::from)?;
            let updated_at = columns.iter().position(|name| name == "_updated_at");
            let updated_at = updated_at.ok_or_else(|| {
                DbError::Message(format!("synced table {table} has no _updated_at column"))
            })?;
            updated_at_by_table.insert(table.to_string(), updated_at);
            columns_by_table.insert(table.to_string(), columns);
        }

        Ok(TableSchema {
            updated_at_by_table,
            columns_by_table,
            synced_tables: synced_tables.to_vec(),
        })
    }

    /// The `_updated_at` column index for a table, or `None` if the table was not
    /// in the synced set passed to `from_db`. Incoming apply rejects an entire
    /// changeset containing an undeclared table before premerge or row arbitration.
    pub fn updated_at(&self, table: &str) -> Option<usize> {
        self.updated_at_by_table.get(table).copied()
    }

    pub fn columns(&self, table: &str) -> Option<&[String]> {
        self.columns_by_table.get(table).map(Vec::as_slice)
    }

    pub fn synced_tables(&self) -> &[SyncedTable] {
        &self.synced_tables
    }
}

pub(crate) fn compare_lww_stamps(
    table: &str,
    incoming: Timestamp,
    local: Timestamp,
    timestamp_policy: IncomingTimestampPolicy,
) -> LwwComparison {
    if let Some(receiver_wall_ms) = timestamp_policy.received_wall_ms() {
        if !incoming.is_within_future_bound(receiver_wall_ms) {
            warn!(
                table,
                incoming = %incoming,
                receiver_wall_ms,
                "incoming _updated_at is grossly beyond the offline-skew allowance, \
                 refusing to let it win; keeping local"
            );
            return LwwComparison::IncomingGrossFuture;
        }
    }
    if incoming > local {
        LwwComparison::IncomingWins
    } else {
        LwwComparison::LocalWins
    }
}

/// Arbitrate one conflicting changeset row: pick the winning row, or omit.
///
/// Rules:
/// - **DATA** (same row, both sides edited): incoming DELETE removes the row;
///   otherwise compare `_updated_at`. Newer wins.
/// - **NOTFOUND** (row deleted locally, incoming UPDATE): OMIT (delete wins).
/// - **CONFLICT** (row exists, incoming INSERT): compare `_updated_at`. Newer wins.
///
/// FOREIGN_KEY conflicts never reach here — the transaction owner resolves them before
/// calling this, because that conflict type's iterator does not expose the row.
/// CONSTRAINT conflicts are also handled by the transaction owner so the caller can
/// surface the table and roll back the entire changeset.
///
/// For DATA/CONFLICT, the incoming `_updated_at` is read from the side the op
/// records — `item.new_value(uat)` for an INSERT/UPDATE, `item.old_value(uat)` for
/// a DELETE (which has no "new" side) — and `item.conflict(uat)` is the existing
/// local one; either can be absent (an unchanged column in an UPDATE) → `None` →
/// OMIT (keep local). Incoming DELETE conflicts are remove-wins because a hard
/// delete carries only the row's pre-delete stamp and cannot be reconstructed
/// from a later partial UPDATE. Non-delete conflicts parse both stamps as HLC
/// [`Timestamp`]s; an unparseable value keeps local. A grossly-future incoming
/// stamp — beyond `receiver_wall_ms` + [`coven_protocol::hlc::MAX_FUTURE_SKEW_MS`] — is
/// refused (kept local) so a broken clock can't win every conflict.
pub(crate) fn arbitrate_row_conflict(
    conflict_type: ConflictType,
    item: ChangesetItem,
    table: &str,
    schema: &TableSchema,
    timestamp_policy: IncomingTimestampPolicy,
) -> ConflictAction {
    match conflict_type {
        ConflictType::SQLITE_CHANGESET_DATA | ConflictType::SQLITE_CHANGESET_CONFLICT => {
            let Some(uat) = schema.updated_at(table) else {
                // Incoming apply rejects an entire changeset containing an
                // undeclared table before this closure. This arm covers a direct
                // caller supplying a schema inconsistent with the item.
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
                    match compare_lww_stamps(table, inc, loc, timestamp_policy) {
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
