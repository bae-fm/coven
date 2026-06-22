//! Production conflict resolution for changeset application.
//!
//! Row-level Last-Writer-Wins (LWW) on the `_updated_at` column: each side's
//! value is parsed as an HLC [`Timestamp`] and the greater one wins (the parsed
//! order equals the lexicographic order of the string form, but parsing also lets
//! the receiver reject a stamp it can't trust). The `_updated_at` column index is
//! looked up dynamically from the schema (via [`TableColumns`]) so adding columns
//! to the end of a table is safe.
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
//! and the pure per-row decision; FK-violation tracking is an `Arc<AtomicBool>`
//! the closure owns, since `Fn` forbids `&mut` state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rusqlite::session::{ChangesetItem, ConflictAction, ConflictType};
use rusqlite::Connection;
use tracing::warn;

use super::hlc::Timestamp;
use crate::changeset::value_ref_to_string;
use crate::database::DbError;

/// Column indices for a synced table, looked up from `PRAGMA table_info`.
pub struct TableColumns {
    /// Index of the `_updated_at` column.
    pub updated_at: usize,
}

/// Schema info for all synced tables: maps table name to column indices. Built
/// once before an apply and moved (owned) into the conflict closure, which must
/// be `'static`.
pub struct TableSchema {
    tables: HashMap<String, TableColumns>,
}

impl TableSchema {
    /// Build schema info by querying `PRAGMA table_info` for each synced table.
    /// A registered table that has no `_updated_at` column is a host integration
    /// error and surfaces as `Err`.
    pub fn from_db(conn: &Connection, synced_tables: &[&str]) -> Result<Self, DbError> {
        let mut tables = HashMap::new();

        for &table in synced_tables {
            let mut stmt = conn
                .prepare(&format!(
                    "PRAGMA table_info({})",
                    super::session::quote_ident(table)
                ))
                .map_err(DbError::from)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get::<_, i64>(0)? as usize, r.get::<_, String>(1)?))
                })
                .map_err(DbError::from)?;

            let mut updated_at = None;
            for row in rows {
                let (col_index, name) = row.map_err(DbError::from)?;
                if name == "_updated_at" {
                    updated_at = Some(col_index);
                }
            }

            let updated_at = updated_at.ok_or_else(|| {
                DbError(format!("synced table {table} has no _updated_at column"))
            })?;
            tables.insert(table.to_string(), TableColumns { updated_at });
        }

        Ok(TableSchema { tables })
    }

    /// The `_updated_at` column index for a table, or `None` if the table was not
    /// in the synced set passed to `from_db`. A remote changeset can carry a table
    /// this device hasn't declared (a newer peer added it); rather than panic mid-
    /// apply, the caller treats an unresolved table as a row to omit.
    pub fn get(&self, table: &str) -> Option<&TableColumns> {
        self.tables.get(table)
    }
}

/// The production LWW decision for one conflicting changeset row.
///
/// Rules:
/// - **DATA** (same row, both sides edited): compare `_updated_at`. Newer wins.
/// - **NOTFOUND** (row deleted locally, incoming UPDATE): OMIT (delete wins).
/// - **CONFLICT** (row exists, incoming INSERT): compare `_updated_at`. Newer wins.
/// - **CONSTRAINT** (uniqueness/other constraint): OMIT and flag for retry.
///
/// FOREIGN_KEY conflicts never reach here — [`super::apply`] resolves them before
/// calling this, because that conflict type's iterator does not expose the row.
///
/// For DATA/CONFLICT, `item.new_value(uat)` is the incoming `_updated_at` and
/// `item.conflict(uat)` the existing local one; either can be absent (an
/// unchanged column in an UPDATE) → `None` → OMIT (keep local). Both are parsed
/// as HLC [`Timestamp`]s; an unparseable value keeps local. A grossly-future
/// incoming stamp — beyond `receiver_wall_ms` + [`super::hlc::MAX_FUTURE_SKEW_MS`]
/// — is refused (kept local) so a broken clock can't win every conflict. `fk_flag`
/// is set on a CONSTRAINT so the caller can retry the changeset once its missing
/// parents have landed.
pub fn lww_conflict_handler(
    conflict_type: ConflictType,
    item: ChangesetItem,
    table: &str,
    schema: &TableSchema,
    receiver_wall_ms: u64,
    fk_flag: &Arc<AtomicBool>,
) -> ConflictAction {
    match conflict_type {
        ConflictType::SQLITE_CHANGESET_DATA | ConflictType::SQLITE_CHANGESET_CONFLICT => {
            let Some(cols) = schema.get(table) else {
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
            let uat = cols.updated_at;
            let incoming = item
                .new_value(uat)
                .ok()
                .and_then(value_ref_to_string)
                .and_then(|s| Timestamp::parse(&s));
            let local = item
                .conflict(uat)
                .ok()
                .and_then(value_ref_to_string)
                .and_then(|s| Timestamp::parse(&s));

            match (incoming, local) {
                (Some(inc), Some(loc)) => {
                    if !inc.is_within_future_bound(receiver_wall_ms) {
                        // A grossly-future stamp (broken clock / buggy client) would
                        // beat every honest stamp and win this conflict forever.
                        // Refuse it: keep local.
                        warn!(
                            table,
                            incoming = %inc,
                            receiver_wall_ms,
                            "incoming _updated_at is grossly beyond the offline-skew \
                             allowance, refusing to let it win; keeping local"
                        );
                        ConflictAction::SQLITE_CHANGESET_OMIT
                    } else if inc > loc {
                        ConflictAction::SQLITE_CHANGESET_REPLACE
                    } else {
                        ConflictAction::SQLITE_CHANGESET_OMIT
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

        ConflictType::SQLITE_CHANGESET_CONSTRAINT => {
            fk_flag.store(true, Ordering::Relaxed);
            ConflictAction::SQLITE_CHANGESET_OMIT
        }

        // FOREIGN_KEY is filtered out in `apply`; `ConflictType` is also
        // `#[non_exhaustive]` (an `UNKNOWN` sentinel for codes outside the five
        // SQLite documents). None reach a well-formed apply here, so keep local.
        _ => {
            warn!(table, "unexpected changeset conflict type, keeping local");
            ConflictAction::SQLITE_CHANGESET_OMIT
        }
    }
}
