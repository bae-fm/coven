//! Apply a changeset to the connection with the production LWW conflict handler.
//!
//! Within a single changeset, SQLite defers FK checks — parent and child rows in
//! the same changeset are applied in recording order. Cross-changeset FK
//! dependencies are handled by applying changesets in seq order (parents are
//! always in earlier changesets than children).
//!
//! If a FK violation remains after applying a changeset, the conflict handler
//! reports it via `FOREIGN_KEY`/`CONSTRAINT` and the returned flag notes it for
//! the caller, which retries the changeset once its parents have landed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rusqlite::session::{ConflictAction, ConflictType};
use rusqlite::Connection;

use super::conflict::{lww_conflict_handler, TableSchema};
#[cfg(test)]
use super::session::SyncedTable;
use crate::database::DbError;

/// Result of applying a changeset.
pub struct ApplyResult {
    /// True if any FK/uniqueness constraint violations were reported. The caller
    /// may retry this changeset after applying other changesets that contain the
    /// missing parent rows.
    pub had_fk_violations: bool,
}

/// Apply `bytes` to `conn` using LWW conflict resolution, building the
/// [`TableSchema`] from `tables` once. A convenience wrapper over
/// [`apply_changeset_lww_with_schema`] for callers that apply a single changeset
/// and don't already hold a schema (tests, snapshot round-trips).
///
/// `receiver_wall_ms` is the receiver's current wall-clock millis, against which
/// a grossly-future incoming `_updated_at` is refused (see [`lww_conflict_handler`]).
#[cfg(test)]
pub fn apply_changeset_lww(
    conn: &Connection,
    bytes: &[u8],
    tables: &[SyncedTable],
    receiver_wall_ms: u64,
) -> Result<ApplyResult, DbError> {
    let table_refs: Vec<&str> = tables.iter().map(|t| t.name()).collect();
    let schema = Arc::new(TableSchema::from_db(conn, &table_refs)?);
    apply_changeset_lww_with_schema(conn, bytes, schema, receiver_wall_ms)
}

/// Apply `bytes` to `conn` using LWW conflict resolution against a pre-built
/// [`TableSchema`].
///
/// The schema's per-table `_updated_at` column index map is derived once (from
/// the live schema, so future migrations that add columns are safe) and reused
/// across every changeset in a pull, rather than re-querying `PRAGMA table_info`
/// per changeset. The conflict closure resolves each conflicting row's table from
/// its operation and decides REPLACE/OMIT by comparing `_updated_at`;
/// FK/constraint violations flip a shared flag for the caller to retry.
///
/// `schema` is an `Arc` so the same map moves into the `'static` conflict closure
/// without re-deriving it per call. `receiver_wall_ms` is the receiver's current
/// wall-clock millis, read once by the caller and moved into the closure to bound
/// a grossly-future incoming `_updated_at` (see [`lww_conflict_handler`]).
pub fn apply_changeset_lww_with_schema(
    conn: &Connection,
    bytes: &[u8],
    schema: Arc<TableSchema>,
    receiver_wall_ms: u64,
) -> Result<ApplyResult, DbError> {
    let fk_flag = Arc::new(AtomicBool::new(false));

    let closure_flag = fk_flag.clone();
    conn.apply_strm(
        &mut &bytes[..],
        // Apply to every table in the changeset (it only ever carries synced
        // tables; the gate already excluded local-only rows on the wire).
        Some(|_table: &str| true),
        move |conflict_type, item| {
            // A FOREIGN_KEY conflict's iterator supports ONLY `fk_conflicts()`;
            // calling `op()`/`new_value()`/`conflict()` on it is undefined (it
            // crashes the process). Resolve it first, without touching the row.
            if conflict_type == ConflictType::SQLITE_CHANGESET_FOREIGN_KEY {
                closure_flag.store(true, Ordering::Relaxed);
                return ConflictAction::SQLITE_CHANGESET_OMIT;
            }
            // Every other conflict type exposes the operation, so the table name
            // (needed to find the `_updated_at` column) is readable.
            let table = match item.op() {
                Ok(op) => op.table_name().to_string(),
                Err(_) => return ConflictAction::SQLITE_CHANGESET_OMIT,
            };
            lww_conflict_handler(
                conflict_type,
                item,
                &table,
                &schema,
                receiver_wall_ms,
                &closure_flag,
            )
        },
    )
    .map_err(DbError::from)?;

    Ok(ApplyResult {
        had_fk_violations: fk_flag.load(Ordering::Relaxed),
    })
}
