//! Row-level sync gating.
//!
//! A host declares a boolean **gate** column on a *root* synced table (via
//! [`SyncedTable::gated_by`](crate::protocol::synced_schema::SyncedTable::gated_by)). A root row
//! is shared — i.e. it syncs to peers — iff its gate column is true. The gate
//! flows down *declared foreign keys*: a child row is shared iff the row at the
//! top of its FK chain (its gated-ancestor root) is shared. A
//! [`SyncedTable::remote_root`](crate::protocol::synced_schema::SyncedTable::remote_root) is a
//! root whose rows and FK descendants always sync, and whose blobs are always
//! Remote. Rows that are not gated and not FK-descendants of a gated or remote root
//! always sync.
//!
//! The gate also flows **up** for declared *ancestors*
//! ([`SyncedTable::gated_by_descendants`](crate::protocol::synced_schema::SyncedTable::gated_by_descendants)).
//! An ancestor is an always-shared FK *parent* of gated rows (e.g. an album is
//! the FK parent of releases). Left alone it would sync even when its whole gated
//! subtree is cut, landing on peers as an orphan with zero children. A
//! gated-by-descendants ancestor is shared iff some inferred child table still
//! holds a kept row referencing it; the keep composes recursively up the FK chain
//! to the gated roots at the bottom. The keep-children are inferred from the live
//! FK graph, never declared — except a child the host marks an *asset*
//! ([`SyncedTable::asset`](crate::protocol::synced_schema::SyncedTable::asset)), a decoration
//! (cover, artist image) that rides its subject's gate but never grants keep, so
//! it is excluded from the subject's keep-children. An asset is typically a
//! host-provided blob; see the [blob concept tree](crate::blob) for the blob-side
//! vocabulary.
//!
//! [`gate_outbound`] is the one entry point. Given the changeset a cycle
//! captured, it returns a new changeset with gated-false rows cut, plus — when a
//! root's gate flips false→true this cycle — full-state INSERTs for that root's
//! whole now-visible subtree (peers never saw it while it was private), so the
//! promotion lands as a complete consistent subtree on every peer.
//!
//! Revoke (gate true→false) is a *retract*: when a previously-shared root flips
//! true→false this cycle, the rows that leave the shared set are emitted as
//! DELETEs so peers remove them — the exact mirror of the false→true re-emit. The
//! flipping device keeps its rows locally (now gated-false = local-only); retract
//! writes only to the outbound changeset, never to the live tables, and fires once
//! on the flip cycle. A root that was never shared has nothing on peers to retract
//! and emits nothing.
//!
//! ## How it is built
//!
//! - **Cut / keep** uses `sqlite3changegroup_add_change`: we walk the captured
//!   changeset and, at each kept row's iterator position, append the change
//!   verbatim into a changegroup, then `sqlite3changegroup_output` the result.
//!   Kept rows keep their exact binary form; nothing is reconstructed.
//! - **Re-emit on flip** uses `sqlite3session_diff`: we attach an empty,
//!   schema-identical in-memory database, create a session on it, diff each
//!   gated table against `main` (empty vs. populated yields a full-state INSERT
//!   per current row), then scope those INSERTs through the same keep-filter,
//!   restricted to the roots that flipped this cycle, and merge them into the
//!   output. The changegroup dedups by primary key, so a row already present
//!   from the captured changeset is not duplicated.
//! - **Retract on flip** is the reverse `sqlite3session_diff`: we create the
//!   session on the *empty* clone and diff `from = "main"` (populated → empty
//!   yields a full-state DELETE per current row), then scope those DELETEs to the
//!   rows leaving the shared set — the structural connected component of the roots
//!   that flipped true→false this cycle, minus the rows still kept by another
//!   managed root — and merge them in.

use std::ffi::c_int;

use rusqlite::{Connection, OptionalExtension, Params};

use crate::database::quote_ident;

mod audience;
mod ffi;
mod model;
mod outbound;

pub(crate) use audience::{
    active_circle_control, align_inbound_scoped_root_audiences, audience_moves,
    capture_routing_changes, filter_inbound_circle_changeset, filter_inbound_store_rows,
    is_routing_table, live_row_audience, normalize_inbound_store_changeset, partition_outbound,
    prune_ineligible_scoped_rows, prune_private_routes_without_rows, retain_snapshot_audience_rows,
    store_audience_transitions, validate_scoped_foreign_key_audiences,
    validate_snapshot_routing_state, AudienceMove, AudiencePartition, CirclePartitionControl,
    RoutingChanges, StoreAudienceTransitions,
};
pub(crate) use model::Gates;
#[cfg(test)]
pub(crate) use model::{from_tables_call_count, reset_from_tables_call_count};
pub(crate) use outbound::attach_empty_clone;
pub(crate) use outbound::query_truth;

/// [`crate::database::table_columns`] with its `rusqlite::Error` adapted
/// into the gate's error at the boundary.
fn gate_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, GateError> {
    crate::database::table_columns(conn, table)
        .map_err(|e| GateError::Sql(format!("read columns of {table}"), e))
}

/// Every row id in `table`, in id order, for the passes that walk a whole table
/// row by row.
fn all_row_ids(conn: &Connection, table: &str) -> Result<Vec<String>, GateError> {
    let sql = format!(
        "SELECT {id} FROM {table} ORDER BY {id}",
        id = quote_ident("id"),
        table = quote_ident(table),
    );
    query_mapped_rows(conn, &sql, [], |row| row.get::<_, String>(0))
}

fn execute_batch(conn: &Connection, sql: &str) -> Result<(), GateError> {
    conn.execute_batch(sql)
        .map_err(|e| GateError::Sql(format!("execute batch: {sql}"), e))
}

/// The shared row query, with the statement that failed named in the error the
/// gate reports.
fn query_mapped_rows<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    mapper: F,
) -> Result<Vec<T>, GateError>
where
    P: Params,
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    crate::database::query_mapped_rows(conn, sql, params, mapper)
        .map_err(|e| GateError::Sql(format!("query: {sql}"), e))
}

fn query_row_optional<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    mapper: F,
) -> Result<Option<T>, GateError>
where
    P: Params,
    F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    conn.query_row(sql, params, mapper)
        .optional()
        .map_err(|e| GateError::Sql(format!("query: {sql}"), e))
}
/// Render a row column read against the live db as text, matching what the raw
/// changeset path produces for the same value, so a gate resolved from a live
/// row and one resolved from a changeset agree. The single rendering rule —
/// including SQLite's REAL→text — lives in the database changeset decoder.
fn row_value_to_string(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<Option<String>> {
    Ok(crate::database::value_ref_to_string(row.get_ref(idx)?))
}

#[derive(Debug)]
pub enum GateError {
    Ffi(&'static str, c_int),
    Session {
        operation: String,
        source: rusqlite::Error,
    },
    MissingGateColumn(String, String),
    MissingFkColumn(String, String),
    ForeignKeySchema(String),
    CompositeGateForeignKey {
        table: String,
        parent: String,
    },
    MissingAudienceParentDeclaration {
        table: String,
    },
    InvalidAudienceParentDeclaration {
        table: String,
        column: String,
        reason: String,
    },
    ScopedOutboundRequiresPartitioning {
        table: String,
    },
    InvalidAudience {
        table: String,
        value: Option<String>,
        reason: String,
    },
    InvalidInboundAudiencePackage(String),
    InvalidMaterializedRouting(String),
    MissingChangesetPrimaryKey(String),
    MissingAudienceRow {
        table: String,
        row_id: String,
    },
    MissingAudienceParent {
        table: String,
        row_id: Option<String>,
        parent: String,
    },
    CircleAuthority {
        circle_id: crate::protocol::circle::CircleId,
        active_records: usize,
    },
    /// A host write named a Circle whose control chain has terminated in a
    /// deletion. The Circle accepts no further content.
    CircleDeleted {
        circle_id: crate::protocol::circle::CircleId,
    },
    InvalidCircleControl {
        circle_id: crate::protocol::circle::CircleId,
        reason: String,
    },
    /// A `gated_by_descendants` ancestor (the table) has no inferred gated
    /// descendant — no synced table has a foreign key into it after the
    /// join-table back-edge is excluded. The keep would be vacuously false, so
    /// the declaration is a host error rather than a silent always-share.
    NoGatedDescendants(String),
    /// The gated tables form an FK cycle, so no parent-first apply order exists.
    FkCycle(Vec<String>),
    CreateTableSchema(crate::database::CreateTableSchemaError),
    Sql(String, rusqlite::Error),
    Cleanup {
        operation: Box<GateError>,
        cleanup: Box<GateError>,
    },
}

impl std::fmt::Display for GateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GateError::Ffi(func, rc) => write!(f, "{func} failed (rc={rc})"),
            GateError::Session { operation, source } => {
                write!(f, "session {operation} failed: {source}")
            }
            GateError::MissingGateColumn(tbl, col) => {
                write!(f, "gated table {tbl} has no gate column {col}")
            }
            GateError::MissingFkColumn(tbl, col) => {
                write!(f, "table {tbl} has no FK column {col}")
            }
            GateError::ForeignKeySchema(error) => write!(f, "foreign-key schema: {error}"),
            GateError::CompositeGateForeignKey { table, parent } => write!(
                f,
                "table {table} inherits its gate through a composite foreign key to {parent}, but gate inheritance requires one child column"
            ),
            GateError::MissingAudienceParentDeclaration { table } => write!(
                f,
                "scoped descendant table {table} must declare its audience-parent foreign key"
            ),
            GateError::InvalidAudienceParentDeclaration {
                table,
                column,
                reason,
            } => write!(
                f,
                "table {table} cannot inherit its audience through {column}: {reason}"
            ),
            GateError::ScopedOutboundRequiresPartitioning { table } => write!(
                f,
                "scoped root {table} must use audience-partitioned outbound capture"
            ),
            GateError::InvalidAudience {
                table,
                value,
                reason,
            } => write!(f, "scoped table {table} has invalid audience {value:?}: {reason}"),
            GateError::InvalidInboundAudiencePackage(reason) => {
                write!(f, "invalid inbound audience package: {reason}")
            }
            GateError::InvalidMaterializedRouting(reason) => {
                write!(f, "invalid materialized routing state: {reason}")
            }
            GateError::MissingChangesetPrimaryKey(table) => {
                write!(f, "scoped changeset row in {table} has no primary key")
            }
            GateError::MissingAudienceRow { table, row_id } => {
                write!(f, "scoped row {table}.{row_id} is absent while resolving its audience")
            }
            GateError::MissingAudienceParent {
                table,
                row_id,
                parent,
            } => write!(
                f,
                "scoped row {table}.{row_id:?} has no audience parent in {parent}"
            ),
            GateError::CircleAuthority {
                circle_id,
                active_records,
            } => write!(
                f,
                "circle {circle_id} has {active_records} active local access records; expected exactly one"
            ),
            GateError::CircleDeleted { circle_id } => {
                write!(f, "circle {circle_id} is deleted and accepts no writes")
            }
            GateError::InvalidCircleControl { circle_id, reason } => {
                write!(f, "circle {circle_id} has invalid active control: {reason}")
            }
            GateError::NoGatedDescendants(tbl) => {
                write!(
                    f,
                    "gated_by_descendants ancestor {tbl} has no inferred gated descendant: no \
                     synced table references it"
                )
            }
            GateError::FkCycle(tables) => {
                write!(f, "gated tables form an FK cycle: {}", tables.join(", "))
            }
            GateError::CreateTableSchema(error) => error.fmt(f),
            GateError::Sql(op, err) => write!(f, "{op} failed: {err}"),
            GateError::Cleanup { operation, cleanup } => {
                write!(
                    f,
                    "{operation}; temporary gate cleanup also failed: {cleanup}"
                )
            }
        }
    }
}

impl std::error::Error for GateError {}

impl From<crate::database::CreateTableSchemaError> for GateError {
    fn from(error: crate::database::CreateTableSchemaError) -> Self {
        Self::CreateTableSchema(error)
    }
}

#[cfg(test)]
mod tests;
