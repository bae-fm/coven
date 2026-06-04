/// Production session management for sync.
///
/// `SyncSession` wraps the low-level FFI `Session` and attaches the
/// synced tables. It provides a clean start/changeset/end lifecycle.
use std::sync::OnceLock;

use super::session_ext::{Changeset, Session};

/// A table that participates in changeset sync, declared once at startup by the
/// host via [`set_synced_tables`].
///
/// A plain [`SyncedTable::new`] table syncs unconditionally — every row goes to
/// peers. [`SyncedTable::gated_by`] makes it a *gated root*: a boolean column
/// whose truth decides, per row, whether that row (and its declared
/// FK-descendants) is shared. A gated-false root and its subtree stay local;
/// flipping the gate true re-emits the whole now-visible subtree to peers. See
/// [`super::gate`] for the gating mechanics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncedTable {
    name: String,
    gate_column: Option<String>,
}

impl SyncedTable {
    /// An ungated synced table: every row syncs.
    pub fn new(name: impl Into<String>) -> Self {
        SyncedTable {
            name: name.into(),
            gate_column: None,
        }
    }

    /// Make this a gated root: rows sync iff the boolean `column` is true.
    pub fn gated_by(mut self, column: impl Into<String>) -> Self {
        self.gate_column = Some(column.into());
        self
    }

    /// The table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The gate column name, if this table is a gated root.
    pub fn gate_column(&self) -> Option<&str> {
        self.gate_column.as_deref()
    }
}

/// The tables that participate in changeset sync, declared once at startup by
/// the host via [`set_synced_tables`].
static SYNCED_TABLES: OnceLock<Vec<SyncedTable>> = OnceLock::new();

/// Declare the tables that participate in changeset sync. The host MUST call
/// this once at startup, before any sync session is created or `init_sync`
/// runs — it is a required integration step, not an optional tuning knob.
///
/// Each table must have an `id` text primary key at column 0 and an
/// `_updated_at TEXT NOT NULL` column (the HLC/LWW timestamp). Tables not listed
/// here are local-only and never synced — that is also the mechanism for keeping
/// device-local state (per-device pin/cache columns, local paths) out of sync:
/// put it in a table you don't list here.
///
/// A table declared with [`SyncedTable::gated_by`] is a gated root: only rows
/// whose gate column is true sync, and the gate flows down declared foreign keys
/// to descendant rows. See [`super::gate`].
///
/// Forgetting this is silent at the session layer (a session with no attached
/// tables just yields empty changesets), so [`super::cycle::init_sync`] treats an
/// empty set as a hard error and refuses to start sync.
pub fn set_synced_tables(tables: &[SyncedTable]) {
    let _ = SYNCED_TABLES.set(tables.to_vec());
}

/// The configured synced tables. Empty only when [`set_synced_tables`] was never
/// called — an integration bug that [`super::cycle::init_sync`] rejects.
pub fn synced_tables() -> &'static [SyncedTable] {
    SYNCED_TABLES.get().map(Vec::as_slice).unwrap_or(&[])
}

/// A sync session that tracks changes to all synced tables on a single connection.
///
/// Lifecycle:
/// 1. `SyncSession::start(db)` -- creates and attaches
/// 2. App writes normally through the connection
/// 3. `session.changeset()` -- grabs the binary diff (None if no changes)
/// 4. Session is dropped (or explicitly ended by dropping)
///
/// The session must be dropped before applying incoming changesets to avoid
/// contaminating the next outgoing changeset with other devices' changes.
pub struct SyncSession {
    session: Session,
}

impl SyncSession {
    /// Create a new sync session on the given raw sqlite3 connection,
    /// attaching all synced tables.
    ///
    /// # Safety
    /// `db` must be a valid, open sqlite3 connection pointer. The session
    /// must be dropped before the connection is closed.
    pub unsafe fn start(db: *mut libsqlite3_sys::sqlite3) -> Result<Self, SyncError> {
        let session = Session::new(db).map_err(SyncError::SessionCreate)?;

        for table in synced_tables() {
            session
                .attach(Some(table.name()))
                .map_err(|rc| SyncError::SessionAttach(table.name().to_string(), rc))?;
        }

        Ok(SyncSession { session })
    }

    /// Grab the binary changeset of all changes since the session started.
    /// Returns `None` if no changes were made (avoids pushing empty changesets).
    pub fn changeset(&self) -> Result<Option<Changeset>, SyncError> {
        let cs = self
            .session
            .changeset()
            .map_err(SyncError::ChangesetExtract)?;

        if cs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(cs))
        }
    }
}

#[derive(Debug)]
pub enum SyncError {
    /// Failed to create a session (sqlite3 error code).
    SessionCreate(i32),
    /// Failed to attach a table (table name, sqlite3 error code).
    SessionAttach(String, i32),
    /// Failed to extract a changeset (sqlite3 error code).
    ChangesetExtract(i32),
    /// Failed to apply a changeset (sqlite3 error code).
    ChangesetApply(i32),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::SessionCreate(rc) => write!(f, "session create failed (rc={rc})"),
            SyncError::SessionAttach(table, rc) => {
                write!(f, "session attach failed for {table} (rc={rc})")
            }
            SyncError::ChangesetExtract(rc) => write!(f, "changeset extract failed (rc={rc})"),
            SyncError::ChangesetApply(rc) => write!(f, "changeset apply failed (rc={rc})"),
        }
    }
}

impl std::error::Error for SyncError {}
