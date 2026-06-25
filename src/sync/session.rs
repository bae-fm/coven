//! Synced-table declarations and the shared identifier-quoting helper.
//!
//! [`SyncedTable`] is how a host declares which tables participate in changeset
//! sync. The set is no longer a process-global: the host passes it to
//! [`crate::database::Database::open`], which owns it for the lifetime of the
//! connection and hands it to the capture session, the gate, and apply.

/// A table that participates in changeset sync, declared at startup by the host
/// and passed to [`crate::database::Database::open`].
///
/// A plain [`SyncedTable::new`] table syncs unconditionally — every row goes to
/// peers. [`SyncedTable::gated_by`] makes it a *gated root*: a boolean column
/// whose truth decides, per row, whether that row (and its declared
/// FK-descendants) is shared. A gated-false root and its subtree stay local;
/// flipping the gate true re-emits the whole now-visible subtree to peers, and
/// flipping it false again retracts that subtree from peers (emitting deletes for
/// the rows leaving the shared set) while the rows stay local.
///
/// [`SyncedTable::gated_by_descendants`] is the upward complement: an
/// always-shared *ancestor* that should sync only while at least one gated
/// descendant survives. Without it, an album whose only releases are gated out
/// would still sync its own row and land on peers as an orphan with zero
/// children. A gated-by-descendants ancestor is cut exactly when its gated
/// subtree is empty, and the keep composes recursively up the foreign-key chain
/// (an artist syncs iff a surviving album references it, which syncs iff a
/// surviving release does). The keep-children are *inferred* from the
/// foreign-key graph, not declared — listing them by hand would restate the
/// schema and drift the moment a new foreign key is added.
///
/// A table is *either* a gated root *or* a gated-by-descendants ancestor *or*
/// plain — never two of these. See [`super::gate`] for the gating mechanics.
///
/// Each table must have an `id` text primary key at column 0 and an
/// `_updated_at TEXT NOT NULL` column (the HLC/LWW timestamp). Tables not in the
/// set the host passes to `Database::open` are local-only and never synced —
/// that is also the mechanism for keeping device-local state (per-device
/// pin/cache columns, local paths) out of sync: put it in a table you don't
/// declare. An empty set is rejected by [`super::cycle::init_sync`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncedTable {
    /// Every row syncs unconditionally.
    Plain { name: String },
    /// A gated root: a row syncs iff its boolean `gate_column` is true, and the
    /// gate flows down declared foreign keys to descendant rows.
    GatedRoot { name: String, gate_column: String },
    /// An always-shared ancestor kept alive by its gated subtree: a row syncs
    /// iff at least one foreign-key descendant table holds a surviving (kept)
    /// row referencing it. This variant is a *marker* only; the keep-children
    /// are inferred from the live foreign-key graph at gate-build time, never
    /// listed here.
    GatedByDescendants { name: String },
}

impl SyncedTable {
    /// An ungated synced table: every row syncs.
    pub fn new(name: impl Into<String>) -> Self {
        SyncedTable::Plain { name: name.into() }
    }

    /// Make this a gated root: rows sync iff the boolean `column` is true.
    pub fn gated_by(self, column: impl Into<String>) -> Self {
        SyncedTable::GatedRoot {
            name: self.name().to_string(),
            gate_column: column.into(),
        }
    }

    /// Make this an always-shared ancestor kept alive by its gated subtree: a
    /// row syncs iff a surviving (kept) descendant row references it. The
    /// keep-children are inferred from the foreign-key graph at gate-build time,
    /// so there is nothing to pass here.
    pub fn gated_by_descendants(self) -> Self {
        SyncedTable::GatedByDescendants {
            name: self.name().to_string(),
        }
    }

    /// The table name.
    pub fn name(&self) -> &str {
        match self {
            SyncedTable::Plain { name }
            | SyncedTable::GatedRoot { name, .. }
            | SyncedTable::GatedByDescendants { name } => name,
        }
    }

    /// The gate column name, if this table is a gated root.
    pub fn gate_column(&self) -> Option<&str> {
        match self {
            SyncedTable::Plain { .. } | SyncedTable::GatedByDescendants { .. } => None,
            SyncedTable::GatedRoot { gate_column, .. } => Some(gate_column),
        }
    }

    /// Whether this is a gated-by-descendants ancestor (kept alive by its gated
    /// subtree rather than by a column of its own).
    pub fn is_gated_by_descendants(&self) -> bool {
        matches!(self, SyncedTable::GatedByDescendants { .. })
    }
}

/// Quote an SQL identifier (table/column name), doubling any embedded quote, so
/// a trusted-but-unbindable name interpolates safely. Identifiers cannot be
/// passed as bound parameters; this is the safe interpolation path for them.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
