//! Synced-table declarations and the shared identifier-quoting helper.
//!
//! [`SyncedTable`] is how a host declares which tables participate in changeset
//! sync. The set is no longer a process-global: the host passes it to
//! [`crate::CovenBuilder::synced_tables`], and coven owns it for the lifetime of
//! the connection and hands it to the capture session, the gate, and apply.

/// A table that participates in changeset sync, declared at startup by the host
/// and passed to [`crate::CovenBuilder::synced_tables`].
///
/// A plain [`SyncedTable::new`] table syncs unconditionally — every row goes to
/// peers. [`SyncedTable::remote_root`] keeps that whole-table row sync and also
/// makes the row a blob-locality root whose blobs are always Remote.
/// [`SyncedTable::gated_by`] makes it a *gated root*: a boolean column whose
/// truth decides, per row, whether that row (and its declared FK-descendants) is
/// shared. A gated-false root and its subtree stay local; flipping the gate true
/// re-emits the whole now-visible subtree to peers, and flipping it false again
/// retracts that subtree from peers (emitting deletes for the rows leaving the
/// shared set) while the rows stay local.
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
/// A table is *either* a remote root, a gated root, a gated-by-descendants
/// ancestor, or plain — never two of these. See [`super::gate`] for the gating
/// mechanics.
/// Orthogonally, any table may *carry a blob* ([`SyncedTable::carries_blob`]):
/// blob-bearing-ness is a property of the row's columns, not of its gate role.
/// A table may also be marked an *asset* ([`SyncedTable::asset`]): a decoration
/// (a cover, an artist image) that rides its foreign-key subject's gate but never
/// keeps that subject alive. Asset-ness is likewise independent of the gate role.
///
/// Each table must have an `id` text primary key at column 0 and an
/// `_updated_at TEXT NOT NULL` column (the HLC/LWW timestamp). Tables not in the
/// set the host declares on the builder are local-only and never synced — that is
/// also the mechanism for keeping device-local state (per-device pin/cache
/// columns, local paths) out of sync: put it in a table you don't declare. An
/// empty set is rejected by [`super::cycle::init_sync`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncedTable {
    name: String,
    role: GateRole,
    blob: Option<BlobDecl>,
    /// Whether this table is an asset of its FK subject: it rides the subject's
    /// gate as an inherited child but is excluded from the subject's
    /// `gated_by_descendants` keep computation, so an asset row never keeps an
    /// otherwise-empty ancestor alive. Orthogonal to [`GateRole`] and the blob.
    asset: bool,
}

/// How a synced table relates to the gate. Orthogonal to whether it carries a
/// blob.
#[derive(Debug, Clone, PartialEq, Eq)]
enum GateRole {
    /// Every row syncs unconditionally.
    Plain,
    /// Every row syncs unconditionally, and blobs on the row or its descendants
    /// are Remote by construction.
    RemoteRoot,
    /// A gated root: a row syncs iff its boolean `gate_column` is true, and the
    /// gate flows down declared foreign keys to descendant rows.
    GatedRoot { gate_column: String },
    /// An always-shared ancestor kept alive by its gated subtree: a row syncs
    /// iff at least one foreign-key descendant table holds a surviving (kept)
    /// row referencing it. A *marker* only; the keep-children are inferred from
    /// the live foreign-key graph at gate-build time, never listed here.
    GatedByDescendants,
}

impl SyncedTable {
    /// An ungated synced table: every row syncs.
    pub fn new(name: impl Into<String>) -> Self {
        SyncedTable {
            name: name.into(),
            role: GateRole::Plain,
            blob: None,
            asset: false,
        }
    }

    /// Make this a gated root: rows sync iff the boolean `column` is true.
    pub fn gated_by(mut self, column: impl Into<String>) -> Self {
        self.role = GateRole::GatedRoot {
            gate_column: column.into(),
        };
        self
    }

    /// Make this a remote root: every row syncs, and blobs on the row or its
    /// foreign-key descendants are always Remote. There is no Local state for
    /// [`crate::blob::transition::make_remote`] or
    /// [`crate::blob::transition::make_local`] to transition.
    pub fn remote_root(mut self) -> Self {
        self.role = GateRole::RemoteRoot;
        self
    }

    /// Make this an always-shared ancestor kept alive by its gated subtree: a
    /// row syncs iff a surviving (kept) descendant row references it. The
    /// keep-children are inferred from the foreign-key graph at gate-build time,
    /// so there is nothing to pass here.
    pub fn gated_by_descendants(mut self) -> Self {
        self.role = GateRole::GatedByDescendants;
        self
    }

    /// Declare that rows of this table carry a blob, located by the columns in
    /// `decl`. coven derives the blob set itself from these columns + the live
    /// schema (see [`crate::blob::decl::BlobDecls`]); it never calls back to the
    /// host to discover blobs. Independent of the gate role.
    pub fn carries_blob(mut self, decl: BlobDecl) -> Self {
        self.blob = Some(decl);
        self
    }

    /// Mark this table an *asset* of its FK subject: a host-provided decoration
    /// (a cover, an artist image) that rides its subject's gate but never grants
    /// keep. The asset still inherits the gate as a child of its subject — it
    /// syncs exactly when the subject is kept — but the gate excludes it from the
    /// subject's `gated_by_descendants` keep computation, so an asset row alone
    /// never keeps an otherwise-empty ancestor alive (and the asset-rides-subject
    /// vs. subject-kept-by-children relation can never form a cycle). Independent
    /// of the gate role; declare it on an FK child of the subject.
    pub fn asset(mut self) -> Self {
        self.asset = true;
        self
    }

    /// The table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The gate column name, if this table is a gated root.
    pub fn gate_column(&self) -> Option<&str> {
        match &self.role {
            GateRole::GatedRoot { gate_column } => Some(gate_column),
            GateRole::Plain | GateRole::RemoteRoot | GateRole::GatedByDescendants => None,
        }
    }

    /// Whether this is a remote root: rows sync unconditionally, and blob
    /// locality for the row and descendants is always Remote.
    pub fn is_remote_root(&self) -> bool {
        matches!(self.role, GateRole::RemoteRoot)
    }

    /// Whether this is a gated-by-descendants ancestor (kept alive by its gated
    /// subtree rather than by a column of its own).
    pub fn is_gated_by_descendants(&self) -> bool {
        matches!(self.role, GateRole::GatedByDescendants)
    }

    /// This table's blob declaration, if it carries one.
    pub fn blob(&self) -> Option<&BlobDecl> {
        self.blob.as_ref()
    }

    /// Whether this table is an asset of its FK subject (rides the subject's gate
    /// but never grants keep). See [`SyncedTable::asset`].
    pub fn is_asset(&self) -> bool {
        self.asset
    }
}

/// Where a blob-bearing table's blob columns live, declared by the host so coven
/// can derive every blob a row references without a runtime callback. Resolved
/// against the live schema into a [`crate::blob::decl::BlobDecls`] each cycle.
///
/// A blob declares two orthogonal properties: [`provenance`](BlobDecl::provenance)
/// (its Local story) and [`fill`](BlobDecl::fill) (its Remote story).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDecl {
    /// The column holding the blob id. Defaults to the primary key (`id`, column
    /// 0), which is the blob id for most tables.
    pub id_column: String,
    /// Cloud namespace for the blob, e.g. `"images"` or `"audio"`.
    pub namespace: String,
    /// The column holding the consumer's readable cloud-relative path, used as the
    /// object key under the plain (browsable) blob-path scheme. `None` means the
    /// blob is keyed only by its hashed id (the default obfuscated scheme).
    pub cloud_path_column: Option<String>,
    /// How the blob is scoped for encryption (see [`BlobScopeSpec`]).
    pub scope: BlobScopeSpec,
    /// The blob's **Local story**: [`crate::blob::Provenance::UserProvided`] (the
    /// user's file at a path) or [`crate::blob::Provenance::HostProvided`] (coven's
    /// own copy in the local store).
    pub provenance: crate::blob::Provenance,
    /// The blob's **Remote story**: [`crate::blob::CacheFill::CacheEager`] (fetched
    /// into the cache on every pull) or [`crate::blob::CacheFill::CacheLazy`]
    /// (fetched into the cache on first read).
    pub fill: crate::blob::CacheFill,
}

/// How a blob's encryption scope is declared on a blob-bearing table. coven
/// resolves it to a [`crate::blob::BlobScope`] per row when it builds the blob's
/// reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobScopeSpec {
    /// The library master key — every member reads it.
    Master,
    /// A per-scope key derived from the master key, named by this fixed string.
    Derived(String),
    /// A coven-managed item key, named by the value of this column in the blob's
    /// row (resolves to [`crate::blob::BlobScope::Item`]).
    ItemColumn(String),
}

impl BlobDecl {
    /// A blob declaration in `namespace` with the given `provenance` (its Local
    /// story) and cache `fill` (its Remote story), the blob id taken from the
    /// primary key (`id`), no readable cloud path, master-scoped. Refine with the
    /// `with_*` builders.
    pub fn new(
        namespace: impl Into<String>,
        provenance: crate::blob::Provenance,
        fill: crate::blob::CacheFill,
    ) -> Self {
        BlobDecl {
            id_column: "id".to_string(),
            namespace: namespace.into(),
            cloud_path_column: None,
            scope: BlobScopeSpec::Master,
            provenance,
            fill,
        }
    }

    /// Take the blob id from `column` instead of the primary key.
    pub fn with_id_column(mut self, column: impl Into<String>) -> Self {
        self.id_column = column.into();
        self
    }

    /// Key the blob at the readable cloud path in `column` (the plain scheme).
    pub fn with_cloud_path_column(mut self, column: impl Into<String>) -> Self {
        self.cloud_path_column = Some(column.into());
        self
    }

    /// Scope the blob's encryption (defaults to [`BlobScopeSpec::Master`]).
    pub fn with_scope(mut self, scope: BlobScopeSpec) -> Self {
        self.scope = scope;
        self
    }
}

/// Quote an SQL identifier (table/column name), doubling any embedded quote, so
/// a trusted-but-unbindable name interpolates safely. Identifiers cannot be
/// passed as bound parameters; this is the safe interpolation path for them.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
