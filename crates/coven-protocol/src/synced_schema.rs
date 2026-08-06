//! Synced-table declarations and the shared identifier-quoting helper.
//!
//! [`SyncedTable`] is how a host declares which tables participate in changeset
//! sync and what `(table, id)` means for each one. The set is no longer a
//! process-global: the host passes it to
//! `CovenBuilder::synced_tables`, and coven owns it for the lifetime of
//! the connection and hands it to each journaled write's capture session, the
//! gate, and apply.

/// How `(table, id)` names one logical row across every device.
///
/// Equality always means one row, including equality between two valid UUIDs.
/// The mode controls which ids may be introduced; it does not change merge
/// equality. Changing a primary key removes the old identity and introduces the
/// new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowIdentity {
    /// Rows created independently use canonical lowercase hyphenated RFC UUID
    /// version 4 or 7 ids.
    IndependentUuid,
    /// Application-assigned keys intentionally name the same logical row on
    /// every device.
    SharedKey,
}

impl RowIdentity {
    pub fn validate(self, table: &str, value: &str) -> Result<(), RowIdentityError> {
        if self == Self::SharedKey {
            return Ok(());
        }

        let valid = uuid::Uuid::parse_str(value).is_ok_and(|parsed| {
            parsed.get_variant() == uuid::Variant::RFC4122
                && matches!(
                    parsed.get_version(),
                    Some(uuid::Version::Random | uuid::Version::SortRand)
                )
                && parsed.hyphenated().to_string() == value
        });
        if valid {
            Ok(())
        } else {
            Err(RowIdentityError::InvalidIndependentUuid {
                table: table.to_string(),
                value: value.to_string(),
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RowIdentityError {
    #[error(
        "synced table {table:?} id {value:?} is invalid for IndependentUuid; expected a canonical lowercase hyphenated RFC UUID version 4 or 7"
    )]
    InvalidIndependentUuid { table: String, value: String },
    #[error("synced table {table:?} changeset has no {side} primary-key value")]
    MissingPrimaryKey { table: String, side: &'static str },
    #[error("synced table {table:?} changeset has a non-TEXT {side} primary-key value")]
    NonTextPrimaryKey { table: String, side: &'static str },
    #[error("synced table {table:?} changeset primary key is not UTF-8: {reason}")]
    NonUtf8PrimaryKey { table: String, reason: String },
}

impl RowIdentityError {
    pub fn table(&self) -> &str {
        match self {
            Self::InvalidIndependentUuid { table, .. }
            | Self::MissingPrimaryKey { table, .. }
            | Self::NonTextPrimaryKey { table, .. }
            | Self::NonUtf8PrimaryKey { table, .. } => table,
        }
    }
}

/// A table that participates in changeset sync, declared at startup by the host
/// and passed to `CovenBuilder::synced_tables`.
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
/// ancestor, or plain — never two of these. See the database's `Gates` for the gating
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
/// empty set is rejected when sync starts.
///
/// The required [`RowIdentity`] defines which ids may name rows. Use
/// [`RowIdentity::IndependentUuid`] for independently created rows and
/// [`RowIdentity::SharedKey`] only when equal application keys intentionally
/// name and merge as the same row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncedTable {
    name: String,
    row_identity: RowIdentity,
    role: GateRole,
    audience_parent_column: Option<String>,
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
pub enum GateRole {
    /// Every row syncs unconditionally.
    Plain,
    /// Every row syncs unconditionally, and blobs on the row or its descendants
    /// are Remote by construction.
    RemoteRoot,
    /// A gated root: a row syncs iff its boolean `gate_column` is true, and the
    /// gate flows down declared foreign keys to descendant rows.
    GatedRoot { gate_column: String },
    /// An audience root: NULL is Store, `local` is device-local, and every
    /// other value is a canonical committed circle id.
    ScopedRoot { audience_column: String },
    /// An always-shared ancestor kept alive by its gated subtree: a row syncs
    /// iff at least one foreign-key descendant table holds a surviving (kept)
    /// row referencing it. A *marker* only; the keep-children are inferred from
    /// the live foreign-key graph at gate-build time, never listed here.
    GatedByDescendants,
}

impl SyncedTable {
    /// An ungated synced table: every row syncs under the required identity
    /// mode.
    pub fn new(name: impl Into<String>, row_identity: RowIdentity) -> Self {
        SyncedTable {
            name: name.into(),
            row_identity,
            role: GateRole::Plain,
            audience_parent_column: None,
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

    /// Make this an audience root whose TEXT column selects Store, Local, or
    /// one committed circle for the row and its foreign-key descendants. A
    /// store with an audience root requires `HomeStorage::Opaque`.
    pub fn scoped_by(mut self, column: impl Into<String>) -> Self {
        self.role = GateRole::ScopedRoot {
            audience_column: column.into(),
        };
        self
    }

    /// Make this plain table inherit its audience through the foreign key whose
    /// child column is `column`. Every descendant of an audience root must
    /// select this relationship explicitly; coven never guesses among the
    /// table's foreign keys.
    pub fn inherits_audience_through(mut self, column: impl Into<String>) -> Self {
        self.audience_parent_column = Some(column.into());
        self
    }

    /// Make this a remote root: every row syncs, and blobs on the row or its
    /// foreign-key descendants are always Remote. There is no Local state for
    /// `CovenHandle::make_remote` or `CovenHandle::make_local`
    /// to transition.
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
    /// schema (see the database's `BlobDecls`); it never calls back to the
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

    /// How this table's ids name logical rows across devices.
    pub fn row_identity(&self) -> RowIdentity {
        self.row_identity
    }

    /// The gate column name, if this table is a gated root.
    pub fn gate_column(&self) -> Option<&str> {
        match &self.role {
            GateRole::GatedRoot { gate_column } => Some(gate_column),
            GateRole::Plain
            | GateRole::RemoteRoot
            | GateRole::ScopedRoot { .. }
            | GateRole::GatedByDescendants => None,
        }
    }

    /// The audience column name, if this table is a scoped root.
    pub fn audience_column(&self) -> Option<&str> {
        match &self.role {
            GateRole::ScopedRoot { audience_column } => Some(audience_column),
            GateRole::Plain
            | GateRole::RemoteRoot
            | GateRole::GatedRoot { .. }
            | GateRole::GatedByDescendants => None,
        }
    }

    /// The complete sync role included in the signed routing contract.
    pub fn gate_role(&self) -> &GateRole {
        &self.role
    }

    /// The child foreign-key column that selects this table's audience parent.
    pub fn audience_parent_column(&self) -> Option<&str> {
        self.audience_parent_column.as_deref()
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
/// against the live schema into the database's `BlobDecls` each cycle.
///
/// A blob declares two orthogonal properties: [`provenance`](BlobDecl::provenance)
/// (its Local story) and [`fill`](BlobDecl::fill) (its Remote story).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDecl {
    /// The column holding the blob id. Defaults to the primary key (`id`, column
    /// 0), which is the blob id for most tables.
    pub id_column: String,
    /// The column holding the blob's plaintext length in bytes.
    pub size_column: String,
    /// The column holding the blob's content hash — the lowercase-hex SHA-256 of
    /// its plaintext, computed at import (see [`crate::blob::content_hash`]). The
    /// row carries it in a signed changeset, so it is signed by the row's author;
    /// on download coven hashes the decrypted plaintext and requires equality with
    /// this value, so the bytes are pinned by the author, not by where they were
    /// found. Defaults to `hash`.
    pub hash_column: String,
    /// Cloud namespace for the blob, e.g. `"images"` or `"audio"`.
    pub namespace: String,
    /// The column holding the consumer's readable cloud-relative path, used as the
    /// object key under the plain (browsable) blob-path scheme. `None` means the
    /// blob is keyed only by its hashed id (the default obfuscated scheme).
    pub cloud_path_column: Option<String>,
    /// How the blob is scoped for encryption (see [`crate::blob::BlobScope`]).
    pub scope: crate::blob::BlobScope,
    /// The blob's **Local story**: [`crate::blob::Provenance::UserProvided`] (the
    /// user's file at a path) or [`crate::blob::Provenance::HostProvided`] (coven's
    /// own copy in the local store).
    pub provenance: crate::blob::Provenance,
    /// The blob's **Remote story**: [`crate::blob::CacheFill::CacheEager`] (fetched
    /// into the cache on every pull) or [`crate::blob::CacheFill::CacheLazy`]
    /// (fetched into the cache on first read).
    pub fill: crate::blob::CacheFill,
    /// The blob's **replacement story**: whether this row may be repointed at a
    /// different blob ([`crate::blob::BlobReplacement`]). Decides what coven requires of
    /// the blob's cloud key so that a cloud object is never rewritten with different
    /// bytes. Defaults to [`crate::blob::BlobReplacement::Replaceable`].
    pub replacement: crate::blob::BlobReplacement,
}

impl BlobDecl {
    /// A blob declaration in `namespace` with the given `provenance` (its Local
    /// story) and cache `fill` (its Remote story), the blob id taken from the
    /// primary key (`id`), no readable cloud path, master-scoped, and
    /// [`Replaceable`](crate::blob::BlobReplacement::Replaceable). Refine with the
    /// `with_*` builders.
    pub fn new(
        namespace: impl Into<String>,
        provenance: crate::blob::Provenance,
        fill: crate::blob::CacheFill,
    ) -> Self {
        BlobDecl {
            id_column: "id".to_string(),
            size_column: "size".to_string(),
            hash_column: "hash".to_string(),
            namespace: namespace.into(),
            cloud_path_column: None,
            scope: crate::blob::BlobScope::Master,
            provenance,
            fill,
            replacement: crate::blob::BlobReplacement::Replaceable,
        }
    }

    /// Declare that this table's row is never repointed at a different blob
    /// ([`crate::blob::BlobReplacement::WriteOnce`]), which frees its readable
    /// `cloud_path` to be a stable, fully human-readable name. coven refuses a
    /// repointing. Read that variant's docs before reaching for this: it is a weaker
    /// contract than the default, and it asks the consumer to guarantee the part coven
    /// cannot see — that a path is never reused by a different blob.
    pub fn write_once(mut self) -> Self {
        self.replacement = crate::blob::BlobReplacement::WriteOnce;
        self
    }

    /// Take the blob id from `column` instead of the primary key.
    pub fn with_id_column(mut self, column: impl Into<String>) -> Self {
        self.id_column = column.into();
        self
    }

    /// Take the plaintext byte length from `column` instead of `size`.
    pub fn with_size_column(mut self, column: impl Into<String>) -> Self {
        self.size_column = column.into();
        self
    }

    /// Take the content hash from `column` instead of `hash`.
    pub fn with_hash_column(mut self, column: impl Into<String>) -> Self {
        self.hash_column = column.into();
        self
    }

    /// Key the blob at the readable cloud path in `column` (the plain scheme).
    pub fn with_cloud_path_column(mut self, column: impl Into<String>) -> Self {
        self.cloud_path_column = Some(column.into());
        self
    }

    /// Scope the blob's encryption (defaults to [`crate::blob::BlobScope::Master`]).
    pub fn with_scope(mut self, scope: crate::blob::BlobScope) -> Self {
        self.scope = scope;
        self
    }
}

#[cfg(test)]
mod row_identity_tests {
    use super::*;

    #[test]
    fn independent_uuid_accepts_only_canonical_rfc_uuid_v4_or_v7() {
        for valid in [
            "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "01890a5d-ac96-774b-bcce-b302099c3f74",
        ] {
            RowIdentity::IndependentUuid
                .validate("things", valid)
                .unwrap_or_else(|error| panic!("{valid} must be accepted: {error}"));
        }

        for invalid in [
            "F47AC10B-58CC-4372-A567-0E02B2C3D479",
            "f47ac10b58cc4372a5670e02b2c3d479",
            "{f47ac10b-58cc-4372-a567-0e02b2c3d479}",
            "urn:uuid:f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "00000000-0000-0000-0000-000000000000",
            "f47ac10b-58cc-1372-a567-0e02b2c3d479",
            "f47ac10b-58cc-2372-a567-0e02b2c3d479",
            "f47ac10b-58cc-3372-a567-0e02b2c3d479",
            "f47ac10b-58cc-5372-a567-0e02b2c3d479",
            "f47ac10b-58cc-6372-a567-0e02b2c3d479",
            "f47ac10b-58cc-8372-a567-0e02b2c3d479",
            "f47ac10b-58cc-4372-0567-0e02b2c3d479",
            "not-a-uuid",
        ] {
            assert!(
                RowIdentity::IndependentUuid
                    .validate("things", invalid)
                    .is_err(),
                "{invalid} must be rejected",
            );
        }

        RowIdentity::SharedKey
            .validate("settings", "preferences")
            .expect("shared keys accept application-assigned ids");
    }
}
