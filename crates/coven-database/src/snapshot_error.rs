//! Snapshot input errors leave the loading transaction unchanged.

/// Snapshots or their gap writes could not be loaded into this database.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// A selected snapshot's plaintext failed while the reload transaction was
    /// reading it: the loader's format, schema and merge checks, or reading
    /// the input itself.
    #[error("reload snapshot {snapshot:?}: {error}")]
    Body {
        /// The supplied snapshot's identity.
        snapshot: coven_format::admin::SnapshotId,
        /// The failed read or check.
        #[source]
        error: Box<crate::DbError>,
    },
    /// Reading the plaintext supplied by sync failed.
    #[error("reading reload input: {0}")]
    Read(#[source] std::io::Error),
    /// A frame or its merge state failed the format's checks.
    #[error(transparent)]
    Format(#[from] coven_format::Error),
    /// The snapshot needs a different application schema.
    #[error("snapshot schema {snapshot} cannot load into database schema {database}")]
    Schema {
        /// Version declared by the snapshot.
        snapshot: u32,
        /// Version prepared on the database connection.
        database: u32,
    },
    /// Authenticating another member's write that the snapshot carries needs
    /// a key this device has not opened yet (§9.6, D9): loading waits for it.
    #[error("snapshot write evidence needs key {key} of {audience:?}")]
    KeyUnavailable {
        /// The audience whose key seals the evidence.
        audience: coven_merge::Audience,
        /// The key the write named.
        key: coven_foundation::id_source::KeyId,
    },
    /// Individually valid records disagree with one another or the schema.
    #[error("inconsistent snapshot: {0}")]
    Inconsistent(&'static str),
}

pub(crate) fn invalid(reason: &'static str) -> crate::DbError {
    SnapshotError::Inconsistent(reason).into()
}
