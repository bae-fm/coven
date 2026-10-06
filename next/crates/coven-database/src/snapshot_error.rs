//! Snapshot input errors leave the loading transaction unchanged.

/// A plaintext snapshot could not be read or cannot describe this database.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// Reading the plaintext supplied by sync failed.
    #[error("reading snapshot: {0}")]
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
    /// Replaying a waiting write would precede causal history absent from this audience.
    #[error("snapshot needs prior writes {missing:?} before replaying waiting changes")]
    Writes {
        /// Causal positions not covered by the snapshot or earlier replayed writes.
        missing: Vec<coven_merge::WriteId>,
    },
    /// Store-log entries are not carried by snapshots and must be applied first.
    #[error("snapshot needs store-log entries {missing:?}")]
    StoreLog {
        /// The snapshot's store-log positions not yet present locally.
        missing: Vec<coven_format::value::EntryId>,
    },
    /// Individually valid records disagree with one another or the schema.
    #[error("inconsistent snapshot: {0}")]
    Inconsistent(&'static str),
}

pub(crate) fn invalid(reason: &'static str) -> crate::DbError {
    SnapshotError::Inconsistent(reason).into()
}
