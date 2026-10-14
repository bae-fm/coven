//! Snapshot input errors leave the loading transaction unchanged.

/// Snapshots or their gap writes could not be loaded into this database.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// A downloaded gap write failed while the reload transaction was applying it.
    #[error("reload write {write:?}: {error}")]
    Write {
        /// The supplied object's position, distinct from local queued writes.
        write: coven_merge::WriteId,
        /// The failed read, format or merge check.
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
    /// Store-log entries are not carried by snapshots and must be applied first.
    #[error("snapshot needs store-log entries {missing:?}")]
    StoreLog {
        /// The snapshot's store-log positions not yet present locally.
        missing: Vec<coven_format::value::EntryId>,
    },
    /// Required gap writes or causal predecessors were not supplied.
    #[error("reload is missing writes {missing:?}")]
    MissingWrites {
        /// Positions not reached by the supplied snapshots and write streams.
        missing: Vec<coven_merge::WriteId>,
    },
    /// A supplied write cannot yet apply under the normal download checks.
    #[error("reload write cannot apply: {0:?}")]
    WriteWaiting(crate::WriteWait),
    /// The snapshot judged a covered write's own retirement window otherwise
    /// than this device's replay does (§10). Its contributions cannot be
    /// rebuilt here, so nothing loads; a snapshot made under the same verdict can.
    #[error("snapshot judged write {write:?}'s retirement window otherwise")]
    RetirementVerdict {
        /// The covered write this device judged itself.
        write: coven_merge::WriteId,
    },
    /// A sealed snapshot does not reproduce the agreement state it was written
    /// from (§15): publication stops and the writer reports it.
    #[error("sealed snapshot does not reproduce its captured state")]
    CapturedState,
    /// Individually valid records disagree with one another or the schema.
    #[error("inconsistent snapshot: {0}")]
    Inconsistent(&'static str),
}

pub(crate) fn invalid(reason: &'static str) -> crate::DbError {
    SnapshotError::Inconsistent(reason).into()
}
