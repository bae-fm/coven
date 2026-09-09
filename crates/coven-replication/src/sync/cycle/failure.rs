#[derive(Debug)]
pub struct SyncCycleFailure {
    kind: SyncCycleFailureKind,
    operation: &'static str,
    cause: Box<SyncCycleCause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncCycleFailureKind {
    Offline,
    Failed,
}

impl SyncCycleFailure {
    pub(crate) fn operation<E>(operation: &'static str, error: E) -> Self
    where
        E: Into<SyncCycleCause>,
    {
        let cause = error.into();
        let kind = if crate::sync::error::error_chain_contains_transport(&cause) {
            SyncCycleFailureKind::Offline
        } else {
            SyncCycleFailureKind::Failed
        };
        Self {
            kind,
            operation,
            cause: Box::new(cause),
        }
    }

    pub(crate) fn is_offline(&self) -> bool {
        self.kind == SyncCycleFailureKind::Offline
    }

    pub(super) fn concurrent(first: Self, second: Self) -> Self {
        let kind = if first.is_offline() || second.is_offline() {
            SyncCycleFailureKind::Offline
        } else {
            SyncCycleFailureKind::Failed
        };
        Self {
            kind,
            operation: "run Store publication and blob upload lanes",
            cause: Box::new(SyncCycleCause::Concurrent {
                first: Box::new(first),
                second: Box::new(second),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.to_string().contains(pattern)
    }
}

impl std::fmt::Display for SyncCycleFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.operation, self.cause)
    }
}

impl std::error::Error for SyncCycleFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SyncCycleCause {
    #[error("{first}; concurrently, {second}")]
    Concurrent {
        first: Box<SyncCycleFailure>,
        second: Box<SyncCycleFailure>,
    },
    #[error("{0}")]
    Database(#[from] coven_database::DbError),
    #[error("{0}")]
    Store(#[from] crate::sync::store::StoreError),
    #[error("{0}")]
    Registration(#[from] crate::sync::store::StoreRegistrationError),
    #[error("{0}")]
    Initialization(#[from] crate::sync::store::StoreInitializationError),
    #[error("{0}")]
    Circle(#[from] crate::sync::store::CircleOperationError),
    #[error("{0}")]
    DeviceExclusion(#[from] crate::sync::store::StoreDeviceExclusionError),
    #[error("{0}")]
    Reclaim(#[from] crate::sync::store::StoreReclaimError),
    #[error("{0}")]
    DeviceJoin(#[from] crate::sync::store::DeviceJoinError),
    #[error("{0}")]
    TombstoneDrain(#[from] crate::blob::delete::TombstoneDrainError),
    #[error("{0}")]
    TombstoneGc(#[from] crate::sync::store::commit_publication::operation::TombstoneGcError),
    #[error("{0}")]
    UploadFailures(#[from] crate::blob::UploadFailures),
    #[error("{0}")]
    WriterAuthorization(
        #[from] crate::sync::store::commit_publication::operation::StoreWriterAuthorizationError,
    ),
    #[error("{0}")]
    Acknowledgement(#[from] crate::sync::store::acknowledgements::StoreAckError),
    #[error("{0}")]
    Membership(#[from] crate::sync::store::MembershipOpsError),
    #[error("{0}")]
    Pull(#[from] crate::sync::store::StorePullError),
    #[error("{0}")]
    AuthorizationRefresh(
        #[from] crate::sync::store::commit_publication::operation::AuthorizationRefreshError,
    ),
    #[error("{0}")]
    PublishedBlobDrop(#[from] crate::sync::store::blob::PublishedBlobDropError),
    #[error("{0}")]
    StoreProtocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("{0}")]
    RowRoutingKey(#[from] coven_protocol::circle::RowRoutingKeyError),
    #[error("{0}")]
    Snapshot(#[from] crate::sync::store::snapshots::SnapshotError),
}

#[cfg(test)]
#[path = "failure_tests.rs"]
mod tests;
