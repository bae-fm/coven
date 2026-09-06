use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::{StoreBatchCommitRef, StoreDeviceId};

#[derive(Debug, thiserror::Error)]
pub enum BlobPreparationCleanupError {
    #[error("prepared blob spool is absent: {}", path.display())]
    MissingSpool { path: std::path::PathBuf },
    #[error("prepared blob file: {0}")]
    File(#[from] coven_foundation::atomic_file::FileError),
}

#[derive(Debug)]
pub struct BlobPreparationRollback {
    operation: Box<StoreError>,
    cleanup: Vec<BlobPreparationCleanupError>,
}

impl BlobPreparationRollback {
    pub(crate) fn new(operation: StoreError, cleanup: Vec<BlobPreparationCleanupError>) -> Self {
        Self {
            operation: Box::new(operation),
            cleanup,
        }
    }
}

impl std::fmt::Display for BlobPreparationRollback {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "blob preparation failed: {}", self.operation)?;
        for cleanup in &self.cleanup {
            write!(formatter, "; cleanup failed: {cleanup}")?;
        }
        Ok(())
    }
}

impl std::error::Error for BlobPreparationRollback {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.operation.as_ref())
    }
}

#[derive(Debug)]
pub enum StorePreparationError {
    Database(coven_database::DbError),
    Gate(String),
    AssetScan(String),
    AssetScanFile(coven_foundation::store_dir::LocalBlobStoreError),
    AssetUpload(String),
    Storage {
        operation: &'static str,
        source: coven_protocol::objects::StorageError,
    },
    LocalUserBlob {
        namespace: String,
        id: String,
    },
    MissingPreparedBlob {
        namespace: String,
        id: String,
    },
}

impl std::fmt::Display for StorePreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(f, "database error: {error}"),
            Self::Gate(error) => write!(f, "gate error: {error}"),
            Self::AssetScan(error) => write!(f, "asset scan error: {error}"),
            Self::AssetScanFile(error) => write!(f, "asset scan error: {error}"),
            Self::AssetUpload(error) => write!(f, "asset upload error: {error}"),
            Self::Storage { operation, source } => write!(f, "{operation}: {source}"),
            Self::LocalUserBlob { namespace, id } => {
                write!(
                    f,
                    "user-provided blob {namespace}/{id} still has a local external ref"
                )
            }
            Self::MissingPreparedBlob { namespace, id } => {
                write!(
                    f,
                    "blob {namespace}/{id} has no prepared exact publication object"
                )
            }
        }
    }
}

impl std::error::Error for StorePreparationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(source) => Some(source),
            Self::Storage { source, .. } => Some(source),
            Self::AssetScanFile(source) => Some(source),
            Self::Gate(_)
            | Self::AssetScan(_)
            | Self::AssetUpload(_)
            | Self::LocalUserBlob { .. }
            | Self::MissingPreparedBlob { .. } => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("local file: {0}")]
    File(#[from] coven_foundation::atomic_file::FileError),
    #[error("inspect host blob source {}: {source}", path.display())]
    InspectBlobSource {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("blob cache: {0}")]
    BlobCache(#[from] crate::sync::BlobCacheError),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("Store protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("Store JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Store changeset: {0}")]
    Changeset(#[from] coven_database::ChangesetError),
    #[error("Store writer authorization: {0}")]
    WriterAuthorization(#[source] Box<crate::sync::store::StoreWriterAuthorizationError>),
    #[error("Store sync cycle: {0}")]
    SyncCycle(#[source] Box<crate::sync::cycle::SyncCycleFailure>),
    #[error("Store membership chain: {0}")]
    AnchoredChain(#[source] Box<crate::sync::store::AnchoredChainError>),
    #[error("Store protocol root: {0}")]
    ProtocolRoot(#[source] Box<crate::sync::store::protocol_root::StoreProtocolRootError>),
    #[error("Store audience package: {0}")]
    AudiencePackage(#[from] coven_protocol::audience_package::AudiencePackageError),
    #[error("Store blob path: {0}")]
    BlobPath(#[from] coven_foundation::store_dir::PathTokenError),
    #[error("Store remote object: {0}")]
    RemoteObject(#[from] coven_protocol::remote_object::RemoteObjectRecordError),
    #[error("Store blob locator: {0}")]
    BlobLocator(#[from] coven_protocol::blob::locator::BlobLocatorError),
    #[error("Store prepared commit: {0}")]
    PreparedCommit(#[source] coven_protocol::prepared_commit::PreparedCommitError),
    #[error("Store membership preparation: {0}")]
    MembershipPreparation(
        #[source] coven_protocol::membership_mutation::MembershipPreparationError,
    ),
    #[error("Store keyring: {0}")]
    Keyring(#[source] Box<crate::sync::store::MembershipMutationError>),
    #[error("Store row routing key: {0}")]
    RowRoutingKey(#[from] coven_protocol::circle::RowRoutingKeyError),
    #[error("Store Circle package: {0}")]
    CirclePackage(#[source] Box<crate::sync::store::CirclePackageReadError>),
    #[error("Store protocol state {key:?} is absent")]
    MissingState { key: &'static str },
    #[error("Store protocol state {key:?} is invalid: {reason}")]
    InvalidState { key: &'static str, reason: String },
    #[error("outbound Store row is invalid: {0}")]
    InvalidOutbound(String),
    /// Another writer took the activation slot between this operation's
    /// preparation and its publication, so the candidate had to be re-prepared
    /// and **nothing was persisted**. Re-derive from durable state and run the
    /// operation again; it is the ordinary outcome of two writers racing, not a
    /// damaged store.
    #[error("another writer activated first; this Store operation persisted nothing")]
    ActivationConflict,
    #[error("outbound Store preparation failed: {0}")]
    Preparation(#[source] StorePreparationError),
    #[error("{0}")]
    BlobPreparationRollback(#[from] BlobPreparationRollback),
    #[error("blob preparation cleanup: {0}")]
    BlobPreparationCleanup(#[from] BlobPreparationCleanupError),
    #[error("outbound blob {namespace}/{id} is local and cannot be published")]
    LocalUserBlob { namespace: String, id: String },
    #[error("outbound blob {namespace}/{id} is absent from storage")]
    MissingBlob { namespace: String, id: String },
    #[error("checking outbound blob {namespace}/{id}: {source}")]
    BlobStorage {
        namespace: String,
        id: String,
        source: coven_protocol::objects::StorageError,
    },
    #[error("Store pull: {0}")]
    Pull(#[from] crate::sync::store::pull::StorePullError),
    #[error("Store sequence {current} has no representable successor")]
    SequenceExhausted { current: u64 },
    #[error("published Store write count has no representable successor")]
    PublishCountExhausted,
    /// Preparation failed AND recording that write's blocked status failed, so
    /// the write is not marked blocked. Carries both failures rather than
    /// reporting one and describing the other.
    #[error("write {write_id} was not marked blocked ({status}) after it failed to prepare ({preparation})")]
    WriteBlockNotRecorded {
        write_id: coven_protocol::write::WriteId,
        preparation: Box<StoreError>,
        status: coven_database::DbError,
    },
    #[error("Store author {device_id} was excluded before candidate activation")]
    AuthorExcluded { device_id: StoreDeviceId },
    #[error("Merge announcement selected {actual:?}, not candidate {expected:?}")]
    MergeAnnouncementOccupied {
        expected: Box<StoreBatchCommitRef>,
        actual: Box<StoreBatchCommitRef>,
    },
    #[error("{0}")]
    CirclePublicationBlocked(coven_protocol::circle::CirclePublicationBlocked),
}

impl StoreError {
    /// A retained prepared object that opens to different bytes is invalid
    /// outbound state, not a provider failure.
    pub(crate) fn prepared_object(error: coven_protocol::objects::StorageError) -> Self {
        match error {
            coven_protocol::objects::StorageError::PreparedObjectMismatch(key) => {
                Self::InvalidOutbound(format!(
                    "prepared exact object {key} differs from its signed bytes"
                ))
            }
            error => StoreObjectError::from(error).into(),
        }
    }

    pub(crate) fn write_block(&self) -> Option<coven_protocol::write::WriteBlock> {
        match self {
            Self::Database(_)
            | Self::File(_)
            | Self::InspectBlobSource { .. }
            | Self::BlobCache(_)
            | Self::BlobPreparationRollback(_)
            | Self::BlobPreparationCleanup(_)
            | Self::WriteBlockNotRecorded { .. }
            | Self::BlobStorage { .. }
            // Nothing was persisted and the caller re-runs the operation, so this
            // blocks no writer.
            | Self::ActivationConflict
            | Self::Pull(_)
            | Self::SyncCycle(_) => None,
            Self::MergeAnnouncementOccupied { .. }
            | Self::SequenceExhausted { .. }
            | Self::PublishCountExhausted
            | Self::AuthorExcluded { .. } => Some(coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: self.to_string(),
            }),
            Self::CirclePublicationBlocked(
                coven_protocol::circle::CirclePublicationBlocked::RotationRequired {
                    circle_id,
                    removed_members,
                },
            ) => Some(coven_protocol::write::WriteBlock::RotationRequired {
                circle_id: *circle_id,
                removed_members: removed_members.clone(),
            }),
            Self::Object(StoreObjectError::Storage(_)) => None,
            Self::MissingBlob { namespace, id } => Some(coven_protocol::write::WriteBlock::MissingBlob {
                namespace: namespace.clone(),
                id: id.clone(),
            }),
            Self::LocalUserBlob { namespace, id } => Some(coven_protocol::write::WriteBlock::LocalUserBlob {
                namespace: namespace.clone(),
                id: id.clone(),
            }),
            Self::MissingState { key } => Some(coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: format!("Store protocol state {key:?} is absent"),
            }),
            Self::InvalidState { key, reason } => Some(coven_protocol::write::WriteBlock::InvalidProtocolState {
                reason: format!("Store protocol state {key:?} is invalid: {reason}"),
            }),
            Self::InvalidOutbound(_)
            | Self::Object(_)
            | Self::Protocol(_)
            | Self::Json(_)
            | Self::Changeset(_)
            | Self::WriterAuthorization(_)
            | Self::AnchoredChain(_)
            | Self::ProtocolRoot(_)
            | Self::AudiencePackage(_)
            | Self::BlobPath(_)
            | Self::RemoteObject(_)
            | Self::BlobLocator(_)
            | Self::PreparedCommit(_)
            | Self::MembershipPreparation(_)
            | Self::Keyring(_)
            | Self::RowRoutingKey(_)
            | Self::CirclePackage(_) => {
                Some(coven_protocol::write::WriteBlock::InvalidPackage {
                    reason: self.to_string(),
                })
            }
            Self::Preparation(StorePreparationError::LocalUserBlob { namespace, id }) => {
                Some(coven_protocol::write::WriteBlock::LocalUserBlob {
                    namespace: namespace.clone(),
                    id: id.clone(),
                })
            }
            Self::Preparation(StorePreparationError::MissingPreparedBlob { namespace, id }) => {
                Some(coven_protocol::write::WriteBlock::MissingBlob {
                    namespace: namespace.clone(),
                    id: id.clone(),
                })
            }
            Self::Preparation(StorePreparationError::Gate(_))
            | Self::Preparation(StorePreparationError::AssetScan(_))
            | Self::Preparation(StorePreparationError::AssetScanFile(_))
            | Self::Preparation(StorePreparationError::Database(_)) => {
                Some(coven_protocol::write::WriteBlock::InvalidPackage {
                    reason: self.to_string(),
                })
            }
            Self::Preparation(StorePreparationError::AssetUpload(_))
            | Self::Preparation(StorePreparationError::Storage { .. }) => None,
        }
    }
}

impl From<crate::sync::store::CirclePackageReadError> for StoreError {
    fn from(error: crate::sync::store::CirclePackageReadError) -> Self {
        Self::CirclePackage(Box::new(error))
    }
}

impl From<coven_protocol::prepared_commit::PreparedCommitError> for StoreError {
    fn from(error: coven_protocol::prepared_commit::PreparedCommitError) -> Self {
        StoreError::PreparedCommit(error)
    }
}

impl From<coven_protocol::membership_mutation::MembershipPreparationError> for StoreError {
    fn from(error: coven_protocol::membership_mutation::MembershipPreparationError) -> Self {
        StoreError::MembershipPreparation(error)
    }
}

impl From<crate::sync::store::MembershipMutationError> for StoreError {
    fn from(error: crate::sync::store::MembershipMutationError) -> Self {
        Self::Keyring(Box::new(error))
    }
}

impl From<crate::sync::store::StoreWriterAuthorizationError> for StoreError {
    fn from(error: crate::sync::store::StoreWriterAuthorizationError) -> Self {
        Self::WriterAuthorization(Box::new(error))
    }
}

impl From<crate::sync::cycle::SyncCycleFailure> for StoreError {
    fn from(error: crate::sync::cycle::SyncCycleFailure) -> Self {
        Self::SyncCycle(Box::new(error))
    }
}

impl From<crate::sync::store::AnchoredChainError> for StoreError {
    fn from(error: crate::sync::store::AnchoredChainError) -> Self {
        Self::AnchoredChain(Box::new(error))
    }
}

impl From<crate::sync::store::protocol_root::StoreProtocolRootError> for StoreError {
    fn from(error: crate::sync::store::protocol_root::StoreProtocolRootError) -> Self {
        Self::ProtocolRoot(Box::new(error))
    }
}
