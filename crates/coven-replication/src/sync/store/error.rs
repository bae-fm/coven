use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::{StoreBatchCommitRef, StoreDeviceId};

#[derive(Debug)]
pub enum StorePreparationError {
    Database(coven_database::DbError),
    Gate(String),
    AssetScan(String),
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
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("Store protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
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
    #[error("candidate cleanup: {0}")]
    CandidateCleanup(#[from] crate::sync::store::owner::pull::StorePullError),
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
    /// Classify an exact create-and-readback failure: an object that opened to
    /// bytes other than the ones sealed into it is an invalid outbound object,
    /// not a storage failure.
    pub(crate) fn readback(error: coven_protocol::objects::StorageError) -> Self {
        match error {
            coven_protocol::objects::StorageError::ReadbackMismatch(key) => Self::InvalidOutbound(
                format!("exact readback of {key} differs from its signed bytes"),
            ),
            error => StoreObjectError::from(error).into(),
        }
    }

    pub(crate) fn write_block(&self) -> Option<coven_protocol::write::WriteBlock> {
        match self {
            Self::Database(_)
            | Self::WriteBlockNotRecorded { .. }
            | Self::BlobStorage { .. }
            // Nothing was persisted and the caller re-runs the operation, so this
            // blocks no writer.
            | Self::ActivationConflict
            | Self::CandidateCleanup(_) => None,
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
            Self::InvalidOutbound(_) | Self::Object(_) | Self::Protocol(_) => {
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

impl From<coven_protocol::prepared_commit::PreparedCommitError> for StoreError {
    fn from(error: coven_protocol::prepared_commit::PreparedCommitError) -> Self {
        StoreError::InvalidOutbound(error.to_string())
    }
}

impl From<coven_protocol::membership_mutation::MembershipPreparationError> for StoreError {
    fn from(error: coven_protocol::membership_mutation::MembershipPreparationError) -> Self {
        StoreError::InvalidOutbound(error.to_string())
    }
}
