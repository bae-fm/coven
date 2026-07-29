use crate::sync::store_commit::{StoreBatchCommitRef, StoreDeviceId};
use crate::sync::store_objects::StoreObjectError;

#[derive(Debug)]
pub enum StorePreparationError {
    Database(crate::database::DbError),
    Gate(String),
    AssetScan(String),
    AssetUpload(String),
    Storage {
        operation: &'static str,
        source: crate::sync::storage::StorageError,
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
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
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
        source: crate::sync::storage::StorageError,
    },
    #[error("candidate cleanup: {0}")]
    CandidateCleanup(#[from] crate::sync::store::owner::pull::StorePullError),
    #[error("Store sequence {current} has no representable successor")]
    SequenceExhausted { current: u64 },
    #[error("Store author {device_id} was excluded before candidate activation")]
    AuthorExcluded { device_id: StoreDeviceId },
    #[error("Merge announcement selected {actual:?}, not candidate {expected:?}")]
    MergeAnnouncementOccupied {
        expected: Box<StoreBatchCommitRef>,
        actual: Box<StoreBatchCommitRef>,
    },
    #[error("{0}")]
    CirclePublicationBlocked(crate::sync::circle::CirclePublicationBlocked),
}

impl From<crate::database::DbError> for StoreError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}
