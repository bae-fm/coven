//! Why connecting, running, or commanding sync failed: the composition
//! boundary's error vocabulary, wrapping the storage, key, and initialization
//! refusals below it.

use coven_database::DbError;
use coven_keys::keys::KeyError;
use coven_storage::cloud::setup::{SetupError, StorageSetupError};
use coven_storage::cloud::CloudHomeError;

use super::cycle::InitSyncError;
use super::sync_loop::SyncLoopError;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("sync is not configured")]
    NotConfigured,
    #[error("sync loop is not running")]
    LoopNotRunning,
    #[error("sharing requires an encrypted cloud home")]
    NotEncryptedHome,
    #[error("no master key is established for this opaque store (locked, or never initialized)")]
    MasterKeyNotEstablished,
    #[error("failed to build cloud home: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("failed to create sync storage: {0}")]
    StorageSetup(#[source] StorageSetupError),
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("sync initialization error: {0}")]
    Init(#[source] Box<InitSyncError>),
    #[error("Store operation: {0}")]
    Store(#[source] Box<crate::sync::store::StoreError>),
    #[error("{0}")]
    Setup(#[from] SetupError),
    #[error("membership error: {0}")]
    Membership(#[source] Box<crate::sync::store::MembershipOpsError>),
    #[error("circle operation: {0}")]
    Circle(#[source] Box<crate::sync::store::CircleOperationError>),
    #[error("device join: {0}")]
    DeviceJoin(#[source] Box<crate::sync::DeviceJoinError>),
    #[error("device join transport: {0}")]
    DeviceJoinTransport(#[source] Box<crate::sync::store::DeviceJoinTransportError>),
    #[error("invalid join request code: {0}")]
    InvalidJoinRequest(#[source] coven_foundation::code_envelope::EnvelopeError),
    #[error("invalid Store membership operation code: {0}")]
    InvalidMembershipOperationCode(#[source] coven_foundation::code_envelope::EnvelopeError),
    #[error("Store device exclusion: {0}")]
    DeviceExclusion(#[source] Box<crate::sync::store::StoreDeviceExclusionError>),
    #[error("Store Owner promotion: {0}")]
    OwnerPromotion(#[source] Box<crate::sync::store::OwnerPromotionError>),
    #[error("{0}")]
    Database(#[source] Box<DbError>),
    #[error("row routing key: {0}")]
    RoutingEncryption(#[from] coven_keys::keys::RoutingEncryptionError),
    #[error("blob upload drain failed: {0}")]
    BlobUpload(#[source] Box<crate::sync::store::StoreError>),
    #[error("sync loop error: {0}")]
    Loop(#[source] SyncLoopError),
}

impl SyncError {
    /// Whether retrying the same operation may succeed because its error chain
    /// contains a transient cloud transport or I/O failure.
    pub fn is_retryable(&self) -> bool {
        error_chain_contains_transport(self)
    }
}

pub(crate) fn error_chain_contains_transport(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<coven_protocol::objects::StorageError>()
            .is_some_and(coven_protocol::objects::StorageError::is_transport)
            || source
                .downcast_ref::<CloudHomeError>()
                .is_some_and(|error| {
                    matches!(
                        error,
                        CloudHomeError::Transport(_)
                            | CloudHomeError::TransportSource { .. }
                            | CloudHomeError::Io(_)
                    )
                })
        {
            return true;
        }
        current = source.source();
    }
    false
}

impl From<crate::sync::store::MembershipOpsError> for SyncError {
    fn from(error: crate::sync::store::MembershipOpsError) -> Self {
        Self::Membership(Box::new(error))
    }
}

impl From<InitSyncError> for SyncError {
    fn from(error: InitSyncError) -> Self {
        Self::Init(Box::new(error))
    }
}

impl From<crate::sync::store::StoreError> for SyncError {
    fn from(error: crate::sync::store::StoreError) -> Self {
        Self::Store(Box::new(error))
    }
}

impl From<crate::sync::store::CircleOperationError> for SyncError {
    fn from(error: crate::sync::store::CircleOperationError) -> Self {
        Self::Circle(Box::new(error))
    }
}

impl From<crate::sync::DeviceJoinError> for SyncError {
    fn from(error: crate::sync::DeviceJoinError) -> Self {
        Self::DeviceJoin(Box::new(error))
    }
}

impl From<crate::sync::store::DeviceJoinTransportError> for SyncError {
    fn from(error: crate::sync::store::DeviceJoinTransportError) -> Self {
        Self::DeviceJoinTransport(Box::new(error))
    }
}

impl From<crate::sync::store::StoreDeviceExclusionError> for SyncError {
    fn from(error: crate::sync::store::StoreDeviceExclusionError) -> Self {
        Self::DeviceExclusion(Box::new(error))
    }
}

impl From<crate::sync::store::OwnerPromotionError> for SyncError {
    fn from(error: crate::sync::store::OwnerPromotionError) -> Self {
        Self::OwnerPromotion(Box::new(error))
    }
}

impl From<DbError> for SyncError {
    fn from(error: DbError) -> Self {
        Self::Database(Box::new(error))
    }
}

impl From<coven_database::DeviceJoinJournalError> for SyncError {
    fn from(error: coven_database::DeviceJoinJournalError) -> Self {
        crate::sync::DeviceJoinError::from(error).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_error_fits_below_clippys_large_result_threshold() {
        let size = std::mem::size_of::<super::SyncError>();
        assert!(size <= 128, "SyncError occupies {size} bytes");
    }

    #[test]
    fn nested_store_initialization_transport_is_retryable() {
        let storage = coven_protocol::objects::StorageError::from(CloudHomeError::Transport(
            "provider unavailable".to_string(),
        ));
        let probe = coven_protocol::provider::ProviderProbeError::Storage(storage);
        let root = crate::sync::store::protocol_root::StoreProtocolRootError::ProviderProbe(probe);
        let initialization = crate::sync::store::StoreInitializationError::ProtocolRoot(root);
        let error = SyncError::from(InitSyncError::Initialization(initialization));

        assert!(error.is_retryable());
    }
}
