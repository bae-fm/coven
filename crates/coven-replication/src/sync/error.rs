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
    StorageSetup(StorageSetupError),
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("sync initialization error: {0}")]
    Init(#[from] InitSyncError),
    #[error("Store protocol state: {0}")]
    Protocol(String),
    #[error("Store operation: {0}")]
    Store(Box<crate::sync::store::StoreError>),
    #[error("{0}")]
    Setup(#[from] SetupError),
    #[error("membership error: {0}")]
    Membership(Box<crate::sync::store::MembershipOpsError>),
    #[error("circle operation: {0}")]
    Circle(Box<crate::sync::store::CircleOperationError>),
    #[error("device join: {0}")]
    DeviceJoin(Box<crate::sync::DeviceJoinError>),
    #[error("device join transport: {0}")]
    DeviceJoinTransport(Box<crate::sync::store::DeviceJoinTransportError>),
    #[error("invalid join request code: {0}")]
    InvalidJoinRequest(String),
    #[error("invalid Store membership operation code: {0}")]
    InvalidMembershipOperationCode(String),
    #[error("Store device exclusion: {0}")]
    DeviceExclusion(String),
    #[error("Store Owner promotion: {0}")]
    OwnerPromotion(String),
    #[error("{0}")]
    Database(#[from] DbError),
    #[error("row routing key: {0}")]
    RoutingEncryption(#[from] coven_keys::keys::RoutingEncryptionError),
    #[error("blob upload drain failed: {0}")]
    BlobUpload(DbError),
    #[error("sync loop error: {0}")]
    Loop(SyncLoopError),
}

impl From<crate::sync::store::MembershipOpsError> for SyncError {
    fn from(error: crate::sync::store::MembershipOpsError) -> Self {
        Self::Membership(Box::new(error))
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

impl From<coven_database::DeviceJoinJournalError> for SyncError {
    fn from(error: coven_database::DeviceJoinJournalError) -> Self {
        crate::sync::DeviceJoinError::from(error).into()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn sync_error_fits_below_clippys_large_result_threshold() {
        let size = std::mem::size_of::<super::SyncError>();
        assert!(size <= 128, "SyncError occupies {size} bytes");
    }
}
