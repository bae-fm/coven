//! Why connecting, running, or commanding sync failed: the composition
//! boundary's error vocabulary, wrapping the storage, key, and initialization
//! refusals below it.

use crate::database::DbError;
use crate::storage::cloud::setup::{SetupError, StorageSetupError};
use crate::storage::cloud::CloudHomeError;
use coven_keys::keys::KeyError;

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
    Store(#[from] crate::sync::store::StoreError),
    #[error("{0}")]
    Setup(#[from] SetupError),
    #[error("membership error: {0}")]
    Membership(Box<crate::sync::store::MembershipOpsError>),
    #[error("circle operation: {0}")]
    Circle(#[from] crate::sync::store::CircleOperationError),
    #[error("device join: {0}")]
    DeviceJoin(#[from] crate::DeviceJoinError),
    #[error("device join transport: {0}")]
    DeviceJoinTransport(#[from] crate::sync::store::DeviceJoinTransportError),
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

impl From<crate::database::DeviceJoinJournalError> for SyncError {
    fn from(error: crate::database::DeviceJoinJournalError) -> Self {
        SyncError::DeviceJoin(crate::DeviceJoinError::from(error))
    }
}
