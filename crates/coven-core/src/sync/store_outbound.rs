//! Durable construction and ordered publication of local Store commits.

use std::future::Future;
use std::pin::Pin;

use super::circle_activation::VerifiedCircleActivations;
use super::membership::{MembershipChain, SerialAuthorizationState};
#[cfg(test)]
use super::membership_ops;
use super::storage::{
    BlobWriteAuthority, CoordinationError, CoordinationStorage, ExactObjectRef, ReplaceHeadError,
    StorageError, SyncStorage, VersionedObject,
};
use super::storage::{PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain};
use super::store_commit::{
    circle_package_semantic_prefix, commit_semantic_prefix, head_slot_prefix,
    package_semantic_prefix, serial_head_key, ActivatedStoreDeviceRegistrationRef,
    CandidateCleanupManifest, CandidateFamilyId, DeviceJoinAttemptRef, DeviceJoinOutcomeRef,
    ObjectHash, StoreBatchCommit, StoreBatchCommitDeletionTarget, StoreBatchCommitRef,
    StoreCommitCoord, StoreCommitOperationsInput, StoreCommitOrder, StoreControl, StoreDeviceHead,
    StoreDeviceHeadRef, StoreDeviceId, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreHistoryCut, StoreOperationMembershipAuthority, StoreProtocolError, StoreRootRef,
    StoreSerialHead, StoreSerialHeadState, StoreSerialPredecessor, SuccessorLink, SERIAL_STREAM_ID,
};
use super::store_objects::{run_blocking_object_verification, StoreObjectError};
use super::{
    audience_package, circle, circle_activation, circle_control, device_join, gate, invite,
    membership, owner_promotion, provider, remote_object, service, storage, store_commit,
    store_objects, store_pull, store_reclaim, wrapped_store_key,
};

pub(crate) const STORE_ROOT_AUTHORITY: &str = "store_root_authority";
pub(crate) const SERIAL_COORDINATION_HEAD: &str = "serial_coordination_head";
use crate::database::{
    Database, MergeCandidateAbandonmentPreparation, PreparedAudienceBlob, PreparedAudienceObjects,
    PreparedAudiencePackage, PreparedProtocolObject, SerialCandidateAbandonmentPreparation,
    StoreWriteBlobFact, StoreWriteBlobFacts, VerifiedMergeMaterialization,
};
use crate::keys::UserKeypair;
use crate::store_dir::StoreDir;

mod abandonment;
mod announcement;
mod audience_preparation;
mod local_authority;
mod operation_candidate;
mod operation_plan;
mod operation_publication;
mod prepared_operation;
mod publication_support;

pub use abandonment::*;
pub(crate) use announcement::*;
pub(crate) use audience_preparation::*;
pub(crate) use local_authority::*;
pub(crate) use operation_candidate::*;
pub(crate) use operation_plan::*;
pub(crate) use operation_publication::*;
pub(crate) use prepared_operation::*;
pub(crate) use publication_support::*;

pub(crate) struct PreparedPartitionPackage {
    pub(crate) audience: super::circle::Audience,
    pub(crate) control: Option<super::gate::CirclePartitionControl>,
    pub(crate) key_fingerprint: Option<crate::KeyFingerprint>,
    pub(crate) semantic_bytes: Vec<u8>,
    pub(crate) prepared: PreparedExactObject,
    pub(crate) blobs: Vec<PreparedPartitionBlob>,
}

pub(crate) struct PreparedPartitionBlob {
    pub(crate) audience: crate::blob::locator::RemoteAudience,
    pub(crate) stored: crate::blob::locator::StoredBlobRef,
    pub(crate) spool_path: Option<std::path::PathBuf>,
    pub(crate) uploaded_verified: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreOutboundError {
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
    #[error("outbound Store preparation failed: {0}")]
    Preparation(#[source] super::service::SyncCycleError),
    #[error("outbound blob {namespace}/{id} is local and cannot be published")]
    LocalUserBlob { namespace: String, id: String },
    #[error("outbound blob {namespace}/{id} is absent from storage")]
    MissingBlob { namespace: String, id: String },
    #[error("checking outbound blob {namespace}/{id}: {source}")]
    BlobStorage {
        namespace: String,
        id: String,
        source: super::storage::StorageError,
    },
    #[error("Serial coordination capability is required")]
    MissingSerialCoordination,
    #[error("Serial coordination: {0}")]
    Coordination(#[source] CoordinationError),
    #[error("Serial control branch is stale: expected {expected:?}, current {current:?}")]
    SerialControlConflict {
        expected: Box<StoreSerialPredecessor>,
        current: Box<StoreSerialPredecessor>,
    },
    #[error("candidate cleanup: {0}")]
    CandidateCleanup(#[from] super::store_pull::StorePullError),
    #[error("Store sequence {current} has no representable successor")]
    SequenceExhausted { current: u64 },
    #[error("Store author {device_id} was excluded before candidate activation")]
    AuthorExcluded { device_id: StoreDeviceId },
    #[error("Merge announcement selected {actual:?}, not candidate {expected:?}")]
    MergeAnnouncementOccupied {
        expected: Box<StoreBatchCommitRef>,
        actual: Box<StoreBatchCommitRef>,
    },
}

impl From<crate::database::DbError> for StoreOutboundError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

pub(crate) fn successor_store_sequence(current: u64) -> Result<u64, StoreOutboundError> {
    current
        .checked_add(1)
        .ok_or(StoreOutboundError::SequenceExhausted { current })
}

pub(crate) fn next_store_sequence(
    previous: Option<&StoreBatchCommitRef>,
) -> Result<u64, StoreOutboundError> {
    previous.map_or(Ok(1), |reference| {
        successor_store_sequence(reference.coord.sequence())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeCandidateAbandonment {
    NotRequired,
    Abandoned,
    CandidateActivated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SerialCandidateAbandonmentWinner {
    Authority {
        accepted: super::storage::VersionedObject,
    },
    OriginalBranch {
        accepted: super::storage::VersionedObject,
    },
    Other {
        current: StoreSerialPredecessor,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialBranchAbandonment {
    Discarded,
    OriginalBranchActivated,
}

#[cfg(test)]
#[path = "store_outbound/tests.rs"]
mod tests;
