use std::sync::Arc;

use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

#[cfg(test)]
use super::cloud_storage::CloudSyncStorage;
use super::cloud_storage::{BlobPathScheme, CloudCipherAccess};
use super::cycle::SyncCycleFailure;
use super::storage::SyncStorage;
use super::store_commit::{CommitFrontier, StoreProtocolRoot};

#[doc(hidden)]
pub mod blob;
mod circle_controls;
mod database;
#[cfg(test)]
pub(super) use database::record_verified_circle_activations_for_test;
#[doc(hidden)]
pub use database::StoreDatabase;
mod device_join_transport;
mod error;
mod membership;
mod owner;
use owner::operations;
mod package_preparation;
#[cfg(not(any(test, feature = "test-utils")))]
mod protocol_root;
#[cfg(any(test, feature = "test-utils"))]
pub(super) mod protocol_root;
mod retained_replay;
#[cfg(test)]
use owner::reclaim;
use owner::snapshot;

pub use crate::blob::cache::BlobDownloadFailureCause;
pub use circle_controls::CircleOperationError;
pub(crate) use circle_controls::{
    CircleCurrentState, CircleOperationJournal, VerifiedCircleImage, VerifiedStreamActivations,
};
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use circle_controls::{
    VerifiedCircleAccess, VerifiedCircleActive, VerifiedCircleReference,
};
#[cfg(test)]
pub(crate) use database::candidate_records::select_author_exclusion_activation_locator;
pub(crate) use database::candidate_records::CandidateCleanupObject;
pub(crate) use database::materialization_models::{
    OwnedVerifiedMergeMaterialization, RetainedMergeMaterializationKey, RetainedPackageApplication,
    VerifiedMergeMaterialization, VerifiedMergeMembershipObjects,
};
pub(crate) use database::reclaim::journal::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, ReclaimCommitActivation,
    ReclaimedStorePackage, StoreReclaimCandidateLoss, StoreReclaimJournalError,
};
#[cfg(test)]
pub(super) use database::store_package_is_retained_for_replay_for_test;
pub(crate) use database::StoreDatabaseRuntime;
pub use device_join_transport::{
    abandon_device_join_via_transport, cancel_device_join_via_transport, drive_device_join,
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinArtifact, DeviceJoinDriveOutcome,
    DeviceJoinOfferBundle, DeviceJoinRoles, DeviceJoinStep, DeviceJoinTransport,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub use error::StoreError;
pub(crate) use error::StorePreparationError;
pub use membership::{AnchoredChainError, InviteError, MembershipOpsError, OWNER_PUBKEY_STATE_KEY};
pub(crate) use owner::device_exclusion::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};
pub use owner::device_exclusion::{
    StoreDeviceExclusionError, StoreDeviceExclusionOperationInfo,
    StoreDeviceExclusionOperationStatus, StoreDeviceExclusionResult,
};
#[doc(hidden)]
pub use owner::device_join::{
    load_pending_device_join_actions, load_pending_device_join_status, DeviceJoinJournalDatabase,
    DeviceJoinJournalRecord, DeviceJoinRoleProgress, JoinerJoinProgress, JoiningStore,
    OwnerJoinProgress, PendingDeviceJoinAuthority, PendingDeviceJoinClosure,
    PendingDeviceJoinObservation, PreparedDeviceJoinObject, ProviderAdminJoinProgress,
};
pub use owner::device_join::{
    DeviceJoinAbandonment, DeviceJoinAbandonmentRef, DeviceJoinAction, DeviceJoinActivation,
    DeviceJoinCancellation, DeviceJoinCleanupActivation, DeviceJoinCleanupProgress,
    DeviceJoinCleanupReceipt, DeviceJoinCleanupReceiptRef, DeviceJoinError, DeviceJoinOffer,
    DeviceJoinProducer, DeviceJoinProducerWriteRevocation, DeviceJoinReadiness, DeviceJoinRole,
    DeviceJoinStatus, DeviceJoinWriteRevocationExecutor, DeviceProviderAccessAdministrator,
    DeviceProviderAccessRequest, DeviceProviderAdmission, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionChallenge, DeviceProviderAdmissionCompletion,
    DeviceProviderChallengePublication, DeviceProviderReadiness, DeviceProviderResponseReservation,
    DeviceRegistrationRequest, JoinedStore, JoinerJoinClosure, JoinerJoinTerminal,
    JoinerResponseDisposition, ProviderAdminJoinClosure, ProviderAdminJoinTerminal,
    ProviderChallengeDisposition, ProviderReadyDeviceBootstrap, ProviderWriteAuthorityRef,
    ProvisionalDeviceBootstrap, SlotDisposition,
};
#[cfg(test)]
pub(crate) use owner::history::retained_membership_floor_is_included;
pub(crate) use owner::operations::CircleAckActivation;
pub(crate) use owner::operations::PreparedStoreOperationCommit;
#[doc(hidden)]
pub use owner::owner_promotion::OwnerPromotionError;
pub(crate) use owner::pull::install_circle_bootstrap_image_on;
#[cfg(test)]
pub(crate) use owner::pull::prepare_merge_abandonment_history_summary;
#[doc(hidden)]
#[doc(hidden)]
pub use owner::pull::StorePullExecution;
pub(crate) use owner::pull::VerifiedStoreSnapshotStability;
pub use owner::pull::{
    BlobDownloadFailure, BlobDownloadFailures, HeldStoreCoordinate, HeldStorePosition,
    HeldStorePositionReason, PullError, StorePullError, StorePullMembershipError, StorePullResult,
    VerifiedStoreDeviceHead,
};
pub use owner::reclaim::{
    reclaim_authorization_semantic_prefix, reclaim_evidence_semantic_prefix,
    reclaim_receipt_semantic_prefix, CirclePackageReclaimClaim, CirclePackageReclaimTarget,
    CircleSnapshotLocator, ReclaimAuthorization, ReclaimAuthorizationRef, ReclaimClaim,
    ReclaimEvidence, ReclaimEvidenceRef, ReclaimReceipt, ReclaimReceiptRef, ReclaimTarget,
    StorePackageReclaimClaim, StorePackageReclaimTarget, StoreReclaimError, StoreReclaimResult,
};
#[cfg(test)]
pub(crate) use owner::snapshot::drain_outbound_store_snapshot;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use owner::snapshot::CreatedSnapshot;
#[doc(hidden)]
pub use owner::snapshot::{
    bootstrap_from_snapshot, create_snapshot, BootstrapResult, SnapshotBlobReconcile, SnapshotError,
};
#[cfg(test)]
pub(crate) use owner::StoreAckError;
pub(crate) use owner::{bootstrap_pending_device, prepare_registration_for_origin};
pub(crate) use owner::{AuthorizedStore, AuthorizedWriterOperation, StoreInitializationError};
#[doc(hidden)]
pub use owner::{HostWriteBlobStaging, Store, StoreRestoreMembership};
pub use owner::{RestoringStore, StoreRegistrationError};
pub(crate) use retained_replay::{
    RetainedReplayAuthority, RetainedReplayBaseline, RetainedReplayGenesisAuthority,
    RetainedReplaySnapshotAuthority,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockedWriteDiscard {
    Discarded(Vec<crate::WriteId>),
    RemoteResolutionRequired,
}

#[cfg(test)]
pub(crate) async fn push_circle_snapshots_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    temp_dir: std::path::PathBuf,
    schema_version: u32,
    identity: &UserKeypair,
    created_at: &str,
    store_routing: &crate::encryption::EncryptionService,
) -> Result<super::store_commit::CircleSnapshotMeta, snapshot::SnapshotError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    store
        .authorize_writer()
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .author_one_circle_snapshot_for_test(temp_dir, schema_version, created_at, store_routing)
        .await
}

/// Drive the resume-aware Circle snapshot publication the cycle runs: resume any
/// pending durable publication first, then author one snapshot per active Circle.
/// A publication failure for one Circle is logged and leaves its durable row for
/// the next run to resume, so this returns `Ok` even when an armed upload fails.
#[cfg(test)]
pub(crate) async fn drive_circle_snapshot_publications_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    temp_dir: std::path::PathBuf,
    schema_version: u32,
    identity: &UserKeypair,
    created_at: &str,
    store_routing: Option<&crate::encryption::EncryptionService>,
) -> Result<(), snapshot::SnapshotError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    store
        .authorize_writer()
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .push_circle_snapshots(temp_dir, schema_version, created_at, store_routing)
        .await
}

#[cfg(test)]
pub(crate) async fn load_circle_snapshot_metas_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    circle_id: crate::sync::circle::CircleId,
    encryption: crate::encryption::EncryptionService,
    signer: &UserKeypair,
) -> Result<Vec<super::store_commit::CircleSnapshotMeta>, snapshot::SnapshotError> {
    Store::load(StoreDatabase::new(db), storage.clone(), signer.clone())
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .authorize_writer()
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .load_circle_snapshot_metas_for_test(circle_id, encryption)
        .await
}

/// Perform the recipient-side verification a restoring device runs against a
/// published standalone Circle snapshot: decrypt the image with the Circle epoch
/// key, then authenticate its row-routing state against the key derived from
/// `store_routing`. This is the check `select_standalone_snapshot_candidate` runs
/// during restore, isolated so a test can assert an authored image authenticates
/// under the *true* Store routing key rather than the author's epoch key.
#[cfg(test)]
pub(crate) async fn verify_standalone_circle_snapshot_image_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    circle_id: crate::sync::circle::CircleId,
    epoch_encryption: crate::encryption::EncryptionService,
    store_routing: &crate::encryption::EncryptionService,
    signer: &UserKeypair,
) -> Result<(), snapshot::SnapshotError> {
    Store::load(StoreDatabase::new(db), storage.clone(), signer.clone())
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .authorize_writer()
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .verify_standalone_circle_snapshot_image_for_test(
            circle_id,
            epoch_encryption,
            store_routing,
        )
        .await
}

#[cfg(test)]
pub(crate) async fn circle_snapshot_is_stable_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    identity: &UserKeypair,
    circle_id: crate::sync::circle::CircleId,
    control: &crate::sync::circle::CircleControlCoord,
    snapshot_cut: &crate::sync::store_commit::CommitFrontier,
) -> Result<bool, snapshot::SnapshotError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    store
        .authorize_writer()
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .circle_snapshot_is_stable(circle_id, control, snapshot_cut)
        .await
}

#[cfg(test)]
pub(crate) async fn reclaim_packages_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    identity: &UserKeypair,
) -> Result<reclaim::StoreReclaimResult, reclaim::StoreReclaimError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| reclaim::StoreReclaimError::Authorization(error.to_string()))?;
    let mut writer = store
        .authorize_writer()
        .await
        .map_err(|error| reclaim::StoreReclaimError::Authorization(error.to_string()))?;
    writer.reclaim_packages().await
}

#[cfg(test)]
pub(crate) async fn load_circle_acknowledgement_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    identity: &UserKeypair,
    reference: &super::store_commit::CircleAckRef,
    control: &super::circle::CircleControlCoord,
) -> Result<super::store_commit::CircleAck, owner::StoreAckError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| owner::StoreAckError::InvalidOutbound(error.to_string()))?;
    store
        .load_circle_acknowledgement_for_test(reference, control)
        .await
}
