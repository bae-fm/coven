use std::sync::Arc;

#[cfg(test)]
use crate::database::Database;
use crate::database::StoreDatabase;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cycle::SyncCycleFailure;
use crate::protocol::store_commit::{CommitFrontier, StoreProtocolRoot};
#[cfg(test)]
use crate::storage::CloudSyncStorage;
use crate::storage::SyncStorage;
use crate::storage::{BlobPathScheme, CloudCipherAccess};

#[doc(hidden)]
pub(crate) mod blob;
mod circle_controls;
pub(crate) mod device_join_transport;
mod error;
mod membership;
pub(crate) mod owner;
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

pub(crate) use crate::database::DurableStoreReclaimObject;
pub(crate) use circle_controls::CircleOperationError;
pub(crate) use circle_controls::{
    CircleAuthoringState, CircleCurrentState, CircleOperationIntent, CircleOperationJournal,
    CirclePackageAccess, LocalCircleExclusion, PreparedCircleOperation, VerifiedCircleActivations,
    VerifiedCircleImage, VerifiedCircleReference, VerifiedStreamActivations,
};
#[cfg(test)]
pub(crate) use circle_controls::{VerifiedCircleAccess, VerifiedCircleActive};
pub(crate) use device_join_transport::{
    abandon_device_join_via_transport, cancel_device_join_via_transport, drive_device_join,
    DeviceJoinRoles, DeviceJoinStep, DeviceJoinTransport,
};
pub use device_join_transport::{
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinDriveOutcome, DeviceJoinOfferBundle,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub(crate) use error::StoreError;
pub(crate) use error::StorePreparationError;
#[cfg(test)]
pub(crate) use membership::AnchoredChainError;
pub(crate) use membership::{InviteError, MembershipOpsError, OWNER_PUBKEY_STATE_KEY};
#[cfg(test)]
pub(crate) use owner::device_exclusion::StoreDeviceExclusionResult;
pub(crate) use owner::device_exclusion::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};
pub(crate) use owner::device_join::{
    load_pending_device_join_actions, load_pending_device_join_status, DeviceJoinAbandonmentRef,
    DeviceJoinCleanupReceiptRef, DeviceProviderAdmissionChallenge,
    DeviceProviderResponseReservation, JoiningStore, PendingDeviceJoinAuthority,
    PendingDeviceJoinObservation,
};
pub use owner::device_join::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinCleanupProgress, DeviceJoinCleanupReceipt,
    DeviceJoinError, DeviceJoinJournalDatabase, DeviceJoinJournalRecord, DeviceJoinOffer,
    DeviceJoinProducer, DeviceJoinProducerWriteRevocation, DeviceJoinReadiness, DeviceJoinRole,
    DeviceJoinStatus, DeviceJoinWriteRevocationExecutor, DeviceProviderAccessAdministrator,
    DeviceProviderAccessRequest, DeviceProviderAdmission, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionCompletion, DeviceProviderReadiness, DeviceRegistrationRequest,
    JoinedStore, JoinerJoinClosure, JoinerJoinTerminal, ProviderAdminJoinClosure,
    ProviderAdminJoinTerminal, ProviderReadyDeviceBootstrap, ProviderWriteAuthorityRef,
    ProvisionalDeviceBootstrap,
};
#[cfg(test)]
pub(crate) use owner::history::prepare_merge_abandonment_history_summary_for_test as prepare_merge_abandonment_history_summary;
#[cfg(test)]
pub(crate) use owner::history::retained_membership_floor_is_included;
pub(crate) use owner::operations::{
    CircleAckActivation, PreparedStoreOperationCommit, StoreMembershipJournalCompletion,
};
pub(crate) use owner::owner_promotion::{OwnerPromotionJournal, OwnerPromotionJournalTransition};
pub(crate) use owner::pull::install_circle_bootstrap_image_on;
pub(crate) use owner::pull::VerifiedStoreSnapshotStability;
pub(crate) use owner::pull::{
    apply_prepared_merge_materialization_on, replay_retained_merge_projection_on, ApplyOutcome,
    DeviceJoinBootstrapPlan, LocalStoreMembership, PreparedMergeMaterialization,
};
#[cfg(test)]
pub(crate) use owner::pull::{
    HeldStoreCoordinate, HeldStorePositionReason, StorePullMembershipError,
};
pub(crate) use owner::pull::{
    HeldStorePosition, PullError, StorePullError, StorePullResult, VerifiedStoreDeviceHead,
};
#[cfg(test)]
pub(crate) use owner::reclaim::StorePackageReclaimTarget;
pub(crate) use owner::reclaim::{
    reclaim_authorization_semantic_prefix, reclaim_evidence_semantic_prefix,
    reclaim_receipt_semantic_prefix, ReclaimAuthorization, ReclaimAuthorizationRef,
    ReclaimEvidence, ReclaimEvidenceRef, ReclaimReceipt, ReclaimReceiptRef, ReclaimTarget,
    StoreReclaimError,
};
#[cfg(test)]
pub(crate) use owner::snapshot::drain_outbound_store_snapshot;
#[doc(hidden)]
pub(crate) use owner::snapshot::{
    bootstrap_from_snapshot, BootstrapResult, SnapshotBlobReconcile, SnapshotError,
};
pub(crate) use owner::snapshot::{
    create_circle_snapshot_with_host_blobs, create_snapshot_with_host_blobs,
    verify_store_snapshot_bytes, CreatedSnapshot,
};
#[cfg(test)]
pub(crate) use owner::StoreAckError;
pub(crate) use owner::{bootstrap_pending_device, prepare_registration_for_origin};
pub(crate) use owner::{
    AuthorizedStore, AuthorizedWriterOperation, HostWriteBlobStaging, Store,
    StoreInitializationError,
};
pub(crate) use owner::{RestoringStore, StoreRegistrationError};
pub(crate) use protocol_root::{StoreCreationAttempt, STORE_CREATION_ATTEMPT_STATE_KEY};
pub(crate) use retained_replay::{
    replace_live_projection, RetainedReplayAuthority, RetainedReplayBaseline,
    RetainedReplayGenesisAuthority, RetainedReplaySnapshotAuthority, GENERATION_ZERO,
};

#[cfg(test)]
pub(crate) async fn push_circle_snapshots_for_test(
    db: &Database,
    storage: &Arc<CloudSyncStorage>,
    temp_dir: std::path::PathBuf,
    schema_version: u32,
    identity: &UserKeypair,
    created_at: &str,
    store_routing: &crate::encryption::EncryptionService,
) -> Result<crate::protocol::store_commit::CircleSnapshotMeta, snapshot::SnapshotError> {
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
    circle_id: crate::protocol::circle::CircleId,
    encryption: crate::encryption::EncryptionService,
    signer: &UserKeypair,
) -> Result<Vec<crate::protocol::store_commit::CircleSnapshotMeta>, snapshot::SnapshotError> {
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
    circle_id: crate::protocol::circle::CircleId,
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
    circle_id: crate::protocol::circle::CircleId,
    control: &crate::protocol::circle::CircleControlCoord,
    snapshot_cut: &crate::protocol::store_commit::CommitFrontier,
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
    reference: &crate::protocol::store_commit::CircleAckRef,
    control: &crate::protocol::circle::CircleControlCoord,
) -> Result<crate::protocol::store_commit::CircleAck, owner::StoreAckError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| owner::StoreAckError::InvalidOutbound(error.to_string()))?;
    store
        .load_circle_acknowledgement_for_test(reference, control)
        .await
}
