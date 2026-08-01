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
mod registration_object;
#[cfg(test)]
use owner::reclaim;
use owner::snapshot;
use registration_object::prepare_registration_object;

pub(crate) use crate::database::DurableStoreReclaimObject;
pub use blob::{BlobCacheError, BlobStream};
pub(crate) use circle_controls::CircleOperationError;
pub(crate) use circle_controls::{
    CircleAuthoringState, CircleCurrentState, CircleOperationIntent, CircleOperationJournal,
    CirclePackageAccess, LocalCircleExclusion, PreparedCircleOperation, VerifiedCircleActivations,
    VerifiedCircleImage, VerifiedCircleReference, VerifiedStreamActivations,
};
#[cfg(test)]
pub(crate) use circle_controls::{VerifiedCircleAccess, VerifiedCircleActive};
pub use device_join_transport::{
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinDriveOutcome, DeviceJoinOfferBundle,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub(crate) use device_join_transport::{DeviceJoinRoles, DeviceJoinStep, DeviceJoinTransport};
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
#[cfg(test)]
pub(crate) use owner::device_join::{begin_joining_store_from_pending, PendingDeviceJoinAuthority};
pub(crate) use owner::device_join::{
    observe_pending_device_join, open_pending_device_join_authority, resume_joining_store,
    DeviceJoinAbandonmentRef, DeviceJoinCleanupReceiptRef, DeviceProviderAdmissionChallenge,
    DeviceProviderResponseReservation, JoiningStore,
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
pub(crate) use owner::history::open_invitation_history;
#[cfg(test)]
pub(crate) use owner::history::prepare_merge_abandonment_history_summary_for_test as prepare_merge_abandonment_history_summary;
pub(crate) use owner::operations::{
    CircleAckActivation, PreparedStoreOperationCommit, StoreMembershipJournalCompletion,
};
pub(crate) use owner::owner_promotion::{OwnerPromotionJournal, OwnerPromotionJournalTransition};
pub(crate) use owner::pull::VerifiedStoreSnapshotStability;
pub(crate) use owner::pull::{
    activated_merge_membership_remote_objects, ApplyOutcome, DeviceJoinBootstrapPlan,
    HeldStorePositionReason, LocalStoreMembership, MembershipAuthorityBytes,
    PreparedMergeMaterialization, PreparedMergeMaterializationPackage,
};
#[cfg(test)]
pub(crate) use owner::pull::{HeldStoreCoordinate, StorePullMembershipError};
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
pub(crate) use owner::snapshot::verify_store_snapshot_bytes;
#[doc(hidden)]
pub(crate) use owner::snapshot::{
    bootstrap_from_snapshot, BootstrapResult, SnapshotBlobReconcile, SnapshotError,
};
#[cfg(test)]
pub(crate) use owner::StoreAckError;
pub(crate) use owner::{
    AuthorizedWriterOperation, HostWriteBlobStaging, Store, StoreInitializationError,
};
pub(crate) use owner::{RestoringStore, StoreRegistrationError};
pub(crate) use protocol_root::{StoreCreationAttempt, STORE_CREATION_ATTEMPT_STATE_KEY};

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
        .circles()
        .snapshots()
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
        .circles()
        .snapshots()
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
        .circles()
        .snapshots()
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
        .circles()
        .snapshots()
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
    snapshot_cut: &crate::protocol::store_commit::CommitFrontier,
) -> Result<bool, snapshot::SnapshotError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    store
        .authorize_writer()
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .circles()
        .snapshots()
        .circle_snapshot_is_stable(circle_id, snapshot_cut)
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
) -> Result<crate::protocol::store_commit::CircleAck, owner::StoreAckError> {
    let store = Store::load(StoreDatabase::new(db), storage.clone(), identity.clone())
        .await
        .map_err(|error| owner::StoreAckError::InvalidOutbound(error.to_string()))?;
    store.load_circle_acknowledgement_for_test(reference).await
}
