use std::sync::Arc;

use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{BlobPathScheme, CloudCipherAccess, CloudSyncStorage};
use super::cycle::SyncCycleFailure;
use super::storage::SyncStorage;
use super::store_commit::{CommitFrontier, StoreProtocolRoot, StoreRootRef};

mod abandonment;
mod acknowledgements;
#[doc(hidden)]
pub mod blob;
mod circle_controls;
mod database;
#[cfg(test)]
pub(in crate::sync) use database::record_verified_circle_activations_for_test;
#[doc(hidden)]
pub use database::{HostWriteBlobStaging, StoreDatabase};
mod device_exclusion;
mod device_join;
mod device_join_transport;
mod error;
mod membership;
mod operations;
mod owner;
mod owner_promotion;
mod package_preparation;
mod preparation;
#[cfg(not(any(test, feature = "test-utils")))]
mod protocol_root;
#[cfg(any(test, feature = "test-utils"))]
pub(in crate::sync) mod protocol_root;
mod publication;
mod pull;
mod reclaim;
mod registration;
mod retained_replay;
mod snapshot;

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
pub(crate) use database::candidate_records::{
    load_author_exclusion_activation_locator_on, CandidateCleanupObject,
};
pub(crate) use database::materialization_models::{
    OwnedVerifiedMergeMaterialization, RetainedMergeMaterializationKey, RetainedPackageApplication,
    VerifiedMergeMaterialization, VerifiedMergeMembershipObjects,
};
#[cfg(test)]
pub(in crate::sync) use database::store_package_is_retained_for_replay_for_test;
pub(crate) use database::StoreDatabaseRuntime;
pub(crate) use device_exclusion::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};
pub use device_exclusion::{
    StoreDeviceExclusionError, StoreDeviceExclusionOperationInfo,
    StoreDeviceExclusionOperationStatus, StoreDeviceExclusionResult,
};
#[cfg(test)]
pub(crate) use device_join::{
    abandon_device_join, activate_device_join_cleanup, cancel_device_join,
    close_device_provider_admission, complete_owner_device_join_cleanup,
    load_store_device_join_actions, load_store_device_join_status, prepare_device_join_cleanup,
    revoke_device_provider_admission_writes, revoke_joining_device_writes,
};
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use device_join::{
    accept_device_registration_request, authorize_device_provider_access, begin_device_join,
    complete_device_provider_admission, finalize_device_join,
    load_current_device_join_authorization, publish_device_provider_challenge,
};
#[doc(hidden)]
pub use device_join::{
    accept_joiner_device_join_cleanup, close_joining_device, complete_device_join,
    complete_joiner_device_join_cleanup, load_pending_device_join_actions,
    load_pending_device_join_status, observe_device_join_abandonment,
    observe_device_join_activation, prepare_device_provider_access_request,
    prepare_device_registration_request, DeviceJoinJournalDatabase, DeviceJoinJournalRecord,
    DeviceJoinRoleProgress, JoinerJoinProgress, OwnerJoinProgress, PreparedDeviceJoinObject,
    ProviderAdminJoinProgress,
};
#[doc(hidden)]
pub use device_join::{bootstrap_joining_device, materialize_joined_store_activation};
pub use device_join::{
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
pub use device_join_transport::{
    abandon_device_join_via_transport, cancel_device_join_via_transport, drive_device_join,
    DeviceJoinApproval, DeviceJoinApprovalPolicy, DeviceJoinArtifact, DeviceJoinDriveOutcome,
    DeviceJoinOfferBundle, DeviceJoinRoles, DeviceJoinStep, DeviceJoinTransport,
    DeviceJoinTransportError, DeviceJoinTransportKind, DeviceJoinTransportParams,
    DeviceJoinTransportTiming,
};
pub use error::StoreError;
pub(crate) use membership::apply_key_rotation;
#[cfg(test)]
pub(crate) use membership::unwrap_store_keyring_for_refs;
#[cfg(test)]
pub(crate) use membership::{
    complete_revoke_rotation_adoption, load_exact_membership_head, revoke_member_durable,
    signed_wrapped_keyring_for_test,
};
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use membership::{invite_member, remove_member};
#[doc(hidden)]
pub use membership::{seed_head_watermark, unwrap_store_keyring};
pub use membership::{AnchoredChainError, InviteError, MembershipOpsError, OWNER_PUBKEY_STATE_KEY};
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use operations::load_local_store_authority as load_local_store_authority_for_test;
#[cfg(test)]
pub(crate) use operations::prepare_plan as prepare_store_operation_plan_for_test;
pub(crate) use operations::CircleAckActivation;
pub(crate) use operations::PreparedStoreOperationCommit;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use owner::anchor_owner_membership;
pub(crate) use owner::{AuthorizedStore, StoreInitializationError};
#[doc(hidden)]
pub use owner::{Store, StoreRestoreMembership};
#[doc(hidden)]
pub use owner_promotion::OwnerPromotionError;
pub(crate) use pull::install_circle_bootstrap_image_on;
#[doc(hidden)]
pub use pull::StoreCommitVerifier;
pub(crate) use pull::VerifiedStoreSnapshotStability;
#[cfg(test)]
pub(crate) use pull::{
    cleanup_merge_candidate, download_blobs, load_merge_conflict_resolution_authorization,
    prepare_merge_abandonment_history_summary, retained_membership_floor_is_included, BlobDownload,
};
#[doc(hidden)]
pub use pull::{load_cycle_membership, pull_store_commits, StorePullExecution};
pub use pull::{
    BlobDownloadFailure, BlobDownloadFailureCause, BlobDownloadFailures, HeldStoreCoordinate,
    HeldStorePosition, HeldStorePositionReason, PullError, StorePullError,
    StorePullMembershipError, StorePullResult, VerifiedStoreDeviceHead,
};
pub(crate) use reclaim::journal::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, ReclaimCommitActivation,
    ReclaimedStorePackage, StoreReclaimCandidateLoss, StoreReclaimJournalError,
};
pub use reclaim::{
    reclaim_authorization_semantic_prefix, reclaim_evidence_semantic_prefix,
    reclaim_receipt_semantic_prefix, CirclePackageReclaimClaim, CirclePackageReclaimTarget,
    CircleSnapshotLocator, ReclaimAuthorization, ReclaimAuthorizationRef, ReclaimClaim,
    ReclaimEvidence, ReclaimEvidenceRef, ReclaimReceipt, ReclaimReceiptRef, ReclaimTarget,
    StorePackageReclaimClaim, StorePackageReclaimTarget, StoreReclaimError, StoreReclaimResult,
};
pub(crate) use registration::{
    bootstrap_pending_device, ensure_active_registration, prepare_registration_for_origin,
};
pub use registration::{recover_owner_device, StoreRegistrationError};
pub(crate) use retained_replay::{
    RetainedReplayAuthority, RetainedReplayBaseline, RetainedReplayGenesisAuthority,
    RetainedReplaySnapshotAuthority,
};
#[cfg(test)]
pub(crate) use snapshot::drain_outbound_store_snapshot;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use snapshot::push_store_snapshot;
#[cfg(any(test, feature = "test-utils"))]
pub(crate) use snapshot::CreatedSnapshot;
#[doc(hidden)]
pub use snapshot::{
    bootstrap_from_snapshot, create_snapshot, load_store_snapshot_ref, reconcile_snapshot_blobs,
    BootstrapResult, SnapshotBlobReconcile, SnapshotError,
};

#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub async fn ensure_active_registration_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
) -> Result<(), StoreRegistrationError> {
    registration::ensure_active_registration(&StoreDatabase::new(db), storage).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockedWriteDiscard {
    Discarded(Vec<crate::WriteId>),
    RemoteResolutionRequired,
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn stage_store_acknowledgement_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    frontier: CommitFrontier,
    sync_time: String,
    identity: &UserKeypair,
) -> Result<super::store_commit::StoreAck, acknowledgements::StoreAckError> {
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| acknowledgements::StoreAckError::InvalidOutbound(error.to_string()))?;
    store
        .stage_acknowledgement(frontier, sync_time, identity)
        .await
}

#[cfg(test)]
pub(crate) async fn exact_next_announcement_slot_for_test(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &super::store_commit::StoreDeviceRegistrationRef,
    registration: &super::store_commit::StoreDeviceRegistration,
    previous: Option<&super::store_commit::StoreBatchCommitRef>,
) -> Result<
    (
        crate::storage::cloud::ObjectSlot,
        Option<super::store_commit::StoreDeviceHeadRef>,
    ),
    StoreError,
> {
    let mut verifier = pull::StoreCommitVerifier::new(storage, root).await?;
    let previous = match previous {
        Some(reference) => Some(verifier.load_ref(reference).await?),
        None => None,
    };
    operations::exact_next_announcement_slot(
        storage,
        root,
        registration_ref,
        registration,
        &mut verifier,
        previous.as_ref(),
    )
    .await
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn drain_store_acknowledgements_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    identity: &UserKeypair,
) -> Result<u64, acknowledgements::StoreAckError> {
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| acknowledgements::StoreAckError::InvalidOutbound(error.to_string()))?;
    store.drain_acknowledgements(identity).await
}

#[cfg(test)]
pub(crate) async fn push_circle_snapshots_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    temp_dir: std::path::PathBuf,
    schema_version: u32,
    identity: &UserKeypair,
    created_at: &str,
    store_routing: &crate::encryption::EncryptionService,
) -> Result<super::store_commit::CircleSnapshotMeta, snapshot::SnapshotError> {
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    store
        .author_one_circle_snapshot_for_test(
            temp_dir,
            schema_version,
            identity,
            created_at,
            store_routing,
        )
        .await
}

/// Drive the resume-aware Circle snapshot publication the cycle runs: resume any
/// pending durable publication first, then author one snapshot per active Circle.
/// A publication failure for one Circle is logged and leaves its durable row for
/// the next run to resume, so this returns `Ok` even when an armed upload fails.
#[cfg(test)]
pub(crate) async fn drive_circle_snapshot_publications_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    temp_dir: std::path::PathBuf,
    schema_version: u32,
    identity: &UserKeypair,
    created_at: &str,
    store_routing: Option<&crate::encryption::EncryptionService>,
) -> Result<(), snapshot::SnapshotError> {
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    store
        .push_circle_snapshots(
            temp_dir,
            schema_version,
            identity,
            created_at,
            store_routing,
        )
        .await
}

#[cfg(test)]
pub(crate) async fn load_circle_snapshot_metas_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    circle_id: crate::sync::circle::CircleId,
    encryption: crate::encryption::EncryptionService,
    signer: &UserKeypair,
) -> Result<Vec<super::store_commit::CircleSnapshotMeta>, snapshot::SnapshotError> {
    let database = StoreDatabase::new(db);
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .ok_or_else(|| {
            snapshot::SnapshotError::PublicationState("local device id absent".to_string())
        })?;
    let (root, registration_ref, registration, _) =
        operations::load_local_store_authority(&database, &device_id, signer)
            .await
            .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    snapshot::load_circle_snapshot_stream(
        storage,
        &root,
        circle_id,
        encryption,
        &registration_ref,
        &registration,
    )
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
    storage: &dyn SyncStorage,
    circle_id: crate::sync::circle::CircleId,
    epoch_encryption: crate::encryption::EncryptionService,
    store_routing: &crate::encryption::EncryptionService,
    signer: &UserKeypair,
) -> Result<(), snapshot::SnapshotError> {
    let database = StoreDatabase::new(db);
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?
        .ok_or_else(|| {
            snapshot::SnapshotError::PublicationState("local device id absent".to_string())
        })?;
    let (root, registration_ref, registration, _) =
        operations::load_local_store_authority(&database, &device_id, signer)
            .await
            .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    let stream = snapshot::load_circle_snapshot_stream(
        storage,
        &root,
        circle_id,
        epoch_encryption.clone(),
        &registration_ref,
        &registration,
    )
    .await?;
    let selected = snapshot::select_maximal_circle_snapshot(stream).ok_or_else(|| {
        snapshot::SnapshotError::PublicationState(
            "no standalone Circle snapshot to verify".to_string(),
        )
    })?;
    let author_device = selected.author_registration.device_id.to_string();
    let image_context = crate::sync::storage::ProtocolObjectContext::circle(
        root.store_root_hash,
        crate::sync::storage::ProtocolObjectDomain::CircleSnapshotImage,
        epoch_encryption,
    );
    let image = storage
        .read_protocol_object(
            &image_context,
            &selected.bootstrap.image.object,
            &crate::sync::store_commit::circle_snapshot_image_semantic_prefix(
                circle_id,
                &author_device,
                selected.bootstrap.image.image_hash,
            ),
        )
        .await
        .map_err(snapshot::SnapshotError::Bucket)?;
    let routing_key =
        crate::sync::circle::derive_row_routing_key(store_routing, root.store_root_hash)
            .map_err(|error| snapshot::SnapshotError::BootstrapState(error.to_string()))?;
    snapshot::verify_circle_bootstrap_image(
        &image,
        &selected.bootstrap,
        circle_id,
        db.synced_tables(),
        Some(&routing_key),
    )
}

#[cfg(test)]
pub(crate) async fn circle_snapshot_is_stable_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    circle_id: crate::sync::circle::CircleId,
    control: &crate::sync::circle::CircleControlCoord,
    snapshot_cut: &crate::sync::store_commit::CommitFrontier,
) -> Result<bool, snapshot::SnapshotError> {
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| snapshot::SnapshotError::PublicationState(error.to_string()))?;
    store
        .circle_snapshot_is_stable(circle_id, control, snapshot_cut)
        .await
}

#[cfg(test)]
pub(crate) async fn stage_circle_acknowledgements_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    frontier: &CommitFrontier,
    sync_time: &str,
    identity: &UserKeypair,
) -> Result<(), acknowledgements::StoreAckError> {
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| acknowledgements::StoreAckError::InvalidOutbound(error.to_string()))?;
    store
        .stage_circle_acknowledgements(frontier, sync_time, identity)
        .await
}

#[cfg(test)]
pub(crate) async fn reclaim_packages_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    identity: &UserKeypair,
) -> Result<reclaim::StoreReclaimResult, reclaim::StoreReclaimError> {
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .expect("read local device id")
        .expect("local device id is installed");
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| reclaim::StoreReclaimError::Authorization(error.to_string()))?;
    store.reclaim_packages(&device_id, identity).await
}

#[cfg(test)]
pub(crate) async fn load_circle_acknowledgement_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    reference: &super::store_commit::CircleAckRef,
    control: &super::circle::CircleControlCoord,
) -> Result<super::store_commit::CircleAck, acknowledgements::StoreAckError> {
    let store = Store::authorize_borrowed(storage, db)
        .await
        .map_err(|error| acknowledgements::StoreAckError::InvalidOutbound(error.to_string()))?;
    acknowledgements::load_circle_acknowledgement_on(
        store.database(),
        store.storage(),
        store.store_root(),
        reference,
        control,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn prepare_store_acknowledgement_activation_for_test(
    db: &Database,
    acknowledgement: super::store_commit::StoreAckRef,
    candidate: crate::sync::store::operations::PreparedStoreOperationCommit,
) -> Result<(), crate::database::DbError> {
    owner::prepare_acknowledgement_activation_for_test(db, acknowledgement, candidate).await
}

pub(crate) async fn verify_store_snapshot_stability(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<pull::VerifiedStoreSnapshotStability, pull::StorePullError> {
    pull::verify_snapshot_stability(storage, root, snapshot).await
}

#[cfg(test)]
pub(crate) async fn verify_store_snapshots_for_acknowledgement_for_test(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshots: &[crate::database::PublishedStoreSnapshot],
) -> Result<(), pull::StorePullError> {
    pull::verify_snapshots_for_acknowledgement(storage, root, snapshots).await
}

pub(crate) fn load_verified_device_join_attempt_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::DeviceJoinAttemptRef,
    owner: &'a super::store_commit::StoreDeviceRegistration,
) -> pull::StorePullFuture<
    'a,
    super::store_objects::VerifiedObject<super::store_commit::DeviceJoinAttempt>,
> {
    Box::pin(async move {
        let evidence =
            pull::load_device_join_attempt_evidence_ref(storage, root, reference, owner).await?;
        pull::verify_device_join_attempt_evidence(storage, root, evidence).await
    })
}

pub(crate) fn verify_device_join_cleanup_activation<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    activation: &'a device_join::DeviceJoinCleanupActivation,
) -> pull::StorePullFuture<'a, device_join::JoinerJoinTerminal> {
    Box::pin(async move {
        let mut history_verifier = pull::MergeHistoryVerifier::new(storage, root).await?;
        let evidence = pull::load_device_join_cleanup_activation(
            &mut history_verifier,
            storage,
            root,
            activation,
        )
        .await?;
        pull::verify_device_join_cleanup_activation(&mut history_verifier, storage, root, evidence)
            .await
    })
}

pub(crate) async fn verify_accepted_provider_access_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    access: &super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &super::provider::ProviderAdminGrantRecord,
    administrator: &super::store_commit::StoreDeviceRegistration,
) -> Result<(), pull::StorePullError> {
    let mut history_verifier = pull::MergeHistoryVerifier::new(storage, root).await?;
    pull::verify_accepted_provider_access_activation(
        &mut history_verifier,
        access,
        provider_admin,
        administrator,
    )
    .await
}

#[cfg(test)]
pub(crate) fn prepare_device_join_bootstrap<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    coverage: &'a super::store_commit::StoreHistoryCut,
    attempt_activation: &'a super::store_commit::StoreBatchCommitRef,
    membership_state: &'a super::circle_control::StoreMembershipStateRef,
) -> pull::StorePullFuture<'a, pull::DeviceJoinBootstrapPlan> {
    pull::prepare_device_join_bootstrap(
        storage,
        root,
        coverage,
        attempt_activation,
        membership_state,
    )
}

pub(crate) fn materialize_device_join_activation<'a>(
    database: &'a StoreDatabase,
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::StoreBatchCommitRef,
    expected_outcome: &'a super::store_commit::DeviceJoinOutcomeRef,
    membership_state: &'a super::circle_control::StoreMembershipStateRef,
) -> pull::StorePullFuture<'a, ()> {
    pull::materialize_device_join_activation(
        database,
        storage,
        root,
        reference,
        expected_outcome,
        membership_state,
    )
}

pub(crate) struct VerifiedOwnerPromotionAcceptance;

pub(in crate::sync::store) async fn find_owner_promotion_request_activation(
    history_verifier: &mut pull::MergeHistoryVerifier<'_>,
    request: &super::store_commit::OwnerPromotionRequest,
) -> Result<pull::VerifiedOwnerPromotionRequestActivation, pull::StorePullError> {
    pull::find_owner_promotion_request_activation(history_verifier, request).await
}

pub(in crate::sync::store) async fn verify_owner_promotion_acceptance_from_request_activation(
    history_verifier: &mut pull::MergeHistoryVerifier<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
    verified: pull::VerifiedOwnerPromotionRequestActivation,
) -> Result<VerifiedOwnerPromotionAcceptance, pull::StorePullError> {
    pull::verify_acceptance_from_request_activation(
        history_verifier,
        storage,
        root,
        acceptance,
        verified,
    )
    .await
    .map(|()| VerifiedOwnerPromotionAcceptance)
}

pub(crate) async fn verify_owner_promotion_acceptance_with_history(
    history_verifier: &mut pull::MergeHistoryVerifier<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<VerifiedOwnerPromotionAcceptance, pull::StorePullError> {
    pull::verify_owner_promotion_acceptance_with_history(
        history_verifier,
        storage,
        root,
        acceptance,
    )
    .await
    .map(|()| VerifiedOwnerPromotionAcceptance)
}
