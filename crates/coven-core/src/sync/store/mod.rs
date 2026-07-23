use std::sync::Arc;

use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{BlobPathScheme, CloudCipherAccess, CloudSyncStorage};
use super::cycle::SyncCycleFailure;
use super::storage::SyncStorage;
use super::store_commit::{CommitFrontier, StoreProtocolRoot, StoreRootRef};

pub(crate) mod abandonment;
mod acknowledgements;
#[doc(hidden)]
pub mod blob;
pub(crate) mod circle_controls;
pub(crate) mod database;
#[cfg(test)]
pub(in crate::sync) use database::record_verified_circle_activations_for_test;
#[doc(hidden)]
pub use database::StoreDatabase;
pub(crate) mod device_exclusion;
#[doc(hidden)]
pub mod device_join;
mod error;
mod membership;
pub(crate) mod operations;
mod owner;
pub mod owner_promotion;
pub(crate) mod package_preparation;
pub(crate) mod preparation;
pub(crate) mod publication;
mod pull;
mod reclaim;
mod registration;
pub(crate) mod retained_replay;
mod snapshot;

#[doc(hidden)]
pub use abandonment::MergeCandidateAbandonment;
pub use circle_controls::CircleOperationError;
#[cfg(test)]
pub(in crate::sync) use database::store_package_is_retained_for_replay_for_test;
pub(crate) use device_exclusion::{
    DurableStoreDeviceExclusionObject, DurableStoreDeviceExclusionOperation,
    StoreDeviceExclusionCompletion, StoreDeviceExclusionJournalError,
};
pub use device_exclusion::{
    StoreDeviceExclusionError, StoreDeviceExclusionOperationInfo,
    StoreDeviceExclusionOperationStatus, StoreDeviceExclusionResult,
};
pub use error::StoreError;
pub(crate) use membership::{
    apply_key_rotation, load_and_persist_owner_anchor, unwrap_store_keyring_for_refs,
};
#[cfg(test)]
pub(crate) use membership::{
    complete_revoke_rotation_adoption, load_exact_membership_head, revoke_member_durable,
    signed_wrapped_keyring_for_test,
};
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub use membership::{invite_member, remove_member};
pub use membership::{
    resolve_membership_conflict, AnchoredChainError, InviteError, MembershipOpsError,
    OWNER_PUBKEY_STATE_KEY,
};
#[doc(hidden)]
pub use membership::{seed_head_watermark, unwrap_store_keyring};
pub(crate) use owner::AuthorizedStore;
#[doc(hidden)]
pub use owner::{Store, StoreRestoreMembership};
pub(crate) use pull::VerifiedStoreSnapshotStability;
#[cfg(test)]
pub(crate) use pull::{
    cleanup_merge_candidate, download_blobs, load_merge_conflict_resolution_authorization,
    prepare_merge_abandonment_history_summary, retained_membership_floor_is_included, BlobDownload,
};
#[doc(hidden)]
pub use pull::{load_cycle_membership, pull_store_commits, CycleMembership};
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
    reclaim_receipt_semantic_prefix, ReclaimAuthorization, ReclaimAuthorizationRef,
    ReclaimEvidence, ReclaimEvidenceRef, ReclaimReceipt, ReclaimReceiptRef,
    StorePackageReclaimClaim, StorePackageReclaimTarget, StoreReclaimError, StoreReclaimResult,
};
pub(crate) use registration::{
    bootstrap_pending_device, ensure_active_registration, install_existing_founder_device,
    prepare_registration_for_origin,
};
pub use registration::{recover_owner_device, StoreRegistrationError};
#[cfg(test)]
pub(crate) use snapshot::drain_outbound_store_snapshot;
#[doc(hidden)]
pub use snapshot::{
    bootstrap_from_snapshot, create_snapshot, load_store_snapshot_ref, reconcile_snapshot_blobs,
    BootstrapResult, SnapshotBlobReconcile, SnapshotError,
};
pub(crate) use snapshot::{
    create_snapshot_with_host_blobs, push_store_snapshot, should_create_snapshot, CreatedSnapshot,
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
pub(crate) async fn verify_store_snapshot_for_acknowledgement_for_test(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(), pull::StorePullError> {
    pull::verify_snapshot_for_acknowledgement(storage, root, snapshot).await
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
        let evidence = pull::load_device_join_cleanup_activation(storage, root, activation).await?;
        pull::verify_device_join_cleanup_activation(storage, root, evidence).await
    })
}

pub(crate) async fn verify_accepted_provider_access_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    access: &super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &super::provider::ProviderAdminGrantRecord,
    administrator: &super::store_commit::StoreDeviceRegistration,
) -> Result<(), pull::StorePullError> {
    let root_value = super::store_objects::load_store_protocol_root(storage, root)
        .await?
        .value;
    pull::verify_accepted_provider_access_activation(
        storage,
        root,
        &root_value,
        access,
        provider_admin,
        administrator,
    )
    .await
}

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

pub(crate) async fn find_owner_promotion_request_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    request: &super::store_commit::OwnerPromotionRequest,
) -> Result<super::store_commit::OwnerPromotionRequestActivation, pull::StorePullError> {
    pull::find_owner_promotion_request_activation(storage, root, request).await
}

pub(crate) async fn verify_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<VerifiedOwnerPromotionAcceptance, pull::StorePullError> {
    pull::verify_owner_promotion_acceptance(storage, root, acceptance)
        .await
        .map(|()| VerifiedOwnerPromotionAcceptance)
}

pub(super) struct StoreContext {
    database: StoreDatabase,
    storage: Arc<CloudSyncStorage>,
    store_root: StoreRootRef,
}

impl StoreContext {
    fn database(&self) -> &StoreDatabase {
        &self.database
    }

    fn storage(&self) -> &Arc<CloudSyncStorage> {
        &self.storage
    }

    fn store_root(&self) -> &StoreRootRef {
        &self.store_root
    }
}

pub(super) fn snapshot_position_for_stream(
    snapshot: &crate::database::PublishedStoreSnapshot,
    stream_id: &str,
) -> u64 {
    snapshot
        .meta
        .coverage
        .clone()
        .into_refs()
        .remove(stream_id)
        .map(|reference| reference.coord.sequence())
        .unwrap_or(0)
}
