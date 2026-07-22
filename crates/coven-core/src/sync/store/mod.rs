use std::sync::Arc;

use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{BlobPathScheme, CloudCipherAccess, CloudSyncStorage};
use super::cycle::SyncCycleFailure;
use super::pull::MembershipDiscoveryProof;
use super::storage::SyncStorage;
use super::store_commit::{CommitFrontier, StoreProtocolRoot, StoreRootRef};

pub(crate) mod abandonment;
mod acknowledgements;
mod database;
#[doc(hidden)]
pub mod device_join;
mod error;
pub(crate) mod operations;
mod owner;
pub(crate) mod package_preparation;
pub(crate) mod preparation;
pub(crate) mod publication;
pub(crate) mod pull;
mod registration;
pub(crate) mod snapshot;

#[doc(hidden)]
pub use abandonment::MergeCandidateAbandonment;
pub use error::StoreError;
pub(crate) use owner::{AuthorizedStore, Store};
pub use pull::{
    StorePullError, StorePullMembershipError, StorePullResult, VerifiedStoreDeviceHead,
};
#[cfg(feature = "test-utils")]
#[doc(hidden)]
pub use registration::ensure_active_registration as ensure_active_registration_for_test;
pub(crate) use registration::{
    bootstrap_pending_device, ensure_active_registration, install_existing_founder_device,
    prepare_registration_for_origin,
};
pub use registration::{recover_owner_device, StoreRegistrationError};
pub use snapshot::load_store_snapshot_ref;

enum StoreLoadError {
    Database(crate::database::DbError),
    Object(super::store_objects::StoreObjectError),
    MissingRoot,
    Invalid(String),
}

impl From<StoreLoadError> for StoreError {
    fn from(error: StoreLoadError) -> Self {
        match error {
            StoreLoadError::Database(error) => error.into(),
            StoreLoadError::Object(error) => Self::Object(error),
            StoreLoadError::MissingRoot => Self::MissingState {
                key: operations::STORE_ROOT_AUTHORITY,
            },
            StoreLoadError::Invalid(reason) => Self::InvalidOutbound(reason),
        }
    }
}

#[doc(hidden)]
pub async fn abandon_merge_candidate(
    db: &Database,
    storage: Arc<CloudSyncStorage>,
    device_id: &str,
    identity: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
    load_store(db, storage)
        .await?
        .abandon_candidate(device_id, identity, write_id)
        .await
}

async fn load_store(
    db: &Database,
    storage: Arc<CloudSyncStorage>,
) -> Result<Store, StoreLoadError> {
    let store_root = db
        .local_store_root_ref()
        .await
        .map_err(StoreLoadError::Database)?
        .ok_or(StoreLoadError::MissingRoot)?;
    let verified_root = super::store_objects::load_store_protocol_root(&*storage, &store_root)
        .await
        .map_err(StoreLoadError::Object)?
        .value;
    Store::new(db.clone(), storage, store_root, &verified_root).map_err(StoreLoadError::Invalid)
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

#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub fn pull_store_commits<'a>(
    db: &'a Database,
    tables: &'a [super::session::SyncedTable],
    storage: &'a dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    store_dir: &'a StoreDir,
    membership: &'a super::membership::MembershipChain,
    identity: Option<&'a UserKeypair>,
) -> pull::StorePullFuture<'a, StorePullResult> {
    pull::pull_store_commits(
        db,
        tables,
        storage,
        store_root_hash,
        store_dir,
        membership,
        identity,
    )
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
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::StoreBatchCommitRef,
    expected_outcome: &'a super::store_commit::DeviceJoinOutcomeRef,
    membership_state: &'a super::circle_control::StoreMembershipStateRef,
) -> pull::StorePullFuture<'a, ()> {
    pull::materialize_device_join_activation(
        db,
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
    db: Database,
    storage: Arc<CloudSyncStorage>,
    store_root: StoreRootRef,
}

impl StoreContext {
    fn db(&self) -> &Database {
        &self.db
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
