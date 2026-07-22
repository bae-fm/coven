use std::sync::Arc;

use crate::database::Database;
use crate::keys::UserKeypair;
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{BlobPathScheme, CloudCipherAccess, CloudSyncStorage};
use super::cycle::SyncCycleFailure;
use super::pull::MembershipDiscoveryProof;
use super::storage::{CoordinationStorage, SyncStorage};
use super::store_commit::{CommitFrontier, StoreProtocolRoot, StoreRootRef};
use super::store_pull::StorePullResult;
use serial::pull::SerialCycleAuthorization;

pub(crate) mod merge;
pub(crate) mod serial;

use merge::{AuthorizedMergeStoreEngine, MergeStoreEngine};
use serial::{AuthorizedSerialStoreEngine, SerialStoreEngine};

#[doc(hidden)]
pub use merge::abandonment::MergeCandidateAbandonment;
#[doc(hidden)]
pub use serial::abandonment::SerialBranchAbandonment;
#[doc(hidden)]
pub use serial::publication::current_serial_head_ref;
#[doc(hidden)]
pub use serial::pull::SerialResolutionPlan;

enum StoreEngineLoadError {
    Database(crate::database::DbError),
    Object(super::store_objects::StoreObjectError),
    MissingRoot,
    Invalid(String),
}

impl From<StoreEngineLoadError> for super::store_outbound::StoreOutboundError {
    fn from(error: StoreEngineLoadError) -> Self {
        match error {
            StoreEngineLoadError::Database(error) => error.into(),
            StoreEngineLoadError::Object(error) => Self::Object(error),
            StoreEngineLoadError::MissingRoot => Self::MissingState {
                key: super::store_outbound::STORE_ROOT_AUTHORITY,
            },
            StoreEngineLoadError::Invalid(reason) => Self::InvalidOutbound(reason),
        }
    }
}

impl From<StoreEngineLoadError> for super::store_pull::StorePullError {
    fn from(error: StoreEngineLoadError) -> Self {
        match error {
            StoreEngineLoadError::Database(error) => error.into(),
            StoreEngineLoadError::Object(error) => Self::Object(error),
            StoreEngineLoadError::MissingRoot => {
                Self::Database("exact Store root authority is absent".to_string())
            }
            StoreEngineLoadError::Invalid(reason) => Self::Database(reason),
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
) -> Result<MergeCandidateAbandonment, super::store_outbound::StoreOutboundError> {
    let engine = load_store_engine(db, storage).await?;
    match engine.0 {
        StoreEngineKind::Merge(engine) => {
            engine
                .abandon_candidate(device_id, identity, write_id)
                .await
        }
        StoreEngineKind::Serial(_) => {
            Err(super::store_outbound::StoreOutboundError::InvalidOutbound(
                "Merge candidate abandonment requires MergeConcurrent policy".to_string(),
            ))
        }
    }
}

#[doc(hidden)]
pub async fn abandon_serial_branch(
    db: &Database,
    storage: Arc<CloudSyncStorage>,
    device_id: &str,
    identity: &UserKeypair,
    store_dir: &StoreDir,
    branch_id: crate::PendingBranchId,
) -> Result<SerialBranchAbandonment, super::store_outbound::StoreOutboundError> {
    let engine = load_store_engine(db, storage).await?;
    match engine.0 {
        StoreEngineKind::Serial(engine) => {
            engine
                .abandon_branch(device_id, identity, store_dir, branch_id)
                .await
        }
        StoreEngineKind::Merge(_) => {
            Err(super::store_outbound::StoreOutboundError::InvalidOutbound(
                "Serial branch abandonment requires Serial policy".to_string(),
            ))
        }
    }
}

#[doc(hidden)]
pub async fn prepare_serial_resolution(
    db: &Database,
    storage: Arc<CloudSyncStorage>,
    store_dir: &StoreDir,
    branch_base: Option<super::store_commit::StoreBatchCommitRef>,
    identity: &UserKeypair,
) -> Result<SerialResolutionPlan, super::store_pull::StorePullError> {
    let engine = load_store_engine(db, storage).await?;
    match engine.0 {
        StoreEngineKind::Serial(engine) => {
            engine
                .prepare_resolution(store_dir, branch_base, identity)
                .await
        }
        StoreEngineKind::Merge(_) => Err(super::store_pull::StorePullError::Serial(
            "Serial resolution requires Serial policy".to_string(),
        )),
    }
}

#[doc(hidden)]
pub async fn cleanup_serial_resolution_candidates(
    db: &Database,
    storage: Arc<CloudSyncStorage>,
    branch_id: crate::PendingBranchId,
    plan: &SerialResolutionPlan,
) -> Result<(), super::store_pull::StorePullError> {
    let engine = load_store_engine(db, storage).await?;
    match engine.0 {
        StoreEngineKind::Serial(engine) => {
            engine.cleanup_resolution_candidates(branch_id, plan).await
        }
        StoreEngineKind::Merge(_) => Err(super::store_pull::StorePullError::Serial(
            "Serial candidate cleanup requires Serial policy".to_string(),
        )),
    }
}

async fn load_store_engine(
    db: &Database,
    storage: Arc<CloudSyncStorage>,
) -> Result<StoreEngine, StoreEngineLoadError> {
    let store_root = db
        .local_store_root_ref()
        .await
        .map_err(StoreEngineLoadError::Database)?
        .ok_or(StoreEngineLoadError::MissingRoot)?;
    let verified_root = super::store_objects::load_store_protocol_root(&*storage, &store_root)
        .await
        .map_err(StoreEngineLoadError::Object)?
        .value;
    StoreEngine::new(db.clone(), storage, store_root, &verified_root)
        .map_err(StoreEngineLoadError::Invalid)
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn stage_merge_acknowledgement_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    frontier: CommitFrontier,
    sync_time: String,
    identity: &UserKeypair,
) -> Result<super::store_commit::StoreAck, super::store_ack::StoreAckError> {
    let engine = StoreEngine::authorize_borrowed(storage, None, db)
        .await
        .map_err(|error| super::store_ack::StoreAckError::InvalidOutbound(error.to_string()))?;
    engine
        .stage_acknowledgement_for_test(frontier, sync_time, identity)
        .await
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn stage_serial_acknowledgement_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    frontier: CommitFrontier,
    sync_time: String,
    identity: &UserKeypair,
) -> Result<super::store_commit::StoreAck, super::store_ack::StoreAckError> {
    let engine = StoreEngine::authorize_borrowed(storage, Some(coordination), db)
        .await
        .map_err(|error| super::store_ack::StoreAckError::InvalidOutbound(error.to_string()))?;
    engine
        .stage_acknowledgement_for_test(frontier, sync_time, identity)
        .await
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn drain_merge_acknowledgements_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    identity: &UserKeypair,
) -> Result<u64, super::store_ack::StoreAckError> {
    let engine = StoreEngine::authorize_borrowed(storage, None, db)
        .await
        .map_err(|error| super::store_ack::StoreAckError::InvalidOutbound(error.to_string()))?;
    engine.drain_acknowledgements_for_test(identity).await
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) async fn drain_serial_acknowledgements_for_test(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    identity: &UserKeypair,
) -> Result<u64, super::store_ack::StoreAckError> {
    let engine = StoreEngine::authorize_borrowed(storage, Some(coordination), db)
        .await
        .map_err(|error| super::store_ack::StoreAckError::InvalidOutbound(error.to_string()))?;
    engine.drain_acknowledgements_for_test(identity).await
}

#[cfg(test)]
pub(crate) async fn prepare_merge_acknowledgement_activation_for_test(
    db: &Database,
    acknowledgement: super::store_commit::StoreAckRef,
    candidate: super::store_outbound::PreparedMergeStoreOperationCommit,
) -> Result<(), crate::database::DbError> {
    merge::prepare_acknowledgement_activation_for_test(db, acknowledgement, candidate).await
}

#[cfg(test)]
pub(crate) async fn prepare_serial_acknowledgement_activation_for_test(
    db: &Database,
    acknowledgement: super::store_commit::StoreAckRef,
    candidate: super::store_outbound::PreparedSerialStoreOperationCommit,
) -> Result<(), crate::database::DbError> {
    serial::prepare_acknowledgement_activation_for_test(db, acknowledgement, candidate).await
}

pub(crate) async fn verify_store_snapshot_stability(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<super::store_pull::VerifiedStoreSnapshotStability, super::store_pull::StorePullError> {
    match snapshot.meta.coverage.policy() {
        crate::WritePolicy::MergeConcurrent => {
            if serial_coordination.is_some() {
                return Err(super::store_pull::StorePullError::Database(
                    "Merge snapshot verification received Serial coordination".to_string(),
                ));
            }
            merge::pull::verify_snapshot_stability(storage, root, snapshot).await
        }
        crate::WritePolicy::Serial => {
            let coordination = serial_coordination.ok_or_else(|| {
                super::store_pull::StorePullError::Serial(
                    "Serial snapshot verification requires coordination capability".to_string(),
                )
            })?;
            serial::pull::verify_snapshot_stability(storage, coordination, root, snapshot).await
        }
    }
}

pub(crate) async fn verify_store_history_authority(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    cut: &super::store_commit::StoreHistoryCut,
    membership: &super::circle_control::StoreMembershipStateRef,
) -> Result<(), super::store_pull::StorePullError> {
    match cut {
        super::store_commit::StoreHistoryCut::MergeConcurrent(_) => {
            if serial_coordination.is_some() {
                return Err(super::store_pull::StorePullError::Database(
                    "Merge history verification received Serial coordination".to_string(),
                ));
            }
            merge::pull::verify_history_authority(storage, root, cut, membership).await
        }
        super::store_commit::StoreHistoryCut::Serial(_) => {
            let coordination = serial_coordination.ok_or_else(|| {
                super::store_pull::StorePullError::Serial(
                    "Serial history verification requires coordination capability".to_string(),
                )
            })?;
            serial::pull::verify_history_authority(storage, coordination, root, cut, membership)
                .await
        }
    }
}

#[cfg(test)]
pub(crate) async fn verify_merge_snapshot_for_acknowledgement_for_test(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(), super::store_pull::StorePullError> {
    merge::pull::verify_snapshot_for_acknowledgement(storage, root, snapshot).await
}

#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub fn pull_store_commits<'a>(
    db: &'a Database,
    tables: &'a [super::session::SyncedTable],
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    store_root_hash: super::store_commit::ObjectHash,
    store_dir: &'a StoreDir,
    membership: Option<&'a super::membership::MembershipChain>,
    identity: Option<&'a UserKeypair>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<StorePullResult, super::store_pull::StorePullError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        match db.write_policy() {
            crate::WritePolicy::MergeConcurrent => {
                let membership = membership.ok_or_else(|| {
                    super::store_pull::StorePullError::Database(
                        "Merge pull has no exact membership state".to_string(),
                    )
                })?;
                merge::pull::pull_store_commits(
                    db,
                    tables,
                    storage,
                    store_root_hash,
                    store_dir,
                    membership,
                    identity,
                )
                .await
            }
            crate::WritePolicy::Serial => {
                serial::pull::pull_store_commits(
                    db,
                    tables,
                    storage,
                    serial_coordination.ok_or_else(|| {
                        super::store_pull::StorePullError::Serial(
                            "coordination capability is absent".to_string(),
                        )
                    })?,
                    store_root_hash,
                    store_dir,
                    identity,
                )
                .await
            }
        }
    })
}

pub(crate) struct StoreEngine(StoreEngineKind);

enum StoreEngineKind {
    Merge(MergeStoreEngine),
    Serial(SerialStoreEngine),
}

struct StoreEngineContext {
    db: Database,
    storage: Arc<CloudSyncStorage>,
    store_root: StoreRootRef,
}

pub(crate) struct AuthorizedStoreEngine<'a>(AuthorizedStoreEngineKind<'a>);

enum AuthorizedStoreEngineKind<'a> {
    Merge(AuthorizedMergeStoreEngine<'a>),
    Serial(AuthorizedSerialStoreEngine<'a>),
}

pub(crate) struct VerifiedOwnerPromotionAcceptance(VerifiedOwnerPromotionAcceptanceKind);

pub(crate) enum DeviceJoinBootstrapAuthorization {
    MergeConcurrent {
        state: super::circle_control::StoreMembershipStateRef,
        chain: super::membership::MembershipChain,
    },
    Serial {
        state: super::circle_control::StoreMembershipStateRef,
        position: super::store_commit::SerialStorePosition,
        authorization: super::membership::SerialAuthorizationState,
    },
}

pub(crate) async fn load_device_join_authorization(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &super::circle_control::StoreMembershipStateRef,
) -> Result<DeviceJoinBootstrapAuthorization, super::store_pull::StorePullError> {
    match state {
        super::circle_control::StoreMembershipStateRef::MergeConcurrent(_) => {
            let chain = merge::pull::load_device_join_authorization(storage, root, state).await?;
            Ok(DeviceJoinBootstrapAuthorization::MergeConcurrent {
                state: state.clone(),
                chain,
            })
        }
        super::circle_control::StoreMembershipStateRef::Serial(_) => {
            let (position, authorization) =
                serial::pull::load_device_join_authorization(storage, root, state).await?;
            Ok(DeviceJoinBootstrapAuthorization::Serial {
                state: state.clone(),
                position,
                authorization,
            })
        }
    }
}

enum VerifiedOwnerPromotionAcceptanceKind {
    MergeConcurrent,
    Serial(serial::publication::SerialAuthorizationSnapshot),
}

impl VerifiedOwnerPromotionAcceptance {
    pub(crate) fn serial_snapshot(
        &self,
    ) -> Option<&serial::publication::SerialAuthorizationSnapshot> {
        match &self.0 {
            VerifiedOwnerPromotionAcceptanceKind::MergeConcurrent => None,
            VerifiedOwnerPromotionAcceptanceKind::Serial(snapshot) => Some(snapshot),
        }
    }
}

pub(crate) async fn find_owner_promotion_request_activation(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    request: &super::store_commit::OwnerPromotionRequest,
) -> Result<super::store_commit::OwnerPromotionRequestActivation, super::store_pull::StorePullError>
{
    match request.finalization {
        super::store_commit::OwnerPromotionFinalization::MergeConcurrent { .. } => {
            if serial_coordination.is_some() {
                return Err(super::store_pull::StorePullError::Database(
                    "Merge Owner-promotion discovery received Serial coordination".to_string(),
                ));
            }
            merge::pull::find_owner_promotion_request_activation(storage, root, request).await
        }
        super::store_commit::OwnerPromotionFinalization::Serial => {
            let coordination = serial_coordination.ok_or_else(|| {
                super::store_pull::StorePullError::Serial(
                    "Serial Owner-promotion discovery requires coordination".to_string(),
                )
            })?;
            serial::pull::find_owner_promotion_request_activation(
                storage,
                coordination,
                root,
                request,
            )
            .await
        }
    }
}

pub(crate) async fn verify_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<VerifiedOwnerPromotionAcceptance, super::store_pull::StorePullError> {
    match acceptance.activation {
        super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent { .. } => {
            if serial_coordination.is_some() {
                return Err(super::store_pull::StorePullError::Database(
                    "Merge Owner-promotion verification received Serial coordination".to_string(),
                ));
            }
            merge::pull::verify_owner_promotion_acceptance(storage, root, acceptance)
                .await
                .map(|()| {
                    VerifiedOwnerPromotionAcceptance(
                        VerifiedOwnerPromotionAcceptanceKind::MergeConcurrent,
                    )
                })
        }
        super::store_commit::OwnerPromotionRequestActivation::Serial { .. } => {
            let coordination = serial_coordination.ok_or_else(|| {
                super::store_pull::StorePullError::Serial(
                    "Serial Owner-promotion verification requires coordination".to_string(),
                )
            })?;
            serial::pull::verify_owner_promotion_acceptance(storage, coordination, root, acceptance)
                .await
                .map(|snapshot| {
                    VerifiedOwnerPromotionAcceptance(VerifiedOwnerPromotionAcceptanceKind::Serial(
                        snapshot,
                    ))
                })
        }
    }
}

pub(crate) struct PostPullStoreEngine<'cycle, 'engine>(PostPullStoreEngineKind<'cycle, 'engine>);

enum PostPullStoreEngineKind<'cycle, 'engine> {
    Merge(&'cycle AuthorizedMergeStoreEngine<'engine>),
    Serial {
        engine: &'cycle AuthorizedSerialStoreEngine<'engine>,
        membership: super::membership::SerialMembershipState,
    },
}

impl StoreEngine {
    pub(crate) fn new(
        db: Database,
        storage: Arc<CloudSyncStorage>,
        store_root: StoreRootRef,
        verified_root: &StoreProtocolRoot,
    ) -> Result<Self, String> {
        if store_root.store_root_hash != verified_root.object_hash() {
            return Err(
                "local Store root reference differs from the verified Store root".to_string(),
            );
        }
        let root_policy = verified_root.descriptor.write_policy;
        if root_policy != db.write_policy() {
            return Err(format!(
                "verified Store root write policy {root_policy:?} differs from local database write policy {:?}",
                db.write_policy()
            ));
        }
        let context = StoreEngineContext {
            db,
            storage,
            store_root,
        };
        match root_policy {
            crate::WritePolicy::MergeConcurrent => {
                Ok(Self(StoreEngineKind::Merge(MergeStoreEngine::new(context))))
            }
            crate::WritePolicy::Serial => {
                context
                    .storage
                    .serial_coordination()
                    .map_err(|error| format!("Serial coordination capability: {error}"))?;
                Ok(Self(StoreEngineKind::Serial(SerialStoreEngine::new(
                    context,
                ))))
            }
        }
    }

    pub(crate) fn database(&self) -> &Database {
        match &self.0 {
            StoreEngineKind::Merge(engine) => engine.db(),
            StoreEngineKind::Serial(engine) => engine.db(),
        }
    }

    pub(crate) fn cloud_storage(&self) -> &Arc<CloudSyncStorage> {
        match &self.0 {
            StoreEngineKind::Merge(engine) => engine.storage(),
            StoreEngineKind::Serial(engine) => engine.storage(),
        }
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.cloud_storage().blob_path_scheme()
    }

    pub(crate) fn self_uploader(&self) -> String {
        self.cloud_storage().self_uploader()
    }

    pub(crate) fn cloud_home(&self) -> &dyn CloudHome {
        self.cloud_storage().cloud_home()
    }

    pub(crate) async fn drain_uploads(
        &self,
        store_dir: &StoreDir,
        clock: &dyn crate::clock::Clock,
        hlc: &super::hlc::Hlc,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        observer: Option<&dyn crate::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, crate::database::DbError> {
        crate::blob::upload::drain_uploads(
            self.database(),
            &**self.cloud_storage(),
            store_dir,
            clock,
            hlc,
            routing_encryption,
            observer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: super::membership::MemberRole,
        encryption: &crate::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, super::membership_ops::MembershipOpsError> {
        match &self.0 {
            StoreEngineKind::Merge(engine) => {
                engine
                    .invite_member(
                        identity,
                        hlc,
                        public_key_hex,
                        invitee_email,
                        role,
                        encryption,
                        store_id,
                        store_name,
                    )
                    .await
            }
            StoreEngineKind::Serial(engine) => {
                engine
                    .invite_member(
                        identity,
                        hlc,
                        public_key_hex,
                        invitee_email,
                        role,
                        encryption,
                        store_id,
                        store_name,
                    )
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_member(
        &self,
        identity: &UserKeypair,
        hlc: &super::hlc::Hlc,
        public_key_hex: &str,
        encryption: &crate::encryption::EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
        cipher: &super::cloud_storage::CloudCipherState,
        pending_rotation: &super::cloud_storage::PendingRotation,
    ) -> Result<String, super::membership_ops::MembershipOpsError> {
        match &self.0 {
            StoreEngineKind::Merge(engine) => {
                engine
                    .remove_member(
                        identity,
                        hlc,
                        public_key_hex,
                        encryption,
                        custody,
                        cipher,
                        pending_rotation,
                    )
                    .await
            }
            StoreEngineKind::Serial(engine) => {
                engine
                    .remove_member(
                        identity,
                        hlc,
                        public_key_hex,
                        encryption,
                        custody,
                        cipher,
                        pending_rotation,
                    )
                    .await
            }
        }
    }

    pub(crate) async fn create_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<super::circle::CircleId, super::circle_ops::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(super::circle_ops::CircleOperationError::BrowsableStorage);
        }
        match &self.0 {
            StoreEngineKind::Merge(engine) => {
                engine
                    .create_circle(device_id, timestamp, name, identity)
                    .await
            }
            StoreEngineKind::Serial(engine) => {
                engine
                    .create_circle(device_id, timestamp, name, identity)
                    .await
            }
        }
    }

    pub(crate) async fn rename_circle(
        &self,
        device_id: &str,
        timestamp: &str,
        circle_id: super::circle::CircleId,
        name: &str,
        identity: &UserKeypair,
    ) -> Result<(), super::circle_ops::CircleOperationError> {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(super::circle_ops::CircleOperationError::BrowsableStorage);
        }
        match &self.0 {
            StoreEngineKind::Merge(engine) => {
                engine
                    .rename_circle(device_id, timestamp, circle_id, name, identity)
                    .await
            }
            StoreEngineKind::Serial(engine) => {
                engine
                    .rename_circle(device_id, timestamp, circle_id, name, identity)
                    .await
            }
        }
    }

    pub(crate) async fn propose_device_exclusion(
        &self,
        identity: &UserKeypair,
        target: &super::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        match &self.0 {
            StoreEngineKind::Merge(engine) => {
                engine.propose_device_exclusion(identity, target).await
            }
            StoreEngineKind::Serial(engine) => {
                engine.propose_device_exclusion(identity, target).await
            }
        }
    }

    pub(crate) async fn cancel_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        match &self.0 {
            StoreEngineKind::Merge(engine) => {
                engine.cancel_device_exclusion(identity, proposal).await
            }
            StoreEngineKind::Serial(engine) => {
                engine.cancel_device_exclusion(identity, proposal).await
            }
        }
    }

    pub(crate) async fn finalize_device_exclusion(
        &self,
        identity: &UserKeypair,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        match &self.0 {
            StoreEngineKind::Merge(engine) => {
                engine.finalize_device_exclusion(identity, proposal).await
            }
            StoreEngineKind::Serial(engine) => {
                engine.finalize_device_exclusion(identity, proposal).await
            }
        }
    }

    pub(crate) async fn device_exclusion_operations(
        &self,
    ) -> Result<
        Vec<super::store_device_exclusion::StoreDeviceExclusionOperationInfo>,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::get_device_exclusion_operations(self.database()).await
    }

    pub(crate) async fn resume_operations(
        &self,
        identity: &UserKeypair,
    ) -> Result<(), SyncCycleFailure> {
        match &self.0 {
            StoreEngineKind::Merge(engine) => engine.resume_operations(identity).await,
            StoreEngineKind::Serial(engine) => engine.resume_operations(identity).await,
        }
    }

    pub(crate) async fn authorize(&self) -> Result<AuthorizedStoreEngine<'_>, SyncCycleFailure> {
        match &self.0 {
            StoreEngineKind::Merge(engine) => engine.authorize().await.map(|authorized| {
                AuthorizedStoreEngine(AuthorizedStoreEngineKind::Merge(authorized))
            }),
            StoreEngineKind::Serial(engine) => engine.authorize().await.map(|authorized| {
                AuthorizedStoreEngine(AuthorizedStoreEngineKind::Serial(authorized))
            }),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn authorize_borrowed<'a>(
        storage: &'a dyn SyncStorage,
        coordination: Option<&'a dyn CoordinationStorage>,
        db: &'a Database,
    ) -> Result<AuthorizedStoreEngine<'a>, SyncCycleFailure> {
        let serial_coordination = match db.write_policy() {
            crate::WritePolicy::MergeConcurrent => None,
            crate::WritePolicy::Serial => Some(
                coordination
                    .ok_or_else(|| "Serial coordination capability is absent".to_string())?,
            ),
        };
        let store_root = db
            .local_store_root_ref()
            .await
            .map_err(|error| format!("read Store root reference: {error}"))?
            .ok_or_else(|| "Store root reference is absent".to_string())?;
        let verified_root = super::store_objects::load_store_protocol_root(storage, &store_root)
            .await
            .map_err(|error| SyncCycleFailure::operation("load Store protocol root", error))?
            .value;
        let root_policy = verified_root.descriptor.write_policy;
        if root_policy != db.write_policy() {
            return Err(format!(
                "verified Store root write policy {root_policy:?} differs from local database write policy {:?}",
                db.write_policy()
            )
            .into());
        }
        match (root_policy, serial_coordination) {
            (crate::WritePolicy::MergeConcurrent, None) => {
                merge::authorize_borrowed(db, storage, store_root)
                    .await
                    .map(|authorized| {
                        AuthorizedStoreEngine(AuthorizedStoreEngineKind::Merge(authorized))
                    })
            }
            (crate::WritePolicy::Serial, Some(coordination)) => {
                serial::authorize_borrowed(db, storage, coordination, store_root)
                    .await
                    .map(|authorized| {
                        AuthorizedStoreEngine(AuthorizedStoreEngineKind::Serial(authorized))
                    })
            }
            _ => Err("Store engine capability does not match its verified policy"
                .to_string()
                .into()),
        }
    }
}

impl StoreEngineContext {
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

impl<'engine> AuthorizedStoreEngine<'engine> {
    pub(crate) fn db(&self) -> &Database {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => engine.db(),
            AuthorizedStoreEngineKind::Serial(engine) => engine.db(),
        }
    }

    pub(crate) fn storage(&self) -> &dyn SyncStorage {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => engine.storage(),
            AuthorizedStoreEngineKind::Serial(engine) => engine.storage(),
        }
    }

    pub(crate) fn store_root(&self) -> &StoreRootRef {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => engine.store_root(),
            AuthorizedStoreEngineKind::Serial(engine) => engine.store_root(),
        }
    }

    pub(crate) fn wrapped_keys(
        &self,
        recipient: &str,
    ) -> Result<Vec<super::wrapped_store_key::WrappedStoreKeyRef>, SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => engine.wrapped_keys(recipient),
            AuthorizedStoreEngineKind::Serial(engine) => engine.wrapped_keys(recipient),
        }
    }

    pub(crate) async fn gc_tombstones(
        &self,
        cloud_home: &dyn CloudHome,
        cipher: &dyn CloudCipherAccess,
        store_id: &str,
        self_pubkey: &str,
        clock: &dyn crate::clock::Clock,
        grace: chrono::Duration,
    ) -> Result<usize, String> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                engine
                    .gc_tombstones(cloud_home, cipher, store_id, self_pubkey, clock, grace)
                    .await
            }
            AuthorizedStoreEngineKind::Serial(engine) => {
                engine
                    .gc_tombstones(cloud_home, cipher, store_id, self_pubkey, clock, grace)
                    .await
            }
        }
    }

    pub(crate) async fn drain_store_writes(
        &self,
    ) -> Result<u64, super::store_outbound::StoreOutboundError> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => engine.drain_store_writes().await,
            AuthorizedStoreEngineKind::Serial(engine) => engine.drain_store_writes().await,
        }
    }

    pub(crate) async fn pull(
        &self,
        store_dir: &StoreDir,
        identity: &UserKeypair,
    ) -> Result<StorePullResult, SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => engine.pull(store_dir, identity).await,
            AuthorizedStoreEngineKind::Serial(engine) => engine.pull(store_dir, identity).await,
        }
    }

    pub(crate) async fn snapshot_position(
        &self,
        snapshot: &crate::database::PublishedStoreSnapshot,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<u64, SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                engine
                    .snapshot_position(snapshot, device_id, identity)
                    .await
            }
            AuthorizedStoreEngineKind::Serial(engine) => engine.snapshot_position(snapshot).await,
        }
    }

    pub(crate) async fn should_stop_before_pull(&self) -> Result<bool, SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => engine.should_stop_before_pull().await,
            AuthorizedStoreEngineKind::Serial(engine) => engine.should_stop_before_pull().await,
        }
    }

    pub(crate) async fn after_pull(
        &self,
    ) -> Result<PostPullStoreEngine<'_, 'engine>, SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                Ok(PostPullStoreEngine(PostPullStoreEngineKind::Merge(engine)))
            }
            AuthorizedStoreEngineKind::Serial(engine) => {
                Ok(PostPullStoreEngine(PostPullStoreEngineKind::Serial {
                    engine,
                    membership: engine.required_membership().await?,
                }))
            }
        }
    }

    pub(crate) async fn ensure_active_registration(&self) -> Result<(), SyncCycleFailure> {
        super::store_registration::ensure_active_registration(self.db(), self.storage())
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("publish Store device registration", error)
            })
    }

    pub(crate) async fn prepare_pending_store_write(
        &self,
        device_id: &str,
        timestamp: &str,
        identity: &UserKeypair,
        store_dir: &StoreDir,
    ) -> Result<bool, SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                engine
                    .prepare_pending_store_write(device_id, timestamp, identity, store_dir)
                    .await
            }
            AuthorizedStoreEngineKind::Serial(engine) => {
                engine
                    .prepare_pending_store_write(device_id, identity, store_dir)
                    .await
            }
        }
    }

    pub(crate) async fn push_snapshot(
        &self,
        snapshot: super::snapshot::CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        identity: &UserKeypair,
        created_at: String,
    ) -> Result<super::store_commit::SnapshotMeta, SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                engine
                    .push_snapshot(snapshot, coverage, schema_version, identity, created_at)
                    .await
            }
            AuthorizedStoreEngineKind::Serial(engine) => {
                engine
                    .push_snapshot(snapshot, coverage, schema_version, identity, created_at)
                    .await
            }
        }
    }

    pub(crate) async fn stage_and_publish_ack(
        &self,
        identity: &UserKeypair,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                engine.stage_and_publish_ack(identity, sync_time).await
            }
            AuthorizedStoreEngineKind::Serial(engine) => {
                engine.stage_and_publish_ack(identity, sync_time).await
            }
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn stage_acknowledgement_for_test(
        &self,
        frontier: CommitFrontier,
        sync_time: String,
        identity: &UserKeypair,
    ) -> Result<super::store_commit::StoreAck, super::store_ack::StoreAckError> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                engine
                    .stage_acknowledgement(frontier, sync_time, identity)
                    .await
            }
            AuthorizedStoreEngineKind::Serial(engine) => {
                engine
                    .stage_acknowledgement(frontier, sync_time, identity)
                    .await
            }
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn drain_acknowledgements_for_test(
        &self,
        identity: &UserKeypair,
    ) -> Result<u64, super::store_ack::StoreAckError> {
        match &self.0 {
            AuthorizedStoreEngineKind::Merge(engine) => {
                Box::pin(engine.drain_acknowledgements(identity)).await
            }
            AuthorizedStoreEngineKind::Serial(engine) => {
                Box::pin(engine.drain_acknowledgements(identity)).await
            }
        }
    }
}

impl PostPullStoreEngine<'_, '_> {
    pub(crate) fn may_author_snapshot(&self, author_pubkey: &str) -> Result<(), String> {
        match &self.0 {
            PostPullStoreEngineKind::Merge(engine) => engine.may_author_snapshot(author_pubkey),
            PostPullStoreEngineKind::Serial { engine, membership } => {
                engine.may_author_snapshot(membership, author_pubkey)
            }
        }
    }

    pub(crate) async fn reclaim_packages(
        &self,
        device_id: &str,
        identity: &UserKeypair,
    ) -> Result<super::store_reclaim::StoreReclaimResult, super::store_reclaim::StoreReclaimError>
    {
        match &self.0 {
            PostPullStoreEngineKind::Merge(engine) => {
                engine.reclaim_packages(device_id, identity).await
            }
            PostPullStoreEngineKind::Serial { engine, membership } => {
                engine
                    .reclaim_packages(membership, device_id, identity)
                    .await
            }
        }
    }
}

fn require_snapshot_policy(
    snapshot: &crate::database::PublishedStoreSnapshot,
    policy: crate::WritePolicy,
) -> Result<(), SyncCycleFailure> {
    if snapshot.meta.coverage.policy() == policy {
        Ok(())
    } else {
        Err(
            "latest local Store snapshot coverage has the wrong write policy"
                .to_string()
                .into(),
        )
    }
}

fn snapshot_position_for_stream(
    snapshot: &crate::database::PublishedStoreSnapshot,
    policy: crate::WritePolicy,
    stream_id: &str,
) -> Result<u64, SyncCycleFailure> {
    require_snapshot_policy(snapshot, policy)?;
    Ok(snapshot
        .meta
        .coverage
        .clone()
        .into_refs()
        .remove(stream_id)
        // Missing local-stream coverage is an exact genesis position.
        .map(|reference| reference.coord.sequence())
        .unwrap_or(0))
}
