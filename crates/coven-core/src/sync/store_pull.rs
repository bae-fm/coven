//! Causal discovery and atomic materialization for immutable Store commits.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rusqlite::session::{ConflictAction, ConflictType};
use tracing::debug;

use super::apply::{
    apply_changeset_strict_on, resolve_and_apply_changeset_with_policy_on, ValidatedChangeset,
};
use super::audience_package::{AudiencePackage, PackageAudience};
use super::circle_activation::{
    CircleMembershipAuthority, VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
use super::circle_control::StoreMembershipStateRef;
use super::conflict::{IncomingTimestampPolicy, TableSchema};
use super::membership::{MembershipChain, MembershipStatus, SerialAuthorizationState};
use super::pull::{
    advance_max_updated_at, cache_eager_blobs, local_blob_cleanup_intents, verify_package_blobs,
};
use super::session::SyncedTable;
use super::storage::{
    BlobSpoolProtection, CoordinationError, CoordinationStorage, ExactObjectRef,
    ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use super::store_commit::{
    head_slot_prefix, serial_head_key, ActivatedStoreDeviceRegistrationRef, CirclePackageRef,
    CommitFrontier, DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinOutcomeBody,
    DeviceStreamAnchor, ObjectHash, OpenedRetainedMergeHistorySummary, OwnerRecoveryCursor,
    OwnerRecoveryNode, OwnerRecoveryNodeRef, OwnerRecoveryPosition, ResolvedStoreDeviceState,
    RetainedStoreDeviceExclusionOutcome, RetainedStoreDeviceExclusionProposal,
    RetainedStoreDeviceOperations, RetainedVerifiedMergeHistorySummary,
    RetainedVerifiedRegistration, StoreBatchCommit, StoreBatchCommitRef, StoreCommitAnchor,
    StoreCommitCoord, StoreDeviceExclusionOutcome, StoreDeviceExclusionProof, StoreDeviceHead,
    StoreDeviceProposalAck, StoreDeviceProposalState, StoreDeviceRegistration,
    StoreDeviceRegistrationActivation, StoreDeviceRegistrationActivationRef,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    StoreDeviceStatus, StoreHistoryCut, StoreProtocolError, StoreRootRef, StoreSerialHead,
    StoreSerialHeadState, StoreSerialPredecessor, VerifiedStoreDeviceOperations, SERIAL_STREAM_ID,
};
use super::store_objects::{
    load_circle_package, load_commit_ref, load_device_exclusion_outcome_ref,
    load_device_exclusion_proposal_ref, load_device_join_outcome_ref, load_founder_registration,
    load_founder_registration_with_root, load_owner_recovery_node_ref,
    load_owner_signed_device_join_attempt_ref, load_reclaim_authorization_ref,
    load_reclaim_receipt_ref, load_registration_ref, load_registration_ref_with_root,
    load_store_ack_predecessor, load_store_ack_ref, load_store_package, load_store_protocol_root,
    run_blocking_object_verification, StoreObjectError, VerifiedObject,
};
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::{self, LocalBlobCleanupIntent};
use crate::changeset::RowChange;
use crate::database::{BlobActivation, Database, DbError, VerifiedMergeMaterialization};
use crate::encryption::{EncryptionService, KeyFingerprint, MasterKeyring};
use crate::store_dir::StoreDir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStorePositionReason {
    MissingCommit,
    MissingPackage,
    MissingDeviceRegistration {
        device_id: String,
        revision: u64,
        registration_hash: ObjectHash,
    },
    MissingPredecessor(StoreBatchCommitRef),
    MissingDependency {
        device_id: String,
        commit: StoreBatchCommitRef,
    },
    NewerSchema {
        local: u32,
        required: u32,
    },
    Unauthorized,
    DeviceExclusionFreeze {
        proposal: super::store_commit::StoreDeviceExclusionProposalRef,
        target_cut: StoreHistoryCut,
    },
    InactiveDevice {
        terminals: Vec<super::store_commit::StoreDeviceTerminalRef>,
        accepted_cut: StoreHistoryCut,
    },
    InvalidChangeset(String),
    InvalidRowIdentity {
        table: String,
        reason: String,
    },
    BlobDownloadFailed,
    ForeignKeyDependency,
    ConstraintConflict(Vec<String>),
    HashMismatch {
        referenced_device_id: String,
        referenced_commit: StoreBatchCommitRef,
        materialized_hash: ObjectHash,
    },
    InvalidSignature,
    WrongSlot(String),
    ObjectCollision(String),
    ObjectUnreadable {
        key: String,
        detail: String,
    },
    InvalidObject(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStoreCoordinate {
    Head {
        device_id: String,
        seq: u64,
        head_hash: ObjectHash,
    },
    Commit {
        device_id: String,
        commit: StoreBatchCommitRef,
    },
    Package {
        device_id: String,
        seq: u64,
        package_hash: ObjectHash,
    },
    Dependency {
        dependent_device_id: String,
        dependent_commit: StoreBatchCommitRef,
        required_device_id: String,
        required_commit: StoreBatchCommitRef,
    },
}

impl HeldStoreCoordinate {
    pub fn device_id(&self) -> &str {
        match self {
            Self::Head { device_id, .. }
            | Self::Commit { device_id, .. }
            | Self::Package { device_id, .. } => device_id,
            Self::Dependency {
                dependent_device_id,
                ..
            } => dependent_device_id,
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            Self::Head { seq, .. } | Self::Package { seq, .. } => *seq,
            Self::Commit { commit, .. } => commit.coord.sequence(),
            Self::Dependency {
                dependent_commit, ..
            } => dependent_commit.coord.sequence(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldStorePosition {
    pub coordinate: HeldStoreCoordinate,
    pub reason: HeldStorePositionReason,
}

#[derive(Debug)]
pub struct StorePullResult {
    pub changesets_applied: u64,
    pub devices_pulled: u64,
    pub held_positions: Vec<HeldStorePosition>,
    pub visible_heads: Vec<VerifiedStoreDeviceHead>,
    pub serial_head: Option<StoreSerialHead>,
    pub row_changes: Vec<RowChange>,
    pub asset_downloads_failed: bool,
    pub local_blob_cleanup_pending: bool,
    pub frontier: BTreeMap<String, StoreBatchCommitRef>,
}

#[derive(Debug, Clone)]
pub struct VerifiedStoreDeviceHead {
    pub head: StoreDeviceHead,
    pub author: StoreDeviceRegistration,
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullError {
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("database: {0}")]
    Database(String),
    #[error("active Store device {device_id} for member {member:?} has no activated acknowledgement for the selected snapshot")]
    SnapshotNotStable { member: String, device_id: String },
    #[error("Store snapshot author is inactive in its exact covered device state")]
    SnapshotAuthorInactive,
    #[error("Store snapshot author is not an Owner in its exact membership state")]
    SnapshotAuthorNotOwner,
    #[error("membership: {0}")]
    Membership(#[source] StorePullMembershipError),
    #[error("Serial Store: {0}")]
    Serial(String),
    #[error("Serial coordination: {0}")]
    Coordination(#[source] CoordinationError),
    #[error("{0}")]
    BlobDownloads(#[source] super::pull::BlobDownloadFailures),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullMembershipError {
    #[error("{0}")]
    Object(#[source] StoreObjectError),
    #[error("{0}")]
    Chain(#[source] super::membership_ops::AnchoredChainError),
    #[error("{0}")]
    Message(String),
}

type StorePullFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StorePullError>> + Send + 'a>>;

impl From<DbError> for StorePullError {
    fn from(error: DbError) -> Self {
        Self::Database(error.into_message())
    }
}

#[derive(Clone)]
struct Candidate {
    commit_ref: StoreBatchCommitRef,
    commit: StoreBatchCommit,
    author: StoreDeviceRegistration,
    package: Option<Vec<u8>>,
    registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    device_operations: CandidateDeviceOperations,
}

#[derive(Clone)]
struct MergeCandidate {
    candidate: Candidate,
    activation_head: StoreDeviceHead,
    activation_head_object: ExactObjectRef,
    predecessor_membership: MembershipChain,
}

struct LoadedMergePredecessorMemberships {
    by_commit: BTreeMap<StoreBatchCommitRef, MembershipChain>,
}

impl LoadedMergePredecessorMemberships {
    fn membership_for(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<&MembershipChain, StorePullError> {
        self.by_commit.get(reference).ok_or_else(|| {
            StorePullError::Database(format!(
                "retained Merge commit {reference:?} has no loaded predecessor membership"
            ))
        })
    }
}

struct SerialApplicationCandidate {
    candidate: Candidate,
    membership_authority: SerialAuthorizationState,
    authorization_after: SerialAuthorizationState,
}

pub(crate) struct VerifiedAuthorExclusionActivation {
    store_root_hash: ObjectHash,
    target: StoreDeviceRegistrationRef,
    target_registration: StoreDeviceRegistration,
    exclusion: super::store_commit::StoreDeviceExclusionRef,
    accepted_cut: BTreeMap<super::causal_grants::AuthorStreamId, StoreBatchCommitRef>,
    activation_head: super::store_commit::StoreDeviceHeadRef,
    candidate: StoreBatchCommitRef,
    candidate_head: super::remote_object::VerifiedCandidateHead,
}

impl VerifiedAuthorExclusionActivation {
    pub(crate) fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub(crate) fn target(&self) -> &StoreDeviceRegistrationRef {
        &self.target
    }

    pub(crate) fn target_registration(&self) -> &StoreDeviceRegistration {
        &self.target_registration
    }

    pub(crate) fn exclusion(&self) -> &super::store_commit::StoreDeviceExclusionRef {
        &self.exclusion
    }

    pub(crate) fn accepted_cut(
        &self,
    ) -> &BTreeMap<super::causal_grants::AuthorStreamId, StoreBatchCommitRef> {
        &self.accepted_cut
    }

    pub(crate) fn activation_head(&self) -> &super::store_commit::StoreDeviceHeadRef {
        &self.activation_head
    }

    pub(crate) fn candidate(&self) -> &StoreBatchCommitRef {
        &self.candidate
    }

    pub(crate) fn candidate_head(&self) -> &super::remote_object::VerifiedCandidateHead {
        &self.candidate_head
    }
}

pub(crate) struct VerifiedMembershipGrantRevocationActivation {
    store_root_hash: ObjectHash,
    grant_id: super::membership::MembershipGrantId,
    membership: super::circle_control::MergeStoreMembershipStateRef,
    activation_commit: StoreBatchCommitRef,
    activation_head: super::store_commit::StoreDeviceHeadRef,
    candidate: StoreBatchCommitRef,
    candidate_author: StoreDeviceRegistration,
    candidate_head: super::remote_object::VerifiedCandidateHead,
}

impl VerifiedMembershipGrantRevocationActivation {
    pub(crate) fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub(crate) fn grant_id(&self) -> &super::membership::MembershipGrantId {
        &self.grant_id
    }

    pub(crate) fn membership(&self) -> &super::circle_control::MergeStoreMembershipStateRef {
        &self.membership
    }

    pub(crate) fn activation_head(&self) -> &super::store_commit::StoreDeviceHeadRef {
        &self.activation_head
    }

    pub(crate) fn activation_commit(&self) -> &StoreBatchCommitRef {
        &self.activation_commit
    }

    pub(crate) fn candidate(&self) -> &StoreBatchCommitRef {
        &self.candidate
    }

    pub(crate) fn candidate_author(&self) -> &StoreDeviceRegistration {
        &self.candidate_author
    }

    pub(crate) fn candidate_head(&self) -> &super::remote_object::VerifiedCandidateHead {
        &self.candidate_head
    }
}

pub(crate) async fn find_owner_promotion_request_activation(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    request: &super::store_commit::OwnerPromotionRequest,
) -> Result<super::store_commit::OwnerPromotionRequestActivation, StorePullError> {
    let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
    request
        .verify(root, &promoter.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    match &request.finalization {
        super::store_commit::OwnerPromotionFinalization::MergeConcurrent { .. } => {
            let discovered = discover_merge_stream(
                storage,
                root,
                &request.promoter_registration,
                &promoter.value,
                None,
            )
            .await?;
            let mut matches =
                discovered
                    .commits
                    .into_iter()
                    .filter_map(|(head_ref, _, commit_ref, commit)| {
                        (commit.owner_promotion_request() == Some(request))
                            .then_some((commit_ref, head_ref))
                    });
            let Some((commit, head)) = matches.next() else {
                return Err(StorePullError::Database(
                    "Owner-promotion request has no accepted Merge activation".to_string(),
                ));
            };
            if matches.next().is_some() {
                return Err(StorePullError::Database(
                    "Owner-promotion request has more than one Merge activation".to_string(),
                ));
            }
            Ok(
                super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent {
                    commit,
                    head,
                },
            )
        }
        super::store_commit::OwnerPromotionFinalization::Serial => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Owner-promotion request discovery requires coordination".to_string(),
                )
            })?;
            let head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &head.head).await?;
            let mut matches = accepted
                .into_iter()
                .filter(|candidate| candidate.commit.owner_promotion_request() == Some(request));
            let Some(accepted) = matches.next() else {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has no accepted Serial activation".to_string(),
                ));
            };
            if matches.next().is_some() {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has more than one Serial activation".to_string(),
                ));
            }
            Ok(
                super::store_commit::OwnerPromotionRequestActivation::Serial {
                    commit: accepted.commit_ref,
                },
            )
        }
    }
}

pub(crate) async fn verify_merge_device_state_ref(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreDeviceStateRef,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let StoreDeviceStateRef::MergeConcurrent { frontier, .. } = reference else {
        return Err(StorePullError::Database(
            "Merge authority carries Serial device state".to_string(),
        ));
    };
    let CommitFrontier::MergeConcurrent(frontier) = frontier else {
        return Err(StorePullError::Database(
            "Merge device state carries Serial frontier".to_string(),
        ));
    };
    let history = verify_merge_history_refs(
        storage,
        root,
        frontier.values().cloned().collect::<Vec<_>>(),
    )
    .await?;
    let state = if frontier.is_empty() {
        history.genesis
    } else {
        ResolvedStoreDeviceState::merge(
            frontier
                .values()
                .map(|commit| {
                    history
                        .commits
                        .get(commit)
                        .map(|verified| verified.state_after.clone())
                        .ok_or_else(|| {
                            StorePullError::Database(
                                "Merge device-state frontier is absent from its verified history"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let expected = StoreDeviceStateRef::merge_concurrent(
        CommitFrontier::MergeConcurrent(frontier.clone()),
        &state,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if &expected != reference {
        return Err(StorePullError::Database(
            "Merge device-state reference differs from its verified history".to_string(),
        ));
    }
    Ok(state)
}

pub(crate) async fn verify_merge_owner_conflict_acceptance(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerConflictResolutionAcceptance,
    resolver_pubkey: &str,
) -> Result<(), StorePullError> {
    let registration = load_registration_ref(storage, root, &acceptance.owner_registration).await?;
    acceptance
        .verify(&registration.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let state = verify_merge_device_state_ref(storage, root, &acceptance.device_state).await?;
    if !device_state_has_active_registration(&state, &acceptance.owner_registration) {
        return Err(StorePullError::Database(
            "conflict-resolution Owner registration is not active at its exact device state"
                .to_string(),
        ));
    }
    verify_canonical_owner_registration(
        storage,
        root,
        &state,
        resolver_pubkey,
        &acceptance.owner_registration,
    )
    .await?;
    Ok(())
}

#[derive(Clone)]
struct LoadedCirclePackage {
    reference: CirclePackageRef,
    bytes: Vec<u8>,
    blob_protection: BlobSpoolProtection,
}

#[derive(Clone)]
struct CirclePackageAccess {
    encryption: EncryptionService,
    key_fingerprint: KeyFingerprint,
    writers: BTreeSet<String>,
}

type CirclePackageAccesses =
    BTreeMap<(super::circle::CircleId, super::circle::CircleControlCoord), CirclePackageAccess>;

#[derive(Clone)]
enum CandidateDeviceOperations {
    Verified(VerifiedStoreDeviceOperations),
    MergePending {
        predecessor_membership: MembershipChain,
    },
}

fn parse_candidate_store_package(
    candidate: &Candidate,
    bytes: &[u8],
) -> Result<AudiencePackage, String> {
    let package = AudiencePackage::parse(bytes)
        .map_err(|error| format!("invalid Store audience package: {error}"))?;
    if !matches!(package.audience(), PackageAudience::Store)
        || package.store_root_hash() != candidate.commit.store_root_hash
        || package.write_id() != &candidate.commit.write_id
        || package.commit_coord() != &candidate.commit_ref.coord
        || package.candidate_family() != candidate.commit.candidate_family()
        || candidate
            .commit
            .store_package()
            .as_ref()
            .is_none_or(|reference| package.schema_version() != reference.schema_version)
    {
        return Err("Store audience package differs from its exact commit".to_string());
    }
    Ok(package)
}

fn parse_candidate_circle_package(
    candidate: &Candidate,
    loaded: &LoadedCirclePackage,
) -> Result<AudiencePackage, String> {
    let package = AudiencePackage::parse(&loaded.bytes)
        .map_err(|error| format!("invalid Circle audience package: {error}"))?;
    let expected = &loaded.reference;
    if !matches!(
        package.audience(),
        PackageAudience::Circle {
            circle_id,
            control,
            key_fingerprint,
        } if *circle_id == expected.circle_id
            && control == &expected.control
            && *key_fingerprint == expected.key_fingerprint
    ) || package.store_root_hash() != candidate.commit.store_root_hash
        || package.write_id() != &candidate.commit.write_id
        || package.commit_coord() != &candidate.commit_ref.coord
        || package.candidate_family() != candidate.commit.candidate_family()
        || package.schema_version() != expected.package.schema_version
    {
        return Err("Circle audience package differs from its exact commit".to_string());
    }
    package
        .validate_blob_uploader(&candidate.commit.author_registration)
        .map_err(|error| format!("invalid Circle blob authority: {error}"))?;
    Ok(package)
}

struct AuthorizedSerialCommit {
    commit_ref: StoreBatchCommitRef,
    commit: StoreBatchCommit,
    author: StoreDeviceRegistration,
    registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    device_operations: VerifiedStoreDeviceOperations,
    device_state_before: ResolvedStoreDeviceState,
    device_state_after: ResolvedStoreDeviceState,
    acknowledgement: Option<(
        super::store_commit::StoreAckRef,
        super::store_commit::StoreAck,
    )>,
    authorization_before: SerialAuthorizationState,
    authorization_after: SerialAuthorizationState,
}

pub(crate) enum RegistrationLoadError {
    Object(StoreObjectError),
    Invalid(String),
}

enum VerifiedAcceptedPredecessor<'a> {
    Exact,
    SerialHistory {
        commits: &'a [AuthorizedSerialCommit],
    },
    MergeHistory {
        commits: &'a BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
        frontier: Vec<StoreBatchCommitRef>,
    },
}

struct VerifiedCommitJoinOutcome {
    attempt: DeviceJoinAttempt,
    owner: StoreDeviceRegistration,
    outcome: super::store_commit::DeviceJoinOutcome,
}

fn registration_attempt_error(error: StorePullError) -> RegistrationLoadError {
    match error {
        StorePullError::Object(error) => RegistrationLoadError::Object(error),
        StorePullError::Storage(error) => {
            RegistrationLoadError::Object(StoreObjectError::Storage(error))
        }
        error => RegistrationLoadError::Invalid(error.to_string()),
    }
}

enum RegistrationPredecessorAuthority<'a> {
    MergeConcurrent(&'a MembershipChain),
    Serial {
        authorization: &'a SerialAuthorizationState,
        position: super::store_commit::SerialStorePosition,
        history: SerialAuthorizationHistory<'a>,
    },
}

enum SerialAuthorizationHistory<'a> {
    ExactPredecessor,
    Prefix {
        genesis_position: &'a super::store_commit::SerialStorePosition,
        genesis_authorization: &'a SerialAuthorizationState,
        commits: &'a [AuthorizedSerialCommit],
    },
}

impl RegistrationPredecessorAuthority<'_> {
    fn provider_admin_state(&self) -> Option<&super::provider::ProviderAdminState> {
        match self {
            Self::MergeConcurrent(chain) => {
                let super::membership::MembershipStatus::Resolved(resolved) = chain.status() else {
                    return None;
                };
                Some(resolved.provider_admin.combined_state())
            }
            Self::Serial { authorization, .. } => Some(&authorization.provider_admin),
        }
    }

    fn verifies_owner(
        &self,
        membership: &StoreMembershipStateRef,
        owner_pubkey: &str,
        owner_grant: &super::membership::MembershipGrantId,
    ) -> bool {
        match self {
            Self::MergeConcurrent(chain) => {
                let MembershipStatus::Resolved(resolved) = chain.status() else {
                    return false;
                };
                StoreMembershipStateRef::merge_concurrent(
                    chain.head_refs().to_vec(),
                    chain.resolution_refs().to_vec(),
                    membership.recovery().to_vec(),
                    resolved.state_hash,
                )
                .is_ok_and(|expected| membership == &expected)
                    && chain.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
            }
            Self::Serial {
                authorization,
                position,
                ..
            } => {
                StoreMembershipStateRef::serial(
                    position.clone(),
                    membership.recovery().to_vec(),
                    authorization,
                )
                .is_ok_and(|expected| membership == &expected)
                    && authorization
                        .membership
                        .authorizes_owner_grant_id(owner_pubkey, owner_grant)
            }
        }
    }

    fn verifies_owner_at_ancestor(
        &self,
        membership: &StoreMembershipStateRef,
        owner_pubkey: &str,
        owner_grant: &super::membership::MembershipGrantId,
    ) -> bool {
        if self.verifies_owner(membership, owner_pubkey, owner_grant) {
            return true;
        }
        let Self::Serial {
            history:
                SerialAuthorizationHistory::Prefix {
                    genesis_position,
                    genesis_authorization,
                    commits,
                },
            ..
        } = self
        else {
            return false;
        };
        let StoreMembershipStateRef::Serial(state) = membership else {
            return false;
        };
        let historical_authorization = match &state.position {
            super::store_commit::SerialStorePosition::Genesis { .. }
                if &state.position == *genesis_position =>
            {
                *genesis_authorization
            }
            super::store_commit::SerialStorePosition::Commit(reference) => {
                let Some(accepted) = commits
                    .iter()
                    .find(|accepted| &accepted.commit_ref == reference)
                else {
                    return false;
                };
                &accepted.authorization_after
            }
            _ => return false,
        };
        StoreMembershipStateRef::serial(
            state.position.clone(),
            state.recovery.clone(),
            historical_authorization,
        )
        .is_ok_and(|expected| membership == &expected)
            && historical_authorization
                .membership
                .authorizes_owner_grant_id(owner_pubkey, owner_grant)
    }

    fn verifies_active_owner(&self, owner_pubkey: &str) -> bool {
        match self {
            Self::MergeConcurrent(chain) => chain.is_owner_now(owner_pubkey),
            Self::Serial { authorization, .. } => authorization.membership.is_owner(owner_pubkey),
        }
    }

    fn verifies_provider_administrator(
        &self,
        grant_id: &super::provider::ProviderAdminGrantId,
        executor: &StoreDeviceRegistrationRef,
        expected: &super::provider::ProviderAdminGrantRecord,
    ) -> bool {
        let Some(state) = self.provider_admin_state() else {
            return false;
        };
        state.authorizes(grant_id, executor)
            && state
                .records()
                .get(grant_id)
                .is_some_and(|record| record == expected)
    }

    fn verifies_provider_administrator_grant(
        &self,
        grant_id: &super::provider::ProviderAdminGrantId,
        executor: &StoreDeviceRegistrationRef,
    ) -> bool {
        self.provider_admin_state()
            .is_some_and(|state| state.authorizes(grant_id, executor))
    }
}

async fn load_merge_predecessor_membership(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
) -> Result<MembershipChain, RegistrationLoadError> {
    load_merge_predecessor_membership_impl(storage, root, state, None, None).await
}

async fn load_merge_predecessor_membership_with_verified_activations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
    verified_activations: &VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, RegistrationLoadError> {
    load_merge_predecessor_membership_impl(
        storage,
        root,
        state,
        Some(verified_activations),
        pending_resolution,
    )
    .await
}

async fn load_merge_predecessor_membership_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
    verified_activations: Option<&VerifiedMergeMembershipPrefix>,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, RegistrationLoadError> {
    let StoreMembershipStateRef::MergeConcurrent(state) = state else {
        return Err(RegistrationLoadError::Invalid(
            "Merge registration lifecycle commit carries Serial membership state".to_string(),
        ));
    };
    let root_value = load_store_protocol_root(storage, root)
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    let membership = match verified_activations {
        Some(verified_activations) => Box::pin(
            super::membership_ops::load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
                storage,
                root,
                &root_value,
                &root_value.descriptor.founder_pubkey,
                &state.heads,
                &state.resolutions,
                verified_activations,
                pending_resolution,
            ),
        )
        .await,
        None => Box::pin(
            super::membership_ops::load_anchored_chain_at_exact_heads_with_root(
                storage,
                root,
                &root_value,
                &root_value.descriptor.founder_pubkey,
                &state.heads,
                &state.resolutions,
            ),
        )
        .await,
    }
    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    Ok(membership)
}

fn verify_merge_membership_state_ref(
    state: &StoreMembershipStateRef,
    membership: &MembershipChain,
    device_state: &ResolvedStoreDeviceState,
) -> Result<(), StorePullError> {
    let MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StorePullError::Database(
            "Store history membership state is conflicted".to_string(),
        ));
    };
    let expected = StoreMembershipStateRef::merge_concurrent(
        membership.head_refs().to_vec(),
        membership.resolution_refs().to_vec(),
        device_state.recovery.clone(),
        resolved.state_hash,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if &expected != state {
        return Err(StorePullError::Database(
            "Store history membership reference differs from its exact resolved state".to_string(),
        ));
    }
    Ok(())
}

pub(crate) enum DeviceJoinBootstrapAuthorization {
    MergeConcurrent {
        state: StoreMembershipStateRef,
        chain: MembershipChain,
    },
    Serial {
        state: StoreMembershipStateRef,
        position: super::store_commit::SerialStorePosition,
        authorization: SerialAuthorizationState,
    },
}

pub(crate) fn load_device_join_authorization<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    state: &'a StoreMembershipStateRef,
) -> StorePullFuture<'a, DeviceJoinBootstrapAuthorization> {
    Box::pin(async move {
        match state {
            StoreMembershipStateRef::MergeConcurrent(_) => {
                let chain = Box::pin(load_merge_predecessor_membership(storage, root, state))
                    .await
                    .map_err(|error| match error {
                        RegistrationLoadError::Object(error) => StorePullError::Object(error),
                        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                    })?;
                Ok(DeviceJoinBootstrapAuthorization::MergeConcurrent {
                    state: state.clone(),
                    chain,
                })
            }
            StoreMembershipStateRef::Serial(state_ref) => {
                let reference = match &state_ref.position {
                    super::store_commit::SerialStorePosition::Genesis { .. } => None,
                    super::store_commit::SerialStorePosition::Commit(reference) => {
                        Some(reference.clone())
                    }
                };
                let authorization = Box::pin(load_serial_authorization_at_position(
                    storage, root, reference,
                ))
                .await?;
                let expected = StoreMembershipStateRef::serial(
                    state_ref.position.clone(),
                    state_ref.recovery.clone(),
                    &authorization,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
                if &expected != state {
                    return Err(StorePullError::Database(
                        "Serial device join membership state differs from its exact authorization"
                            .to_string(),
                    ));
                }
                Ok(DeviceJoinBootstrapAuthorization::Serial {
                    state: expected,
                    position: state_ref.position.clone(),
                    authorization,
                })
            }
        }
    })
}

pub(crate) async fn verify_device_join_cleanup_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activation: &super::device_join::DeviceJoinCleanupActivation,
) -> Result<super::device_join::JoinerJoinTerminal, StorePullError> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    let (commit, author) =
        load_commit_with_author_at_root(storage, root, &root_value, &activation.activation).await?;
    if commit.device_join_cleanup_receipts() != std::slice::from_ref(&activation.receipt) {
        return Err(StorePullError::Database(
            "device join cleanup activation does not contain its exact sole receipt".to_string(),
        ));
    }
    let authorization =
        load_device_join_authorization(storage, root, &commit.membership_state).await?;
    let predecessor = match &authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            RegistrationPredecessorAuthority::MergeConcurrent(chain)
        }
        DeviceJoinBootstrapAuthorization::Serial {
            position,
            authorization,
            ..
        } => RegistrationPredecessorAuthority::Serial {
            authorization,
            position: position.clone(),
            history: SerialAuthorizationHistory::ExactPredecessor,
        },
    };
    let receipts = Box::pin(validate_commit_join_cleanup_receipts(
        storage,
        root,
        &root_value,
        &commit,
        &author,
        Some(&predecessor),
        None,
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })?;
    let [receipt] = receipts.as_slice() else {
        return Err(StorePullError::Database(
            "device join cleanup activation does not resolve to one verified receipt".to_string(),
        ));
    };
    Ok(receipt.joiner_terminal.clone())
}

async fn validate_commit_acknowledgement(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
) -> Result<
    Option<(
        super::store_commit::StoreAckRef,
        super::store_commit::StoreAck,
    )>,
    RegistrationLoadError,
> {
    let Some(reference) = commit.acknowledgement() else {
        return Ok(None);
    };
    let ack = Box::pin(load_store_ack_ref(
        storage,
        root,
        reference,
        activating_author,
    ))
    .await
    .map_err(RegistrationLoadError::Object)?
    .value;
    let predecessor_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    if ack.registration != commit.author_registration
        || ack.store_cut != predecessor_cut
        || ack.device_state != commit.device_state
    {
        return Err(RegistrationLoadError::Invalid(
            "Store acknowledgement differs from its activating commit predecessor".to_string(),
        ));
    }
    if let Some(snapshot) = &ack.snapshot {
        let snapshot_author = load_registration_ref(storage, root, &snapshot.author_registration)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let (_, metadata) = Box::pin(super::store_snapshot::load_store_snapshot_ref(
            storage,
            root,
            &snapshot.author_registration,
            &snapshot_author.value,
            &snapshot.snapshot,
        ))
        .await
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        if !ack.store_cut.frontier().covers(&metadata.coverage) {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement does not cover its exact snapshot".to_string(),
            ));
        }
    }
    Ok(Some((reference.clone(), ack)))
}

async fn load_acknowledgement_proof_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    latest_ref: super::store_commit::StoreAckRef,
    latest: super::store_commit::StoreAck,
    registration: &StoreDeviceRegistration,
) -> Result<
    BTreeMap<
        u64,
        (
            super::store_commit::StoreAckRef,
            super::store_commit::StoreAck,
        ),
    >,
    RegistrationLoadError,
> {
    let mut chain = BTreeMap::new();
    let mut current_ref = latest_ref;
    let mut current = latest;
    loop {
        if chain
            .insert(current_ref.sequence, (current_ref.clone(), current.clone()))
            .is_some()
        {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement proof chain repeats a sequence".to_string(),
            ));
        }
        let Some((predecessor_ref, predecessor)) =
            load_store_ack_predecessor(storage, root, &current_ref, &current, registration)
                .await
                .map_err(RegistrationLoadError::Object)?
        else {
            break;
        };
        current_ref = predecessor_ref;
        current = predecessor.value;
    }
    if chain.first_key_value().map(|(sequence, _)| *sequence) != Some(1)
        || chain.last_key_value().map(|(sequence, _)| *sequence) != Some(chain.len() as u64)
    {
        return Err(RegistrationLoadError::Invalid(
            "Store acknowledgement proof chain is not contiguous from sequence one".to_string(),
        ));
    }
    Ok(chain)
}

pub(crate) async fn retain_activated_acknowledgement(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activating_commit: &StoreBatchCommitRef,
    activating_commit_value: &StoreBatchCommit,
    registration: &StoreDeviceRegistration,
    reference: super::store_commit::StoreAckRef,
    value: super::store_commit::StoreAck,
) -> Result<super::store_commit::RetainedVerifiedActivatedAck, StorePullError> {
    if activating_commit_value.acknowledgement() != Some(&reference)
        || activating_commit_value.author_registration != reference.registration
        || value.registration != reference.registration
    {
        return Err(StorePullError::Database(
            "Store acknowledgement differs from its activating commit".to_string(),
        ));
    }
    activating_commit
        .verify_commit(activating_commit_value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let chain = load_acknowledgement_proof_chain(storage, root, reference, value, registration)
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
    Ok(super::store_commit::RetainedVerifiedActivatedAck {
        chain,
        activating_commit: activating_commit.clone(),
        activating_commit_value: activating_commit_value.clone(),
    })
}

async fn validate_commit_reclaim_authorization(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    reference: &super::store_reclaim::ReclaimAuthorizationRef,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim authorization activation has no exact predecessor owner authority".to_string(),
        )
    })?;
    let opened = load_reclaim_authorization_ref(storage, root, reference)
        .await
        .map_err(RegistrationLoadError::Object)?;
    let evidence = &opened.evidence.value;
    let authorization = &opened.authorization.value;
    let owner_authorized = match &authorization.authority.membership {
        StoreMembershipStateRef::MergeConcurrent { .. } => {
            authorization.authority.membership == commit.membership_state
                && predecessor.verifies_owner(
                    &authorization.authority.membership,
                    &evidence.author_pubkey,
                    &authorization.authority.owner_grant,
                )
        }
        StoreMembershipStateRef::Serial { .. } => predecessor.verifies_owner_at_ancestor(
            &authorization.authority.membership,
            &evidence.author_pubkey,
            &authorization.authority.owner_grant,
        ),
    };
    if evidence.author_pubkey != activating_author.author_pubkey || !owner_authorized {
        return Err(RegistrationLoadError::Invalid(
            "reclaim authorization signer is not an active Owner at its exact predecessor"
                .to_string(),
        ));
    }
    let activation = Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        root_value,
        &commit.order,
        Box::new(|candidate, _| candidate == &evidence.claim.target.activation),
    ))
    .await?
    .ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim evidence package activation is absent from predecessor history".to_string(),
        )
    })?;
    if activation.1.store_package() != Some(&authorization.target) {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence target differs from its exact package activation".to_string(),
        ));
    }
    Ok(())
}

async fn validate_commit_reclaim_receipt(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    reference: &super::store_reclaim::ReclaimReceiptRef,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "reclaim receipt activation has no exact predecessor provider authority".to_string(),
        )
    })?;
    let (receipt_executor, provider_admin_state, provider_admin_grant, authorization, executor) = {
        let opened = Box::pin(load_reclaim_receipt_ref(storage, root, reference))
            .await
            .map_err(RegistrationLoadError::Object)?;
        (
            opened.receipt.value.executor.clone(),
            opened.receipt.value.provider_admin_state.clone(),
            opened.receipt.value.provider_admin_grant.clone(),
            opened.receipt.value.authorization.clone(),
            opened.executor,
        )
    };
    if receipt_executor != commit.author_registration
        || executor != *activating_author
        || provider_admin_state != commit.membership_state
        || !predecessor
            .verifies_provider_administrator_grant(&provider_admin_grant, &receipt_executor)
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim receipt signer is not the effective provider administrator at its exact predecessor"
                .to_string(),
        ));
    }
    if Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        root_value,
        &commit.order,
        Box::new(|_, candidate| candidate.reclaim_authorization() == Some(&authorization)),
    ))
    .await?
    .is_none()
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim receipt authorization is absent from predecessor history".to_string(),
        ));
    }
    Ok(())
}

async fn load_commit_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    let root_value = load_store_protocol_root(storage, root)
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    Box::pin(load_commit_registrations_with_root(
        storage,
        root,
        &root_value,
        commit,
        activating_author,
        predecessor,
        accepted_predecessor,
    ))
    .await
}

async fn load_commit_registrations_with_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    if commit.acknowledgement().is_some() {
        Box::pin(validate_commit_acknowledgement(
            storage,
            root,
            commit,
            activating_author,
        ))
        .await?;
    }
    if let Some(reference) = commit.reclaim_authorization() {
        Box::pin(validate_commit_reclaim_authorization(
            storage,
            root,
            root_value,
            commit,
            reference,
            activating_author,
            predecessor,
        ))
        .await?;
    }
    if let Some(reference) = commit.reclaim_receipt() {
        Box::pin(validate_commit_reclaim_receipt(
            storage,
            root,
            root_value,
            commit,
            reference,
            activating_author,
            predecessor,
        ))
        .await?;
    }
    let has_join_attempt = commit
        .device_join_attempt_decisions()
        .iter()
        .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)));
    if has_join_attempt {
        Box::pin(validate_commit_join_attempts(
            storage,
            root,
            commit,
            activating_author,
            predecessor,
            accepted_predecessor,
        ))
        .await?;
    }
    let verified_join_outcomes = if commit.device_join_outcomes().is_empty() {
        BTreeMap::new()
    } else {
        Box::pin(validate_commit_join_outcomes(
            storage,
            root,
            root_value,
            commit,
            activating_author,
            predecessor,
            accepted_predecessor,
        ))
        .await?
    };
    let has_join_abandonment = commit
        .device_join_attempt_decisions()
        .iter()
        .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Abandoned(_)));
    if has_join_abandonment {
        Box::pin(validate_commit_join_abandonments(
            storage,
            root,
            commit,
            activating_author,
            predecessor,
        ))
        .await?;
    }
    if !commit.device_join_cleanup_receipts().is_empty() {
        Box::pin(validate_commit_join_cleanup_receipts(
            storage,
            root,
            root_value,
            commit,
            activating_author,
            predecessor,
            accepted_predecessor,
        ))
        .await?;
    }
    let mut registrations = Vec::with_capacity(commit.device_registrations().len());
    for activated in commit.device_registrations() {
        let registration = Box::pin(load_registration_ref_with_root(
            storage,
            root,
            root_value,
            &activated.registration,
        ))
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "registration activation has no exact predecessor membership authority".to_string(),
            )
        })?;
        let authority = Box::pin(registration_activation(
            storage,
            root,
            activated,
            &registration,
            activating_author,
            commit.serial_recovery_activation(),
            predecessor,
            &verified_join_outcomes,
        ))
        .await?;
        registrations.push((registration, authority));
    }
    for retirement in commit.device_retirements() {
        if retirement.target != commit.author_registration {
            return Err(RegistrationLoadError::Invalid(
                "self-retirement targets a different exact registration".to_string(),
            ));
        }
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceSelfRetirement,
        );
        let bytes = storage
            .read_protocol_object(
                &context,
                &retirement.object,
                &super::store_commit::device_self_retirement_semantic_prefix(
                    commit.candidate_family(),
                    &retirement.target.device_id,
                    retirement.retirement_hash,
                ),
            )
            .await
            .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
        super::store_commit::StoreDeviceSelfRetirement::parse_at(
            &bytes,
            retirement,
            activating_author,
        )
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    }
    Ok(registrations)
}

fn device_state_has_active_registration(
    state: &ResolvedStoreDeviceState,
    registration: &StoreDeviceRegistrationRef,
) -> bool {
    state
        .devices
        .get(&registration.device_id)
        .is_some_and(|record| {
            record.registration == *registration
                && matches!(record.status, StoreDeviceStatus::Active)
        })
}

async fn verify_canonical_owner_registration(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &ResolvedStoreDeviceState,
    owner_pubkey: &str,
    selected: &StoreDeviceRegistrationRef,
) -> Result<(), StorePullError> {
    let active = load_active_history_registrations(storage, root, state).await?;
    let canonical = active
        .values()
        .filter(|(_, registration)| registration.author_pubkey == owner_pubkey)
        .map(|(reference, _)| reference)
        .min();
    if canonical != Some(selected) {
        return Err(StorePullError::Database(
            "conflict-resolution acceptance does not use the canonical active Owner registration"
                .to_string(),
        ));
    }
    Ok(())
}

fn device_state_has_pending_proposal(
    state: &ResolvedStoreDeviceState,
    proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
) -> bool {
    state
        .devices
        .get(&proposal.target.device_id)
        .and_then(|record| record.proposals.get(&proposal.proposal_id))
        .is_some_and(|state| {
            matches!(state, StoreDeviceProposalState::Pending { proposal: pending } if pending == proposal)
        })
}

enum DeviceStateResolver<'a> {
    Database(&'a Database),
    Loaded {
        genesis: &'a ResolvedStoreDeviceState,
        states: &'a BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    },
}

fn resolve_loaded_device_state(
    reference: &StoreDeviceStateRef,
    genesis: &ResolvedStoreDeviceState,
    states: &BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
    let state = match reference {
        StoreDeviceStateRef::MergeConcurrent { frontier, .. } => {
            let CommitFrontier::MergeConcurrent(frontier) = frontier else {
                return Err(RegistrationLoadError::Invalid(
                    "Merge device state contains a Serial frontier".to_string(),
                ));
            };
            if frontier.is_empty() {
                genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    frontier
                        .values()
                        .map(|commit| {
                            states.get(commit).cloned().ok_or_else(|| {
                                RegistrationLoadError::Invalid(
                                    "device state references an unloaded predecessor snapshot"
                                        .to_string(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?
            }
        }
        StoreDeviceStateRef::Serial { position, .. } => match position {
            StoreSerialPredecessor::Genesis { .. } => genesis.clone(),
            StoreSerialPredecessor::Commit(commit) => {
                states.get(commit).cloned().ok_or_else(|| {
                    RegistrationLoadError::Invalid(
                        "Serial device state references an unloaded predecessor snapshot"
                            .to_string(),
                    )
                })?
            }
        },
    };
    if state.state_hash != reference.state_hash() || state.recovery != reference.recovery() {
        return Err(RegistrationLoadError::Invalid(
            "device state differs from its exact predecessor snapshots".to_string(),
        ));
    }
    Ok(state)
}

async fn resolve_device_state(
    resolver: &DeviceStateResolver<'_>,
    reference: &StoreDeviceStateRef,
) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
    match resolver {
        DeviceStateResolver::Database(db) => db
            .resolved_store_device_state(reference)
            .await
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string())),
        DeviceStateResolver::Loaded { genesis, states } => {
            resolve_loaded_device_state(reference, genesis, states)
        }
    }
}

async fn predecessor_acknowledgement_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::StoreAckRef,
    ack: &super::store_commit::StoreAck,
) -> Result<bool, RegistrationLoadError> {
    let mut pending = match order {
        super::store_commit::StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => predecessor
            .iter()
            .chain(dependencies.values())
            .cloned()
            .collect::<Vec<_>>(),
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: StoreSerialPredecessor::Commit(predecessor),
            ..
        } => vec![predecessor.clone()],
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: StoreSerialPredecessor::Genesis { .. },
            ..
        } => Vec::new(),
    };
    let mut visited = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !visited.insert(reference.clone()) {
            continue;
        }
        let (commit, _) = load_commit_with_author(storage, root, &reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        if commit.acknowledgement() == Some(expected) {
            let predecessor_cut = commit
                .order
                .predecessor_cut()
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            return Ok(commit.author_registration == expected.registration
                && ack.registration == expected.registration
                && ack.store_cut == predecessor_cut
                && ack.device_state == commit.device_state);
        }
        match commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            } => {
                pending.extend(predecessor);
                pending.extend(dependencies.into_values());
            }
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => pending.push(predecessor),
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Genesis { .. },
                ..
            } => {}
        }
    }
    Ok(false)
}

async fn verify_merge_device_exclusion_proof(
    resolver: &DeviceStateResolver<'_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    proposal: &super::store_objects::VerifiedDeviceExclusionProposal,
    remaining_device_acks: &[super::store_commit::StoreAckRef],
    cutoff: &StoreHistoryCut,
) -> Result<(), RegistrationLoadError> {
    let frozen = resolve_device_state(resolver, &proposal.object.value.frozen_device_state).await?;
    if !device_state_has_active_registration(&frozen, &proposal.object.value.target) {
        return Err(RegistrationLoadError::Invalid(
            "device exclusion proposal frozen state does not contain its active target".to_string(),
        ));
    }
    let required = frozen
        .devices
        .values()
        .filter(|record| {
            record.registration != proposal.object.value.target
                && matches!(record.status, StoreDeviceStatus::Active)
        })
        .map(|record| (record.registration.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let target_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &proposal.object.value.target,
        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let mut certified = BTreeSet::new();
    let mut joined = BTreeMap::new();
    for reference in remaining_device_acks {
        let required_record = required.get(&reference.registration).ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device exclusion proof contains an acknowledgement from an ineligible registration"
                    .to_string(),
            )
        })?;
        if !certified.insert(reference.registration.clone()) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof repeats a remaining registration".to_string(),
            ));
        }
        let registration = load_registration_ref(storage, root, &required_record.registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        let ack = load_store_ack_ref(storage, root, reference, &registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        if !predecessor_acknowledgement_activation(storage, root, &commit.order, reference, &ack)
            .await?
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement is not activated in the outcome predecessor"
                    .to_string(),
            ));
        }
        let ack_state = resolve_device_state(resolver, &ack.device_state).await?;
        if !device_state_has_pending_proposal(&ack_state, &proposal.reference) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement does not observe the pending proposal"
                    .to_string(),
            ));
        }
        let freezes = match &ack.exclusions {
            super::store_commit::StoreAckExclusionState::MergeConcurrent { proposal_freezes } => {
                proposal_freezes
            }
            super::store_commit::StoreAckExclusionState::Serial => {
                return Err(RegistrationLoadError::Invalid(
                    "Merge device exclusion proof contains a Serial acknowledgement".to_string(),
                ))
            }
        };
        let freeze = freezes
            .iter()
            .find(|freeze| freeze.proposal == proposal.reference)
            .ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "device exclusion proof acknowledgement omits the exact proposal freeze"
                        .to_string(),
                )
            })?;
        let StoreHistoryCut::MergeConcurrent(target_cut) = &freeze.target_cut else {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement carries a Serial target cut".to_string(),
            ));
        };
        if target_cut.len() > 1 || target_cut.keys().any(|stream| stream != &target_stream) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement includes a non-target stream".to_string(),
            ));
        }
        if !ack
            .store_cut
            .frontier()
            .covers(&freeze.target_cut.frontier())
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proof acknowledgement target cut exceeds its Store cut"
                    .to_string(),
            ));
        }
        if let Some(reference) = target_cut.get(&target_stream) {
            match joined.entry(target_stream) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(reference.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get();
                    if reference.coord.sequence() > current.coord.sequence() {
                        entry.insert(reference.clone());
                    } else if reference.coord.sequence() == current.coord.sequence()
                        && reference != current
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof target cuts fork at one sequence".to_string(),
                        ));
                    }
                }
            }
        }
    }
    if certified != required.into_keys().collect()
        || cutoff != &StoreHistoryCut::MergeConcurrent(joined)
    {
        return Err(RegistrationLoadError::Invalid(
            "device exclusion proof does not certify every remaining registration and exact cutoff"
                .to_string(),
        ));
    }
    let predecessor_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    let StoreHistoryCut::MergeConcurrent(predecessor_frontier) = predecessor_cut else {
        return Err(RegistrationLoadError::Invalid(
            "Merge device exclusion outcome carries a Serial predecessor".to_string(),
        ));
    };
    let predecessor_target = predecessor_frontier
        .get(&target_stream)
        .map(|reference| BTreeMap::from([(target_stream, reference.clone())]));
    let target_predecessor_cut =
        StoreHistoryCut::MergeConcurrent(predecessor_target.unwrap_or_default());
    if !cutoff.frontier().covers(&target_predecessor_cut.frontier()) {
        return Err(RegistrationLoadError::Invalid(
            "device exclusion outcome predecessor advances the target beyond its certified cutoff"
                .to_string(),
        ));
    }
    Ok(())
}

async fn load_commit_device_operations(
    resolver: Option<&DeviceStateResolver<'_>>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    predecessor_state: &ResolvedStoreDeviceState,
    predecessor_authority: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<VerifiedStoreDeviceOperations, RegistrationLoadError> {
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()));
    }
    let authority = predecessor_authority.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device exclusion activation has no exact predecessor membership authority".to_string(),
        )
    })?;
    let mut proposals = Vec::with_capacity(commit.device_exclusion_proposals().len());
    for reference in commit.device_exclusion_proposals() {
        let opened = load_device_exclusion_proposal_ref(storage, root, reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let proposal = &opened.object.value;
        if proposal.frozen_device_state != commit.device_state
            || !device_state_has_active_registration(predecessor_state, &proposal.target)
            || !device_state_has_active_registration(
                predecessor_state,
                &proposal.owner_registration,
            )
            || !authority.verifies_owner(
                &commit.membership_state,
                &opened.owner.author_pubkey,
                &proposal.owner_grant,
            )
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion proposal differs from its active predecessor authority"
                    .to_string(),
            ));
        }
        proposals.push(RetainedStoreDeviceExclusionProposal::from_verified(&opened));
    }
    let mut outcomes = Vec::with_capacity(commit.device_exclusion_outcomes().len());
    for reference in commit.device_exclusion_outcomes() {
        if !device_state_has_pending_proposal(predecessor_state, reference.proposal()) {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion outcome does not resolve an exact pending proposal".to_string(),
            ));
        }
        let proposal = load_device_exclusion_proposal_ref(storage, root, reference.proposal())
            .await
            .map_err(RegistrationLoadError::Object)?;
        let outcome = load_device_exclusion_outcome_ref(storage, root, reference, &proposal)
            .await
            .map_err(RegistrationLoadError::Object)?;
        let (owner_registration, owner_grant) = match &outcome.object.value {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => {
                (&exclusion.owner_registration, &exclusion.owner_grant)
            }
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                (&cancellation.owner_registration, &cancellation.owner_grant)
            }
        };
        if !device_state_has_active_registration(predecessor_state, owner_registration)
            || !authority.verifies_owner(
                &commit.membership_state,
                &outcome.owner.author_pubkey,
                owner_grant,
            )
        {
            return Err(RegistrationLoadError::Invalid(
                "device exclusion outcome signer is not an active Owner at its predecessor"
                    .to_string(),
            ));
        }
        match (&outcome.object.value, reference) {
            (
                StoreDeviceExclusionOutcome::Cancelled(_),
                super::store_commit::StoreDeviceExclusionOutcomeRef::Cancelled(_),
            ) => {}
            (
                StoreDeviceExclusionOutcome::Excluded(exclusion),
                super::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(_),
            ) => match &exclusion.proof {
                StoreDeviceExclusionProof::Serial
                    if commit.policy() == crate::WritePolicy::Serial => {}
                StoreDeviceExclusionProof::Serial => {
                    return Err(RegistrationLoadError::Invalid(
                        "Merge device exclusion outcome carries a Serial proof".to_string(),
                    ))
                }
                StoreDeviceExclusionProof::MergeConcurrent {
                    frozen_device_state,
                    remaining_device_acks,
                    cutoff,
                } if commit.policy() == crate::WritePolicy::MergeConcurrent => {
                    if frozen_device_state != &proposal.object.value.frozen_device_state {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof names another frozen device state".to_string(),
                        ));
                    }
                    let resolver = resolver.ok_or_else(|| {
                        RegistrationLoadError::Invalid(
                            "Merge device exclusion proof has no materialized state resolver"
                                .to_string(),
                        )
                    })?;
                    verify_merge_device_exclusion_proof(
                        resolver,
                        storage,
                        root,
                        commit,
                        &proposal,
                        remaining_device_acks,
                        cutoff,
                    )
                    .await?;
                }
                StoreDeviceExclusionProof::MergeConcurrent { .. } => {
                    return Err(RegistrationLoadError::Invalid(
                        "Serial device exclusion outcome carries a Merge proof".to_string(),
                    ))
                }
            },
            _ => {
                return Err(RegistrationLoadError::Invalid(
                    "device exclusion outcome variant differs from its exact reference".to_string(),
                ))
            }
        }
        outcomes.push(
            RetainedStoreDeviceExclusionOutcome::from_verified(
                reference,
                RetainedStoreDeviceExclusionProposal::from_verified(&proposal),
                &outcome,
            )
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?,
        );
    }
    RetainedStoreDeviceOperations::from_sources(proposals, outcomes)
        .verify_for(root, commit)
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

pub(crate) async fn load_local_commit_device_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| StorePullError::Database(error.to_string()));
    }
    let (state_ref, state) = db.store_device_state_for_order(&commit.order).await?;
    if state_ref != commit.device_state {
        return Err(StorePullError::Database(
            "local exclusion commit differs from its materialized predecessor device state"
                .to_string(),
        ));
    }
    let authorization =
        load_device_join_authorization(storage, root, &commit.membership_state).await?;
    let authority = match &authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            RegistrationPredecessorAuthority::MergeConcurrent(chain)
        }
        DeviceJoinBootstrapAuthorization::Serial {
            position,
            authorization,
            ..
        } => RegistrationPredecessorAuthority::Serial {
            authorization,
            position: position.clone(),
            history: SerialAuthorizationHistory::ExactPredecessor,
        },
    };
    load_local_commit_device_operations_with_authority(db, storage, root, commit, state, &authority)
        .await
}

pub(crate) async fn load_local_commit_device_operations_with_merge_membership(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    membership: &MembershipChain,
    state_ref: &StoreDeviceStateRef,
    state: ResolvedStoreDeviceState,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    if commit.device_exclusion_proposals().is_empty()
        && commit.device_exclusion_outcomes().is_empty()
    {
        return VerifiedStoreDeviceOperations::without_exclusions(commit)
            .map_err(|error| StorePullError::Database(error.to_string()));
    }
    if commit.policy() != crate::WritePolicy::MergeConcurrent {
        return Err(StorePullError::Database(
            "retained Merge membership authority received a Serial commit".to_string(),
        ));
    }
    if state_ref != &commit.device_state {
        return Err(StorePullError::Database(
            "local exclusion commit differs from its materialized predecessor device state"
                .to_string(),
        ));
    }
    verify_merge_membership_state_ref(&commit.membership_state, membership, &state)?;
    let authority = RegistrationPredecessorAuthority::MergeConcurrent(membership);
    load_local_commit_device_operations_with_authority(db, storage, root, commit, state, &authority)
        .await
}

async fn load_local_commit_device_operations_with_authority(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    state: ResolvedStoreDeviceState,
    authority: &RegistrationPredecessorAuthority<'_>,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    let resolver = DeviceStateResolver::Database(db);
    Box::pin(load_commit_device_operations(
        Some(&resolver),
        storage,
        root,
        commit,
        &state,
        Some(authority),
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })
}

pub(crate) async fn derive_local_merge_post_device_state(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    predecessor_state: ResolvedStoreDeviceState,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
    device_operations: VerifiedStoreDeviceOperations,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let (authorized_predecessor, recovery_author) =
        predecessor_with_recovery_author(predecessor_state, commit, registrations)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let owner_recovery = Box::pin(verify_commit_owner_recovery_activation(
        storage, root, commit, None,
    ))
    .await?;
    device_operations
        .apply_to(authorized_predecessor, &commit.device_state)
        .and_then(|state| {
            apply_verified_device_lifecycle(
                state,
                commit,
                registrations,
                recovery_author.as_ref(),
                owner_recovery,
            )
        })
        .map_err(|error| StorePullError::Database(error.to_string()))
}

async fn validate_commit_join_abandonments(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join abandonment activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join abandonment activation author is not an active Owner".to_string(),
        ));
    }
    for reference in commit
        .device_join_attempt_decisions()
        .iter()
        .filter_map(|decision| match decision {
            DeviceJoinAttemptDecisionRef::Attempt(_) => None,
            DeviceJoinAttemptDecisionRef::Abandoned(reference) => Some(reference),
        })
    {
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAbandonment,
        );
        let bytes = storage
            .read_protocol_object(
                &context,
                &reference.object,
                &super::store_commit::device_join_abandonment_semantic_prefix(reference.attempt_id),
            )
            .await
            .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
        let abandonment: super::device_join::DeviceJoinAbandonmentObject =
            serde_json::from_slice(&bytes)
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        if abandonment.store_root_hash != root.store_root_hash
            || abandonment.owner_registration != commit.author_registration
            || abandonment.attempt_slot != *reference.object.slot()
        {
            return Err(RegistrationLoadError::Invalid(
                "device join abandonment differs from its activating commit".to_string(),
            ));
        }
        reference
            .verify(&abandonment, activating_author)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    }
    Ok(())
}

async fn load_commit_device_join_attempt(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &super::store_commit::DeviceJoinAttemptRef,
    owner: &StoreDeviceRegistration,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<DeviceJoinAttempt, RegistrationLoadError> {
    let attempt = match accepted_predecessor {
        Some(accepted_predecessor) => load_verified_device_join_attempt_evidence_ref(
            storage,
            root,
            reference,
            owner,
            Some(accepted_predecessor),
        ),
        None => load_verified_device_join_attempt_ref(storage, root, reference, owner),
    }
    .await
    .map_err(registration_attempt_error)?;
    Ok(attempt.value)
}

async fn validate_commit_join_cleanup_receipts(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<Vec<super::device_join::DeviceJoinCleanupReceiptObject>, RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join cleanup activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join cleanup activation author is not an active Owner".to_string(),
        ));
    }
    let mut receipts = Vec::with_capacity(commit.device_join_cleanup_receipts().len());
    for reference in commit.device_join_cleanup_receipts() {
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinCleanupReceipt,
        );
        let bytes = storage
            .read_protocol_object(
                &context,
                &reference.object,
                &super::store_commit::device_join_cleanup_receipt_semantic_prefix(
                    reference.attempt_id,
                ),
            )
            .await
            .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
        let receipt: super::device_join::DeviceJoinCleanupReceiptObject =
            serde_json::from_slice(&bytes)
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        if receipt.executor != commit.author_registration
            || receipt.membership != commit.membership_state
            || !predecessor_contains_join_outcome(
                storage,
                root,
                root_value,
                &commit.order,
                &receipt.cancellation,
            )
            .await?
        {
            return Err(RegistrationLoadError::Invalid(
                "device join cleanup receipt differs from its activating predecessor".to_string(),
            ));
        }
        let attempt_ref = receipt.cancellation.attempt();
        let attempt_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let attempt_bytes = storage
            .read_protocol_object(
                &attempt_context,
                &attempt_ref.object,
                &super::store_commit::device_join_attempt_semantic_prefix(attempt_ref.attempt_id),
            )
            .await
            .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
        let unverified: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        let owner = load_registration_ref(storage, root, &unverified.owner_registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        let attempt = Box::pin(load_commit_device_join_attempt(
            storage,
            root,
            attempt_ref,
            &owner,
            accepted_predecessor,
        ))
        .await?;
        let expected_administrator = &attempt.provider_approval.request.offer.provider_admin;
        let protocol_root = load_store_protocol_root(storage, root)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        if !predecessor.verifies_provider_administrator(
            &receipt.provider_admin_grant,
            &receipt.executor,
            expected_administrator,
        ) || activating_author.provider != expected_administrator.provider
            || attempt.provider_approval.request.offer.provider != protocol_root.descriptor.provider
        {
            return Err(RegistrationLoadError::Invalid(
                "device join cleanup executor is not the exact effective provider administrator"
                    .to_string(),
            ));
        }
        reference
            .verify(&receipt, activating_author)
            .and_then(|_| receipt.verify(&attempt, activating_author))
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        match &receipt.administrator_terminal {
            super::device_join::ProviderAdminJoinTerminal::Completed(_) => {}
            super::device_join::ProviderAdminJoinTerminal::Cancelled(closure) => {
                let administrator =
                    load_registration_ref(storage, root, &closure.administrator_registration)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                closure
                    .verify(&administrator)
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            }
            super::device_join::ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
                let executor = load_registration_ref(storage, root, &revocation.executor)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                revocation
                    .verify(&executor)
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            }
        }
        match &receipt.joiner_terminal {
            super::device_join::JoinerJoinTerminal::Ready(_) => {}
            super::device_join::JoinerJoinTerminal::Cancelled(closure) => closure
                .verify()
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?,
            super::device_join::JoinerJoinTerminal::WriteRevoked(revocation) => {
                let executor = load_registration_ref(storage, root, &revocation.executor)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                revocation
                    .verify(&executor)
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            }
        }
        receipts.push(receipt);
    }
    Ok(receipts)
}

async fn validate_commit_join_outcomes(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<
    BTreeMap<super::store_commit::DeviceJoinOutcomeRef, VerifiedCommitJoinOutcome>,
    RegistrationLoadError,
> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join outcome activation has no exact predecessor authority".to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join outcome activation author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    let mut verified = BTreeMap::new();
    for outcome_ref in commit.device_join_outcomes() {
        if !Box::pin(predecessor_contains_join_attempt(
            storage,
            root,
            root_value,
            &commit.order,
            outcome_ref.attempt(),
        ))
        .await?
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome names an attempt absent from its predecessor history"
                    .to_string(),
            ));
        }
        let attempt_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let attempt_bytes = storage
            .read_protocol_object(
                &attempt_context,
                &outcome_ref.attempt().object,
                &super::store_commit::device_join_attempt_semantic_prefix(
                    outcome_ref.attempt().attempt_id,
                ),
            )
            .await
            .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
        let unverified: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        let owner = load_registration_ref(storage, root, &unverified.owner_registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        let attempt = Box::pin(load_commit_device_join_attempt(
            storage,
            root,
            outcome_ref.attempt(),
            &owner,
            accepted_predecessor,
        ))
        .await?;
        if owner != *activating_author
            || attempt.owner_registration != commit.author_registration
            || outcome_ref.slot() != &attempt.outcome_slot
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome differs from its exact Owner attempt".to_string(),
            ));
        }
        let outcome = load_device_join_outcome_ref(storage, root, outcome_ref, &owner)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        if outcome.owner_registration != attempt.owner_registration
            || outcome.owner_grant != attempt.owner_grant
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome signer differs from its attempt".to_string(),
            ));
        }
        let activation = commit.device_registrations().iter().find(|activation| {
            matches!(
                &activation.authority,
                StoreDeviceRegistrationActivationRef::Join { outcome, .. }
                    if outcome == outcome_ref
            )
        });
        if matches!(&outcome.body, DeviceJoinOutcomeBody::Activated { .. }) != activation.is_some()
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome and registration activation are not one closed operation"
                    .to_string(),
            ));
        }
        if verified
            .insert(
                outcome_ref.clone(),
                VerifiedCommitJoinOutcome {
                    attempt,
                    owner,
                    outcome,
                },
            )
            .is_some()
        {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome is duplicated in one commit".to_string(),
            ));
        }
    }
    Ok(verified)
}

async fn validate_commit_join_attempts(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<(), RegistrationLoadError> {
    let predecessor = predecessor.ok_or_else(|| {
        RegistrationLoadError::Invalid(
            "device join attempt activation has no exact predecessor membership authority"
                .to_string(),
        )
    })?;
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "device join attempt activation author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    let bootstrap_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
    for reference in commit
        .device_join_attempt_decisions()
        .iter()
        .filter_map(|decision| match decision {
            DeviceJoinAttemptDecisionRef::Attempt(reference) => Some(reference),
            DeviceJoinAttemptDecisionRef::Abandoned(_) => None,
        })
    {
        let attempt = Box::pin(load_commit_device_join_attempt(
            storage,
            root,
            reference,
            activating_author,
            accepted_predecessor,
        ))
        .await?;
        if attempt.owner_registration != commit.author_registration
            || attempt.membership != commit.membership_state
            || attempt.bootstrap_cut != bootstrap_cut
            || !predecessor.verifies_owner(
                &attempt.membership,
                &activating_author.author_pubkey,
                &attempt.owner_grant,
            )
        {
            return Err(RegistrationLoadError::Invalid(
                "device join attempt differs from its exact activating predecessor authority"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

async fn registration_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activated: &ActivatedStoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    activating_author: &StoreDeviceRegistration,
    serial_recovery_activation: Option<&super::store_commit::SerialRecoveryActivation>,
    predecessor: &RegistrationPredecessorAuthority<'_>,
    verified_join_outcomes: &BTreeMap<
        super::store_commit::DeviceJoinOutcomeRef,
        VerifiedCommitJoinOutcome,
    >,
) -> Result<StoreDeviceRegistrationActivation, RegistrationLoadError> {
    if !predecessor.verifies_active_owner(&activating_author.author_pubkey) {
        return Err(RegistrationLoadError::Invalid(
            "registration activation commit author is not an active Owner at its predecessor"
                .to_string(),
        ));
    }
    match (&registration.origin, &activated.authority) {
        (
            StoreDeviceRegistrationOrigin::Join {
                attempt_id: origin_attempt,
                outcome_slot,
                ..
            },
            StoreDeviceRegistrationActivationRef::Join {
                attempt_id,
                outcome,
            },
        ) if origin_attempt == attempt_id && outcome_slot == outcome.slot() => {
            let verified = verified_join_outcomes.get(outcome).ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "registration activation has no verified join outcome".to_string(),
                )
            })?;
            let attempt = &verified.attempt;
            let owner = &verified.owner;
            if attempt.expected_registration != *registration
                || attempt.registration_slot != *activated.registration.object.slot()
                || !predecessor.verifies_owner_at_ancestor(
                    &attempt.membership,
                    &owner.author_pubkey,
                    &attempt.owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "activated registration differs from its exact join attempt".to_string(),
                ));
            }
            let outcome_value = &verified.outcome;
            if outcome_value.owner_registration != attempt.owner_registration
                || outcome_value.owner_grant != attempt.owner_grant
            {
                return Err(RegistrationLoadError::Invalid(
                    "join outcome signer differs from its exact attempt authority".to_string(),
                ));
            }
            let DeviceJoinOutcomeBody::Activated { readiness } = &outcome_value.body else {
                return Err(RegistrationLoadError::Invalid(
                    "cancelled device join outcome cannot activate a registration".to_string(),
                ));
            };
            let initial_ack =
                load_store_ack_ref(storage, root, &readiness.initial_ack, registration)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
            readiness
                .verify(
                    outcome.attempt(),
                    attempt,
                    registration,
                    &readiness.initial_ack,
                    &initial_ack,
                )
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            Ok(StoreDeviceRegistrationActivation::Join {
                attempt_id: *attempt_id,
                outcome: outcome.clone(),
            })
        }
        (
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id: origin_recovery,
                recovery_slot,
                ..
            },
            StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node },
        ) if origin_recovery == recovery_id && recovery_slot == node.slot() => {
            if matches!(predecessor, RegistrationPredecessorAuthority::Serial { .. })
                && serial_recovery_activation.is_none_or(|body| &body.registration != activated)
            {
                return Err(RegistrationLoadError::Invalid(
                    "Serial recovery activation differs from its closed commit body".to_string(),
                ));
            }
            let node_value = load_owner_recovery_node_ref(storage, root, node)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let mut reached_ref = node.clone();
            let mut reached = node_value.clone();
            while let Some(predecessor_ref) = reached.predecessor.clone() {
                let predecessor = load_owner_recovery_node_ref(storage, root, &predecessor_ref)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                if predecessor.next_slot != *reached_ref.object.slot() {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery node does not occupy its exact predecessor successor slot"
                            .to_string(),
                    ));
                }
                if predecessor.recovery_id != node_value.recovery_id {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery predecessor belongs to another recovery operation".to_string(),
                    ));
                }
                reached_ref = predecessor_ref;
                reached = predecessor;
            }
            if node_value.recovery_id != *recovery_id
                || node_value.readiness.registration != activated.registration
                || node_value.next_slot == *node.object.slot()
                || registration.author_pubkey != node_value.owner_pubkey
                || !predecessor.verifies_owner(
                    &node_value.membership,
                    &node_value.owner_pubkey,
                    &node_value.owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "recovery node differs from its exact registration".to_string(),
                ));
            }
            let initial_ack = load_store_ack_ref(
                storage,
                root,
                &node_value.readiness.initial_ack,
                registration,
            )
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
            if initial_ack.sequence != 1
                || initial_ack.successor.predecessor.is_some()
                || initial_ack.registration != activated.registration
                || initial_ack.store_cut != node_value.readiness.bootstrap_cut
            {
                return Err(RegistrationLoadError::Invalid(
                    "recovery readiness differs from its initial acknowledgement".to_string(),
                ));
            }
            Ok(StoreDeviceRegistrationActivation::Recovery {
                recovery_id: *recovery_id,
                node: node.clone(),
            })
        }
        _ => Err(RegistrationLoadError::Invalid(format!(
            "Store registration {} origin differs from its activation authority",
            registration.device_id
        ))),
    }
}

async fn predecessor_contains_join_attempt(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::DeviceJoinAttemptRef,
) -> Result<bool, RegistrationLoadError> {
    Ok(
        Box::pin(predecessor_commit_matching_at_root(
            storage,
            root,
            root_value,
            order,
            Box::new(|_, commit| {
                commit.device_join_attempt_decisions().iter().any(|decision| {
                    matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(reference) if reference == expected)
                })
            }),
        ))
        .await?
        .is_some(),
    )
}

type PredecessorCommitPredicate<'a> =
    Box<dyn FnMut(&StoreBatchCommitRef, &StoreBatchCommit) -> bool + Send + 'a>;

pub(crate) async fn predecessor_commit_matching(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    matches: PredecessorCommitPredicate<'_>,
) -> Result<Option<(StoreBatchCommitRef, StoreBatchCommit)>, RegistrationLoadError> {
    let root_value = load_store_protocol_root(storage, root)
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        &root_value,
        order,
        matches,
    ))
    .await
}

async fn predecessor_commit_matching_at_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    order: &super::store_commit::StoreCommitOrder,
    mut matches: PredecessorCommitPredicate<'_>,
) -> Result<Option<(StoreBatchCommitRef, StoreBatchCommit)>, RegistrationLoadError> {
    let mut pending = match order {
        super::store_commit::StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => predecessor
            .iter()
            .chain(dependencies.values())
            .cloned()
            .collect::<Vec<_>>(),
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: super::store_commit::StoreSerialPredecessor::Commit(predecessor),
            ..
        } => vec![predecessor.clone()],
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: super::store_commit::StoreSerialPredecessor::Genesis { .. },
            ..
        } => Vec::new(),
    };
    let mut visited = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !visited.insert(reference.clone()) {
            continue;
        }
        let (commit, _) = load_commit_with_author_at_root(storage, root, root_value, &reference)
            .await
            .map_err(RegistrationLoadError::Object)?;
        if matches(&reference, &commit) {
            return Ok(Some((reference, commit)));
        }
        match commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            } => {
                pending.extend(predecessor);
                pending.extend(dependencies.into_values());
            }
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: super::store_commit::StoreSerialPredecessor::Commit(predecessor),
                ..
            } => pending.push(predecessor),
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: super::store_commit::StoreSerialPredecessor::Genesis { .. },
                ..
            } => {}
        }
    }
    Ok(None)
}

async fn predecessor_contains_join_outcome(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::DeviceJoinOutcomeRef,
) -> Result<bool, RegistrationLoadError> {
    Ok(Box::pin(predecessor_commit_matching_at_root(
        storage,
        root,
        root_value,
        order,
        Box::new(|_, commit| {
            commit
                .device_join_outcomes()
                .binary_search(expected)
                .is_ok()
        }),
    ))
    .await?
    .is_some())
}

#[doc(hidden)]
pub struct SerialResolutionCommit {
    pub(crate) commit: StoreBatchCommit,
    pub(crate) commit_ref: super::store_commit::StoreBatchCommitRef,
    pub(crate) packages: Vec<AudiencePackage>,
    pub(crate) changesets: super::gate::SerialInboundChangesets,
    pub(crate) registrations: Vec<(
        StoreDeviceRegistration,
        super::store_commit::StoreDeviceRegistrationActivation,
    )>,
    pub(crate) verified_circle_activations: VerifiedCircleActivations,
    pub(crate) device_operations: VerifiedStoreDeviceOperations,
    pub(crate) authorization_after: SerialAuthorizationState,
}

#[doc(hidden)]
pub struct SerialResolutionPlan {
    head: StoreSerialHead,
    head_object: super::storage::VersionedObject,
    commits: Vec<SerialResolutionCommit>,
    verified_suffix: Option<VerifiedSerialAcceptedSuffix>,
}

impl SerialResolutionPlan {
    pub(crate) fn head(&self) -> &StoreSerialHead {
        &self.head
    }

    pub(crate) fn head_object(&self) -> &super::storage::VersionedObject {
        &self.head_object
    }

    pub(crate) fn commits(&self) -> &[SerialResolutionCommit] {
        &self.commits
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        StoreSerialHead,
        super::storage::VersionedObject,
        Vec<SerialResolutionCommit>,
    ) {
        (self.head, self.head_object, self.commits)
    }

    pub(crate) fn verified_suffix(&self) -> Result<VerifiedSerialAcceptedSuffix, StorePullError> {
        self.verified_suffix.clone().ok_or_else(|| {
            StorePullError::Serial("Serial resolution has no accepted successor suffix".to_string())
        })
    }
}

enum ApplyOutcome {
    Applied(Vec<RowChange>),
    Held(HeldStorePositionReason),
}

/// Discover every visible immutable head, then repeatedly materialize any commit
/// whose exact predecessor and dependency positions are already durable.
#[allow(clippy::too_many_arguments)]
pub async fn pull_store_commits(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<StorePullResult, StorePullError> {
    Box::pin(pull_store_commits_with_identity(
        db,
        tables,
        storage,
        None,
        store_root_hash,
        store_dir,
        membership,
        None,
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub async fn pull_store_commits_with_coordination(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<StorePullResult, StorePullError> {
    Box::pin(pull_store_commits_with_identity(
        db,
        tables,
        storage,
        serial_coordination,
        store_root_hash,
        store_dir,
        membership,
        None,
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
pub fn pull_store_commits_with_identity<'a>(
    db: &'a Database,
    tables: &'a [SyncedTable],
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    store_root_hash: ObjectHash,
    store_dir: &'a StoreDir,
    membership: Option<&'a MembershipChain>,
    identity: Option<&'a crate::keys::UserKeypair>,
) -> Pin<Box<dyn Future<Output = Result<StorePullResult, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let root = db
            .local_store_root_ref()
            .await
            .map_err(|error| StorePullError::Database(format!("load exact Store root: {error}")))?
            .ok_or_else(|| {
                StorePullError::Database("Store root exact reference is absent".to_string())
            })?;
        if root.store_root_hash != store_root_hash {
            return Err(StorePullError::Database(
                "requested Store root differs from the durable exact root reference".to_string(),
            ));
        }
        let verified_root = load_store_protocol_root(storage, &root).await?.value;
        if db.write_policy() == crate::WritePolicy::Serial {
            let serial_pull: Pin<Box<dyn Future<Output = _> + Send + '_>> =
                Box::pin(pull_serial_store_commits(
                    db,
                    tables,
                    storage,
                    serial_coordination.ok_or_else(|| {
                        StorePullError::Serial("coordination capability is absent".to_string())
                    })?,
                    &root,
                    verified_root,
                    store_dir,
                    identity,
                ));
            return serial_pull.await;
        }
        if verified_root.descriptor.write_policy != crate::WritePolicy::MergeConcurrent {
            return Err(StorePullError::Database(
                "durable write policy differs from the signed Store root".to_string(),
            ));
        }
        resume_merge_retraction_cleanups(db, storage, &root).await?;

        let local_frontier = db.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load discovery device-state frontier: {error}"))
        })?;
        let local_frontier = local_frontier
            .into_values()
            .map(|reference| match reference.coord {
                StoreCommitCoord::MergeConcurrent { stream_id, .. } => Ok((stream_id, reference)),
                StoreCommitCoord::Serial { .. } => Err(StorePullError::Database(
                    "Merge discovery frontier contains a Serial commit".to_string(),
                )),
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let (_, discovery_device_state) = db
            .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(local_frontier))
            .await?;

        let mut active = load_active_merge_registrations(db, storage, &root)
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load active Merge registrations: {error}"))
            })?;
        if let Some(membership) = membership {
            for recovered in
                discover_merge_owner_recoveries(storage, &root, &verified_root, membership).await?
            {
                if active
                    .iter()
                    .all(|(reference, _)| reference != &recovered.0)
                {
                    active.push(recovered);
                }
            }
        }
        let mut candidates = BTreeMap::new();
        let mut visible_heads = Vec::new();
        let mut held = Vec::new();
        for (registration_ref, registration) in active {
            let inactive_cut = match discovery_device_state
                .devices
                .get(&registration_ref.device_id)
            {
                Some(record) if record.registration != registration_ref => {
                    return Err(StorePullError::Database(format!(
                        "discovery device state names another registration for {}",
                        registration_ref.device_id
                    )));
                }
                Some(record) => match &record.status {
                    StoreDeviceStatus::Active => None,
                    StoreDeviceStatus::Inactive { accepted_cut, .. } => Some(accepted_cut),
                },
                None => None,
            };
            let discovered = discover_merge_stream(
                storage,
                &root,
                &registration_ref,
                &registration,
                inactive_cut,
            )
            .await
            .map_err(|error| {
                StorePullError::Database(format!(
                    "discover Merge stream for {}: {error}",
                    registration.device_id
                ))
            })?;
            if let Some(head) = discovered.latest_head {
                visible_heads.push(VerifiedStoreDeviceHead {
                    head,
                    author: registration.clone(),
                });
            }
            if let Some(block) = discovered.block {
                held.push(block.into_position());
            }
            for (activation_head_ref, activation_head, commit_ref, commit) in discovered.commits {
                if commit_ref.coord.sequence() != commit.seq() {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::InvalidObject(
                            "exact commit coordinate differs from signed sequence".to_string(),
                        ),
                    ));
                    continue;
                }
                let stream_id = commit_stream_id(&commit_ref.coord);
                if let Some(materialized) = db
                    .exact_materialized_ref(&stream_id, commit_ref.coord.sequence())
                    .await?
                {
                    if materialized == commit_ref {
                        continue;
                    }
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::HashMismatch {
                            referenced_device_id: stream_id,
                            referenced_commit: commit_ref.clone(),
                            materialized_hash: materialized.commit_hash,
                        },
                    ));
                    continue;
                }
                if let Some(package) = commit.store_package() {
                    if package.schema_version > db.schema_version() {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::NewerSchema {
                                local: db.schema_version(),
                                required: package.schema_version,
                            },
                        ));
                        continue;
                    }
                }
                let predecessor_membership = match load_merge_predecessor_membership(
                    storage,
                    &root,
                    &commit.membership_state,
                )
                .await
                {
                    Ok(membership) => membership,
                    Err(RegistrationLoadError::Object(error)) => {
                        held.push(held_commit(&commit_ref, held_object_error(error)));
                        continue;
                    }
                    Err(RegistrationLoadError::Invalid(error)) => {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::InvalidObject(error),
                        ));
                        continue;
                    }
                };
                let predecessor_authority =
                    RegistrationPredecessorAuthority::MergeConcurrent(&predecessor_membership);
                let requires_accepted_predecessor = commit
                    .device_join_attempt_decisions()
                    .iter()
                    .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)))
                    || !commit.device_join_outcomes().is_empty()
                    || !commit.device_join_cleanup_receipts().is_empty()
                    || commit.device_registrations().iter().any(|activation| {
                        matches!(
                            activation.authority,
                            StoreDeviceRegistrationActivationRef::Join { .. }
                        )
                    });
                let verified_accepted_predecessor = if requires_accepted_predecessor {
                    let predecessor_cut = commit
                        .order
                        .predecessor_cut()
                        .map_err(|error| StorePullError::Database(error.to_string()))?;
                    Some(
                        Box::pin(verify_store_history_state(
                            storage,
                            None,
                            &root,
                            &predecessor_cut,
                            &commit.membership_state,
                        ))
                        .await?,
                    )
                } else {
                    None
                };
                let accepted_predecessor = verified_accepted_predecessor
                    .as_ref()
                    .map(|_| VerifiedAcceptedPredecessor::Exact);
                let registrations = match Box::pin(load_commit_registrations(
                    storage,
                    &root,
                    &commit,
                    &registration,
                    Some(&predecessor_authority),
                    accepted_predecessor.as_ref(),
                ))
                .await
                {
                    Ok(registrations) => registrations,
                    Err(RegistrationLoadError::Object(error)) => {
                        held.push(held_commit(&commit_ref, held_object_error(error)));
                        continue;
                    }
                    Err(RegistrationLoadError::Invalid(error)) => {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::InvalidObject(error),
                        ));
                        continue;
                    }
                };
                if !membership_authorizes(Some(&predecessor_membership), &commit, &registration) {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::Unauthorized,
                    ));
                    continue;
                }
                let package = match load_store_package(storage, &commit_ref, &commit).await {
                    Ok(package) => package.map(|package| package.value),
                    Err(error) => {
                        held.push(held_package(&commit_ref, &commit, held_object_error(error)));
                        continue;
                    }
                };
                let key = (
                    commit_stream_id(&commit_ref.coord),
                    commit_ref.coord.sequence(),
                );
                let device_operations = if commit.device_exclusion_proposals().is_empty()
                    && commit.device_exclusion_outcomes().is_empty()
                {
                    CandidateDeviceOperations::Verified(
                        VerifiedStoreDeviceOperations::without_exclusions(&commit)
                            .map_err(|error| StorePullError::Database(error.to_string()))?,
                    )
                } else {
                    CandidateDeviceOperations::MergePending {
                        predecessor_membership: predecessor_membership.clone(),
                    }
                };
                candidates.insert(
                    key,
                    MergeCandidate {
                        activation_head,
                        activation_head_object: activation_head_ref.object,
                        candidate: Candidate {
                            commit_ref,
                            commit,
                            author: registration.clone(),
                            package,
                            registrations,
                            device_operations,
                        },
                        predecessor_membership,
                    },
                );
            }
        }

        let retained = Box::pin(db.retained_merge_replay_inputs()).await?;
        let mut loaded_predecessor_memberships = BTreeMap::new();
        for materialization in retained {
            if materialization.commit().membership_authority.is_none() {
                continue;
            }
            let membership = Box::pin(load_merge_predecessor_membership(
                storage,
                &root,
                &materialization.commit().membership_state,
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            loaded_predecessor_memberships.insert(materialization.commit_ref().clone(), membership);
        }
        for candidate in candidates.values() {
            loaded_predecessor_memberships.insert(
                candidate.candidate.commit_ref.clone(),
                candidate.predecessor_membership.clone(),
            );
        }
        let loaded_predecessor_memberships = LoadedMergePredecessorMemberships {
            by_commit: loaded_predecessor_memberships,
        };

        let schema: Arc<TableSchema> = {
            let tables = tables.to_vec();
            let gates = db.gates();
            Arc::new(
                db.call(move |conn| {
                    TableSchema::for_apply(
                        conn,
                        &tables,
                        &gates,
                        crate::WritePolicy::MergeConcurrent,
                    )
                })
                .await
                .map_err(|error| {
                    StorePullError::Database(format!("load synced table schema: {error}"))
                })?,
            )
        };
        let coverage = db.snapshot_coverage_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load snapshot coverage frontier: {error}"))
        })?;
        let mut frontier = db.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load materialized frontier: {error}"))
        })?;
        let mut applied_devices = BTreeSet::new();
        let mut row_changes = Vec::new();
        let mut changesets_applied = 0_u64;
        let mut asset_downloads_failed = false;
        let mut blocked = BTreeMap::new();

        loop {
            let mut progressed = false;
            let keys = candidates.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let candidate = candidates.get(&key).ok_or_else(|| {
                    StorePullError::Database(
                        "Merge candidate disappeared while evaluating readiness".to_string(),
                    )
                })?;
                let exclusion_freezes = db.store_device_exclusion_freezes().await?;
                let current_frontier = CommitFrontier::from_refs(
                    crate::WritePolicy::MergeConcurrent,
                    frontier.clone(),
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
                let CommitFrontier::MergeConcurrent(current_frontier) = current_frontier else {
                    return Err(StorePullError::Database(
                        "Merge pull produced a Serial materialized frontier".to_string(),
                    ));
                };
                let (_, current_device_state) = db
                    .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(
                        current_frontier,
                    ))
                    .await?;
                match readiness(
                    db,
                    storage,
                    &root,
                    &coverage,
                    &frontier,
                    &current_device_state,
                    &exclusion_freezes,
                    &candidate.candidate.commit_ref,
                    &candidate.candidate.commit,
                )
                .await
                .map_err(|error| {
                    StorePullError::Database(format!(
                        "evaluate Store commit readiness for {}/{}: {error}",
                        key.0, key.1
                    ))
                })? {
                    Readiness::AlreadyMaterialized => {
                        candidates.remove(&key);
                        blocked.remove(&key);
                        progressed = true;
                    }
                    Readiness::Held(held_position) => {
                        blocked.insert(key, held_position);
                    }
                    Readiness::Ready => {
                        let candidate = candidates.remove(&key).ok_or_else(|| {
                            StorePullError::Database(
                                "ready Merge candidate disappeared before apply".to_string(),
                            )
                        })?;
                        match Box::pin(apply_candidate(
                            db,
                            storage,
                            &root,
                            store_dir,
                            schema.clone(),
                            &candidate,
                            &loaded_predecessor_memberships,
                            identity,
                        ))
                        .await?
                        {
                            ApplyOutcome::Applied(changes) => {
                                let stream_id =
                                    commit_stream_id(&candidate.candidate.commit_ref.coord);
                                frontier.insert(
                                    stream_id.clone(),
                                    candidate.candidate.commit_ref.clone(),
                                );
                                applied_devices.insert(stream_id);
                                row_changes.extend(changes);
                                changesets_applied =
                                    changesets_applied.checked_add(1).ok_or_else(|| {
                                        StorePullError::Database(
                                            "Store apply count exceeded u64".to_string(),
                                        )
                                    })?;
                                blocked.remove(&key);
                                progressed = true;
                            }
                            ApplyOutcome::Held(reason) => {
                                if matches!(reason, HeldStorePositionReason::BlobDownloadFailed) {
                                    asset_downloads_failed = true;
                                }
                                let held_position =
                                    held_commit(&candidate.candidate.commit_ref, reason);
                                candidates.insert(key.clone(), candidate);
                                blocked.insert(key, held_position);
                            }
                        }
                    }
                }
            }
            if !progressed {
                break;
            }
        }

        held.extend(blocked.into_values());
        held.sort_by(|left, right| {
            (left.coordinate.device_id(), left.coordinate.seq())
                .cmp(&(right.coordinate.device_id(), right.coordinate.seq()))
        });
        let local_blob_cleanup_pending =
            local_cleanup::drain(db, store_dir).await.map_err(|error| {
                StorePullError::Database(format!("drain local blob cleanup intents: {error}"))
            })?;
        let devices_pulled = u64::try_from(applied_devices.len())
            .map_err(|_| StorePullError::Database("pulled device count exceeds u64".to_string()))?;

        Ok(StorePullResult {
            changesets_applied,
            devices_pulled,
            held_positions: held,
            visible_heads,
            serial_head: None,
            row_changes,
            asset_downloads_failed,
            local_blob_cleanup_pending,
            frontier,
        })
    })
}

struct MergeStreamDiscovery {
    latest_head: Option<StoreDeviceHead>,
    commits: Vec<(
        super::store_commit::StoreDeviceHeadRef,
        StoreDeviceHead,
        StoreBatchCommitRef,
        StoreBatchCommit,
    )>,
    block: Option<MergeStreamBlock>,
}

enum MergeStreamBlock {
    Unauthenticated(HeldStorePosition),
    Authenticated(HeldStorePosition),
}

impl MergeStreamBlock {
    fn into_position(self) -> HeldStorePosition {
        match self {
            Self::Unauthenticated(position) | Self::Authenticated(position) => position,
        }
    }
}

async fn load_active_merge_registrations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
    let durable = db.activated_store_device_registration_records().await?;
    let mut verified = Vec::with_capacity(durable.len());
    for (reference, expected) in durable {
        let opened = load_registration_ref(storage, root, &reference).await?;
        if opened.value != expected {
            return Err(StorePullError::Database(format!(
                "activated Store registration {} differs from its exact remote bytes",
                reference.device_id
            )));
        }
        if !matches!(
            opened.value.store_commits,
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { .. }
            }
        ) {
            return Err(StorePullError::Database(format!(
                "activated Store registration {} has no Merge announcement anchor",
                reference.device_id
            )));
        }
        verified.push((reference, opened.value));
    }
    Ok(verified)
}

async fn discover_merge_owner_recoveries(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    protocol: &super::store_commit::StoreProtocolRoot,
    membership: &MembershipChain,
) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
    if membership
        .active_owner_grant(&protocol.descriptor.founder_pubkey)
        .as_ref()
        != Some(&protocol.descriptor.founder_grant)
    {
        return Ok(Vec::new());
    }
    let super::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
        &protocol.descriptor.founder_recovery
    else {
        return Err(StorePullError::Database(
            "Store founder recovery authority has no recovery stream".into(),
        ));
    };
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let authority = RegistrationPredecessorAuthority::MergeConcurrent(membership);
    let mut slot = first_slot.clone();
    let mut predecessor: Option<OwnerRecoveryNodeRef> = None;
    let mut sequence = 1_u64;
    let mut recovered = Vec::new();
    loop {
        let prefix = super::store_commit::owner_recovery_semantic_prefix(
            &protocol.descriptor.founder_pubkey,
            protocol.descriptor.founder_grant.clone(),
            sequence,
        );
        let (bytes, object) = match storage.read_protocol_slot(&context, &slot, &prefix).await {
            Ok(opened) => opened,
            Err(StorageError::NotFound(_)) => break,
            Err(error) => return Err(StoreObjectError::Storage(error).into()),
        };
        let unverified: OwnerRecoveryNode = serde_json::from_slice(&bytes)
            .map_err(|error| StorePullError::Database(format!("Owner recovery node: {error}")))?;
        let reference = OwnerRecoveryNodeRef {
            owner_pubkey: unverified.owner_pubkey.clone(),
            owner_grant: unverified.owner_grant.clone(),
            sequence: unverified.sequence,
            node_hash: unverified.node_hash(),
            object,
        };
        let node = OwnerRecoveryNode::parse_at(&bytes, root, &reference)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        if reference.owner_pubkey != protocol.descriptor.founder_pubkey
            || reference.owner_grant != protocol.descriptor.founder_grant
            || reference.sequence != sequence
            || node.predecessor != predecessor
            || !authority.verifies_owner(&node.membership, &node.owner_pubkey, &node.owner_grant)
        {
            return Err(StorePullError::Database(
                "Owner recovery stream differs from its root-anchored authority".into(),
            ));
        }
        let registration = load_registration_ref(storage, root, &node.readiness.registration)
            .await?
            .value;
        let initial_ack =
            load_store_ack_ref(storage, root, &node.readiness.initial_ack, &registration)
                .await?
                .value;
        let origin_matches = matches!(
            &registration.origin,
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id,
                recovery_slot,
                owner_grant,
            } if *recovery_id == node.recovery_id
                && recovery_slot == reference.slot()
                && owner_grant == &node.owner_grant
        );
        if !origin_matches
            || registration.author_pubkey != node.owner_pubkey
            || initial_ack.sequence != 1
            || initial_ack.successor.predecessor.is_some()
            || initial_ack.store_cut != node.readiness.bootstrap_cut
            || initial_ack.registration != node.readiness.registration
        {
            return Err(StorePullError::Database(
                "Owner recovery readiness differs from its registration graph".into(),
            ));
        }
        recovered.push((node.readiness.registration.clone(), registration));
        slot = node.next_slot.clone();
        predecessor = Some(reference);
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| StorePullError::Database("Owner recovery sequence overflow".into()))?;
    }
    Ok(recovered)
}

async fn discover_merge_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &StoreDeviceRegistrationRef,
    registration: &StoreDeviceRegistration,
    inactive_accepted_cut: Option<&StoreHistoryCut>,
) -> Result<MergeStreamDiscovery, StorePullError> {
    let StoreCommitAnchor::MergeConcurrent {
        announcements: DeviceStreamAnchor::StoreAnnouncements { first_slot },
    } = &registration.store_commits
    else {
        return Err(StorePullError::Database(format!(
            "Store registration {} has no Merge announcement anchor",
            registration.device_id
        )));
    };
    let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        registration_ref,
        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let maximum_sequence = match inactive_accepted_cut {
        None => None,
        Some(StoreHistoryCut::MergeConcurrent(cut)) => Some(
            cut.get(&stream_id)
                .map_or(0, |reference| reference.coord.sequence()),
        ),
        Some(StoreHistoryCut::Serial(_)) => {
            return Err(StorePullError::Database(
                "Merge device state carries a Serial inactive cutoff".to_string(),
            ));
        }
    };
    let activation = registration
        .store_announcement_activation(registration_ref)
        .map_err(|error| StorePullError::Database(error.to_string()))?
        .activation_id();
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let mut slot = first_slot.clone();
    let mut predecessor = None;
    let mut sequence = 1_u64;
    let mut latest_head = None;
    let mut commits = Vec::new();
    let mut block = None;
    let mut visited = BTreeSet::new();

    loop {
        if maximum_sequence.is_some_and(|maximum| sequence > maximum) {
            break;
        }
        if !visited.insert(slot.clone()) {
            return Err(StorePullError::Database(format!(
                "Store announcement stream {stream_id} repeats a reserved slot"
            )));
        }
        let semantic_prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
        let (bytes, object) = match storage
            .read_protocol_slot(&context, &slot, &semantic_prefix)
            .await
        {
            Ok(opened) => opened,
            Err(StorageError::NotFound(_)) => break,
            Err(error) => return Err(StoreObjectError::Storage(error).into()),
        };
        let unverified: StoreDeviceHead = match serde_json::from_slice(&bytes) {
            Ok(head) => head,
            Err(error) => {
                block = Some(MergeStreamBlock::Unauthenticated(HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: stream_id.to_string(),
                        seq: sequence,
                        head_hash: ObjectHash::digest(&bytes),
                    },
                    reason: HeldStorePositionReason::InvalidObject(error.to_string()),
                }));
                break;
            }
        };
        let authenticated = unverified.signature_is_valid_for(registration);
        let coord_matches = matches!(
            unverified.commit.coord,
            StoreCommitCoord::MergeConcurrent {
                stream_id: declared,
                sequence: declared_sequence,
            } if declared == stream_id && declared_sequence == sequence
        );
        if !coord_matches
            || unverified.author_registration != *registration_ref
            || unverified.successor.activation != activation
            || unverified.successor.predecessor != predecessor
        {
            let position = HeldStorePosition {
                coordinate: HeldStoreCoordinate::Head {
                    device_id: stream_id.to_string(),
                    seq: sequence,
                    head_hash: unverified.head_hash(),
                },
                reason: HeldStorePositionReason::WrongSlot(
                    "Store head differs from its activated successor chain".to_string(),
                ),
            };
            block = Some(if authenticated {
                MergeStreamBlock::Authenticated(position)
            } else {
                MergeStreamBlock::Unauthenticated(position)
            });
            break;
        }
        let head = match StoreDeviceHead::parse_at(
            &bytes,
            root.store_root_hash,
            registration,
            &unverified.commit,
        ) {
            Ok(head) => head,
            Err(error) => {
                let position = HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: stream_id.to_string(),
                        seq: sequence,
                        head_hash: unverified.head_hash(),
                    },
                    reason: held_protocol_error(error),
                };
                block = Some(if authenticated {
                    MergeStreamBlock::Authenticated(position)
                } else {
                    MergeStreamBlock::Unauthenticated(position)
                });
                break;
            }
        };
        let commit = match load_commit_ref(
            storage,
            root.store_root_hash,
            &unverified.commit,
            registration,
        )
        .await
        {
            Ok(commit) => commit,
            Err(error) => {
                block = Some(MergeStreamBlock::Authenticated(held_commit(
                    &unverified.commit,
                    held_object_error(error),
                )));
                break;
            }
        };
        let next_slot = head.successor.next_slot.clone();
        let head_ref = super::store_commit::StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: object.clone(),
        };
        predecessor = Some(object);
        sequence = sequence.checked_add(1).ok_or_else(|| {
            StorePullError::Database(format!(
                "Store announcement stream {stream_id} sequence overflow"
            ))
        })?;
        commits.push((head_ref, head.clone(), head.commit.clone(), commit.value));
        latest_head = Some(head);
        slot = next_slot;
    }

    Ok(MergeStreamDiscovery {
        latest_head,
        commits,
        block,
    })
}

fn held_protocol_error(error: StoreProtocolError) -> HeldStorePositionReason {
    match error {
        StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
        StoreProtocolError::RelocatedSlot { .. }
        | StoreProtocolError::RelocatedPackage { .. }
        | StoreProtocolError::StoreRootMismatch { .. }
        | StoreProtocolError::StoreMismatch { .. }
        | StoreProtocolError::FounderMismatch { .. } => {
            HeldStorePositionReason::WrongSlot(error.to_string())
        }
        error => HeldStorePositionReason::InvalidObject(error.to_string()),
    }
}
pub(crate) async fn load_commit_with_author(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
) -> Result<(StoreBatchCommit, StoreDeviceRegistration), StoreObjectError> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    load_commit_with_author_at_root(storage, root, &root_value, reference).await
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitCoverageError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
}

pub(crate) async fn commit_position_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    covering: &StoreBatchCommitRef,
    covered: &StoreBatchCommitRef,
) -> Result<bool, CommitCoverageError> {
    let same_stream = match (&covering.coord, &covered.coord) {
        (
            super::store_commit::StoreCommitCoord::MergeConcurrent {
                stream_id: covering,
                ..
            },
            super::store_commit::StoreCommitCoord::MergeConcurrent {
                stream_id: covered, ..
            },
        ) => covering == covered,
        (
            super::store_commit::StoreCommitCoord::Serial { .. },
            super::store_commit::StoreCommitCoord::Serial { .. },
        ) => true,
        _ => false,
    };
    if !same_stream || covering.coord.sequence() < covered.coord.sequence() {
        return Ok(false);
    }
    let mut cursor = covering.clone();
    while cursor.coord.sequence() > covered.coord.sequence() {
        let (commit, _) = load_commit_with_author(storage, root, &cursor).await?;
        cursor =
            commit
                .order
                .predecessor()
                .cloned()
                .ok_or(CommitCoverageError::MissingAncestry {
                    commit_hash: cursor.commit_hash,
                })?;
    }
    Ok(cursor == *covered)
}

fn coverage_error(error: CommitCoverageError) -> StorePullError {
    match error {
        CommitCoverageError::Object(error) => StorePullError::Object(error),
        CommitCoverageError::MissingAncestry { commit_hash } => StorePullError::Database(format!(
            "exact Store ancestry is missing commit {commit_hash}"
        )),
    }
}

pub(crate) async fn history_cut_covers(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    covered: &StoreBatchCommitRef,
) -> Result<bool, StorePullError> {
    let covering = match (cut, &covered.coord) {
        (
            StoreHistoryCut::MergeConcurrent(frontier),
            StoreCommitCoord::MergeConcurrent { stream_id, .. },
        ) => frontier.get(stream_id),
        (
            StoreHistoryCut::Serial(StoreSerialPredecessor::Commit(reference)),
            StoreCommitCoord::Serial { .. },
        ) => Some(reference),
        _ => None,
    };
    match covering {
        Some(covering) => commit_position_covers(storage, root, covering, covered)
            .await
            .map_err(coverage_error),
        None => Ok(false),
    }
}

fn verify_provider_access_evidence<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    verified_root: &'a super::store_commit::StoreProtocolRoot,
    access: &'a super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &'a super::provider::ProviderAdminGrantRecord,
    administrator: &'a StoreDeviceRegistration,
    accepted_predecessor: Option<&'a VerifiedAcceptedPredecessor<'a>>,
) -> StorePullFuture<'a, StoreBatchCommit> {
    Box::pin(verify_provider_access_evidence_impl(
        storage,
        root,
        verified_root,
        access,
        provider_admin,
        administrator,
        accepted_predecessor,
    ))
}

async fn verify_provider_access_evidence_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    verified_root: &super::store_commit::StoreProtocolRoot,
    access: &super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &super::provider::ProviderAdminGrantRecord,
    administrator: &StoreDeviceRegistration,
    accepted_predecessor: Option<&VerifiedAcceptedPredecessor<'_>>,
) -> Result<StoreBatchCommit, StorePullError> {
    let grant = super::store_objects::load_provider_access_grant_ref_with_root(
        storage,
        root,
        verified_root,
        &access.grant_ref,
        administrator,
    )
    .await?;
    if grant.value != access.grant {
        return Err(StorePullError::Database(
            "device provider approval embeds a different access grant than its exact reference"
                .to_string(),
        ));
    }
    if let Some(verified) = accepted_predecessor
        .map(|predecessor| predecessor.serial_history_commit(&access.activation))
        .transpose()?
        .flatten()
    {
        let activation = &verified.commit;
        if activation.provider_access_grants() != std::slice::from_ref(&access.grant_ref)
            || activation.author_registration != access.grant.administrator
            || verified.author != *administrator
        {
            return Err(StorePullError::Database(
                "device provider approval activation is not the administrator's exact sole access grant"
                    .to_string(),
            ));
        }
        let provider_admin_state = &verified.authorization_before.provider_admin;
        if !provider_admin_state.authorizes(
            &access.grant.administrator_grant,
            &activation.author_registration,
        ) || provider_admin_state
            .records()
            .get(&access.grant.administrator_grant)
            != Some(provider_admin)
        {
            return Err(StorePullError::Database(
                "device provider approval activation lacks exact predecessor provider-administrator authority"
                    .to_string(),
            ));
        }
        return Ok(activation.clone());
    }
    if let Some(verified) = accepted_predecessor
        .map(|predecessor| predecessor.merge_history_commit(&access.activation))
        .transpose()?
        .flatten()
    {
        let activation = &verified.commit;
        if activation.provider_access_grants() != std::slice::from_ref(&access.grant_ref)
            || activation.author_registration != access.grant.administrator
        {
            return Err(StorePullError::Database(
                "device provider approval activation is not the administrator's exact sole access grant"
                    .to_string(),
            ));
        }
        let authority =
            RegistrationPredecessorAuthority::MergeConcurrent(&verified.predecessor_membership);
        if !authority.verifies_provider_administrator(
            &access.grant.administrator_grant,
            &activation.author_registration,
            provider_admin,
        ) {
            return Err(StorePullError::Database(
                "device provider approval activation lacks exact predecessor provider-administrator authority"
                    .to_string(),
            ));
        }
        return Ok(activation.clone());
    }
    let (activation, author) =
        load_commit_with_author_at_root(storage, root, verified_root, &access.activation).await?;
    if activation.provider_access_grants() != std::slice::from_ref(&access.grant_ref)
        || activation.author_registration != access.grant.administrator
        || author != *administrator
    {
        return Err(StorePullError::Database(
            "device provider approval activation is not the administrator's exact sole access grant"
                .to_string(),
        ));
    }
    let authorization =
        load_device_join_authorization(storage, root, &activation.membership_state).await?;
    let authority = match &authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            RegistrationPredecessorAuthority::MergeConcurrent(chain)
        }
        DeviceJoinBootstrapAuthorization::Serial {
            position,
            authorization,
            ..
        } => RegistrationPredecessorAuthority::Serial {
            authorization,
            position: position.clone(),
            history: SerialAuthorizationHistory::ExactPredecessor,
        },
    };
    if !authority.verifies_provider_administrator(
        &access.grant.administrator_grant,
        &activation.author_registration,
        provider_admin,
    ) {
        return Err(StorePullError::Database(
            "device provider approval activation lacks exact predecessor provider-administrator authority"
                .to_string(),
        ));
    }
    Ok(activation)
}

fn load_verified_device_join_attempt_evidence_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::DeviceJoinAttemptRef,
    owner: &'a StoreDeviceRegistration,
    accepted_predecessor: Option<&'a VerifiedAcceptedPredecessor<'a>>,
) -> StorePullFuture<'a, VerifiedObject<DeviceJoinAttempt>> {
    Box::pin(async move {
        let attempt =
            load_owner_signed_device_join_attempt_ref(storage, root, reference, owner).await?;
        let verified_root = load_store_protocol_root(storage, root).await?;
        if attempt.value.store_root != *root {
            return Err(StorePullError::Database(
                "device join attempt names another Store root".to_string(),
            ));
        }
        let offer = &attempt.value.provider_approval.request.offer;
        let administrator =
            load_registration_ref(storage, root, &offer.provider_admin.administrator)
                .await?
                .value;
        attempt
            .value
            .provider_approval
            .verify(&verified_root, owner, &administrator)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        verify_provider_access_evidence(
            storage,
            root,
            &verified_root.value,
            &attempt.value.provider_approval.access_grant,
            &offer.provider_admin,
            &administrator,
            accepted_predecessor,
        )
        .await?;
        if !history_cut_covers(
            storage,
            root,
            &attempt.value.bootstrap_cut,
            &attempt.value.provider_approval.access_grant.activation,
        )
        .await?
        {
            return Err(StorePullError::Database(
            "device join attempt predecessor cut does not include its provider-access activation"
                .to_string(),
        ));
        }
        Ok(attempt)
    })
}

pub(crate) fn load_verified_device_join_attempt_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    reference: &'a super::store_commit::DeviceJoinAttemptRef,
    owner: &'a StoreDeviceRegistration,
) -> StorePullFuture<'a, VerifiedObject<DeviceJoinAttempt>> {
    Box::pin(async move {
        let attempt =
            load_verified_device_join_attempt_evidence_ref(storage, root, reference, owner, None)
                .await?;
        match &attempt.value.bootstrap_cut {
            StoreHistoryCut::MergeConcurrent(_) => {
                Box::pin(verify_store_history_state(
                    storage,
                    None,
                    root,
                    &attempt.value.bootstrap_cut,
                    &attempt.value.membership,
                ))
                .await?;
            }
            StoreHistoryCut::Serial(cut_position) => {
                let authorization =
                    load_device_join_authorization(storage, root, &attempt.value.membership)
                        .await?;
                let DeviceJoinBootstrapAuthorization::Serial { position, .. } = authorization
                else {
                    return Err(StorePullError::Database(
                        "Serial device join attempt carries Merge membership authority".to_string(),
                    ));
                };
                if &position != cut_position {
                    return Err(StorePullError::Serial(
                    "device join attempt cut differs from its exact Serial authorization position"
                        .to_string(),
                ));
                }
            }
        }
        Ok(attempt)
    })
}

pub(crate) async fn verify_accepted_provider_access_activation(
    storage: &dyn SyncStorage,
    coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    access: &super::provider::ActivatedStoreMemberProviderAccessGrant,
    provider_admin: &super::provider::ProviderAdminGrantRecord,
    administrator: &StoreDeviceRegistration,
) -> Result<(), StorePullError> {
    let root_value = load_store_protocol_root(storage, root).await?;
    let activation = verify_provider_access_evidence(
        storage,
        root,
        &root_value.value,
        access,
        provider_admin,
        administrator,
        None,
    )
    .await?;
    let accepted = match root_value.value.descriptor.write_policy {
        crate::WritePolicy::MergeConcurrent => {
            if coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge provider-access verification received Serial coordination".to_string(),
                ));
            }
            let membership =
                load_merge_predecessor_membership(storage, root, &activation.membership_state)
                    .await
                    .map_err(|error| match error {
                        RegistrationLoadError::Object(error) => StorePullError::Object(error),
                        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                    })?;
            current_merge_history_contains(
                storage,
                root,
                &root_value.value,
                &membership,
                &access.activation,
            )
            .await?
        }
        crate::WritePolicy::Serial => {
            let coordination = coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "provider-access verification requires coordination capability".to_string(),
                )
            })?;
            let head = read_serial_head(storage, coordination, root).await?;
            load_authorized_serial_chain(storage, root, &head.head)
                .await?
                .iter()
                .any(|accepted| accepted.commit_ref == access.activation)
        }
    };
    if !accepted {
        return Err(StorePullError::Database(
            "device provider approval activation is absent from current accepted Store history"
                .to_string(),
        ));
    }
    Ok(())
}

async fn current_merge_history_contains(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    membership: &MembershipChain,
    expected: &StoreBatchCommitRef,
) -> Result<bool, StorePullError> {
    let initial = verify_merge_history_refs(storage, root, [expected.clone()]).await?;
    let mut state = initial
        .commits
        .get(expected)
        .ok_or_else(|| {
            StorePullError::Database(
                "provider-access activation is absent from its verified Merge graph".to_string(),
            )
        })?
        .state_after
        .clone();
    let mut registrations = BTreeMap::new();
    let founder = load_founder_registration_with_root(storage, root, root_value).await?;
    let founder_ref = StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object);
    registrations.insert(founder_ref.device_id, (founder_ref, founder.value));
    for recovered in discover_merge_owner_recoveries(storage, root, root_value, membership).await? {
        registrations.insert(recovered.0.device_id, recovered);
    }
    load_state_registrations(storage, root, &state, &mut registrations).await?;

    let mut accepted = BTreeMap::new();
    let mut observed_states = BTreeSet::new();
    loop {
        let mut next = BTreeMap::new();
        for (registration_ref, registration) in registrations.values() {
            let inactive_cut = match state.devices.get(&registration_ref.device_id) {
                Some(record) if record.registration != *registration_ref => {
                    return Err(StorePullError::Database(
                        "current Merge device state names another registration revision"
                            .to_string(),
                    ));
                }
                Some(record) => match &record.status {
                    StoreDeviceStatus::Active => None,
                    StoreDeviceStatus::Inactive { accepted_cut, .. } => Some(accepted_cut),
                },
                None => None,
            };
            let discovered =
                discover_merge_stream(storage, root, registration_ref, registration, inactive_cut)
                    .await?;
            if matches!(discovered.block, Some(MergeStreamBlock::Authenticated(_))) {
                return Err(StorePullError::Database(
                    "an authenticated Merge stream position cannot be verified".to_string(),
                ));
            }
            if let Some((_, _, reference, _)) = discovered.commits.last() {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = reference.coord else {
                    return Err(StorePullError::Database(
                        "Merge stream discovery returned a Serial commit".to_string(),
                    ));
                };
                next.insert(stream_id, reference.clone());
            }
        }
        let history = verify_merge_history_refs(storage, root, next.values().cloned()).await?;
        let next_state = if next.is_empty() {
            history.genesis.clone()
        } else {
            ResolvedStoreDeviceState::merge(
                next.values()
                    .map(|reference| {
                        history
                            .commits
                            .get(reference)
                            .map(|commit| commit.state_after.clone())
                            .ok_or_else(|| {
                                StorePullError::Database(
                                    "current Merge frontier is absent from its verified graph"
                                        .to_string(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?
        };
        let registration_count = registrations.len();
        load_state_registrations(storage, root, &next_state, &mut registrations).await?;
        let stable =
            next == accepted && next_state == state && registrations.len() == registration_count;
        if stable {
            return Ok(history.commits.contains_key(expected));
        }
        let state_fingerprint = ObjectHash::digest(
            &serde_json::to_vec(&(&next, &next_state))
                .map_err(|error| StorePullError::Database(error.to_string()))?,
        );
        if !observed_states.insert(state_fingerprint) {
            return Err(StorePullError::Database(
                "current Merge authority discovery does not reach one stable frontier".to_string(),
            ));
        }
        accepted = next;
        state = next_state;
    }
}

async fn load_state_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &ResolvedStoreDeviceState,
    registrations: &mut BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
) -> Result<(), StorePullError> {
    for (device_id, record) in &state.devices {
        if registrations
            .get(device_id)
            .is_some_and(|(reference, _)| reference == &record.registration)
        {
            continue;
        }
        let registration = load_registration_ref(storage, root, &record.registration).await?;
        if registration.value.device_id != *device_id {
            return Err(StorePullError::Database(
                "current Merge device state registration has another device id".to_string(),
            ));
        }
        registrations.insert(
            *device_id,
            (record.registration.clone(), registration.value),
        );
    }
    Ok(())
}

fn load_commit_with_author_at_root<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    root_value: &'a super::store_commit::StoreProtocolRoot,
    reference: &'a StoreBatchCommitRef,
) -> super::store_objects::StoreObjectFuture<'a, (StoreBatchCommit, StoreDeviceRegistration)> {
    Box::pin(load_commit_with_author_at_root_impl(
        storage, root, root_value, reference,
    ))
}

async fn load_commit_with_author_at_root_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    reference: &StoreBatchCommitRef,
) -> Result<(StoreBatchCommit, StoreDeviceRegistration), StoreObjectError> {
    let semantic_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let bytes = storage
        .read_protocol_object(&context, &reference.object, &semantic_prefix)
        .await
        .map_err(StoreObjectError::Storage)?;
    #[derive(serde::Deserialize)]
    struct StoreCommitAuthorProjection {
        author_registration: StoreDeviceRegistrationRef,
    }

    let parse_bytes = bytes.clone();
    let author_reference = run_blocking_object_verification(
        &semantic_prefix,
        &reference.object,
        Box::new(move || {
            serde_json::from_slice::<StoreCommitAuthorProjection>(&parse_bytes)
                .map(|projection| projection.author_registration)
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        }),
    )
    .await?;
    let author = load_registration_ref_with_root(storage, root, root_value, &author_reference)
        .await?
        .value;
    let expected_reference = reference.clone();
    let expected_author = author.clone();
    let store_root_hash = root.store_root_hash;
    let verify_bytes = bytes;
    let commit = run_blocking_object_verification(
        &semantic_prefix,
        &reference.object,
        Box::new(move || {
            let commit = StoreBatchCommit::parse_at(
                &verify_bytes,
                store_root_hash,
                &expected_reference.coord,
                &expected_author,
            )?;
            expected_reference.verify_commit(&commit)?;
            Ok(commit)
        }),
    )
    .await?;
    Ok((commit, author))
}

pub(crate) struct DeviceJoinBootstrapCommit {
    pub reference: StoreBatchCommitRef,
    pub commit: StoreBatchCommit,
    pub registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub device_operations: VerifiedStoreDeviceOperations,
    pub activation: DeviceJoinBootstrapActivation,
}

pub(crate) enum DeviceJoinBootstrapActivation {
    MergeConcurrent {
        head: StoreDeviceHead,
        object: ExactObjectRef,
        history_summary: RetainedVerifiedMergeHistorySummary,
    },
    Serial,
}

pub(crate) struct DeviceJoinBootstrapPlan {
    pub founder_reference: StoreDeviceRegistrationRef,
    pub founder: StoreDeviceRegistration,
    pub founder_bytes: Vec<u8>,
    pub genesis: ResolvedStoreDeviceState,
    pub coverage: StoreHistoryCut,
    pub commits: Vec<DeviceJoinBootstrapCommit>,
}

fn history_cut_references(cut: &StoreHistoryCut) -> Vec<StoreBatchCommitRef> {
    match cut {
        StoreHistoryCut::MergeConcurrent(frontier) => frontier.values().cloned().collect(),
        StoreHistoryCut::Serial(StoreSerialPredecessor::Commit(reference)) => {
            vec![reference.clone()]
        }
        StoreHistoryCut::Serial(StoreSerialPredecessor::Genesis { .. }) => Vec::new(),
    }
}

fn commit_predecessor_references(commit: &StoreBatchCommit) -> Vec<StoreBatchCommitRef> {
    match &commit.order {
        super::store_commit::StoreCommitOrder::MergeConcurrent {
            predecessor,
            dependencies,
            ..
        } => predecessor
            .iter()
            .chain(dependencies.values())
            .cloned()
            .collect(),
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: StoreSerialPredecessor::Commit(reference),
            ..
        } => vec![reference.clone()],
        super::store_commit::StoreCommitOrder::Serial {
            predecessor: StoreSerialPredecessor::Genesis { .. },
            ..
        } => Vec::new(),
    }
}

fn registration_recovery_cursor(
    origin: &StoreDeviceRegistrationOrigin,
    activation: &super::store_commit::StoreDeviceRegistrationActivation,
) -> Result<Option<super::store_commit::OwnerRecoveryCursor>, StoreProtocolError> {
    match (origin, activation) {
        (
            StoreDeviceRegistrationOrigin::Recovery {
                recovery_id,
                recovery_slot,
                owner_grant,
            },
            StoreDeviceRegistrationActivation::Recovery {
                recovery_id: activated_recovery_id,
                node,
            },
        ) if recovery_id == activated_recovery_id
            && recovery_slot == node.object.slot()
            && owner_grant == &node.owner_grant =>
        {
            Ok(Some(OwnerRecoveryCursor {
                owner_grant: owner_grant.clone(),
                position: OwnerRecoveryPosition::At { node: node.clone() },
            }))
        }
        (
            StoreDeviceRegistrationOrigin::Join {
                attempt_id,
                outcome_slot,
                ..
            },
            StoreDeviceRegistrationActivation::Join {
                attempt_id: activated_attempt_id,
                outcome,
            },
        ) if attempt_id == activated_attempt_id && outcome_slot == outcome.slot() => Ok(None),
        (
            StoreDeviceRegistrationOrigin::Founder { .. },
            StoreDeviceRegistrationActivation::Founder { .. },
        ) => Ok(None),
        _ => Err(StoreProtocolError::Malformed(
            "registration origin differs from its exact activation authority".to_string(),
        )),
    }
}

fn predecessor_with_recovery_author(
    mut predecessor: ResolvedStoreDeviceState,
    commit: &StoreBatchCommit,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
) -> Result<(ResolvedStoreDeviceState, Option<StoreDeviceRegistrationRef>), StoreProtocolError> {
    if commit.device_registrations().len() != registrations.len() {
        return Err(StoreProtocolError::Malformed(
            "verified registrations do not cover every activation".to_string(),
        ));
    }
    for (activated, (registration, authority)) in
        commit.device_registrations().iter().zip(registrations)
    {
        activated.registration.verify_registration(registration)?;
        if activated.registration == commit.author_registration {
            if let Some(cursor) = registration_recovery_cursor(&registration.origin, authority)? {
                predecessor = predecessor
                    .activate_registration(activated.registration.clone(), Some(cursor))?;
                return Ok((predecessor, Some(activated.registration.clone())));
            }
        }
    }
    Ok((predecessor, None))
}

async fn verify_commit_owner_recovery_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    serial_predecessor: Option<(&SerialAuthorizationState, &ResolvedStoreDeviceState)>,
) -> Result<
    Option<(
        super::membership::MembershipGrantId,
        super::store_commit::OwnerRecoveryActivationId,
    )>,
    StorePullError,
> {
    if let Some(super::store_commit::StoreControl::SerialMembership { entry }) = commit.control() {
        let super::membership::SerialMembershipChange::SetMember {
            user_pubkey,
            role:
                super::membership::StoreMembershipRoleGrant::Owner {
                    recovery: super::membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
                },
            grant_id,
            replaces,
            ..
        } = &entry.change
        else {
            return Ok(None);
        };
        let Some((authorization, devices)) = serial_predecessor else {
            return Err(StorePullError::Serial(
                "Serial Owner promotion has no verified predecessor authority".to_string(),
            ));
        };
        let request = &acceptance.request;
        let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
        let candidate = load_registration_ref(storage, root, &request.member_registration).await?;
        request
            .verify(root, &promoter.value)
            .and_then(|()| acceptance.verify(&candidate.value))
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
        let super::store_commit::OwnerPromotionRequestActivation::Serial {
            commit: request_commit_ref,
        } = &acceptance.activation
        else {
            return Err(StorePullError::Serial(
                "Serial Owner promotion carries Merge activation".to_string(),
            ));
        };
        if commit.order.predecessor() != Some(request_commit_ref)
            || user_pubkey != &request.member_pubkey
            || grant_id != &request.intended_owner_grant
            || replaces != &BTreeSet::from([request.member_grant.clone()])
            || authorization
                .membership
                .active_owner_grant(&promoter.value.author_pubkey)
                .as_ref()
                != Some(&request.promoter_owner_grant)
            || authorization
                .membership
                .active_grant_ids(&request.member_pubkey)
                != BTreeSet::from([request.member_grant.clone()])
            || !authorization
                .membership
                .is_member_grant(&request.member_pubkey, &request.member_grant)
            || !device_state_has_active_registration(devices, &request.promoter_registration)
            || !device_state_has_active_registration(devices, &request.member_registration)
        {
            return Err(StorePullError::Serial(
                "Serial Owner promotion differs from its exact predecessor authority".to_string(),
            ));
        }
        let request_commit = load_commit_ref(
            storage,
            root.store_root_hash,
            request_commit_ref,
            &promoter.value,
        )
        .await?;
        if request_commit.value.owner_promotion_request() != Some(request)
            || request_commit.value.membership_state != request.predecessor_membership
            || request_commit.value.device_state != request.predecessor_devices
            || request_commit.value.author_registration != request.promoter_registration
        {
            return Err(StorePullError::Serial(
                "Serial Owner-promotion request commit differs from its acceptance".to_string(),
            ));
        }
        return super::store_commit::OwnerRecoveryActivationId::derive(
            root,
            &request.member_pubkey,
            grant_id,
            acceptance.anchors.recovery(),
        )
        .map(|activation| Some((grant_id.clone(), activation)))
        .map_err(|error| StorePullError::Serial(error.to_string()));
    }

    let mut recoveries = commit.stream_activations().iter().filter_map(|activation| {
        let super::store_commit::StreamActivation::GrantAuthorized {
            author_registration,
            grant_id,
            anchor: anchor @ super::store_commit::GrantStreamAnchor::OwnerRecovery { .. },
            ..
        } = activation
        else {
            return None;
        };
        Some((author_registration, grant_id, anchor))
    });
    let Some((registration_ref, grant_id, anchor)) = recoveries.next() else {
        return Ok(None);
    };
    if recoveries.next().is_some() {
        return Err(StorePullError::Database(
            "Store commit activates more than one Owner recovery stream".to_string(),
        ));
    }
    let registration = load_registration_ref(storage, root, registration_ref).await?;
    super::store_commit::OwnerRecoveryActivationId::derive(
        root,
        &registration.value.author_pubkey,
        grant_id,
        anchor,
    )
    .map(|activation| Some((grant_id.clone(), activation)))
    .map_err(|error| StorePullError::Database(error.to_string()))
}

fn apply_verified_device_lifecycle(
    mut state: ResolvedStoreDeviceState,
    commit: &StoreBatchCommit,
    registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
    preactivated: Option<&StoreDeviceRegistrationRef>,
    owner_recovery: Option<(
        super::membership::MembershipGrantId,
        super::store_commit::OwnerRecoveryActivationId,
    )>,
) -> Result<ResolvedStoreDeviceState, StoreProtocolError> {
    if commit.device_registrations().len() != registrations.len() {
        return Err(StoreProtocolError::Malformed(
            "verified registrations do not cover every activation".to_string(),
        ));
    }
    for (activated, (registration, authority)) in
        commit.device_registrations().iter().zip(registrations)
    {
        activated.registration.verify_registration(registration)?;
        if preactivated != Some(&activated.registration) {
            state = state.activate_registration(
                activated.registration.clone(),
                registration_recovery_cursor(&registration.origin, authority)?,
            )?;
        }
    }
    for retirement in commit.device_retirements() {
        state = state.self_retire(retirement.clone())?;
    }
    if let Some((grant_id, activation)) = owner_recovery {
        state = state.activate_owner_recovery(grant_id, activation)?;
    }
    Ok(state)
}

fn verified_merge_predecessor_state(
    genesis: &ResolvedStoreDeviceState,
    states: &BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    commit: &StoreBatchCommit,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let super::store_commit::StoreCommitOrder::MergeConcurrent {
        predecessor,
        dependencies,
        ..
    } = &commit.order
    else {
        return Err(StorePullError::Database(
            "Merge history contains a Serial commit order".to_string(),
        ));
    };
    let mut predecessor_refs = dependencies.values().collect::<Vec<_>>();
    predecessor_refs.extend(predecessor.iter());
    let predecessor_state = if predecessor_refs.is_empty() {
        genesis.clone()
    } else {
        ResolvedStoreDeviceState::merge(
            predecessor_refs
                .into_iter()
                .map(|dependency| {
                    states.get(dependency).cloned().ok_or_else(|| {
                        StorePullError::Database(
                            "Merge history has an unresolved predecessor state".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let mut frontier = dependencies.clone();
    if let Some(predecessor) = predecessor {
        let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord else {
            return Err(StorePullError::Database(
                "Merge predecessor carries a Serial coordinate".to_string(),
            ));
        };
        if frontier
            .insert(stream_id, predecessor.clone())
            .is_some_and(|existing| existing != *predecessor)
        {
            return Err(StorePullError::Database(
                "Merge predecessor conflicts with its dependency cut".to_string(),
            ));
        }
    }
    let expected_state = StoreDeviceStateRef::merge_concurrent(
        CommitFrontier::MergeConcurrent(frontier),
        &predecessor_state,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if commit.device_state != expected_state {
        return Err(StorePullError::Database(
            "Merge commit names another predecessor device state".to_string(),
        ));
    }
    Ok(predecessor_state)
}

struct VerifiedMergeHistoryCommit {
    commit: StoreBatchCommit,
    predecessor_membership: MembershipChain,
    predecessor_state: ResolvedStoreDeviceState,
    state_after: ResolvedStoreDeviceState,
    operations: VerifiedStoreDeviceOperations,
    acknowledgement: Option<(
        super::store_commit::StoreAckRef,
        super::store_commit::StoreAck,
    )>,
    membership_control: Option<VerifiedMergeMembershipControl>,
    history: OpenedRetainedMergeHistorySummary,
}

impl VerifiedAcceptedPredecessor<'_> {
    fn serial_history_commit(
        &self,
        target: &StoreBatchCommitRef,
    ) -> Result<Option<&AuthorizedSerialCommit>, StorePullError> {
        let Self::SerialHistory { commits } = self else {
            return Ok(None);
        };
        commits
            .iter()
            .find(|accepted| &accepted.commit_ref == target)
            .map(Some)
            .ok_or_else(|| {
                StorePullError::Serial(
                    "provider-access activation is outside the accepted Serial predecessor history"
                        .to_string(),
                )
            })
    }

    fn merge_history_commit(
        &self,
        target: &StoreBatchCommitRef,
    ) -> Result<Option<&VerifiedMergeHistoryCommit>, StorePullError> {
        let Self::MergeHistory { commits, frontier } = self else {
            return Ok(None);
        };
        let mut pending = frontier.clone();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let commit = commits.get(&reference).ok_or_else(|| {
                StorePullError::Database(
                    "accepted Merge predecessor graph is missing an exact commit".to_string(),
                )
            })?;
            if &reference == target {
                return Ok(Some(commit));
            }
            pending.extend(commit_predecessor_references(&commit.commit));
        }
        Err(StorePullError::Database(
            "provider-access activation is outside the accepted Merge predecessor graph"
                .to_string(),
        ))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeMembershipHeadActivation {
    commit: StoreBatchCommitRef,
    transition: super::membership::MergeMembershipHeadTransition,
}

impl VerifiedMergeMembershipHeadActivation {
    pub(crate) fn verifies(
        &self,
        reference: &super::membership::MembershipHeadRef,
        head: &super::membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> bool {
        &self.commit == commit && self.transition.matches_head(head, reference)
    }
}

struct VerifiedMergeMembershipControl {
    activations: VerifiedCircleActivations,
    head_activation: VerifiedMergeMembershipHeadActivation,
    conflict_resolution: Option<VerifiedMergeConflictResolutionActivation>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeConflictResolutionActivation {
    reference: super::membership::StoreMembershipConflictResolutionRef,
}

impl VerifiedMergeConflictResolutionActivation {
    pub(crate) fn reference(&self) -> &super::membership::StoreMembershipConflictResolutionRef {
        &self.reference
    }

    pub(crate) fn verifies(
        &self,
        reference: &super::membership::StoreMembershipConflictResolutionRef,
    ) -> bool {
        &self.reference == reference
    }
}

#[derive(Clone, Default)]
pub(crate) struct VerifiedMergeMembershipPrefix {
    commits: BTreeSet<StoreBatchCommitRef>,
    predecessor_memberships: Vec<MembershipChain>,
    head_activations: BTreeMap<StoreBatchCommitRef, VerifiedMergeMembershipHeadActivation>,
    conflict_resolutions: BTreeMap<
        super::membership::StoreMembershipConflictResolutionRef,
        VerifiedMergeConflictResolutionActivation,
    >,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VerifiedMergePrefixHeadStatus {
    Included,
    OutsidePrefix,
}

impl VerifiedMergeMembershipPrefix {
    pub(crate) fn head_activation(
        &self,
        commit: &StoreBatchCommitRef,
    ) -> Option<&VerifiedMergeMembershipHeadActivation> {
        self.head_activations.get(commit)
    }

    pub(crate) fn verifies_conflict_resolution(
        &self,
        reference: &super::membership::StoreMembershipConflictResolutionRef,
    ) -> bool {
        self.conflict_resolutions
            .get(reference)
            .is_some_and(|proof| proof.verifies(reference))
    }

    pub(crate) fn classify_head(
        &self,
        reference: &super::membership::MembershipHeadRef,
        head: &super::membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> Result<VerifiedMergePrefixHeadStatus, String> {
        if !self.commits.contains(commit) {
            return Ok(VerifiedMergePrefixHeadStatus::OutsidePrefix);
        }
        let proof = self.head_activations.get(commit).ok_or_else(|| {
            "in-prefix membership activation is absent from its verified Store control".to_string()
        })?;
        if !proof.verifies(reference, head, commit) {
            return Err(
                "membership head differs from its in-prefix verified Store control".to_string(),
            );
        }
        Ok(VerifiedMergePrefixHeadStatus::Included)
    }

    pub(crate) fn validate_complete_membership(
        &self,
        membership: &MembershipChain,
    ) -> Result<(), String> {
        if self
            .predecessor_memberships
            .iter()
            .any(|predecessor| !membership.causally_includes(predecessor))
        {
            return Err(
                "membership state regresses below an exact Store predecessor membership"
                    .to_string(),
            );
        }
        if self
            .head_activations
            .values()
            .any(|proof| !membership.contains_coord(&proof.transition.body.entry.coord))
        {
            return Err("membership state omits an accepted Store membership control".to_string());
        }
        if self.conflict_resolutions.keys().any(|reference| {
            membership
                .resolution_refs()
                .binary_search(reference)
                .is_err()
        }) {
            return Err("membership state omits an accepted Store conflict resolution".to_string());
        }
        Ok(())
    }
}

fn verified_merge_membership_prefix(
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
    let closure = verified_merge_commit_closure(commits, tips)?;
    let mut prefix = VerifiedMergeMembershipPrefix {
        commits: closure.clone(),
        ..VerifiedMergeMembershipPrefix::default()
    };
    for reference in closure {
        let verified = &commits[&reference];
        prefix
            .predecessor_memberships
            .push(verified.predecessor_membership.clone());
        if let Some(control) = &verified.membership_control {
            prefix
                .head_activations
                .insert(reference, control.head_activation.clone());
            if let Some(resolution) = &control.conflict_resolution {
                prefix
                    .conflict_resolutions
                    .insert(resolution.reference.clone(), resolution.clone());
            }
        }
    }
    Ok(prefix)
}

fn verified_merge_commit_closure(
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<BTreeSet<StoreBatchCommitRef>, StorePullError> {
    let mut pending = tips.into_iter().collect::<Vec<_>>();
    let mut closure = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !closure.insert(reference.clone()) {
            continue;
        }
        let verified = commits.get(&reference).ok_or_else(|| {
            StorePullError::Database(
                "verified Merge predecessor closure is absent from its history".to_string(),
            )
        })?;
        pending.extend(commit_predecessor_references(&verified.commit));
    }
    Ok(closure)
}

fn merge_device_state_from_verified_history(
    reference: &StoreDeviceStateRef,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let StoreDeviceStateRef::MergeConcurrent { frontier, .. } = reference else {
        return Err(StorePullError::Database(
            "Merge authority carries Serial device state".to_string(),
        ));
    };
    let CommitFrontier::MergeConcurrent(frontier) = frontier else {
        return Err(StorePullError::Database(
            "Merge device state carries Serial frontier".to_string(),
        ));
    };
    let allowed = verified_merge_commit_closure(commits, allowed_tips)?;
    if frontier
        .values()
        .any(|reference| !allowed.contains(reference))
    {
        return Err(StorePullError::Database(
            "Merge device state names a commit outside its causal predecessor history".to_string(),
        ));
    }
    let state = if frontier.is_empty() {
        genesis.clone()
    } else {
        ResolvedStoreDeviceState::merge(
            frontier
                .values()
                .map(|reference| {
                    commits
                        .get(reference)
                        .map(|verified| verified.state_after.clone())
                        .ok_or_else(|| {
                            StorePullError::Database(
                                "Merge device-state frontier is absent from its verified history"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let expected = StoreDeviceStateRef::merge_concurrent(
        CommitFrontier::MergeConcurrent(frontier.clone()),
        &state,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if &expected != reference {
        return Err(StorePullError::Database(
            "Merge device-state reference differs from its verified history".to_string(),
        ));
    }
    Ok(state)
}

async fn verify_merge_owner_conflict_acceptance_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerConflictResolutionAcceptance,
    resolver_pubkey: &str,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<(), StorePullError> {
    let registration = load_registration_ref(storage, root, &acceptance.owner_registration).await?;
    acceptance
        .verify(&registration.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let state = merge_device_state_from_verified_history(
        &acceptance.device_state,
        genesis,
        commits,
        allowed_tips,
    )?;
    if !device_state_has_active_registration(&state, &acceptance.owner_registration) {
        return Err(StorePullError::Database(
            "conflict-resolution Owner registration is not active at its exact device state"
                .to_string(),
        ));
    }
    verify_canonical_owner_registration(
        storage,
        root,
        &state,
        resolver_pubkey,
        &acceptance.owner_registration,
    )
    .await?;
    Ok(())
}

async fn verify_merge_resolution_activation_acceptance_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
) -> Result<Option<VerifiedMergeConflictResolutionActivation>, StorePullError> {
    let Some(super::store_commit::StoreControl::MergeMembership { transition }) = commit.control()
    else {
        return Ok(None);
    };
    let entry = super::store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &transition.body.entry,
    )
    .await?;
    let super::membership::MembershipChange::ResolutionActivation { resolution } =
        &entry.value.change
    else {
        return Ok(None);
    };
    if entry.value.coord() != transition.body.entry.coord {
        return Err(StorePullError::Database(
            "Merge resolution activation differs from its exact transition".to_string(),
        ));
    }
    let value = super::store_objects::load_membership_resolution_ref(
        storage,
        root.store_root_hash,
        resolution,
    )
    .await?;
    let registration = load_registration_ref(storage, root, &commit.author_registration).await?;
    let acceptance = &value.value.replacement_acceptance;
    let mut expected_activations = vec![
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.owner_registration.clone(),
            value.value.replacement_grant.clone(),
            acceptance.membership.clone(),
        ),
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.owner_registration.clone(),
            value.value.replacement_grant.clone(),
            acceptance.recovery.clone(),
        ),
    ];
    expected_activations.sort();
    if acceptance.owner_registration != commit.author_registration
        || registration.value.author_pubkey != value.value.resolver_pubkey
        || entry.value.author_pubkey != value.value.resolver_pubkey
        || transition.body.author_registration != commit.author_registration
        || commit.stream_activations() != expected_activations
    {
        return Err(StorePullError::Database(
            "Merge resolution activation differs from its accepted Owner authority".to_string(),
        ));
    }
    verify_merge_owner_conflict_acceptance_with_history(
        storage,
        root,
        acceptance,
        &value.value.resolver_pubkey,
        genesis,
        commits,
        commit_predecessor_references(commit),
    )
    .await?;
    Ok(Some(VerifiedMergeConflictResolutionActivation {
        reference: resolution.clone(),
    }))
}

struct VerifiedMergeHistory {
    genesis: ResolvedStoreDeviceState,
    commits: BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
}

pub(crate) struct MergeOutboundAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) membership_state: StoreMembershipStateRef,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}

pub(crate) struct PreparedMergeHistorySuccessor {
    pub(crate) summary: RetainedVerifiedMergeHistorySummary,
    pub(crate) head_slot: crate::storage::cloud::ObjectSlot,
    pub(crate) predecessor_head: Option<super::store_commit::StoreDeviceHeadRef>,
}

pub(crate) struct MergeHistorySuccessorEvidence {
    pub(crate) registrations: Vec<RetainedVerifiedRegistration>,
    pub(crate) acknowledgement: Option<super::store_commit::RetainedVerifiedActivatedAck>,
    pub(crate) membership_proof: Option<super::store_commit::RetainedMergeMembershipProof>,
}

impl MergeHistorySuccessorEvidence {
    pub(crate) fn none() -> Self {
        Self {
            registrations: Vec::new(),
            acknowledgement: None,
            membership_proof: None,
        }
    }
}

fn insert_exact<K, V>(
    target: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    conflict: &str,
) -> Result<(), StorePullError>
where
    K: Ord,
    V: PartialEq,
{
    match target.entry(key) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => Ok(()),
        std::collections::btree_map::Entry::Occupied(_) => {
            Err(StorePullError::Database(conflict.to_string()))
        }
    }
}

fn insert_latest_acknowledgement(
    target: &mut BTreeMap<
        super::store_commit::StoreDeviceId,
        super::store_commit::RetainedVerifiedActivatedAck,
    >,
    device_id: super::store_commit::StoreDeviceId,
    value: super::store_commit::RetainedVerifiedActivatedAck,
) -> Result<(), StorePullError> {
    match target.entry(device_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => Ok(()),
        std::collections::btree_map::Entry::Occupied(mut entry)
            if value.exactly_extends(entry.get()) =>
        {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get().exactly_extends(&value) =>
        {
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(_) => Err(StorePullError::Database(
            "Merge predecessor checkpoints contain forked acknowledgement proof chains".to_string(),
        )),
    }
}

fn insert_latest_announcement(
    target: &mut BTreeMap<
        super::membership::AuthorStreamId,
        super::store_commit::RetainedAcceptedStoreAnnouncement,
    >,
    stream_id: super::membership::AuthorStreamId,
    value: super::store_commit::RetainedAcceptedStoreAnnouncement,
) -> Result<(), StorePullError> {
    match target.entry(stream_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => Ok(()),
        std::collections::btree_map::Entry::Occupied(mut entry)
            if entry.get().value.commit.coord.sequence() < value.value.commit.coord.sequence() =>
        {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get().value.commit.coord.sequence() > value.value.commit.coord.sequence() =>
        {
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(_) => Err(StorePullError::Database(
            "Merge predecessor checkpoints contain conflicting announcement heads at one sequence"
                .to_string(),
        )),
    }
}

fn insert_membership_proof(
    target: &mut BTreeMap<StoreBatchCommitRef, super::store_commit::RetainedMergeMembershipProof>,
    reference: StoreBatchCommitRef,
    value: super::store_commit::RetainedMergeMembershipProof,
) -> Result<(), StorePullError> {
    if target
        .keys()
        .any(|existing| existing.coord == reference.coord && existing != &reference)
    {
        return Err(StorePullError::Database(
            "Merge predecessor checkpoints contain conflicting membership proofs at one Store coordinate"
                .to_string(),
        ));
    }
    insert_exact(
        target,
        reference,
        value,
        "Merge predecessor checkpoints disagree on a membership proof",
    )
}

pub(crate) async fn prepare_merge_history_successor(
    db: &Database,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
    membership: &MembershipChain,
    author: &StoreDeviceRegistration,
    recovery_author: Option<&StoreDeviceRegistrationRef>,
    state_after: ResolvedStoreDeviceState,
    evidence: MergeHistorySuccessorEvidence,
) -> Result<PreparedMergeHistorySuccessor, StorePullError> {
    commit_ref
        .verify_commit(commit)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    state_after.validate_canonical().map_err(|error| {
        StorePullError::Database(format!("validate Merge successor post-state: {error}"))
    })?;
    let predecessor_refs = commit_predecessor_references(commit);
    let predecessors = db
        .retained_merge_history_frontier(predecessor_refs.clone())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if predecessors.len() != predecessor_refs.len() {
        return Err(StorePullError::Database(
            "Merge successor is missing a retained direct predecessor".to_string(),
        ));
    }
    let (expected_predecessor_ref, predecessor_state) = db
        .store_device_state_for_order(&commit.order)
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if commit.device_state != expected_predecessor_ref {
        return Err(StorePullError::Database(
            "Merge successor names another predecessor device state".to_string(),
        ));
    }
    if let Some(recovery_author) = recovery_author {
        let retained_recovery_registration = evidence.registrations.iter().any(|registration| {
            registration.reference == *recovery_author
                && matches!(
                    &registration.value.origin,
                    super::store_commit::StoreDeviceRegistrationOrigin::Recovery { .. }
                )
        });
        let recovery_activation = commit.device_registrations().iter().any(|activation| {
            activation.registration == *recovery_author
                && matches!(
                    &activation.authority,
                    super::store_commit::StoreDeviceRegistrationActivationRef::Recovery { .. }
                )
        });
        if recovery_author != &commit.author_registration
            || !retained_recovery_registration
            || !recovery_activation
        {
            return Err(StorePullError::Database(
                "Merge successor recovery author lacks its exact retained activation".to_string(),
            ));
        }
    }
    if !device_state_has_active_registration(&predecessor_state, &commit.author_registration)
        && recovery_author != Some(&commit.author_registration)
    {
        return Err(StorePullError::Database(
            "Merge successor author is inactive at its exact predecessor cut".to_string(),
        ));
    }
    verify_merge_membership_state_ref(&commit.membership_state, membership, &predecessor_state)?;

    compose_merge_history_successor(
        root,
        commit,
        commit_ref,
        membership,
        author,
        state_after,
        predecessors,
        evidence,
    )
}

struct MergedRetainedMergeHistory {
    causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    registrations: BTreeMap<super::store_commit::StoreDeviceId, RetainedVerifiedRegistration>,
    acknowledgements: BTreeMap<
        super::store_commit::StoreDeviceId,
        super::store_commit::RetainedVerifiedActivatedAck,
    >,
    membership_proofs:
        BTreeMap<StoreBatchCommitRef, super::store_commit::RetainedMergeMembershipProof>,
    announcement_frontier: BTreeMap<
        super::membership::AuthorStreamId,
        super::store_commit::RetainedAcceptedStoreAnnouncement,
    >,
}

fn merge_retained_merge_history(
    root: &StoreRootRef,
    membership: &MembershipChain,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<MergedRetainedMergeHistory, StorePullError> {
    let mut causal_cut = BTreeMap::new();
    let mut registrations = BTreeMap::new();
    let mut acknowledgements = BTreeMap::new();
    let mut membership_proofs = BTreeMap::new();
    let mut announcement_frontier = BTreeMap::new();
    for predecessor in predecessors {
        let predecessor_cut = predecessor.summary.causal_cut.clone();
        if predecessor.summary.store_root_hash != root.store_root_hash
            || predecessor.summary.policy != crate::WritePolicy::MergeConcurrent
        {
            return Err(StorePullError::Database(
                "Merge predecessor checkpoint belongs to another Store or policy".to_string(),
            ));
        }
        if predecessor
            .summary
            .membership_floor
            .effective_coordinates
            .iter()
            .any(|coordinate| !membership.effectively_contains_coord(coordinate))
            || predecessor
                .summary
                .membership_floor
                .resolutions
                .iter()
                .any(|reference| {
                    membership
                        .resolution_refs()
                        .binary_search(reference)
                        .is_err()
                })
        {
            return Err(StorePullError::Database(
                "Merge successor membership omits its retained causal floor".to_string(),
            ));
        }
        for (key, value) in predecessor.summary.causal_cut {
            insert_exact(
                &mut causal_cut,
                key,
                value,
                "Merge predecessor checkpoints disagree on a Store coordinate",
            )?;
        }
        for (key, value) in predecessor.summary.registrations {
            insert_exact(
                &mut registrations,
                key,
                value,
                "Merge predecessor checkpoints disagree on a device registration",
            )?;
        }
        for (key, value) in predecessor.summary.acknowledgements {
            insert_latest_acknowledgement(&mut acknowledgements, key, value)?;
        }
        for (key, mut value) in predecessor.summary.membership_proofs {
            if predecessor_cut.get(&value.commit.coord) == Some(&value.commit)
                && value.announcement.is_none()
            {
                let StoreCommitCoord::MergeConcurrent { stream_id, .. } = value.commit.coord else {
                    return Err(StorePullError::Database(
                        "Merge membership proof contains a Serial commit".to_string(),
                    ));
                };
                value.announcement = predecessor
                    .announcement_frontier
                    .get(&stream_id)
                    .filter(|announcement| announcement.value.commit == value.commit)
                    .cloned();
            }
            insert_membership_proof(&mut membership_proofs, key, value)?;
        }
        for (key, value) in predecessor.announcement_frontier {
            insert_latest_announcement(&mut announcement_frontier, key, value)?;
        }
    }
    Ok(MergedRetainedMergeHistory {
        causal_cut,
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    })
}

fn compose_merge_history_successor(
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    commit_ref: &StoreBatchCommitRef,
    membership: &MembershipChain,
    author: &StoreDeviceRegistration,
    state_after: ResolvedStoreDeviceState,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
    evidence: MergeHistorySuccessorEvidence,
) -> Result<PreparedMergeHistorySuccessor, StorePullError> {
    let MergedRetainedMergeHistory {
        mut causal_cut,
        mut registrations,
        mut acknowledgements,
        mut membership_proofs,
        announcement_frontier,
    } = merge_retained_merge_history(root, membership, predecessors)?;
    let mut membership_floor =
        super::store_commit::MembershipCausalFloor::from_membership(membership);
    insert_exact(
        &mut causal_cut,
        commit_ref.coord.clone(),
        commit_ref.clone(),
        "Merge successor conflicts at its Store coordinate",
    )?;
    for registration in evidence.registrations {
        if !commit
            .device_registrations()
            .iter()
            .any(|activation| activation.registration == registration.reference)
        {
            return Err(StorePullError::Database(
                "Merge history registration is absent from its activating commit".to_string(),
            ));
        }
        insert_exact(
            &mut registrations,
            registration.reference.device_id,
            registration,
            "Merge successor registration conflicts with retained authority",
        )?;
    }
    if let Some(retained) = evidence.acknowledgement {
        let (reference, _) = retained.latest().ok_or_else(|| {
            StorePullError::Database(
                "Merge history acknowledgement proof chain is empty".to_string(),
            )
        })?;
        if commit.acknowledgement() != Some(reference)
            || retained.activating_commit != *commit_ref
            || retained.activating_commit_value != *commit
        {
            return Err(StorePullError::Database(
                "Merge history acknowledgement differs from its activating commit".to_string(),
            ));
        }
        insert_latest_acknowledgement(
            &mut acknowledgements,
            reference.registration.device_id,
            retained,
        )?;
    }
    if let Some(proof) = evidence.membership_proof {
        if proof.commit != *commit_ref {
            return Err(StorePullError::Database(
                "Merge membership proof names another activating commit".to_string(),
            ));
        }
        membership_floor
            .advance(
                proof.entry.coord.clone(),
                &proof.head_value.body.resolutions,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        insert_membership_proof(&mut membership_proofs, commit_ref.clone(), proof)?;
    }
    let author_ref = commit.author_registration.clone();
    author_ref
        .verify_registration(author)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    insert_exact(
        &mut registrations,
        author_ref.device_id,
        RetainedVerifiedRegistration {
            reference: author_ref.clone(),
            value: author.clone(),
        },
        "Merge successor author registration conflicts with retained authority",
    )?;
    let mut post_frontier = BTreeMap::new();
    for reference in causal_cut.values() {
        let StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence,
        } = reference.coord
        else {
            return Err(StorePullError::Database(
                "Merge causal cut contains a Serial coordinate".to_string(),
            ));
        };
        match post_frontier.entry(stream_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(reference.clone());
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if sequence > entry.get().coord.sequence() =>
            {
                entry.insert(reference.clone());
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    let summary = RetainedVerifiedMergeHistorySummary {
        version: super::store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        policy: crate::WritePolicy::MergeConcurrent,
        causal_cut,
        post_state: StoreDeviceStateRef::merge_concurrent(
            CommitFrontier::MergeConcurrent(post_frontier),
            &state_after,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?,
        membership_floor,
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    };
    summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = commit_ref.coord
    else {
        return Err(StorePullError::Database(
            "Merge successor carries a Serial coordinate".to_string(),
        ));
    };
    let predecessor_head = summary
        .announcement_frontier
        .get(&stream_id)
        .map(|accepted| accepted.reference.clone());
    let head_slot = match summary.announcement_frontier.get(&stream_id) {
        Some(accepted) => accepted.value.successor.next_slot.clone(),
        None => match &author.store_commits {
            StoreCommitAnchor::MergeConcurrent {
                announcements: DeviceStreamAnchor::StoreAnnouncements { first_slot },
            } if sequence == 1 => first_slot.clone(),
            StoreCommitAnchor::MergeConcurrent { .. } | StoreCommitAnchor::Serial => {
                return Err(StorePullError::Database(
                    "Merge successor has no exact retained announcement predecessor".to_string(),
                ));
            }
        },
    };
    Ok(PreparedMergeHistorySuccessor {
        summary,
        head_slot,
        predecessor_head,
    })
}

pub(crate) async fn prepare_merge_snapshot_history_summary(
    db: &Database,
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &super::store_commit::StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let CommitFrontier::MergeConcurrent(frontier) = coverage else {
        return Err(StorePullError::Database(
            "Merge snapshot history received Serial coverage".to_string(),
        ));
    };
    let predecessors = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if predecessors.len() != frontier.len() {
        return Err(StorePullError::Database(
            "Merge snapshot is missing a retained checkpoint at its coverage frontier".to_string(),
        ));
    }
    compose_merge_snapshot_history_summary(
        root,
        coverage,
        membership,
        state,
        author_ref,
        author,
        predecessors,
    )
}

fn compose_merge_snapshot_history_summary(
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let CommitFrontier::MergeConcurrent(frontier) = coverage else {
        return Err(StorePullError::Database(
            "Merge snapshot history received Serial coverage".to_string(),
        ));
    };
    let MergedRetainedMergeHistory {
        causal_cut,
        mut registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    } = merge_retained_merge_history(root, membership, predecessors)?;
    author_ref
        .verify_registration(author)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    insert_exact(
        &mut registrations,
        author_ref.device_id,
        RetainedVerifiedRegistration {
            reference: author_ref.clone(),
            value: author.clone(),
        },
        "Merge snapshot author registration conflicts with retained authority",
    )?;
    let summary = RetainedVerifiedMergeHistorySummary {
        version: super::store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        policy: crate::WritePolicy::MergeConcurrent,
        causal_cut,
        post_state: StoreDeviceStateRef::merge_concurrent(coverage.clone(), state)
            .map_err(|error| StorePullError::Database(error.to_string()))?,
        membership_floor: super::store_commit::MembershipCausalFloor::from_membership(membership),
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    };
    summary
        .validate_snapshot_baseline()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if summary
        .frontier()
        .map_err(|error| StorePullError::Database(error.to_string()))?
        != *frontier
    {
        return Err(StorePullError::Database(
            "Merge snapshot history does not exactly cover its signed frontier".to_string(),
        ));
    }
    Ok(summary)
}

pub(crate) fn prepare_merge_abandonment_history_summary(
    candidate_summary: &RetainedVerifiedMergeHistorySummary,
    candidate: &StoreBatchCommitRef,
    candidate_value: &StoreBatchCommit,
    abandonment: &StoreBatchCommitRef,
    abandonment_value: &StoreBatchCommit,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    candidate_summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate
        .verify_commit(candidate_value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    abandonment
        .verify_commit(abandonment_value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if candidate.coord != abandonment.coord
        || candidate_value.order != abandonment_value.order
        || candidate_value.membership_state != abandonment_value.membership_state
        || candidate_value.device_state != abandonment_value.device_state
        || candidate_summary.causal_cut.get(&candidate.coord) != Some(candidate)
        || candidate_summary.membership_proofs.contains_key(candidate)
    {
        return Err(StorePullError::Database(
            "Merge abandonment differs from its retained candidate history".to_string(),
        ));
    }
    let mut summary = candidate_summary.clone();
    summary
        .causal_cut
        .insert(abandonment.coord.clone(), abandonment.clone());
    let frontier = CommitFrontier::MergeConcurrent(
        summary
            .frontier()
            .map_err(|error| StorePullError::Database(error.to_string()))?,
    );
    let StoreDeviceStateRef::MergeConcurrent {
        frontier: post_state_frontier,
        ..
    } = &mut summary.post_state
    else {
        return Err(StorePullError::Database(
            "Merge abandonment retained a Serial post-state".to_string(),
        ));
    };
    *post_state_frontier = frontier;
    summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(summary)
}

pub(crate) struct MergeConflictResolutionAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}

fn retained_merge_membership_prefix(
    checkpoints: &[OpenedRetainedMergeHistorySummary],
) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
    let mut prefix = VerifiedMergeMembershipPrefix::default();
    for checkpoint in checkpoints {
        for reference in checkpoint.summary.causal_cut.values() {
            prefix.commits.insert(reference.clone());
        }
        for proof in checkpoint.summary.membership_proofs.values() {
            let Some(super::store_commit::StoreControl::MergeMembership { transition }) =
                proof.commit_value.control()
            else {
                return Err(StorePullError::Database(
                    "retained Merge membership proof has no membership control".to_string(),
                ));
            };
            let activation = VerifiedMergeMembershipHeadActivation {
                commit: proof.commit.clone(),
                transition: transition.clone(),
            };
            match prefix.head_activations.entry(proof.commit.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(activation);
                }
                std::collections::btree_map::Entry::Occupied(entry)
                    if entry.get() == &activation => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(StorePullError::Database(
                        "retained checkpoints disagree on a membership activation".to_string(),
                    ));
                }
            }
            if let Some(reference) = &proof.resolution {
                let activation = VerifiedMergeConflictResolutionActivation {
                    reference: reference.clone(),
                };
                match prefix.conflict_resolutions.entry(reference.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(activation);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() == &activation => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(StorePullError::Database(
                            "retained checkpoints disagree on a conflict resolution".to_string(),
                        ));
                    }
                }
            }
        }
    }
    Ok(prefix)
}

fn validate_retained_membership_floors(
    checkpoints: &[OpenedRetainedMergeHistorySummary],
    membership: &MembershipChain,
) -> Result<(), StorePullError> {
    if checkpoints.iter().any(|checkpoint| {
        !retained_membership_floor_is_included(&checkpoint.summary.membership_floor, membership)
    }) {
        return Err(StorePullError::Database(
            "Merge membership omits retained effective predecessor authority".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn retained_membership_floor_is_included(
    floor: &super::store_commit::MembershipCausalFloor,
    membership: &MembershipChain,
) -> bool {
    floor
        .effective_coordinates
        .iter()
        .all(|coordinate| membership.effectively_contains_coord(coordinate))
        && floor.resolutions.iter().all(|reference| {
            membership
                .resolution_refs()
                .binary_search(reference)
                .is_ok()
        })
}

async fn retained_merge_device_state(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    frontier: &BTreeMap<super::membership::AuthorStreamId, StoreBatchCommitRef>,
    checkpoints: &[OpenedRetainedMergeHistorySummary],
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), StorePullError> {
    let state = if checkpoints.is_empty() {
        let founder = load_founder_registration_with_root(storage, root, root_value).await?;
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        ResolvedStoreDeviceState::founder(
            root,
            founder_ref,
            &root_value.descriptor.founder_pubkey,
            root_value.descriptor.founder_grant.clone(),
            &root_value.descriptor.founder_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    } else {
        ResolvedStoreDeviceState::merge(
            checkpoints
                .iter()
                .map(|checkpoint| checkpoint.post_state.clone()),
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let reference = StoreDeviceStateRef::merge_concurrent(
        CommitFrontier::MergeConcurrent(frontier.clone()),
        &state,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok((reference, state))
}

pub(crate) async fn retained_merge_device_state_for_order(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), StorePullError> {
    let StoreHistoryCut::MergeConcurrent(frontier) = order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
    else {
        return Err(StorePullError::Database(
            "Merge device-state authority received a Serial predecessor".to_string(),
        ));
    };
    let checkpoints = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if checkpoints.len() != frontier.len()
        || checkpoints.iter().any(|checkpoint| {
            checkpoint.summary.store_root_hash != root.store_root_hash
                || checkpoint.summary.policy != crate::WritePolicy::MergeConcurrent
        })
    {
        return Err(StorePullError::Database(
            "Merge device-state authority is missing a retained predecessor checkpoint".to_string(),
        ));
    }
    let root_value = load_store_protocol_root(storage, root).await?.value;
    retained_merge_device_state(storage, root, &root_value, &frontier, &checkpoints).await
}

pub(crate) async fn load_merge_conflict_resolution_authorization(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    candidate_membership_heads: &[super::membership::MembershipHeadRef],
    author_registration: &StoreDeviceRegistrationRef,
    resolver_pubkey: &str,
) -> Result<MergeConflictResolutionAuthorization, StorePullError> {
    let StoreHistoryCut::MergeConcurrent(frontier) = order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
    else {
        return Err(StorePullError::Database(
            "Merge conflict resolution received a Serial predecessor".to_string(),
        ));
    };
    let checkpoints = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if checkpoints.len() != frontier.len()
        || checkpoints.iter().any(|checkpoint| {
            checkpoint.summary.store_root_hash != root.store_root_hash
                || checkpoint.summary.policy != crate::WritePolicy::MergeConcurrent
        })
    {
        return Err(StorePullError::Database(
            "Merge conflict resolution is missing its retained predecessor authority".to_string(),
        ));
    }
    let root_value = load_store_protocol_root(storage, root).await?.value;
    let prefix = retained_merge_membership_prefix(&checkpoints)?;
    let membership = super::membership_ops::project_anchored_chain_to_verified_store_prefix(
        storage,
        root,
        &root_value.descriptor.founder_pubkey,
        candidate_membership_heads,
        &prefix,
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    validate_retained_membership_floors(&checkpoints, &membership)?;
    prefix
        .validate_complete_membership(&membership)
        .map_err(StorePullError::Database)?;
    let (device_state_ref, device_state) =
        retained_merge_device_state(storage, root, &root_value, &frontier, &checkpoints).await?;
    if !device_state_has_active_registration(&device_state, author_registration) {
        return Err(StorePullError::Database(
            "Merge conflict-resolution author is inactive at its predecessor cut".to_string(),
        ));
    }
    verify_canonical_owner_registration(
        storage,
        root,
        &device_state,
        resolver_pubkey,
        author_registration,
    )
    .await?;
    Ok(MergeConflictResolutionAuthorization {
        membership,
        device_state_ref,
        device_state,
    })
}

pub(crate) async fn load_retained_merge_outbound_authorization(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    candidate_membership_heads: &[super::membership::MembershipHeadRef],
    author_registration: &StoreDeviceRegistrationRef,
) -> Result<MergeOutboundAuthorization, StorePullError> {
    let StoreHistoryCut::MergeConcurrent(frontier) = order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
    else {
        return Err(StorePullError::Database(
            "Merge outbound authorization received a Serial predecessor".to_string(),
        ));
    };
    let checkpoints = db
        .retained_merge_history_frontier(frontier.values().cloned().collect())
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if checkpoints.len() != frontier.len()
        || checkpoints.iter().any(|checkpoint| {
            checkpoint.summary.store_root_hash != root.store_root_hash
                || checkpoint.summary.policy != crate::WritePolicy::MergeConcurrent
        })
    {
        return Err(StorePullError::Database(
            "Merge outbound authorization is missing retained predecessor authority".to_string(),
        ));
    }
    let prefix = retained_merge_membership_prefix(&checkpoints)?;
    let root_value = load_store_protocol_root(storage, root).await?.value;
    let membership = super::membership_ops::project_anchored_chain_to_verified_store_prefix(
        storage,
        root,
        &root_value.descriptor.founder_pubkey,
        candidate_membership_heads,
        &prefix,
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    validate_retained_membership_floors(&checkpoints, &membership)?;
    prefix
        .validate_complete_membership(&membership)
        .map_err(StorePullError::Database)?;
    let (device_state_ref, device_state) =
        retained_merge_device_state(storage, root, &root_value, &frontier, &checkpoints).await?;
    if !device_state_has_active_registration(&device_state, author_registration) {
        return Err(StorePullError::Database(
            "Merge outbound author is inactive at its exact predecessor cut".to_string(),
        ));
    }
    let MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StorePullError::Database(
            "Merge outbound predecessor membership is conflicted".to_string(),
        ));
    };
    let membership_state = StoreMembershipStateRef::merge_concurrent(
        membership.head_refs().to_vec(),
        membership.resolution_refs().to_vec(),
        device_state.recovery.clone(),
        resolved.state_hash,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(MergeOutboundAuthorization {
        membership,
        membership_state,
        device_state_ref,
        device_state,
    })
}

fn verify_merge_history_refs<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> StorePullFuture<'a, VerifiedMergeHistory> {
    let pending = tips.into_iter().collect::<Vec<_>>();
    Box::pin(verify_merge_history_refs_impl(storage, root, pending))
}

async fn verify_merge_history_refs_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    mut pending: Vec<StoreBatchCommitRef>,
) -> Result<VerifiedMergeHistory, StorePullError> {
    let verified_root = Box::pin(load_store_protocol_root(storage, root))
        .await?
        .value;
    if verified_root.descriptor.write_policy != crate::WritePolicy::MergeConcurrent {
        return Err(StorePullError::Database(
            "Merge history belongs to a non-Merge Store".to_string(),
        ));
    }
    let founder = Box::pin(load_founder_registration_with_root(
        storage,
        root,
        &verified_root,
    ))
    .await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_ref.clone(),
        &verified_root.descriptor.founder_pubkey,
        verified_root.descriptor.founder_grant.clone(),
        &verified_root.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;

    let mut loaded =
        BTreeMap::<StoreBatchCommitRef, (StoreBatchCommit, StoreDeviceRegistration)>::new();
    while let Some(reference) = pending.pop() {
        if loaded.contains_key(&reference) {
            continue;
        }
        if !matches!(reference.coord, StoreCommitCoord::MergeConcurrent { .. }) {
            return Err(StorePullError::Database(
                "Merge history contains a Serial commit".to_string(),
            ));
        }
        let (commit, author) = Box::pin(load_commit_with_author_at_root(
            storage,
            root,
            &verified_root,
            &reference,
        ))
        .await?;
        pending.extend(commit_predecessor_references(&commit));
        loaded.insert(reference, (commit, author));
    }

    let mut states = BTreeMap::<StoreBatchCommitRef, ResolvedStoreDeviceState>::new();
    let mut verified = BTreeMap::new();
    while !loaded.is_empty() {
        let next = loaded.iter().find_map(|(reference, (commit, _))| {
            commit_predecessor_references(commit)
                .iter()
                .all(|dependency| states.contains_key(dependency))
                .then(|| reference.clone())
        });
        let Some(reference) = next else {
            return Err(StorePullError::Database(
                "Merge history is cyclic or has an unresolved predecessor".to_string(),
            ));
        };
        let (commit, author) = loaded.remove(&reference).ok_or_else(|| {
            StorePullError::Database(
                "selected exclusion-history commit disappeared before verification".to_string(),
            )
        })?;
        let (_, accepted_head) = Box::pin(
            super::store_outbound::exact_next_announcement_slot_for_verified_commit(
                storage,
                root,
                &commit.author_registration,
                &author,
                &reference,
                &commit,
            ),
        )
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let activation_head_ref = accepted_head.ok_or_else(|| {
            StorePullError::Database(
                "Merge history commit has no accepted announcement head".to_string(),
            )
        })?;
        let predecessor_state = verified_merge_predecessor_state(&genesis, &states, &commit)?;
        let verified_membership_prefix =
            verified_merge_membership_prefix(&verified, commit_predecessor_references(&commit))?;
        let pending_resolution =
            Box::pin(verify_merge_resolution_activation_acceptance_with_history(
                storage, root, &commit, &genesis, &verified,
            ))
            .await?;
        let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
            storage,
            root,
            &commit.membership_state,
            &verified_membership_prefix,
            pending_resolution.as_ref(),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        verified_membership_prefix
            .validate_complete_membership(&membership)
            .map_err(StorePullError::Database)?;
        verify_merge_membership_state_ref(
            &commit.membership_state,
            &membership,
            &predecessor_state,
        )?;
        if !membership_authorizes(Some(&membership), &commit, &author) {
            return Err(StorePullError::Database(
                "Merge history commit lacks exact membership authority".to_string(),
            ));
        }
        let authority = RegistrationPredecessorAuthority::MergeConcurrent(&membership);
        let accepted_predecessor = VerifiedAcceptedPredecessor::MergeHistory {
            commits: &verified,
            frontier: commit_predecessor_references(&commit),
        };
        let registrations = Box::pin(load_commit_registrations_with_root(
            storage,
            root,
            &verified_root,
            &commit,
            &author,
            Some(&authority),
            Some(&accepted_predecessor),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let (authorized_predecessor, recovery_author) =
            predecessor_with_recovery_author(predecessor_state.clone(), &commit, &registrations)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        if !device_state_has_active_registration(
            &authorized_predecessor,
            &commit.author_registration,
        ) {
            return Err(StorePullError::Database(
                "author exclusion history commit author is inactive at its predecessor".to_string(),
            ));
        }
        let resolver = DeviceStateResolver::Loaded {
            genesis: &genesis,
            states: &states,
        };
        let operations = Box::pin(load_commit_device_operations(
            Some(&resolver),
            storage,
            root,
            &commit,
            &authorized_predecessor,
            Some(&authority),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let acknowledgement = Box::pin(validate_commit_acknowledgement(
            storage, root, &commit, &author,
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let membership_control =
            if let Some(super::store_commit::StoreControl::MergeMembership { transition }) =
                commit.control()
            {
                let (activations, conflict_resolution) =
                    Box::pin(verify_merge_membership_control_with_history(
                        storage,
                        root,
                        &reference,
                        &commit,
                        &membership,
                        &predecessor_state,
                        &verified,
                        pending_resolution.as_ref(),
                    ))
                    .await
                    .map_err(StorePullError::Database)?;
                Some(VerifiedMergeMembershipControl {
                    activations,
                    head_activation: VerifiedMergeMembershipHeadActivation {
                        commit: reference.clone(),
                        transition: transition.clone(),
                    },
                    conflict_resolution,
                })
            } else {
                None
            };
        let owner_recovery = Box::pin(verify_commit_owner_recovery_activation(
            storage, root, &commit, None,
        ))
        .await?;
        let state = operations
            .apply_to(authorized_predecessor, &commit.device_state)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let state = apply_verified_device_lifecycle(
            state,
            &commit,
            &registrations,
            recovery_author.as_ref(),
            owner_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let predecessor_histories = commit_predecessor_references(&commit)
            .iter()
            .map(|predecessor| {
                verified
                    .get(predecessor)
                    .map(|verified: &VerifiedMergeHistoryCommit| verified.history.clone())
                    .ok_or_else(|| {
                        StorePullError::Database(
                            "Merge history summary has an unresolved predecessor".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let membership_closure = Box::pin(verified_merge_membership_objects(
            storage, root, &reference, &commit,
        ))
        .await?;
        let retained_registrations = commit
            .device_registrations()
            .iter()
            .zip(&registrations)
            .map(|(activation, (value, _))| RetainedVerifiedRegistration {
                reference: activation.registration.clone(),
                value: value.clone(),
            })
            .collect();
        let retained_acknowledgement = match acknowledgement.clone() {
            Some((acknowledgement_ref, acknowledgement_value)) => Some(
                retain_activated_acknowledgement(
                    storage,
                    root,
                    &reference,
                    &commit,
                    &author,
                    acknowledgement_ref,
                    acknowledgement_value,
                )
                .await?,
            ),
            None => None,
        };
        let successor = compose_merge_history_successor(
            root,
            &commit,
            &reference,
            &membership,
            &author,
            state.clone(),
            predecessor_histories,
            MergeHistorySuccessorEvidence {
                registrations: retained_registrations,
                acknowledgement: retained_acknowledgement,
                membership_proof: membership_closure.map(|closure| closure.proof),
            },
        )?;
        let activation_head = Box::pin(super::store_objects::load_head_ref(
            storage,
            root.store_root_hash,
            &activation_head_ref,
            &author,
            &reference,
        ))
        .await?;
        let history = successor
            .summary
            .open(
                &commit,
                &reference,
                &activation_head.value,
                &activation_head_ref,
                &state,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        states.insert(reference.clone(), state.clone());
        verified.insert(
            reference,
            VerifiedMergeHistoryCommit {
                commit,
                predecessor_membership: membership,
                predecessor_state,
                state_after: state,
                operations,
                acknowledgement,
                membership_control,
                history,
            },
        );
    }
    Ok(VerifiedMergeHistory {
        genesis,
        commits: verified,
    })
}

async fn replay_merge_device_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    tip: &StoreBatchCommitRef,
) -> Result<
    (
        ResolvedStoreDeviceState,
        VerifiedStoreDeviceOperations,
        StoreBatchCommit,
        Option<VerifiedCircleActivations>,
    ),
    StorePullError,
> {
    let history = verify_merge_history_refs(storage, root, [tip.clone()]).await?;
    let verified = history.commits.get(tip).ok_or_else(|| {
        StorePullError::Database(
            "author exclusion activation is absent from its verified history".to_string(),
        )
    })?;
    Ok((
        verified.predecessor_state.clone(),
        verified.operations.clone(),
        verified.commit.clone(),
        verified
            .membership_control
            .as_ref()
            .map(|control| control.activations.clone()),
    ))
}

pub(crate) struct VerifiedActivatedStoreAck {
    reference: super::store_commit::StoreAckRef,
    value: super::store_commit::StoreAck,
    chain: BTreeMap<
        u64,
        (
            super::store_commit::StoreAckRef,
            super::store_commit::StoreAck,
        ),
    >,
    activating_commit: StoreBatchCommitRef,
    activating_commit_value: StoreBatchCommit,
}

enum VerifiedStoreMembership {
    MergeConcurrent {
        membership: MembershipChain,
        checkpoints: Vec<OpenedRetainedMergeHistorySummary>,
    },
    Serial(SerialAuthorizationState),
}

pub(crate) struct VerifiedStoreHistoryState {
    cut: StoreHistoryCut,
    membership_ref: StoreMembershipStateRef,
    membership: VerifiedStoreMembership,
    device_state_ref: StoreDeviceStateRef,
    device_state: ResolvedStoreDeviceState,
    active_registrations: BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
}

impl VerifiedStoreHistoryState {
    fn is_owner(&self, author: &str) -> bool {
        match &self.membership {
            VerifiedStoreMembership::MergeConcurrent { membership, .. } => {
                membership.is_owner_now(author)
            }
            VerifiedStoreMembership::Serial(authorization) => {
                authorization.membership.is_owner(author)
            }
        }
    }
}

async fn load_active_history_registrations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &ResolvedStoreDeviceState,
) -> Result<
    BTreeMap<
        super::store_commit::StoreDeviceId,
        (StoreDeviceRegistrationRef, StoreDeviceRegistration),
    >,
    StorePullError,
> {
    let mut active = BTreeMap::new();
    for (device_id, record) in &state.devices {
        if !matches!(record.status, StoreDeviceStatus::Active) {
            continue;
        }
        let registration = load_registration_ref(storage, root, &record.registration).await?;
        if registration.value.device_id != *device_id {
            return Err(StorePullError::Database(
                "resolved Store device state names another exact registration".to_string(),
            ));
        }
        active.insert(
            *device_id,
            (record.registration.clone(), registration.value),
        );
    }
    Ok(active)
}

pub(crate) fn verify_store_history_state<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    cut: &'a StoreHistoryCut,
    membership_ref: &'a StoreMembershipStateRef,
) -> StorePullFuture<'a, VerifiedStoreHistoryState> {
    Box::pin(verify_store_history_state_impl(
        storage,
        serial_coordination,
        root,
        cut,
        membership_ref,
    ))
}

async fn verify_store_history_state_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
    membership_ref: &StoreMembershipStateRef,
) -> Result<VerifiedStoreHistoryState, StorePullError> {
    match (cut, membership_ref) {
        (
            StoreHistoryCut::MergeConcurrent(frontier),
            StoreMembershipStateRef::MergeConcurrent(_),
        ) => {
            if serial_coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge history verification received Serial coordination".to_string(),
                ));
            }
            let history = Box::pin(verify_merge_history_refs(
                storage,
                root,
                frontier.values().cloned().collect::<Vec<_>>(),
            ))
            .await?;
            let device_state = if frontier.is_empty() {
                history.genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    frontier
                        .values()
                        .map(|reference| {
                            history
                                .commits
                                .get(reference)
                                .map(|commit| commit.state_after.clone())
                                .ok_or_else(|| {
                                    StorePullError::Database(
                                        "Merge history frontier is absent from its verified graph"
                                            .to_string(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?
            };
            let device_state_ref = StoreDeviceStateRef::merge_concurrent(
                CommitFrontier::MergeConcurrent(frontier.clone()),
                &device_state,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
            let verified_membership_activations =
                verified_merge_membership_prefix(&history.commits, frontier.values().cloned())?;
            let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
                storage,
                root,
                membership_ref,
                &verified_membership_activations,
                None,
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            verified_membership_activations
                .validate_complete_membership(&membership)
                .map_err(StorePullError::Database)?;
            verify_merge_membership_state_ref(membership_ref, &membership, &device_state)?;
            let active_registrations = Box::pin(load_active_history_registrations(
                storage,
                root,
                &device_state,
            ))
            .await?;
            let checkpoints = frontier
                .values()
                .map(|reference| {
                    history
                        .commits
                        .get(reference)
                        .map(|commit| commit.history.clone())
                        .ok_or_else(|| {
                            StorePullError::Database(
                                "Merge snapshot frontier is absent from its verified history"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(VerifiedStoreHistoryState {
                cut: cut.clone(),
                membership_ref: membership_ref.clone(),
                membership: VerifiedStoreMembership::MergeConcurrent {
                    membership,
                    checkpoints,
                },
                device_state_ref,
                device_state,
                active_registrations,
            })
        }
        (StoreHistoryCut::Serial(position), StoreMembershipStateRef::Serial(_)) => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial history verification requires coordination capability".to_string(),
                )
            })?;
            let verified_head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
            let (_, genesis_authorization, genesis_state) =
                Box::pin(load_authorized_serial_prefix(storage, root, None)).await?;
            let founder = load_founder_registration(storage, root).await?;
            let founder_ref = StoreDeviceRegistrationRef::from_registration(
                &founder.value,
                founder.object.clone(),
            );
            let expected_genesis = super::store_commit::StoreSerialPredecessor::Genesis {
                root: root.clone(),
                founder_registration: founder_ref,
            };
            let accepted_prefix = match position {
                super::store_commit::StoreSerialPredecessor::Genesis { .. }
                    if position == &expected_genesis =>
                {
                    &accepted[..0]
                }
                super::store_commit::StoreSerialPredecessor::Genesis { .. } => {
                    return Err(StorePullError::Serial(
                        "Serial history cut names another genesis authority".to_string(),
                    ));
                }
                super::store_commit::StoreSerialPredecessor::Commit(reference) => {
                    let index = accepted
                        .iter()
                        .position(|candidate| &candidate.commit_ref == reference)
                        .ok_or_else(|| {
                            StorePullError::Serial(
                                "Serial history cut is absent from the signed coordinated chain"
                                    .to_string(),
                            )
                        })?;
                    &accepted[..=index]
                }
            };
            let (authorization, device_state) = accepted_prefix.last().map_or_else(
                || (genesis_authorization, genesis_state),
                |accepted| {
                    (
                        accepted.authorization_after.clone(),
                        accepted.device_state_after.clone(),
                    )
                },
            );
            let expected_membership = StoreMembershipStateRef::serial(
                position.clone(),
                device_state.recovery.clone(),
                &authorization,
            )
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
            if &expected_membership != membership_ref {
                return Err(StorePullError::Serial(
                    "Serial history membership reference differs from its accepted state"
                        .to_string(),
                ));
            }
            let device_state_ref = StoreDeviceStateRef::serial(position.clone(), &device_state)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            let active_registrations =
                load_active_history_registrations(storage, root, &device_state).await?;
            Ok(VerifiedStoreHistoryState {
                cut: cut.clone(),
                membership_ref: expected_membership,
                membership: VerifiedStoreMembership::Serial(authorization),
                device_state_ref,
                device_state,
                active_registrations,
            })
        }
        _ => Err(StorePullError::Database(
            "Store history cut and membership state use different policies".to_string(),
        )),
    }
}

#[derive(Debug)]
pub(crate) struct VerifiedStoreSnapshotStability {
    authority: super::retained_replay::RetainedReplaySnapshotAuthority,
}

impl VerifiedStoreSnapshotStability {
    pub(crate) fn into_authority(self) -> super::retained_replay::RetainedReplaySnapshotAuthority {
        self.authority
    }
}

fn snapshot_history_cut(
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<StoreHistoryCut, StorePullError> {
    match (&snapshot.meta.coverage, &snapshot.meta.state.devices) {
        (
            CommitFrontier::MergeConcurrent(frontier),
            StoreDeviceStateRef::MergeConcurrent { .. },
        ) => Ok(StoreHistoryCut::MergeConcurrent(frontier.clone())),
        (CommitFrontier::Serial(_), StoreDeviceStateRef::Serial { position, .. }) => {
            Ok(StoreHistoryCut::Serial(position.clone()))
        }
        _ => Err(StorePullError::Database(
            "Store snapshot coverage and device state use different policies".to_string(),
        )),
    }
}

async fn accepted_history_cut_for_snapshot_stability(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot_state: &VerifiedStoreHistoryState,
) -> Result<StoreHistoryCut, StorePullError> {
    match &snapshot_state.cut {
        StoreHistoryCut::MergeConcurrent(snapshot_frontier) => {
            if serial_coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge snapshot stability received Serial coordination".to_string(),
                ));
            }
            let mut accepted = snapshot_frontier.clone();
            for (registration_ref, registration) in snapshot_state.active_registrations.values() {
                let stream_id = super::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    registration_ref,
                    super::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
                let discovery =
                    discover_merge_stream(storage, root, registration_ref, registration, None)
                        .await?;
                let Some((_, _, latest, _)) = discovery.commits.last() else {
                    if accepted.contains_key(&stream_id) {
                        return Err(StorePullError::Database(
                            "accepted Merge snapshot history is absent from its author stream"
                                .to_string(),
                        ));
                    }
                    continue;
                };
                if let Some(snapshot_tip) = accepted.get(&stream_id) {
                    if latest.coord.sequence() < snapshot_tip.coord.sequence()
                        || (latest.coord.sequence() == snapshot_tip.coord.sequence()
                            && latest != snapshot_tip)
                    {
                        return Err(StorePullError::Database(
                            "current Merge author stream does not contain the snapshot cut"
                                .to_string(),
                        ));
                    }
                }
                accepted.insert(stream_id, latest.clone());
            }
            Ok(StoreHistoryCut::MergeConcurrent(accepted))
        }
        StoreHistoryCut::Serial(_) => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial snapshot stability requires coordination capability".to_string(),
                )
            })?;
            let head = read_serial_head(storage, coordination, root).await?;
            Ok(StoreHistoryCut::Serial(match head.head.state {
                StoreSerialHeadState::Genesis {
                    root,
                    founder_registration,
                } => StoreSerialPredecessor::Genesis {
                    root,
                    founder_registration,
                },
                StoreSerialHeadState::Commit { commit, .. } => {
                    StoreSerialPredecessor::Commit(commit)
                }
            }))
        }
    }
}

fn activated_acknowledgements_through_cut<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    cut: &'a StoreHistoryCut,
) -> StorePullFuture<'a, Vec<VerifiedActivatedStoreAck>> {
    Box::pin(activated_acknowledgements_through_cut_impl(
        storage,
        serial_coordination,
        root,
        cut,
    ))
}

async fn activated_acknowledgements_through_cut_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    cut: &StoreHistoryCut,
) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
    match cut {
        StoreHistoryCut::MergeConcurrent(frontier) => {
            if serial_coordination.is_some() {
                return Err(StorePullError::Database(
                    "Merge acknowledgement history received Serial coordination".to_string(),
                ));
            }
            let history = verify_merge_history_refs(
                storage,
                root,
                frontier.values().cloned().collect::<Vec<_>>(),
            )
            .await?;
            let mut acknowledgements = Vec::new();
            for (activating_commit, commit) in history.commits {
                let Some((reference, value)) = commit.acknowledgement else {
                    continue;
                };
                let chain = commit
                    .history
                    .summary
                    .acknowledgements
                    .get(&reference.registration.device_id)
                    .ok_or_else(|| {
                        StorePullError::Database(
                            "verified acknowledgement history lacks its exact chain".to_string(),
                        )
                    })?
                    .chain
                    .clone();
                acknowledgements.push(VerifiedActivatedStoreAck {
                    reference,
                    value,
                    chain,
                    activating_commit,
                    activating_commit_value: commit.commit,
                });
            }
            Ok(acknowledgements)
        }
        StoreHistoryCut::Serial(position) => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial acknowledgement history requires coordination capability".to_string(),
                )
            })?;
            let head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &head.head).await?;
            let prefix = match position {
                StoreSerialPredecessor::Genesis {
                    root: cut_root,
                    founder_registration,
                } => {
                    let founder = load_founder_registration(storage, root).await?;
                    let founder_ref = StoreDeviceRegistrationRef::from_registration(
                        &founder.value,
                        founder.object,
                    );
                    if cut_root != root || founder_registration != &founder_ref {
                        return Err(StorePullError::Serial(
                            "Serial acknowledgement cut names another genesis authority"
                                .to_string(),
                        ));
                    }
                    &accepted[..0]
                }
                StoreSerialPredecessor::Commit(reference) => {
                    let index = accepted
                        .iter()
                        .position(|candidate| &candidate.commit_ref == reference)
                        .ok_or_else(|| {
                            StorePullError::Serial(
                                "Serial acknowledgement cut is absent from the accepted chain"
                                    .to_string(),
                            )
                        })?;
                    &accepted[..=index]
                }
            };
            let mut acknowledgements = Vec::new();
            for accepted in prefix {
                let Some((reference, value)) = &accepted.acknowledgement else {
                    continue;
                };
                let chain = load_acknowledgement_proof_chain(
                    storage,
                    root,
                    reference.clone(),
                    value.clone(),
                    &accepted.author,
                )
                .await
                .map_err(|error| match error {
                    RegistrationLoadError::Object(error) => StorePullError::Object(error),
                    RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
                })?;
                acknowledgements.push(VerifiedActivatedStoreAck {
                    reference: reference.clone(),
                    value: value.clone(),
                    chain,
                    activating_commit: accepted.commit_ref.clone(),
                    activating_commit_value: accepted.commit.clone(),
                });
            }
            Ok(acknowledgements)
        }
    }
}

fn verify_store_snapshot_authority<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    snapshot: &'a crate::database::PublishedStoreSnapshot,
) -> StorePullFuture<'a, VerifiedStoreHistoryState> {
    Box::pin(verify_store_snapshot_authority_impl(
        storage,
        serial_coordination,
        root,
        snapshot,
    ))
}

async fn verify_store_snapshot_authority_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreHistoryState, StorePullError> {
    let snapshot_cut = snapshot_history_cut(snapshot)?;
    let snapshot_state = verify_store_history_state(
        storage,
        serial_coordination,
        root,
        &snapshot_cut,
        &snapshot.meta.state.membership,
    )
    .await?;
    if snapshot_state.membership_ref != snapshot.meta.state.membership
        || snapshot_state.device_state_ref != snapshot.meta.state.devices
    {
        return Err(StorePullError::Database(
            "Store snapshot state differs from its exact accepted history".to_string(),
        ));
    }
    let (_, snapshot_author) = snapshot_state
        .active_registrations
        .get(&snapshot.meta.author_registration.device_id)
        .filter(|(reference, _)| reference == &snapshot.meta.author_registration)
        .ok_or(StorePullError::SnapshotAuthorInactive)?;
    if !snapshot_state.is_owner(&snapshot_author.author_pubkey) {
        return Err(StorePullError::SnapshotAuthorNotOwner);
    }
    match (&snapshot_state.membership, &snapshot.meta.history_summary) {
        (
            VerifiedStoreMembership::MergeConcurrent {
                membership,
                checkpoints,
            },
            super::store_commit::StoreSnapshotHistorySummary::MergeConcurrent(summary),
        ) => {
            let canonical = compose_merge_snapshot_history_summary(
                root,
                &snapshot.meta.coverage,
                membership,
                &snapshot_state.device_state,
                &snapshot.meta.author_registration,
                snapshot_author,
                checkpoints.clone(),
            )?;
            if summary != &canonical {
                return Err(StorePullError::Database(
                    "Store snapshot history summary differs from its exact verified cut"
                        .to_string(),
                ));
            }
        }
        (
            VerifiedStoreMembership::Serial(_),
            super::store_commit::StoreSnapshotHistorySummary::Serial,
        ) => {}
        _ => {
            return Err(StorePullError::Database(
                "Store snapshot history summary uses another write policy".to_string(),
            ));
        }
    }
    Ok(snapshot_state)
}

pub(crate) async fn verify_store_snapshot_for_acknowledgement(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<(), StorePullError> {
    verify_store_snapshot_authority(storage, serial_coordination, root, snapshot)
        .await
        .map(|_| ())
}

pub(crate) fn verify_store_snapshot_stability<'a>(
    storage: &'a dyn SyncStorage,
    serial_coordination: Option<&'a dyn CoordinationStorage>,
    root: &'a StoreRootRef,
    snapshot: &'a crate::database::PublishedStoreSnapshot,
) -> StorePullFuture<'a, VerifiedStoreSnapshotStability> {
    Box::pin(verify_store_snapshot_stability_impl(
        storage,
        serial_coordination,
        root,
        snapshot,
    ))
}

async fn verify_store_snapshot_stability_impl(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    snapshot: &crate::database::PublishedStoreSnapshot,
) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
    let snapshot_state =
        verify_store_snapshot_authority(storage, serial_coordination, root, snapshot).await?;
    let snapshot_cut = snapshot_state.cut.clone();

    let accepted_cut = Box::pin(accepted_history_cut_for_snapshot_stability(
        storage,
        serial_coordination,
        root,
        &snapshot_state,
    ))
    .await?;
    let accepted_acknowledgements =
        activated_acknowledgements_through_cut(storage, serial_coordination, root, &accepted_cut)
            .await?;
    let mut acknowledgements = BTreeMap::new();
    for (device_id, (registration_ref, registration)) in &snapshot_state.active_registrations {
        let matching = accepted_acknowledgements
            .iter()
            .filter(|ack| {
                ack.value.registration == *registration_ref
                    && ack.value.snapshot.as_ref().is_some_and(|acknowledged| {
                        acknowledged.author_registration == snapshot.meta.author_registration
                            && acknowledged.snapshot == snapshot.reference
                    })
                    && ack.value.device_state == snapshot.meta.state.devices
                    && ack
                        .value
                        .store_cut
                        .frontier()
                        .covers(&snapshot.meta.coverage)
            })
            .max_by_key(|ack| (ack.reference.sequence, ack.activating_commit.clone()))
            .ok_or_else(|| StorePullError::SnapshotNotStable {
                member: registration.author_pubkey.clone(),
                device_id: device_id.to_string(),
            })?;
        acknowledgements.insert(
            *device_id,
            super::store_commit::RetainedVerifiedActivatedAck {
                chain: matching.chain.clone(),
                activating_commit: matching.activating_commit.clone(),
                activating_commit_value: matching.activating_commit_value.clone(),
            },
        );
    }
    let founder = load_founder_registration(storage, root).await?;
    let authority = super::retained_replay::RetainedReplaySnapshotAuthority {
        store_root: root.clone(),
        founder_registration: StoreDeviceRegistrationRef::from_registration(
            &founder.value,
            founder.object,
        ),
        snapshot: snapshot.reference.clone(),
        metadata: snapshot.meta.clone(),
        snapshot_cut,
        accepted_cut,
        device_state: snapshot_state.device_state,
        active_registrations: snapshot_state
            .active_registrations
            .into_iter()
            .map(|(device_id, (reference, value))| {
                (
                    device_id,
                    super::store_commit::RetainedVerifiedRegistration { reference, value },
                )
            })
            .collect(),
        acknowledgements,
    };
    authority
        .validate()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(VerifiedStoreSnapshotStability { authority })
}

async fn verify_merge_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<(), StorePullError> {
    let super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent {
        commit: activation_commit,
        ..
    } = &acceptance.activation
    else {
        return Err(StorePullError::Database(
            "Merge Owner promotion carries Serial activation".to_string(),
        ));
    };
    let history = verify_merge_history_refs(storage, root, [activation_commit.clone()]).await?;
    verify_merge_owner_promotion_acceptance_with_history(
        storage,
        root,
        acceptance,
        &history.commits,
    )
    .await
}

async fn verify_merge_owner_promotion_acceptance_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
    verified_commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
) -> Result<(), StorePullError> {
    let request = &acceptance.request;
    let super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent {
        commit: activation_commit,
        head: activation_head,
    } = &acceptance.activation
    else {
        return Err(StorePullError::Database(
            "Merge Owner promotion carries Serial activation".to_string(),
        ));
    };
    let promoter = load_registration_ref(storage, root, &request.promoter_registration).await?;
    let candidate = load_registration_ref(storage, root, &request.member_registration).await?;
    request
        .verify(root, &promoter.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    acceptance
        .verify(&candidate.value)
        .map_err(|error| StorePullError::Database(error.to_string()))?;

    let head_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&activation_head.object, ".json")
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_bytes = storage
        .read_protocol_object(&context, &activation_head.object, &head_prefix)
        .await?;
    activation_head.object.verify(&head_bytes)?;
    let head: StoreDeviceHead = serde_json::from_slice(&head_bytes).map_err(|error| {
        StorePullError::Database(format!("Owner-promotion activation head: {error}"))
    })?;
    let opened = super::store_objects::load_head_ref(
        storage,
        root.store_root_hash,
        activation_head,
        &promoter.value,
        activation_commit,
    )
    .await?;
    let (_, exact_head) = super::store_outbound::exact_next_announcement_slot(
        storage,
        root,
        &request.promoter_registration,
        &promoter.value,
        Some(activation_commit),
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if opened.value != head
        || head.head_hash() != activation_head.head_hash
        || head.commit != *activation_commit
        || exact_head.as_ref() != Some(activation_head)
    {
        return Err(StorePullError::Database(
            "Owner-promotion request is not activated by its exact Merge head".to_string(),
        ));
    }
    let verified = verified_commits.get(activation_commit).ok_or_else(|| {
        StorePullError::Database(
            "Owner-promotion request activation is absent from its verified history".to_string(),
        )
    })?;
    if verified.commit.owner_promotion_request() != Some(request)
        || verified.commit.membership_state != request.predecessor_membership
        || verified.commit.device_state != request.predecessor_devices
        || verified.commit.author_registration != request.promoter_registration
    {
        return Err(StorePullError::Database(
            "Owner-promotion request commit differs from its signed predecessor authority"
                .to_string(),
        ));
    }
    let verified_membership_activations = verified_merge_membership_prefix(
        verified_commits,
        commit_predecessor_references(&verified.commit),
    )?;
    let membership = load_merge_predecessor_membership_with_verified_activations(
        storage,
        root,
        &request.predecessor_membership,
        &verified_membership_activations,
        None,
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })?;
    verify_merge_membership_state_ref(
        &request.predecessor_membership,
        &membership,
        &verified.predecessor_state,
    )?;
    if !device_state_has_active_registration(
        &verified.predecessor_state,
        &request.promoter_registration,
    ) || !device_state_has_active_registration(
        &verified.predecessor_state,
        &request.member_registration,
    ) {
        return Err(StorePullError::Database(
            "Owner-promotion request registrations are not active at its exact predecessor"
                .to_string(),
        ));
    }
    if membership
        .active_owner_grant(&promoter.value.author_pubkey)
        .as_ref()
        != Some(&request.promoter_owner_grant)
        || membership.active_grant_ids(&request.member_pubkey)
            != BTreeSet::from([request.member_grant.clone()])
        || membership
            .active_grant(&request.member_grant)
            .is_none_or(|record| {
                record.member_pubkey != request.member_pubkey
                    || record.role != super::membership::StoreMembershipRoleGrant::Member
            })
        || candidate.value.author_pubkey != request.member_pubkey
    {
        return Err(StorePullError::Database(
            "Owner-promotion request does not name the exact active Owner and Member grants"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) enum VerifiedOwnerPromotionAcceptance {
    MergeConcurrent,
    Serial(super::store_outbound::SerialAuthorizationSnapshot),
}

pub(crate) async fn verify_owner_promotion_acceptance(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    root: &StoreRootRef,
    acceptance: &super::store_commit::OwnerPromotionAcceptance,
) -> Result<VerifiedOwnerPromotionAcceptance, StorePullError> {
    match &acceptance.activation {
        super::store_commit::OwnerPromotionRequestActivation::MergeConcurrent { .. } => {
            verify_merge_owner_promotion_acceptance(storage, root, acceptance).await?;
            Ok(VerifiedOwnerPromotionAcceptance::MergeConcurrent)
        }
        super::store_commit::OwnerPromotionRequestActivation::Serial { .. } => {
            let coordination = serial_coordination.ok_or_else(|| {
                StorePullError::Serial(
                    "Serial Owner-promotion verification requires coordination".to_string(),
                )
            })?;
            let request = &acceptance.request;
            let promoter =
                load_registration_ref(storage, root, &request.promoter_registration).await?;
            let candidate =
                load_registration_ref(storage, root, &request.member_registration).await?;
            request
                .verify(root, &promoter.value)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            acceptance
                .verify(&candidate.value)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
            let verified_head = read_serial_head(storage, coordination, root).await?;
            let accepted = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
            let mut matches = accepted
                .iter()
                .filter(|candidate| candidate.commit.owner_promotion_request() == Some(request));
            let Some(activated) = matches.next() else {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has no accepted Serial activation".to_string(),
                ));
            };
            if matches.next().is_some() {
                return Err(StorePullError::Serial(
                    "Owner-promotion request has more than one Serial activation".to_string(),
                ));
            }
            let discovered = super::store_commit::OwnerPromotionRequestActivation::Serial {
                commit: activated.commit_ref.clone(),
            };
            if discovered != acceptance.activation {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion acceptance names another activation".to_string(),
                ));
            }
            let commit = &activated.commit;
            if commit.owner_promotion_request() != Some(request)
                || commit.membership_state != request.predecessor_membership
                || commit.device_state != request.predecessor_devices
                || commit.author_registration != request.promoter_registration
            {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion request commit differs from its signed authority"
                        .to_string(),
                ));
            }
            if !device_state_has_active_registration(
                &activated.device_state_before,
                &request.promoter_registration,
            ) || !device_state_has_active_registration(
                &activated.device_state_before,
                &request.member_registration,
            ) {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion registrations are not active at its predecessor"
                        .to_string(),
                ));
            }
            if activated
                .authorization_before
                .membership
                .active_owner_grant(&promoter.value.author_pubkey)
                .as_ref()
                != Some(&request.promoter_owner_grant)
                || activated
                    .authorization_before
                    .membership
                    .active_grant_ids(&request.member_pubkey)
                    != BTreeSet::from([request.member_grant.clone()])
                || !activated
                    .authorization_before
                    .membership
                    .is_member_grant(&request.member_pubkey, &request.member_grant)
                || candidate.value.author_pubkey != request.member_pubkey
            {
                return Err(StorePullError::Serial(
                    "Serial Owner-promotion request does not name the active Owner and Member"
                        .to_string(),
                ));
            }
            let authorization = accepted
                .last()
                .ok_or_else(|| {
                    StorePullError::Serial(
                        "Serial Owner-promotion activation has no accepted commit".to_string(),
                    )
                })?
                .authorization_after
                .clone();
            let base = match &verified_head.head.state {
                StoreSerialHeadState::Genesis { .. } => None,
                StoreSerialHeadState::Commit { commit, .. } => Some(commit.clone()),
            };
            Ok(VerifiedOwnerPromotionAcceptance::Serial(
                super::store_outbound::SerialAuthorizationSnapshot {
                    base,
                    base_head: verified_head.object,
                    authorization,
                },
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn verify_terminal_candidate_head(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    candidate: &StoreBatchCommitRef,
    candidate_commit: &StoreBatchCommit,
    candidate_head: &StoreDeviceHead,
    candidate_head_object: &ExactObjectRef,
    candidate_author: &StoreDeviceRegistration,
) -> Result<super::remote_object::VerifiedCandidateHead, StorePullError> {
    if candidate_head.commit != *candidate
        || candidate_head.author_registration != candidate_commit.author_registration
    {
        return Err(StorePullError::Database(
            "terminal candidate head names another commit or author".to_string(),
        ));
    }
    StoreDeviceHead::parse_at(
        &candidate_head.to_bytes(),
        root.store_root_hash,
        candidate_author,
        candidate,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate
        .verify_commit(candidate_commit)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate_commit
        .verify_at(root.store_root_hash, &candidate.coord, candidate_author)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate_head_object.verify(&candidate_head.to_bytes())?;
    let (candidate_slot, predecessor_head) = super::store_outbound::exact_next_announcement_slot(
        storage,
        root,
        &candidate_commit.author_registration,
        candidate_author,
        candidate_commit.order.predecessor(),
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    let activation = candidate_author
        .store_announcement_activation(&candidate_commit.author_registration)
        .map_err(|error| StorePullError::Database(error.to_string()))?
        .activation_id();
    if candidate_slot != *candidate_head_object.slot()
        || candidate_head.successor.activation != activation
        || candidate_head.successor.predecessor
            != predecessor_head.map(|reference| reference.object)
    {
        return Err(StorePullError::Database(
            "terminal candidate head does not occupy its exact successor slot".to_string(),
        ));
    }
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let candidate_prefix = head_slot_prefix(
        &candidate_head.author_registration.device_id.to_string(),
        candidate.coord.sequence(),
    );
    match storage
        .read_protocol_slot(&context, &candidate_slot, &candidate_prefix)
        .await
    {
        Err(StorageError::NotFound(_)) => Ok(
            super::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                object: candidate_head_object.clone(),
            },
        ),
        Ok((bytes, object))
            if bytes == candidate_head.to_bytes() && object == *candidate_head_object =>
        {
            Ok(
                super::remote_object::VerifiedCandidateHead::ExactLateCandidate {
                    object: candidate_head_object.clone(),
                },
            )
        }
        Ok((bytes, object)) => {
            object.verify(&bytes)?;
            let unverified: StoreDeviceHead = serde_json::from_slice(&bytes).map_err(|error| {
                StorePullError::Database(format!(
                    "parse competing terminal candidate head: {error}"
                ))
            })?;
            if object.slot() != candidate_head_object.slot()
                || unverified.author_registration != candidate_head.author_registration
                || unverified.commit.coord != candidate_head.commit.coord
                || unverified.successor != candidate_head.successor
            {
                return Err(StorePullError::Database(
                    "competing terminal candidate head differs from the exact successor point"
                        .to_string(),
                ));
            }
            load_commit_ref(
                storage,
                root.store_root_hash,
                &unverified.commit,
                candidate_author,
            )
            .await?;
            let winner = StoreDeviceHead::parse_at(
                &bytes,
                root.store_root_hash,
                candidate_author,
                &unverified.commit,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
            if winner != unverified {
                return Err(StorePullError::Database(
                    "competing terminal candidate head is not authenticated".to_string(),
                ));
            }
            Ok(
                super::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                    object: candidate_head_object.clone(),
                },
            )
        }
        Err(error) => Err(StorePullError::Storage(error)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn verify_author_exclusion_activation_with_verified_operation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    locator: &crate::database::AuthorExclusionActivationLocator,
    activation_head: &StoreDeviceHead,
    activation_head_object: &ExactObjectRef,
    activation_commit_ref: &StoreBatchCommitRef,
    activation_commit: &StoreBatchCommit,
    activation_predecessor_state: &ResolvedStoreDeviceState,
    operations: &VerifiedStoreDeviceOperations,
    candidate: &StoreBatchCommitRef,
    candidate_commit: &StoreBatchCommit,
    candidate_head: &StoreDeviceHead,
    candidate_head_object: &ExactObjectRef,
) -> Result<VerifiedAuthorExclusionActivation, StorePullError> {
    let verified_activation_head = super::store_commit::StoreDeviceHeadRef {
        head_hash: activation_head.head_hash(),
        object: activation_head_object.clone(),
    };
    activation_commit_ref
        .verify_commit(activation_commit)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if activation_head.commit != *activation_commit_ref
        || locator.activation_head() != &verified_activation_head
        || !activation_commit.device_exclusion_outcomes().contains(
            &super::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(
                locator.exclusion().clone(),
            ),
        )
        || !device_state_has_active_registration(
            activation_predecessor_state,
            &locator.exclusion().proposal.target,
        )
    {
        return Err(StorePullError::Database(
            "author exclusion activation differs from its verified commit and predecessor"
                .to_string(),
        ));
    }
    let exact_cut = operations
        .exclusions()
        .find_map(|(exclusion, cut)| (exclusion == locator.exclusion()).then_some(cut));
    if exact_cut
        != Some(&StoreHistoryCut::MergeConcurrent(
            locator.accepted_cut().clone(),
        ))
    {
        return Err(StorePullError::Database(
            "author exclusion locator differs from the verified outcome cutoff".to_string(),
        ));
    }
    let target_registration = Box::pin(load_registration_ref(
        storage,
        root,
        &locator.exclusion().proposal.target,
    ))
    .await?;
    if candidate_head.commit != *candidate
        || candidate_head.author_registration != locator.exclusion().proposal.target
        || candidate_commit.author_registration != candidate_head.author_registration
    {
        return Err(StorePullError::Database(
            "candidate head differs from the excluded author and exact candidate".to_string(),
        ));
    }
    let verified_candidate_head = Box::pin(verify_terminal_candidate_head(
        storage,
        root,
        candidate,
        candidate_commit,
        candidate_head,
        candidate_head_object,
        &target_registration.value,
    ))
    .await?;
    Ok(VerifiedAuthorExclusionActivation {
        store_root_hash: root.store_root_hash,
        target: locator.exclusion().proposal.target.clone(),
        target_registration: target_registration.value,
        exclusion: locator.exclusion().clone(),
        accepted_cut: locator.accepted_cut().clone(),
        activation_head: verified_activation_head,
        candidate: candidate.clone(),
        candidate_head: verified_candidate_head,
    })
}

pub(crate) async fn verify_author_exclusion_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    locator: &crate::database::AuthorExclusionActivationLocator,
    candidate: &StoreBatchCommitRef,
    candidate_commit: &StoreBatchCommit,
    candidate_head: &StoreDeviceHead,
    candidate_head_object: &ExactObjectRef,
) -> Result<VerifiedAuthorExclusionActivation, StorePullError> {
    let retained =
        Box::pin(db.retained_merge_materialization(locator.activation_commit().clone())).await?;
    let (_, predecessor_state) =
        Box::pin(db.store_device_state_for_order(&retained.commit().order)).await?;
    Box::pin(verify_author_exclusion_activation_with_verified_operation(
        storage,
        root,
        locator,
        retained.activation_head(),
        retained.activation_head_object(),
        retained.commit_ref(),
        retained.commit(),
        &predecessor_state,
        retained.device_operations(),
        candidate,
        candidate_commit,
        candidate_head,
        candidate_head_object,
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn verify_membership_grant_revocation_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    grant_id: &super::membership::MembershipGrantId,
    membership: &super::circle_control::MergeStoreMembershipStateRef,
    activation_commit: &StoreBatchCommitRef,
    activation_head: &super::store_commit::StoreDeviceHeadRef,
    candidate: &StoreBatchCommitRef,
    candidate_commit: &StoreBatchCommit,
    candidate_head: &StoreDeviceHead,
    candidate_head_object: &ExactObjectRef,
) -> Result<VerifiedMembershipGrantRevocationActivation, StorePullError> {
    let head_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&activation_head.object, ".json")
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_bytes = storage
        .read_protocol_object(&context, &activation_head.object, &head_prefix)
        .await?;
    activation_head.object.verify(&head_bytes)?;
    let witness_head: StoreDeviceHead = serde_json::from_slice(&head_bytes).map_err(|error| {
        StorePullError::Database(format!("membership revocation witness head: {error}"))
    })?;
    if witness_head.head_hash() != activation_head.head_hash
        || &witness_head.commit != activation_commit
    {
        return Err(StorePullError::Database(
            "membership revocation witness head differs from its exact activation".to_string(),
        ));
    }
    let witness_author =
        load_registration_ref(storage, root, &witness_head.author_registration).await?;
    let opened = super::store_objects::load_head_ref(
        storage,
        root.store_root_hash,
        activation_head,
        &witness_author.value,
        &witness_head.commit,
    )
    .await?;
    let (_, exact_head) = super::store_outbound::exact_next_announcement_slot(
        storage,
        root,
        &witness_head.author_registration,
        &witness_author.value,
        Some(&witness_head.commit),
    )
    .await
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    if exact_head.as_ref() != Some(activation_head) || opened.value != witness_head {
        return Err(StorePullError::Database(
            "membership revocation witness is not an accepted exact head".to_string(),
        ));
    }
    let witness_commit = load_commit_ref(
        storage,
        root.store_root_hash,
        &witness_head.commit,
        &witness_author.value,
    )
    .await?;
    let (_, _, replayed_witness_commit, _) = Box::pin(replay_merge_device_history(
        storage,
        root,
        &witness_head.commit,
    ))
    .await?;
    if replayed_witness_commit != witness_commit.value {
        return Err(StorePullError::Database(
            "membership revocation witness commit differs from its verified history".to_string(),
        ));
    }
    if witness_commit.value.membership_state
        != StoreMembershipStateRef::MergeConcurrent(membership.clone())
    {
        return Err(StorePullError::Database(
            "membership revocation witness commit names another membership state".to_string(),
        ));
    }
    let current_membership =
        load_merge_predecessor_membership(storage, root, &witness_commit.value.membership_state)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
    let MembershipStatus::Resolved(current) = current_membership.status() else {
        return Err(StorePullError::Database(
            "membership revocation witness state is conflicted".to_string(),
        ));
    };
    let Some(super::causal_grants::GrantState::Tombstoned {
        record: current_record,
        ..
    }) = current.grants.get(grant_id)
    else {
        return Err(StorePullError::Database(
            "membership revocation witness grant is not tombstoned".to_string(),
        ));
    };
    let candidate_author =
        load_registration_ref(storage, root, &candidate_commit.author_registration).await?;
    candidate
        .verify_commit(candidate_commit)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let predecessor_membership =
        load_merge_predecessor_membership(storage, root, &candidate_commit.membership_state)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
    let MembershipStatus::Resolved(predecessor) = predecessor_membership.status() else {
        return Err(StorePullError::Database(
            "membership revocation candidate predecessor is conflicted".to_string(),
        ));
    };
    let Some(predecessor_record) = predecessor.active_grant(grant_id) else {
        return Err(StorePullError::Database(
            "membership revocation grant was not active at the candidate predecessor".to_string(),
        ));
    };
    if predecessor_record != current_record
        || predecessor_record.member_pubkey != candidate_author.value.author_pubkey
        || candidate_commit.membership_authority.as_ref()
            != Some(&predecessor_record.creation_authority)
    {
        return Err(StorePullError::Database(
            "membership revocation grant differs from the candidate's signed authority".to_string(),
        ));
    }
    let StoreHistoryCut::MergeConcurrent(cap) = witness_commit
        .value
        .order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
    else {
        return Err(StorePullError::Database(
            "membership revocation witness is not Merge".to_string(),
        ));
    };
    let expected_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &candidate_commit.author_registration,
        super::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = candidate.coord
    else {
        return Err(StorePullError::Database(
            "membership revocation candidate is not Merge".to_string(),
        ));
    };
    if stream_id != expected_stream
        || cap
            .get(&expected_stream)
            .is_some_and(|covered| sequence <= covered.coord.sequence())
    {
        return Err(StorePullError::Database(
            "membership revocation candidate is not beyond the accepted witness cut".to_string(),
        ));
    }
    let verified_candidate_head = verify_terminal_candidate_head(
        storage,
        root,
        candidate,
        candidate_commit,
        candidate_head,
        candidate_head_object,
        &candidate_author.value,
    )
    .await?;
    Ok(VerifiedMembershipGrantRevocationActivation {
        store_root_hash: root.store_root_hash,
        grant_id: grant_id.clone(),
        membership: membership.clone(),
        activation_commit: witness_head.commit,
        activation_head: activation_head.clone(),
        candidate: candidate.clone(),
        candidate_author: candidate_author.value,
        candidate_head: verified_candidate_head,
    })
}

pub(crate) async fn prepare_device_join_bootstrap(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &StoreHistoryCut,
    attempt_activation: &StoreBatchCommitRef,
    verified_authorization: &DeviceJoinBootstrapAuthorization,
) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
    if coverage.policy() != attempt_activation.coord.policy() {
        return Err(StorePullError::Database(
            "device join bootstrap cut and attempt activation use different policies".to_string(),
        ));
    }
    if matches!(coverage, StoreHistoryCut::Serial(_)) {
        return Box::pin(prepare_serial_device_join_bootstrap(
            storage,
            root,
            coverage,
            attempt_activation,
            verified_authorization,
        ))
        .await;
    }
    let DeviceJoinBootstrapAuthorization::MergeConcurrent {
        state: verified_state,
        chain: _,
    } = verified_authorization
    else {
        return Err(StorePullError::Database(
            "Merge device join bootstrap received Serial membership authority".to_string(),
        ));
    };
    let verified_root = load_store_protocol_root(storage, root).await?.value;
    let founder = Box::pin(load_founder_registration(storage, root)).await?;
    let founder_reference =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_reference.clone(),
        &verified_root.descriptor.founder_pubkey,
        verified_root.descriptor.founder_grant.clone(),
        &verified_root.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;

    let mut pending = history_cut_references(coverage);
    pending.push(attempt_activation.clone());
    let verified_history =
        verify_merge_history_refs(storage, root, pending.iter().cloned()).await?;
    let mut loaded =
        BTreeMap::<StoreBatchCommitRef, (StoreBatchCommit, StoreDeviceRegistration)>::new();
    while let Some(reference) = pending.pop() {
        if loaded.contains_key(&reference) {
            continue;
        }
        let (commit, author) = Box::pin(load_commit_with_author(storage, root, &reference)).await?;
        pending.extend(commit_predecessor_references(&commit));
        loaded.insert(reference, (commit, author));
    }
    let activation = loaded.get(attempt_activation).ok_or_else(|| {
        StorePullError::Database("device join attempt activation is absent from its graph".into())
    })?;
    if activation
        .0
        .order
        .predecessor_cut()
        .map_err(|error| StorePullError::Database(error.to_string()))?
        != *coverage
    {
        return Err(StorePullError::Database(
            "device join attempt activation predecessor differs from its signed bootstrap cut"
                .to_string(),
        ));
    }
    let verified_activation = verified_history
        .commits
        .get(attempt_activation)
        .ok_or_else(|| {
            StorePullError::Database(
                "device join attempt activation is absent from its verified Merge history"
                    .to_string(),
            )
        })?;
    if &verified_activation.commit.membership_state != verified_state {
        return Err(StorePullError::Database(
            "device join attempt activation differs from its exact verified membership state"
                .to_string(),
        ));
    }

    let mut states = BTreeMap::<StoreBatchCommitRef, ResolvedStoreDeviceState>::new();
    let mut ordered = Vec::with_capacity(loaded.len());
    while !loaded.is_empty() {
        let next = loaded.iter().find_map(|(reference, (commit, _))| {
            commit_predecessor_references(commit)
                .iter()
                .all(|dependency| states.contains_key(dependency))
                .then(|| reference.clone())
        });
        let Some(reference) = next else {
            return Err(StorePullError::Database(
                "device join bootstrap history is cyclic or has an unresolved predecessor"
                    .to_string(),
            ));
        };
        let (commit, author) = loaded
            .remove(&reference)
            .expect("selected bootstrap commit remains loaded");
        let predecessor_state = match &commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                verified_merge_predecessor_state(&genesis, &states, &commit)?
            }
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Genesis { .. },
                ..
            } => genesis.clone(),
            super::store_commit::StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => states
                .get(predecessor)
                .expect("topological Serial predecessor state exists")
                .clone(),
        };
        let verified_commit = verified_history.commits.get(&reference).ok_or_else(|| {
            StorePullError::Database(
                "device join bootstrap commit is absent from its verified Merge history"
                    .to_string(),
            )
        })?;
        if verified_commit.commit != commit
            || verified_commit.predecessor_state != predecessor_state
        {
            return Err(StorePullError::Database(
                "device join bootstrap commit differs from its verified Merge history".to_string(),
            ));
        }
        let carries_lifecycle = !(commit.device_join_attempt_decisions().is_empty()
            && commit.device_join_outcomes().is_empty()
            && commit.device_join_cleanup_receipts().is_empty()
            && commit.device_registrations().is_empty()
            && commit.device_exclusion_proposals().is_empty()
            && commit.device_exclusion_outcomes().is_empty()
            && commit.reclaim_authorization().is_none()
            && commit.reclaim_receipt().is_none());
        let authority = RegistrationPredecessorAuthority::MergeConcurrent(
            &verified_commit.predecessor_membership,
        );
        let accepted_predecessor = VerifiedAcceptedPredecessor::MergeHistory {
            commits: &verified_history.commits,
            frontier: commit_predecessor_references(&commit),
        };
        let registrations = Box::pin(load_commit_registrations(
            storage,
            root,
            &commit,
            &author,
            carries_lifecycle.then_some(&authority),
            Some(&accepted_predecessor),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let (authorized_predecessor, recovery_author) =
            predecessor_with_recovery_author(predecessor_state, &commit, &registrations)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        if !device_state_has_active_registration(
            &authorized_predecessor,
            &commit.author_registration,
        ) {
            return Err(StorePullError::Database(
                "device join bootstrap commit author is inactive at its predecessor".to_string(),
            ));
        }
        let resolver = DeviceStateResolver::Loaded {
            genesis: &genesis,
            states: &states,
        };
        let device_operations = load_commit_device_operations(
            Some(&resolver),
            storage,
            root,
            &commit,
            &authorized_predecessor,
            carries_lifecycle.then_some(&authority),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        if matches!(
            commit.control(),
            Some(super::store_commit::StoreControl::MergeMembership { .. })
        ) {
            verify_merge_membership_control(storage, root, &reference, &commit)
                .await
                .map_err(StorePullError::Database)?;
        }
        let owner_recovery =
            verify_commit_owner_recovery_activation(storage, root, &commit, None).await?;
        let activation = match &commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                let (_, head_ref) = super::store_outbound::exact_next_announcement_slot(
                    storage,
                    root,
                    &commit.author_registration,
                    &author,
                    Some(&reference),
                )
                .await
                .map_err(|error| StorePullError::Database(error.to_string()))?;
                let head_ref = head_ref.ok_or_else(|| {
                    StorePullError::Database(
                        "Merge bootstrap commit has no exact accepted activation head".to_string(),
                    )
                })?;
                let head = super::store_objects::load_head_ref(
                    storage,
                    root.store_root_hash,
                    &head_ref,
                    &author,
                    &reference,
                )
                .await?;
                DeviceJoinBootstrapActivation::MergeConcurrent {
                    head: head.value,
                    object: head.object,
                    history_summary: verified_commit.history.summary.clone(),
                }
            }
            super::store_commit::StoreCommitOrder::Serial { .. } => {
                DeviceJoinBootstrapActivation::Serial
            }
        };
        let state = device_operations
            .apply_to(authorized_predecessor, &commit.device_state)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let state = apply_verified_device_lifecycle(
            state,
            &commit,
            &registrations,
            recovery_author.as_ref(),
            owner_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        states.insert(reference.clone(), state);
        ordered.push(DeviceJoinBootstrapCommit {
            reference,
            commit,
            registrations,
            device_operations,
            activation,
        });
    }

    Ok(DeviceJoinBootstrapPlan {
        founder_reference,
        founder: founder.value,
        founder_bytes: founder.bytes,
        genesis,
        coverage: coverage.clone(),
        commits: ordered,
    })
}

async fn prepare_serial_device_join_bootstrap(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &StoreHistoryCut,
    attempt_activation: &StoreBatchCommitRef,
    verified_authorization: &DeviceJoinBootstrapAuthorization,
) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
    let StoreHistoryCut::Serial(coverage_position) = coverage else {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap received a Merge history cut".to_string(),
        ));
    };
    let DeviceJoinBootstrapAuthorization::Serial {
        state: verified_state,
        position: verified_position,
        authorization: verified_authorization,
    } = verified_authorization
    else {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap received Merge membership authority".to_string(),
        ));
    };
    if verified_position != coverage_position {
        return Err(StorePullError::Serial(
            "Serial device join bootstrap cut differs from its verified membership position"
                .to_string(),
        ));
    }

    let (authorized, _, _) = Box::pin(load_authorized_serial_prefix(
        storage,
        root,
        Some(attempt_activation.clone()),
    ))
    .await?;
    let activation = authorized.last().ok_or_else(|| {
        StorePullError::Serial(
            "device join attempt activation is absent from its Serial history".to_string(),
        )
    })?;
    if activation.commit_ref != *attempt_activation
        || activation
            .commit
            .order
            .predecessor_cut()
            .map_err(|error| StorePullError::Serial(error.to_string()))?
            != *coverage
    {
        return Err(StorePullError::Serial(
            "device join attempt activation predecessor differs from its signed bootstrap cut"
                .to_string(),
        ));
    }
    if &activation.commit.membership_state != verified_state
        || &activation.authorization_before != verified_authorization
    {
        return Err(StorePullError::Serial(
            "device join attempt activation differs from its exact verified membership state"
                .to_string(),
        ));
    }

    let verified_root = load_store_protocol_root(storage, root).await?.value;
    let founder = load_founder_registration_with_root(storage, root, &verified_root).await?;
    let founder_reference =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_reference.clone(),
        &verified_root.descriptor.founder_pubkey,
        verified_root.descriptor.founder_grant.clone(),
        &verified_root.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let commits = authorized
        .into_iter()
        .map(|authorized| DeviceJoinBootstrapCommit {
            reference: authorized.commit_ref,
            commit: authorized.commit,
            registrations: authorized.registrations,
            device_operations: authorized.device_operations,
            activation: DeviceJoinBootstrapActivation::Serial,
        })
        .collect();
    Ok(DeviceJoinBootstrapPlan {
        founder_reference,
        founder: founder.value,
        founder_bytes: founder.bytes,
        genesis,
        coverage: coverage.clone(),
        commits,
    })
}

pub(crate) async fn materialize_device_join_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
    expected_outcome: &super::store_commit::DeviceJoinOutcomeRef,
    authorization: &DeviceJoinBootstrapAuthorization,
) -> Result<(), StorePullError> {
    let (stream_id, sequence) = match reference.coord {
        StoreCommitCoord::MergeConcurrent {
            stream_id,
            sequence,
        } => (stream_id.to_string(), sequence),
        StoreCommitCoord::Serial { sequence } => (SERIAL_STREAM_ID.to_string(), sequence),
    };
    if let Some(materialized) = db.exact_materialized_ref(&stream_id, sequence).await? {
        if materialized == *reference {
            return Ok(());
        }
        return Err(StorePullError::Database(format!(
            "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
        )));
    }
    let (commit, author) = Box::pin(load_commit_with_author(storage, root, reference)).await?;
    if commit.device_join_outcomes() != std::slice::from_ref(expected_outcome)
        || !commit.device_join_attempt_decisions().is_empty()
        || !commit.device_join_cleanup_receipts().is_empty()
        || commit.device_registrations().len() != 1
        || !commit.provider_access_grants().is_empty()
        || !commit.provider_access_withdrawals().is_empty()
        || !commit.device_retirements().is_empty()
        || !commit.circle_controls().is_empty()
        || !commit.circle_packages().is_empty()
        || commit.store_package().is_some()
        || commit.reclaim_authorization().is_some()
        || commit.reclaim_receipt().is_some()
        || commit.control().is_some()
    {
        return Err(StorePullError::Database(
            "device join activation commit carries unrelated operations".to_string(),
        ));
    }
    let authority = match authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            RegistrationPredecessorAuthority::MergeConcurrent(chain)
        }
        DeviceJoinBootstrapAuthorization::Serial {
            position,
            authorization,
            ..
        } => RegistrationPredecessorAuthority::Serial {
            authorization,
            position: position.clone(),
            history: SerialAuthorizationHistory::ExactPredecessor,
        },
    };
    let accepted_predecessor = VerifiedAcceptedPredecessor::Exact;
    let registrations = Box::pin(load_commit_registrations(
        storage,
        root,
        &commit,
        &author,
        Some(&authority),
        Some(&accepted_predecessor),
    ))
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })?;
    let author_authorized = match authorization {
        DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. } => {
            membership_authorizes(Some(chain), &commit, &author)
        }
        DeviceJoinBootstrapAuthorization::Serial { authorization, .. } => {
            authorization.membership.can_write(&author.author_pubkey)
        }
    };
    if !author_authorized {
        return Err(StorePullError::Database(
            "device join activation author is not authorized by its exact predecessor membership"
                .to_string(),
        ));
    }
    enum Materialization {
        MergeConcurrent {
            head: StoreDeviceHead,
            object: ExactObjectRef,
            history_summary: RetainedVerifiedMergeHistorySummary,
        },
        Serial(SerialAuthorizationState),
    }
    let activation = match (&reference.coord, authorization) {
        (
            StoreCommitCoord::MergeConcurrent { .. },
            DeviceJoinBootstrapAuthorization::MergeConcurrent { chain, .. },
        ) => {
            let (_, head_ref) = super::store_outbound::exact_next_announcement_slot(
                storage,
                root,
                &commit.author_registration,
                &author,
                Some(reference),
            )
            .await
            .map_err(|error| StorePullError::Database(error.to_string()))?;
            let head_ref = head_ref.ok_or_else(|| {
                StorePullError::Database(
                    "device join activation has no exact accepted activation head".to_string(),
                )
            })?;
            let head = super::store_objects::load_head_ref(
                storage,
                root.store_root_hash,
                &head_ref,
                &author,
                reference,
            )
            .await?;
            let (_, predecessor_state) = db.store_device_state_for_order(&commit.order).await?;
            let (authorized_predecessor, recovery_author) =
                predecessor_with_recovery_author(predecessor_state, &commit, &registrations)
                    .map_err(|error| StorePullError::Database(error.to_string()))?;
            let device_operations = VerifiedStoreDeviceOperations::without_exclusions(&commit)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let state_after = device_operations
                .apply_to(authorized_predecessor, &commit.device_state)
                .and_then(|state| {
                    apply_verified_device_lifecycle(
                        state,
                        &commit,
                        &registrations,
                        recovery_author.as_ref(),
                        None,
                    )
                })
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let history = prepare_merge_history_successor(
                db,
                root,
                &commit,
                reference,
                chain,
                &author,
                recovery_author.as_ref(),
                state_after.clone(),
                MergeHistorySuccessorEvidence {
                    registrations: commit
                        .device_registrations()
                        .iter()
                        .zip(&registrations)
                        .map(|(activation, (value, _))| RetainedVerifiedRegistration {
                            reference: activation.registration.clone(),
                            value: value.clone(),
                        })
                        .collect(),
                    acknowledgement: None,
                    membership_proof: None,
                },
            )
            .await?;
            history
                .summary
                .open(&commit, reference, &head.value, &head_ref, &state_after)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            Materialization::MergeConcurrent {
                head: head.value,
                object: head.object,
                history_summary: history.summary,
            }
        }
        (
            StoreCommitCoord::Serial { .. },
            DeviceJoinBootstrapAuthorization::Serial { authorization, .. },
        ) => Materialization::Serial(authorization.clone()),
        _ => {
            return Err(StorePullError::Database(
                "device join activation authority differs from commit policy".to_string(),
            ));
        }
    };
    let root = root.clone();
    let commit_ref = reference.clone();
    let expected_ref = reference.clone();
    db.call(move |connection| {
        let tx = connection
            .unchecked_transaction()
            .map_err(DbError::from)?;
        if let Some(materialized) =
            Database::materialized_commit_ref_on(&tx, &stream_id, sequence)?
        {
            if materialized != expected_ref {
                return Err(DbError::Message(format!(
                    "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
                )));
            }
            tx.commit().map_err(DbError::from)?;
            return Ok(());
        }
        Database::record_activated_store_device_registrations_on(
            &tx,
            &commit,
            &registrations,
        )?;
        match activation {
            Materialization::MergeConcurrent {
                head,
                object,
                history_summary,
            } => {
                Database::record_materialized_merge_commit_on(
                    &tx,
                    &root,
                    &commit,
                    &commit_ref,
                    &registrations,
                    &head,
                    &object,
                    &history_summary,
                    &[],
                    None,
                )?;
            }
            Materialization::Serial(authorization) => {
                Database::record_materialized_serial_commit_on(
                    &tx,
                    &commit,
                    &commit_ref,
                    &authorization,
                )?;
            }
        }
        tx.commit().map_err(DbError::from)
    })
    .await?;
    Ok(())
}

async fn load_authorized_serial_prefix(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    tip: Option<StoreBatchCommitRef>,
) -> Result<
    (
        Vec<AuthorizedSerialCommit>,
        SerialAuthorizationState,
        ResolvedStoreDeviceState,
    ),
    StorePullError,
> {
    let root_value = load_store_protocol_root(storage, root).await?.value;
    if root_value.descriptor.write_policy != crate::WritePolicy::Serial {
        return Err(StorePullError::Serial(format!(
            "Store protocol root uses {:?}, not Serial",
            root_value.descriptor.write_policy
        )));
    }

    let mut expected = tip;
    let mut reverse = Vec::new();
    while let Some(reference) = expected {
        if !matches!(reference.coord, StoreCommitCoord::Serial { .. }) {
            return Err(StorePullError::Serial(
                "global predecessor chain contains a Merge commit reference".to_string(),
            ));
        }
        let (commit, author) =
            load_commit_with_author_at_root(storage, root, &root_value, &reference).await?;
        expected = commit.order.predecessor().cloned();
        reverse.push((reference, commit, author));
    }
    reverse.reverse();

    let founder = load_founder_registration_with_root(storage, root, &root_value).await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let mut authorization =
        SerialAuthorizationState::from_founder(root, &root_value, &founder_ref, &founder.value)
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let genesis_authorization = Box::new(authorization.clone());
    let genesis_position = super::store_commit::SerialStorePosition::Genesis {
        root: root.clone(),
        founder_registration: founder_ref.clone(),
    };
    let mut device_state = ResolvedStoreDeviceState::founder(
        root,
        founder_ref.clone(),
        &root_value.descriptor.founder_pubkey,
        root_value.descriptor.founder_grant.clone(),
        &root_value.descriptor.founder_recovery,
    )
    .map_err(|error| StorePullError::Serial(error.to_string()))?;
    let mut predecessor = None;
    let mut authorized = Vec::with_capacity(reverse.len());
    let mut accepted_commits = BTreeSet::new();

    for (reference, commit, author) in reverse {
        match (&predecessor, &commit.order) {
            (
                None,
                super::store_commit::StoreCommitOrder::Serial {
                    seq: 1,
                    predecessor:
                        StoreSerialPredecessor::Genesis {
                            root: genesis_root,
                            founder_registration,
                        },
                },
            ) if genesis_root == root && founder_registration == &founder_ref => {
                let recovery_author =
                    commit
                        .serial_recovery_activation()
                        .as_ref()
                        .is_some_and(|activation| {
                            activation.registration.registration == commit.author_registration
                        });
                if founder.value.author_pubkey != root_value.descriptor.founder_pubkey
                    || (!recovery_author && founder_registration != &commit.author_registration)
                {
                    return Err(StorePullError::Serial(
                        "Serial genesis registration is not the Store founder".to_string(),
                    ));
                }
            }
            (
                Some(previous),
                super::store_commit::StoreCommitOrder::Serial {
                    seq,
                    predecessor: StoreSerialPredecessor::Commit(declared),
                },
            ) if declared == previous
                && *seq
                    == previous.coord.sequence().checked_add(1).ok_or_else(|| {
                        StorePullError::Serial("Serial predecessor sequence overflow".to_string())
                    })? => {}
            _ => {
                return Err(StorePullError::Serial(format!(
                    "Serial commit {} does not extend the exact accepted predecessor",
                    reference.coord.sequence()
                )));
            }
        }

        let expected_device_state = StoreDeviceStateRef::serial(
            match &commit.order {
                super::store_commit::StoreCommitOrder::Serial { predecessor, .. } => {
                    predecessor.clone()
                }
                super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                    return Err(StorePullError::Serial(
                        "Serial chain contains a Merge commit order".to_string(),
                    ));
                }
            },
            &device_state,
        )
        .map_err(|error| StorePullError::Serial(error.to_string()))?;
        if commit.device_state != expected_device_state {
            return Err(StorePullError::Serial(format!(
                "Serial commit {} names a different predecessor device state",
                reference.coord.sequence()
            )));
        }
        if author.device_id != commit.author_registration.device_id {
            return Err(StorePullError::Serial(
                "Serial commit author bytes differ from its exact registration".to_string(),
            ));
        }
        let predecessor_authority = RegistrationPredecessorAuthority::Serial {
            authorization: &authorization,
            position: match &commit.order {
                super::store_commit::StoreCommitOrder::Serial { predecessor, .. } => {
                    predecessor.clone()
                }
                super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                    return Err(StorePullError::Serial(
                        "Serial chain contains a Merge commit order".to_string(),
                    ));
                }
            },
            history: SerialAuthorizationHistory::Prefix {
                genesis_position: &genesis_position,
                genesis_authorization: genesis_authorization.as_ref(),
                commits: &authorized,
            },
        };
        let accepted_predecessor = VerifiedAcceptedPredecessor::SerialHistory {
            commits: &authorized,
        };
        let registrations = load_commit_registrations(
            storage,
            root,
            &commit,
            &author,
            Some(&predecessor_authority),
            Some(&accepted_predecessor),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
        })?;
        let acknowledgement = validate_commit_acknowledgement(storage, root, &commit, &author)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
            })?;
        let device_state_before = device_state.clone();
        let (authorized_device_state, recovery_author) =
            predecessor_with_recovery_author(device_state, &commit, &registrations)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
        if !device_state_has_active_registration(
            &authorized_device_state,
            &commit.author_registration,
        ) {
            return Err(StorePullError::Serial(format!(
                "Serial commit {} author registration is not active at its predecessor",
                reference.coord.sequence()
            )));
        }
        let device_operations = load_commit_device_operations(
            None,
            storage,
            root,
            &commit,
            &authorized_device_state,
            Some(&predecessor_authority),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
        })?;
        validate_serial_control_wrapped_keys(storage, root, commit.control()).await?;
        validate_serial_provider_admin_control(storage, root, &root_value, commit.control())
            .await?;
        let owner_recovery = verify_commit_owner_recovery_activation(
            storage,
            root,
            &commit,
            Some((&authorization, &authorized_device_state)),
        )
        .await?;
        let authorization_before = authorization.clone();
        authorization = authorization
            .authorize_and_apply(&reference, &commit, &author)
            .map_err(|error| {
                StorePullError::Serial(format!(
                    "commit {} authorization: {error}",
                    reference.coord.sequence()
                ))
            })?;
        let reduced_state = device_operations
            .apply_to(authorized_device_state, &commit.device_state)
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
        device_state = apply_verified_device_lifecycle(
            reduced_state,
            &commit,
            &registrations,
            recovery_author.as_ref(),
            owner_recovery,
        )
        .map_err(|error| StorePullError::Serial(error.to_string()))?;
        predecessor = Some(reference.clone());
        accepted_commits.insert(reference.clone());
        authorized.push(AuthorizedSerialCommit {
            commit_ref: reference,
            commit,
            author,
            registrations,
            device_operations,
            device_state_before,
            device_state_after: device_state.clone(),
            acknowledgement,
            authorization_before,
            authorization_after: authorization.clone(),
        });
    }
    Ok((authorized, authorization, device_state))
}

pub(crate) async fn validate_serial_provider_admin_control(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    control: Option<&super::store_commit::StoreControl>,
) -> Result<(), StorePullError> {
    let Some(super::store_commit::StoreControl::ProviderAdmin {
        change:
            super::provider::ProviderAdminChange::Set {
                administrator,
                provider,
                capability,
                ..
            },
    }) = control
    else {
        return Ok(());
    };
    let registration =
        super::store_objects::load_registration_ref(storage, root, administrator).await?;
    if registration.value.store_root != *root || registration.value.provider != *provider {
        return Err(StorePullError::Serial(
            "provider administrator grant does not match its exact device registration".to_string(),
        ));
    }
    capability
        .verify(&root_value.descriptor.provider, provider, true)
        .map_err(|error| StorePullError::Serial(error.to_string()))
}

pub(crate) async fn validate_serial_control_wrapped_keys(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    control: Option<&super::store_commit::StoreControl>,
) -> Result<(), StorePullError> {
    let Some(control) = control else {
        return Ok(());
    };
    let store_id = root.store_root_id.to_string();
    for reference in control.introduced_wrapped_keys() {
        let wrapped = super::wrapped_store_key::load_wrapped_store_key(
            storage,
            root.store_root_hash,
            reference,
        )
        .await?;
        wrapped
            .verify_and_unwrap(
                &store_id,
                &reference.recipient_pubkey,
                std::iter::once(reference.owner_pubkey.as_str()),
            )
            .map_err(|error| {
                StorePullError::Serial(format!(
                    "membership control wrapped Store key is not authentic: {error}"
                ))
            })?;
    }
    Ok(())
}

async fn load_authorized_serial_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    head: &StoreSerialHead,
) -> Result<Vec<AuthorizedSerialCommit>, StorePullError> {
    let founder = load_founder_registration(storage, root).await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let tip = match &head.state {
        StoreSerialHeadState::Genesis {
            root: head_root,
            founder_registration,
        } => {
            if head_root != root || founder_registration != &founder_ref {
                return Err(StorePullError::Serial(
                    "Serial genesis head does not name the exact Store founder".to_string(),
                ));
            }
            None
        }
        StoreSerialHeadState::Commit { commit, .. } => Some(commit.clone()),
    };
    let (authorized, _, _) =
        Box::pin(load_authorized_serial_prefix(storage, root, tip.clone())).await?;
    match (&head.state, authorized.last()) {
        (StoreSerialHeadState::Genesis { .. }, None) => {}
        (
            StoreSerialHeadState::Commit {
                author_registration,
                commit,
            },
            Some(accepted),
        ) if commit == &accepted.commit_ref
            && author_registration == &accepted.commit.author_registration => {}
        _ => {
            return Err(StorePullError::Serial(
                "signed global head is not bound to its exact tip commit".to_string(),
            ));
        }
    }
    Ok(authorized)
}

pub(crate) enum SerialSuccessorObservation {
    Unchanged(super::storage::VersionedObject),
    Advanced(VerifiedSerialAcceptedSuffix),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedSerialAcceptedSuffix {
    store_root_hash: ObjectHash,
    durable: super::remote_object::SerialAcceptedSuffix,
}

impl VerifiedSerialAcceptedSuffix {
    pub(crate) fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub(crate) fn durable(&self) -> &super::remote_object::SerialAcceptedSuffix {
        &self.durable
    }

    pub(crate) fn commits(&self) -> &[StoreBatchCommitRef] {
        &self.durable.commits
    }
}

pub(crate) async fn observe_serial_successors_after(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    predecessor: &super::store_commit::StoreSerialPredecessor,
) -> Result<SerialSuccessorObservation, StorePullError> {
    let verified_head = read_serial_head(storage, coordination, root).await?;
    let authorized = load_authorized_serial_chain(storage, root, &verified_head.head).await?;
    let first = match predecessor {
        super::store_commit::StoreSerialPredecessor::Genesis {
            root: expected_root,
            founder_registration,
        } => {
            let actual = authorized.first().map_or_else(
                || match &verified_head.head.state {
                    StoreSerialHeadState::Genesis {
                        root,
                        founder_registration,
                    } => super::store_commit::StoreSerialPredecessor::Genesis {
                        root: root.clone(),
                        founder_registration: founder_registration.clone(),
                    },
                    StoreSerialHeadState::Commit { .. } => {
                        unreachable!("a commit head has an authorized tip")
                    }
                },
                |first| match &first.commit.order {
                    super::store_commit::StoreCommitOrder::Serial { predecessor, .. } => {
                        predecessor.clone()
                    }
                    super::store_commit::StoreCommitOrder::MergeConcurrent { .. } => {
                        unreachable!("authorized Serial chain contains only Serial commits")
                    }
                },
            );
            let expected = super::store_commit::StoreSerialPredecessor::Genesis {
                root: expected_root.clone(),
                founder_registration: founder_registration.clone(),
            };
            if actual != expected {
                return Err(StorePullError::Serial(
                    "global chain does not descend from the exact Serial genesis".to_string(),
                ));
            }
            0
        }
        super::store_commit::StoreSerialPredecessor::Commit(base) => authorized
            .iter()
            .position(|accepted| &accepted.commit_ref == base)
            .map(|index| index + 1)
            .ok_or_else(|| {
                StorePullError::Serial(
                    "global chain does not descend from the exact Serial predecessor".to_string(),
                )
            })?,
    };
    let commits = authorized[first..]
        .iter()
        .map(|accepted| accepted.commit_ref.clone())
        .collect::<Vec<_>>();
    if commits.is_empty() {
        return Ok(SerialSuccessorObservation::Unchanged(verified_head.object));
    }
    Ok(SerialSuccessorObservation::Advanced(
        VerifiedSerialAcceptedSuffix {
            store_root_hash: root.store_root_hash,
            durable: super::remote_object::SerialAcceptedSuffix {
                predecessor: match predecessor {
                    super::store_commit::StoreSerialPredecessor::Genesis { .. } => None,
                    super::store_commit::StoreSerialPredecessor::Commit(base) => Some(base.clone()),
                },
                commits,
                canonical_signed_head_bytes: verified_head.object.bytes,
                observed_version_hash: super::store_commit::ObjectHash::digest(
                    verified_head
                        .object
                        .version
                        .cloud()
                        .as_provider()
                        .as_bytes(),
                ),
            },
        },
    ))
}

struct VerifiedSerialHead {
    head: StoreSerialHead,
    object: super::storage::VersionedObject,
}

async fn read_serial_head(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
) -> Result<VerifiedSerialHead, StorePullError> {
    let object = match coordination.read_head(serial_head_key()).await {
        Ok(object) => object,
        Err(CoordinationError::NotFound(_)) => {
            return Err(StorePullError::Serial("global head is absent".to_string()));
        }
        Err(error) => return Err(StorePullError::Coordination(error)),
    };
    let unverified: StoreSerialHead = serde_json::from_slice(&object.bytes)
        .map_err(|error| StorePullError::Serial(format!("invalid head: {error}")))?;
    let executor_ref = match &unverified.state {
        StoreSerialHeadState::Genesis {
            founder_registration,
            ..
        } => founder_registration,
        StoreSerialHeadState::Commit {
            author_registration,
            ..
        } => author_registration,
    };
    let executor = load_registration_ref(storage, root, executor_ref)
        .await?
        .value;
    let head = StoreSerialHead::parse(&object.bytes, root.store_root_hash, &executor)
        .map_err(|error| StorePullError::Serial(format!("invalid head: {error}")))?;
    Ok(VerifiedSerialHead { head, object })
}

pub(crate) async fn load_serial_authorization_at_head(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    head: &StoreSerialHead,
) -> Result<SerialAuthorizationState, StorePullError> {
    let authorized = load_authorized_serial_chain(storage, root, head).await?;
    match authorized.last() {
        Some(tip) => Ok(tip.authorization_after.clone()),
        None => load_serial_authorization_at_position(storage, root, None).await,
    }
}

pub(crate) struct SerialCycleAuthorization {
    pub authorization: SerialAuthorizationState,
    pub head: Option<StoreBatchCommitRef>,
}

pub(crate) async fn load_serial_cycle_authorization(
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
) -> Result<SerialCycleAuthorization, StorePullError> {
    let head = read_serial_head(storage, coordination, root).await?.head;
    let authorized = load_authorized_serial_chain(storage, root, &head).await?;
    let authorization = match authorized.last() {
        Some(tip) => tip.authorization_after.clone(),
        None => load_serial_authorization_at_position(storage, root, None).await?,
    };
    let head = match head.state {
        StoreSerialHeadState::Genesis { .. } => None,
        StoreSerialHeadState::Commit { commit, .. } => Some(commit),
    };
    Ok(SerialCycleAuthorization {
        authorization,
        head,
    })
}

pub async fn load_serial_authorization_at_position(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: Option<StoreBatchCommitRef>,
) -> Result<SerialAuthorizationState, StorePullError> {
    let (_, authorization, _) =
        Box::pin(load_authorized_serial_prefix(storage, root, reference)).await?;
    Ok(authorization)
}

pub(crate) async fn load_serial_snapshot_authorities_at_position(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: Option<StoreBatchCommitRef>,
) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
    let (authorized, authorization, _) =
        Box::pin(load_authorized_serial_prefix(storage, root, reference)).await?;
    let founder = load_founder_registration(storage, root).await?;
    let founder_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let mut active = BTreeMap::from([(founder_ref, founder.value)]);
    for accepted in authorized {
        for (activated, (registration, _)) in accepted
            .commit
            .device_registrations()
            .iter()
            .zip(accepted.registrations)
        {
            active.insert(activated.registration.clone(), registration);
        }
        for retirement in accepted.commit.device_retirements() {
            active.remove(&retirement.target);
        }
    }
    Ok(active
        .into_iter()
        .filter(|(_, registration)| {
            authorization
                .membership
                .is_owner(&registration.author_pubkey)
        })
        .collect())
}

async fn pull_serial_store_commits(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    root: &StoreRootRef,
    root_value: super::store_commit::StoreProtocolRoot,
    store_dir: &StoreDir,
    identity: Option<&crate::keys::UserKeypair>,
) -> Result<StorePullResult, StorePullError> {
    if root_value.descriptor.write_policy != crate::WritePolicy::Serial {
        return Err(StorePullError::Serial(
            "signed Store root is not Serial".to_string(),
        ));
    }
    let local = db.materialized_frontier().await?.remove(SERIAL_STREAM_ID);
    let head = read_serial_head(storage, coordination, root).await?.head;
    let authorized_chain = Box::pin(load_authorized_serial_chain(storage, root, &head)).await?;
    let tip = match &head.state {
        StoreSerialHeadState::Genesis { .. } => None,
        StoreSerialHeadState::Commit { commit, .. } => Some(commit.clone()),
    };
    let Some(tip) = tip else {
        if local.is_some() {
            return Err(StorePullError::Serial(format!(
                "global head is genesis but the durable Serial frontier is {local:?}"
            )));
        }
        return empty_serial_pull_result(db, store_dir, Some(head)).await;
    };
    if local
        .as_ref()
        .is_some_and(|local| local.coord.sequence() > tip.coord.sequence())
    {
        return Err(StorePullError::Serial(format!(
            "local Serial reference is ahead of the signed head: local={local:?}, head={tip:?}"
        )));
    }

    let first_unmaterialized = match local.as_ref() {
        None => 0,
        Some(local) => authorized_chain
            .iter()
            .position(|authorized| &authorized.commit_ref == local)
            .map(|index| index + 1)
            .ok_or_else(|| {
                StorePullError::Serial(format!(
                    "exact Serial predecessor chain does not reach local reference {local:?}"
                ))
            })?,
    };
    if let Some(local) = local.as_ref() {
        let authorization = authorized_chain
            .get(first_unmaterialized - 1)
            .expect("materialized Serial reference was found in the authorized chain")
            .authorization_after
            .clone();
        db.install_serial_authorization_at_position(local.clone(), authorization)
            .await?;
    }

    let mut candidates = Vec::with_capacity(authorized_chain.len() - first_unmaterialized);
    for authorized in authorized_chain.into_iter().skip(first_unmaterialized) {
        let package =
            load_serial_store_package(db, storage, &authorized.commit_ref, &authorized.commit)
                .await?;
        candidates.push(SerialApplicationCandidate {
            candidate: Candidate {
                commit_ref: authorized.commit_ref,
                commit: authorized.commit,
                author: authorized.author,
                package,
                registrations: authorized.registrations,
                device_operations: CandidateDeviceOperations::Verified(
                    authorized.device_operations,
                ),
            },
            membership_authority: authorized.authorization_before,
            authorization_after: authorized.authorization_after,
        });
    }

    let schema: Arc<TableSchema> = {
        let tables = tables.to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut row_changes = Vec::new();
    let mut authors = BTreeSet::new();
    let mut applied_candidates = 0_u64;
    for candidate in &candidates {
        let changes = match Box::pin(apply_serial_candidate(
            db,
            storage,
            store_dir,
            schema.clone(),
            candidate,
            root,
            identity,
        ))
        .await
        {
            Ok(changes) => changes,
            Err(StorePullError::BlobDownloads(failures)) if !failures.has_transport_failure() => {
                tracing::warn!(
                    stream_id = %commit_stream_id(&candidate.candidate.commit_ref.coord),
                    seq = candidate.candidate.commit_ref.coord.sequence(),
                    %failures,
                    "holding Serial commit on blob download failure"
                );
                let frontier = db.materialized_frontier().await?;
                let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
                return Ok(StorePullResult {
                    changesets_applied: applied_candidates,
                    devices_pulled: u64::try_from(authors.len()).map_err(|_| {
                        StorePullError::Serial("author count exceeds u64".to_string())
                    })?,
                    held_positions: vec![held_commit(
                        &candidate.candidate.commit_ref,
                        HeldStorePositionReason::BlobDownloadFailed,
                    )],
                    visible_heads: Vec::new(),
                    serial_head: Some(head),
                    row_changes,
                    asset_downloads_failed: true,
                    local_blob_cleanup_pending,
                    frontier,
                });
            }
            Err(error) => return Err(error),
        };
        authors.insert(candidate.candidate.author.device_id);
        row_changes.extend(changes);
        applied_candidates = applied_candidates
            .checked_add(1)
            .ok_or_else(|| StorePullError::Serial("apply count exceeds u64".to_string()))?;
    }
    let frontier = db.materialized_frontier().await?;
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
    Ok(StorePullResult {
        changesets_applied: applied_candidates,
        devices_pulled: u64::try_from(authors.len())
            .map_err(|_| StorePullError::Serial("author count exceeds u64".to_string()))?,
        held_positions: Vec::new(),
        visible_heads: Vec::new(),
        serial_head: Some(head),
        row_changes,
        asset_downloads_failed: false,
        local_blob_cleanup_pending,
        frontier,
    })
}

async fn empty_serial_pull_result(
    db: &Database,
    store_dir: &StoreDir,
    serial_head: Option<StoreSerialHead>,
) -> Result<StorePullResult, StorePullError> {
    let frontier = db.materialized_frontier().await?;
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;
    Ok(StorePullResult {
        changesets_applied: 0,
        devices_pulled: 0,
        held_positions: Vec::new(),
        visible_heads: Vec::new(),
        serial_head,
        row_changes: Vec::new(),
        asset_downloads_failed: false,
        local_blob_cleanup_pending,
        frontier,
    })
}

#[doc(hidden)]
pub async fn prepare_serial_resolution(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    branch_base: Option<StoreBatchCommitRef>,
    identity: &crate::keys::UserKeypair,
) -> Result<SerialResolutionPlan, StorePullError> {
    let root = db.local_store_root_ref().await?.ok_or_else(|| {
        StorePullError::Serial("Store root exact reference is absent".to_string())
    })?;
    if root.store_root_hash != store_root_hash {
        return Err(StorePullError::Serial(
            "Serial resolution root differs from durable exact root".to_string(),
        ));
    }
    let verified_head = read_serial_head(storage, coordination, &root).await?;
    let head = verified_head.head;
    let authorized_chain = load_authorized_serial_chain(storage, &root, &head).await?;
    let first = match branch_base.as_ref() {
        None => 0,
        Some(base) => authorized_chain
            .iter()
            .position(|authorized| &authorized.commit_ref == base)
            .map(|index| index + 1)
            .ok_or_else(|| {
                StorePullError::Serial(
                    "global chain does not descend from the exact conflicting branch base"
                        .to_string(),
                )
            })?,
    };
    let schema: Arc<TableSchema> = {
        let tables = db.synced_tables().to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut commits = Vec::with_capacity(authorized_chain.len() - first);
    let mut prior_circle_accesses = CirclePackageAccesses::new();
    let mut verified_prefix = VerifiedStreamActivationPrefix::empty();
    for authorized in authorized_chain.into_iter().skip(first) {
        let package =
            load_serial_store_package(db, storage, &authorized.commit_ref, &authorized.commit)
                .await?;
        let verified_circle_activations = match load_pull_circle_activations(
            db,
            storage,
            &root,
            &authorized.commit_ref,
            &authorized.commit,
            &authorized.author,
            Some(identity),
            &CircleMembershipAuthority::Serial(authorized.authorization_before.clone()),
            &verified_prefix,
        )
        .await
        {
            Ok(activations) => activations,
            Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
            Err(PullCircleActivationError::Invalid(error)) => {
                return Err(StorePullError::Serial(error));
            }
        };
        let candidate = Candidate {
            commit_ref: authorized.commit_ref.clone(),
            commit: authorized.commit,
            author: authorized.author,
            package,
            registrations: authorized.registrations,
            device_operations: CandidateDeviceOperations::Verified(authorized.device_operations),
        };
        let prepared = prepare_serial_candidate(
            db,
            storage,
            store_dir,
            schema.clone(),
            &candidate,
            verified_circle_activations.circles(),
            &prior_circle_accesses,
        )
        .await?;
        for (key, access) in circle_package_accesses(verified_circle_activations.circles())
            .map_err(StorePullError::Serial)?
        {
            if prior_circle_accesses.insert(key, access).is_some() {
                return Err(StorePullError::Serial(
                    "Serial resolution repeats one exact Circle control".to_string(),
                ));
            }
        }
        let device_operations = match candidate.device_operations {
            CandidateDeviceOperations::Verified(operations) => operations,
            CandidateDeviceOperations::MergePending { .. } => {
                return Err(StorePullError::Serial(
                    "Serial resolution contains unresolved Merge device operations".to_string(),
                ))
            }
        };
        verified_prefix
            .extend(verified_circle_activations.stream_activations())
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
        commits.push(SerialResolutionCommit {
            commit: candidate.commit,
            commit_ref: candidate.commit_ref,
            packages: prepared.packages,
            changesets: prepared.changesets,
            registrations: candidate.registrations,
            verified_circle_activations,
            device_operations,
            authorization_after: authorized.authorization_after,
        });
    }
    let accepted_refs = commits
        .iter()
        .map(|commit| commit.commit_ref.clone())
        .collect::<Vec<_>>();
    let verified_suffix = (!accepted_refs.is_empty()).then(|| VerifiedSerialAcceptedSuffix {
        store_root_hash: root.store_root_hash,
        durable: super::remote_object::SerialAcceptedSuffix {
            predecessor: branch_base,
            commits: accepted_refs,
            canonical_signed_head_bytes: verified_head.object.bytes.clone(),
            observed_version_hash: ObjectHash::digest(
                verified_head
                    .object
                    .version
                    .cloud()
                    .as_provider()
                    .as_bytes(),
            ),
        },
    });
    Ok(SerialResolutionPlan {
        head,
        head_object: verified_head.object,
        commits,
        verified_suffix,
    })
}

#[doc(hidden)]
pub async fn cleanup_serial_candidates(
    db: &Database,
    storage: &dyn SyncStorage,
    branch_id: crate::PendingBranchId,
    plan: &SerialResolutionPlan,
) -> Result<(), StorePullError> {
    let targets = db.prepare_serial_candidate_cleanup(branch_id, plan).await?;
    for target in targets {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    Ok(())
}

pub async fn cleanup_serial_abandonment_authority(
    db: &Database,
    storage: &dyn SyncStorage,
    plan: &SerialResolutionPlan,
) -> Result<(), StorePullError> {
    let target = db
        .prepare_serial_abandonment_authority_cleanup(plan)
        .await?;
    if let Some(target) = target {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    Ok(())
}

pub async fn cleanup_merge_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    write_id: crate::WriteId,
) -> Result<(), StorePullError> {
    let root = db.local_store_root_ref().await?.ok_or_else(|| {
        StorePullError::Database("Merge candidate cleanup has no Store root".to_string())
    })?;
    for verification in db
        .merge_candidate_terminal_verifications(write_id.clone())
        .await?
    {
        let nonactivation =
            verify_terminal_cleanup_candidate(db, storage, &root, &verification).await?;
        db.reconcile_merge_candidate_terminal_head(write_id.clone(), nonactivation)
            .await?;
    }
    let targets = db.merge_candidate_cleanup_targets(write_id).await?;
    for target in targets {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    Ok(())
}

async fn verify_terminal_cleanup_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    verification: &crate::database::TerminalCandidateCleanupVerification,
) -> Result<super::remote_object::VerifiedCandidateNonactivation, StorePullError> {
    let reference = &verification.candidate.head.value.commit;
    let target = super::store_commit::StoreBatchCommitDeletionTarget {
        coord: reference.coord.clone(),
        object: verification.candidate.commit.object.clone(),
        canonical_signed_bytes: verification.candidate.commit.bytes.clone(),
    };
    match &verification.authority {
        crate::database::TerminalCandidateAuthority::AuthorExclusion(locator) => {
            let activation = Box::pin(verify_author_exclusion_activation(
                db,
                storage,
                root,
                locator,
                reference,
                &verification.candidate.commit.value,
                &verification.candidate.head.value,
                &verification.candidate.head.object,
            ))
            .await?;
            super::remote_object::VerifiedCandidateNonactivation::author_exclusion(
                &activation,
                target,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))
        }
        crate::database::TerminalCandidateAuthority::MembershipGrantRevocation {
            grant_id,
            membership,
            activation_commit,
            activation_head,
        } => {
            let activation = Box::pin(verify_membership_grant_revocation_activation(
                storage,
                root,
                grant_id,
                membership,
                activation_commit,
                activation_head,
                reference,
                &verification.candidate.commit.value,
                &verification.candidate.head.value,
                &verification.candidate.head.object,
            ))
            .await?;
            super::remote_object::VerifiedCandidateNonactivation::membership_grant_revocation(
                &activation,
                target,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))
        }
        crate::database::TerminalCandidateAuthority::DependencyRetraction(authority) => {
            let author = load_registration_ref(
                storage,
                root,
                &verification.candidate.commit.value.author_registration,
            )
            .await
            .map_err(StorePullError::Object)?
            .value;
            super::remote_object::VerifiedCandidateNonactivation::from_verified_dependency_retraction_authority(
                authority.clone(),
                target,
                &author,
                verification.candidate.head.object.clone(),
            )
            .map_err(|error| StorePullError::Database(error.to_string()))
        }
    }
}

async fn resume_merge_retraction_cleanups(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<(), StorePullError> {
    for candidate in db.pending_merge_retraction_cleanups().await? {
        let verification = db
            .merge_retraction_cleanup_verification(candidate.clone())
            .await?;
        let verified = verify_terminal_cleanup_candidate(db, storage, root, &verification).await?;
        db.confirm_merge_retraction_cleanup_nonactivation(candidate.clone(), verified)
            .await?;
        for target in db
            .merge_retraction_cleanup_targets(candidate.clone())
            .await?
        {
            super::store_objects::delete_exact_object(storage, &target.object).await?;
            db.mark_candidate_cleanup_absent(target.object).await?;
        }
        db.finish_merge_retraction_cleanup(candidate).await?;
    }
    Ok(())
}

async fn apply_serial_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    application: &SerialApplicationCandidate,
    root: &StoreRootRef,
    identity: Option<&crate::keys::UserKeypair>,
) -> Result<Vec<RowChange>, StorePullError> {
    let candidate = &application.candidate;
    let device_operations = match &candidate.device_operations {
        CandidateDeviceOperations::Verified(operations) => operations.clone(),
        CandidateDeviceOperations::MergePending { .. } => {
            return Err(StorePullError::Serial(
                "Serial candidate carries unresolved Merge device operations".to_string(),
            ))
        }
    };
    let verified_prefix = VerifiedStreamActivationPrefix::empty();
    let verified_circle_activations = match Box::pin(load_pull_circle_activations(
        db,
        storage,
        root,
        &candidate.commit_ref,
        &candidate.commit,
        &candidate.author,
        identity,
        &CircleMembershipAuthority::Serial(application.membership_authority.clone()),
        &verified_prefix,
    ))
    .await
    {
        Ok(activations) => activations,
        Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
        Err(PullCircleActivationError::Invalid(error)) => {
            return Err(StorePullError::Serial(error));
        }
    };
    let no_prior_circle_accesses = CirclePackageAccesses::new();
    let prepared = prepare_serial_candidate(
        db,
        storage,
        store_dir,
        schema.clone(),
        candidate,
        verified_circle_activations.circles(),
        &no_prior_circle_accesses,
    )
    .await?;
    let resolution = SerialResolutionCommit {
        commit: candidate.commit.clone(),
        commit_ref: candidate.commit_ref.clone(),
        packages: prepared.packages,
        changesets: prepared.changesets,
        registrations: candidate.registrations.clone(),
        verified_circle_activations,
        device_operations,
        authorization_after: application.authorization_after.clone(),
    };
    let blob_decls = db.blob_decls();
    let gates = db.gates();
    let synced_tables = db.synced_tables().to_vec();
    let apply_schema = schema.clone();
    let returned_changes = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let changes = apply_prepared_serial_commit_on(
                &tx,
                apply_schema,
                &gates,
                &synced_tables,
                &blob_decls,
                &resolution,
            )?;
            tx.commit().map_err(DbError::from)?;
            Ok(changes)
        })
        .await?;
    let mut changeset_max = None;
    advance_max_updated_at(
        &mut changeset_max,
        &returned_changes,
        &schema,
        db.receive_wall_ms(),
    );
    if let Some(max_applied) = changeset_max.as_ref() {
        db.hlc().advance_past(max_applied);
    }
    Ok(returned_changes)
}

pub(crate) fn apply_prepared_serial_commit_on(
    conn: &rusqlite::Connection,
    schema: Arc<TableSchema>,
    gates: &super::gate::Gates,
    synced_tables: &[SyncedTable],
    blob_decls: &BlobDecls,
    resolution: &SerialResolutionCommit,
) -> Result<Vec<RowChange>, DbError> {
    let deletes = ValidatedChangeset::new(resolution.changesets.deletes.as_slice(), schema.clone())
        .map_err(|error| DbError::Message(format!("invalid Serial deletes: {error}")))?;
    let writes = ValidatedChangeset::new(resolution.changesets.writes.as_slice(), schema.clone())
        .map_err(|error| DbError::Message(format!("invalid Serial writes: {error}")))?;
    let mut materialization_session =
        rusqlite::session::Session::new(conn).map_err(DbError::from)?;
    for table in synced_tables {
        materialization_session
            .attach(Some(table.name()))
            .map_err(DbError::from)?;
    }
    for package in &resolution.packages {
        super::gate::validate_serial_visibility_deletes(
            conn,
            gates,
            package.changeset(),
            &package_audience(package.audience()),
        )
        .map_err(|error| {
            DbError::Message(format!("validate Serial visibility removal: {error}"))
        })?;
    }
    apply_serial_visibility_deletes_on(conn, deletes).map_err(|error| {
        DbError::Message(format!(
            "apply Serial commit {} visibility removals: {error}",
            resolution.commit_ref.coord.sequence()
        ))
    })?;
    if !writes.bytes().is_empty() {
        apply_changeset_strict_on(conn, writes).map_err(|error| {
            DbError::Message(format!(
                "Serial commit {} did not apply exactly: {error}",
                resolution.commit_ref.coord.sequence()
            ))
        })?;
    }
    Database::record_activated_store_device_registrations_on(
        conn,
        &resolution.commit,
        &resolution.registrations,
    )
    .map_err(|error| DbError::Message(format!("record Serial registrations: {error}")))?;
    Database::record_verified_circle_activations_on(
        conn,
        &resolution.commit,
        &resolution.commit_ref,
        resolution.verified_circle_activations.circles(),
    )
    .map_err(|error| DbError::Message(format!("record Serial Circle controls: {error}")))?;
    for package in &resolution.packages {
        let expected_audience = package_audience(package.audience());
        let winning_rows = crate::sync::apply::current_winning_rows_with_schema(
            conn,
            &schema,
            package.changeset(),
        )?;
        for winner in winning_rows
            .iter()
            .filter(|winner| winner.row_stamp.is_some())
        {
            let live = super::gate::live_row_audience(conn, gates, &winner.table, &winner.row_id)
                .map_err(|error| {
                DbError::Message(format!(
                    "resolve Serial package row audience for {}.{}: {error}",
                    winner.table, winner.row_id
                ))
            })?;
            if live != expected_audience {
                return Err(DbError::Message(format!(
                    "Serial {:?} package cannot write {}.{} into {:?}",
                    expected_audience, winner.table, winner.row_id, live
                )));
            }
        }
    }
    let inactive_circles = resolution
        .verified_circle_activations
        .circles()
        .iter()
        .filter_map(|activation| {
            activation
                .local_access
                .as_ref()
                .filter(|access| access.active.is_none())
                .map(|_| activation.circle_id)
        })
        .collect::<BTreeSet<_>>();
    super::gate::prune_inactive_serial_circles(conn, gates, &inactive_circles)
        .map_err(|error| DbError::Message(format!("prune inactive Serial Circles: {error}")))?;
    let mut materialized_changeset = Vec::new();
    materialization_session
        .changeset_strm(&mut materialized_changeset)
        .map_err(DbError::from)?;
    drop(materialization_session);
    let old_changes =
        crate::changeset::walk_old(&materialized_changeset).map_err(DbError::Message)?;
    let changes = crate::changeset::walk(&materialized_changeset).map_err(DbError::Message)?;
    for intent in local_blob_cleanup_intents(blob_decls, &old_changes, &changes)
        .map_err(|error| DbError::Message(error.to_string()))?
    {
        local_cleanup::record_obsolete_copy_intents_on(conn, blob_decls, &intent)?;
    }
    for package in &resolution.packages {
        let winning_rows = crate::sync::apply::current_winning_rows_with_schema(
            conn,
            &schema,
            package.changeset(),
        )?;
        Database::install_pulled_package_activation_on(
            conn,
            &resolution.commit,
            &resolution.commit_ref,
            package,
        )
        .map_err(|error| DbError::Message(format!("record Serial package activation: {error}")))?;
        Database::install_pulled_blob_activations_on(conn, package, &resolution.commit_ref)
            .map_err(|error| {
                DbError::Message(format!("record Serial blob activations: {error}"))
            })?;
        Database::install_winning_blob_bindings_on(
            conn,
            gates,
            synced_tables,
            package,
            &BlobActivation {
                coord: resolution.commit_ref.coord.clone(),
            },
            &winning_rows,
        )
        .map_err(|error| DbError::Message(format!("record Serial blob bindings: {error}")))?;
    }
    Database::record_materialized_serial_commit_with_device_operations_on(
        conn,
        &resolution.commit,
        &resolution.commit_ref,
        &resolution.authorization_after,
        &resolution.device_operations,
        resolution.verified_circle_activations.stream_activations(),
    )
    .map_err(|error| DbError::Message(format!("record Serial commit position: {error}")))?;
    Ok(changes)
}

fn package_audience(audience: &PackageAudience) -> super::circle::Audience {
    match audience {
        PackageAudience::Store => super::circle::Audience::Store,
        PackageAudience::Circle { circle_id, .. } => super::circle::Audience::Circle(*circle_id),
    }
}

fn apply_serial_visibility_deletes_on<B: AsRef<[u8]>>(
    conn: &rusqlite::Connection,
    changeset: ValidatedChangeset<B>,
) -> Result<(), DbError> {
    if changeset.bytes().is_empty() {
        return Ok(());
    }
    if crate::changeset::walk(changeset.bytes())
        .map_err(DbError::Message)?
        .iter()
        .any(|change| change.op != crate::changeset::ChangeOp::Delete)
    {
        return Err(DbError::Message(
            "Serial visibility removal contains a non-delete operation".to_string(),
        ));
    }
    let bytes = changeset.bytes();
    conn.apply_strm(
        &mut &bytes[..],
        None::<fn(&str) -> bool>,
        |conflict, _item| match conflict {
            ConflictType::SQLITE_CHANGESET_DATA => ConflictAction::SQLITE_CHANGESET_REPLACE,
            ConflictType::SQLITE_CHANGESET_NOTFOUND => ConflictAction::SQLITE_CHANGESET_OMIT,
            _ => ConflictAction::SQLITE_CHANGESET_ABORT,
        },
    )
    .map_err(DbError::from)
}

struct PreparedSerialCandidate {
    packages: Vec<AudiencePackage>,
    changesets: super::gate::SerialInboundChangesets,
}

async fn prepare_serial_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    candidate: &Candidate,
    circle_activations: &[super::circle_ops::VerifiedCircleReference],
    prior_circle_accesses: &CirclePackageAccesses,
) -> Result<PreparedSerialCandidate, StorePullError> {
    let mut packages = Vec::<(AudiencePackage, BlobSpoolProtection)>::new();
    if let Some(package_bytes) = candidate.package.as_ref() {
        let package = parse_candidate_store_package(candidate, package_bytes)
            .map_err(StorePullError::Serial)?;
        packages.push((package, storage.store_blob_protection()?));
    }
    let circle_packages = load_applicable_circle_packages_with_prior_accesses(
        db,
        storage,
        &candidate.commit_ref,
        &candidate.commit,
        circle_activations,
        &candidate.author,
        prior_circle_accesses,
    )
    .await
    .map_err(|error| match error {
        PullCircleActivationError::Database(error) => StorePullError::Database(error.to_string()),
        PullCircleActivationError::Invalid(error) => StorePullError::Serial(error),
    })?;
    for loaded in circle_packages {
        let package =
            parse_candidate_circle_package(candidate, &loaded).map_err(StorePullError::Serial)?;
        packages.push((package, loaded.blob_protection));
    }

    let blob_decls = db.blob_decls();
    for (package, protection) in &packages {
        let validated = ValidatedChangeset::new(package.changeset(), schema.clone())
            .map_err(|error| StorePullError::Serial(format!("invalid changeset: {error}")))?;
        let changes = crate::changeset::walk(validated.bytes())
            .map_err(|error| StorePullError::Serial(format!("invalid changeset: {error}")))?;
        let eager = cache_eager_blobs(&blob_decls, &changes, package)
            .map_err(|error| StorePullError::Serial(format!("invalid blob changes: {error}")))?;
        verify_package_blobs(
            db,
            storage,
            store_dir,
            package.blob_bindings(),
            protection.clone(),
            &eager,
        )
        .await
        .map_err(StorePullError::BlobDownloads)?;
    }
    let package_changesets = packages
        .iter()
        .map(|(package, _)| package.changeset().to_vec())
        .collect::<Vec<_>>();
    let changesets = db
        .call(move |conn| {
            let changesets = package_changesets
                .iter()
                .map(Vec::as_slice)
                .collect::<Vec<_>>();
            super::gate::combine_serial_inbound_changesets(conn, &changesets)
                .map_err(|error| DbError::Message(error.to_string()))
        })
        .await?;
    ValidatedChangeset::new(changesets.deletes.as_slice(), schema.clone())
        .map_err(|error| StorePullError::Serial(format!("invalid Serial deletes: {error}")))?;
    ValidatedChangeset::new(changesets.writes.as_slice(), schema)
        .map_err(|error| StorePullError::Serial(format!("invalid Serial writes: {error}")))?;
    Ok(PreparedSerialCandidate {
        packages: packages.into_iter().map(|(package, _)| package).collect(),
        changesets,
    })
}
fn membership_authorizes(
    membership: Option<&MembershipChain>,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> bool {
    if commit.operations().is_none() {
        return true;
    }
    let Some(chain) = membership else {
        return false;
    };
    commit
        .membership_authority
        .as_ref()
        .is_some_and(|authority| chain.authorizes_write_authority(authority, &author.author_pubkey))
}

fn carries_circle_payload(commit: &StoreBatchCommit) -> bool {
    !commit.circle_controls().is_empty()
        || !commit.circle_packages().is_empty()
        || !commit.stream_activations().is_empty()
}

pub(crate) async fn verify_merge_membership_control(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<VerifiedCircleActivations, String> {
    let history = verify_merge_history_refs(storage, root, commit_predecessor_references(commit))
        .await
        .map_err(|error| error.to_string())?;
    let states = history
        .commits
        .iter()
        .map(|(reference, verified)| (reference.clone(), verified.state_after.clone()))
        .collect::<BTreeMap<_, _>>();
    let predecessor_state = verified_merge_predecessor_state(&history.genesis, &states, commit)
        .map_err(|error| error.to_string())?;
    let verified_membership_activations =
        verified_merge_membership_prefix(&history.commits, commit_predecessor_references(commit))
            .map_err(|error| error.to_string())?;
    let pending_resolution = verify_merge_resolution_activation_acceptance_with_history(
        storage,
        root,
        commit,
        &history.genesis,
        &history.commits,
    )
    .await
    .map_err(|error| error.to_string())?;
    let predecessor_membership = load_merge_predecessor_membership_with_verified_activations(
        storage,
        root,
        &commit.membership_state,
        &verified_membership_activations,
        pending_resolution.as_ref(),
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => error.to_string(),
        RegistrationLoadError::Invalid(error) => error,
    })?;
    verify_merge_membership_state_ref(
        &commit.membership_state,
        &predecessor_membership,
        &predecessor_state,
    )
    .map_err(|error| error.to_string())?;
    verify_merge_membership_control_with_history(
        storage,
        root,
        commit_ref,
        commit,
        &predecessor_membership,
        &predecessor_state,
        &history.commits,
        pending_resolution.as_ref(),
    )
    .await
    .map(|(activations, _)| activations)
}

async fn verify_merge_membership_control_with_history(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    predecessor_membership: &MembershipChain,
    predecessor_state: &ResolvedStoreDeviceState,
    verified_commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<
    (
        VerifiedCircleActivations,
        Option<VerifiedMergeConflictResolutionActivation>,
    ),
    String,
> {
    let Some(super::store_commit::StoreControl::MergeMembership { transition }) = commit.control()
    else {
        return Err("Merge membership verifier received another Store control".to_string());
    };
    let StoreMembershipStateRef::MergeConcurrent(state) = &commit.membership_state else {
        return Err("Merge membership control carries Serial membership state".to_string());
    };
    let commit_author =
        super::store_objects::load_registration_ref(storage, root, &commit.author_registration)
            .await
            .map_err(|error| error.to_string())?;
    if transition.body.author_registration != commit.author_registration
        || transition.body.entry.coord.author_pubkey != commit_author.value.author_pubkey
        || transition.body.resolutions != state.resolutions
        || transition.body.successor.predecessor
            != transition
                .body
                .predecessor
                .as_ref()
                .map(|reference| reference.object.clone())
    {
        return Err("Merge membership transition differs from its Store authority".to_string());
    }
    match &transition.body.predecessor {
        Some(predecessor) if state.heads.binary_search(predecessor).is_err() => {
            return Err(
                "Merge membership transition predecessor is absent from its signed state"
                    .to_string(),
            )
        }
        None if state
            .heads
            .iter()
            .any(|head| head.coord.stream_key() == transition.body.entry.coord.stream_key()) =>
        {
            return Err(
                "first Merge membership transition has an existing signed predecessor".to_string(),
            )
        }
        _ => {}
    }
    let opened_entry = super::store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &transition.body.entry,
    )
    .await
    .map_err(|error| error.to_string())?;
    if opened_entry.value.coord() != transition.body.entry.coord
        || opened_entry.value.dependencies != predecessor_membership.effective_frontier()
        || opened_entry.value.resolution_dependencies != transition.body.resolutions
    {
        return Err("Merge membership transition differs from its exact entry".to_string());
    }
    if let super::membership::MembershipChange::RemoveMember {
        user_pubkey,
        removes,
        retirement_device_state,
        ..
    } = &opened_entry.value.change
    {
        let removes_exact_member = removes == &predecessor_membership.active_grant_ids(user_pubkey);
        let retires_owner = removes.iter().any(|grant| {
            predecessor_membership
                .active_grant(grant)
                .is_some_and(|record| {
                    matches!(
                        record.role,
                        super::membership::StoreMembershipRoleGrant::Owner { .. }
                    )
                })
        });
        if !removes_exact_member
            || !retires_owner
            || retirement_device_state.as_ref() != Some(&commit.device_state)
            || !commit.stream_activations().is_empty()
        {
            return Err(
                "Merge Owner-removal control differs from its exact membership entry".to_string(),
            );
        }
        let mut successor_membership = predecessor_membership.clone();
        successor_membership
            .add_entry(opened_entry.value)
            .map_err(|error| error.to_string())?;
        return VerifiedCircleActivations::membership_control(commit, commit_ref)
            .map(|activations| (activations, None))
            .map_err(|error| error.to_string());
    }
    if let super::membership::MembershipChange::ResolutionActivation { resolution } =
        &opened_entry.value.change
    {
        let resolution = resolution.clone();
        let resolution_proof = pending_resolution
            .filter(|proof| proof.verifies(&resolution))
            .ok_or_else(|| {
                "Merge conflict resolution lacks its verified Store activation".to_string()
            })?
            .clone();
        let opened_resolution = super::store_objects::load_membership_resolution_ref(
            storage,
            root.store_root_hash,
            &resolution,
        )
        .await
        .map_err(|error| error.to_string())?;
        let acceptance = &opened_resolution.value.replacement_acceptance;
        let mut expected = vec![
            super::store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.owner_registration.clone(),
                opened_resolution.value.replacement_grant.clone(),
                acceptance.membership.clone(),
            ),
            super::store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.owner_registration.clone(),
                opened_resolution.value.replacement_grant.clone(),
                acceptance.recovery.clone(),
            ),
        ];
        expected.sort();
        if transition.body.predecessor.is_some()
            || transition
                .body
                .resolutions
                .binary_search(&resolution)
                .is_err()
            || commit.stream_activations() != expected
        {
            return Err(
                "Merge conflict-resolution control differs from its exact membership entry"
                    .to_string(),
            );
        }
        let mut successor_membership = predecessor_membership.clone();
        successor_membership
            .add_entry(opened_entry.value)
            .map_err(|error| error.to_string())?;
        return VerifiedCircleActivations::membership_control(commit, commit_ref)
            .map(|activations| (activations, Some(resolution_proof)))
            .map_err(|error| error.to_string());
    }
    let super::membership::MembershipChange::SetMember {
        user_pubkey,
        role:
            super::membership::StoreMembershipRoleGrant::Owner {
                recovery: super::membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
            },
        grant_id,
        membership: Some(membership_anchor),
        replaces,
        retirement_device_state,
        ..
    } = &opened_entry.value.change
    else {
        return Err("Merge membership control does not activate one Owner promotion".to_string());
    };
    if retirement_device_state.is_some()
        || user_pubkey != &acceptance.request.member_pubkey
        || grant_id != &acceptance.request.intended_owner_grant
        || replaces != &BTreeSet::from([acceptance.request.member_grant.clone()])
        || acceptance.request.promoter_registration != commit.author_registration
    {
        return Err(
            "Merge Owner-promotion control differs from its exact membership entry".to_string(),
        );
    }
    verify_merge_owner_promotion_acceptance_with_history(
        storage,
        root,
        acceptance,
        verified_commits,
    )
    .await
    .map_err(|error| error.to_string())?;
    let request_activation = acceptance.activation.commit();
    let request_commit = verified_commits.get(request_activation).ok_or_else(|| {
        "Merge Owner-promotion request activation is absent from its verified history".to_string()
    })?;
    let verified_membership_activations = verified_merge_membership_prefix(
        verified_commits,
        commit_predecessor_references(&request_commit.commit),
    )
    .map_err(|error| error.to_string())?;
    let request_membership = load_merge_predecessor_membership_with_verified_activations(
        storage,
        root,
        &acceptance.request.predecessor_membership,
        &verified_membership_activations,
        None,
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => error.to_string(),
        RegistrationLoadError::Invalid(error) => error,
    })?;
    let predecessor_cut = commit
        .order
        .predecessor_cut()
        .map_err(|error| error.to_string())?;
    let StoreHistoryCut::MergeConcurrent(predecessor_frontier) = &predecessor_cut else {
        return Err("Merge membership transition carries a Serial Store cut".to_string());
    };
    let StoreCommitCoord::MergeConcurrent {
        stream_id: request_stream,
        ..
    } = request_activation.coord
    else {
        return Err("Merge Owner promotion carries a Serial request activation".to_string());
    };
    let activation_is_covered = predecessor_frontier
        .get(&request_stream)
        .is_some_and(|head| head.coord.sequence() >= request_activation.coord.sequence());
    let promoter_is_active = device_state_has_active_registration(
        predecessor_state,
        &acceptance.request.promoter_registration,
    );
    let candidate_is_active = device_state_has_active_registration(
        predecessor_state,
        &acceptance.request.member_registration,
    );
    let promoter_grant_is_active = predecessor_membership
        .active_owner_grant(&commit_author.value.author_pubkey)
        .as_ref()
        == Some(&acceptance.request.promoter_owner_grant);
    let candidate_grant_is_active = predecessor_membership
        .active_grant(&acceptance.request.member_grant)
        .is_some_and(|record| {
            record.member_pubkey == acceptance.request.member_pubkey
                && record.role == super::membership::StoreMembershipRoleGrant::Member
        });
    if !predecessor_membership.causally_includes(&request_membership)
        || !activation_is_covered
        || !promoter_is_active
        || !candidate_is_active
        || !promoter_grant_is_active
        || !candidate_grant_is_active
    {
        return Err(
            "Merge Owner-promotion transition does not include its accepted authority".to_string(),
        );
    }
    let super::store_commit::OwnerPromotionAnchors::MergeConcurrent {
        membership,
        recovery,
    } = &acceptance.anchors
    else {
        return Err("Merge Owner promotion carries Serial anchors".to_string());
    };
    if membership != membership_anchor {
        return Err("Merge Owner-promotion entry carries another membership anchor".to_string());
    }
    let mut expected = vec![
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.request.member_registration.clone(),
            acceptance.request.intended_owner_grant.clone(),
            membership.clone(),
        ),
        super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.request.member_registration.clone(),
            acceptance.request.intended_owner_grant.clone(),
            recovery.clone(),
        ),
    ];
    expected.sort();
    if commit.stream_activations() != expected {
        return Err(
            "Merge Owner-promotion control carries different stream activations".to_string(),
        );
    }
    VerifiedCircleActivations::membership_control(commit, commit_ref)
        .map(|activations| (activations, None))
        .map_err(|error| error.to_string())
}

pub(crate) async fn verify_merge_membership_head_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &super::membership::MembershipHeadRef,
    head: &super::membership::AuthorHead,
    activation: &StoreBatchCommitRef,
) -> Result<bool, String> {
    let (commit, author) = Box::pin(load_commit_with_author(storage, root, activation))
        .await
        .map_err(|error| error.to_string())?;
    let transition = commit
        .control()
        .and_then(super::store_commit::StoreControl::merge_membership_transition)
        .ok_or_else(|| {
            "membership head activation commit has no Merge membership transition".to_string()
        })?;
    if !transition.matches_head(head, reference)
        || transition.body.author_registration != commit.author_registration
    {
        return Err(
            "membership head differs from its exact activating Store transition".to_string(),
        );
    }
    let activation_observation = super::store_outbound::exact_next_announcement_slot(
        storage,
        root,
        &commit.author_registration,
        &author,
        Some(activation),
    )
    .await;
    match activation_observation {
        Ok((_, Some(_))) => {}
        Ok((_, None)) => return Ok(false),
        Err(super::store_outbound::StoreOutboundError::MergeAnnouncementOccupied { .. })
        | Err(super::store_outbound::StoreOutboundError::Object(
            super::store_objects::StoreObjectError::Storage(StorageError::NotFound(_)),
        )) => return Ok(false),
        Err(error) => return Err(error.to_string()),
    }
    let (_, _, replayed, verified_control) =
        Box::pin(replay_merge_device_history(storage, root, activation))
            .await
            .map_err(|error| error.to_string())?;
    if replayed != commit {
        return Err("membership head activation replay changed its Store commit".to_string());
    }
    if verified_control.is_none() {
        return Err(
            "membership head activation replay did not verify its Merge membership control"
                .to_string(),
        );
    }
    Ok(true)
}

enum PullCircleActivationError {
    Database(DbError),
    Invalid(String),
}

async fn load_pull_circle_activations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    identity: Option<&crate::keys::UserKeypair>,
    membership_authority: &CircleMembershipAuthority,
    verified_prefix: &VerifiedStreamActivationPrefix,
) -> Result<VerifiedCircleActivations, PullCircleActivationError> {
    if matches!(
        commit.control(),
        Some(super::store_commit::StoreControl::MergeMembership { .. })
    ) {
        return verify_merge_membership_control(storage, root, commit_ref, commit)
            .await
            .map_err(PullCircleActivationError::Invalid);
    }
    if !carries_circle_payload(commit) {
        return VerifiedCircleActivations::none(commit, commit_ref)
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()));
    }
    let identity = identity.ok_or_else(|| {
        PullCircleActivationError::Invalid(format!(
            "commit {} carries circle controls but no device identity was supplied",
            commit.seq()
        ))
    })?;
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(PullCircleActivationError::Database)?
        .ok_or_else(|| {
            PullCircleActivationError::Invalid(
                "Store founder is absent while loading circle controls".to_string(),
            )
        })?;
    Box::pin(
        super::circle_activation::load_circle_activations_with_prefix(
            db,
            storage,
            root,
            commit_ref,
            commit,
            author,
            identity,
            &founder,
            membership_authority,
            verified_prefix,
        ),
    )
    .await
    .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))
}

async fn load_applicable_circle_packages(
    db: &Database,
    storage: &dyn SyncStorage,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    activations: &[super::circle_ops::VerifiedCircleReference],
    author: &StoreDeviceRegistration,
) -> Result<Vec<LoadedCirclePackage>, PullCircleActivationError> {
    load_applicable_circle_packages_with_prior_accesses(
        db,
        storage,
        commit_ref,
        commit,
        activations,
        author,
        &CirclePackageAccesses::new(),
    )
    .await
}

fn circle_package_access(
    activation: &super::circle_ops::VerifiedCircleReference,
) -> Result<Option<CirclePackageAccess>, String> {
    let Some(access) = activation.local_access.as_ref() else {
        return Ok(None);
    };
    let Some(active) = access.active.as_ref() else {
        return Ok(None);
    };
    if !active.roster.verify() {
        return Err(format!(
            "Circle {} package roster is invalid",
            activation.circle_id
        ));
    }
    let super::circle::CircleAccessDisposition::Active {
        keyring,
        key_fingerprint,
        ..
    } = &access.leaf.value.disposition
    else {
        return Err(format!(
            "active Circle access for {} has an inactive leaf",
            activation.circle_id
        ));
    };
    if *key_fingerprint != activation.control.value.key_fingerprint() {
        return Err(format!(
            "Circle package key for {} differs from its activated control",
            activation.circle_id
        ));
    }
    let keyring = MasterKeyring::from_serialized(keyring).map_err(|error| {
        format!(
            "parse Circle package keyring for {}: {error}",
            activation.circle_id
        )
    })?;
    let encryption = EncryptionService::from(keyring)
        .service_for_fingerprint(key_fingerprint.as_bytes())
        .map_err(|error| {
            format!(
                "select Circle package key for {}: {error}",
                activation.circle_id
            )
        })?;
    Ok(Some(CirclePackageAccess {
        encryption,
        key_fingerprint: *key_fingerprint,
        writers: active.roster.members().keys().cloned().collect(),
    }))
}

fn circle_package_accesses(
    activations: &[super::circle_ops::VerifiedCircleReference],
) -> Result<CirclePackageAccesses, String> {
    let mut accesses = CirclePackageAccesses::new();
    for activation in activations {
        let Some(access) = circle_package_access(activation)? else {
            continue;
        };
        let key = (activation.circle_id, activation.control.coord.clone());
        if accesses.insert(key, access).is_some() {
            return Err(format!(
                "Circle {} has duplicate package access at one control",
                activation.circle_id
            ));
        }
    }
    Ok(accesses)
}

async fn load_applicable_circle_packages_with_prior_accesses(
    db: &Database,
    storage: &dyn SyncStorage,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    activations: &[super::circle_ops::VerifiedCircleReference],
    author: &StoreDeviceRegistration,
    prior_accesses: &CirclePackageAccesses,
) -> Result<Vec<LoadedCirclePackage>, PullCircleActivationError> {
    let mut loaded = Vec::new();
    for reference in commit.circle_packages() {
        if reference.package.schema_version > db.schema_version() {
            return Err(PullCircleActivationError::Invalid(format!(
                "Circle package for {} requires schema {}, local schema is {}",
                reference.circle_id,
                reference.package.schema_version,
                db.schema_version()
            )));
        }
        let same_commit = activations.iter().find(|activation| {
            activation.circle_id == reference.circle_id
                && activation.control.coord == reference.control
        });
        let context = if let Some(activation) = same_commit {
            let Some(access) =
                circle_package_access(activation).map_err(PullCircleActivationError::Invalid)?
            else {
                debug!(
                    circle_id = %reference.circle_id,
                    control = ?reference.control,
                    "skipping Circle package without active local access"
                );
                continue;
            };
            if !access.writers.contains(&author.author_pubkey) {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package author is not a member of {} at its exact control",
                    reference.circle_id
                )));
            }
            if access.key_fingerprint != reference.key_fingerprint {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package key for {} differs from its activated control",
                    reference.circle_id
                )));
            }
            access.encryption
        } else if let Some(access) =
            prior_accesses.get(&(reference.circle_id, reference.control.clone()))
        {
            if !access.writers.contains(&author.author_pubkey) {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package author is not a member of {} at its exact control",
                    reference.circle_id
                )));
            }
            if access.key_fingerprint != reference.key_fingerprint {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package key for {} differs from prepared access",
                    reference.circle_id
                )));
            }
            access.encryption.clone()
        } else {
            let Some((encryption, key_fingerprint)) = db
                .circle_access_context(reference.circle_id, reference.control.clone())
                .await
                .map_err(PullCircleActivationError::Database)?
            else {
                debug!(
                    circle_id = %reference.circle_id,
                    control = ?reference.control,
                    "skipping Circle package without durable local access"
                );
                continue;
            };
            if !db
                .circle_authorizes_writer(
                    reference.circle_id,
                    reference.control.clone(),
                    author.author_pubkey.clone(),
                )
                .await
                .map_err(PullCircleActivationError::Database)?
            {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package author is not a member of {} at its exact control",
                    reference.circle_id
                )));
            }
            if key_fingerprint != reference.key_fingerprint {
                return Err(PullCircleActivationError::Invalid(format!(
                    "Circle package key for {} differs from durable access",
                    reference.circle_id
                )));
            }
            encryption
                .service_for_fingerprint(reference.key_fingerprint.as_bytes())
                .map_err(|error| {
                    PullCircleActivationError::Invalid(format!(
                        "select durable Circle package key for {}: {error}",
                        reference.circle_id
                    ))
                })?
        };
        let blob_protection = BlobSpoolProtection::Opaque(context.clone());
        let package = load_circle_package(storage, commit_ref, commit, reference, context)
            .await
            .map_err(|error| PullCircleActivationError::Invalid(error.to_string()))?;
        loaded.push(LoadedCirclePackage {
            reference: reference.clone(),
            bytes: package.value,
            blob_protection,
        });
    }
    Ok(loaded)
}

async fn load_serial_store_package(
    db: &Database,
    storage: &dyn SyncStorage,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Option<Vec<u8>>, StorePullError> {
    if let Some(package) = commit.store_package() {
        if package.schema_version > db.schema_version() {
            return Err(StorePullError::Serial(format!(
                "commit {} requires schema {}, local schema is {}",
                commit.seq(),
                package.schema_version,
                db.schema_version()
            )));
        }
    }
    match load_store_package(storage, commit_ref, commit).await? {
        Some(package) => Ok(Some(package.value)),
        None if commit.store_package().is_none() => Ok(None),
        None => Err(StorePullError::Serial(format!(
            "commit {} Store package is absent",
            commit.seq()
        ))),
    }
}

enum Readiness {
    Ready,
    AlreadyMaterialized,
    Held(HeldStorePosition),
}

enum MaterializedCheck {
    Yes,
    Missing,
    Held(HeldStorePositionReason),
}

fn held_object_error(error: StoreObjectError) -> HeldStorePositionReason {
    match error {
        StoreObjectError::Storage(source) => HeldStorePositionReason::ObjectUnreadable {
            key: "exact Store object".to_string(),
            detail: source.to_string(),
        },
        StoreObjectError::InvalidObject { key, source, .. } => match *source {
            StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
            StoreProtocolError::RelocatedSlot { .. }
            | StoreProtocolError::RelocatedPackage { .. }
            | StoreProtocolError::StoreRootMismatch { .. }
            | StoreProtocolError::StoreMismatch { .. }
            | StoreProtocolError::FounderMismatch { .. } => {
                HeldStorePositionReason::WrongSlot(source.to_string())
            }
            source => HeldStorePositionReason::ObjectUnreadable {
                key,
                detail: source.to_string(),
            },
        },
    }
}

async fn readiness(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &super::store_commit::CommitFrontier,
    frontier: &BTreeMap<String, StoreBatchCommitRef>,
    device_state: &ResolvedStoreDeviceState,
    exclusion_freezes: &[StoreDeviceProposalAck],
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Readiness, StorePullError> {
    let stream_id = commit_stream_id(&commit_ref.coord);
    if let Some(current) = frontier.get(&stream_id) {
        if commit_ref.coord.sequence() <= current.coord.sequence() {
            match reference_is_materialized(db, storage, root, coverage, &stream_id, commit_ref)
                .await?
            {
                MaterializedCheck::Yes => return Ok(Readiness::AlreadyMaterialized),
                MaterializedCheck::Missing => {
                    return Ok(Readiness::Held(held_commit(
                        commit_ref,
                        HeldStorePositionReason::MissingCommit,
                    )))
                }
                MaterializedCheck::Held(reason) => {
                    return Ok(Readiness::Held(held_commit(commit_ref, reason)))
                }
            }
        }
        if commit.order.predecessor() != Some(current) {
            let reason = match commit.order.predecessor() {
                Some(missing) => HeldStorePositionReason::MissingPredecessor(missing.clone()),
                None => HeldStorePositionReason::InvalidObject(
                    "non-genesis Merge commit omits its exact predecessor".to_string(),
                ),
            };
            return Ok(Readiness::Held(held_commit(commit_ref, reason)));
        }
        if commit_ref.coord.sequence() != current.coord.sequence() + 1 {
            return Ok(Readiness::Held(held_commit(
                commit_ref,
                HeldStorePositionReason::InvalidObject(
                    "Merge commit sequence does not immediately follow its materialized frontier"
                        .to_string(),
                ),
            )));
        }
    } else if commit_ref.coord.sequence() != 1 || commit.order.predecessor().is_some() {
        let reason = match commit.order.predecessor() {
            Some(missing) => HeldStorePositionReason::MissingPredecessor(missing.clone()),
            None => HeldStorePositionReason::InvalidObject(
                "Merge commit beyond genesis omits its exact predecessor".to_string(),
            ),
        };
        return Ok(Readiness::Held(held_commit(commit_ref, reason)));
    }

    for record in device_state.devices.values() {
        let target_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &record.registration,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        if target_stream.to_string() != stream_id {
            continue;
        }
        let StoreDeviceStatus::Inactive {
            terminals,
            accepted_cut,
        } = &record.status
        else {
            break;
        };
        let StoreHistoryCut::MergeConcurrent(target_cut) = accepted_cut else {
            return Err(StorePullError::Database(
                "inactive Merge device carries a Serial accepted cut".to_string(),
            ));
        };
        let terminal_sequence = match target_cut.get(&target_stream) {
            Some(reference) => reference.coord.sequence(),
            None => 0,
        };
        if commit_ref.coord.sequence() > terminal_sequence {
            return Ok(Readiness::Held(held_commit(
                commit_ref,
                HeldStorePositionReason::InactiveDevice {
                    terminals: terminals.clone(),
                    accepted_cut: accepted_cut.clone(),
                },
            )));
        }
        break;
    }

    for freeze in exclusion_freezes {
        let target_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &freeze.proposal.target,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        if target_stream.to_string() != stream_id {
            continue;
        }
        let StoreHistoryCut::MergeConcurrent(target_cut) = &freeze.target_cut else {
            return Err(StorePullError::Database(
                "Merge exclusion freeze carries a Serial target cut".to_string(),
            ));
        };
        let frozen_sequence = match target_cut.get(&target_stream) {
            Some(reference) => reference.coord.sequence(),
            None => 0,
        };
        if commit_ref.coord.sequence() > frozen_sequence {
            return Ok(Readiness::Held(held_commit(
                commit_ref,
                HeldStorePositionReason::DeviceExclusionFreeze {
                    proposal: freeze.proposal.clone(),
                    target_cut: freeze.target_cut.clone(),
                },
            )));
        }
    }

    for (required_stream, required_ref) in commit.merge_dependencies().map_err(|error| {
        StorePullError::Database(format!("MergeConcurrent commit order: {error}"))
    })? {
        let required_stream = required_stream.to_string();
        match reference_is_materialized(db, storage, root, coverage, &required_stream, required_ref)
            .await?
        {
            MaterializedCheck::Yes => {}
            MaterializedCheck::Missing => {
                return Ok(Readiness::Held(held_dependency(
                    commit_ref,
                    &required_stream,
                    required_ref,
                    HeldStorePositionReason::MissingDependency {
                        device_id: required_stream.clone(),
                        commit: required_ref.clone(),
                    },
                )))
            }
            MaterializedCheck::Held(reason) => {
                return Ok(Readiness::Held(held_dependency(
                    commit_ref,
                    &required_stream,
                    required_ref,
                    reason,
                )))
            }
        }
    }
    Ok(Readiness::Ready)
}

async fn reference_is_materialized(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    coverage: &super::store_commit::CommitFrontier,
    stream_id: &str,
    reference: &StoreBatchCommitRef,
) -> Result<MaterializedCheck, StorePullError> {
    if commit_stream_id(&reference.coord) != stream_id {
        return Ok(MaterializedCheck::Held(HeldStorePositionReason::WrongSlot(
            format!(
                "commit reference stream {} differs from dependency stream {stream_id}",
                commit_stream_id(&reference.coord)
            ),
        )));
    }
    if let Some(actual) = db
        .exact_materialized_ref(stream_id, reference.coord.sequence())
        .await?
    {
        if actual != *reference {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: stream_id.to_string(),
                    referenced_commit: reference.clone(),
                    materialized_hash: actual.commit_hash,
                },
            ));
        }
        return Ok(MaterializedCheck::Yes);
    }
    let coverage = coverage.clone().into_refs();
    let Some(covered) = coverage.get(stream_id) else {
        return Ok(MaterializedCheck::Missing);
    };
    if reference.coord.sequence() > covered.coord.sequence() {
        return Ok(MaterializedCheck::Missing);
    }
    let mut cursor = covered.clone();
    loop {
        if cursor == *reference {
            return Ok(MaterializedCheck::Yes);
        }
        if cursor.coord.sequence() <= reference.coord.sequence() {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: stream_id.to_string(),
                    referenced_commit: reference.clone(),
                    materialized_hash: cursor.commit_hash,
                },
            ));
        }
        let (commit, _) = match load_commit_with_author(storage, root, &cursor).await {
            Ok(commit) => commit,
            Err(error) => return Ok(MaterializedCheck::Held(held_object_error(error))),
        };
        let Some(predecessor) = commit.order.predecessor() else {
            return Ok(MaterializedCheck::Missing);
        };
        cursor = predecessor.clone();
    }
}

pub(crate) async fn verify_merge_commit_currently_materialized(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &StoreBatchCommitRef,
) -> Result<(), StorePullError> {
    let StoreCommitCoord::MergeConcurrent { stream_id, .. } = &reference.coord else {
        return Err(StorePullError::Database(
            "Merge activation authority names a Serial commit".to_string(),
        ));
    };
    let stream_id = stream_id.to_string();
    let coverage = db.snapshot_coverage_frontier().await?;
    match reference_is_materialized(db, storage, root, &coverage, &stream_id, reference).await? {
        MaterializedCheck::Yes => Ok(()),
        MaterializedCheck::Missing => Err(StorePullError::Database(
            "Merge activation commit is absent from current accepted history".to_string(),
        )),
        MaterializedCheck::Held(reason) => Err(StorePullError::Database(format!(
            "Merge activation commit is not current accepted history: {reason:?}"
        ))),
    }
}

async fn resolve_candidate_device_operations(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    candidate: &Candidate,
) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
    match &candidate.device_operations {
        CandidateDeviceOperations::Verified(operations) => Ok(operations.clone()),
        CandidateDeviceOperations::MergePending {
            predecessor_membership,
        } => {
            let (state_ref, state) = db
                .store_device_state_for_order(&candidate.commit.order)
                .await?;
            if state_ref != candidate.commit.device_state {
                return Err(StorePullError::Database(
                    "Merge exclusion commit differs from its materialized predecessor device state"
                        .to_string(),
                ));
            }
            let authority =
                RegistrationPredecessorAuthority::MergeConcurrent(predecessor_membership);
            let resolver = DeviceStateResolver::Database(db);
            load_commit_device_operations(
                Some(&resolver),
                storage,
                root,
                &candidate.commit,
                &state,
                Some(&authority),
            )
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })
        }
    }
}

async fn apply_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    merge_candidate: &MergeCandidate,
    loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
    identity: Option<&crate::keys::UserKeypair>,
) -> Result<ApplyOutcome, StorePullError> {
    let candidate = &merge_candidate.candidate;
    let device_operations =
        resolve_candidate_device_operations(db, storage, root, candidate).await?;
    let verified_prefix = VerifiedStreamActivationPrefix::empty();
    let verified_circle_activations = match load_pull_circle_activations(
        db,
        storage,
        root,
        &candidate.commit_ref,
        &candidate.commit,
        &candidate.author,
        identity,
        &CircleMembershipAuthority::MergeConcurrent,
        &verified_prefix,
    )
    .await
    {
        Ok(activations) => activations,
        Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
        Err(PullCircleActivationError::Invalid(error)) => {
            return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
                error,
            )))
        }
    };
    let circle_packages = match load_applicable_circle_packages(
        db,
        storage,
        &candidate.commit_ref,
        &candidate.commit,
        verified_circle_activations.circles(),
        &candidate.author,
    )
    .await
    {
        Ok(packages) => packages,
        Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
        Err(PullCircleActivationError::Invalid(error)) => {
            return Ok(ApplyOutcome::Held(HeldStorePositionReason::InvalidObject(
                error,
            )))
        }
    };
    let mut packages =
        Vec::with_capacity(usize::from(candidate.package.is_some()) + circle_packages.len());
    if let Some(bytes) = candidate.package.as_ref() {
        let package = match parse_candidate_store_package(candidate, bytes) {
            Ok(package) => package,
            Err(error) => {
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::InvalidChangeset(error),
                ))
            }
        };
        let protection = storage.store_blob_protection()?;
        match prepare_merge_candidate_package(
            db,
            storage,
            store_dir,
            schema.clone(),
            package,
            protection,
        )
        .await?
        {
            Ok(package) => packages.push(package),
            Err(reason) => return Ok(ApplyOutcome::Held(reason)),
        }
    }
    for loaded in &circle_packages {
        let package = match parse_candidate_circle_package(candidate, loaded) {
            Ok(package) => package,
            Err(error) => {
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::InvalidChangeset(error),
                ))
            }
        };
        match prepare_merge_candidate_package(
            db,
            storage,
            store_dir,
            schema.clone(),
            package,
            loaded.blob_protection.clone(),
        )
        .await?
        {
            Ok(package) => packages.push(package),
            Err(reason) => return Ok(ApplyOutcome::Held(reason)),
        }
    }
    let outcome = Box::pin(commit_candidate(
        db,
        storage,
        root,
        merge_candidate,
        packages,
        device_operations,
        verified_circle_activations,
        loaded_predecessor_memberships,
    ))
    .await?;
    #[cfg(any(test, feature = "test-utils"))]
    if matches!(outcome, ApplyOutcome::Applied(_)) {
        db.reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
            device_id: commit_stream_id(&candidate.commit_ref.coord),
            seq: candidate.commit.seq(),
        })
        .await;
    }
    Ok(outcome)
}

struct PreparedMergeMaterializationPackage {
    package: AudiencePackage,
    changeset: ValidatedChangeset<Vec<u8>>,
    cleanup: Vec<LocalBlobCleanupIntent>,
}

async fn prepare_merge_candidate_package(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    package: AudiencePackage,
    blob_protection: BlobSpoolProtection,
) -> Result<Result<PreparedMergeMaterializationPackage, HeldStorePositionReason>, StorePullError> {
    let changeset = match ValidatedChangeset::new(package.changeset().to_vec(), schema) {
        Ok(changeset) => changeset,
        Err(super::session::ChangesetIdentityError::Row(error)) => {
            return Ok(Err(HeldStorePositionReason::InvalidRowIdentity {
                table: error.table().to_string(),
                reason: error.to_string(),
            }))
        }
        Err(error) => {
            return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                error.to_string(),
            )))
        }
    };
    let changes = crate::changeset::walk(changeset.bytes())
        .map_err(HeldStorePositionReason::InvalidChangeset);
    let changes = match changes {
        Ok(changes) => changes,
        Err(reason) => return Ok(Err(reason)),
    };
    let old_changes = match crate::changeset::walk_old(changeset.bytes()) {
        Ok(changes) => changes,
        Err(error) => return Ok(Err(HeldStorePositionReason::InvalidChangeset(error))),
    };
    let blob_decls = db.blob_decls();
    let eager = match cache_eager_blobs(&blob_decls, &changes, &package) {
        Ok(eager) => eager,
        Err(error) => {
            return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                error.to_string(),
            )))
        }
    };
    if let Err(failures) = verify_package_blobs(
        db,
        storage,
        store_dir,
        package.blob_bindings(),
        blob_protection,
        &eager,
    )
    .await
    {
        if failures.has_transport_failure() {
            return Err(StorePullError::BlobDownloads(failures));
        }
        return Ok(Err(HeldStorePositionReason::BlobDownloadFailed));
    }
    let cleanup = match local_blob_cleanup_intents(&blob_decls, &old_changes, &changes) {
        Ok(cleanup) => cleanup,
        Err(error) => {
            return Ok(Err(HeldStorePositionReason::InvalidChangeset(
                error.to_string(),
            )))
        }
    };
    Ok(Ok(PreparedMergeMaterializationPackage {
        package,
        changeset,
        cleanup,
    }))
}

struct PreparedMergeMaterialization {
    root: StoreRootRef,
    commit: StoreBatchCommit,
    commit_ref: StoreBatchCommitRef,
    activation_head: StoreDeviceHead,
    activation_head_object: ExactObjectRef,
    history_summary: RetainedVerifiedMergeHistorySummary,
    membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    membership_remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    packages: Vec<PreparedMergeMaterializationPackage>,
    device_operations: VerifiedStoreDeviceOperations,
    circle_activations: VerifiedCircleActivations,
    package_application: Option<crate::database::RetainedPackageApplication>,
}

struct AppliedMergeMaterialization {
    outcome: ApplyOutcome,
    max_updated_at: Option<super::hlc::Timestamp>,
    write_status_notifications: Vec<(crate::WriteId, crate::WriteStatus)>,
}

fn apply_prepared_merge_materialization_on(
    conn: &rusqlite::Transaction<'_>,
    blob_decls: &BlobDecls,
    gates: &super::gate::Gates,
    synced_tables: &[SyncedTable],
    timestamp_policy: IncomingTimestampPolicy,
    materialization: PreparedMergeMaterialization,
) -> Result<AppliedMergeMaterialization, DbError> {
    let PreparedMergeMaterialization {
        root,
        commit,
        commit_ref,
        activation_head,
        activation_head_object,
        history_summary,
        membership_objects,
        membership_remote_objects,
        registrations,
        packages,
        device_operations,
        circle_activations,
        package_application,
    } = materialization;
    let inactive_circles = circle_activations
        .circles()
        .iter()
        .filter_map(|activation| {
            activation
                .local_access
                .as_ref()
                .filter(|access| access.active.is_none())
                .map(|_| activation.circle_id)
        })
        .collect::<BTreeSet<_>>();
    let mut changeset_max = None;
    let mut returned_changes = Vec::new();
    let mut package_reported_fk_violation = false;
    Database::record_activated_store_device_registrations_on(conn, &commit, &registrations)?;
    Database::record_verified_circle_activations_on(
        conn,
        &commit,
        &commit_ref,
        circle_activations.circles(),
    )?;
    let retained_packages = packages
        .iter()
        .map(|prepared| prepared.package.clone())
        .collect::<Vec<_>>();
    for prepared in packages {
        let PreparedMergeMaterializationPackage {
            package,
            changeset,
            cleanup,
        } = prepared;
        let applied_bytes = match package.audience() {
            PackageAudience::Store => package.changeset().to_vec(),
            PackageAudience::Circle { circle_id, .. } => {
                super::gate::filter_inbound_circle_changeset(
                    conn,
                    package.changeset(),
                    *circle_id,
                    gates,
                )
                .map_err(|error| DbError::Message(error.to_string()))?
            }
        };
        let applied_changeset = changeset
            .validate_subset(applied_bytes.clone())
            .map_err(|error| DbError::Message(error.to_string()))?;
        let actual_changes = crate::changeset::walk(&applied_bytes).map_err(DbError::Message)?;
        if let Some(receiver_wall_ms) = timestamp_policy.received_wall_ms() {
            advance_max_updated_at(
                &mut changeset_max,
                &actual_changes,
                changeset.schema(),
                receiver_wall_ms,
            );
        }
        returned_changes.extend(
            actual_changes
                .iter()
                .filter(|change| !super::gate::is_routing_table(&change.table))
                .cloned(),
        );
        let apply =
            resolve_and_apply_changeset_with_policy_on(conn, applied_changeset, timestamp_policy)?;
        if !apply.constraint_conflict_tables.is_empty() {
            return Ok(AppliedMergeMaterialization {
                outcome: ApplyOutcome::Held(HeldStorePositionReason::ConstraintConflict(
                    apply.constraint_conflict_tables,
                )),
                max_updated_at: None,
                write_status_notifications: Vec::new(),
            });
        }
        package_reported_fk_violation |= apply.had_fk_violations;
        let winning_rows = crate::sync::apply::current_winning_rows_with_schema(
            conn,
            changeset.schema(),
            &applied_bytes,
        )?;
        for intent in cleanup {
            local_cleanup::record_obsolete_copy_intents_on(conn, blob_decls, &intent)?;
        }
        Database::install_pulled_package_activation_on(conn, &commit, &commit_ref, &package)?;
        Database::install_pulled_blob_activations_on(conn, &package, &commit_ref)?;
        Database::install_winning_blob_bindings_on(
            conn,
            gates,
            synced_tables,
            &package,
            &BlobActivation {
                coord: commit_ref.coord.clone(),
            },
            &winning_rows,
        )?;
    }
    let mut removal_session = rusqlite::session::Session::new(conn).map_err(DbError::from)?;
    for table in synced_tables {
        removal_session
            .attach(Some(table.name()))
            .map_err(DbError::from)?;
    }
    super::gate::prune_ineligible_scoped_rows(conn, gates, &inactive_circles)
        .map_err(|error| DbError::Message(error.to_string()))?;
    let mut removal_changeset = Vec::new();
    removal_session
        .changeset_strm(&mut removal_changeset)
        .map_err(DbError::from)?;
    drop(removal_session);
    let removed = crate::changeset::walk_old(&removal_changeset).map_err(DbError::Message)?;
    let removal_cleanup = local_blob_cleanup_intents(blob_decls, &removed, &[])
        .map_err(|error| DbError::Message(error.to_string()))?;
    returned_changes.extend(crate::changeset::walk(&removal_changeset).map_err(DbError::Message)?);
    for intent in removal_cleanup {
        local_cleanup::record_obsolete_copy_intents_on(conn, blob_decls, &intent)?;
    }
    if package_reported_fk_violation {
        let violations: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if violations {
            return Ok(AppliedMergeMaterialization {
                outcome: ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency),
                max_updated_at: None,
                write_status_notifications: Vec::new(),
            });
        }
    }
    let verified = VerifiedMergeMaterialization::verify(
        &root,
        &commit,
        &commit_ref,
        &registrations,
        &device_operations,
        &circle_activations,
        &activation_head,
        &activation_head_object,
        &history_summary,
        membership_objects.as_ref(),
        &retained_packages,
        package_application,
    )?;
    Database::install_pulled_merge_membership_activations_on(
        conn,
        &commit_ref,
        &membership_remote_objects,
    )?;
    Database::record_verified_merge_materialization_on(conn, verified)?;
    Ok(AppliedMergeMaterialization {
        outcome: ApplyOutcome::Applied(returned_changes),
        max_updated_at: changeset_max,
        write_status_notifications: Vec::new(),
    })
}

async fn verified_terminal_merge_retractions(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activation_head: &StoreDeviceHead,
    activation_head_object: &ExactObjectRef,
    activation_commit_ref: &StoreBatchCommitRef,
    activation_commit: &StoreBatchCommit,
    activation_predecessor_state: &ResolvedStoreDeviceState,
    activation_predecessor_membership: &MembershipChain,
    device_operations: &VerifiedStoreDeviceOperations,
    loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
) -> Result<Vec<super::remote_object::VerifiedCandidateNonactivation>, StorePullError> {
    let retained = Box::pin(db.retained_merge_replay_inputs()).await?;
    let activation_head_ref = super::store_commit::StoreDeviceHeadRef {
        head_hash: activation_head.head_hash(),
        object: activation_head_object.clone(),
    };
    let StoreMembershipStateRef::MergeConcurrent(current_membership_ref) =
        &activation_commit.membership_state
    else {
        return Err(StorePullError::Database(
            "Merge terminal retraction witness carries Serial membership".to_string(),
        ));
    };
    let MembershipStatus::Resolved(current_resolved) = activation_predecessor_membership.status()
    else {
        return Err(StorePullError::Database(
            "Merge terminal retraction witness membership is conflicted".to_string(),
        ));
    };
    let mut retractions = Vec::new();
    for materialization in &retained {
        let mut locator = Box::pin(db.author_exclusion_activation_for_candidate(
            materialization.commit_ref().clone(),
            materialization.commit().author_registration.clone(),
        ))
        .await?;
        if locator.is_none() {
            let expected_stream =
                super::store_commit::StreamActivation::device_authorized_stream_id(
                    root.store_root_hash,
                    &materialization.commit().author_registration,
                    super::store_commit::StreamAnchorDomain::StoreAnnouncements,
                );
            for (exclusion, accepted_cut) in device_operations.exclusions() {
                if exclusion.proposal.target != materialization.commit().author_registration {
                    continue;
                }
                let StoreHistoryCut::MergeConcurrent(accepted_cut) = accepted_cut else {
                    return Err(StorePullError::Database(
                        "Merge terminal retraction carries a Serial accepted cut".to_string(),
                    ));
                };
                let beyond_cutoff = accepted_cut.get(&expected_stream).is_none_or(|reference| {
                    materialization.commit_ref().coord.sequence() > reference.coord.sequence()
                });
                if beyond_cutoff {
                    locator = Some(crate::database::AuthorExclusionActivationLocator::verified(
                        exclusion.clone(),
                        accepted_cut.clone(),
                        activation_commit_ref.clone(),
                        activation_head_ref.clone(),
                    ));
                    break;
                }
            }
        }
        let Some(locator) = locator else {
            let Some(authority) = materialization.commit().membership_authority.as_ref() else {
                continue;
            };
            let predecessor_membership =
                loaded_predecessor_memberships.membership_for(materialization.commit_ref())?;
            let MembershipStatus::Resolved(predecessor_resolved) = predecessor_membership.status()
            else {
                return Err(StorePullError::Database(
                    "retained candidate predecessor membership is conflicted".to_string(),
                ));
            };
            let mut matching = predecessor_resolved
                .active_grants()
                .filter(|(_, record)| &record.creation_authority == authority);
            let Some((grant_id, _)) = matching.next() else {
                return Err(StorePullError::Database(
                    "retained candidate has no exact predecessor grant authority".to_string(),
                ));
            };
            if matching.next().is_some() {
                return Err(StorePullError::Database(
                    "retained candidate authority identifies multiple predecessor grants"
                        .to_string(),
                ));
            }
            if !matches!(
                current_resolved.grants.get(grant_id),
                Some(super::causal_grants::GrantState::Tombstoned { .. })
            ) {
                continue;
            }
            let verification = Box::pin(verify_membership_grant_revocation_activation(
                storage,
                root,
                grant_id,
                current_membership_ref,
                activation_commit_ref,
                &activation_head_ref,
                materialization.commit_ref(),
                materialization.commit(),
                materialization.activation_head(),
                materialization.activation_head_object(),
            ))
            .await?;
            retractions.push(
                super::remote_object::VerifiedCandidateNonactivation::membership_grant_revocation(
                    &verification,
                    super::store_commit::StoreBatchCommitDeletionTarget {
                        coord: materialization.commit_ref().coord.clone(),
                        object: materialization.commit_ref().object.clone(),
                        canonical_signed_bytes: materialization.commit().to_bytes(),
                    },
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?,
            );
            continue;
        };
        let activation = Box::pin(verify_author_exclusion_activation_with_verified_operation(
            storage,
            root,
            &locator,
            activation_head,
            activation_head_object,
            activation_commit_ref,
            activation_commit,
            activation_predecessor_state,
            device_operations,
            materialization.commit_ref(),
            materialization.commit(),
            materialization.activation_head(),
            materialization.activation_head_object(),
        ))
        .await?;
        retractions.push(
            super::remote_object::VerifiedCandidateNonactivation::author_exclusion(
                &activation,
                super::store_commit::StoreBatchCommitDeletionTarget {
                    coord: materialization.commit_ref().coord.clone(),
                    object: materialization.commit_ref().object.clone(),
                    canonical_signed_bytes: materialization.commit().to_bytes(),
                },
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?,
        );
    }
    let mut verified_by_reference = retractions
        .into_iter()
        .map(|verified| {
            let reference = verified
                .candidate_reference()
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            Ok((reference, verified))
        })
        .collect::<Result<BTreeMap<_, _>, StorePullError>>()?;
    loop {
        let mut additions = Vec::new();
        for materialization in &retained {
            if verified_by_reference.contains_key(materialization.commit_ref()) {
                continue;
            }
            let dependency = commit_predecessor_references(materialization.commit())
                .into_iter()
                .filter_map(|reference| {
                    verified_by_reference
                        .get(&reference)
                        .map(|verified| (reference, verified))
                })
                .next();
            let Some((_dependency_reference, dependency)) = dependency else {
                continue;
            };
            let author = Box::pin(db.activated_store_device_registration(
                materialization.commit().author_registration.clone(),
            ))
            .await?;
            let verified =
                super::remote_object::VerifiedCandidateNonactivation::dependency_retraction(
                    dependency,
                    super::store_commit::StoreBatchCommitDeletionTarget {
                        coord: materialization.commit_ref().coord.clone(),
                        object: materialization.commit_ref().object.clone(),
                        canonical_signed_bytes: materialization.commit().to_bytes(),
                    },
                    &author,
                    materialization.activation_head_object().clone(),
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            additions.push((materialization.commit_ref().clone(), verified));
        }
        if additions.is_empty() {
            break;
        }
        for (reference, verified) in additions {
            if verified_by_reference.insert(reference, verified).is_some() {
                return Err(StorePullError::Database(
                    "transitive Merge retraction constructed duplicate proof".to_string(),
                ));
            }
        }
    }
    let removed = verified_by_reference
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if retained.iter().any(|materialization| {
        !removed.contains(materialization.commit_ref())
            && materialization
                .history_summary()
                .causal_cut
                .values()
                .any(|reference| removed.contains(reference))
    }) {
        return Err(StorePullError::Database(
            "surviving retained Merge summary contains a retracted dependency".to_string(),
        ));
    }
    Ok(verified_by_reference.into_values().collect())
}

fn replay_retained_merge_projection_on(
    live: &rusqlite::Transaction<'_>,
    blob_decls: &BlobDecls,
    gates: &super::gate::Gates,
    synced_tables: &[SyncedTable],
    retracted: &BTreeSet<StoreBatchCommitRef>,
) -> Result<rusqlite::Connection, DbError> {
    super::retained_replay::validate_merge_generation_zero_preconditions(live)?;
    let baseline = Database::generation_zero_replay_baseline_on(live)?;
    let replay = super::retained_replay::open_image(&baseline.image_bytes)?;
    replay
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(DbError::from)?;
    let schema = Arc::new(TableSchema::for_apply(
        &replay,
        synced_tables,
        gates,
        crate::WritePolicy::MergeConcurrent,
    )?);
    let retained = Database::load_retained_merge_replay_inputs_on(live)?;
    let active_references = retained
        .iter()
        .filter(|materialization| !retracted.contains(materialization.commit_ref()))
        .map(|materialization| materialization.commit_ref().clone())
        .collect::<BTreeSet<_>>();
    for materialization in retained
        .iter()
        .filter(|materialization| !retracted.contains(materialization.commit_ref()))
    {
        let mut dependencies = materialization
            .commit()
            .order
            .dependencies()
            .into_iter()
            .flat_map(|dependencies| dependencies.values())
            .cloned()
            .collect::<BTreeSet<_>>();
        if let Some(predecessor) = materialization.commit().order.predecessor() {
            dependencies.insert(predecessor.clone());
        }
        for dependency in dependencies {
            if retracted.contains(&dependency) {
                return Err(DbError::Message(format!(
                    "surviving retained Merge commit {:?} depends on retracted commit {:?}",
                    materialization.commit_ref(),
                    dependency
                )));
            }
            if !active_references.contains(&dependency)
                && !replay_dependency_is_baseline_covered(&dependency, &baseline.exact_cut)
            {
                return Err(DbError::Message(format!(
                    "surviving retained Merge commit {:?} has unretained dependency {:?}",
                    materialization.commit_ref(),
                    dependency
                )));
            }
        }
    }
    let active_accepted_writes = retained
        .iter()
        .filter(|materialization| !retracted.contains(materialization.commit_ref()))
        .map(|materialization| materialization.commit().write_id.clone())
        .collect::<BTreeSet<_>>();
    let retracted_writes = retained
        .iter()
        .filter(|materialization| retracted.contains(materialization.commit_ref()))
        .map(|materialization| materialization.commit().write_id.clone())
        .collect::<BTreeSet<_>>();
    let write_overlays = Database::load_merge_replay_write_overlays_on(
        live,
        &active_accepted_writes,
        &retracted_writes,
    )?;
    let mut pending = retained
        .into_iter()
        .filter(|materialization| !retracted.contains(materialization.commit_ref()))
        .map(|materialization| (materialization.commit_ref().clone(), materialization))
        .collect::<BTreeMap<_, _>>();
    let mut applied = BTreeSet::new();
    while !pending.is_empty() {
        let ready = pending
            .iter()
            .filter_map(|(reference, materialization)| {
                let predecessor_ready =
                    materialization
                        .commit()
                        .order
                        .predecessor()
                        .is_none_or(|predecessor| {
                            replay_dependency_is_settled(predecessor, &applied, &baseline.exact_cut)
                        });
                let dependencies_ready = materialization
                    .commit()
                    .order
                    .dependencies()
                    .into_iter()
                    .flat_map(|dependencies| dependencies.values())
                    .all(|dependency| {
                        replay_dependency_is_settled(dependency, &applied, &baseline.exact_cut)
                    });
                (predecessor_ready && dependencies_ready).then(|| reference.clone())
            })
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(DbError::Message(
                "retained Merge replay is cyclic or has an unresolved dependency".to_string(),
            ));
        }
        let mut made_progress = false;
        for reference in ready {
            let materialization = pending
                .get(&reference)
                .expect("ready retained replay input remains pending")
                .clone();
            let timestamp_policy = match materialization.package_application() {
                None => IncomingTimestampPolicy::LocallyAuthored,
                Some(crate::database::RetainedPackageApplication::Received {
                    receiver_wall_ms,
                }) => IncomingTimestampPolicy::Received { receiver_wall_ms },
                Some(crate::database::RetainedPackageApplication::LocallyAuthored) => {
                    IncomingTimestampPolicy::LocallyAuthored
                }
            };
            let packages = materialization
                .packages()
                .iter()
                .cloned()
                .map(|package| {
                    let changeset =
                        ValidatedChangeset::new(package.changeset().to_vec(), schema.clone())
                            .map_err(|error| {
                                DbError::Message(format!(
                                    "retained Merge replay changeset: {error}"
                                ))
                            })?;
                    Ok(PreparedMergeMaterializationPackage {
                        package,
                        changeset,
                        cleanup: Vec::new(),
                    })
                })
                .collect::<Result<Vec<_>, DbError>>()?;
            let replay_materialization = PreparedMergeMaterialization {
                root: materialization.root().clone(),
                commit: materialization.commit().clone(),
                commit_ref: materialization.commit_ref().clone(),
                activation_head: materialization.activation_head().clone(),
                activation_head_object: materialization.activation_head_object().clone(),
                history_summary: materialization.history_summary().clone(),
                membership_objects: materialization.membership_objects().cloned(),
                membership_remote_objects: Vec::new(),
                registrations: materialization.registrations().to_vec(),
                packages,
                device_operations: materialization.device_operations().clone(),
                circle_activations: materialization.circle_activations().clone(),
                package_application: materialization.package_application(),
            };
            let tx = replay.unchecked_transaction().map_err(DbError::from)?;
            let outcome = apply_prepared_merge_materialization_on(
                &tx,
                blob_decls,
                gates,
                synced_tables,
                timestamp_policy,
                replay_materialization,
            )?;
            match outcome.outcome {
                ApplyOutcome::Applied(_) => {
                    tx.commit().map_err(DbError::from)?;
                    pending.remove(&reference);
                    applied.insert(reference);
                    made_progress = true;
                }
                ApplyOutcome::Held(HeldStorePositionReason::ForeignKeyDependency) => {
                    tx.rollback().map_err(DbError::from)?;
                }
                ApplyOutcome::Held(reason) => {
                    tx.rollback().map_err(DbError::from)?;
                    return Err(DbError::Message(format!(
                        "retained Merge replay held accepted commit {reference:?}: {reason:?}"
                    )));
                }
            }
        }
        if !made_progress {
            return Err(DbError::Message(
                "retained Merge replay has an unresolved foreign-key dependency".to_string(),
            ));
        }
    }
    for overlay in write_overlays {
        let tx = replay.unchecked_transaction().map_err(DbError::from)?;
        tx.pragma_update(None, "defer_foreign_keys", "ON")
            .map_err(DbError::from)?;
        let partitions = overlay
            .partitions
            .store
            .into_iter()
            .chain(overlay.partitions.circles)
            .chain(overlay.partitions.local);
        for partition in partitions {
            let changeset =
                ValidatedChangeset::new(partition.changeset, schema.clone()).map_err(|error| {
                    DbError::Message(format!(
                        "local replay write {} changeset: {error}",
                        overlay.write_id
                    ))
                })?;
            let applied = resolve_and_apply_changeset_with_policy_on(
                &tx,
                changeset,
                IncomingTimestampPolicy::LocallyAuthored,
            )?;
            if applied.had_fk_violations || !applied.constraint_conflict_tables.is_empty() {
                return Err(DbError::Message(format!(
                    "local replay write {} conflicts with accepted history",
                    overlay.write_id
                )));
            }
        }
        let violations: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if violations {
            return Err(DbError::Message(format!(
                "local replay write {} violates foreign keys",
                overlay.write_id
            )));
        }
        tx.commit().map_err(DbError::from)?;
    }
    Ok(replay)
}

fn replay_dependency_is_settled(
    dependency: &StoreBatchCommitRef,
    applied: &BTreeSet<StoreBatchCommitRef>,
    baseline: &CommitFrontier,
) -> bool {
    if applied.contains(dependency) {
        return true;
    }
    replay_dependency_is_baseline_covered(dependency, baseline)
}

fn replay_dependency_is_baseline_covered(
    dependency: &StoreBatchCommitRef,
    baseline: &CommitFrontier,
) -> bool {
    let CommitFrontier::MergeConcurrent(coverage) = baseline else {
        return false;
    };
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = &dependency.coord
    else {
        return false;
    };
    coverage.get(stream_id).is_some_and(|covered| {
        covered.coord.sequence() > *sequence
            || (covered.coord.sequence() == *sequence && covered == dependency)
    })
}

async fn verified_merge_membership_objects(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
    let Some(super::store_commit::StoreControl::MergeMembership { transition }) = commit.control()
    else {
        return Ok(None);
    };
    let entry = super::store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &transition.body.entry,
    )
    .await
    .map_err(StorePullError::Object)?;
    let author = super::store_objects::load_registration_ref(
        storage,
        root,
        &transition.body.author_registration,
    )
    .await
    .map_err(StorePullError::Object)?
    .value;
    let semantic_prefix = transition
        .head_slot
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StorePullError::Database(
                "Merge membership head slot has no protocol extension".to_string(),
            )
        })?;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let (head_bytes, head_object) = storage
        .read_protocol_slot(&context, &transition.head_slot, semantic_prefix)
        .await
        .map_err(StoreObjectError::from)
        .map_err(StorePullError::Object)?;
    let head: super::membership::AuthorHead = serde_json::from_slice(&head_bytes)
        .map_err(|error| StorePullError::Database(format!("Merge membership head: {error}")))?;
    if !head.verify(&author)
        || serde_json::to_vec(&head).map_err(|error| {
            StorePullError::Database(format!("serialize membership head: {error}"))
        })? != head_bytes
    {
        return Err(StorePullError::Database(
            "Merge membership head is not canonical or has an invalid device signature".to_string(),
        ));
    }
    let head_ref = super::membership::MembershipHeadRef {
        coord: head.entry_coord(),
        head_hash: head.head_hash(),
        object: head_object,
    };
    let objects = crate::database::VerifiedMergeMembershipObjects::verify(
        commit,
        commit_ref,
        &entry.value,
        &head,
        head_ref.clone(),
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    let family = commit.candidate_family();
    let mut remote_objects = vec![activate_pulled_merge_membership_authority(
        super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
            family,
            transition.body.entry.clone(),
            entry.bytes.clone(),
            entry.bytes,
            commit_ref.clone(),
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?,
        commit_ref,
    )?];
    remote_objects.push(activate_pulled_merge_membership_authority(
        super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
            family,
            head_ref.clone(),
            head_bytes.clone(),
            head_bytes,
            commit_ref.clone(),
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?,
        commit_ref,
    )?);
    let resolution = match &entry.value.change {
        super::membership::MembershipChange::ResolutionActivation { resolution } => {
            Some(resolution.clone())
        }
        _ => None,
    };
    let resolution_value = if let Some(resolution) = &resolution {
        let loaded = super::store_objects::load_membership_resolution_ref(
            storage,
            root.store_root_hash,
            resolution,
        )
        .await
        .map_err(StorePullError::Object)?;
        remote_objects.push(activate_pulled_merge_membership_authority(
            super::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                resolution.clone(),
                loaded.bytes.clone(),
                loaded.bytes,
                commit_ref.clone(),
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?,
            commit_ref,
        )?);
        Some(loaded.value)
    } else {
        None
    };
    let proof = super::store_commit::RetainedMergeMembershipProof {
        commit: commit_ref.clone(),
        commit_value: commit.clone(),
        announcement: None,
        entry: transition.body.entry.clone(),
        entry_value: entry.value,
        head: head_ref,
        head_value: head,
        resolution,
        resolution_value,
    };
    Ok(Some(VerifiedMergeMembershipClosure {
        objects,
        remote_objects,
        proof,
    }))
}

struct VerifiedMergeMembershipClosure {
    objects: crate::database::VerifiedMergeMembershipObjects,
    remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    proof: super::store_commit::RetainedMergeMembershipProof,
}

fn activate_pulled_merge_membership_authority(
    mut remote: super::remote_object::RemoteObjectRecord,
    commit_ref: &StoreBatchCommitRef,
) -> Result<super::remote_object::RemoteObjectRecord, StorePullError> {
    remote
        .mark_uploaded_verified()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    remote
        .into_activated(commit_ref)
        .map_err(|error| StorePullError::Database(error.to_string()))
}

async fn commit_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    merge_candidate: &MergeCandidate,
    packages: Vec<PreparedMergeMaterializationPackage>,
    device_operations: VerifiedStoreDeviceOperations,
    verified_circle_activations: VerifiedCircleActivations,
    loaded_predecessor_memberships: &LoadedMergePredecessorMemberships,
) -> Result<ApplyOutcome, StorePullError> {
    let candidate = &merge_candidate.candidate;
    let predecessor_membership = &merge_candidate.predecessor_membership;
    let (_, predecessor_state) = db
        .store_device_state_for_order(&candidate.commit.order)
        .await?;
    verify_merge_membership_state_ref(
        &candidate.commit.membership_state,
        predecessor_membership,
        &predecessor_state,
    )?;
    let (authorized_predecessor, recovery_author) = predecessor_with_recovery_author(
        predecessor_state,
        &candidate.commit,
        &candidate.registrations,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    let owner_recovery =
        verify_commit_owner_recovery_activation(storage, root, &candidate.commit, None).await?;
    let state_after = device_operations
        .apply_to(
            authorized_predecessor.clone(),
            &candidate.commit.device_state,
        )
        .and_then(|state| {
            apply_verified_device_lifecycle(
                state,
                &candidate.commit,
                &candidate.registrations,
                recovery_author.as_ref(),
                owner_recovery,
            )
        })
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let acknowledgement =
        validate_commit_acknowledgement(storage, root, &candidate.commit, &candidate.author)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
    let retained_acknowledgement = match acknowledgement {
        Some((acknowledgement_ref, acknowledgement_value)) => Some(
            retain_activated_acknowledgement(
                storage,
                root,
                &candidate.commit_ref,
                &candidate.commit,
                &candidate.author,
                acknowledgement_ref,
                acknowledgement_value,
            )
            .await?,
        ),
        None => None,
    };
    let membership =
        verified_merge_membership_objects(storage, root, &candidate.commit_ref, &candidate.commit)
            .await?;
    let registrations = candidate
        .commit
        .device_registrations()
        .iter()
        .zip(&candidate.registrations)
        .map(|(activation, (value, _))| RetainedVerifiedRegistration {
            reference: activation.registration.clone(),
            value: value.clone(),
        })
        .collect();
    let history = prepare_merge_history_successor(
        db,
        root,
        &candidate.commit,
        &candidate.commit_ref,
        predecessor_membership,
        &candidate.author,
        recovery_author.as_ref(),
        state_after.clone(),
        MergeHistorySuccessorEvidence {
            registrations,
            acknowledgement: retained_acknowledgement,
            membership_proof: membership.as_ref().map(|closure| closure.proof.clone()),
        },
    )
    .await?;
    let activation_head_ref = super::store_commit::StoreDeviceHeadRef {
        head_hash: merge_candidate.activation_head.head_hash(),
        object: merge_candidate.activation_head_object.clone(),
    };
    history
        .summary
        .open(
            &candidate.commit,
            &candidate.commit_ref,
            &merge_candidate.activation_head,
            &activation_head_ref,
            &state_after,
        )
        .map_err(|error| {
            StorePullError::Database(format!("open prepared Merge history summary: {error}"))
        })?;
    let retractions = Box::pin(verified_terminal_merge_retractions(
        db,
        storage,
        root,
        &merge_candidate.activation_head,
        &merge_candidate.activation_head_object,
        &candidate.commit_ref,
        &candidate.commit,
        &authorized_predecessor,
        predecessor_membership,
        &device_operations,
        loaded_predecessor_memberships,
    ))
    .await?;
    let receiver_wall_ms = db.receive_wall_ms();
    let materialization = PreparedMergeMaterialization {
        root: root.clone(),
        commit: candidate.commit.clone(),
        commit_ref: candidate.commit_ref.clone(),
        activation_head: merge_candidate.activation_head.clone(),
        activation_head_object: merge_candidate.activation_head_object.clone(),
        history_summary: history.summary,
        membership_objects: membership.as_ref().map(|closure| closure.objects.clone()),
        membership_remote_objects: membership
            .map(|closure| closure.remote_objects)
            .unwrap_or_default(),
        registrations: candidate.registrations.clone(),
        package_application: (!packages.is_empty())
            .then_some(crate::database::RetainedPackageApplication::Received { receiver_wall_ms }),
        packages,
        device_operations,
        circle_activations: verified_circle_activations,
    };
    let blob_decls = db.blob_decls();
    let gates = db.gates();
    let synced_tables = db.synced_tables().to_vec();
    #[cfg(any(test, feature = "test-utils"))]
    let materialization_failure = db.merge_materialization_failure_injection();
    let applied = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let mut applied = apply_prepared_merge_materialization_on(
                &tx,
                &blob_decls,
                &gates,
                &synced_tables,
                IncomingTimestampPolicy::Received { receiver_wall_ms },
                materialization,
            )?;
            if matches!(applied.outcome, ApplyOutcome::Applied(_)) {
                #[cfg(any(test, feature = "test-utils"))]
                if materialization_failure.reach(
                    crate::database::MergeMaterializationFailurePoint::SummaryMaterialization,
                )? {
                    return Err(DbError::Message(
                        "injected failure after Merge summary materialization".to_string(),
                    ));
                }
                if !retractions.is_empty() {
                    let retracted = retractions
                        .iter()
                        .map(|retraction| {
                            retraction
                                .candidate_reference()
                                .map_err(|error| DbError::Message(error.to_string()))
                        })
                        .collect::<Result<BTreeSet<_>, _>>()?;
                    applied.write_status_notifications =
                        Database::retract_verified_merge_materializations_on(&tx, retractions)?;
                    #[cfg(any(test, feature = "test-utils"))]
                    if materialization_failure.reach(
                        crate::database::MergeMaterializationFailurePoint::RetractionDeletion,
                    )? {
                        return Err(DbError::Message(
                            "injected failure after Merge retraction deletion".to_string(),
                        ));
                    }
                    let replay = replay_retained_merge_projection_on(
                        &tx,
                        &blob_decls,
                        &gates,
                        &synced_tables,
                        &retracted,
                    )?;
                    let projection_changeset = super::retained_replay::replace_live_projection(
                        &tx,
                        &replay,
                        &synced_tables,
                        gates.has_scoped_graph(),
                    )?;
                    #[cfg(any(test, feature = "test-utils"))]
                    if materialization_failure.reach(
                        crate::database::MergeMaterializationFailurePoint::ProjectionReplacement,
                    )? {
                        return Err(DbError::Message(
                            "injected failure after Merge projection replacement".to_string(),
                        ));
                    }
                    Database::replace_store_device_exclusion_freezes_from_replay_on(&tx)?;
                    let old_projection = crate::changeset::walk_old(&projection_changeset)
                        .map_err(DbError::Message)?;
                    let new_projection =
                        crate::changeset::walk(&projection_changeset).map_err(DbError::Message)?;
                    for intent in
                        local_blob_cleanup_intents(&blob_decls, &old_projection, &new_projection)
                            .map_err(|error| DbError::Message(error.to_string()))?
                    {
                        local_cleanup::record_obsolete_copy_intents_on(&tx, &blob_decls, &intent)?;
                    }
                    if let ApplyOutcome::Applied(rows) = &mut applied.outcome {
                        rows.extend(new_projection);
                    }
                }
                tx.commit().map_err(DbError::from)?;
            }
            Ok(applied)
        })
        .await?;
    if let Some(max_applied) = applied.max_updated_at.as_ref() {
        db.hlc().advance_past(max_applied);
    }
    for (write_id, status) in applied.write_status_notifications {
        db.notify_write_status(write_id, status);
    }
    resume_merge_retraction_cleanups(db, storage, root).await?;
    Ok(applied.outcome)
}

fn held_commit(
    reference: &StoreBatchCommitRef,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Commit {
            device_id: commit_stream_id(&reference.coord),
            commit: reference.clone(),
        },
        reason,
    }
}

fn held_package(
    reference: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    let package = commit
        .store_package()
        .expect("held Store package is named by the commit");
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Package {
            device_id: commit_stream_id(&reference.coord),
            seq: commit.seq(),
            package_hash: package.content_hash,
        },
        reason,
    }
}

fn held_dependency(
    dependent: &StoreBatchCommitRef,
    required_device_id: &str,
    required: &StoreBatchCommitRef,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Dependency {
            dependent_device_id: commit_stream_id(&dependent.coord),
            dependent_commit: dependent.clone(),
            required_device_id: required_device_id.to_string(),
            required_commit: required.clone(),
        },
        reason,
    }
}

fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    match coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn one_retained_checkpoint() -> (
        Database,
        crate::sync::test_helpers::TestStore,
        MembershipChain,
        OpenedRetainedMergeHistorySummary,
    ) {
        let db = crate::sync::test_helpers::open_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "retained-checkpoint-conflict",
            crate::keys::UserKeypair::generate(),
        )
        .await
        .expect("create retained-checkpoint Store");
        let membership = super::super::pull::load_cycle_membership(&store.storage, &db)
            .await
            .expect("load checkpoint membership")
            .chain
            .expect("Merge Store has membership");
        crate::sync::test_helpers::host_exec(
            &db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('checkpoint-conflict', 'checkpoint', NULL, 1, \
                     '0000000001000-0000-checkpoint', '2026-07-21')",
        )
        .await;
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load checkpoint device id")
            .expect("checkpoint device id exists");
        let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        assert!(super::super::store_outbound::prepare_pending_store_write(
            &db,
            &store.storage,
            &device_id,
            "2026-07-21T00:00:00Z",
            &store.signer,
            &store_dir,
            Some(&membership),
        )
        .await
        .expect("prepare checkpoint commit"));
        assert_eq!(
            super::super::store_outbound::drain_store_writes(&db, &store.storage)
                .await
                .expect("publish checkpoint commit"),
            1,
        );
        let reference = db
            .latest_local_store_position()
            .await
            .expect("load checkpoint position")
            .expect("checkpoint position exists");
        let mut retained = db
            .retained_merge_history_frontier(vec![reference])
            .await
            .expect("open retained checkpoint");
        assert_eq!(retained.len(), 1);
        (db, store, membership, retained.remove(0))
    }

    #[tokio::test]
    async fn retained_checkpoint_merge_rejects_same_coordinate_competitors() {
        let (_db, store, membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;

        let mut conflicting_commit = checkpoint.clone();
        let (coordinate, reference) = conflicting_commit
            .summary
            .causal_cut
            .first_key_value()
            .map(|(coordinate, reference)| (coordinate.clone(), reference.clone()))
            .expect("checkpoint causal cut is nonempty");
        let mut replacement = reference;
        replacement.commit_hash = ObjectHash::digest(b"same-coordinate competing commit");
        conflicting_commit
            .summary
            .causal_cut
            .insert(coordinate, replacement);
        assert!(merge_retained_merge_history(
            &store.root,
            &membership,
            vec![checkpoint.clone(), conflicting_commit],
        )
        .is_err());

        let mut conflicting_head = checkpoint.clone();
        let announcement = conflicting_head
            .announcement_frontier
            .values_mut()
            .next()
            .expect("opened checkpoint has an announcement frontier");
        announcement.reference.head_hash = ObjectHash::digest(b"same-stream competing head");
        assert!(merge_retained_merge_history(
            &store.root,
            &membership,
            vec![checkpoint, conflicting_head],
        )
        .is_err());
    }

    #[tokio::test]
    async fn retained_checkpoint_merge_rejects_different_sequence_acknowledgement_forks() {
        let (db, store, membership, checkpoint) = Box::pin(one_retained_checkpoint()).await;
        let coverage = CommitFrontier::from_refs(
            crate::WritePolicy::MergeConcurrent,
            db.materialized_frontier()
                .await
                .expect("load acknowledgement coverage"),
        )
        .expect("derive acknowledgement coverage");
        crate::sync::test_helpers::publish_store_ack_fixture(
            &db,
            &store.storage,
            None,
            coverage,
            &store.signer,
            Some(&membership),
        )
        .await
        .expect("publish retained acknowledgement");
        let acknowledgement_commit = db
            .latest_local_store_position()
            .await
            .expect("load acknowledgement commit")
            .expect("acknowledgement commit exists");
        let mut retained = db
            .retained_merge_history_frontier(vec![acknowledgement_commit])
            .await
            .expect("open acknowledgement checkpoint");
        let acknowledgement = retained
            .remove(0)
            .summary
            .acknowledgements
            .into_values()
            .next()
            .expect("checkpoint retains its acknowledgement");
        let mut forged_higher_fork = acknowledgement.clone();
        let (latest_ref, latest_value) = acknowledgement
            .latest()
            .expect("acknowledgement proof chain has a latest entry");
        let device_id = latest_ref.registration.device_id;
        let mut forked_at_same_sequence = (latest_ref.clone(), latest_value.clone());
        forked_at_same_sequence.0.ack_hash = ObjectHash::digest(b"forked acknowledgement");
        forged_higher_fork
            .chain
            .insert(latest_ref.sequence, forked_at_same_sequence.clone());
        let higher_sequence = latest_ref.sequence + 1;
        forked_at_same_sequence.0.sequence = higher_sequence;
        forked_at_same_sequence.1.sequence = higher_sequence;
        forged_higher_fork
            .chain
            .insert(higher_sequence, forked_at_same_sequence);

        let mut merged = checkpoint.summary.acknowledgements;
        insert_latest_acknowledgement(&mut merged, device_id, acknowledgement)
            .expect("first acknowledgement establishes the retained stream");
        assert!(
            insert_latest_acknowledgement(&mut merged, device_id, forged_higher_fork,).is_err()
        );
    }

    #[test]
    fn recovery_cursor_requires_the_exact_origin_activation_pair() {
        let recovery_id = super::super::store_commit::DeviceRecoveryId::from_hash(
            ObjectHash::digest(b"recovery cursor id"),
        );
        let owner_grant = super::super::causal_grants::MembershipGrantId(ObjectHash::digest(
            b"recovery cursor owner grant",
        ));
        let recovery_slot = crate::storage::cloud::ObjectSlot::opaque(
            "store-v1/test/recovery.json".to_string(),
            "recovery-cursor-slot".to_string(),
        )
        .expect("construct recovery cursor slot");
        let node = OwnerRecoveryNodeRef {
            owner_pubkey: "recovery-owner".to_string(),
            owner_grant: owner_grant.clone(),
            sequence: 1,
            node_hash: ObjectHash::digest(b"recovery cursor node"),
            object: ExactObjectRef::new(
                recovery_slot.clone(),
                1,
                ObjectHash::digest(b"recovery cursor bytes"),
            ),
        };
        let origin = StoreDeviceRegistrationOrigin::Recovery {
            recovery_id,
            recovery_slot,
            owner_grant: owner_grant.clone(),
        };
        let activation = StoreDeviceRegistrationActivation::Recovery {
            recovery_id,
            node: node.clone(),
        };

        assert_eq!(
            registration_recovery_cursor(&origin, &activation)
                .expect("derive exact recovery cursor"),
            Some(OwnerRecoveryCursor {
                owner_grant,
                position: OwnerRecoveryPosition::At { node: node.clone() },
            })
        );

        let wrong_activation = StoreDeviceRegistrationActivation::Recovery {
            recovery_id: super::super::store_commit::DeviceRecoveryId::from_hash(
                ObjectHash::digest(b"another recovery cursor id"),
            ),
            node,
        };
        assert!(registration_recovery_cursor(&origin, &wrong_activation).is_err());
    }

    #[tokio::test]
    async fn cycle_authorization_rejects_an_absent_serial_coordination_head() {
        let db = crate::sync::test_helpers::open_serial_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "absent-serial-cycle-head",
            crate::keys::UserKeypair::generate(),
        )
        .await
        .expect("create Serial Store");
        store.home.remove(serial_head_key());

        let result = load_serial_cycle_authorization(
            &store.storage,
            store
                .storage
                .serial_coordination()
                .expect("Serial coordination"),
            &store.root,
        )
        .await;

        assert!(matches!(
            result,
            Err(StorePullError::Serial(reason)) if reason == "global head is absent"
        ));
    }

    #[tokio::test]
    async fn cycle_authorization_rejects_a_nonfounder_serial_genesis_head() {
        let db = crate::sync::test_helpers::open_serial_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "nonfounder-serial-genesis-head",
            crate::keys::UserKeypair::generate(),
        )
        .await
        .expect("create Serial Store");
        let (_, founder_registration, _) = store
            .founder_device_authority()
            .await
            .expect("load founder Store device");
        let other_identity = crate::keys::UserKeypair::generate();
        let other_origin = StoreDeviceRegistrationOrigin::Join {
            attempt_id: super::super::store_commit::DeviceJoinAttemptId::from_hash(
                ObjectHash::digest(b"non-founder genesis registration"),
            ),
            attempt_slot: crate::storage::cloud::ObjectSlot::logical(
                "store-v1/test/non-founder-genesis/attempt.json".to_string(),
            )
            .expect("construct attempt slot"),
            outcome_slot: crate::storage::cloud::ObjectSlot::logical(
                "store-v1/test/non-founder-genesis/outcome.json".to_string(),
            )
            .expect("construct outcome slot"),
        };
        let other = StoreDeviceRegistration::signed(
            store.root.clone(),
            other_origin,
            founder_registration.provider,
            StoreCommitAnchor::Serial,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/test/non-founder-genesis/ack/1.json".to_string(),
                )
                .expect("construct acknowledgement slot"),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/test/non-founder-genesis/snapshot/1.json".to_string(),
                )
                .expect("construct snapshot slot"),
            },
            &other_identity,
        )
        .expect("sign another Store registration");
        let other_signer = other
            .device_signer(&other_identity)
            .expect("derive another device signer");
        let registration_prefix =
            super::super::store_commit::registration_semantic_prefix(&other.device_id.to_string());
        let registration_context = ProtocolObjectContext::signed_plaintext(
            store.root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_slot = store
            .storage
            .allocate_protocol_slot(&registration_context, &registration_prefix, ".json")
            .await
            .expect("allocate another registration slot");
        let prepared = store
            .storage
            .prepare_protocol_object(
                &registration_context,
                registration_slot,
                &registration_prefix,
                other.to_bytes(),
            )
            .expect("prepare another registration");
        let registration_object =
            super::super::store_objects::create_exact_object(&store.storage, &prepared)
                .await
                .expect("publish another registration");
        let other_registration =
            StoreDeviceRegistrationRef::from_registration(&other, registration_object);
        let forged = StoreSerialHead::signed(
            store.root.store_root_hash,
            StoreSerialHeadState::Genesis {
                root: store.root.clone(),
                founder_registration: other_registration,
            },
            &other_signer,
        )
        .expect("sign non-founder genesis head");
        let coordination = store
            .storage
            .serial_coordination()
            .expect("Serial coordination");
        let current = coordination
            .read_head(serial_head_key())
            .await
            .expect("read current Serial head");
        coordination
            .replace_head(serial_head_key(), &current.version, &forged.to_bytes())
            .await
            .expect("replace Serial head with non-founder genesis");

        let result =
            load_serial_cycle_authorization(&store.storage, coordination, &store.root).await;

        match result {
            Err(StorePullError::Serial(reason)) => assert_eq!(
                reason,
                "Serial genesis head does not name the exact Store founder"
            ),
            Err(error) => panic!("unexpected error: {error:?}"),
            Ok(_) => panic!("non-founder Serial genesis head was accepted"),
        }
    }

    #[tokio::test]
    async fn merge_outbound_projects_membership_to_the_commits_predecessors() {
        let founder = crate::sync::test_helpers::user_keypair_from_seed([42; 32]);
        let founder_db = crate::sync::test_helpers::open_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &founder_db,
            "causal-membership-proof",
            founder.clone(),
        )
        .await
        .expect("create Merge Store");
        let candidate = crate::sync::test_helpers::user_keypair_from_seed([43; 32]);
        let encryption = crate::encryption::EncryptionService::from_key([73; 32]);
        crate::sync::membership_ops::invite_member(
            &store.storage,
            store.home.as_ref(),
            &founder,
            &super::super::hlc::Hlc::new("causal-membership-proof".to_string()),
            &crate::sync::test_helpers::pubkey_hex(&candidate),
            None,
            super::super::membership::MemberRole::Member,
            &encryption,
            "causal-membership-proof",
            "Causal Membership Proof",
            &founder_db,
        )
        .await
        .expect("invite exact Store member");

        let candidate_db = crate::sync::test_helpers::open_test_db();
        crate::sync::test_helpers::install_active_device_fixture(
            &store,
            &founder_db,
            &candidate_db,
            &candidate,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate candidate device");
        crate::sync::test_helpers::promote_active_member_fixture(
            &store,
            &founder_db,
            &candidate_db,
            &founder,
            &candidate,
            &encryption,
        )
        .await
        .expect("promote candidate Owner");
        let candidate_membership =
            super::super::pull::load_cycle_membership(&store.storage, &candidate_db)
                .await
                .expect("load candidate Owner membership");
        let (_candidate_temp, candidate_store_dir) = crate::sync::test_helpers::temp_store_dir();
        let candidate_pull = Box::pin(pull_store_commits_with_identity(
            &candidate_db,
            candidate_db.synced_tables(),
            &store.storage,
            None,
            store.root.store_root_hash,
            &candidate_store_dir,
            candidate_membership.chain.as_ref(),
            Some(&candidate),
        ))
        .await
        .expect("pull candidate Owner to the common Store history");
        assert!(candidate_pull.held_positions.is_empty());

        let earlier_db = &candidate_db;
        let earlier_owner = &candidate;
        let later_db = &founder_db;
        let later_owner = &founder;

        let mut earlier_membership =
            super::super::pull::load_cycle_membership(&store.storage, earlier_db)
                .await
                .expect("load earlier Owner membership")
                .chain
                .expect("initialized Store has membership");
        let _rotated = super::super::invite::revoke_member_durable(
            &store.storage,
            store.home.as_ref(),
            store.root.store_root_hash,
            &mut earlier_membership,
            earlier_owner,
            &crate::sync::test_helpers::pubkey_hex(&candidate),
            &store.root.store_root_id.to_string(),
            "0000000003000-0000-causal-proof",
            &encryption,
            &super::super::cloud_storage::PendingRotation::none(),
            earlier_db,
        )
        .await
        .expect("publish traversal-earlier Owner removal control");
        let earlier_control = earlier_db
            .latest_local_store_position()
            .await
            .expect("load earlier Owner position")
            .expect("earlier Owner published the membership control");
        let (earlier_value, _) =
            load_commit_with_author(&store.storage, &store.root, &earlier_control)
                .await
                .expect("load traversal-earlier control");
        let Some(super::super::store_commit::StoreControl::MergeMembership { transition }) =
            earlier_value.control()
        else {
            panic!("earlier Owner position is not a Merge membership control");
        };

        let changeset = crate::sync::test_helpers::capture_bytes(
            &crate::sync::test_helpers::open_test_db(),
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('causal-proof-row', 'causal proof', NULL, \
                       '0000000001000-0000-causal-proof', '2026-07-21')",
            ],
        )
        .await;
        later_db
            .enqueue_store_changeset_for_test(changeset)
            .await
            .expect("enqueue later concurrent write");
        let later_membership = super::super::pull::load_cycle_membership(&store.storage, later_db)
            .await
            .expect("load membership containing the concurrent control");
        let caller_membership = later_membership
            .chain
            .as_ref()
            .expect("initialized Store has membership");
        let earlier_head_ref = caller_membership
            .head_refs()
            .iter()
            .find(|head| head.coord == transition.body.entry.coord)
            .expect("caller membership contains the concurrent control")
            .clone();
        let earlier_head = super::super::membership_ops::load_exact_membership_head(
            &store.storage,
            &store.root,
            &earlier_head_ref,
        )
        .await
        .expect("load concurrent membership head");
        let later_device_id = later_db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("load later Owner device id")
            .expect("later Owner device is activated");
        let (_later_temp, later_store_dir) = crate::sync::test_helpers::temp_store_dir();
        assert!(super::super::store_outbound::prepare_pending_store_write(
            later_db,
            &store.storage,
            &later_device_id,
            "2026-07-21T00:02:00Z",
            later_owner,
            &later_store_dir,
            later_membership.chain.as_ref(),
        )
        .await
        .expect("prepare later concurrent write"));
        super::super::store_outbound::drain_store_writes(later_db, &store.storage)
            .await
            .expect("publish later concurrent write");
        let later_commit = later_db
            .latest_local_store_position()
            .await
            .expect("load later Owner position")
            .expect("later Owner published the data commit");

        let (later_value, _) = load_commit_with_author(&store.storage, &store.root, &later_commit)
            .await
            .expect("load later concurrent commit");
        let later_predecessors = commit_predecessor_references(&later_value);
        assert!(!later_predecessors.contains(&earlier_control));
        let super::super::circle_control::StoreMembershipStateRef::MergeConcurrent(
            signed_membership,
        ) = &later_value.membership_state
        else {
            panic!("later commit carries Serial membership state");
        };
        assert!(!signed_membership
            .heads
            .iter()
            .any(|head| head.coord == transition.body.entry.coord));

        let verified = verify_merge_history_refs(
            &store.storage,
            &store.root,
            [later_commit.clone(), earlier_control.clone()],
        )
        .await
        .expect("verify both concurrent commits");
        let later_prefix = verified_merge_membership_prefix(&verified.commits, later_predecessors)
            .expect("derive the later commit's exact membership prefix");
        assert_eq!(
            later_prefix
                .classify_head(&earlier_head_ref, &earlier_head, &earlier_control,)
                .expect("classify concurrent control against later prefix"),
            VerifiedMergePrefixHeadStatus::OutsidePrefix,
        );
    }

    #[tokio::test]
    async fn merge_gap_reports_the_exact_signed_predecessor() {
        let source = crate::sync::test_helpers::open_test_db();
        let store = crate::sync::test_helpers::TestStore::create(
            &source,
            "exact-predecessor-test",
            crate::keys::UserKeypair::generate(),
        )
        .await
        .expect("create exact predecessor test Store");
        let changeset = crate::sync::test_helpers::capture_bytes(
            &crate::sync::test_helpers::open_test_db(),
            &[
                "INSERT INTO notes (id, title, body, _updated_at, created_at) \
               VALUES ('gap-row', 'gap', NULL, '0000000001000-0000-gap', '2026-01-01')",
            ],
        )
        .await;
        let first = store
            .publish_changeset("founder", 1, &changeset, source.schema_version())
            .await
            .expect("publish first exact commit");
        let second = store
            .publish_changeset("founder", 2, &changeset, source.schema_version())
            .await
            .expect("publish second exact commit");
        let third = store
            .publish_changeset("founder", 3, &changeset, source.schema_version())
            .await
            .expect("publish third exact commit");
        let (_, founder, _) = store
            .founder_device_authority()
            .await
            .expect("load founder authority");
        let commit = super::super::store_objects::load_commit_ref(
            &store.storage,
            store.root.store_root_hash,
            &third,
            &founder,
        )
        .await
        .expect("load third exact commit")
        .value;
        let stream_id = commit_stream_id(&first.coord);
        let frontier = BTreeMap::from([(stream_id.clone(), first.clone())]);
        let coverage =
            CommitFrontier::from_refs(crate::WritePolicy::MergeConcurrent, frontier.clone())
                .expect("build exact frontier");
        let CommitFrontier::MergeConcurrent(device_cut) = coverage.clone() else {
            panic!("Merge test frontier changed policy")
        };
        let (_, device_state) = source
            .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(device_cut))
            .await
            .expect("load exact device state");
        let target = crate::sync::test_helpers::open_test_db();

        let readiness = readiness(
            &target,
            &store.storage,
            &store.root,
            &coverage,
            &frontier,
            &device_state,
            &[],
            &third,
            &commit,
        )
        .await
        .expect("evaluate exact predecessor gap");

        assert!(matches!(
            readiness,
            Readiness::Held(HeldStorePosition {
                reason: HeldStorePositionReason::MissingPredecessor(missing),
                ..
            }) if missing == second
        ));
    }
}
