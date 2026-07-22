//! Causal discovery and atomic materialization for immutable Store commits.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rusqlite::session::{ConflictAction, ConflictType};
use tracing::debug;

use super::{
    causal_grants, circle, circle_activation, circle_control, circle_ops, device_join, gate, hlc,
    membership, membership_ops, provider, remote_object, retained_replay, session, storage,
    store_commit, store_objects, store_outbound, store_reclaim, store_snapshot, wrapped_store_key,
};

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

mod ancestry;
mod circle_packages;
mod device_lifecycle_state;
mod device_operations;
mod history_state;
mod join_bootstrap;
mod join_validation;
mod merge_discovery;
mod merge_history;
mod merge_materialization;
mod merge_membership;
mod merge_replay;
mod merge_retained_authority;
mod pull;
mod registration;
mod serial;
mod serial_apply;
mod terminal_authority;
mod terminal_cleanup;

pub(crate) use circle_packages::load_serial_store_package;
use circle_packages::*;
pub(crate) use join_validation::*;
pub(crate) use merge_discovery::*;
use merge_replay::*;

pub(crate) use ancestry::*;
pub(crate) use device_lifecycle_state::*;
pub(crate) use device_operations::*;
pub(crate) use history_state::*;
pub(crate) use join_bootstrap::*;
pub(crate) use merge_history::*;
pub(crate) use merge_materialization::*;
pub(crate) use merge_membership::*;
pub(crate) use merge_retained_authority::*;
pub use pull::*;
pub(crate) use registration::*;
pub use serial::*;
pub(crate) use serial_apply::*;
pub(crate) use terminal_authority::*;
pub use terminal_cleanup::*;

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
pub(crate) struct Candidate {
    pub(crate) commit_ref: StoreBatchCommitRef,
    pub(crate) commit: StoreBatchCommit,
    pub(crate) author: StoreDeviceRegistration,
    pub(crate) package: Option<Vec<u8>>,
    pub(crate) registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub(crate) device_operations: CandidateDeviceOperations,
}

#[derive(Clone)]
pub(crate) struct MergeCandidate {
    pub(crate) candidate: Candidate,
    pub(crate) activation_head: StoreDeviceHead,
    pub(crate) activation_head_object: ExactObjectRef,
    pub(crate) predecessor_membership: MembershipChain,
}

pub(crate) struct LoadedMergePredecessorMemberships {
    pub(crate) by_commit: BTreeMap<StoreBatchCommitRef, MembershipChain>,
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

pub(crate) struct SerialApplicationCandidate {
    pub(crate) candidate: Candidate,
    pub(crate) membership_authority: SerialAuthorizationState,
    pub(crate) authorization_after: SerialAuthorizationState,
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
pub(crate) enum CandidateDeviceOperations {
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

pub(crate) struct AuthorizedSerialCommit {
    pub(crate) commit_ref: StoreBatchCommitRef,
    pub(crate) commit: StoreBatchCommit,
    pub(crate) author: StoreDeviceRegistration,
    pub(crate) registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub(crate) device_operations: VerifiedStoreDeviceOperations,
    pub(crate) device_state_before: ResolvedStoreDeviceState,
    pub(crate) device_state_after: ResolvedStoreDeviceState,
    pub(crate) acknowledgement: Option<(
        super::store_commit::StoreAckRef,
        super::store_commit::StoreAck,
    )>,
    pub(crate) authorization_before: SerialAuthorizationState,
    pub(crate) authorization_after: SerialAuthorizationState,
}

pub(crate) fn held_commit(
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

pub(crate) fn held_package(
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

pub(crate) fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    match coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    }
}

#[cfg(test)]
#[path = "store_pull/tests.rs"]
mod tests;
