//! Causal discovery and atomic materialization for immutable Store commits.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rusqlite::session::{ConflictAction, ConflictType};
use tracing::debug;

use super::apply::{
    apply_changeset_strict_on, resolve_and_apply_changeset_with_schema_on, ValidatedChangeset,
};
use super::audience_package::{AudiencePackage, PackageAudience};
use super::circle_control::StoreMembershipStateRef;
use super::conflict::TableSchema;
use super::membership::{MembershipChain, MembershipStatus, SerialAuthorizationState};
use super::pull::{
    advance_max_updated_at, cache_eager_blobs, local_blob_cleanup_intents, verify_package_blobs,
};
use super::session::SyncedTable;
use super::storage::{
    BlobSpoolProtection, CoordinationError, CoordinationStorage, ProtocolObjectContext,
    ProtocolObjectDomain, StorageError, SyncStorage,
};
use super::store_commit::{
    head_slot_prefix, serial_head_key, ActivatedStoreDeviceRegistrationRef, CirclePackageRef,
    CommitFrontier, DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinOutcomeBody,
    DeviceStreamAnchor, ObjectHash, OwnerRecoveryCursor, OwnerRecoveryNode, OwnerRecoveryNodeRef,
    OwnerRecoveryPosition, ResolvedStoreDeviceState, StoreBatchCommit, StoreBatchCommitRef,
    StoreCommitAnchor, StoreCommitCoord, StoreDeviceExclusionOutcome, StoreDeviceExclusionProof,
    StoreDeviceHead, StoreDeviceProposalAck, StoreDeviceProposalState, StoreDeviceRegistration,
    StoreDeviceRegistrationActivation, StoreDeviceRegistrationActivationRef,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    StoreDeviceStatus, StoreHistoryCut, StoreProtocolError, StoreRootRef, StoreSerialHead,
    StoreSerialHeadState, StoreSerialPredecessor, StreamActivationId,
    VerifiedStoreDeviceExclusionOutcome, VerifiedStoreDeviceOperations, SERIAL_STREAM_ID,
};
use super::store_objects::{
    load_circle_package, load_commit_ref, load_device_exclusion_outcome_ref,
    load_device_exclusion_proposal_ref, load_device_join_attempt_ref, load_device_join_outcome_ref,
    load_founder_registration, load_owner_recovery_node_ref, load_reclaim_authorization_ref,
    load_reclaim_receipt_ref, load_registration_ref, load_store_ack_ref, load_store_package,
    load_store_protocol_root, StoreObjectError,
};
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::{self, LocalBlobCleanupIntent};
use crate::changeset::RowChange;
use crate::database::{BlobActivation, Database, DbError};
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
    circle_activations: Vec<super::circle_ops::VerifiedCircleReference>,
    device_operations: CandidateDeviceOperations,
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
    authorization_after: SerialAuthorizationState,
}

enum RegistrationLoadError {
    Object(StoreObjectError),
    Invalid(String),
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
    let StoreMembershipStateRef::MergeConcurrent(state) = state else {
        return Err(RegistrationLoadError::Invalid(
            "Merge registration lifecycle commit carries Serial membership state".to_string(),
        ));
    };
    let root_value = Box::pin(load_store_protocol_root(storage, root))
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
    Box::pin(super::membership_ops::load_anchored_chain_at_exact_heads(
        storage,
        root,
        &root_value.descriptor.founder_pubkey,
        &state.heads,
        &state.resolutions,
    ))
    .await
    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
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

pub(crate) async fn load_device_join_authorization(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    state: &StoreMembershipStateRef,
) -> Result<DeviceJoinBootstrapAuthorization, StorePullError> {
    match state {
        StoreMembershipStateRef::MergeConcurrent(_) => {
            let chain = load_merge_predecessor_membership(storage, root, state)
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
            let authorization =
                load_serial_authorization_at_position(storage, root, reference).await?;
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
}

pub(crate) async fn verify_device_join_cleanup_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    activation: &super::device_join::DeviceJoinCleanupActivation,
) -> Result<super::device_join::JoinerJoinTerminal, StorePullError> {
    let (commit, author) = load_commit_with_author(storage, root, &activation.activation).await?;
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
    let receipts =
        validate_commit_join_cleanup_receipts(storage, root, &commit, &author, Some(&predecessor))
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
) -> Result<(), RegistrationLoadError> {
    let Some(reference) = commit.acknowledgement() else {
        return Ok(());
    };
    let ack = load_store_ack_ref(storage, root, reference, activating_author)
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
        let (_, metadata) = super::store_snapshot::load_store_snapshot_ref(
            storage,
            root,
            &commit.author_registration,
            activating_author,
            snapshot,
        )
        .await
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
        if !ack.store_cut.frontier().covers(&metadata.coverage) {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement does not cover its exact snapshot".to_string(),
            ));
        }
    }
    Ok(())
}

async fn validate_commit_reclaim_authorization(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let Some(reference) = commit.reclaim_authorization() else {
        return Ok(());
    };
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
    let activation = predecessor_commit_matching(storage, root, &commit.order, |candidate, _| {
        candidate == &evidence.claim.target.activation
    })
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
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    let Some(reference) = commit.reclaim_receipt() else {
        return Ok(());
    };
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
    if Box::pin(predecessor_commit_matching(
        storage,
        root,
        &commit.order,
        |_, candidate| candidate.reclaim_authorization() == Some(&authorization),
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
) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, RegistrationLoadError>
{
    Box::pin(validate_commit_acknowledgement(
        storage,
        root,
        commit,
        activating_author,
    ))
    .await?;
    Box::pin(validate_commit_reclaim_authorization(
        storage,
        root,
        commit,
        activating_author,
        predecessor,
    ))
    .await?;
    Box::pin(validate_commit_reclaim_receipt(
        storage,
        root,
        commit,
        activating_author,
        predecessor,
    ))
    .await?;
    Box::pin(validate_commit_join_attempts(
        storage,
        root,
        commit,
        activating_author,
        predecessor,
    ))
    .await?;
    Box::pin(validate_commit_join_outcomes(
        storage,
        root,
        commit,
        activating_author,
        predecessor,
    ))
    .await?;
    Box::pin(validate_commit_join_abandonments(
        storage,
        root,
        commit,
        activating_author,
        predecessor,
    ))
    .await?;
    Box::pin(validate_commit_join_cleanup_receipts(
        storage,
        root,
        commit,
        activating_author,
        predecessor,
    ))
    .await?;
    let mut registrations = Vec::with_capacity(commit.device_registrations().len());
    for activated in commit.device_registrations() {
        let registration = load_registration_ref(storage, root, &activated.registration)
            .await
            .map_err(RegistrationLoadError::Object)?
            .value;
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "registration activation has no exact predecessor membership authority".to_string(),
            )
        })?;
        let authority = registration_activation(
            storage,
            root,
            activated,
            &registration,
            activating_author,
            &commit.order,
            commit.serial_recovery_activation(),
            predecessor,
        )
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
    let target_stream =
        super::membership::AuthorStreamId::store_announcements(root, &proposal.object.value.target);
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
        proposals.push((reference.clone(), proposal.clone()));
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
        let verified = match (&outcome.object.value, reference) {
            (
                StoreDeviceExclusionOutcome::Cancelled(_),
                super::store_commit::StoreDeviceExclusionOutcomeRef::Cancelled(reference),
            ) => VerifiedStoreDeviceExclusionOutcome::Cancelled(reference.clone()),
            (
                StoreDeviceExclusionOutcome::Excluded(exclusion),
                super::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(reference),
            ) => {
                let accepted_cut = match &exclusion.proof {
                    StoreDeviceExclusionProof::Serial
                        if commit.policy() == crate::WritePolicy::Serial =>
                    {
                        commit
                            .order
                            .predecessor_cut()
                            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?
                    }
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
                                "device exclusion proof names another frozen device state"
                                    .to_string(),
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
                        cutoff.clone()
                    }
                    StoreDeviceExclusionProof::MergeConcurrent { .. } => {
                        return Err(RegistrationLoadError::Invalid(
                            "Serial device exclusion outcome carries a Merge proof".to_string(),
                        ))
                    }
                };
                if accepted_cut.policy() != commit.policy() {
                    return Err(RegistrationLoadError::Invalid(
                        "device exclusion proof policy differs from its activating commit"
                            .to_string(),
                    ));
                }
                VerifiedStoreDeviceExclusionOutcome::Excluded {
                    reference: reference.clone(),
                    accepted_cut,
                }
            }
            _ => {
                return Err(RegistrationLoadError::Invalid(
                    "device exclusion outcome variant differs from its exact reference".to_string(),
                ))
            }
        };
        outcomes.push(verified);
    }
    VerifiedStoreDeviceOperations::from_commit(commit, proposals, outcomes)
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
    let resolver = DeviceStateResolver::Database(db);
    load_commit_device_operations(
        Some(&resolver),
        storage,
        root,
        commit,
        &state,
        Some(&authority),
    )
    .await
    .map_err(|error| match error {
        RegistrationLoadError::Object(error) => StorePullError::Object(error),
        RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
    })
}

async fn validate_commit_join_abandonments(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    if !commit
        .device_join_attempt_decisions()
        .iter()
        .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Abandoned(_)))
    {
        return Ok(());
    }
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

async fn validate_commit_join_cleanup_receipts(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<Vec<super::device_join::DeviceJoinCleanupReceiptObject>, RegistrationLoadError> {
    if commit.device_join_cleanup_receipts().is_empty() {
        return Ok(Vec::new());
    }
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
        let attempt = DeviceJoinAttempt::parse_at(&attempt_bytes, attempt_ref, &owner)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
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
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    if commit.device_join_outcomes().is_empty() {
        return Ok(());
    }
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
    for outcome_ref in commit.device_join_outcomes() {
        if !predecessor_contains_join_attempt(storage, root, &commit.order, outcome_ref.attempt())
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
        let attempt = DeviceJoinAttempt::parse_at(&attempt_bytes, outcome_ref.attempt(), &owner)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
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
        if matches!(outcome.body, DeviceJoinOutcomeBody::Activated { .. }) != activation.is_some() {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome and registration activation are not one closed operation"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

async fn validate_commit_join_attempts(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    activating_author: &StoreDeviceRegistration,
    predecessor: Option<&RegistrationPredecessorAuthority<'_>>,
) -> Result<(), RegistrationLoadError> {
    if !commit
        .device_join_attempt_decisions()
        .iter()
        .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)))
    {
        return Ok(());
    }
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
        let attempt = Box::pin(load_device_join_attempt_ref(
            storage,
            root,
            reference,
            activating_author,
        ))
        .await
        .map_err(RegistrationLoadError::Object)?
        .value;
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
    activating_order: &super::store_commit::StoreCommitOrder,
    serial_recovery_activation: Option<&super::store_commit::SerialRecoveryActivation>,
    predecessor: &RegistrationPredecessorAuthority<'_>,
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
            let attempt_context = ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                ProtocolObjectDomain::DeviceJoinAttempt,
            );
            let attempt_prefix =
                super::store_commit::device_join_attempt_semantic_prefix(*attempt_id);
            let attempt_bytes = storage
                .read_protocol_object(&attempt_context, &outcome.attempt().object, &attempt_prefix)
                .await
                .map_err(|error| RegistrationLoadError::Object(StoreObjectError::Storage(error)))?;
            let unverified_attempt: DeviceJoinAttempt = serde_json::from_slice(&attempt_bytes)
                .map_err(|error| {
                    RegistrationLoadError::Invalid(format!("device join attempt: {error}"))
                })?;
            let owner =
                load_registration_ref(storage, root, &unverified_attempt.owner_registration)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
            let attempt = load_device_join_attempt_ref(storage, root, outcome.attempt(), &owner)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            if !predecessor_contains_join_attempt(
                storage,
                root,
                activating_order,
                outcome.attempt(),
            )
            .await?
                || attempt.expected_registration != *registration
                || attempt.registration_slot != *activated.registration.object.slot()
                || !predecessor.verifies_owner(
                    &attempt.membership,
                    &owner.author_pubkey,
                    &attempt.owner_grant,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "activated registration differs from its exact join attempt".to_string(),
                ));
            }
            let outcome_value = load_device_join_outcome_ref(storage, root, outcome, &owner)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
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
                    &attempt,
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
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::DeviceJoinAttemptRef,
) -> Result<bool, RegistrationLoadError> {
    Ok(
        predecessor_commit_matching(storage, root, order, |_, commit| {
            commit.device_join_attempt_decisions().iter().any(|decision| {
                matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(reference) if reference == expected)
            })
        })
        .await?
        .is_some(),
    )
}

async fn predecessor_commit_matching(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    order: &super::store_commit::StoreCommitOrder,
    mut matches: impl FnMut(&StoreBatchCommitRef, &StoreBatchCommit) -> bool,
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
        let (commit, _) = load_commit_with_author(storage, root, &reference)
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
    order: &super::store_commit::StoreCommitOrder,
    expected: &super::store_commit::DeviceJoinOutcomeRef,
) -> Result<bool, RegistrationLoadError> {
    Ok(
        predecessor_commit_matching(storage, root, order, |_, commit| {
            commit
                .device_join_outcomes()
                .binary_search(expected)
                .is_ok()
        })
        .await?
        .is_some(),
    )
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
    pub(crate) circle_activations: Vec<super::circle_ops::VerifiedCircleReference>,
    pub(crate) device_operations: VerifiedStoreDeviceOperations,
    pub(crate) authorization_after: SerialAuthorizationState,
}

#[doc(hidden)]
pub struct SerialResolutionPlan {
    pub(crate) head: StoreSerialHead,
    pub(crate) head_object: super::storage::VersionedObject,
    pub(crate) commits: Vec<SerialResolutionCommit>,
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
    pull_store_commits_with_identity(
        db,
        tables,
        storage,
        None,
        store_root_hash,
        store_dir,
        membership,
        None,
    )
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
    pull_store_commits_with_identity(
        db,
        tables,
        storage,
        serial_coordination,
        store_root_hash,
        store_dir,
        membership,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn pull_store_commits_with_identity(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn CoordinationStorage>,
    store_root_hash: ObjectHash,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
    identity: Option<&crate::keys::UserKeypair>,
) -> Result<StorePullResult, StorePullError> {
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
        let discovered = discover_merge_stream(storage, &root, &registration_ref, &registration)
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
        held.extend(discovered.held);
        for (commit_ref, commit) in discovered.commits {
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
            if commit
                .store_package()
                .as_ref()
                .is_some_and(|package| package.schema_version > db.schema_version())
            {
                let required = commit
                    .store_package()
                    .as_ref()
                    .expect("checked Store package")
                    .schema_version;
                held.push(held_commit(
                    &commit_ref,
                    HeldStorePositionReason::NewerSchema {
                        local: db.schema_version(),
                        required,
                    },
                ));
                continue;
            }
            let predecessor_membership = if commit.device_join_attempt_decisions().is_empty()
                && commit.device_join_outcomes().is_empty()
                && commit.device_join_cleanup_receipts().is_empty()
                && commit.device_registrations().is_empty()
                && commit.device_exclusion_proposals().is_empty()
                && commit.device_exclusion_outcomes().is_empty()
                && commit.reclaim_authorization().is_none()
                && commit.reclaim_receipt().is_none()
            {
                None
            } else {
                match load_merge_predecessor_membership(storage, &root, &commit.membership_state)
                    .await
                {
                    Ok(membership) => Some(membership),
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
                }
            };
            let predecessor_authority = predecessor_membership
                .as_ref()
                .map(RegistrationPredecessorAuthority::MergeConcurrent);
            let registrations = match load_commit_registrations(
                storage,
                &root,
                &commit,
                &registration,
                predecessor_authority.as_ref(),
            )
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
            if !membership_authorizes(membership, &commit, &registration) {
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
            let circle_activations = match load_pull_circle_activations(
                db,
                storage,
                &root,
                &commit_ref,
                &commit,
                &registration,
                identity,
            )
            .await
            {
                Ok(activations) => activations,
                Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
                Err(PullCircleActivationError::Invalid(error)) => {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::InvalidObject(error),
                    ));
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
                    predecessor_membership: predecessor_membership.clone().ok_or_else(|| {
                        StorePullError::Database(
                            "device exclusion commit has no predecessor membership authority"
                                .to_string(),
                        )
                    })?,
                }
            };
            candidates.insert(
                key,
                Candidate {
                    commit_ref,
                    commit,
                    author: registration.clone(),
                    package,
                    registrations,
                    circle_activations,
                    device_operations,
                },
            );
        }
    }

    let schema: Arc<TableSchema> = {
        let tables = tables.to_vec();
        let gates = db.gates();
        Arc::new(
            db.call(move |conn| {
                TableSchema::for_apply(conn, &tables, &gates, crate::WritePolicy::MergeConcurrent)
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
            let candidate = candidates
                .get(&key)
                .expect("candidate key came from the same map");
            let exclusion_freezes = db.store_device_exclusion_freezes().await?;
            let current_frontier =
                CommitFrontier::from_refs(crate::WritePolicy::MergeConcurrent, frontier.clone())
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
                &candidate.commit_ref,
                &candidate.commit,
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
                    let candidate = candidates
                        .remove(&key)
                        .expect("ready candidate remains present");
                    match apply_candidate(db, storage, &root, store_dir, schema.clone(), &candidate)
                        .await?
                    {
                        ApplyOutcome::Applied(changes) => {
                            let stream_id = commit_stream_id(&candidate.commit_ref.coord);
                            frontier.insert(stream_id.clone(), candidate.commit_ref.clone());
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
                            let held_position = held_commit(&candidate.commit_ref, reason);
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

    Ok(StorePullResult {
        changesets_applied,
        devices_pulled: u64::try_from(applied_devices.len()).expect("device count fits in u64"),
        held_positions: held,
        visible_heads,
        serial_head: None,
        row_changes,
        asset_downloads_failed,
        local_blob_cleanup_pending,
        frontier,
    })
}

struct MergeStreamDiscovery {
    latest_head: Option<StoreDeviceHead>,
    commits: Vec<(StoreBatchCommitRef, StoreBatchCommit)>,
    held: Vec<HeldStorePosition>,
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
    let stream_id = super::membership::AuthorStreamId::store_announcements(root, registration_ref);
    let activation = StreamActivationId::store_announcements(root, registration_ref);
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let mut slot = first_slot.clone();
    let mut predecessor = None;
    let mut sequence = 1_u64;
    let mut latest_head = None;
    let mut commits = Vec::new();
    let mut held = Vec::new();
    let mut visited = BTreeSet::new();

    loop {
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
                held.push(HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: stream_id.to_string(),
                        seq: sequence,
                        head_hash: ObjectHash::digest(&bytes),
                    },
                    reason: HeldStorePositionReason::InvalidObject(error.to_string()),
                });
                break;
            }
        };
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
            held.push(HeldStorePosition {
                coordinate: HeldStoreCoordinate::Head {
                    device_id: stream_id.to_string(),
                    seq: sequence,
                    head_hash: unverified.head_hash(),
                },
                reason: HeldStorePositionReason::WrongSlot(
                    "Store head differs from its activated successor chain".to_string(),
                ),
            });
            break;
        }
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
                held.push(held_commit(&unverified.commit, held_object_error(error)));
                break;
            }
        };
        let head = match StoreDeviceHead::parse_at(
            &bytes,
            root.store_root_hash,
            registration,
            &unverified.commit,
        ) {
            Ok(head) => head,
            Err(error) => {
                held.push(HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: stream_id.to_string(),
                        seq: sequence,
                        head_hash: unverified.head_hash(),
                    },
                    reason: held_protocol_error(error),
                });
                break;
            }
        };
        let next_slot = head.successor.next_slot.clone();
        predecessor = Some(object);
        sequence = sequence.checked_add(1).ok_or_else(|| {
            StorePullError::Database(format!(
                "Store announcement stream {stream_id} sequence overflow"
            ))
        })?;
        commits.push((head.commit.clone(), commit.value));
        latest_head = Some(head);
        slot = next_slot;
    }

    Ok(MergeStreamDiscovery {
        latest_head,
        commits,
        held,
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
    let unverified: StoreBatchCommit =
        serde_json::from_slice(&bytes).map_err(|error| StoreObjectError::InvalidObject {
            semantic_prefix: semantic_prefix.clone(),
            key: reference.object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(error.to_string())),
        })?;
    let author = load_registration_ref(storage, root, &unverified.author_registration)
        .await?
        .value;
    let commit =
        StoreBatchCommit::parse_at(&bytes, root.store_root_hash, &reference.coord, &author)
            .and_then(|commit| {
                reference.verify_commit(&commit)?;
                Ok(commit)
            })
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
    Ok((commit, author))
}

pub(crate) struct DeviceJoinBootstrapCommit {
    pub reference: StoreBatchCommitRef,
    pub commit: StoreBatchCommit,
    pub registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub device_operations: VerifiedStoreDeviceOperations,
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
    let verified_root = Box::pin(load_store_protocol_root(storage, root))
        .await?
        .value;
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
            super::store_commit::StoreCommitOrder::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            } => {
                let mut refs = dependencies.values().collect::<Vec<_>>();
                refs.extend(predecessor.iter());
                if refs.is_empty() {
                    genesis.clone()
                } else {
                    ResolvedStoreDeviceState::merge(refs.into_iter().map(|dependency| {
                        states
                            .get(dependency)
                            .expect("topological predecessor state exists")
                            .clone()
                    }))
                    .map_err(|error| StorePullError::Database(error.to_string()))?
                }
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
        let expected_state = match &commit.order {
            super::store_commit::StoreCommitOrder::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            } => {
                let mut frontier = dependencies.clone();
                if let Some(predecessor) = predecessor {
                    let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord
                    else {
                        return Err(StorePullError::Database(
                            "Merge bootstrap predecessor carries a Serial coordinate".to_string(),
                        ));
                    };
                    if frontier
                        .insert(stream_id, predecessor.clone())
                        .is_some_and(|existing| existing != *predecessor)
                    {
                        return Err(StorePullError::Database(
                            "Merge bootstrap predecessor conflicts with its dependency cut"
                                .to_string(),
                        ));
                    }
                }
                StoreDeviceStateRef::merge_concurrent(
                    CommitFrontier::MergeConcurrent(frontier),
                    &predecessor_state,
                )
            }
            super::store_commit::StoreCommitOrder::Serial { predecessor, .. } => {
                StoreDeviceStateRef::serial(predecessor.clone(), &predecessor_state)
            }
        }
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        if commit.device_state != expected_state
            || !predecessor_state
                .devices
                .get(&commit.author_registration.device_id)
                .is_some_and(|record| {
                    record.registration == commit.author_registration
                        && matches!(
                            record.status,
                            super::store_commit::StoreDeviceStatus::Active
                        )
                })
        {
            return Err(StorePullError::Database(
                "device join bootstrap commit differs from its exact predecessor device state"
                    .to_string(),
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
        let verified_state = match verified_authorization {
            DeviceJoinBootstrapAuthorization::MergeConcurrent { state, .. }
            | DeviceJoinBootstrapAuthorization::Serial { state, .. } => state,
        };
        if carries_lifecycle && verified_state != &commit.membership_state {
            return Err(StorePullError::Database(
                "device join bootstrap history carries lifecycle authority outside the exact verified membership state"
                    .to_string(),
            ));
        }
        let authority = match verified_authorization {
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
        let registrations = Box::pin(load_commit_registrations(
            storage,
            root,
            &commit,
            &author,
            carries_lifecycle.then_some(&authority),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let resolver = DeviceStateResolver::Loaded {
            genesis: &genesis,
            states: &states,
        };
        let device_operations = load_commit_device_operations(
            Some(&resolver),
            storage,
            root,
            &commit,
            &predecessor_state,
            carries_lifecycle.then_some(&authority),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let mut state = device_operations
            .apply_to(predecessor_state, &commit.device_state)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        for activation in commit.device_registrations() {
            state = state
                .activate_registration(activation.registration.clone(), None)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        }
        for retirement in commit.device_retirements() {
            state = state
                .self_retire(retirement.clone())
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        }
        states.insert(reference.clone(), state);
        ordered.push(DeviceJoinBootstrapCommit {
            reference,
            commit,
            registrations,
            device_operations,
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
        || commit.membership_authority.is_some()
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
    let registrations = Box::pin(load_commit_registrations(
        storage,
        root,
        &commit,
        &author,
        Some(&authority),
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
        Database::record_materialized_commit_on(&tx, &commit, &commit_ref)?;
        tx.commit().map_err(DbError::from)
    })
    .await?;
    Ok(())
}

async fn load_authorized_serial_prefix(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    tip: Option<StoreBatchCommitRef>,
) -> Result<(Vec<AuthorizedSerialCommit>, SerialAuthorizationState), StorePullError> {
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
        let (commit, author) = load_commit_with_author(storage, root, &reference).await?;
        expected = commit.order.predecessor().cloned();
        reverse.push((reference, commit, author));
    }
    reverse.reverse();

    let founder = load_founder_registration(storage, root).await?;
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

        let recovery_author =
            commit
                .serial_recovery_activation()
                .as_ref()
                .is_some_and(|activation| {
                    activation.registration.registration == commit.author_registration
                });
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
        if !device_state_has_active_registration(&device_state, &commit.author_registration)
            && !recovery_author
        {
            return Err(StorePullError::Serial(format!(
                "Serial commit {} author registration is not active at its predecessor",
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
        let registrations = load_commit_registrations(
            storage,
            root,
            &commit,
            &author,
            Some(&predecessor_authority),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
        })?;
        let device_operations = load_commit_device_operations(
            None,
            storage,
            root,
            &commit,
            &device_state,
            Some(&predecessor_authority),
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Serial(error),
        })?;
        validate_serial_provider_admin_control(storage, root, &root_value, commit.control())
            .await?;
        authorization = authorization
            .authorize_and_apply(&reference, &commit, &author)
            .map_err(|error| {
                StorePullError::Serial(format!(
                    "commit {} authorization: {error}",
                    reference.coord.sequence()
                ))
            })?;
        device_state = device_operations
            .apply_to(device_state, &commit.device_state)
            .map_err(|error| StorePullError::Serial(error.to_string()))?;
        for activated in commit.device_registrations() {
            let (registration, authority) = registrations
                .iter()
                .find(|(registration, _)| {
                    registration.device_id == activated.registration.device_id
                })
                .ok_or_else(|| {
                    StorePullError::Serial(
                        "verified registration activation disappeared before device reduction"
                            .to_string(),
                    )
                })?;
            let recovery = match (&registration.origin, authority) {
                (
                    StoreDeviceRegistrationOrigin::Recovery { owner_grant, .. },
                    StoreDeviceRegistrationActivation::Recovery { node, .. },
                ) => Some(OwnerRecoveryCursor {
                    owner_grant: owner_grant.clone(),
                    position: OwnerRecoveryPosition::At { node: node.clone() },
                }),
                (
                    StoreDeviceRegistrationOrigin::Join { .. },
                    StoreDeviceRegistrationActivation::Join { .. },
                )
                | (StoreDeviceRegistrationOrigin::Founder { .. }, _) => None,
                _ => {
                    return Err(StorePullError::Serial(
                        "verified registration origin differs from its activation authority"
                            .to_string(),
                    ))
                }
            };
            device_state = device_state
                .activate_registration(activated.registration.clone(), recovery)
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
        }
        for retirement in commit.device_retirements() {
            device_state = device_state
                .self_retire(retirement.clone())
                .map_err(|error| StorePullError::Serial(error.to_string()))?;
        }
        predecessor = Some(reference.clone());
        authorized.push(AuthorizedSerialCommit {
            commit_ref: reference,
            commit,
            author,
            registrations,
            device_operations,
            authorization_after: authorization.clone(),
        });
    }
    Ok((authorized, authorization))
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

async fn load_authorized_serial_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    head: &StoreSerialHead,
) -> Result<Vec<AuthorizedSerialCommit>, StorePullError> {
    let tip = match &head.state {
        StoreSerialHeadState::Genesis {
            root: head_root, ..
        } => {
            if head_root != root {
                return Err(StorePullError::Serial(
                    "Serial genesis head names another exact Store root".to_string(),
                ));
            }
            None
        }
        StoreSerialHeadState::Commit { commit, .. } => Some(commit.clone()),
    };
    let (authorized, _) = load_authorized_serial_prefix(storage, root, tip.clone()).await?;
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
    Advanced(super::remote_object::SerialAcceptedSuffix),
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
        super::remote_object::SerialAcceptedSuffix {
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
    let (_, authorization) = load_authorized_serial_prefix(storage, root, reference).await?;
    Ok(authorization)
}

pub(crate) async fn load_serial_snapshot_authorities_at_position(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: Option<StoreBatchCommitRef>,
) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
    let (authorized, authorization) =
        load_authorized_serial_prefix(storage, root, reference).await?;
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
    let authorized_chain = load_authorized_serial_chain(storage, root, &head).await?;
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
        let circle_activations = match load_pull_circle_activations(
            db,
            storage,
            root,
            &authorized.commit_ref,
            &authorized.commit,
            &authorized.author,
            identity,
        )
        .await
        {
            Ok(activations) => activations,
            Err(PullCircleActivationError::Database(error)) => return Err(error.into()),
            Err(PullCircleActivationError::Invalid(error)) => {
                return Err(StorePullError::Serial(error));
            }
        };
        candidates.push((
            Candidate {
                commit_ref: authorized.commit_ref,
                commit: authorized.commit,
                author: authorized.author,
                package,
                registrations: authorized.registrations,
                circle_activations,
                device_operations: CandidateDeviceOperations::Verified(
                    authorized.device_operations,
                ),
            },
            authorized.authorization_after,
        ));
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
    for (candidate, authorization_after) in &candidates {
        let changes = match apply_serial_candidate(
            db,
            storage,
            store_dir,
            schema.clone(),
            candidate,
            authorization_after,
        )
        .await
        {
            Ok(changes) => changes,
            Err(StorePullError::BlobDownloads(failures)) if !failures.has_transport_failure() => {
                tracing::warn!(
                    stream_id = %commit_stream_id(&candidate.commit_ref.coord),
                    seq = candidate.commit_ref.coord.sequence(),
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
                        &candidate.commit_ref,
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
        authors.insert(candidate.author.device_id);
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
    for authorized in authorized_chain.into_iter().skip(first) {
        let package =
            load_serial_store_package(db, storage, &authorized.commit_ref, &authorized.commit)
                .await?;
        let circle_activations = match load_pull_circle_activations(
            db,
            storage,
            &root,
            &authorized.commit_ref,
            &authorized.commit,
            &authorized.author,
            Some(identity),
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
            circle_activations,
            device_operations: CandidateDeviceOperations::Verified(authorized.device_operations),
        };
        let prepared = prepare_serial_candidate(
            db,
            storage,
            store_dir,
            schema.clone(),
            &candidate,
            &prior_circle_accesses,
        )
        .await?;
        for (key, access) in circle_package_accesses(&candidate.circle_activations)
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
        commits.push(SerialResolutionCommit {
            commit: candidate.commit,
            commit_ref: candidate.commit_ref,
            packages: prepared.packages,
            changesets: prepared.changesets,
            registrations: candidate.registrations,
            circle_activations: candidate.circle_activations,
            device_operations,
            authorization_after: authorized.authorization_after,
        });
    }
    Ok(SerialResolutionPlan {
        head,
        head_object: verified_head.object,
        commits,
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
    let targets = db.merge_candidate_cleanup_targets(write_id).await?;
    for target in targets {
        super::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    Ok(())
}

async fn apply_serial_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    candidate: &Candidate,
    authorization_after: &SerialAuthorizationState,
) -> Result<Vec<RowChange>, StorePullError> {
    let device_operations = match &candidate.device_operations {
        CandidateDeviceOperations::Verified(operations) => operations.clone(),
        CandidateDeviceOperations::MergePending { .. } => {
            return Err(StorePullError::Serial(
                "Serial candidate carries unresolved Merge device operations".to_string(),
            ))
        }
    };
    let no_prior_circle_accesses = CirclePackageAccesses::new();
    let prepared = prepare_serial_candidate(
        db,
        storage,
        store_dir,
        schema.clone(),
        candidate,
        &no_prior_circle_accesses,
    )
    .await?;
    let resolution = SerialResolutionCommit {
        commit: candidate.commit.clone(),
        commit_ref: candidate.commit_ref.clone(),
        packages: prepared.packages,
        changesets: prepared.changesets,
        registrations: candidate.registrations.clone(),
        circle_activations: candidate.circle_activations.clone(),
        device_operations,
        authorization_after: authorization_after.clone(),
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
        &resolution.circle_activations,
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
    let inactive_circles = resolution
        .circle_activations
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
        local_cleanup::record_if_unreferenced_on(conn, blob_decls, &intent)?;
    }
    Database::record_materialized_serial_commit_with_device_operations_on(
        conn,
        &resolution.commit,
        &resolution.commit_ref,
        &resolution.authorization_after,
        &resolution.device_operations,
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
        &candidate.circle_activations,
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
    let Some(chain) = membership else {
        return true;
    };
    let Some(authority) = commit.membership_authority.as_ref() else {
        return chain.contains_member_now(&author.author_pubkey);
    };
    chain.authorizes_write_authority(authority, &author.author_pubkey)
}

fn carries_circle_payload(commit: &StoreBatchCommit) -> bool {
    !commit.circle_controls().is_empty() || !commit.circle_packages().is_empty()
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
) -> Result<Vec<super::circle_ops::VerifiedCircleReference>, PullCircleActivationError> {
    if !carries_circle_payload(commit) {
        return Ok(Vec::new());
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
    super::circle_ops::load_circle_activations(
        storage, root, commit_ref, commit, author, identity, &founder,
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
        let target_stream =
            super::membership::AuthorStreamId::store_announcements(root, &record.registration);
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
        let target_stream =
            super::membership::AuthorStreamId::store_announcements(root, &freeze.proposal.target);
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
    candidate: &Candidate,
) -> Result<ApplyOutcome, StorePullError> {
    let device_operations =
        resolve_candidate_device_operations(db, storage, root, candidate).await?;
    let circle_packages = match load_applicable_circle_packages(
        db,
        storage,
        &candidate.commit_ref,
        &candidate.commit,
        &candidate.circle_activations,
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
    let outcome = commit_candidate(db, candidate, packages, device_operations).await?;
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

struct PreparedMergeCandidatePackage {
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
) -> Result<Result<PreparedMergeCandidatePackage, HeldStorePositionReason>, StorePullError> {
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
    Ok(Ok(PreparedMergeCandidatePackage {
        package,
        changeset,
        cleanup,
    }))
}

async fn commit_candidate(
    db: &Database,
    candidate: &Candidate,
    packages: Vec<PreparedMergeCandidatePackage>,
    device_operations: VerifiedStoreDeviceOperations,
) -> Result<ApplyOutcome, StorePullError> {
    let commit = candidate.commit.clone();
    let commit_ref = candidate.commit_ref.clone();
    let registrations = candidate.registrations.clone();
    let circle_activations = candidate.circle_activations.clone();
    let receiver_wall_ms = db.receive_wall_ms();
    let blob_decls = db.blob_decls();
    let gates = db.gates();
    let synced_tables = db.synced_tables().to_vec();
    let inactive_circles = circle_activations
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
    let hlc = db.hlc();
    let outcome = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let mut returned_changes = Vec::new();
            let mut package_reported_fk_violation = false;
            Database::record_activated_store_device_registrations_on(&tx, &commit, &registrations)?;
            Database::record_verified_circle_activations_on(
                &tx,
                &commit,
                &commit_ref,
                &circle_activations,
            )?;
            for prepared in packages {
                let PreparedMergeCandidatePackage {
                    package,
                    changeset,
                    cleanup,
                    ..
                } = prepared;
                let applied_bytes = match package.audience() {
                    PackageAudience::Store => package.changeset().to_vec(),
                    PackageAudience::Circle { circle_id, .. } => {
                        super::gate::filter_inbound_circle_changeset(
                            &tx,
                            package.changeset(),
                            *circle_id,
                            &gates,
                        )
                        .map_err(|error| DbError::Message(error.to_string()))?
                    }
                };
                let applied_changeset = changeset
                    .validate_subset(applied_bytes.clone())
                    .map_err(|error| DbError::Message(error.to_string()))?;
                let actual_changes =
                    crate::changeset::walk(&applied_bytes).map_err(DbError::Message)?;
                advance_max_updated_at(
                    &mut changeset_max,
                    &actual_changes,
                    changeset.schema(),
                    receiver_wall_ms,
                );
                returned_changes.extend(
                    actual_changes
                        .iter()
                        .filter(|change| !super::gate::is_routing_table(&change.table))
                        .cloned(),
                );
                let apply = resolve_and_apply_changeset_with_schema_on(
                    &tx,
                    applied_changeset,
                    receiver_wall_ms,
                )?;
                if !apply.constraint_conflict_tables.is_empty() {
                    tx.rollback().map_err(DbError::from)?;
                    return Ok(ApplyOutcome::Held(
                        HeldStorePositionReason::ConstraintConflict(
                            apply.constraint_conflict_tables,
                        ),
                    ));
                }
                package_reported_fk_violation |= apply.had_fk_violations;
                let winning_rows = crate::sync::apply::current_winning_rows_with_schema(
                    &tx,
                    changeset.schema(),
                    &applied_bytes,
                )?;
                Database::install_pulled_blob_activations_on(&tx, &package, &commit_ref)?;
                Database::install_winning_blob_bindings_on(
                    &tx,
                    &gates,
                    &synced_tables,
                    &package,
                    &BlobActivation {
                        coord: commit_ref.coord.clone(),
                    },
                    &winning_rows,
                )?;
                for intent in cleanup {
                    local_cleanup::record_if_unreferenced_on(&tx, &blob_decls, &intent)?;
                }
            }
            let mut removal_session =
                rusqlite::session::Session::new(&tx).map_err(DbError::from)?;
            for table in &synced_tables {
                removal_session
                    .attach(Some(table.name()))
                    .map_err(DbError::from)?;
            }
            super::gate::prune_ineligible_scoped_rows(&tx, &gates, &inactive_circles)
                .map_err(|error| DbError::Message(error.to_string()))?;
            let mut removal_changeset = Vec::new();
            removal_session
                .changeset_strm(&mut removal_changeset)
                .map_err(DbError::from)?;
            drop(removal_session);
            let removed =
                crate::changeset::walk_old(&removal_changeset).map_err(DbError::Message)?;
            let removal_cleanup = local_blob_cleanup_intents(&blob_decls, &removed, &[])
                .map_err(|error| DbError::Message(error.to_string()))?;
            returned_changes
                .extend(crate::changeset::walk(&removal_changeset).map_err(DbError::Message)?);
            for intent in removal_cleanup {
                local_cleanup::record_if_unreferenced_on(&tx, &blob_decls, &intent)?;
            }
            if package_reported_fk_violation {
                let violations: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(DbError::from)?;
                if violations {
                    tx.rollback().map_err(DbError::from)?;
                    return Ok(ApplyOutcome::Held(
                        HeldStorePositionReason::ForeignKeyDependency,
                    ));
                }
            }
            Database::record_materialized_commit_with_device_operations_on(
                &tx,
                &commit,
                &commit_ref,
                &device_operations,
            )?;
            tx.commit().map_err(DbError::from)?;
            if let Some(max_applied) = changeset_max.as_ref() {
                hlc.advance_past(max_applied);
            }
            Ok(ApplyOutcome::Applied(returned_changes))
        })
        .await?;
    Ok(outcome)
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
