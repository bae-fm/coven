use super::*;
use crate::keys::UserKeypair;
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{MembershipChain, MembershipStatus};
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::store::circle_controls::activation::VerifiedCircleActivations;
use crate::sync::store::owner::pull::*;
use crate::sync::store_commit::{
    ActivatedStoreDeviceRegistrationRef, DeviceJoinAttempt, DeviceJoinAttemptDecisionRef,
    DeviceJoinOutcomeBody, DeviceStreamAnchor, ObjectHash, OpenedRetainedMergeHistorySummary,
    OwnerRecoveryNode, OwnerRecoveryNodeRef, ResolvedStoreDeviceState,
    RetainedVerifiedMergeHistorySummary, RetainedVerifiedRegistration, StoreBatchCommit,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceProposalAck,
    StoreDeviceProposalState, StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreDeviceStatus, StoreHistoryCut,
    StoreProtocolError, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use crate::sync::store_commit::{
    DeviceJoinAttemptRef, DeviceJoinOutcome, DeviceJoinOutcomeRef, SnapshotMeta, StoreAck,
    StoreAckRef, StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProposalRef,
    StoreDeviceHeadRef, StoreSnapshotRef, VerifiedDeviceExclusionOutcome,
    VerifiedDeviceExclusionProposal,
};
use crate::sync::store_objects::{StoreObjectError, VerifiedObject};
use crate::sync::{
    causal_grants, membership as protocol_membership, provider, remote_object, store_commit,
    store_objects,
};
use std::collections::{BTreeMap, BTreeSet};

use super::{device_join, reclaim as store_reclaim};

pub(super) mod join_validation;
mod membership;
pub(super) mod registration;
use join_validation::*;
use registration::*;

pub(crate) async fn load_membership_at_exact_heads_with_verified_activations(
    commit_verifier: &StoreCommitVerifier<'_>,
    heads: &[protocol_membership::MembershipHeadRef],
    resolutions: &[protocol_membership::StoreMembershipConflictResolutionRef],
    verified_activations: &VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
    membership::load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
        commit_verifier,
        heads,
        resolutions,
        verified_activations,
        pending_resolution,
    )
    .await
}

pub(crate) async fn load_merge_predecessor_membership_with_history(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    state: &StoreMembershipStateRef,
) -> Result<MembershipChain, RegistrationLoadError> {
    Box::pin(history_verifier.load_membership_at_exact_heads(&state.heads, &state.resolutions))
        .await
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

pub(crate) async fn load_merge_predecessor_membership_with_verified_activations(
    commit_verifier: &StoreCommitVerifier<'_>,
    state: &StoreMembershipStateRef,
    verified_activations: &VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, RegistrationLoadError> {
    Box::pin(commit_verifier.load_membership_at_verified_prefix(
        &state.heads,
        &state.resolutions,
        verified_activations,
        pending_resolution,
    ))
    .await
    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

pub(crate) async fn load_merge_predecessor_membership_with_retained_history(
    history_verifier: &MergeHistoryVerifier<'_>,
    state: &StoreMembershipStateRef,
    verified_activations: &VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, RegistrationLoadError> {
    Box::pin(history_verifier.load_membership_at_verified_prefix(
        &state.heads,
        &state.resolutions,
        verified_activations,
        pending_resolution,
    ))
    .await
    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
}

pub(crate) fn verify_merge_membership_state_ref(
    state: &StoreMembershipStateRef,
    membership: &MembershipChain,
    device_state: &ResolvedStoreDeviceState,
) -> Result<(), StorePullError> {
    let MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StorePullError::Database(
            "Store history membership state is conflicted".to_string(),
        ));
    };
    let expected = StoreMembershipStateRef::from_parts(
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

pub(crate) fn verify_merge_owner(
    membership: &StoreMembershipStateRef,
    chain: &MembershipChain,
    owner_pubkey: &str,
    owner_grant: &crate::sync::membership::MembershipGrantId,
) -> bool {
    let MembershipStatus::Resolved(resolved) = chain.status() else {
        return false;
    };
    StoreMembershipStateRef::from_parts(
        chain.head_refs().to_vec(),
        chain.resolution_refs().to_vec(),
        membership.recovery().to_vec(),
        resolved.state_hash,
    )
    .is_ok_and(|expected| membership == &expected)
        && chain.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
}

pub(crate) fn verify_merge_provider_administrator(
    chain: &MembershipChain,
    grant_id: &crate::sync::provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
    expected: &crate::sync::provider::ProviderAdminGrantRecord,
) -> bool {
    let MembershipStatus::Resolved(resolved) = chain.status() else {
        return false;
    };
    let state = resolved.provider_admin.combined_state();
    state.authorizes(grant_id, executor) && state.records().get(grant_id) == Some(expected)
}

pub(crate) struct VerifiedMergeHistoryCommit {
    pub(crate) verified: VerifiedStoreBatchCommit,
    pub(crate) predecessor_membership: MembershipChain,
    pub(crate) predecessor_state: ResolvedStoreDeviceState,
    pub(crate) state_after: ResolvedStoreDeviceState,
    pub(crate) registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub(crate) operations: VerifiedStoreDeviceOperations,
    pub(crate) acknowledgement: Option<(store_commit::StoreAckRef, store_commit::StoreAck)>,
    pub(crate) membership_control: Option<VerifiedMergeMembershipControl>,
    pub(crate) activation_head: StoreDeviceHead,
    pub(crate) activation_head_object: ExactObjectRef,
    pub(crate) history: OpenedRetainedMergeHistorySummary,
}

#[derive(Clone, Copy)]
pub(crate) struct VerifiedMergePredecessorHistory<'a> {
    commits: &'a BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    frontier: &'a [StoreBatchCommitRef],
}

impl<'a> VerifiedMergePredecessorHistory<'a> {
    pub(crate) fn new(
        commits: &'a BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
        frontier: &'a [StoreBatchCommitRef],
    ) -> Self {
        Self { commits, frontier }
    }

    pub(crate) fn find(
        &self,
        mut matches: impl FnMut(&StoreBatchCommitRef, &StoreBatchCommit) -> bool,
    ) -> Result<Option<&'a VerifiedMergeHistoryCommit>, StorePullError> {
        let mut pending = self.frontier.to_vec();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let verified = self.commits.get(&reference).ok_or_else(|| {
                StorePullError::Database(
                    "verified Merge predecessor graph is missing an exact commit".to_string(),
                )
            })?;
            if matches(&reference, verified.verified.value()) {
                return Ok(Some(verified));
            }
            pending.extend(commit_predecessor_references(verified.verified.value()));
        }
        Ok(None)
    }
}

fn verify_commit_join_evidence<'a>(
    commit: &'a StoreBatchCommit,
    loaded: LoadedCommitJoinEvidence,
    accepted: VerifiedMergePredecessorHistory<'a>,
) -> StorePullFuture<'a, VerifiedCommitJoinEvidence> {
    Box::pin(async move {
        if loaded.attempts.is_empty() {
            return Ok(VerifiedCommitJoinEvidence {
                commit: commit.clone(),
                attempts: BTreeMap::new(),
                cleanup_receipts: loaded.cleanup_receipts,
            });
        }
        let mut attempts = BTreeMap::new();
        for (reference, evidence) in loaded.attempts {
            let access = &evidence.attempt.value.provider_approval.access_grant;
            let verified = accepted
                .find(|candidate, _| candidate == &access.activation)?
                .ok_or_else(|| {
                    StorePullError::Database(
                        "provider-access activation is outside the accepted Merge predecessor graph"
                            .to_string(),
                    )
                })?;
            if !verify_merge_provider_administrator(
                &verified.predecessor_membership,
                &access.grant.administrator_grant,
                &verified.verified.value().author_registration,
                &evidence
                    .attempt
                    .value
                    .provider_approval
                    .request
                    .offer
                    .provider_admin,
            ) {
                return Err(StorePullError::Database(
                    "device join attempt lacks exact Merge provider-administrator authority"
                        .to_string(),
                ));
            }
            attempts.insert(reference, evidence.attempt.value);
        }
        Ok(VerifiedCommitJoinEvidence {
            commit: commit.clone(),
            attempts,
            cleanup_receipts: loaded.cleanup_receipts,
        })
    })
}

pub(crate) struct VerifiedMergeHistoryAuthority {
    pub(crate) device_state: ResolvedStoreDeviceState,
    pub(crate) membership: MembershipChain,
}

struct VerifiedMergeSnapshotState {
    common: VerifiedSnapshotState,
    membership: MembershipChain,
    checkpoints: Vec<OpenedRetainedMergeHistorySummary>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeMembershipHeadActivation {
    commit: StoreBatchCommitRef,
    transition: protocol_membership::MergeMembershipHeadTransition,
}

impl VerifiedMergeMembershipHeadActivation {
    pub(crate) fn verifies(
        &self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> bool {
        &self.commit == commit && self.transition.matches_head(head, reference)
    }
}

pub(crate) struct VerifiedMergeMembershipControl {
    pub(crate) activations: VerifiedCircleActivations,
    head_activation: VerifiedMergeMembershipHeadActivation,
    conflict_resolution: Option<VerifiedMergeConflictResolutionActivation>,
}

impl VerifiedMergeMembershipControl {
    pub(crate) fn verifies_head_activation(
        &self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> bool {
        self.head_activation.verifies(reference, head, commit)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeConflictResolutionActivation {
    reference: protocol_membership::StoreMembershipConflictResolutionRef,
}

impl VerifiedMergeConflictResolutionActivation {
    pub(crate) fn reference(&self) -> &protocol_membership::StoreMembershipConflictResolutionRef {
        &self.reference
    }

    pub(crate) fn verifies(
        &self,
        reference: &protocol_membership::StoreMembershipConflictResolutionRef,
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
        protocol_membership::StoreMembershipConflictResolutionRef,
        VerifiedMergeConflictResolutionActivation,
    >,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VerifiedMergePrefixHeadStatus {
    Included,
    OutsidePrefix,
}

impl VerifiedMergeMembershipPrefix {
    pub(crate) fn from_retained(
        checkpoints: &[OpenedRetainedMergeHistorySummary],
    ) -> Result<Self, StorePullError> {
        let mut prefix = Self::default();
        for checkpoint in checkpoints {
            for reference in checkpoint.summary.causal_cut.values() {
                prefix.commits.insert(reference.clone());
            }
            for proof in checkpoint.summary.membership_proofs.values() {
                let Some(store_commit::StoreControl { transition }) = proof.commit_value.control()
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
                                "retained checkpoints disagree on a conflict resolution"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(prefix)
    }

    pub(crate) fn head_activation(
        &self,
        commit: &StoreBatchCommitRef,
    ) -> Option<&VerifiedMergeMembershipHeadActivation> {
        self.head_activations.get(commit)
    }

    pub(crate) fn verifies_conflict_resolution(
        &self,
        reference: &protocol_membership::StoreMembershipConflictResolutionRef,
    ) -> bool {
        self.conflict_resolutions
            .get(reference)
            .is_some_and(|proof| proof.verifies(reference))
    }

    pub(crate) fn classify_head(
        &self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
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

pub(crate) struct VerifiedOwnerPromotionRequestActivation {
    activation: store_commit::OwnerPromotionRequestActivation,
}

impl VerifiedOwnerPromotionRequestActivation {
    pub(crate) fn activation(&self) -> &store_commit::OwnerPromotionRequestActivation {
        &self.activation
    }
}

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn verify_resolution_activation_acceptance(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeConflictResolutionActivation>, StorePullError> {
        verify_merge_resolution_activation_acceptance_with_history(
            &self.commit_verifier,
            commit,
            &self.history.genesis,
            &self.history.commits,
        )
        .await
    }

    pub(crate) async fn verify_membership_control_with_retained_history(
        &mut self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        predecessor_membership: &MembershipChain,
        predecessor_state: &ResolvedStoreDeviceState,
        pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
    ) -> Result<
        (
            VerifiedCircleActivations,
            Option<VerifiedMergeConflictResolutionActivation>,
        ),
        String,
    > {
        verify_merge_membership_control_with_history(
            &mut self.commit_verifier,
            commit_ref,
            commit,
            predecessor_membership,
            predecessor_state,
            &self.history.commits,
            pending_resolution,
        )
        .await
    }

    pub(crate) async fn verified_membership_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
        verified_merge_membership_objects(&self.commit_verifier, commit_ref, commit).await
    }

    pub(crate) async fn verify_owner_recovery_activation(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<
        Option<(
            protocol_membership::MembershipGrantId,
            store_commit::OwnerRecoveryActivationId,
        )>,
        StorePullError,
    > {
        verify_commit_owner_recovery_activation(&self.commit_verifier, commit).await
    }

    pub(crate) async fn retain_acknowledgement(
        &self,
        activating_commit: &StoreBatchCommitRef,
        activating_commit_value: &StoreBatchCommit,
        registration: &StoreDeviceRegistration,
        reference: StoreAckRef,
        value: StoreAck,
    ) -> Result<store_commit::RetainedVerifiedActivatedAck, StorePullError> {
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
        let chain = self
            .load_acknowledgement_proof_chain(reference, value, registration)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
        Ok(store_commit::RetainedVerifiedActivatedAck {
            chain,
            activating_commit: activating_commit.clone(),
            activating_commit_value: activating_commit_value.clone(),
        })
    }

    pub(crate) async fn load_acknowledgement_proof_chain(
        &self,
        latest_ref: StoreAckRef,
        latest: StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<BTreeMap<u64, (StoreAckRef, StoreAck)>, RegistrationLoadError> {
        load_acknowledgement_proof_chain(self, latest_ref, latest, registration).await
    }

    pub(crate) async fn verify_canonical_owner_registration(
        &self,
        state: &ResolvedStoreDeviceState,
        owner_pubkey: &str,
        selected: &StoreDeviceRegistrationRef,
    ) -> Result<(), StorePullError> {
        verify_canonical_owner_registration(&self.commit_verifier, state, owner_pubkey, selected)
            .await
    }

    pub(crate) async fn load_local_device_operations(
        &mut self,
        database: &StoreDatabase,
        verified_commit: &VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        state_ref: &StoreDeviceStateRef,
        state: ResolvedStoreDeviceState,
    ) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
        load_local_commit_device_operations(
            database,
            &mut self.commit_verifier,
            verified_commit,
            membership,
            state_ref,
            state,
        )
        .await
    }

    pub(crate) async fn derive_local_post_device_state(
        &self,
        commit: &StoreBatchCommit,
        predecessor_state: ResolvedStoreDeviceState,
        registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
        device_operations: VerifiedStoreDeviceOperations,
    ) -> Result<ResolvedStoreDeviceState, StorePullError> {
        derive_local_post_device_state(
            &self.commit_verifier,
            commit,
            predecessor_state,
            registrations,
            device_operations,
        )
        .await
    }

    pub(crate) async fn install_existing_founder_device(
        &self,
        database: &StoreDatabase,
        identity: &UserKeypair,
    ) -> Result<(), super::registration::StoreRegistrationError> {
        super::registration::install_existing_founder_device(
            database,
            &self.commit_verifier,
            identity,
        )
        .await
    }

    pub(crate) async fn load_head(
        &self,
        reference: &StoreDeviceHeadRef,
        registration: &StoreDeviceRegistration,
        commit: &StoreBatchCommitRef,
    ) -> Result<VerifiedObject<StoreDeviceHead>, StoreObjectError> {
        self.commit_verifier
            .load_head(reference, registration, commit)
            .await
    }

    pub(crate) async fn readiness(
        &mut self,
        database: &StoreDatabase,
        coverage: &CommitFrontier,
        frontier: &BTreeMap<String, StoreBatchCommitRef>,
        device_state: &ResolvedStoreDeviceState,
        exclusion_freezes: &[StoreDeviceProposalAck],
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Readiness, StorePullError> {
        readiness(
            database,
            self,
            coverage,
            frontier,
            device_state,
            exclusion_freezes,
            commit_ref,
            commit,
        )
        .await
    }

    pub(crate) async fn history_cut_covers(
        &mut self,
        cut: &StoreHistoryCut,
        target: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        let Some(covering) = cut.0.get(&target.coord.stream_id) else {
            return Ok(false);
        };
        self.commit_position_covers(covering, target)
            .await
            .map_err(|error| match error {
                CommitCoverageError::Object(error) => StorePullError::Object(error),
                CommitCoverageError::MissingAncestry { commit_hash } => StorePullError::Database(
                    format!("exact Store ancestry is missing commit {commit_hash}"),
                ),
            })
    }

    pub(crate) async fn find_owner_promotion_request_activation(
        &mut self,
        request: &store_commit::OwnerPromotionRequest,
    ) -> Result<VerifiedOwnerPromotionRequestActivation, StorePullError> {
        let root = self.root().clone();
        let promoter = self
            .commit_verifier
            .load_registration(&request.promoter_registration)
            .await?;
        request
            .verify(&root, &promoter.value)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let discovered =
            discover_merge_stream(self, &request.promoter_registration, &promoter.value, None)
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
        self.verify_refs([commit.clone()]).await?;
        Ok(VerifiedOwnerPromotionRequestActivation {
            activation: store_commit::OwnerPromotionRequestActivation { commit, head },
        })
    }

    pub(crate) async fn verify_owner_promotion_acceptance_with_history(
        &mut self,
        acceptance: &store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), StorePullError> {
        let store_commit::OwnerPromotionRequestActivation {
            commit: activation_commit,
            ..
        } = &acceptance.activation;
        self.verify_refs([activation_commit.clone()]).await?;
        let commit_verifier = &mut self.commit_verifier;
        let history = &self.history;
        verify_merge_owner_promotion_acceptance_with_history(
            commit_verifier,
            acceptance,
            &history.commits,
        )
        .await
    }

    pub(crate) async fn verify_owner_promotion_acceptance_from_request_activation(
        &mut self,
        acceptance: &store_commit::OwnerPromotionAcceptance,
        verified: VerifiedOwnerPromotionRequestActivation,
    ) -> Result<(), StorePullError> {
        if acceptance.activation != verified.activation {
            return Err(StorePullError::Database(
                "Owner-promotion acceptance names another request activation".to_string(),
            ));
        }
        let commit_verifier = &mut self.commit_verifier;
        let history = &self.history;
        verify_merge_owner_promotion_acceptance_with_history(
            commit_verifier,
            acceptance,
            &history.commits,
        )
        .await
    }

    pub(crate) async fn verify_accepted_provider_access_activation(
        &mut self,
        access: &crate::sync::provider::ActivatedStoreMemberProviderAccessGrant,
        provider_admin: &crate::sync::provider::ProviderAdminGrantRecord,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), StorePullError> {
        let activation = load_provider_access_activation(self, access, administrator).await?;
        let membership = load_merge_predecessor_membership_with_history(
            self,
            &activation.value().membership_state,
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        if !verify_merge_provider_administrator(
            &membership,
            &access.grant.administrator_grant,
            &activation.value().author_registration,
            provider_admin,
        ) {
            return Err(StorePullError::Database(
                "device provider approval activation lacks exact predecessor provider-administrator authority"
                    .to_string(),
            ));
        }
        if !self
            .current_history_contains(&membership, &access.activation)
            .await?
        {
            return Err(StorePullError::Database(
                "device provider approval activation is absent from current accepted Store history"
                    .to_string(),
            ));
        }
        Ok(())
    }

    async fn current_history_contains(
        &mut self,
        membership: &MembershipChain,
        expected: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        self.verify_refs([expected.clone()]).await?;
        let mut state = self
            .history()
            .commits
            .get(expected)
            .ok_or_else(|| {
                StorePullError::Database(
                    "provider-access activation is absent from its verified Merge graph"
                        .to_string(),
                )
            })?
            .state_after
            .clone();
        let mut registrations = BTreeMap::new();
        let founder = self.commit_verifier.load_founder_registration().await?;
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object);
        registrations.insert(founder_ref.device_id, (founder_ref, founder.value));
        for recovered in discover_merge_owner_recoveries(self, membership).await? {
            registrations.insert(recovered.0.device_id, recovered);
        }
        self.load_state_registrations(&state, &mut registrations)
            .await?;

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
                    discover_merge_stream(self, registration_ref, registration, inactive_cut)
                        .await?;
                if matches!(discovered.block, Some(MergeStreamBlock::Authenticated(_))) {
                    return Err(StorePullError::Database(
                        "an authenticated Merge stream position cannot be verified".to_string(),
                    ));
                }
                if let Some((_, _, reference, _)) = discovered.commits.last() {
                    let stream_id = reference.coord.stream_id;
                    next.insert(stream_id, reference.clone());
                }
            }
            self.verify_refs(next.values().cloned()).await?;
            let next_state = if next.is_empty() {
                self.history().genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    next.values()
                        .map(|reference| {
                            self.history()
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
            self.load_state_registrations(&next_state, &mut registrations)
                .await?;
            let stable = next == accepted
                && next_state == state
                && registrations.len() == registration_count;
            if stable {
                return Ok(self.history().commits.contains_key(expected));
            }
            let state_fingerprint = ObjectHash::digest(
                &serde_json::to_vec(&(&next, &next_state))
                    .map_err(|error| StorePullError::Database(error.to_string()))?,
            );
            if !observed_states.insert(state_fingerprint) {
                return Err(StorePullError::Database(
                    "current Merge authority discovery does not reach one stable frontier"
                        .to_string(),
                ));
            }
            accepted = next;
            state = next_state;
        }
    }

    async fn load_state_registrations(
        &self,
        state: &ResolvedStoreDeviceState,
        registrations: &mut BTreeMap<
            store_commit::StoreDeviceId,
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
            let registration = self
                .commit_verifier
                .load_registration(&record.registration)
                .await?;
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

    pub(crate) async fn verify_device_join_cleanup_activation(
        &mut self,
        activation: LoadedDeviceJoinCleanupActivation,
    ) -> Result<crate::sync::store::JoinerJoinTerminal, StorePullError> {
        let membership = load_merge_predecessor_membership_with_history(
            self,
            &activation.verified_commit.value().membership_state,
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        if !membership.is_owner_now(&activation.verified_commit.author().author_pubkey) {
            return Err(StorePullError::Database(
                "device join cleanup activation author is not an active Merge Owner".to_string(),
            ));
        }
        let [loaded] = <[_; 1]>::try_from(activation.receipts).map_err(|_| {
            StorePullError::Database(
                "device join cleanup activation does not resolve to one verified receipt"
                    .to_string(),
            )
        })?;
        let attempt = self
            .verify_device_join_attempt_evidence(loaded.attempt)
            .await?;
        let expected = &attempt.value.provider_approval.request.offer.provider_admin;
        if !verify_merge_provider_administrator(
            &membership,
            &loaded.receipt.provider_admin_grant,
            &loaded.receipt.executor,
            expected,
        ) {
            return Err(StorePullError::Database(
                "device join cleanup executor is not the effective Merge provider administrator"
                    .to_string(),
            ));
        }
        Ok(loaded.receipt.joiner_terminal)
    }
}

pub(crate) fn verified_merge_membership_prefix(
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
        pending.extend(commit_predecessor_references(verified.verified.value()));
    }
    Ok(closure)
}

fn merge_device_state_from_verified_history(
    reference: &StoreDeviceStateRef,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let frontier = reference.frontier();
    let allowed = verified_merge_commit_closure(commits, allowed_tips)?;
    if frontier
        .commits()
        .values()
        .any(|reference| !allowed.contains(reference))
    {
        return Err(StorePullError::Database(
            "Merge device state names a commit outside its causal predecessor history".to_string(),
        ));
    }
    let state = if frontier.commits().is_empty() {
        genesis.clone()
    } else {
        ResolvedStoreDeviceState::merge(
            frontier
                .commits()
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
    let expected = StoreDeviceStateRef::from_resolved(frontier.clone(), &state)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if &expected != reference {
        return Err(StorePullError::Database(
            "Merge device-state reference differs from its verified history".to_string(),
        ));
    }
    Ok(state)
}

async fn verify_merge_owner_conflict_acceptance_with_history(
    commit_verifier: &StoreCommitVerifier<'_>,
    acceptance: &store_commit::OwnerConflictResolutionAcceptance,
    resolver_pubkey: &str,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<(), StorePullError> {
    let registration = commit_verifier
        .load_registration(&acceptance.owner_registration)
        .await?;
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
        commit_verifier,
        &state,
        resolver_pubkey,
        &acceptance.owner_registration,
    )
    .await?;
    Ok(())
}

pub(crate) async fn verify_merge_resolution_activation_acceptance_with_history(
    commit_verifier: &StoreCommitVerifier<'_>,
    commit: &StoreBatchCommit,
    genesis: &ResolvedStoreDeviceState,
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
) -> Result<Option<VerifiedMergeConflictResolutionActivation>, StorePullError> {
    let storage = commit_verifier.storage();
    let root = commit_verifier.root();
    let Some(store_commit::StoreControl { transition }) = commit.control() else {
        return Ok(None);
    };
    let entry = store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &transition.body.entry,
    )
    .await?;
    let protocol_membership::MembershipChange::ResolutionActivation { resolution } =
        &entry.value.change
    else {
        return Ok(None);
    };
    if entry.value.coord() != transition.body.entry.coord {
        return Err(StorePullError::Database(
            "Merge resolution activation differs from its exact transition".to_string(),
        ));
    }
    let value =
        store_objects::load_membership_resolution_ref(storage, root.store_root_hash, resolution)
            .await?;
    let registration = commit_verifier
        .load_registration(&commit.author_registration)
        .await?;
    let acceptance = &value.value.replacement_acceptance;
    let mut expected_activations = vec![
        store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            acceptance.owner_registration.clone(),
            value.value.replacement_grant.clone(),
            acceptance.membership.clone(),
        ),
        store_commit::StreamActivation::grant_authorized(
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
        commit_verifier,
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

pub(crate) struct VerifiedMergeHistory {
    pub(crate) genesis: ResolvedStoreDeviceState,
    pub(crate) commits: BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
}

pub(crate) struct MergeHistoryVerifier<'a> {
    commit_verifier: StoreCommitVerifier<'a>,
    history: VerifiedMergeHistory,
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
    pub(crate) predecessor_head: Option<store_commit::StoreDeviceHeadRef>,
}

pub(crate) struct MergeHistorySuccessorEvidence {
    pub(crate) registrations: Vec<RetainedVerifiedRegistration>,
    pub(crate) acknowledgement: Option<store_commit::RetainedVerifiedActivatedAck>,
    pub(crate) membership_proof: Option<store_commit::RetainedMergeMembershipProof>,
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

pub(crate) fn insert_latest_acknowledgement(
    target: &mut BTreeMap<store_commit::StoreDeviceId, store_commit::RetainedVerifiedActivatedAck>,
    device_id: store_commit::StoreDeviceId,
    value: store_commit::RetainedVerifiedActivatedAck,
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
        protocol_membership::AuthorStreamId,
        store_commit::RetainedAcceptedStoreAnnouncement,
    >,
    stream_id: protocol_membership::AuthorStreamId,
    value: store_commit::RetainedAcceptedStoreAnnouncement,
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
    target: &mut BTreeMap<StoreBatchCommitRef, store_commit::RetainedMergeMembershipProof>,
    reference: StoreBatchCommitRef,
    value: store_commit::RetainedMergeMembershipProof,
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

pub(crate) struct MergedRetainedMergeHistory {
    causal_cut: BTreeMap<StoreCommitCoord, StoreBatchCommitRef>,
    registrations: BTreeMap<store_commit::StoreDeviceId, RetainedVerifiedRegistration>,
    acknowledgements:
        BTreeMap<store_commit::StoreDeviceId, store_commit::RetainedVerifiedActivatedAck>,
    membership_proofs: BTreeMap<StoreBatchCommitRef, store_commit::RetainedMergeMembershipProof>,
    announcement_frontier: BTreeMap<
        protocol_membership::AuthorStreamId,
        store_commit::RetainedAcceptedStoreAnnouncement,
    >,
}

pub(crate) fn merge_retained_merge_history(
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
        if predecessor.summary.store_root_hash != root.store_root_hash {
            return Err(StorePullError::Database(
                "Merge predecessor checkpoint belongs to another Store".to_string(),
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
                let stream_id = value.commit.coord.stream_id;
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

pub(crate) fn compose_merge_history_successor(
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
    let mut membership_floor = store_commit::MembershipCausalFloor::from_membership(membership);
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
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = reference.coord;
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
        version: store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        causal_cut,
        post_state: StoreDeviceStateRef::from_resolved(CommitFrontier(post_frontier), &state_after)
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
    let StoreCommitCoord {
        stream_id,
        sequence,
    } = commit_ref.coord;
    let predecessor_head = summary
        .announcement_frontier
        .get(&stream_id)
        .map(|accepted| accepted.reference.clone());
    let head_slot = match summary.announcement_frontier.get(&stream_id) {
        Some(accepted) => accepted.value.successor.next_slot.clone(),
        None => match &author.store_commits {
            DeviceStreamAnchor::StoreAnnouncements { first_slot } if sequence == 1 => {
                first_slot.clone()
            }
            _ => {
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

pub(crate) fn compose_merge_snapshot_history_summary(
    root: &StoreRootRef,
    coverage: &CommitFrontier,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
    author_ref: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    let frontier = &coverage.0;
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
        version: store_commit::STORE_PROTOCOL_VERSION,
        store_root_hash: root.store_root_hash,
        causal_cut,
        post_state: StoreDeviceStateRef::from_resolved(coverage.clone(), state)
            .map_err(|error| StorePullError::Database(error.to_string()))?,
        membership_floor: store_commit::MembershipCausalFloor::from_membership(membership),
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
    candidate: &VerifiedStoreBatchCommit,
    abandonment: &VerifiedStoreBatchCommit,
) -> Result<RetainedVerifiedMergeHistorySummary, StorePullError> {
    candidate_summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let candidate_value = candidate.value();
    let candidate = candidate.reference();
    let abandonment_value = abandonment.value();
    let abandonment = abandonment.reference();
    if candidate_summary.store_root_hash != candidate_value.store_root_hash
        || candidate_summary.store_root_hash != abandonment_value.store_root_hash
    {
        return Err(StorePullError::Database(
            "Merge abandonment history belongs to another Store root".to_string(),
        ));
    }
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
    let frontier = CommitFrontier(
        summary
            .frontier()
            .map_err(|error| StorePullError::Database(error.to_string()))?,
    );
    summary.post_state = candidate_summary
        .post_state
        .with_frontier(frontier)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    summary
        .validate_shape()
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    Ok(summary)
}

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn load_exact_anchored_membership(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
        owner: Option<&str>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        membership::load_exact_anchored_chain_with_history(self, heads, owner).await
    }

    pub(crate) async fn load_membership_at_exact_heads(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
        resolutions: &[protocol_membership::StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        membership::load_anchored_chain_at_exact_heads_with_history(self, heads, resolutions).await
    }

    pub(crate) async fn load_membership_at_verified_prefix(
        &self,
        heads: &[protocol_membership::MembershipHeadRef],
        resolutions: &[protocol_membership::StoreMembershipConflictResolutionRef],
        verified_activations: &VerifiedMergeMembershipPrefix,
        pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.commit_verifier
            .load_membership_at_verified_prefix(
                heads,
                resolutions,
                verified_activations,
                pending_resolution,
            )
            .await
    }

    pub(crate) async fn load_exact_membership_head(
        &mut self,
        reference: &protocol_membership::MembershipHeadRef,
    ) -> Result<protocol_membership::AuthorHead, crate::sync::store::membership::AnchoredChainError>
    {
        membership::load_exact_membership_head_with_history(self, reference).await
    }

    pub(crate) async fn project_membership_to_verified_prefix(
        &self,
        candidate_heads: &[protocol_membership::MembershipHeadRef],
        prefix: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        membership::project_anchored_chain_to_verified_store_prefix(
            &self.commit_verifier,
            candidate_heads,
            prefix,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn assert_deep_membership_projection(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
    ) {
        membership::assert_deep_valid_predecessor_path_is_iterative(self, heads).await;
    }

    pub(super) async fn new(
        storage: &'a dyn SyncStorage,
        root: &StoreRootRef,
    ) -> Result<Self, StorePullError> {
        let verified_root =
            crate::sync::store::protocol_root::load_pinned_store_protocol_root(storage, root)
                .await
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        let commit_verifier = StoreCommitVerifier::from_verified_root(storage, root, verified_root)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        Self::from_commit_verifier(commit_verifier).await
    }

    pub(super) async fn from_commit_verifier(
        commit_verifier: StoreCommitVerifier<'a>,
    ) -> Result<Self, StorePullError> {
        let founder = commit_verifier.load_founder_registration().await?;
        Self::from_commit_verifier_and_founder(commit_verifier, &founder)
    }

    fn from_commit_verifier_and_founder(
        commit_verifier: StoreCommitVerifier<'a>,
        founder: &VerifiedObject<StoreDeviceRegistration>,
    ) -> Result<Self, StorePullError> {
        let root = commit_verifier.root();
        let verified_root = commit_verifier.verified_root();
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        let founder_origin_matches = matches!(
            founder.value.origin,
            store_commit::StoreDeviceRegistrationOrigin::Founder { creation_id }
                if creation_id == verified_root.descriptor.creation_id
        );
        if founder.value.store_root != *root
            || founder.value.author_pubkey != verified_root.descriptor.founder_pubkey
            || founder.value.provider != verified_root.descriptor.founder_provider_admin.provider
            || founder.object.slot() != &verified_root.descriptor.founder_registration
            || founder.semantic_hash != founder_ref.registration_hash
            || !founder_origin_matches
        {
            return Err(StorePullError::Database(
                "verified founder registration belongs to another Store root".to_string(),
            ));
        }
        let genesis = ResolvedStoreDeviceState::founder(
            root,
            founder_ref,
            &verified_root.descriptor.founder_pubkey,
            verified_root.descriptor.founder_grant.clone(),
            &verified_root.descriptor.founder_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(Self {
            commit_verifier,
            history: VerifiedMergeHistory {
                genesis,
                commits: BTreeMap::new(),
            },
        })
    }

    pub(crate) async fn load_ref(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StorePullError> {
        if let Some(verified) = self.history.commits.get(reference) {
            return Ok(verified.verified.clone());
        }
        Ok(self.commit_verifier.load_ref(reference).await?)
    }

    pub(crate) async fn load_covered_commits(
        &mut self,
        coverage: &CommitFrontier,
    ) -> Result<Vec<(StoreBatchCommitRef, VerifiedStoreBatchCommit)>, StorePullError> {
        let mut commits = BTreeMap::new();
        for tip in coverage.0.values() {
            let mut cursor = Some(tip.clone());
            while let Some(reference) = cursor {
                if commits.contains_key(&reference) {
                    break;
                }
                let commit = self.load_ref(&reference).await?;
                cursor = commit.value().order.predecessor().cloned();
                commits.insert(reference, commit);
            }
        }
        Ok(commits.into_iter().collect())
    }

    pub(crate) async fn commit_position_covers(
        &mut self,
        covering: &StoreBatchCommitRef,
        covered: &StoreBatchCommitRef,
    ) -> Result<bool, CommitCoverageError> {
        if covering.coord.stream_id != covered.coord.stream_id
            || covering.coord.sequence() < covered.coord.sequence()
        {
            return Ok(false);
        }
        let mut cursor = covering.clone();
        while cursor.coord.sequence() > covered.coord.sequence() {
            let commit = self.commit_verifier.load_ref(&cursor).await?;
            cursor = commit.value().order.predecessor().cloned().ok_or(
                CommitCoverageError::MissingAncestry {
                    commit_hash: cursor.commit_hash,
                },
            )?;
        }
        Ok(cursor == *covered)
    }

    pub(crate) async fn verify_currently_materialized(
        &mut self,
        database: &StoreDatabase,
        reference: &StoreBatchCommitRef,
    ) -> Result<(), StorePullError> {
        verify_merge_commit_currently_materialized(database, self, reference).await
    }

    pub(crate) async fn authenticate_bytes(
        &mut self,
        reference: &StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        self.commit_verifier
            .authenticate_bytes(reference, bytes)
            .await
    }

    pub(crate) async fn load_registration(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        self.commit_verifier.load_registration(reference).await
    }

    pub(crate) async fn load_founder_registration(
        &self,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        self.commit_verifier.load_founder_registration().await
    }

    pub(crate) async fn load_device_join_attempt_and_owner(
        &self,
        reference: &DeviceJoinAttemptRef,
    ) -> Result<
        (
            VerifiedObject<DeviceJoinAttempt>,
            VerifiedObject<StoreDeviceRegistration>,
        ),
        StoreObjectError,
    > {
        self.commit_verifier
            .load_device_join_attempt_and_owner(reference)
            .await
    }

    pub(crate) async fn load_device_join_outcome(
        &self,
        reference: &DeviceJoinOutcomeRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<DeviceJoinOutcome>, StoreObjectError> {
        self.commit_verifier
            .load_device_join_outcome(reference, owner)
            .await
    }

    pub(crate) async fn load_device_exclusion_proposal(
        &self,
        reference: &StoreDeviceExclusionProposalRef,
    ) -> Result<VerifiedDeviceExclusionProposal, StoreObjectError> {
        self.commit_verifier
            .load_device_exclusion_proposal(reference)
            .await
    }

    pub(crate) async fn load_device_exclusion_outcome(
        &self,
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: &VerifiedDeviceExclusionProposal,
    ) -> Result<VerifiedDeviceExclusionOutcome, StoreObjectError> {
        self.commit_verifier
            .load_device_exclusion_outcome(reference, proposal)
            .await
    }

    pub(crate) async fn load_store_ack(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<StoreAck>, StoreObjectError> {
        self.commit_verifier
            .load_store_ack(reference, registration)
            .await
    }

    pub(crate) async fn load_store_ack_predecessor(
        &self,
        successor_ref: &StoreAckRef,
        successor: &StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<Option<(StoreAckRef, VerifiedObject<StoreAck>)>, StoreObjectError> {
        self.commit_verifier
            .load_store_ack_predecessor(successor_ref, successor, registration)
            .await
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &StoreSnapshotRef,
    ) -> Result<(StoreSnapshotRef, SnapshotMeta), StoreObjectError> {
        self.commit_verifier
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }

    pub(crate) async fn load_reclaim_authorization(
        &self,
        reference: &super::super::ReclaimAuthorizationRef,
    ) -> Result<super::verification::VerifiedReclaimAuthorization, StoreObjectError> {
        self.commit_verifier
            .load_reclaim_authorization(reference)
            .await
    }

    pub(crate) async fn load_reclaim_receipt(
        &self,
        reference: &super::super::ReclaimReceiptRef,
    ) -> Result<super::verification::VerifiedReclaimReceipt, StoreObjectError> {
        self.commit_verifier.load_reclaim_receipt(reference).await
    }

    pub(crate) async fn load_owner_recovery_node(
        &self,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<VerifiedObject<OwnerRecoveryNode>, StoreObjectError> {
        self.commit_verifier
            .load_owner_recovery_node(reference)
            .await
    }

    pub(crate) async fn load_store_package(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<VerifiedObject<Vec<u8>>>, StoreObjectError> {
        self.commit_verifier.load_store_package(reference).await
    }

    pub(crate) async fn load_provider_access_grant(
        &self,
        reference: &crate::sync::provider::StoreMemberProviderAccessGrantRef,
        administrator: &StoreDeviceRegistration,
    ) -> Result<
        VerifiedObject<crate::sync::provider::StoreMemberProviderAccessGrant>,
        StoreObjectError,
    > {
        self.commit_verifier
            .load_provider_access_grant(reference, administrator)
            .await
    }

    pub(crate) async fn validate_device_join_attempt_evidence(
        &self,
        attempt: VerifiedObject<DeviceJoinAttempt>,
        owner: &StoreDeviceRegistration,
    ) -> Result<LoadedDeviceJoinAttemptEvidence, StorePullError> {
        self.commit_verifier
            .validate_device_join_attempt_evidence(attempt, owner)
            .await
    }

    pub(crate) async fn exact_next_announcement_slot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        previous: Option<&VerifiedStoreBatchCommit>,
    ) -> Result<
        (
            crate::storage::cloud::ObjectSlot,
            Option<StoreDeviceHeadRef>,
        ),
        StoreError,
    > {
        self.commit_verifier
            .exact_next_announcement_slot(registration_ref, registration, previous)
            .await
    }

    pub(crate) async fn verify_author_exclusion_nonactivation(
        &mut self,
        locator: &crate::database::AuthorExclusionActivationLocator,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
        activation_commit: &VerifiedStoreBatchCommit,
        activation_predecessor_state: &ResolvedStoreDeviceState,
        operations: &VerifiedStoreDeviceOperations,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<remote_object::VerifiedCandidateNonactivation, StorePullError> {
        self.commit_verifier
            .verify_author_exclusion_nonactivation(
                locator,
                activation_head,
                activation_head_object,
                activation_commit,
                activation_predecessor_state,
                operations,
                candidate,
                candidate_head,
                candidate_head_object,
            )
            .await
    }

    pub(crate) fn remember(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        self.commit_verifier.remember(commit)
    }

    pub(crate) fn history(&self) -> &VerifiedMergeHistory {
        &self.history
    }

    pub(crate) fn storage(&self) -> &'a dyn SyncStorage {
        self.commit_verifier.storage()
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        self.commit_verifier.root()
    }

    pub(crate) fn verified_root(&self) -> &store_commit::StoreProtocolRoot {
        self.commit_verifier.verified_root()
    }

    pub(crate) fn verified_root_object(&self) -> &VerifiedObject<store_commit::StoreProtocolRoot> {
        self.commit_verifier.verified_root_object()
    }

    pub(crate) async fn load_verified_device_join_attempt(
        &mut self,
        reference: &store_commit::DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<store_commit::DeviceJoinAttempt>, StorePullError> {
        let evidence = self
            .commit_verifier
            .load_device_join_attempt_evidence(reference, owner)
            .await?;
        self.verify_device_join_attempt_evidence(evidence).await
    }

    pub(crate) async fn authenticate_blocked_candidate(
        &mut self,
        candidate: &crate::database::BlockedMergeCandidate,
    ) -> Result<VerifiedStoreBatchCommit, StoreError> {
        let reference = &candidate.head.value.commit;
        let verified = self
            .commit_verifier
            .authenticate_bytes(reference, &candidate.commit.bytes)
            .await?;
        if verified.value() != candidate.commit.value.value()
            || verified.reference().object != candidate.commit.object
            || verified.value().to_bytes() != candidate.commit.bytes
        {
            return Err(StoreError::InvalidOutbound(
                "blocked Merge candidate differs from its authenticated commit".to_string(),
            ));
        }
        Ok(verified)
    }

    pub(crate) async fn load_verified_device_join_attempt_and_owner(
        &mut self,
        reference: &store_commit::DeviceJoinAttemptRef,
    ) -> Result<
        (
            VerifiedObject<store_commit::DeviceJoinAttempt>,
            VerifiedObject<StoreDeviceRegistration>,
        ),
        StorePullError,
    > {
        let (attempt, owner) = self
            .commit_verifier
            .load_device_join_attempt_and_owner(reference)
            .await?;
        let evidence = self
            .commit_verifier
            .validate_device_join_attempt_evidence(attempt, &owner.value)
            .await?;
        let attempt = self.verify_device_join_attempt_evidence(evidence).await?;
        Ok((attempt, owner))
    }

    pub(super) async fn verify_device_join_attempt_evidence(
        &mut self,
        evidence: LoadedDeviceJoinAttemptEvidence,
    ) -> Result<VerifiedObject<store_commit::DeviceJoinAttempt>, StorePullError> {
        let frontier = &evidence.attempt.value.bootstrap_cut.0;
        self.verify_merge_history_authority(frontier, &evidence.attempt.value.membership)
            .await?;
        let access = &evidence.attempt.value.provider_approval.access_grant;
        let verified = self
            .history
            .commits
            .get(&access.activation)
            .ok_or_else(|| {
                StorePullError::Database(
                    "provider-access activation is outside the verified Merge bootstrap history"
                        .to_string(),
                )
            })?;
        if !verify_merge_provider_administrator(
            &verified.predecessor_membership,
            &access.grant.administrator_grant,
            &verified.verified.value().author_registration,
            &evidence
                .attempt
                .value
                .provider_approval
                .request
                .offer
                .provider_admin,
        ) {
            return Err(StorePullError::Database(
                "device join attempt lacks exact Merge provider-administrator authority"
                    .to_string(),
            ));
        }
        Ok(evidence.attempt)
    }

    pub(crate) async fn verify_attempt_and_prepare_device_join_bootstrap(
        &mut self,
        attempt: &store_commit::DeviceJoinAttemptRef,
        attempt_owner: &StoreDeviceRegistration,
        attempt_activation: &StoreBatchCommitRef,
    ) -> Result<
        (
            VerifiedObject<store_commit::DeviceJoinAttempt>,
            DeviceJoinBootstrapPlan,
        ),
        StorePullError,
    > {
        let evidence = self
            .commit_verifier
            .load_device_join_attempt_evidence(attempt, attempt_owner)
            .await?;
        let verified_attempt = self.verify_device_join_attempt_evidence(evidence).await?;
        let plan = self
            .prepare_device_join_bootstrap(
                &verified_attempt.value.bootstrap_cut,
                attempt_activation,
                &verified_attempt.value.membership,
            )
            .await?;
        Ok((verified_attempt, plan))
    }

    pub(crate) async fn prepare_device_join_bootstrap(
        &mut self,
        coverage: &StoreHistoryCut,
        attempt_activation: &StoreBatchCommitRef,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
        let membership = load_merge_predecessor_membership_with_history(self, membership_state)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
        let founder = self.commit_verifier.load_founder_registration().await?;
        let founder_reference =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        let genesis = self.history.genesis.clone();

        let mut pending = history_cut_references(coverage);
        pending.push(attempt_activation.clone());
        self.verify_refs(pending).await?;
        let activation = self
            .history
            .commits
            .get(attempt_activation)
            .ok_or_else(|| {
                StorePullError::Database(
                    "device join attempt activation is absent from its graph".into(),
                )
            })?;
        if activation
            .verified
            .value()
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
        if &activation.verified.value().membership_state != membership_state {
            return Err(StorePullError::Database(
                "device join attempt activation differs from its exact verified membership state"
                    .to_string(),
            ));
        }

        let mut emitted = BTreeSet::new();
        let mut ordered = Vec::with_capacity(self.history.commits.len());
        while emitted.len() != self.history.commits.len() {
            let next = self
                .history
                .commits
                .iter()
                .find_map(|(reference, verified)| {
                    (!emitted.contains(reference)
                        && commit_predecessor_references(verified.verified.value())
                            .iter()
                            .all(|dependency| emitted.contains(dependency)))
                    .then(|| reference.clone())
                });
            let Some(reference) = next else {
                return Err(StorePullError::Database(
                    "verified device join bootstrap history has an unresolved predecessor"
                        .to_string(),
                ));
            };
            let verified = &self.history.commits[&reference];
            ordered.push(DeviceJoinBootstrapCommit {
                reference: reference.clone(),
                commit: verified.verified.clone(),
                registrations: verified.registrations.clone(),
                device_operations: verified.operations.clone(),
                activation: DeviceJoinBootstrapActivation {
                    head: verified.activation_head.clone(),
                    object: verified.activation_head_object.clone(),
                    history_summary: verified.history.summary.clone(),
                },
            });
            emitted.insert(reference);
        }

        Ok(DeviceJoinBootstrapPlan {
            founder_reference,
            founder: founder.value,
            founder_bytes: founder.bytes,
            genesis,
            membership: crate::database::InitialStoreMembershipAuthority {
                head_refs: membership.head_refs().to_vec(),
            },
            commits: ordered,
        })
    }

    pub(crate) async fn verify_merge_history_authority(
        &mut self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<VerifiedMergeHistoryAuthority, StorePullError> {
        self.verify_refs(frontier.values().cloned()).await?;
        let device_state = if frontier.is_empty() {
            self.history.genesis.clone()
        } else {
            ResolvedStoreDeviceState::merge(
                frontier
                    .values()
                    .map(|reference| {
                        self.history
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
        let verified_membership_activations =
            verified_merge_membership_prefix(&self.history.commits, frontier.values().cloned())?;
        let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
            &self.commit_verifier,
            membership_state,
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
        verify_merge_membership_state_ref(membership_state, &membership, &device_state)?;
        Ok(VerifiedMergeHistoryAuthority {
            device_state,
            membership,
        })
    }

    async fn verify_snapshot_history_state(
        &mut self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        membership_ref: &StoreMembershipStateRef,
    ) -> Result<VerifiedMergeSnapshotState, StorePullError> {
        let authority = self
            .verify_merge_history_authority(frontier, membership_ref)
            .await?;
        let active_registrations =
            load_active_history_registrations(&self.commit_verifier, &authority.device_state)
                .await?;
        let checkpoints = frontier
            .values()
            .map(|reference| {
                self.history
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
        Ok(VerifiedMergeSnapshotState {
            common: VerifiedSnapshotState {
                device_state: authority.device_state,
                active_registrations,
            },
            membership: authority.membership,
            checkpoints,
        })
    }

    async fn verify_snapshot_authority(
        &mut self,
        snapshot: &crate::database::PublishedStoreSnapshot,
    ) -> Result<(StoreHistoryCut, VerifiedMergeSnapshotState), StorePullError> {
        let frontier = &snapshot.meta.coverage.0;
        let state = self
            .verify_snapshot_history_state(frontier, &snapshot.meta.state.membership)
            .await?;
        let expected_device_state = StoreDeviceStateRef::from_resolved(
            snapshot.meta.coverage.clone(),
            &state.common.device_state,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        if expected_device_state != snapshot.meta.state.devices {
            return Err(StorePullError::Database(
                "Merge snapshot device state differs from its exact verified history".to_string(),
            ));
        }
        let (_, author) = state
            .common
            .active_registrations
            .get(&snapshot.meta.author_registration.device_id)
            .filter(|(reference, _)| reference == &snapshot.meta.author_registration)
            .ok_or(StorePullError::SnapshotAuthorInactive)?;
        if !state.membership.is_owner_now(&author.author_pubkey) {
            return Err(StorePullError::SnapshotAuthorNotOwner);
        }
        let canonical = compose_merge_snapshot_history_summary(
            self.root(),
            &snapshot.meta.coverage,
            &state.membership,
            &state.common.device_state,
            &snapshot.meta.author_registration,
            author,
            state.checkpoints.clone(),
        )?;
        if snapshot.meta.history_summary != canonical {
            return Err(StorePullError::Database(
                "Merge snapshot history summary differs from its exact verified cut".to_string(),
            ));
        }
        Ok((StoreHistoryCut(frontier.clone()), state))
    }

    async fn accepted_snapshot_cut(
        &mut self,
        snapshot_frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        state: &VerifiedMergeSnapshotState,
    ) -> Result<StoreHistoryCut, StorePullError> {
        let root = self.root().clone();
        let mut accepted = snapshot_frontier.clone();
        for (registration_ref, registration) in state.common.active_registrations.values() {
            let stream_id = store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                registration_ref,
                store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
            let discovery =
                discover_merge_stream(self, registration_ref, registration, None).await?;
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
                        "current Merge author stream does not contain the snapshot cut".to_string(),
                    ));
                }
            }
            accepted.insert(stream_id, latest.clone());
        }
        Ok(StoreHistoryCut(accepted))
    }

    async fn activated_snapshot_acknowledgements(
        &mut self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
    ) -> Result<Vec<VerifiedActivatedStoreAck>, StorePullError> {
        self.verify_refs(frontier.values().cloned()).await?;
        let mut acknowledgements = Vec::new();
        for (activating_commit, commit) in &self.history.commits {
            let Some((reference, value)) = commit.acknowledgement.as_ref() else {
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
                reference: reference.clone(),
                value: value.clone(),
                chain,
                activating_commit: activating_commit.clone(),
                activating_commit_value: commit.verified.value().clone(),
            });
        }
        Ok(acknowledgements)
    }

    pub(crate) async fn verify_snapshots_for_acknowledgement(
        &mut self,
        snapshots: &[crate::database::PublishedStoreSnapshot],
    ) -> Result<(), StorePullError> {
        for snapshot in snapshots {
            self.verify_snapshot_authority(snapshot).await?;
        }
        Ok(())
    }

    pub(crate) async fn verify_snapshot_stability(
        &mut self,
        snapshot: &crate::database::PublishedStoreSnapshot,
    ) -> Result<VerifiedStoreSnapshotStability, StorePullError> {
        let (snapshot_cut, state) = self.verify_snapshot_authority(snapshot).await?;
        let snapshot_frontier = &snapshot_cut.0;
        let accepted_cut = self
            .accepted_snapshot_cut(snapshot_frontier, &state)
            .await?;
        let acknowledgements = self
            .activated_snapshot_acknowledgements(&accepted_cut.0)
            .await?;
        assemble_snapshot_stability(
            &self.commit_verifier,
            snapshot,
            snapshot_cut,
            accepted_cut,
            state.common,
            acknowledgements,
        )
        .await
    }

    pub(crate) async fn verify_membership_grant_revocation_nonactivation(
        &mut self,
        grant_id: &protocol_membership::MembershipGrantId,
        membership: &StoreMembershipStateRef,
        activation_commit: &StoreBatchCommitRef,
        activation_head: &store_commit::StoreDeviceHeadRef,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<remote_object::VerifiedCandidateNonactivation, StorePullError> {
        let root = self.root().clone();
        let head_prefix =
            store_commit::semantic_prefix_from_exact_object(&activation_head.object, ".json")
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_bytes = self
            .storage()
            .read_protocol_object(&context, &activation_head.object, &head_prefix)
            .await?;
        activation_head.object.verify(&head_bytes)?;
        let witness_head: StoreDeviceHead =
            serde_json::from_slice(&head_bytes).map_err(|error| {
                StorePullError::Database(format!("membership revocation witness head: {error}"))
            })?;
        if witness_head.head_hash() != activation_head.head_hash
            || &witness_head.commit != activation_commit
        {
            return Err(StorePullError::Database(
                "membership revocation witness head differs from its exact activation".to_string(),
            ));
        }
        let witness_author = self
            .commit_verifier
            .load_registration(&witness_head.author_registration)
            .await?;
        let opened = self
            .commit_verifier
            .load_head(activation_head, &witness_author.value, &witness_head.commit)
            .await?;
        self.verify_refs([witness_head.commit.clone()]).await?;
        let witness_commit = self
            .history
            .commits
            .get(&witness_head.commit)
            .ok_or_else(|| {
                StorePullError::Database(
                    "membership revocation witness is absent from its verified history".to_string(),
                )
            })?
            .verified
            .clone();
        if witness_commit.author() != &witness_author.value {
            return Err(StorePullError::Database(
                "membership revocation witness commit belongs to another author".to_string(),
            ));
        }
        let (_, exact_head) = self
            .commit_verifier
            .exact_next_announcement_slot(
                &witness_head.author_registration,
                &witness_author.value,
                Some(&witness_commit),
            )
            .await
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        if exact_head.as_ref() != Some(activation_head) || opened.value != witness_head {
            return Err(StorePullError::Database(
                "membership revocation witness is not an accepted exact head".to_string(),
            ));
        }
        if witness_commit.value().membership_state != *membership {
            return Err(StorePullError::Database(
                "membership revocation witness commit names another membership state".to_string(),
            ));
        }
        let current_membership = load_merge_predecessor_membership_with_history(
            self,
            &witness_commit.value().membership_state,
        )
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
        let Some(causal_grants::GrantState::Tombstoned {
            record: current_record,
            ..
        }) = current.grants.get(grant_id)
        else {
            return Err(StorePullError::Database(
                "membership revocation witness grant is not tombstoned".to_string(),
            ));
        };
        let candidate_ref = candidate.reference();
        let candidate_commit = candidate.value();
        let candidate_author = candidate.author();
        let predecessor_membership = load_merge_predecessor_membership_with_history(
            self,
            &candidate_commit.membership_state,
        )
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
                "membership revocation grant was not active at the candidate predecessor"
                    .to_string(),
            ));
        };
        if predecessor_record != current_record
            || predecessor_record.member_pubkey != candidate_author.author_pubkey
            || candidate_commit.membership_authority.as_ref()
                != Some(&predecessor_record.creation_authority)
        {
            return Err(StorePullError::Database(
                "membership revocation grant differs from the candidate's signed authority"
                    .to_string(),
            ));
        }
        let cap = witness_commit
            .value()
            .order
            .predecessor_cut()
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let expected_stream = store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &candidate_commit.author_registration,
            store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = candidate_ref.coord;
        if stream_id != expected_stream
            || cap
                .commits()
                .get(&expected_stream)
                .is_some_and(|covered| sequence <= covered.coord.sequence())
        {
            return Err(StorePullError::Database(
                "membership revocation candidate is not beyond the accepted witness cut"
                    .to_string(),
            ));
        }
        let verified_candidate_head = self
            .commit_verifier
            .verify_terminal_candidate_head(candidate, candidate_head, candidate_head_object)
            .await?;
        let durable = remote_object::CandidateNonactivation::from_durable_parts(
            candidate_ref,
            candidate_commit,
            remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
                grant_id: grant_id.clone(),
                membership: membership.clone(),
                activation_commit: witness_head.commit,
                activation_head: activation_head.clone(),
            },
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        remote_object::VerifiedCandidateNonactivation::from_verified_membership_grant_revocation(
            durable,
            candidate_ref.clone(),
            verified_candidate_head,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))
    }

    pub(crate) async fn load_merge_commit_registrations(
        &self,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        membership: &MembershipChain,
        accepted_frontier: &[StoreBatchCommitRef],
    ) -> Result<Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>, StorePullError>
    {
        let accepted =
            VerifiedMergePredecessorHistory::new(&self.history.commits, accepted_frontier);
        let loaded = load_commit_join_evidence(self, commit, author).await;
        let loaded = loaded.map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let join_evidence = verify_commit_join_evidence(commit, loaded, accepted).await?;
        load_commit_registrations(
            self,
            commit,
            author,
            Some(membership),
            &join_evidence,
            accepted,
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })
    }

    pub(crate) async fn load_activation_head(
        &mut self,
        verified_commit: &VerifiedStoreBatchCommit,
    ) -> Result<VerifiedObject<StoreDeviceHead>, StorePullError> {
        let author = verified_commit.author().clone();
        let commit = verified_commit.value();
        let (_, head_ref) = self
            .commit_verifier
            .exact_next_announcement_slot(
                &commit.author_registration,
                &author,
                Some(verified_commit),
            )
            .await
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let head_ref = head_ref.ok_or_else(|| {
            StorePullError::Database(
                "device join activation has no exact accepted activation head".to_string(),
            )
        })?;
        Ok(self
            .commit_verifier
            .load_head(&head_ref, &author, verified_commit.reference())
            .await?)
    }

    pub(crate) async fn verify_owner_conflict_acceptance(
        &mut self,
        acceptance: &store_commit::OwnerConflictResolutionAcceptance,
        resolver_pubkey: &str,
    ) -> Result<(), StorePullError> {
        let frontier = acceptance.device_state.frontier();
        let tips = frontier.commits().values().cloned().collect::<Vec<_>>();
        self.verify_refs(tips.clone()).await?;
        verify_merge_owner_conflict_acceptance_with_history(
            &self.commit_verifier,
            acceptance,
            resolver_pubkey,
            &self.history.genesis,
            &self.history.commits,
            tips,
        )
        .await
    }

    pub(crate) async fn verify_refs(
        &mut self,
        tips: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), StorePullError> {
        let root = self.root().clone();
        let mut pending = tips.into_iter().collect::<Vec<_>>();
        let mut loaded = BTreeMap::<StoreBatchCommitRef, VerifiedStoreBatchCommit>::new();
        while let Some(reference) = pending.pop() {
            if self.history.commits.contains_key(&reference) || loaded.contains_key(&reference) {
                continue;
            }
            let verified = self.load_ref(&reference).await?;
            pending.extend(commit_predecessor_references(verified.value()));
            loaded.insert(reference, verified);
        }

        let mut states = self
            .history
            .commits
            .iter()
            .map(|(reference, verified)| (reference.clone(), verified.state_after.clone()))
            .collect::<BTreeMap<_, _>>();
        while !loaded.is_empty() {
            let next = loaded.iter().find_map(|(reference, verified)| {
                commit_predecessor_references(verified.value())
                    .iter()
                    .all(|dependency| states.contains_key(dependency))
                    .then(|| reference.clone())
            });
            let Some(reference) = next else {
                return Err(StorePullError::Database(
                    "Merge history is cyclic or has an unresolved predecessor".to_string(),
                ));
            };
            let verified = loaded.remove(&reference).ok_or_else(|| {
                StorePullError::Database(
                    "selected exclusion-history commit disappeared before verification".to_string(),
                )
            })?;
            let commit = verified.value().clone();
            let author = verified.author().clone();
            let (_, accepted_head) = self
                .commit_verifier
                .exact_next_announcement_slot(&commit.author_registration, &author, Some(&verified))
                .await
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let activation_head_ref = accepted_head.ok_or_else(|| {
                StorePullError::Database(
                    "Merge history commit has no accepted announcement head".to_string(),
                )
            })?;
            let predecessor_state =
                verified_merge_predecessor_state(&self.history.genesis, &states, &commit)?;
            let verified_membership_prefix = verified_merge_membership_prefix(
                &self.history.commits,
                commit_predecessor_references(&commit),
            )?;
            let pending_resolution =
                Box::pin(verify_merge_resolution_activation_acceptance_with_history(
                    &self.commit_verifier,
                    &commit,
                    &self.history.genesis,
                    &self.history.commits,
                ))
                .await?;
            let membership = Box::pin(load_merge_predecessor_membership_with_verified_activations(
                &self.commit_verifier,
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
            let accepted_frontier = commit_predecessor_references(&commit);
            let registrations = Box::pin(self.load_merge_commit_registrations(
                &commit,
                &author,
                &membership,
                &accepted_frontier,
            ))
            .await?;
            let (authorized_predecessor, recovery_author) = predecessor_with_recovery_author(
                predecessor_state.clone(),
                &commit,
                &registrations,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
            if !device_state_has_active_registration(
                &authorized_predecessor,
                &commit.author_registration,
            ) {
                return Err(StorePullError::Database(
                    "author exclusion history commit author is inactive at its predecessor"
                        .to_string(),
                ));
            }
            let resolver = DeviceStateResolver::Loaded {
                genesis: &self.history.genesis,
                states: &states,
            };
            let operations = Box::pin(load_commit_device_operations(
                Some(&resolver),
                &mut self.commit_verifier,
                &commit,
                &authorized_predecessor,
                Some(&membership),
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            let acknowledgement = Box::pin(validate_commit_acknowledgement(self, &commit, &author))
                .await
                .map_err(|error| match error {
                    RegistrationLoadError::Object(error) => StorePullError::Object(error),
                    RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                })?;
            let membership_control =
                if let Some(store_commit::StoreControl { transition }) = commit.control() {
                    let (activations, conflict_resolution) =
                        Box::pin(self.verify_membership_control_with_retained_history(
                            &reference,
                            &commit,
                            &membership,
                            &predecessor_state,
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
                &self.commit_verifier,
                &commit,
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
                    self.history
                        .commits
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
                &self.commit_verifier,
                &reference,
                &commit,
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
                    self.retain_acknowledgement(
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
                &root,
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
            let activation_head = self
                .commit_verifier
                .load_head(&activation_head_ref, &author, &reference)
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
            self.history.commits.insert(
                reference,
                VerifiedMergeHistoryCommit {
                    verified,
                    predecessor_membership: membership,
                    predecessor_state,
                    state_after: state,
                    registrations,
                    operations,
                    acknowledgement,
                    membership_control,
                    activation_head: activation_head.value,
                    activation_head_object: activation_head.object,
                    history,
                },
            );
        }
        Ok(())
    }
}
