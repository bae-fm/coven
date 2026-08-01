use super::verification::StoreMembershipObjectVerifier;
use super::*;
use crate::protocol::circle_control::StoreMembershipStateRef;
use crate::protocol::membership::{MembershipChain, MembershipStatus};
use crate::protocol::store_commit::{
    ActivatedStoreDeviceRegistrationRef, DeviceJoinAttempt, DeviceJoinAttemptDecisionRef,
    DeviceJoinOutcomeBody, DeviceStreamAnchor, ObjectHash, OpenedRetainedMergeHistorySummary,
    OwnerRecoveryNode, OwnerRecoveryNodeRef, ResolvedStoreDeviceState,
    RetainedVerifiedMergeHistorySummary, RetainedVerifiedRegistration, StoreBatchCommit,
    StoreBatchCommitRef, StoreCommitCoord, StoreDeviceHead, StoreDeviceProposalState,
    StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreDeviceStatus, StoreHistoryCut,
    StoreProtocolError, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use crate::protocol::store_commit::{
    DeviceJoinAttemptRef, DeviceJoinOutcome, DeviceJoinOutcomeRef, SnapshotMeta, StoreAck,
    StoreAckRef, StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProposalRef,
    StoreDeviceHeadRef, StoreSnapshotRef, VerifiedDeviceExclusionOutcome,
    VerifiedDeviceExclusionProposal,
};
use crate::protocol::{
    causal_grants, membership as protocol_membership, provider, remote_object, store_commit,
};
use crate::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use crate::storage::{StoreObjectError, VerifiedObject};
use crate::sync::store::circle_controls::activation::VerifiedCircleActivations;
use crate::sync::store::owner::pull::*;
use std::collections::{BTreeMap, BTreeSet};

use super::verification::DeviceStateResolver;
use super::{device_join, reclaim as store_reclaim};

pub(super) mod join_validation;
mod membership;
pub(super) mod registration;
use join_validation::*;
use registration::*;

pub(crate) async fn load_membership_at_exact_heads_with_verified_activations(
    root: &super::super::protocol_root::VerifiedStoreRoot,
    commit_verifier: &StoreCommitVerifier<'_>,
    heads: &[protocol_membership::MembershipHeadRef],
    resolutions: &[protocol_membership::StoreMembershipConflictResolutionRef],
    verified_activations: &VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
    membership::load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
        root,
        commit_verifier,
        heads,
        resolutions,
        verified_activations,
        pending_resolution,
    )
    .await
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

pub(crate) fn predecessor_verifies_owner(
    predecessor: &MembershipChain,
    membership: &StoreMembershipStateRef,
    owner_pubkey: &str,
    owner_grant: &crate::protocol::membership::MembershipGrantId,
) -> bool {
    let MembershipStatus::Resolved(resolved) = predecessor.status() else {
        return false;
    };
    StoreMembershipStateRef::from_parts(
        predecessor.head_refs().to_vec(),
        predecessor.resolution_refs().to_vec(),
        membership.recovery().to_vec(),
        resolved.state_hash,
    )
    .is_ok_and(|expected| membership == &expected)
        && predecessor.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
}

fn predecessor_provider_admin_state(
    predecessor: &MembershipChain,
) -> Option<&provider::ProviderAdminState> {
    let MembershipStatus::Resolved(resolved) = predecessor.status() else {
        return None;
    };
    Some(resolved.provider_admin.combined_state())
}

fn predecessor_verifies_provider_administrator(
    predecessor: &MembershipChain,
    grant_id: &provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
    expected: &provider::ProviderAdminGrantRecord,
) -> bool {
    let Some(state) = predecessor_provider_admin_state(predecessor) else {
        return false;
    };
    state.authorizes(grant_id, executor) && state.records().get(grant_id) == Some(expected)
}

fn predecessor_verifies_provider_administrator_grant(
    predecessor: &MembershipChain,
    grant_id: &provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
) -> bool {
    predecessor_provider_admin_state(predecessor)
        .is_some_and(|state| state.authorizes(grant_id, executor))
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

    fn find(
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

    fn contains_join_attempt(
        &self,
        expected: &DeviceJoinAttemptRef,
    ) -> Result<bool, StorePullError> {
        self.find(|_, commit| {
            commit.device_join_attempt_decisions().iter().any(|decision| {
                matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(reference) if reference == expected)
            })
        })
        .map(|found| found.is_some())
    }

    fn contains_join_outcome(
        &self,
        expected: &DeviceJoinOutcomeRef,
    ) -> Result<bool, StorePullError> {
        self.find(|_, commit| {
            commit
                .device_join_outcomes()
                .binary_search(expected)
                .is_ok()
        })
        .map(|found| found.is_some())
    }

    /// Bind a row blob to the package that published it. The blob is never named in a
    /// commit body — only inside the package's bindings — so what a commit establishes
    /// is that the named package was activated by a commit in this device's
    /// predecessor history. The blob's own reference is self-binding: its object key is
    /// derived from its locator, which names the audience and uploading device, and
    /// the audience must be the one the package addresses. Reading the bindings
    /// themselves requires the package's audience key, which a Store member outside a
    /// Circle does not hold; the Owner re-reads them before authorizing any delete.
    fn validate_package_bound_reclaim_target(
        &self,
        target: &store_reclaim::ReclaimTarget,
        activation: &store_reclaim::PackageBlobBindingActivation<'_>,
    ) -> Result<(), RegistrationLoadError> {
        let store_reclaim::ReclaimTarget::AudienceBlob(blob) = target else {
            return Err(RegistrationLoadError::Invalid(
                "reclaim target is not published by a package binding".to_string(),
            ));
        };
        let expected = activation.activation.clone();
        let activating = self
            .find(|candidate, _| candidate == &expected)
            .map_err(registration_attempt_error)?
            .ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "reclaim evidence blob activation is absent from predecessor history"
                        .to_string(),
                )
            })?;
        let names_package = match activation.package {
            store_reclaim::AudienceBlobBindingPackage::Store(package) => {
                activating.verified.value().store_package() == Some(package)
            }
            store_reclaim::AudienceBlobBindingPackage::Circle(package) => activating
                .verified
                .value()
                .circle_packages()
                .contains(package),
        };
        if !names_package {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence blob package differs from its exact activation".to_string(),
            ));
        }
        if blob.blob.locator().audience() != activation.package.remote_audience() {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence blob names a package for another audience".to_string(),
            ));
        }
        Ok(())
    }

    /// Bind a reclaim target to the retained Store commit that published it: the
    /// commit must sit in this device's predecessor history and its body must name the
    /// exact object the evidence authorizes deleting.
    fn validate_commit_activated_reclaim_target(
        &self,
        target: &store_reclaim::ReclaimTarget,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<(), RegistrationLoadError> {
        let expected = activating_commit.clone();
        let activation = self
            .find(|candidate, _| candidate == &expected)
            .map_err(registration_attempt_error)?
            .ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "reclaim evidence package activation is absent from predecessor history"
                        .to_string(),
                )
            })?;
        let names_target = match target {
            store_reclaim::ReclaimTarget::StorePackage(store) => {
                activation.verified.value().store_package() == Some(&store.package)
            }
            store_reclaim::ReclaimTarget::CirclePackage(circle) => activation
                .verified
                .value()
                .circle_packages()
                .contains(&circle.package),
            store_reclaim::ReclaimTarget::CircleBootstrapImage(bootstrap) => activation
                .verified
                .value()
                .circle_controls()
                .iter()
                .flat_map(|control| control.objects.access.iter())
                .any(|access| {
                    access.bootstrap.as_ref() == Some(&bootstrap.coverage.bootstrap.image)
                }),
            store_reclaim::ReclaimTarget::CircleSnapshotImage(_)
            | store_reclaim::ReclaimTarget::AudienceBlob(_) => {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim target claims a Store commit activation it is not published by"
                        .to_string(),
                ));
            }
        };
        if !names_target {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence target differs from its exact package activation".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_commit_join_cleanup_receipts(
        &self,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
        join_evidence: &VerifiedCommitJoinEvidence,
    ) -> Result<(), RegistrationLoadError> {
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join cleanup activation has no exact predecessor authority".to_string(),
            )
        })?;
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "device join cleanup activation author is not an active Owner".to_string(),
            ));
        }
        for loaded in &join_evidence.cleanup_receipts {
            if !self
                .contains_join_outcome(&loaded.receipt.cancellation)
                .map_err(registration_attempt_error)?
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup receipt outcome is absent from its verified predecessor history"
                        .to_string(),
                ));
            }
            let attempt = join_evidence.attempts.get(&loaded.attempt).ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "device join cleanup receipt has no verified exact attempt".to_string(),
                )
            })?;
            let expected_administrator = &attempt.provider_approval.request.offer.provider_admin;
            if !predecessor_verifies_provider_administrator(
                predecessor,
                &loaded.receipt.provider_admin_grant,
                &loaded.receipt.executor,
                expected_administrator,
            ) {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup executor is not the exact effective provider administrator"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    fn verify_commit_join_evidence(
        &self,
        commit: &StoreBatchCommit,
        loaded: LoadedCommitJoinEvidence,
    ) -> Result<VerifiedCommitJoinEvidence, StorePullError> {
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
            let verified = self
                .find(|candidate, _| candidate == &access.activation)?
                .ok_or_else(|| {
                    StorePullError::Database(
                        "provider-access activation is outside the accepted Merge predecessor graph"
                            .to_string(),
                    )
                })?;
            if !predecessor_verifies_provider_administrator(
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
    }
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
    pub(crate) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'a> {
        self.commit_verifier.membership_objects()
    }

    pub(crate) async fn verify_resolution_activation_acceptance(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeConflictResolutionActivation>, StorePullError> {
        let root = self.root.reference();
        let Some(store_commit::StoreControl { transition }) = commit.control() else {
            return Ok(None);
        };
        let entry = self
            .commit_verifier
            .membership_objects()
            .load_entry(&transition.body.entry)
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
        let value = self
            .commit_verifier
            .membership_objects()
            .load_resolution(resolution)
            .await?;
        let registration = self
            .commit_verifier
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
        self.verify_owner_conflict_acceptance_at_tips(
            acceptance,
            &value.value.resolver_pubkey,
            commit_predecessor_references(commit),
        )
        .await?;
        Ok(Some(VerifiedMergeConflictResolutionActivation {
            reference: resolution.clone(),
        }))
    }

    async fn verify_owner_conflict_acceptance_at_tips(
        &self,
        acceptance: &store_commit::OwnerConflictResolutionAcceptance,
        resolver_pubkey: &str,
        allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), StorePullError> {
        let registration = self
            .commit_verifier
            .load_registration(&acceptance.owner_registration)
            .await?;
        acceptance
            .verify(&registration.value)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let state = merge_device_state_from_verified_history(
            &acceptance.device_state,
            &self.history.genesis,
            &self.history.commits,
            allowed_tips,
        )?;
        if !device_state_has_active_registration(&state, &acceptance.owner_registration) {
            return Err(StorePullError::Database(
                "conflict-resolution Owner registration is not active at its exact device state"
                    .to_string(),
            ));
        }
        self.commit_verifier
            .verify_canonical_owner_registration(
                &state,
                resolver_pubkey,
                &acceptance.owner_registration,
            )
            .await?;
        Ok(())
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
            self.root.reference(),
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
        self.commit_verifier
            .verify_owner_recovery_activation(commit)
            .await
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
            let Some((predecessor_ref, predecessor)) = self
                .load_store_ack_predecessor(&current_ref, &current, registration)
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

    pub(crate) async fn verify_canonical_owner_registration(
        &self,
        state: &ResolvedStoreDeviceState,
        owner_pubkey: &str,
        selected: &StoreDeviceRegistrationRef,
    ) -> Result<(), StorePullError> {
        self.commit_verifier
            .verify_canonical_owner_registration(state, owner_pubkey, selected)
            .await
    }

    pub(crate) async fn discover_owner_recoveries(
        &self,
        membership: &MembershipChain,
    ) -> Result<Vec<(StoreDeviceRegistrationRef, StoreDeviceRegistration)>, StorePullError> {
        self.commit_verifier
            .discover_owner_recoveries(membership)
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
        if verified_commit.store_root_hash() != self.root.reference().store_root_hash {
            return Err(StorePullError::Database(
                "local device-operation commit belongs to another Store root".to_string(),
            ));
        }
        let commit = verified_commit.value();
        if commit.device_exclusion_proposals().is_empty()
            && commit.device_exclusion_outcomes().is_empty()
        {
            return VerifiedStoreDeviceOperations::without_exclusions(commit)
                .map_err(|error| StorePullError::Database(error.to_string()));
        }
        if state_ref != &commit.device_state {
            return Err(StorePullError::Database(
                "local exclusion commit differs from its materialized predecessor device state"
                    .to_string(),
            ));
        }
        verify_merge_membership_state_ref(&commit.membership_state, membership, &state)?;
        let resolver = DeviceStateResolver::Database(database);
        Box::pin(self.commit_verifier.load_commit_device_operations(
            Some(&resolver),
            commit,
            &state,
            Some(membership),
        ))
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })
    }

    pub(crate) async fn derive_local_post_device_state(
        &self,
        commit: &StoreBatchCommit,
        predecessor_state: ResolvedStoreDeviceState,
        registrations: &[(StoreDeviceRegistration, StoreDeviceRegistrationActivation)],
        device_operations: VerifiedStoreDeviceOperations,
    ) -> Result<ResolvedStoreDeviceState, StorePullError> {
        let (authorized_predecessor, recovery_author) =
            predecessor_with_recovery_author(predecessor_state, commit, registrations)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        let owner_recovery = self
            .commit_verifier
            .verify_owner_recovery_activation(commit)
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

    pub(crate) async fn discover_merge_stream(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        inactive_accepted_cut: Option<&StoreHistoryCut>,
    ) -> Result<MergeStreamDiscovery, StorePullError> {
        let DeviceStreamAnchor::StoreAnnouncements { first_slot } = &registration.store_commits
        else {
            return Err(StorePullError::Database(format!(
                "Store registration {} has no Merge announcement anchor",
                registration.device_id
            )));
        };
        let root = self.root.reference().clone();
        let stream_id = store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            registration_ref,
            store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let maximum_sequence = inactive_accepted_cut.map(|cut| {
            cut.0
                .get(&stream_id)
                .map_or(0, |reference| reference.coord.sequence())
        });
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
            let semantic_prefix =
                store_commit::head_slot_prefix(&registration.device_id.to_string(), sequence);
            let (bytes, object) = match self
                .commit_verifier
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
            let coord_matches = unverified.commit.coord.stream_id == stream_id
                && unverified.commit.coord.sequence == sequence;
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
            let commit = match self.load_ref(&unverified.commit).await {
                Ok(verified)
                    if verified.value().author_registration == *registration_ref
                        && verified.author() == registration =>
                {
                    verified.value().clone()
                }
                Ok(_) => {
                    block = Some(MergeStreamBlock::Authenticated(HeldStorePosition::commit(
                        &unverified.commit,
                        HeldStorePositionReason::Unauthorized,
                    )));
                    break;
                }
                Err(error) => {
                    let reason = match error {
                        StorePullError::Object(error) => held_object_error(error),
                        error => HeldStorePositionReason::InvalidObject(error.to_string()),
                    };
                    block = Some(MergeStreamBlock::Authenticated(HeldStorePosition::commit(
                        &unverified.commit,
                        reason,
                    )));
                    break;
                }
            };
            let next_slot = head.successor.next_slot.clone();
            let head_ref = StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object: object.clone(),
            };
            predecessor = Some(object);
            sequence = sequence.checked_add(1).ok_or_else(|| {
                StorePullError::Database(format!(
                    "Store announcement stream {stream_id} sequence overflow"
                ))
            })?;
            commits.push((head_ref, head.clone(), head.commit.clone(), commit));
            latest_head = Some(head);
            slot = next_slot;
        }

        Ok(MergeStreamDiscovery {
            latest_head,
            commits,
            block,
        })
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
        let root = self.root.reference().clone();
        let promoter = self
            .commit_verifier
            .load_registration(&request.promoter_registration)
            .await?;
        request
            .verify(&root, &promoter.value)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let discovered = self
            .discover_merge_stream(&request.promoter_registration, &promoter.value, None)
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
            self.root.reference(),
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
            self.root.reference(),
            commit_verifier,
            acceptance,
            &history.commits,
        )
        .await
    }

    pub(crate) async fn verify_accepted_provider_access_activation(
        &mut self,
        access: &crate::protocol::provider::ActivatedStoreMemberProviderAccessGrant,
        provider_admin: &crate::protocol::provider::ProviderAdminGrantRecord,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), StorePullError> {
        let grant = self
            .load_provider_access_grant(&access.grant_ref, administrator)
            .await?;
        if grant.value != access.grant {
            return Err(StorePullError::Database(
                "device provider approval embeds a different access grant than its exact reference"
                    .to_string(),
            ));
        }
        let activation = self.load_ref(&access.activation).await?;
        if activation.value().provider_access_grants() != std::slice::from_ref(&access.grant_ref)
            || activation.value().author_registration != access.grant.administrator
            || activation.author() != administrator
        {
            return Err(StorePullError::Database(
                "device provider approval activation is not the administrator's exact sole access grant"
                    .to_string(),
            ));
        }
        let membership = self
            .load_predecessor_membership(&activation.value().membership_state)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
        if !predecessor_verifies_provider_administrator(
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
            .history
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
        for recovered in self.discover_owner_recoveries(membership).await? {
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
                let discovered = self
                    .discover_merge_stream(registration_ref, registration, inactive_cut)
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
                self.history.genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    next.values()
                        .map(|reference| {
                            self.history
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
                let accepted_closure =
                    verified_merge_commit_closure(&self.history.commits, next.values().cloned())?;
                return Ok(accepted_closure.contains(expected));
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

    pub(crate) async fn load_device_join_cleanup_activation(
        &mut self,
        activation: &device_join::DeviceJoinCleanupActivation,
    ) -> Result<LoadedDeviceJoinCleanupActivation, StorePullError> {
        let verified_commit = self.load_ref(&activation.activation).await?;
        if verified_commit.value().device_join_cleanup_receipts()
            != std::slice::from_ref(&activation.receipt)
        {
            return Err(StorePullError::Database(
                "device join cleanup activation does not contain its exact sole receipt"
                    .to_string(),
            ));
        }
        let receipts = self
            .load_commit_join_cleanup_receipts(verified_commit.value(), verified_commit.author())
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
        Ok(LoadedDeviceJoinCleanupActivation {
            verified_commit,
            receipts,
        })
    }

    pub(crate) async fn verify_device_join_cleanup_activation(
        &mut self,
        activation: LoadedDeviceJoinCleanupActivation,
    ) -> Result<crate::sync::store::JoinerJoinTerminal, StorePullError> {
        let membership = self
            .load_predecessor_membership(&activation.verified_commit.value().membership_state)
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
        if !predecessor_verifies_provider_administrator(
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

pub(crate) struct VerifiedMergeHistory {
    pub(crate) genesis: ResolvedStoreDeviceState,
    pub(crate) commits: BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
}

pub(crate) struct MergeHistoryVerifier<'a> {
    root: super::super::protocol_root::VerifiedStoreRoot,
    commit_verifier: StoreCommitVerifier<'a>,
    history: VerifiedMergeHistory,
}

pub(crate) struct SelectedStableStoreSnapshot {
    pub(crate) snapshot: crate::database::PublishedStoreSnapshot,
    pub(crate) stability: crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
}

type PredecessorCommitPredicate<'a> = Box<dyn FnMut(&VerifiedStoreBatchCommit) -> bool + Send + 'a>;

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

impl MergedRetainedMergeHistory {
    fn insert_membership_proof(
        &mut self,
        reference: StoreBatchCommitRef,
        value: store_commit::RetainedMergeMembershipProof,
    ) -> Result<(), StorePullError> {
        if self
            .membership_proofs
            .keys()
            .any(|existing| existing.coord == reference.coord && existing != &reference)
        {
            return Err(StorePullError::Database(
                "Merge predecessor checkpoints contain conflicting membership proofs at one Store coordinate"
                    .to_string(),
            ));
        }
        insert_exact(
            &mut self.membership_proofs,
            reference,
            value,
            "Merge predecessor checkpoints disagree on a membership proof",
        )
    }
}

pub(crate) fn merge_retained_merge_history(
    root: &StoreRootRef,
    membership: &MembershipChain,
    predecessors: Vec<OpenedRetainedMergeHistorySummary>,
) -> Result<MergedRetainedMergeHistory, StorePullError> {
    let mut merged = MergedRetainedMergeHistory {
        causal_cut: BTreeMap::new(),
        registrations: BTreeMap::new(),
        acknowledgements: BTreeMap::new(),
        membership_proofs: BTreeMap::new(),
        announcement_frontier: BTreeMap::new(),
    };
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
                &mut merged.causal_cut,
                key,
                value,
                "Merge predecessor checkpoints disagree on a Store coordinate",
            )?;
        }
        for (key, value) in predecessor.summary.registrations {
            insert_exact(
                &mut merged.registrations,
                key,
                value,
                "Merge predecessor checkpoints disagree on a device registration",
            )?;
        }
        for (key, value) in predecessor.summary.acknowledgements {
            insert_latest_acknowledgement(&mut merged.acknowledgements, key, value)?;
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
            merged.insert_membership_proof(key, value)?;
        }
        for (key, value) in predecessor.announcement_frontier {
            insert_latest_announcement(&mut merged.announcement_frontier, key, value)?;
        }
    }
    Ok(merged)
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
    let mut merged = merge_retained_merge_history(root, membership, predecessors)?;
    let mut membership_floor = store_commit::MembershipCausalFloor::from_membership(membership);
    insert_exact(
        &mut merged.causal_cut,
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
            &mut merged.registrations,
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
            &mut merged.acknowledgements,
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
        merged.insert_membership_proof(commit_ref.clone(), proof)?;
    }
    let author_ref = commit.author_registration.clone();
    author_ref
        .verify_registration(author)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    insert_exact(
        &mut merged.registrations,
        author_ref.device_id,
        RetainedVerifiedRegistration {
            reference: author_ref.clone(),
            value: author.clone(),
        },
        "Merge successor author registration conflicts with retained authority",
    )?;
    let mut post_frontier = BTreeMap::new();
    for reference in merged.causal_cut.values() {
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
    let MergedRetainedMergeHistory {
        causal_cut,
        registrations,
        acknowledgements,
        membership_proofs,
        announcement_frontier,
    } = merged;
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
    pub(crate) async fn select_maximal_stable_store_snapshot(
        &mut self,
        candidates: Vec<crate::database::PublishedStoreSnapshot>,
    ) -> Result<Option<SelectedStableStoreSnapshot>, StorePullError> {
        let Some(maximal_candidate) =
            super::snapshot::select_maximal_store_snapshot(candidates.clone())
        else {
            return Ok(None);
        };
        let maximal_reference = maximal_candidate.reference;
        let mut stable = Vec::new();
        let mut maximal_rejection = None;
        for snapshot in candidates {
            match self.verify_snapshot_stability(&snapshot).await {
                Ok(stability) => stable.push(SelectedStableStoreSnapshot {
                    snapshot,
                    stability,
                }),
                Err(error) => match &error {
                    StorePullError::SnapshotNotStable { .. }
                    | StorePullError::SnapshotAuthorInactive
                    | StorePullError::SnapshotAuthorNotOwner => {
                        if snapshot.reference == maximal_reference {
                            maximal_rejection = Some(error);
                        }
                    }
                    _ => return Err(error),
                },
            }
        }
        let selected = super::snapshot::select_maximal_store_snapshot(
            stable
                .iter()
                .map(|candidate| candidate.snapshot.clone())
                .collect(),
        );
        if let Some(selected) = selected {
            let index = stable
                .iter()
                .position(|candidate| candidate.snapshot.reference == selected.reference)
                .ok_or_else(|| {
                    StorePullError::Database(
                        "stable Store snapshot selection lost its verified candidate".to_string(),
                    )
                })?;
            return Ok(Some(stable.swap_remove(index)));
        }
        Err(maximal_rejection.ok_or_else(|| {
            StorePullError::Database(
                "Store snapshot candidates produced no stability decision".to_string(),
            )
        })?)
    }

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

    pub(crate) async fn load_predecessor_membership(
        &mut self,
        state: &StoreMembershipStateRef,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        self.load_membership_at_exact_heads(&state.heads, &state.resolutions)
            .await
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
    }

    pub(crate) async fn load_predecessor_membership_at_verified_prefix(
        &self,
        state: &StoreMembershipStateRef,
        verified_activations: &VerifiedMergeMembershipPrefix,
        pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        self.load_membership_at_verified_prefix(
            &state.heads,
            &state.resolutions,
            verified_activations,
            pending_resolution,
        )
        .await
        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
    }

    pub(crate) async fn load_exact_membership_head(
        &mut self,
        reference: &protocol_membership::MembershipHeadRef,
    ) -> Result<protocol_membership::AuthorHead, crate::sync::store::membership::AnchoredChainError>
    {
        self.commit_verifier
            .membership_objects()
            .load_head(reference)
            .await
            .map(|loaded| loaded.value)
            .map_err(membership::map_membership_object_error)
    }

    pub(crate) async fn project_membership_to_verified_prefix(
        &self,
        candidate_heads: &[protocol_membership::MembershipHeadRef],
        prefix: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        membership::project_anchored_chain_to_verified_store_prefix(
            &self.root,
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

    pub(super) async fn from_commit_verifier(
        _authority: super::history::HistoryConstructionAuthority,
        root: super::super::protocol_root::VerifiedStoreRoot,
        commit_verifier: StoreCommitVerifier<'a>,
    ) -> Result<Self, StorePullError> {
        let founder = commit_verifier.load_founder_registration().await?;
        Self::from_commit_verifier_and_founder(root, commit_verifier, &founder)
    }

    fn from_commit_verifier_and_founder(
        root: super::super::protocol_root::VerifiedStoreRoot,
        commit_verifier: StoreCommitVerifier<'a>,
        founder: &VerifiedObject<StoreDeviceRegistration>,
    ) -> Result<Self, StorePullError> {
        let verified_root = root.protocol();
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        let founder_origin_matches = matches!(
            founder.value.origin,
            store_commit::StoreDeviceRegistrationOrigin::Founder { creation_id }
                if creation_id == verified_root.descriptor.creation_id
        );
        if founder.value.store_root != *root.reference()
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
            root.reference(),
            founder_ref,
            &verified_root.descriptor.founder_pubkey,
            verified_root.descriptor.founder_grant.clone(),
            &verified_root.descriptor.founder_recovery,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(Self {
            root,
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

    pub(super) async fn covered_reference_status(
        &mut self,
        coverage: &CommitFrontier,
        stream_id: &str,
        reference: &StoreBatchCommitRef,
    ) -> MaterializedCheck {
        if commit_stream_id(&reference.coord) != stream_id {
            return MaterializedCheck::Held(HeldStorePositionReason::WrongSlot(format!(
                "commit reference stream {} differs from dependency stream {stream_id}",
                commit_stream_id(&reference.coord)
            )));
        }
        let coverage = coverage.clone().into_refs();
        let Some(covered) = coverage.get(stream_id) else {
            return MaterializedCheck::Missing;
        };
        if reference.coord.sequence() > covered.coord.sequence() {
            return MaterializedCheck::Missing;
        }
        let mut cursor = covered.clone();
        loop {
            if cursor == *reference {
                return MaterializedCheck::Yes;
            }
            if cursor.coord.sequence() <= reference.coord.sequence() {
                return MaterializedCheck::Held(HeldStorePositionReason::HashMismatch {
                    referenced_device_id: stream_id.to_string(),
                    referenced_commit: reference.clone(),
                    materialized_hash: cursor.commit_hash,
                });
            }
            let verified_commit = match self.load_ref(&cursor).await {
                Ok(commit) => commit,
                Err(error) => {
                    return MaterializedCheck::Held(HeldStorePositionReason::ObjectUnreadable {
                        key: "exact Store commit".to_string(),
                        detail: error.to_string(),
                    });
                }
            };
            let Some(predecessor) = verified_commit.value().order.predecessor() else {
                return MaterializedCheck::Missing;
            };
            cursor = predecessor.clone();
        }
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

    async fn validate_commit_join_abandonments(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
    ) -> Result<(), RegistrationLoadError> {
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join abandonment activation has no exact predecessor authority".to_string(),
            )
        })?;
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
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
                self.root.reference().store_root_hash,
                ProtocolObjectDomain::DeviceJoinAbandonment,
            );
            let semantic_prefix =
                store_commit::device_join_abandonment_semantic_prefix(reference.attempt_id);
            let bytes = self
                .commit_verifier
                .read_protocol_object(&context, &reference.object, &semantic_prefix)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let abandonment: device_join::DeviceJoinAbandonmentObject =
                serde_json::from_slice(&bytes)
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            if abandonment.store_root_hash != self.root.reference().store_root_hash
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

    async fn load_commit_join_cleanup_receipts(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
    ) -> Result<Vec<LoadedCommitJoinCleanupReceipt>, RegistrationLoadError> {
        let mut receipts = Vec::with_capacity(commit.device_join_cleanup_receipts().len());
        for reference in commit.device_join_cleanup_receipts() {
            let context = ProtocolObjectContext::signed_plaintext(
                self.root.reference().store_root_hash,
                ProtocolObjectDomain::DeviceJoinCleanupReceipt,
            );
            let semantic_prefix =
                store_commit::device_join_cleanup_receipt_semantic_prefix(reference.attempt_id);
            let bytes = self
                .commit_verifier
                .read_protocol_object(&context, &reference.object, &semantic_prefix)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let receipt: device_join::DeviceJoinCleanupReceiptObject =
                serde_json::from_slice(&bytes)
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            if receipt.executor != commit.author_registration
                || receipt.membership != commit.membership_state
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup receipt differs from its activating predecessor"
                        .to_string(),
                ));
            }
            let attempt_ref = receipt.cancellation.attempt();
            let (attempt, owner) = self
                .load_device_join_attempt_and_owner(attempt_ref)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let attempt = self
                .validate_device_join_attempt_evidence(attempt, &owner.value)
                .await
                .map_err(registration_attempt_error)?;
            let expected_administrator = &attempt
                .attempt
                .value
                .provider_approval
                .request
                .offer
                .provider_admin;
            if activating_author.provider != expected_administrator.provider
                || attempt
                    .attempt
                    .value
                    .provider_approval
                    .request
                    .offer
                    .provider
                    != self.root.protocol().descriptor.provider
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup executor differs from its exact provider authority"
                        .to_string(),
                ));
            }
            reference
                .verify(&receipt, activating_author)
                .and_then(|_| receipt.verify(&attempt.attempt.value, activating_author))
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
            match &receipt.administrator_terminal {
                device_join::ProviderAdminJoinTerminal::Completed(_) => {}
                device_join::ProviderAdminJoinTerminal::Cancelled(closure) => {
                    let administrator = self
                        .load_registration(&closure.administrator_registration)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    closure
                        .verify(&administrator)
                        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                }
                device_join::ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
                    let executor = self
                        .load_registration(&revocation.executor)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    revocation
                        .verify(&executor)
                        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                }
            }
            match &receipt.joiner_terminal {
                device_join::JoinerJoinTerminal::Ready(_) => {}
                device_join::JoinerJoinTerminal::Cancelled(closure) => closure
                    .verify()
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?,
                device_join::JoinerJoinTerminal::WriteRevoked(revocation) => {
                    let executor = self
                        .load_registration(&revocation.executor)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    revocation
                        .verify(&executor)
                        .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                }
            }
            receipts.push(LoadedCommitJoinCleanupReceipt { receipt, attempt });
        }
        Ok(receipts)
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

    pub(crate) async fn validate_commit_acknowledgement(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
    ) -> Result<Option<(StoreAckRef, StoreAck)>, RegistrationLoadError> {
        let Some(reference) = commit.acknowledgement() else {
            return Ok(None);
        };
        let ack = self
            .load_store_ack(reference, activating_author)
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
            let snapshot_author = self
                .load_registration(&snapshot.author_registration)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let (_, metadata) = self
                .load_store_snapshot(
                    &snapshot.author_registration,
                    &snapshot_author.value,
                    &snapshot.snapshot,
                )
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
        reference: &crate::protocol::provider::StoreMemberProviderAccessGrantRef,
        administrator: &StoreDeviceRegistration,
    ) -> Result<
        VerifiedObject<crate::protocol::provider::StoreMemberProviderAccessGrant>,
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

    pub(super) fn verified_predecessor_state(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<ResolvedStoreDeviceState, StorePullError> {
        let states = self
            .history
            .commits
            .iter()
            .map(|(reference, verified)| (reference.clone(), verified.state_after.clone()))
            .collect::<BTreeMap<_, _>>();
        verified_merge_predecessor_state(&self.history.genesis, &states, commit)
    }

    pub(super) fn verified_membership_prefix(
        &self,
        predecessors: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
        verified_merge_membership_prefix(&self.history.commits, predecessors)
    }

    pub(super) fn retained_commit_proofs(
        &self,
    ) -> BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit> {
        self.history
            .commits
            .iter()
            .map(|(reference, verified)| (reference.clone(), verified.verified.clone()))
            .collect()
    }

    pub(super) fn verified_pull_candidate(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<pull::VerifiedPullCandidate> {
        self.history
            .commits
            .get(reference)
            .map(|commit| pull::VerifiedPullCandidate {
                verified: commit.verified.clone(),
                predecessor_membership: commit.predecessor_membership.clone(),
                registrations: commit.registrations.clone(),
                operations: commit.operations.clone(),
                membership_control: commit
                    .membership_control
                    .as_ref()
                    .map(|control| control.activations.clone()),
            })
    }

    pub(super) fn verifies_membership_head_activation(
        &self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        activation: &StoreBatchCommitRef,
    ) -> bool {
        self.history
            .commits
            .get(activation)
            .and_then(|commit| commit.membership_control.as_ref())
            .is_some_and(|control| control.verifies_head_activation(reference, head, activation))
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
        if !predecessor_verifies_provider_administrator(
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
        let membership = self
            .load_predecessor_membership(membership_state)
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
        let active_registrations = self
            .commit_verifier
            .load_active_registrations(&authority.device_state)
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
            self.root.reference(),
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
        let root = self.root.reference().clone();
        let mut accepted = snapshot_frontier.clone();
        for (registration_ref, registration) in state.common.active_registrations.values() {
            let stream_id = store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                registration_ref,
                store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
            let discovery = self
                .discover_merge_stream(registration_ref, registration, None)
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
        let mut retained_acknowledgements = BTreeMap::new();
        for (device_id, (registration_ref, registration)) in &state.common.active_registrations {
            let matching = acknowledgements
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
            retained_acknowledgements.insert(
                *device_id,
                store_commit::RetainedVerifiedActivatedAck {
                    chain: matching.chain.clone(),
                    activating_commit: matching.activating_commit.clone(),
                    activating_commit_value: matching.activating_commit_value.clone(),
                },
            );
        }
        let founder = self.commit_verifier.load_founder_registration().await?;
        let authority = crate::database::RetainedReplaySnapshotAuthority {
            store_root: self.root.reference().clone(),
            founder_registration: StoreDeviceRegistrationRef::from_registration(
                &founder.value,
                founder.object,
            ),
            snapshot: snapshot.reference.clone(),
            metadata: snapshot.meta.clone(),
            snapshot_cut,
            accepted_cut,
            device_state: state.common.device_state,
            active_registrations: state
                .common
                .active_registrations
                .into_iter()
                .map(|(device_id, (reference, value))| {
                    (
                        device_id,
                        store_commit::RetainedVerifiedRegistration { reference, value },
                    )
                })
                .collect(),
            acknowledgements: retained_acknowledgements,
        };
        VerifiedStoreSnapshotStability::from_authority(authority)
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
        let root = self.root.reference().clone();
        let head_prefix =
            store_commit::semantic_prefix_from_exact_object(&activation_head.object, ".json")
                .map_err(|error| StorePullError::Database(error.to_string()))?;
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_bytes = self
            .commit_verifier
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
        let current_membership = self
            .load_predecessor_membership(&witness_commit.value().membership_state)
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
        let predecessor_membership = self
            .load_predecessor_membership(&candidate_commit.membership_state)
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

    pub(crate) async fn load_commit_join_evidence(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
    ) -> Result<LoadedCommitJoinEvidence, RegistrationLoadError> {
        let loaded_cleanup = self
            .load_commit_join_cleanup_receipts(commit, activating_author)
            .await?;
        let mut attempts = BTreeMap::new();
        let mut cleanup_receipts = Vec::with_capacity(loaded_cleanup.len());
        for loaded in loaded_cleanup {
            let attempt = loaded.receipt.cancellation.attempt().clone();
            attempts.entry(attempt.clone()).or_insert(loaded.attempt);
            cleanup_receipts.push(CommitJoinCleanupReceiptEvidence {
                receipt: loaded.receipt,
                attempt,
            });
        }
        let references = commit
            .device_join_attempt_decisions()
            .iter()
            .filter_map(|decision| match decision {
                DeviceJoinAttemptDecisionRef::Attempt(reference) => Some(reference),
                DeviceJoinAttemptDecisionRef::Abandoned(_) => None,
            })
            .chain(
                commit
                    .device_join_outcomes()
                    .iter()
                    .map(|outcome| outcome.attempt()),
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        for reference in references {
            if attempts.contains_key(&reference) {
                continue;
            }
            let (attempt, owner) = self
                .load_device_join_attempt_and_owner(&reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let evidence = self
                .validate_device_join_attempt_evidence(attempt, &owner.value)
                .await
                .map_err(registration_attempt_error)?;
            attempts.insert(reference, evidence);
        }
        Ok(LoadedCommitJoinEvidence {
            attempts,
            cleanup_receipts,
        })
    }

    pub(crate) async fn validate_commit_join_outcomes(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
        join_evidence: &VerifiedCommitJoinEvidence,
        accepted: VerifiedMergePredecessorHistory<'_>,
    ) -> Result<BTreeMap<DeviceJoinOutcomeRef, VerifiedCommitJoinOutcome>, RegistrationLoadError>
    {
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join outcome activation has no exact predecessor authority".to_string(),
            )
        })?;
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome activation author is not an active Owner at its predecessor"
                    .to_string(),
            ));
        }
        let mut verified = BTreeMap::new();
        for outcome_ref in commit.device_join_outcomes() {
            if !accepted
                .contains_join_attempt(outcome_ref.attempt())
                .map_err(registration_attempt_error)?
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join outcome names an attempt absent from its predecessor history"
                        .to_string(),
                ));
            }
            let attempt = join_evidence
                .attempts
                .get(outcome_ref.attempt())
                .ok_or_else(|| {
                    RegistrationLoadError::Invalid(
                        "device join outcome has no verified exact attempt".to_string(),
                    )
                })?;
            if attempt.owner_registration != commit.author_registration
                || outcome_ref.slot() != &attempt.outcome_slot
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join outcome differs from its exact Owner attempt".to_string(),
                ));
            }
            let outcome = self
                .load_device_join_outcome(outcome_ref, activating_author)
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
            if matches!(&outcome.body, DeviceJoinOutcomeBody::Activated { .. })
                != activation.is_some()
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
                        attempt: attempt.clone(),
                        owner: activating_author.clone(),
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

    pub(crate) async fn registration_activation(
        &self,
        activated: &ActivatedStoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        activating_author: &StoreDeviceRegistration,
        predecessor: &MembershipChain,
        verified_join_outcomes: &BTreeMap<DeviceJoinOutcomeRef, VerifiedCommitJoinOutcome>,
    ) -> Result<StoreDeviceRegistrationActivation, RegistrationLoadError> {
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
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
                    || !predecessor_verifies_owner(
                        predecessor,
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
                let initial_ack = self
                    .load_store_ack(&readiness.initial_ack, registration)
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
                let node_value = self
                    .load_owner_recovery_node(node)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                let mut reached_ref = node.clone();
                let mut reached = node_value.clone();
                while let Some(predecessor_ref) = reached.predecessor.clone() {
                    let predecessor_node = self
                        .load_owner_recovery_node(&predecessor_ref)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    if predecessor_node.next_slot != *reached_ref.object.slot() {
                        return Err(RegistrationLoadError::Invalid(
                            "recovery node does not occupy its exact predecessor successor slot"
                                .to_string(),
                        ));
                    }
                    if predecessor_node.recovery_id != node_value.recovery_id {
                        return Err(RegistrationLoadError::Invalid(
                            "recovery predecessor belongs to another recovery operation"
                                .to_string(),
                        ));
                    }
                    reached_ref = predecessor_ref;
                    reached = predecessor_node;
                }
                if node_value.recovery_id != *recovery_id
                    || node_value.readiness.registration != activated.registration
                    || node_value.next_slot == *node.object.slot()
                    || registration.author_pubkey != node_value.owner_pubkey
                    || !predecessor_verifies_owner(
                        predecessor,
                        &node_value.membership,
                        &node_value.owner_pubkey,
                        &node_value.owner_grant,
                    )
                {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery node differs from its exact registration".to_string(),
                    ));
                }
                let initial_ack = self
                    .load_store_ack(&node_value.readiness.initial_ack, registration)
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

    pub(crate) async fn predecessor_commit_matching(
        &mut self,
        order: &store_commit::StoreCommitOrder,
        mut matches: PredecessorCommitPredicate<'_>,
    ) -> Result<Option<VerifiedStoreBatchCommit>, RegistrationLoadError> {
        let mut pending = order
            .predecessor
            .iter()
            .chain(order.dependencies.values())
            .cloned()
            .collect::<Vec<_>>();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let commit = self
                .load_ref(&reference)
                .await
                .map_err(registration_attempt_error)?;
            if matches(&commit) {
                return Ok(Some(commit));
            }
            pending.extend(commit.value().order.predecessor.iter().cloned());
            pending.extend(commit.value().order.dependencies.values().cloned());
        }
        Ok(None)
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
        let loaded = self.load_commit_join_evidence(commit, author).await;
        let loaded = loaded.map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let join_evidence = accepted.verify_commit_join_evidence(commit, loaded)?;
        let registrations = self
            .load_commit_registrations(commit, author, Some(membership), &join_evidence, accepted)
            .await;
        registrations.map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })
    }

    async fn load_commit_registrations(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
        join_evidence: &VerifiedCommitJoinEvidence,
        accepted: VerifiedMergePredecessorHistory<'_>,
    ) -> Result<
        Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
        RegistrationLoadError,
    > {
        if join_evidence.commit != *commit {
            return Err(RegistrationLoadError::Invalid(
                "verified device-join evidence belongs to another Store commit".to_string(),
            ));
        }
        if commit.acknowledgement().is_some() {
            self.validate_commit_acknowledgement(commit, activating_author)
                .await?;
        }
        if let Some(reference) = commit.reclaim_authorization() {
            let predecessor = predecessor.ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "reclaim authorization activation has no exact predecessor owner authority"
                        .to_string(),
                )
            })?;
            let opened = self
                .load_reclaim_authorization(reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let evidence = &opened.evidence.value;
            let authorization = &opened.authorization.value;
            let owner_authorized = authorization.authority.membership == commit.membership_state
                && predecessor_verifies_owner(
                    predecessor,
                    &authorization.authority.membership,
                    &evidence.author_pubkey,
                    &authorization.authority.owner_grant,
                );
            if evidence.author_pubkey != activating_author.author_pubkey || !owner_authorized {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim authorization signer is not an active Owner at its exact predecessor"
                        .to_string(),
                ));
            }
            // Each kind of activating authority is re-read differently, so the binding
            // between the evidence and the object it authorizes deleting dispatches on
            // which authority published the target.
            let target = evidence.claim.target();
            match target.activation() {
                store_reclaim::ReclaimActivation::Commit(activating_commit) => {
                    accepted.validate_commit_activated_reclaim_target(&target, activating_commit)
                }
                store_reclaim::ReclaimActivation::CircleSnapshotMetadata(activation) => {
                    validate_circle_snapshot_activated_reclaim_target(&target, &activation)
                }
                store_reclaim::ReclaimActivation::PackageBlobBinding(activation) => {
                    accepted.validate_package_bound_reclaim_target(&target, &activation)
                }
            }?;
        }
        if let Some(reference) = commit.reclaim_receipt() {
            let predecessor = predecessor.ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "reclaim receipt activation has no exact predecessor provider authority"
                        .to_string(),
                )
            })?;
            let opened = self
                .load_reclaim_receipt(reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let receipt = &opened.receipt.value;
            if receipt.executor != commit.author_registration
                || opened.executor != *activating_author
                || receipt.provider_admin_state != commit.membership_state
                || !predecessor_verifies_provider_administrator_grant(
                    predecessor,
                    &receipt.provider_admin_grant,
                    &receipt.executor,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim receipt signer is not the effective provider administrator at its exact predecessor"
                        .to_string(),
                ));
            }
            if accepted
                .find(|_, candidate| {
                    candidate.reclaim_authorization() == Some(&receipt.authorization)
                })
                .map_err(registration_attempt_error)?
                .is_none()
            {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim receipt authorization is absent from predecessor history".to_string(),
                ));
            }
        }
        let has_join_attempt = commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)));
        if has_join_attempt {
            validate_commit_join_attempts(commit, activating_author, predecessor, join_evidence)?;
        }
        let verified_join_outcomes = if commit.device_join_outcomes().is_empty() {
            BTreeMap::new()
        } else {
            Box::pin(self.validate_commit_join_outcomes(
                commit,
                activating_author,
                predecessor,
                join_evidence,
                accepted,
            ))
            .await?
        };
        let has_join_abandonment = commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Abandoned(_)));
        if has_join_abandonment {
            self.validate_commit_join_abandonments(commit, activating_author, predecessor)
                .await?;
        }
        if !commit.device_join_cleanup_receipts().is_empty() {
            accepted.validate_commit_join_cleanup_receipts(
                activating_author,
                predecessor,
                join_evidence,
            )?;
        }
        let mut registrations = Vec::with_capacity(commit.device_registrations().len());
        for activated in commit.device_registrations() {
            let registration = Box::pin(self.load_registration(&activated.registration))
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let predecessor = predecessor.ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "registration activation has no exact predecessor membership authority"
                        .to_string(),
                )
            })?;
            let authority = Box::pin(self.registration_activation(
                activated,
                &registration,
                activating_author,
                predecessor,
                &verified_join_outcomes,
            ))
            .await?;
            registrations.push((registration, authority));
        }
        Ok(registrations)
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
        self.verify_owner_conflict_acceptance_at_tips(acceptance, resolver_pubkey, tips)
            .await
    }

    pub(crate) async fn verify_refs(
        &mut self,
        tips: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), StorePullError> {
        let root = self.root.reference().clone();
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
                Box::pin(self.verify_resolution_activation_acceptance(&commit)).await?;
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
            let operations = Box::pin(self.commit_verifier.load_commit_device_operations(
                Some(&resolver),
                &commit,
                &authorized_predecessor,
                Some(&membership),
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            let acknowledgement = self
                .validate_commit_acknowledgement(&commit, &author)
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
            let owner_recovery = self
                .commit_verifier
                .verify_owner_recovery_activation(&commit)
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

pub(super) async fn open_merge_history_verifier<'storage>(
    authority: super::history::HistoryConstructionAuthority,
    storage: &'storage dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<MergeHistoryVerifier<'storage>, StorePullError> {
    open_merge_history_verifier_with_root(authority, storage, root)
        .await
        .map(|(_, verifier)| verifier)
}

pub(super) async fn open_merge_history_verifier_with_root<'storage>(
    authority: super::history::HistoryConstructionAuthority,
    storage: &'storage dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<
    (
        super::super::protocol_root::VerifiedStoreRoot,
        MergeHistoryVerifier<'storage>,
    ),
    StorePullError,
> {
    let verified_root =
        crate::sync::store::protocol_root::load_pinned_store_protocol_root(storage, root)
            .await
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    let verified_root = super::super::protocol_root::VerifiedStoreRoot::from_verified_object(
        root.clone(),
        verified_root,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    let commit_verifier =
        StoreCommitVerifier::from_verified_root(authority, storage, verified_root.clone());
    let verifier = MergeHistoryVerifier::from_commit_verifier(
        authority,
        verified_root.clone(),
        commit_verifier,
    )
    .await?;
    Ok((verified_root, verifier))
}
