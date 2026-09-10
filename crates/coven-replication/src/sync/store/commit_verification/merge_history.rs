use super::commit::{
    StoreCommitVerifier, StoreMembershipObjectVerifier, VerifiedMergeMembershipClosure,
};
use crate::sync::store::pull;
use crate::sync::store::pull::*;
use coven_database::VerifiedStoreSnapshotAuthority;
use coven_database::{DeviceJoinBootstrapCommit, DeviceJoinBootstrapPlan};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::circle_control::StoreMembershipStateRef;
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::objects::{StoreObjectError, VerifiedObject};
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, ActivatedStoreDeviceRegistrationRef, CommitFrontier,
    DeviceJoinAttemptDecisionRef, OpenedRetainedMergeHistorySummary, OwnerRecoveryNode,
    OwnerRecoveryNodeRef, ReferencedStoreDeviceRegistration, ResolvedStoreDeviceState,
    RetainedVerifiedMergeHistorySummary, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceProposalState, StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreDeviceStatus, StoreHistoryCut,
    StoreProtocolError, StoreRootRef, VerifiedStoreBatchCommit, VerifiedStoreDeviceOperations,
};
use coven_protocol::store_commit::{
    SnapshotMeta, StoreAck, StoreAckRef, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProposalRef, StoreSnapshotRef, VerifiedDeviceExclusionOutcome,
    VerifiedDeviceExclusionProposal,
};
use coven_protocol::{membership as protocol_membership, provider, store_commit};
use std::collections::{BTreeMap, BTreeSet};

use crate::sync::store::device_join;

mod acknowledgements;
mod device_join_verification;
mod loaders;
mod membership_control;
mod nonactivation;
mod predecessor;
use predecessor::{
    predecessor_verifies_provider_administrator, predecessor_verifies_provider_administrator_grant,
};
mod promotion;
mod publication;
mod rollup;
mod snapshot_retirement;
mod snapshots;
mod stream;
mod successor;
pub use membership_control::VerifiedMergeMembershipPrefix;
pub(crate) use membership_control::{
    merge_membership_state_ref, verified_merge_membership_prefix,
    verify_merge_membership_state_ref, VerifiedMergeMembershipControl,
    VerifiedMergeMembershipHeadActivation, VerifiedMergePrefixHeadStatus,
};
pub(crate) use predecessor::{predecessor_verifies_owner, VerifiedMergePredecessorHistory};
pub(crate) use promotion::VerifiedOwnerPromotionRequestActivation;
pub(crate) use publication::{
    AcceptedStoreSnapshot, StorePublicationReplayInstallation, VerifiedStorePublication,
};
pub(crate) use snapshots::SelectedStoreSnapshot;
pub use successor::MergeHistorySuccessorEvidence;
pub use successor::PreparedMergeHistorySuccessor;
pub(crate) use successor::{
    compose_merge_snapshot_history_summary, compose_verified_merge_snapshot_history_summary,
    validate_composed_snapshot_history_summary,
};
#[cfg(test)]
pub(crate) use successor::{insert_latest_acknowledgement, merge_retained_merge_history};
pub(super) mod join_validation;
mod membership;
pub use membership::AcceptedMembershipAuthority;
use membership::VerifiedPrefixMembershipActivation;
pub(crate) mod registration;
use join_validation::*;
pub(crate) use registration::RegistrationLoadError;
use registration::*;

#[derive(Clone)]
pub(crate) struct VerifiedMergeHistoryCommit {
    pub(crate) verified: VerifiedStoreBatchCommit,
    pub(crate) predecessor_membership: MembershipChain,
    pub(crate) predecessor_state: ResolvedStoreDeviceState,
    pub(crate) state_after: ResolvedStoreDeviceState,
    pub(crate) registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub(crate) operations: VerifiedStoreDeviceOperations,
    pub(crate) membership_control: Option<VerifiedMergeMembershipControl>,
    pub(crate) history_evidence: store_commit::RetainedMergeCommitEvidence,
}

pub(crate) struct VerifiedMergeHistoryAuthority {
    pub(crate) device_state: ResolvedStoreDeviceState,
    pub(crate) membership: MembershipChain,
}

impl<'a> MergeHistoryVerifier<'a> {
    fn cached_verified_membership(
        &self,
        state: &StoreMembershipStateRef,
        authority: &VerifiedMergeMembershipPrefix,
    ) -> Option<MembershipChain> {
        self.verified_memberships
            .iter()
            .rev()
            .find(|verified| {
                verified.membership.head_refs() == state.heads
                    && authority.extends(&verified.authority)
            })
            .map(|verified| verified.membership.clone())
    }

    fn remember_verified_membership(
        &mut self,
        authority: VerifiedMergeMembershipPrefix,
        membership: MembershipChain,
    ) {
        if self.verified_memberships.iter().any(|verified| {
            verified.membership.head_refs() == membership.head_refs()
                && authority.extends(&verified.authority)
        }) {
            return;
        }
        self.verified_memberships.push(VerifiedMembershipChain {
            authority,
            membership,
        });
    }

    pub(crate) fn verified_root(&self) -> &crate::sync::store::protocol_root::VerifiedStoreRoot {
        &self.root
    }

    pub(crate) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'a> {
        self.commit_verifier.membership_objects()
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
            return Err(StorePullError::InvalidState(
                "Store acknowledgement differs from its activating commit".to_string(),
            ));
        }
        activating_commit
            .verify_commit(activating_commit_value)
            .map_err(StorePullError::Protocol)?;
        // Only the acknowledgement this commit activated. Its predecessors are
        // retained beside the commits that activated them, and each
        // acknowledgement names the object of the one before it, so the chain is
        // walkable across rows. Walking it here and storing the result made the
        // row grow with the history in front of it, and cost a provider read per
        // link at the moment of applying a commit.
        let object = value.to_bytes();
        reference
            .object
            .verify(&object)
            .map_err(|error| StorePullError::context("retained acknowledgement object", error))?;
        StoreAck::parse_at(&object, self.root.reference(), &reference, registration)
            .map_err(StorePullError::Protocol)?;
        Ok(store_commit::RetainedVerifiedActivatedAck {
            acknowledgement: (reference, value),
            activating_commit: activating_commit.clone(),
            predecessors: Vec::new(),
        })
    }

    pub(crate) async fn load_local_device_operations(
        &mut self,
        verified_commit: &VerifiedStoreBatchCommit,
        membership: &MembershipChain,
        state_ref: &StoreDeviceStateRef,
        state: ResolvedStoreDeviceState,
    ) -> Result<VerifiedStoreDeviceOperations, StorePullError> {
        if verified_commit.store_root_hash() != self.root.reference().store_root_hash {
            return Err(StorePullError::InvalidState(
                "local device-operation commit belongs to another Store root".to_string(),
            ));
        }
        let commit = verified_commit.value();
        if commit.device_exclusion_proposals().is_empty()
            && commit.device_exclusion_outcomes().is_empty()
        {
            return VerifiedStoreDeviceOperations::without_exclusions(commit)
                .map_err(StorePullError::Protocol);
        }
        if state_ref != &commit.device_state {
            return Err(StorePullError::InvalidState(
                "local exclusion commit differs from its materialized predecessor device state"
                    .to_string(),
            ));
        }
        verify_merge_membership_state_ref(&commit.membership_state, membership, &state)?;
        Box::pin(self.commit_verifier.load_commit_device_operations(
            commit,
            &state,
            Some(membership),
        ))
        .await
        .map_err(StorePullError::from)
    }

    pub(crate) async fn derive_local_post_device_state(
        &self,
        commit: &StoreBatchCommit,
        predecessor_state: ResolvedStoreDeviceState,
        registrations: &[ActivatedStoreDeviceRegistration],
        device_operations: VerifiedStoreDeviceOperations,
    ) -> Result<ResolvedStoreDeviceState, StorePullError> {
        let (authorized_predecessor, recovery_author) = predecessor_state
            .preactivate_recovery_author(commit, registrations)
            .map_err(StorePullError::Protocol)?;
        let owner_recovery = self
            .commit_verifier
            .verify_owner_recovery_activation(commit)
            .await?;
        device_operations
            .apply_to(authorized_predecessor)
            .and_then(|state| {
                state.apply_verified_lifecycle(
                    commit,
                    registrations,
                    recovery_author.as_ref(),
                    owner_recovery,
                )
            })
            .map_err(StorePullError::Protocol)
    }

    /// Bind a history verifier to its Store root.
    ///
    /// Reads the founder once, to confirm it belongs to this root and to derive
    /// the genesis device state, then keeps only its reference. The registration
    /// itself stays where every other one does — the commit verifier's
    /// registration cache — so asking for it later is a lookup, not a copy held
    /// here as well.
    pub(crate) async fn from_commit_verifier(
        _authority: crate::sync::store::authorization::HistoryConstructionAuthority,
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
        commit_verifier: StoreCommitVerifier<'a>,
    ) -> Result<Self, StorePullError> {
        let founder = commit_verifier.load_founder_registration().await?;
        let founder = &founder;
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
            return Err(StorePullError::InvalidState(
                "verified founder registration belongs to another Store root".to_string(),
            ));
        }
        let genesis = ResolvedStoreDeviceState::founder(
            root.reference(),
            founder_ref.clone(),
            &verified_root.descriptor.founder_pubkey,
            verified_root.descriptor.founder_grant.clone(),
            &verified_root.descriptor.founder_recovery,
        )
        .map_err(StorePullError::Protocol)?;
        Ok(Self {
            root,
            commit_verifier,
            founder: founder_ref,
            accepted_publications: BTreeMap::new(),
            history: VerifiedMergeHistory {
                genesis,
                baseline: coven_database::InstalledReplayBaseline::default(),
                retained: BTreeMap::new(),
                commits: BTreeMap::new(),
            },
            verified_memberships: Vec::new(),
        })
    }

    pub(crate) async fn covered_reference_status(
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
                    return MaterializedCheck::Held(
                        HeldStorePositionReason::ObjectUnreadablePull {
                            key: "exact Store commit".to_string(),
                            source: error.into(),
                        },
                    );
                }
            };
            let Some(predecessor) = verified_commit.value().order.predecessor() else {
                return MaterializedCheck::Missing;
            };
            cursor = predecessor.clone();
        }
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
            .map_err(RegistrationLoadError::Object)?;
        let predecessor_cut = commit
            .order
            .predecessor_cut()
            .map_err(RegistrationLoadError::from)?;
        if ack.registration != commit.author_registration
            || ack.store_cut != predecessor_cut
            || ack.device_state != commit.device_state
        {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement differs from its activating commit predecessor".to_string(),
            ));
        }
        Ok(Some((reference.clone(), ack)))
    }

    pub(crate) fn remember(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        self.commit_verifier.remember(commit)
    }

    /// Use the database's committed replay baseline as the floor of history walks.
    ///
    /// A writer can advance that baseline while keeping this verifier alive.
    /// When it changes, discard conclusions derived from the previous baseline;
    /// retained inputs are admitted and verified against the replacement next.
    /// Independently authenticated object bytes remain reusable.
    pub(crate) fn admit_installed_baseline(
        &mut self,
        baseline: coven_database::InstalledReplayBaseline,
    ) -> Result<(), StorePullError> {
        let baseline_changed = self.history.baseline.coverage() != baseline.coverage()
            || self
                .history
                .baseline
                .snapshot()
                .map(|snapshot| &snapshot.reference)
                != baseline.snapshot().map(|snapshot| &snapshot.reference);
        if baseline_changed {
            self.history.retained.clear();
            self.history.commits.clear();
            self.accepted_publications.clear();
            self.verified_memberships.clear();
        }
        // Every acknowledgement the baseline's signed summary states, admitted
        // before anything walks a chain. A chain walk demands contiguity from
        // sequence one, and the acknowledgements under the coverage are exactly
        // the rows the advance retired — so without this a device pays one
        // provider read per acknowledgement it has ever made, every time it
        // verifies a snapshot. The owner signed those chains into the snapshot
        // this baseline stands on: covered positions resolve to the coverage,
        // here as everywhere else.
        if let Some(snapshot) = baseline.snapshot() {
            let summary = &snapshot.meta.history_summary;
            for proof in summary.membership_proofs.values() {
                self.membership_objects().remember_retained_proof(proof)?;
            }
            for reference in summary.causal_cut.values() {
                self.accepted_publications
                    .entry(reference.clone())
                    .or_insert(AcceptedStoreCommitEvidence::SnapshotCovered);
            }
            for chain in summary.acknowledgements.values() {
                for (reference, value) in chain.chain.values() {
                    self.commit_verifier
                        .remember_acknowledgement(reference, value)
                        .map_err(StorePullError::Protocol)?;
                }
            }
        }
        self.history.baseline = baseline;
        Ok(())
    }

    pub(crate) fn admit_retained_history(
        &mut self,
        retained: &[coven_database::OwnedVerifiedMergeMaterialization],
    ) -> Result<(), StorePullError> {
        for materialization in retained {
            if let Some(proof) = &materialization.history_evidence().membership_proof {
                self.membership_objects().remember_retained_proof(proof)?;
            }
            let commit_ref = materialization.commit_ref();
            self.history
                .retained
                .insert(commit_ref.clone(), materialization.registrations().to_vec());
            self.accepted_publications.insert(
                commit_ref.clone(),
                match materialization.acceptance().exact_publication() {
                    Some(publication) => AcceptedStoreCommitEvidence::Exact(publication.clone()),
                    None => AcceptedStoreCommitEvidence::SnapshotCovered,
                },
            );
            self.commit_verifier
                .remember(materialization.verified_commit().clone())
                .map_err(StorePullError::Protocol)?;
            // The one acknowledgement this commit activated. Across the retained
            // rows that is every acknowledgement the device has made, which is
            // what the chain walk used to re-read from the provider per commit —
            // the rows hold it between them rather than each holding all of it.
            if let Some(activated) = &materialization.history_evidence().acknowledgement {
                for (reference, value) in activated.proof_objects() {
                    self.commit_verifier
                        .remember_acknowledgement(reference, value)
                        .map_err(StorePullError::Protocol)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn retain_local_same_principal_join_activation(
        &mut self,
        materialization: coven_database::OwnedVerifiedMergeMaterialization,
    ) -> Result<(), StorePullError> {
        let reference = materialization.commit_ref().clone();
        self.admit_retained_history(std::slice::from_ref(&materialization))?;
        self.verify_refs([reference]).await
    }

    pub(crate) fn verified_predecessor_state(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<ResolvedStoreDeviceState, StorePullError> {
        let frontier = commit.order.predecessor_cut()?.frontier();
        let state = self.history.state_at_frontier(&frontier)?;
        if commit.device_state != StoreDeviceStateRef::from_resolved(frontier, &state)? {
            return Err(StorePullError::InvalidState(
                "Merge commit names another predecessor device state".into(),
            ));
        }
        Ok(state)
    }

    pub(crate) fn verified_membership_prefix(
        &self,
        predecessors: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
        verified_merge_membership_prefix(&self.history, predecessors)
    }

    pub(crate) fn verified_pull_candidate(
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

    pub(crate) fn verify_commit_acceptance(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<(), StorePullError> {
        if !self.accepted_publications.contains_key(reference) {
            return Err(StorePullError::InvalidState(
                "Store commit has no verified acceptance evidence".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn accepted_publication(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&coven_database::AcceptedStoreCommitPublication> {
        match self.accepted_publications.get(reference) {
            Some(AcceptedStoreCommitEvidence::Exact(publication)) => Some(publication),
            Some(AcceptedStoreCommitEvidence::SnapshotCovered) | None => None,
        }
    }

    pub(super) async fn verify_membership_head_activation(
        &mut self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        activation: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        if !self.accepted_publications.contains_key(activation) {
            return Ok(false);
        }
        self.verify_refs([activation.clone()]).await?;
        let prefix = verified_merge_membership_prefix(&self.history, [activation.clone()])?;
        if !prefix
            .head_activation(activation)
            .is_some_and(|proof| proof.verifies(reference, head, activation))
        {
            return Err(StorePullError::InvalidState(
                "membership head activation differs from its verified Merge membership control"
                    .to_string(),
            ));
        }
        Ok(true)
    }

    pub(crate) async fn verify_merge_history_authority(
        &mut self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<VerifiedMergeHistoryAuthority, StorePullError> {
        self.verify_refs(frontier.values().cloned()).await?;
        let (device_state, verified_membership_activations) =
            self.verified_merge_history_authority_parts(frontier)?;
        let membership = match self
            .cached_verified_membership(membership_state, &verified_membership_activations)
        {
            Some(membership) => membership,
            None => self
                .load_membership_at_verified_prefix(
                    &membership_state.heads,
                    &verified_membership_activations,
                )
                .await
                .map_err(StorePullError::MembershipChain)?,
        };
        verified_membership_activations.validate_complete_membership(&membership)?;
        verify_merge_membership_state_ref(membership_state, &membership, &device_state)?;
        self.remember_verified_membership(verified_membership_activations, membership.clone());
        Ok(VerifiedMergeHistoryAuthority {
            device_state,
            membership,
        })
    }

    fn verified_merge_history_authority_parts(
        &self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
    ) -> Result<(ResolvedStoreDeviceState, VerifiedMergeMembershipPrefix), StorePullError> {
        let device_state = self
            .history
            .state_at_frontier(&CommitFrontier(frontier.clone()))?;
        let membership =
            verified_merge_membership_prefix(&self.history, frontier.values().cloned())?;
        Ok((device_state, membership))
    }
}

/// Every commit `tips` causally depends on, down to the installed baseline.
///
/// A covered reference is a member of the closure but not a step in the walk:
/// the baseline restates what stands there, and the commits behind it are
/// retired. Walking past one would demand history this device deliberately
/// dropped.
fn verified_merge_commit_closure(
    history: &VerifiedMergeHistory,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<BTreeSet<StoreBatchCommitRef>, StorePullError> {
    let mut pending = tips.into_iter().collect::<Vec<_>>();
    let mut closure = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !closure.insert(reference.clone()) {
            continue;
        }
        if history.superseded(&reference) {
            continue;
        }
        let verified = history.commits.get(&reference).ok_or_else(|| {
            StorePullError::InvalidState(
                "verified Merge predecessor closure is absent from its history".to_string(),
            )
        })?;
        pending.extend(commit_predecessor_references(verified.verified.value()));
    }
    Ok(closure)
}

#[derive(Clone)]
pub(crate) struct VerifiedMergeHistory {
    pub(crate) genesis: ResolvedStoreDeviceState,
    /// Where a walk down this history stops, and what it reads there.
    ///
    /// Below an installed baseline there is nothing to walk to: the commits are
    /// retired and their rows are restated by one signed image. The two ends of
    /// a history are the same shape — `genesis` is the state before the first
    /// commit, `baseline` is the state at the positions the image covers.
    pub(crate) baseline: coven_database::InstalledReplayBaseline,
    /// The commits this device still holds a retained materialization for, and
    /// the registrations that row proved active at each of them.
    ///
    /// A baseline image keeps a closure of rows at or under its own coverage —
    /// historical Circle epoch access, author-exclusion recovery — because
    /// those paths read the rows rather than a replay. Being covered therefore
    /// does not mean the commit is gone; holding no row for it does.
    pub(crate) retained: BTreeMap<StoreBatchCommitRef, Vec<ActivatedStoreDeviceRegistration>>,
    pub(crate) commits: BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
}

impl VerifiedMergeHistory {
    /// Whether the installed baseline stands in for `reference` outright: it
    /// restates the position and this device kept no row behind it. Nothing
    /// walks past such a reference, because there is nothing left to walk to.
    pub(crate) fn superseded(&self, reference: &StoreBatchCommitRef) -> bool {
        self.baseline.covers(reference) && !self.retained.contains_key(reference)
    }

    /// The registrations a retained row already proved active at its commit.
    ///
    /// Re-deriving them reads whatever the commit's body names from the
    /// provider — a reclaim authorization, its evidence, its receipt — on every
    /// pull, for a commit this device verified once and wrote a row for. The
    /// row was written by the transaction that verified and applied the commit,
    /// and opening it re-parses and re-checks the commit against its activated
    /// registration, so it is the answer rather than a cache of one.
    pub(crate) fn retained_registrations(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&[ActivatedStoreDeviceRegistration]> {
        self.retained
            .get(reference)
            .map(|registrations| registrations.as_slice())
    }

    /// The device state standing after `reference`, from the verified graph
    /// when it holds the commit and from the baseline when the commit is one it
    /// superseded.
    pub(crate) fn state_after(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&ResolvedStoreDeviceState> {
        self.commits
            .get(reference)
            .map(|commit| &commit.state_after)
            .or_else(|| self.baseline.covered_state(reference))
    }

    fn is_baseline_tip(&self, reference: &StoreBatchCommitRef) -> bool {
        self.baseline
            .coverage()
            .commits()
            .get(&reference.coord.stream_id)
            == Some(reference)
    }

    /// Resolve an exact cut from retained commit states or from the whole
    /// installed checkpoint and a verified suffix. The checkpoint's aggregate
    /// state is never attributed to an individual historical commit.
    fn state_at_frontier(
        &self,
        frontier: &CommitFrontier,
    ) -> Result<ResolvedStoreDeviceState, StorePullError> {
        if frontier.commits().is_empty() {
            return Ok(self.genesis.clone());
        }
        if frontier
            .commits()
            .values()
            .all(|reference| self.state_after(reference).is_some())
        {
            return ResolvedStoreDeviceState::merge(frontier.commits().values().map(|reference| {
                self.state_after(reference)
                    .expect("every exact frontier state was checked")
                    .clone()
            }))
            .map_err(StorePullError::Protocol);
        }
        let baseline = self.baseline.snapshot().ok_or_else(|| {
            StorePullError::InvalidState("Merge history has an unresolved predecessor state".into())
        })?;
        let mut pending = frontier.commits().values().cloned().collect::<Vec<_>>();
        let mut reached = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !reached.insert(reference.clone()) || self.is_baseline_tip(&reference) {
                continue;
            }
            let commit = self.commits.get(&reference).ok_or_else(|| {
                StorePullError::InvalidState(
                    "Merge device-state cut lacks exact checkpoint ancestry".into(),
                )
            })?;
            pending.extend(commit_predecessor_references(commit.verified.value()));
        }
        if self
            .baseline
            .coverage()
            .commits()
            .values()
            .any(|reference| !reached.contains(reference))
        {
            return Err(StorePullError::InvalidState(
                "Merge device-state cut does not include its complete checkpoint".into(),
            ));
        }
        ResolvedStoreDeviceState::merge(
            std::iter::once(baseline.meta.state.devices.clone()).chain(
                frontier
                    .commits()
                    .values()
                    .filter_map(|reference| self.state_after(reference).cloned()),
            ),
        )
        .map_err(StorePullError::Protocol)
    }
}

#[derive(Clone)]
struct VerifiedMembershipChain {
    authority: VerifiedMergeMembershipPrefix,
    membership: MembershipChain,
}

pub struct MergeHistoryVerifier<'a> {
    root: crate::sync::store::protocol_root::VerifiedStoreRoot,
    commit_verifier: StoreCommitVerifier<'a>,
    /// Which registration founded this Store. Established at construction from
    /// the founder this verifier validated against the root; the registration it
    /// names is held by `commit_verifier`, not again here.
    founder: StoreDeviceRegistrationRef,
    accepted_publications: BTreeMap<StoreBatchCommitRef, AcceptedStoreCommitEvidence>,
    history: VerifiedMergeHistory,
    verified_memberships: Vec<VerifiedMembershipChain>,
}

#[derive(Clone)]
enum AcceptedStoreCommitEvidence {
    Exact(coven_database::AcceptedStoreCommitPublication),
    SnapshotCovered,
}

type PredecessorCommitPredicate<'a> = Box<dyn FnMut(&VerifiedStoreBatchCommit) -> bool + Send + 'a>;

pub struct MergeOutboundAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) membership_state: StoreMembershipStateRef,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}
