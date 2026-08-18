use super::commit::{
    StoreCommitVerifier, StoreMembershipObjectVerifier, VerifiedMergeMembershipClosure,
};
use crate::sync::store::pull;
use crate::sync::store::pull::*;
use crate::sync::store::StoreError;
use coven_database::VerifiedStoreSnapshotStability;
use coven_database::{
    DeviceJoinBootstrapActivation, DeviceJoinBootstrapCommit, DeviceJoinBootstrapPlan,
};
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::circle_control::StoreMembershipStateRef;
use coven_protocol::membership::{MembershipChain, MembershipStatus};
use coven_protocol::objects::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use coven_protocol::objects::{StoreObjectError, VerifiedObject};
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, ActivatedStoreDeviceRegistrationRef, CommitFrontier,
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinDisposition, DeviceStreamAnchor,
    ObjectHash, OpenedRetainedMergeHistorySummary, OwnerRecoveryNode, OwnerRecoveryNodeRef,
    ReferencedStoreDeviceRegistration, ResolvedStoreDeviceState,
    RetainedVerifiedMergeHistorySummary, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceProposalState, StoreDeviceRegistration,
    StoreDeviceRegistrationActivation, StoreDeviceRegistrationActivationRef,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    StoreDeviceStatus, StoreHistoryCut, StoreProtocolError, StoreRootRef, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
};
use coven_protocol::store_commit::{
    DeviceJoinAttemptRef, DeviceJoinOutcome, DeviceJoinOutcomeRef, SnapshotMeta, StoreAck,
    StoreAckRef, StoreDeviceExclusionOutcomeRef, StoreDeviceExclusionProposalRef,
    StoreDeviceHeadRef, StoreSnapshotRef, VerifiedDeviceExclusionOutcome,
    VerifiedDeviceExclusionProposal,
};
use coven_protocol::{
    causal_grants, membership as protocol_membership, provider, remote_object, store_commit,
};
use std::collections::{BTreeMap, BTreeSet};

use super::commit::DeviceStateResolver;
use crate::sync::store::device_join;

mod device_join_verification;
mod loaders;
mod membership_control;
mod predecessor;
use predecessor::{
    predecessor_verifies_provider_administrator, predecessor_verifies_provider_administrator_grant,
};
mod promotion;
mod snapshots;
mod stream;
mod successor;
pub use membership_control::VerifiedMergeMembershipPrefix;
pub(crate) use membership_control::{
    verified_merge_membership_prefix, verify_merge_membership_state_ref,
    VerifiedMergeMembershipControl, VerifiedMergeMembershipHeadActivation,
    VerifiedMergePrefixHeadStatus,
};
pub(crate) use predecessor::{predecessor_verifies_owner, VerifiedMergePredecessorHistory};
pub(crate) use promotion::{
    VerifiedMergeConflictResolutionActivation, VerifiedOwnerPromotionRequestActivation,
};
pub(crate) use snapshots::SelectedStableStoreSnapshot;
pub use successor::MergeHistorySuccessorEvidence;
pub use successor::PreparedMergeHistorySuccessor;
pub(crate) use successor::{
    compose_merge_snapshot_history_summary, compose_verified_merge_snapshot_history_summary,
};
#[cfg(test)]
pub(crate) use successor::{insert_latest_acknowledgement, merge_retained_merge_history};
pub(super) mod join_validation;
mod membership;
use membership::VerifiedPrefixMembershipActivation;
pub(crate) mod registration;
use join_validation::*;
pub(crate) use registration::RegistrationLoadError;
use registration::*;

pub(crate) struct VerifiedMergeHistoryCommit {
    pub(crate) verified: VerifiedStoreBatchCommit,
    pub(crate) predecessor_membership: MembershipChain,
    pub(crate) predecessor_state: ResolvedStoreDeviceState,
    pub(crate) state_after: ResolvedStoreDeviceState,
    pub(crate) registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub(crate) operations: VerifiedStoreDeviceOperations,
    pub(crate) acknowledgement: Option<(store_commit::StoreAckRef, store_commit::StoreAck)>,
    pub(crate) membership_control: Option<VerifiedMergeMembershipControl>,
    pub(crate) activation_head: StoreDeviceHead,
    pub(crate) activation_head_object: ExactObjectRef,
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
                    && verified.membership.resolution_refs() == state.resolutions
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
                && verified.membership.resolution_refs() == membership.resolution_refs()
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
        let chain = self
            .load_acknowledgement_proof_chain(reference, value, registration)
            .await
            .map_err(StorePullError::from)?;
        Ok(store_commit::RetainedVerifiedActivatedAck {
            chain,
            activating_commit: activating_commit.clone(),
            activating_commit_value: activating_commit_value.clone(),
        })
    }

    pub(crate) async fn load_local_device_operations_with_resolver(
        &mut self,
        resolver: &DeviceStateResolver<'_>,
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
            Some(resolver),
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
            .apply_to(authorized_predecessor, &commit.device_state)
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

    pub(crate) async fn from_commit_verifier(
        _authority: crate::sync::store::authorization::HistoryConstructionAuthority,
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
        commit_verifier: StoreCommitVerifier<'a>,
    ) -> Result<Self, StorePullError> {
        let founder = commit_verifier.load_founder_registration().await?;
        Self::from_commit_verifier_and_founder(root, commit_verifier, &founder)
    }

    fn from_commit_verifier_and_founder(
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
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
            return Err(StorePullError::InvalidState(
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
        .map_err(StorePullError::Protocol)?;
        Ok(Self {
            root,
            commit_verifier,
            founder: founder.clone(),
            history: VerifiedMergeHistory {
                genesis,
                commits: BTreeMap::new(),
            },
            verified_memberships: Vec::new(),
            verified_acknowledgements: std::sync::Mutex::new(BTreeMap::new()),
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
            .map_err(RegistrationLoadError::Object)?
            .value;
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
                .map_err(RegistrationLoadError::from)?;
            if !ack.store_cut.frontier().covers(&metadata.coverage) {
                return Err(RegistrationLoadError::Invalid(
                    "Store acknowledgement does not cover its exact snapshot".to_string(),
                ));
            }
        }
        Ok(Some((reference.clone(), ack)))
    }

    pub(crate) fn remember(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        self.commit_verifier.remember(commit)
    }

    pub(crate) async fn retain_local_same_principal_join_activation(
        &mut self,
        materialization: coven_database::OwnedVerifiedMergeMaterialization,
    ) -> Result<(), StorePullError> {
        let reference = materialization.commit_ref().clone();
        self.verify_refs(commit_predecessor_references(materialization.commit()))
            .await?;
        if let Some(existing) = self.history.commits.get(&reference) {
            if existing.verified.value() == materialization.commit()
                && existing.verified.author() == materialization.verified_commit().author()
            {
                return Ok(());
            }
            return Err(StorePullError::InvalidState(
                "local join activation conflicts with its already-verified Store commit"
                    .to_string(),
            ));
        }
        let commit = materialization.commit();
        if commit.control().is_some()
            || commit.acknowledgement().is_some()
            || commit.device_join_attempt_decisions().len() != 1
            || commit.device_join_outcomes().len() != 1
            || commit.device_registrations().len() != 1
            || materialization.registrations().len() != 1
        {
            return Err(StorePullError::InvalidState(
                "local same-principal activation is not one exact join operation".to_string(),
            ));
        }
        let predecessor_cut = commit
            .order
            .predecessor_cut()
            .map_err(StorePullError::Protocol)?;
        let authority = self.verify_merge_history_authority_from_verified_history(
            &predecessor_cut.0,
            &commit.membership_state,
        )?;
        let predecessor_state = authority.device_state;
        let registrations = materialization.registrations().to_vec();
        let operations = materialization.device_operations().clone();
        let state_after = self
            .derive_local_post_device_state(
                commit,
                predecessor_state.clone(),
                &registrations,
                operations.clone(),
            )
            .await?;
        let verified = materialization.verified_commit().clone();
        let activation_head = materialization.activation_head().clone();
        let activation_head_object = materialization.activation_head_object().clone();
        let history_evidence = materialization.history_evidence().clone();
        self.history.commits.insert(
            reference,
            VerifiedMergeHistoryCommit {
                verified,
                predecessor_membership: authority.membership,
                predecessor_state,
                state_after,
                registrations,
                operations,
                acknowledgement: None,
                membership_control: None,
                activation_head,
                activation_head_object,
                history_evidence,
            },
        );
        Ok(())
    }

    pub(crate) fn verified_predecessor_state(
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

    pub(crate) fn verified_membership_prefix(
        &self,
        predecessors: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
        verified_merge_membership_prefix(&self.history.commits, predecessors)
    }

    pub(crate) fn retained_commit_proofs(
        &self,
    ) -> BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit> {
        self.history
            .commits
            .iter()
            .map(|(reference, verified)| (reference.clone(), verified.verified.clone()))
            .collect()
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

    pub(crate) fn verified_predecessor_membership(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&MembershipChain> {
        self.history
            .commits
            .get(reference)
            .map(|commit| &commit.predecessor_membership)
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

    pub(super) async fn verify_membership_head_activation(
        &mut self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        activation: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        let verified = self.load_ref(activation).await?;
        let commit = verified.value();
        let author = verified.author();
        let transition = commit
            .control()
            .map(|control| &control.transition)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "membership head activation commit has no Merge membership transition"
                        .to_string(),
                )
            })?;
        if !transition.matches_head(head, reference)
            || transition.body.author_registration != commit.author_registration
        {
            return Err(StorePullError::InvalidState(
                "membership head differs from its exact activating Store transition".to_string(),
            ));
        }
        let activation_observation = self
            .exact_next_announcement_slot(&commit.author_registration, author, Some(&verified))
            .await;
        match activation_observation {
            Ok((_, Some(_))) => {}
            Ok((_, None)) => return Ok(false),
            Err(StoreError::MergeAnnouncementOccupied { .. })
            | Err(StoreError::Object(coven_protocol::objects::StoreObjectError::Storage(
                StorageError::NotFound(_),
            ))) => return Ok(false),
            Err(error) => return Err(StorePullError::Store(Box::new(error))),
        }
        self.verify_refs([activation.clone()]).await?;
        if !self.verifies_membership_head_activation(reference, head, activation) {
            return Err(StorePullError::InvalidState(
                "membership head activation differs from its verified Merge membership control"
                    .to_string(),
            ));
        }
        Ok(true)
    }

    pub(super) async fn verify_device_join_attempt_evidence(
        &mut self,
        evidence: LoadedDeviceJoinAttemptEvidence,
    ) -> Result<VerifiedObject<store_commit::DeviceJoinAttempt>, StorePullError> {
        let frontier = &evidence.attempt.value.bootstrap_cut.0;
        let authority = self
            .verify_merge_history_authority(frontier, &evidence.attempt.value.membership)
            .await?;
        let approval = &evidence.attempt.value.provider_approval;
        let provider_admin = &approval.request.offer.provider_admin;
        let verifies_administrator = match approval.access_grant() {
            None => predecessor_verifies_provider_administrator(
                &authority.membership,
                &provider_admin.grant_id,
                &provider_admin.administrator,
                provider_admin,
            ),
            Some(access) => self
                .history
                .commits
                .get(&access.activation)
                .is_some_and(|verified| {
                    predecessor_verifies_provider_administrator(
                        &verified.predecessor_membership,
                        &access.grant.administrator_grant,
                        &verified.verified.value().author_registration,
                        provider_admin,
                    )
                }),
        };
        if !verifies_administrator {
            return Err(StorePullError::InvalidState(
                "device join attempt lacks exact Merge provider-administrator authority"
                    .to_string(),
            ));
        }
        Ok(evidence.attempt)
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
                    &membership_state.resolutions,
                    &verified_membership_activations,
                    None,
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

    pub(crate) fn verify_merge_history_authority_from_verified_history(
        &self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<VerifiedMergeHistoryAuthority, StorePullError> {
        let (device_state, verified_membership_activations) =
            self.verified_merge_history_authority_parts(frontier)?;
        let membership = self
            .cached_verified_membership(membership_state, &verified_membership_activations)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "Merge membership authority is absent from the already-verified history"
                        .to_string(),
                )
            })?;
        verified_membership_activations.validate_complete_membership(&membership)?;
        verify_merge_membership_state_ref(membership_state, &membership, &device_state)?;
        Ok(VerifiedMergeHistoryAuthority {
            device_state,
            membership,
        })
    }

    fn verified_merge_history_authority_parts(
        &self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
    ) -> Result<(ResolvedStoreDeviceState, VerifiedMergeMembershipPrefix), StorePullError> {
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
                                StorePullError::InvalidState(
                                    "Merge history frontier is absent from its verified graph"
                                        .to_string(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(StorePullError::Protocol)?
        };
        let membership =
            verified_merge_membership_prefix(&self.history.commits, frontier.values().cloned())?;
        Ok((device_state, membership))
    }
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
            StorePullError::InvalidState(
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
        return Err(StorePullError::InvalidState(
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
                            StorePullError::InvalidState(
                                "Merge device-state frontier is absent from its verified history"
                                    .to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(StorePullError::Protocol)?
    };
    let expected = StoreDeviceStateRef::from_resolved(frontier.clone(), &state)
        .map_err(StorePullError::Protocol)?;
    if &expected != reference {
        return Err(StorePullError::InvalidState(
            "Merge device-state reference differs from its verified history".to_string(),
        ));
    }
    Ok(state)
}

pub(crate) struct VerifiedMergeHistory {
    pub(crate) genesis: ResolvedStoreDeviceState,
    pub(crate) commits: BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
}

struct VerifiedMembershipChain {
    authority: VerifiedMergeMembershipPrefix,
    membership: MembershipChain,
}

pub struct MergeHistoryVerifier<'a> {
    root: crate::sync::store::protocol_root::VerifiedStoreRoot,
    commit_verifier: StoreCommitVerifier<'a>,
    founder: VerifiedObject<StoreDeviceRegistration>,
    history: VerifiedMergeHistory,
    verified_memberships: Vec<VerifiedMembershipChain>,
    /// Exact acknowledgement objects already authenticated under this
    /// verifier's Store root and registration. Retained commits share long
    /// prefixes, so walking each proof back to sequence one must reuse them.
    verified_acknowledgements: std::sync::Mutex<BTreeMap<ExactObjectRef, (StoreAckRef, StoreAck)>>,
}

type PredecessorCommitPredicate<'a> = Box<dyn FnMut(&VerifiedStoreBatchCommit) -> bool + Send + 'a>;

pub struct MergeOutboundAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) membership_state: StoreMembershipStateRef,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}
