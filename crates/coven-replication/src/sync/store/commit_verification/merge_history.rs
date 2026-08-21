use super::commit::{
    StoreCommitVerifier, StoreMembershipObjectVerifier, VerifiedMergeMembershipClosure,
};
use crate::sync::store::pull;
use crate::sync::store::pull::*;
use crate::sync::store::StoreError;
use coven_database::{
    DeviceJoinBootstrapActivation, DeviceJoinBootstrapCommit, DeviceJoinBootstrapPlan,
};
use coven_database::{VerifiedAcknowledgedStoreSnapshot, VerifiedStoreSnapshotAuthority};
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
pub(crate) use predecessor::{
    predecessor_verifies_owner, PredecessorSearch, VerifiedMergePredecessorHistory,
};
pub(crate) use promotion::{
    VerifiedMergeConflictResolutionActivation, VerifiedOwnerPromotionRequestActivation,
};
pub(crate) use snapshots::{SelectedAcknowledgedStoreSnapshot, SelectedInstallableStoreSnapshot};
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

    /// Admit the commits, announcement heads, and accepted announcement path
    /// this device has already verified, from the retained materialization rows
    /// that recorded them.
    ///
    /// The verifier's reuse memos (`commits`, `verified_heads`,
    /// `accepted_announcements`) exist so one cycle never reads the same
    /// protocol object twice, and they work — but they are built fresh with the
    /// verifier, which every cycle rebuilds. Retained history therefore paid one
    /// provider read per commit and one per activation head on every single
    /// cycle, for commits this device verified and materialized long ago.
    ///
    /// The durable half of that reuse already exists one layer down: a retained
    /// materialization row holds the commit's canonical bytes and its activation
    /// head's, pinned by an input hash, written by the transaction that verified
    /// and applied them, and re-parsed and signature-checked against the
    /// activated registration every time the row is opened. This seeds the
    /// per-cycle memos from that durable authority, so the verification below
    /// runs unchanged and reaches the provider only for what this device has not
    /// already verified.
    ///
    /// Nothing here is taken on trust: every value admitted came back through
    /// the same signature check a provider read would have run, and the
    /// `remember_*` entry points reject a value that disagrees with its
    /// reference or with an entry already admitted.
    /// Adopt the announcement position the installed Store snapshot restates
    /// for each stream, as the point a chain walk resumes from.
    ///
    /// Admitted before [`admit_retained_history`](Self::admit_retained_history)
    /// because it decides where the accepted path starts. Without it a device
    /// whose retained rows stop above the snapshot cut has no accepted prefix
    /// at all, and every walk falls back to the stream anchor and re-reads
    /// every head and commit under the cut, on every pull, for as long as the
    /// store exists.
    ///
    /// The authority is the one the baseline itself rests on: the owner signed
    /// this announcement into the snapshot's history summary alongside the
    /// state it restates, and the database refuses a frontier naming a commit
    /// its own coverage does not.
    pub(crate) fn admit_snapshot_announcements(
        &mut self,
        frontier: &BTreeMap<
            coven_protocol::causal_grants::AuthorStreamId,
            store_commit::RetainedAcceptedStoreAnnouncement,
        >,
    ) -> Result<(), StorePullError> {
        for announcement in frontier.values() {
            let head = &announcement.value;
            let head_ref = StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object: announcement.reference.object.clone(),
            };
            if announcement.reference != head_ref {
                return Err(StorePullError::InvalidState(
                    "snapshot announcement differs from its own head reference".to_string(),
                ));
            }
            self.commit_verifier
                .remember_verified_head(
                    &head_ref,
                    VerifiedObject {
                        value: head.clone(),
                        bytes: head.to_bytes(),
                        semantic_hash: head_ref.head_hash,
                        object: head_ref.object.clone(),
                    },
                )
                .map_err(StorePullError::Protocol)?;
            self.commit_verifier
                .remember_covered_announcement(
                    &head.author_registration,
                    crate::sync::store::commit_verification::commit::CoveredStoreAnnouncement {
                        sequence: head.commit.coord.sequence(),
                        commit: head.commit.clone(),
                        head: head_ref,
                        next_slot: head.successor.next_slot.clone(),
                    },
                )
                .map_err(StorePullError::Protocol)?;
        }
        Ok(())
    }

    /// Adopt the replay baseline this device stands on as the floor of every
    /// history walk this verifier runs.
    ///
    /// Admitted before the retained rows, for the same reason
    /// [`admit_snapshot_announcements`](Self::admit_snapshot_announcements) is:
    /// it decides where a walk stops, and a walk that starts before knowing
    /// that runs to genesis over commits the baseline retired.
    ///
    /// Refuses to replace a baseline already admitted with a different one. One
    /// verifier serves one operation, and a coverage that moves under it would
    /// silently change what the walks it already ran were allowed to skip.
    pub(crate) fn admit_installed_baseline(
        &mut self,
        baseline: coven_database::InstalledReplayBaseline,
    ) -> Result<(), StorePullError> {
        let installed = self.history.baseline.coverage();
        if !installed.commits().is_empty() && installed != baseline.coverage() {
            return Err(StorePullError::InvalidState(
                "installed replay baseline coverage moved under its history verifier".to_string(),
            ));
        }
        self.history.baseline = baseline;
        Ok(())
    }

    /// Whether this device's installed replay baseline already stands at or
    /// past `coverage`.
    ///
    /// A snapshot covering no more than the baseline has nothing this device
    /// can verify it against — the history behind it was retired — and nothing
    /// to offer it, because the baseline restates at least as much.
    pub(crate) fn replay_baseline_stands_past(&self, coverage: &CommitFrontier) -> bool {
        !coverage.covers(self.history.baseline.coverage())
    }

    pub(crate) fn admit_retained_history(
        &mut self,
        retained: &[coven_database::OwnedVerifiedMergeMaterialization],
    ) -> Result<(), StorePullError> {
        let mut announced = BTreeMap::<StoreDeviceRegistrationRef, u64>::new();
        for materialization in retained {
            let commit = materialization.commit();
            let commit_ref = materialization.commit_ref();
            self.history
                .retained
                .insert(commit_ref.clone(), materialization.registrations().to_vec());
            self.commit_verifier
                .remember(materialization.verified_commit().clone())
                .map_err(StorePullError::Protocol)?;
            // The one acknowledgement this commit activated. Across the retained
            // rows that is every acknowledgement the device has made, which is
            // what the chain walk used to re-read from the provider per commit —
            // the rows hold it between them rather than each holding all of it.
            if let Some(activated) = &materialization.history_evidence().acknowledgement {
                let (reference, value) = activated.acknowledgement();
                self.commit_verifier
                    .remember_acknowledgement(reference, value)
                    .map_err(StorePullError::Protocol)?;
            }
            let head = materialization.activation_head();
            let head_ref = StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object: materialization.activation_head_object().clone(),
            };
            self.commit_verifier
                .remember_verified_head(
                    &head_ref,
                    VerifiedObject {
                        value: head.clone(),
                        bytes: head.to_bytes(),
                        semantic_hash: head_ref.head_hash,
                        object: head_ref.object.clone(),
                    },
                )
                .map_err(StorePullError::Protocol)?;
            // The accepted path is a dense sequence above the snapshot
            // coverage, which is where `admit_snapshot_announcements` has
            // already put its floor. Admit each stream's contiguous prefix from
            // there and leave the rest to the discovery walk, which resumes at
            // the first sequence the path does not cover.
            let sequence = commit_ref.coord.sequence();
            // The contiguous run starts one above the snapshot's coverage, not
            // at sequence one: rows at or under the coverage are the closure
            // the image keeps for its own reasons, not a prefix of the accepted
            // path, and treating one of them as the start would leave the run
            // stuck at a position the path does not hold.
            let expected = match announced.get(&commit.author_registration) {
                Some(previous) => previous.saturating_add(1),
                None => self
                    .commit_verifier
                    .covered_announcement_floor(&commit.author_registration)
                    .saturating_add(1),
            };
            if sequence != expected {
                continue;
            }
            self.commit_verifier
                .remember_accepted_announcement(
                    &commit.author_registration,
                    sequence,
                    commit_ref.clone(),
                    head_ref,
                    head.successor.next_slot.clone(),
                )
                .map_err(StorePullError::Protocol)?;
            announced.insert(commit.author_registration.clone(), sequence);
        }
        Ok(())
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
        let states = self.history.resolved_states();
        verified_merge_predecessor_state(&self.history.genesis, &states, commit)
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
                        self.history.state_after(reference).cloned().ok_or_else(|| {
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

fn merge_device_state_from_verified_history(
    reference: &StoreDeviceStateRef,
    history: &VerifiedMergeHistory,
    allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let genesis = &history.genesis;
    let frontier = reference.frontier();
    let allowed = verified_merge_commit_closure(history, allowed_tips)?;
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
                    history.state_after(reference).cloned().ok_or_else(|| {
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

    /// Every position this history can answer a device state for: the commits
    /// it verified, plus the covered positions the baseline restates.
    pub(crate) fn resolved_states(
        &self,
    ) -> BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState> {
        self.baseline
            .covered_states()
            .map(|(reference, state)| (reference.clone(), state.clone()))
            .chain(
                self.commits
                    .iter()
                    .map(|(reference, verified)| (reference.clone(), verified.state_after.clone())),
            )
            .collect()
    }
}

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
    history: VerifiedMergeHistory,
    verified_memberships: Vec<VerifiedMembershipChain>,
}

type PredecessorCommitPredicate<'a> = Box<dyn FnMut(&VerifiedStoreBatchCommit) -> bool + Send + 'a>;

pub struct MergeOutboundAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) membership_state: StoreMembershipStateRef,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}
