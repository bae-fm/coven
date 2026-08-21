//! The durable Owner-promotion journal: the request, acceptance, and
//! finalization values one promotion binds, validated against the exact
//! target and identities they retain.

use crate::store_commit::ObjectHash;

/// A promotion journal whose recorded state contradicts the request, target,
/// or acceptance it retains. Workflow errors wrap it at the operation
/// boundary.
#[derive(Debug, thiserror::Error)]
pub enum OwnerPromotionJournalError {
    #[error("Owner promotion journal: {0}")]
    Invariant(String),
    #[error("Owner promotion journal JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Owner promotion journal prepared commit: {0}")]
    PreparedCommit(#[from] crate::prepared_commit::PreparedCommitError),
    #[error("Owner promotion journal candidate: {0}")]
    Candidate(#[from] crate::remote_object::RemoteObjectRecordError),
}

const TARGET_PREFIX: &str = "owner_promotion_target/";

pub fn target_key(
    target: &StoreDeviceRegistrationRef,
) -> Result<String, OwnerPromotionJournalError> {
    let bytes = serde_json::to_vec(target)?;
    Ok(format!("{TARGET_PREFIX}{}", ObjectHash::digest(&bytes)))
}

use serde::{Deserialize, Serialize};

use crate::circle_control::StoreMembershipStateRef;
use crate::membership::StoreMembershipRoleGrant;
use crate::membership_mutation::{PreparedMembershipPublication, PreparedMembershipTransition};
use crate::prepared_commit::PreparedStoreOperationCommit;
use crate::store_commit::{
    membership_head_slot_prefix, owner_recovery_semantic_prefix, GrantStreamAnchor,
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionFinalization, OwnerPromotionId,
    OwnerPromotionRequest, OwnerPromotionRequestActivation, OwnerPromotionStaleReason,
    StoreDeviceRegistrationRef, StreamActivation, StreamAnchorDomain,
};
use crate::wrapped_store_key::PreparedWrappedStoreKey;

#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionJournal {
    pub promotion_id: OwnerPromotionId,
    pub target: StoreDeviceRegistrationRef,
    pub state: OwnerPromotionJournalState,
}

#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionJournalState {
    Allocated,
    RequestPrepared {
        request: OwnerPromotionRequest,
        candidate: Box<PreparedStoreOperationCommit>,
    },
    AwaitingAcceptance {
        request: OwnerPromotionRequest,
        activation: OwnerPromotionRequestActivation,
    },
    AcceptanceReady {
        acceptance: OwnerPromotionAcceptance,
    },
    MergeMembershipPrepared {
        acceptance: OwnerPromotionAcceptance,
        wrapped_key: PreparedWrappedStoreKey,
        transition: Box<PreparedMembershipTransition>,
    },
    MergeHeadPrepared {
        acceptance: OwnerPromotionAcceptance,
        wrapped_key: PreparedWrappedStoreKey,
        transition: Box<PreparedMembershipTransition>,
        publication: Box<PreparedMembershipPublication>,
        candidate: Box<PreparedStoreOperationCommit>,
    },
    Finalized {
        acceptance: OwnerPromotionAcceptance,
        membership: StoreMembershipStateRef,
        receipt: Box<OwnerPromotionFinalizationReceipt>,
    },
    Nonactivated {
        request: OwnerPromotionRequest,
        nonactivation: crate::remote_object::CandidateNonactivation,
    },
    Stale {
        acceptance: OwnerPromotionAcceptance,
        reason: OwnerPromotionStaleReason,
        evidence: Box<OwnerPromotionStaleEvidence>,
    },
}

#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPromotionFinalizationReceipt {
    pub candidate: Box<PreparedStoreOperationCommit>,
    pub publication: Box<PreparedMembershipPublication>,
}

#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPromotionStaleEvidence {
    BeforePublication,
    Candidate {
        nonactivation: crate::remote_object::CandidateNonactivation,
        receipt: Box<OwnerPromotionFinalizationReceipt>,
        /// Every object the lost candidate owns, so the deletion this state owes
        /// is stated rather than re-derived: whoever next reads this journal —
        /// the retry of this attempt, or the attempt that replaces it — finishes
        /// an interrupted cleanup from the list the transition validated.
        published: Vec<crate::objects::ExactObjectRef>,
    },
}

pub fn owner_promotion_published_objects(
    candidate: &PreparedStoreOperationCommit,
    transition: &PreparedMembershipTransition,
    publication: &PreparedMembershipPublication,
    wrapped_key: &PreparedWrappedStoreKey,
) -> Result<Vec<crate::objects::ExactObjectRef>, OwnerPromotionJournalError> {
    Ok(candidate
        .merge_owner_promotion_remote_objects(transition, publication, wrapped_key)?
        .iter()
        .map(|remote| remote.record().object().clone())
        .collect())
}

fn prepared_candidate_is_exact_request(
    candidate: &PreparedStoreOperationCommit,
    request: &OwnerPromotionRequest,
) -> bool {
    candidate.validate_closed_shape().is_ok()
        && candidate.commit.owner_promotion_request() == Some(request)
        && candidate.commit.author_registration == request.promoter_registration
        && candidate.commit.membership_state == request.predecessor_membership
        && candidate.commit.device_state == request.predecessor_devices
}

fn same_prepared_candidate(
    previous: &PreparedStoreOperationCommit,
    next: &PreparedStoreOperationCommit,
) -> bool {
    previous.reference == next.reference && previous.commit.to_bytes() == next.commit.to_bytes()
}

fn request_activation_matches_candidate(
    request: &OwnerPromotionRequest,
    candidate: &PreparedStoreOperationCommit,
    activation: &OwnerPromotionRequestActivation,
) -> bool {
    prepared_candidate_is_exact_request(candidate, request)
        && activation.commit == candidate.reference
        && candidate.head_ref() == activation.head
}

fn nonactivation_matches_candidate(
    candidate: &PreparedStoreOperationCommit,
    nonactivation: &crate::remote_object::CandidateNonactivation,
) -> bool {
    if nonactivation.validate().is_err() {
        return false;
    }
    let Ok(reference) = nonactivation.reference() else {
        return false;
    };
    reference == candidate.reference
        && nonactivation.candidate().canonical_signed_bytes == candidate.commit.to_bytes()
}

fn nonactivation_commit(
    nonactivation: &crate::remote_object::CandidateNonactivation,
) -> Result<crate::store_commit::StoreBatchCommit, OwnerPromotionJournalError> {
    nonactivation.validate()?;
    serde_json::from_slice(&nonactivation.candidate().canonical_signed_bytes)
        .map_err(OwnerPromotionJournalError::from)
}

fn nonactivation_matches_request(
    nonactivation: &crate::remote_object::CandidateNonactivation,
    request: &OwnerPromotionRequest,
) -> bool {
    nonactivation_commit(nonactivation).is_ok_and(|commit| {
        commit.owner_promotion_request() == Some(request)
            && commit.author_registration == request.promoter_registration
            && commit.membership_state == request.predecessor_membership
            && commit.device_state == request.predecessor_devices
    })
}

fn wrapped_key_matches_acceptance(
    wrapped_key: &PreparedWrappedStoreKey,
    acceptance: &OwnerPromotionAcceptance,
) -> bool {
    wrapped_key.validate().is_ok()
        && wrapped_key.reference.recipient_pubkey == acceptance.request.member_pubkey
}

fn transition_matches_acceptance(
    transition: &PreparedMembershipTransition,
    wrapped_key: &crate::wrapped_store_key::WrappedStoreKeyRef,
    acceptance: &OwnerPromotionAcceptance,
) -> bool {
    let OwnerPromotionFinalization {
        author_stream,
        seq,
        previous_hash,
    } = &acceptance.request.finalization;
    let entry = &transition.entry;
    let expected_replacements =
        std::collections::BTreeSet::from([acceptance.request.member_grant.clone()]);
    transition.validate().is_ok()
        && transition.transition.body.author_registration
            == acceptance.request.promoter_registration
        && transition.transition.body.resolutions == entry.resolution_dependencies
        && entry.author_owner_grant == acceptance.request.promoter_owner_grant
        && entry.stream_id == *author_stream
        && entry.seq == *seq
        && entry.previous_hash == *previous_hash
        && matches!(
            &entry.change,
            crate::membership::MembershipChange::SetMember {
                user_pubkey,
                role: StoreMembershipRoleGrant::Owner {
                    recovery: crate::membership::OwnerRecoveryAnchorRef::Promotion {
                        acceptance: entry_acceptance,
                    },
                },
                grant_id,
                membership: Some(membership),
                replaces,
                wrapped_key: entry_wrapped_key,
                ..
            } if user_pubkey == &acceptance.request.member_pubkey
                && entry_acceptance.as_ref() == acceptance
                && grant_id == &acceptance.request.intended_owner_grant
                && membership == &acceptance.anchors.membership
                && replaces == &expected_replacements
                && entry_wrapped_key == wrapped_key
        )
}

fn merge_candidate_matches_finalization(
    candidate: &PreparedStoreOperationCommit,
    transition: &PreparedMembershipTransition,
    acceptance: &OwnerPromotionAcceptance,
) -> bool {
    let OwnerPromotionAnchors {
        membership,
        recovery,
    } = &acceptance.anchors;
    let mut expected_activations = vec![
        StreamActivation::grant_authorized(
            acceptance.request.store_root_hash,
            acceptance.request.member_registration.clone(),
            acceptance.request.intended_owner_grant.clone(),
            membership.clone(),
        ),
        StreamActivation::grant_authorized(
            acceptance.request.store_root_hash,
            acceptance.request.member_registration.clone(),
            acceptance.request.intended_owner_grant.clone(),
            recovery.clone(),
        ),
    ];
    expected_activations.sort();
    let Some(operations) = candidate.commit.operations() else {
        return false;
    };
    candidate.validate_closed_shape().is_ok()
        && candidate.commit.author_registration == acceptance.request.promoter_registration
        && candidate.commit.control()
            == Some(&crate::store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
        && operations.acknowledgement.is_none()
        && operations.device_join_attempt_decisions.is_empty()
        && operations.device_join_outcomes.is_empty()
        && operations.provider_access_grants.is_empty()
        && operations.device_registrations.is_empty()
        && operations.device_exclusion_proposals.is_empty()
        && operations.device_exclusion_outcomes.is_empty()
        && operations.stream_activations == expected_activations
        && operations.circle_controls.is_empty()
        && operations.store_package.is_none()
        && operations.circle_packages.is_empty()
}

fn finalization_receipt_matches_acceptance(
    receipt: &OwnerPromotionFinalizationReceipt,
    acceptance: &OwnerPromotionAcceptance,
) -> bool {
    {
        let OwnerPromotionFinalizationReceipt {
            candidate,
            publication,
        } = receipt;
        let Some(crate::store_commit::StoreControl { transition }) = candidate.commit.control()
        else {
            return false;
        };
        let crate::membership::MembershipChange::SetMember { wrapped_key, .. } =
            &publication.entry.change
        else {
            return false;
        };
        let prepared_transition = PreparedMembershipTransition {
            entry: publication.entry.clone(),
            entry_ref: publication.entry_ref.clone(),
            transition: transition.clone(),
        };
        publication.validate().is_ok()
            && transition_matches_acceptance(&prepared_transition, wrapped_key, acceptance)
            && merge_candidate_matches_finalization(candidate, &prepared_transition, acceptance)
            && prepared_transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            && matches!(
                &publication.head.activation,
                crate::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &candidate.reference
            )
    }
}

fn finalization_receipt_candidate(
    receipt: &OwnerPromotionFinalizationReceipt,
) -> &PreparedStoreOperationCommit {
    &receipt.candidate
}

fn finalization_receipt_matches_membership(
    receipt: &OwnerPromotionFinalizationReceipt,
    membership: &StoreMembershipStateRef,
) -> bool {
    membership
        .heads
        .binary_search(&receipt.publication.head_ref)
        .is_ok()
}

fn stale_candidate_evidence_matches(
    nonactivation: &crate::remote_object::CandidateNonactivation,
    receipt: &OwnerPromotionFinalizationReceipt,
    acceptance: &OwnerPromotionAcceptance,
) -> bool {
    let candidate = finalization_receipt_candidate(receipt);
    finalization_receipt_matches_acceptance(receipt, acceptance)
        && nonactivation_matches_candidate(candidate, nonactivation)
}

fn receipt_matches_merge_preparation(
    receipt: &OwnerPromotionFinalizationReceipt,
    candidate: &PreparedStoreOperationCommit,
    publication: &PreparedMembershipPublication,
) -> bool {
    matches!(
        receipt,
        OwnerPromotionFinalizationReceipt {
            candidate: receipt_candidate,
            publication: receipt_publication,
        } if same_prepared_candidate(candidate, receipt_candidate)
            && publication.entry == receipt_publication.entry
            && publication.entry_ref == receipt_publication.entry_ref
            && publication.head == receipt_publication.head
            && publication.head_ref == receipt_publication.head_ref
    )
}

impl OwnerPromotionJournal {
    fn request_has_closed_shape(&self, request: &OwnerPromotionRequest) -> bool {
        request.require_version().is_ok()
            && request.promotion_id == self.promotion_id
            && request.member_registration == self.target
            && !request.member_pubkey.is_empty()
            && request.intended_owner_grant
                == crate::store_commit::derive_owner_promotion_grant(
                    request.store_root_hash,
                    request.promotion_id,
                    &request.member_pubkey,
                )
            && request.finalization.seq != 0
    }

    fn acceptance_has_closed_shape(&self, acceptance: &OwnerPromotionAcceptance) -> bool {
        let request = acceptance.request.as_ref();
        if !self.request_has_closed_shape(request)
            || !matches!(
                acceptance.anchors.recovery(),
                GrantStreamAnchor::OwnerRecovery { .. }
            )
        {
            return false;
        }
        let GrantStreamAnchor::StoreMembership { first_slot } = &acceptance.anchors.membership
        else {
            return false;
        };
        let GrantStreamAnchor::OwnerRecovery {
            first_slot: recovery_slot,
        } = &acceptance.anchors.recovery
        else {
            return false;
        };
        let membership_stream = StreamActivation::grant_authorized_stream_id(
            request.store_root_hash,
            &request.member_registration,
            &request.intended_owner_grant,
            StreamAnchorDomain::StoreMembership,
        );
        first_slot.logical_key()
            == format!(
                "{}.json",
                membership_head_slot_prefix(
                    &request.member_pubkey,
                    &request.intended_owner_grant,
                    membership_stream,
                    1,
                )
            )
            && recovery_slot.logical_key()
                == format!(
                    "{}.json",
                    owner_recovery_semantic_prefix(
                        &request.member_pubkey,
                        request.intended_owner_grant.clone(),
                        1,
                    )
                )
    }

    pub fn promotion_id(&self) -> OwnerPromotionId {
        self.promotion_id
    }

    pub fn target_state_key(&self) -> Result<String, OwnerPromotionJournalError> {
        target_key(&self.target)
    }

    pub fn validate_id(
        &self,
        expected: OwnerPromotionId,
    ) -> Result<(), OwnerPromotionJournalError> {
        self.validate_contents()?;
        if self.promotion_id != expected {
            return Err(OwnerPromotionJournalError::Invariant(
                "promotion journal is stored under another identity".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_target_key(&self, expected: &str) -> Result<(), OwnerPromotionJournalError> {
        self.validate_contents()?;
        if self.target_state_key()? != expected {
            return Err(OwnerPromotionJournalError::Invariant(
                "promotion journal is stored under another target".to_string(),
            ));
        }
        Ok(())
    }

    pub fn into_predecessor(
        self,
    ) -> Result<
        (OwnerPromotionJournalPredecessor, OwnerPromotionJournalState),
        OwnerPromotionJournalError,
    > {
        self.validate_contents()?;
        let previous_value = serde_json::to_string(&self)?;
        let Self {
            promotion_id,
            target,
            state,
        } = self;
        Ok((
            OwnerPromotionJournalPredecessor {
                promotion_id,
                target,
                previous_value,
            },
            state,
        ))
    }

    fn validate_contents(&self) -> Result<(), OwnerPromotionJournalError> {
        let valid = match &self.state {
            OwnerPromotionJournalState::Allocated => true,
            OwnerPromotionJournalState::RequestPrepared { request, candidate } => {
                self.request_has_closed_shape(request)
                    && prepared_candidate_is_exact_request(candidate, request)
            }
            OwnerPromotionJournalState::AwaitingAcceptance {
                request,
                activation,
            } => {
                self.request_has_closed_shape(request) && activation.commit.coord.validate().is_ok()
            }
            OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                self.acceptance_has_closed_shape(acceptance)
            }
            OwnerPromotionJournalState::MergeMembershipPrepared {
                acceptance,
                wrapped_key,
                transition,
            } => {
                self.acceptance_has_closed_shape(acceptance)
                    && wrapped_key_matches_acceptance(wrapped_key, acceptance)
                    && transition_matches_acceptance(transition, &wrapped_key.reference, acceptance)
            }
            OwnerPromotionJournalState::MergeHeadPrepared {
                acceptance,
                wrapped_key,
                transition,
                publication,
                candidate,
            } => {
                self.acceptance_has_closed_shape(acceptance)
                    && wrapped_key_matches_acceptance(wrapped_key, acceptance)
                    && transition_matches_acceptance(transition, &wrapped_key.reference, acceptance)
                    && merge_candidate_matches_finalization(candidate, transition, acceptance)
                    && publication.validate().is_ok()
                    && transition.entry == publication.entry
                    && transition.entry_ref == publication.entry_ref
                    && transition
                        .transition
                        .matches_head(&publication.head, &publication.head_ref)
                    && matches!(
                        &publication.head.activation,
                        crate::membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
            }
            OwnerPromotionJournalState::Finalized {
                acceptance,
                membership,
                receipt,
            } => {
                self.acceptance_has_closed_shape(acceptance)
                    && finalization_receipt_matches_acceptance(receipt, acceptance)
                    && finalization_receipt_matches_membership(receipt, membership)
            }
            OwnerPromotionJournalState::Nonactivated {
                request,
                nonactivation,
            } => {
                self.request_has_closed_shape(request)
                    && nonactivation_matches_request(nonactivation, request)
            }
            OwnerPromotionJournalState::Stale {
                acceptance,
                reason,
                evidence,
            } => {
                self.acceptance_has_closed_shape(acceptance)
                    && match (reason, evidence.as_ref()) {
                        (
                            OwnerPromotionStaleReason::MergeFinalizationPointOccupied { winner },
                            OwnerPromotionStaleEvidence::BeforePublication,
                        ) => {
                            winner.coord.author_owner_grant
                                == acceptance.request.promoter_owner_grant
                                && winner.coord.stream_id
                                    == acceptance.request.finalization.author_stream
                                && winner.coord.seq >= acceptance.request.finalization.seq
                        }
                        (
                            OwnerPromotionStaleReason::MergeActivationRejected,
                            OwnerPromotionStaleEvidence::Candidate {
                                nonactivation,
                                receipt,
                                ..
                            },
                        ) => stale_candidate_evidence_matches(nonactivation, receipt, acceptance),
                        _ => false,
                    }
            }
        };
        if !valid {
            return Err(OwnerPromotionJournalError::Invariant(
                "promotion journal state violates its closed protocol invariants".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_begin(&self) -> Result<(), OwnerPromotionJournalError> {
        self.validate_contents()?;
        if !matches!(self.state, OwnerPromotionJournalState::Allocated) {
            return Err(OwnerPromotionJournalError::Invariant(
                "promotion journal begins in a non-initial state".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_acceptance_begin(&self) -> Result<(), OwnerPromotionJournalError> {
        self.validate_contents()?;
        if !matches!(
            self.state,
            OwnerPromotionJournalState::AcceptanceReady { .. }
        ) {
            return Err(OwnerPromotionJournalError::Invariant(
                "candidate promotion journal must begin with its signed acceptance".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_transition(
        &self,
        next: &OwnerPromotionJournal,
    ) -> Result<(), OwnerPromotionJournalError> {
        self.validate_contents()?;
        next.validate_contents()?;
        if self.promotion_id != next.promotion_id || self.target != next.target {
            return Err(OwnerPromotionJournalError::Invariant(
                "promotion journal transition changes its identity".to_string(),
            ));
        }
        let valid = match (&self.state, &next.state) {
            (
                OwnerPromotionJournalState::Allocated,
                OwnerPromotionJournalState::RequestPrepared { request, candidate },
            ) => prepared_candidate_is_exact_request(candidate, request),
            (
                OwnerPromotionJournalState::RequestPrepared { request, candidate },
                OwnerPromotionJournalState::RequestPrepared {
                    request: successor,
                    candidate: successor_candidate,
                },
            ) => {
                request == successor
                    && same_prepared_candidate(candidate, successor_candidate)
                    && prepared_candidate_is_exact_request(successor_candidate, successor)
            }
            (
                OwnerPromotionJournalState::RequestPrepared { request, candidate },
                OwnerPromotionJournalState::AwaitingAcceptance {
                    request: successor,
                    activation,
                },
            ) => {
                request == successor
                    && request_activation_matches_candidate(request, candidate, activation)
            }
            (
                OwnerPromotionJournalState::RequestPrepared { request, candidate },
                OwnerPromotionJournalState::Nonactivated {
                    request: successor,
                    nonactivation,
                },
            ) => request == successor && nonactivation_matches_candidate(candidate, nonactivation),
            (
                OwnerPromotionJournalState::AwaitingAcceptance {
                    request,
                    activation,
                },
                OwnerPromotionJournalState::AcceptanceReady { acceptance },
            ) => request == acceptance.request.as_ref() && activation == &acceptance.activation,
            (
                OwnerPromotionJournalState::AcceptanceReady { acceptance },
                OwnerPromotionJournalState::MergeMembershipPrepared {
                    acceptance: successor,
                    ..
                },
            ) => acceptance == successor,
            (
                OwnerPromotionJournalState::AcceptanceReady { acceptance },
                OwnerPromotionJournalState::Stale {
                    acceptance: successor,
                    reason,
                    evidence,
                },
            ) => {
                acceptance == successor
                    && matches!(
                        evidence.as_ref(),
                        OwnerPromotionStaleEvidence::BeforePublication
                    )
                    && matches!(
                        reason,
                        OwnerPromotionStaleReason::MergeFinalizationPointOccupied { .. }
                    )
            }
            (
                OwnerPromotionJournalState::MergeMembershipPrepared {
                    acceptance,
                    wrapped_key,
                    transition,
                },
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance: successor,
                    wrapped_key: successor_key,
                    transition: successor_transition,
                    publication,
                    candidate,
                },
            ) => {
                acceptance == successor
                    && wrapped_key.reference == successor_key.reference
                    && transition.entry == publication.entry
                    && transition.entry_ref == publication.entry_ref
                    && transition.entry_ref == successor_transition.entry_ref
                    && transition.transition == successor_transition.transition
                    && merge_candidate_matches_finalization(candidate, transition, acceptance)
                    && transition
                        .transition
                        .matches_head(&publication.head, &publication.head_ref)
                    && matches!(
                        &publication.head.activation,
                        crate::membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
            }
            (
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance,
                    wrapped_key,
                    transition,
                    publication,
                    candidate,
                },
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance: successor,
                    wrapped_key: successor_key,
                    transition: successor_transition,
                    publication: successor_publication,
                    candidate: successor_candidate,
                },
            ) => {
                acceptance == successor
                    && wrapped_key.reference == successor_key.reference
                    && transition.entry_ref == successor_transition.entry_ref
                    && transition.transition == successor_transition.transition
                    && publication.entry_ref == successor_publication.entry_ref
                    && publication.head_ref == successor_publication.head_ref
                    && same_prepared_candidate(candidate, successor_candidate)
                    && transition.entry == publication.entry
                    && transition.entry == successor_publication.entry
                    && transition
                        .transition
                        .matches_head(&successor_publication.head, &successor_publication.head_ref)
                    && matches!(
                        &successor_publication.head.activation,
                        crate::membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &successor_candidate.reference
                    )
            }
            (
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance,
                    publication,
                    candidate,
                    ..
                },
                OwnerPromotionJournalState::Finalized {
                    acceptance: successor,
                    membership,
                    receipt,
                },
            ) => {
                acceptance == successor
                    && receipt_matches_merge_preparation(receipt, candidate, publication)
                    && membership
                        .heads
                        .binary_search(&publication.head_ref)
                        .is_ok()
                    && matches!(
                        &publication.head.activation,
                        crate::membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
            }
            (
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance,
                    wrapped_key,
                    transition,
                    publication,
                    candidate,
                },
                OwnerPromotionJournalState::Stale {
                    acceptance: successor,
                    reason,
                    evidence,
                },
            ) => {
                acceptance == successor
                    && matches!(reason, OwnerPromotionStaleReason::MergeActivationRejected)
                    && matches!(
                        evidence.as_ref(),
                        OwnerPromotionStaleEvidence::Candidate {
                            nonactivation,
                            receipt,
                            published,
                        } if nonactivation_matches_candidate(candidate, nonactivation)
                            && receipt_matches_merge_preparation(
                                receipt,
                                candidate,
                                publication,
                            )
                            && owner_promotion_published_objects(
                                candidate,
                                transition,
                                publication,
                                wrapped_key,
                            )
                            .is_ok_and(|expected| expected == *published)
                    )
            }
            _ => false,
        };
        if !valid {
            return Err(OwnerPromotionJournalError::Invariant(
                "promotion journal transition skips or reverses protocol state".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_failed_attempt_replacement(
        &self,
        replacement: &OwnerPromotionJournal,
    ) -> Result<(), OwnerPromotionJournalError> {
        self.validate_contents()?;
        replacement.validate_begin()?;
        if self.target != replacement.target || self.promotion_id == replacement.promotion_id {
            return Err(OwnerPromotionJournalError::Invariant(
                "promotion retry must retain its target and use a fresh identity".to_string(),
            ));
        }
        if !matches!(
            self.state,
            OwnerPromotionJournalState::Nonactivated { .. }
                | OwnerPromotionJournalState::Stale { .. }
        ) {
            return Err(OwnerPromotionJournalError::Invariant(
                "only a failed promotion attempt can be replaced".to_string(),
            ));
        }
        Ok(())
    }
}

pub struct OwnerPromotionJournalPredecessor {
    pub promotion_id: OwnerPromotionId,
    pub target: StoreDeviceRegistrationRef,
    previous_value: String,
}

impl OwnerPromotionJournalPredecessor {
    pub fn transition_to(
        &self,
        next: &OwnerPromotionJournal,
        remote_objects: Vec<crate::remote_object::ClosedRemoteObject>,
    ) -> Result<OwnerPromotionJournalTransition, OwnerPromotionJournalError> {
        let previous: OwnerPromotionJournal = serde_json::from_str(&self.previous_value)?;
        previous.validate_transition(next)?;
        let next_value = serde_json::to_string(next)?;
        Ok(OwnerPromotionJournalTransition {
            journal_key: format!("owner_promotion/{}", self.promotion_id),
            target_key: target_key(&self.target)?,
            previous_value: self.previous_value.clone(),
            next_value,
            remote_objects,
        })
    }
}

pub struct OwnerPromotionJournalTransition {
    journal_key: String,
    target_key: String,
    previous_value: String,
    next_value: String,
    remote_objects: Vec<crate::remote_object::ClosedRemoteObject>,
}

impl OwnerPromotionJournalTransition {
    pub fn into_values(
        self,
    ) -> (
        String,
        String,
        String,
        String,
        Vec<crate::remote_object::ClosedRemoteObject>,
    ) {
        (
            self.journal_key,
            self.target_key,
            self.previous_value,
            self.next_value,
            self.remote_objects,
        )
    }
}
