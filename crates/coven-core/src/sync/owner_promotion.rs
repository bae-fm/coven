use serde::{Deserialize, Serialize};

use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};

use super::circle_control::StoreMembershipStateRef;
use super::invite::{PreparedMembershipPublication, PreparedMembershipTransition};
use super::membership::{MembershipChain, MembershipGrantId, StoreMembershipRoleGrant};
use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use super::store_commit::{
    membership_head_slot_prefix, owner_recovery_semantic_prefix, GrantStreamAnchor,
    OwnerPromotionAcceptance, OwnerPromotionAnchors, OwnerPromotionFinalization, OwnerPromotionId,
    OwnerPromotionRequest, OwnerPromotionRequestActivation, OwnerPromotionStaleReason,
    OwnerPromotionStatus, StoreDeviceRegistrationRef, StoreRootRef, StreamActivation,
    StreamAnchorDomain,
};
use super::store_outbound::{
    PreparedStoreOperationCommit, StoreOperationBatch, StoreOperationPublicationOutcome,
};
use super::wrapped_store_key::PreparedWrappedStoreKey;

const TARGET_PREFIX: &str = "owner_promotion_target/";

#[derive(Debug, thiserror::Error)]
pub enum OwnerPromotionError {
    #[error("Owner promotion database state: {0}")]
    Database(String),
    #[error("Owner promotion protocol state: {0}")]
    Protocol(String),
    #[error("Owner promotion storage: {0}")]
    Storage(String),
    #[error("Owner promotion request has not activated")]
    RequestNotActivated,
    #[error("Owner promotion {0} is absent")]
    NotFound(OwnerPromotionId),
    #[error("Owner promotion is stale: {0:?}")]
    Stale(Box<OwnerPromotionStaleReason>),
}

impl From<crate::database::DbError> for OwnerPromotionError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

impl From<super::store_outbound::StoreOutboundError> for OwnerPromotionError {
    fn from(error: super::store_outbound::StoreOutboundError) -> Self {
        Self::Protocol(error.to_string())
    }
}

impl From<super::storage::StorageError> for OwnerPromotionError {
    fn from(error: super::storage::StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnerPromotionJournal {
    promotion_id: OwnerPromotionId,
    target: StoreDeviceRegistrationRef,
    state: OwnerPromotionJournalState,
}

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum OwnerPromotionJournalState {
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
        nonactivation: super::remote_object::CandidateNonactivation,
    },
    Stale {
        acceptance: OwnerPromotionAcceptance,
        reason: OwnerPromotionStaleReason,
        evidence: Box<OwnerPromotionStaleEvidence>,
    },
}

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerPromotionFinalizationReceipt {
    candidate: Box<PreparedStoreOperationCommit>,
    publication: Box<PreparedMembershipPublication>,
}

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum OwnerPromotionStaleEvidence {
    BeforePublication,
    Candidate {
        nonactivation: super::remote_object::CandidateNonactivation,
        receipt: Box<OwnerPromotionFinalizationReceipt>,
    },
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
    previous.reference == next.reference
        && previous.commit.to_bytes() == next.commit.to_bytes()
        && previous.prepared.reference() == next.prepared.reference()
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
    nonactivation: &super::remote_object::CandidateNonactivation,
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
    nonactivation: &super::remote_object::CandidateNonactivation,
) -> Result<super::store_commit::StoreBatchCommit, String> {
    nonactivation
        .validate()
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&nonactivation.candidate().canonical_signed_bytes)
        .map_err(|error| format!("parse nonactivated candidate commit: {error}"))
}

fn nonactivation_matches_request(
    nonactivation: &super::remote_object::CandidateNonactivation,
    request: &OwnerPromotionRequest,
) -> bool {
    nonactivation_commit(nonactivation).is_ok_and(|commit| {
        commit.owner_promotion_request() == Some(request)
            && commit.author_registration == request.promoter_registration
            && commit.membership_state == request.predecessor_membership
            && commit.device_state == request.predecessor_devices
    })
}

fn request_has_closed_shape(
    promotion_id: OwnerPromotionId,
    target: &StoreDeviceRegistrationRef,
    request: &OwnerPromotionRequest,
) -> bool {
    request.version == super::store_commit::STORE_PROTOCOL_VERSION
        && request.promotion_id == promotion_id
        && request.member_registration == *target
        && !request.member_pubkey.is_empty()
        && request.intended_owner_grant
            == super::store_commit::derive_owner_promotion_grant(
                request.store_root_hash,
                request.promotion_id,
                &request.member_pubkey,
            )
        && request.finalization.seq != 0
}

fn acceptance_has_closed_shape(
    promotion_id: OwnerPromotionId,
    target: &StoreDeviceRegistrationRef,
    acceptance: &OwnerPromotionAcceptance,
) -> bool {
    let request = acceptance.request.as_ref();
    if !request_has_closed_shape(promotion_id, target, request)
        || !matches!(
            acceptance.anchors.recovery(),
            GrantStreamAnchor::OwnerRecovery { .. }
        )
    {
        return false;
    }
    let GrantStreamAnchor::StoreMembership { first_slot } = &acceptance.anchors.membership else {
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

fn wrapped_key_matches_acceptance(
    wrapped_key: &PreparedWrappedStoreKey,
    acceptance: &OwnerPromotionAcceptance,
) -> bool {
    wrapped_key.validate().is_ok()
        && wrapped_key.reference.recipient_pubkey == acceptance.request.member_pubkey
}

fn transition_matches_acceptance(
    transition: &PreparedMembershipTransition,
    wrapped_key: &super::wrapped_store_key::WrappedStoreKeyRef,
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
    super::invite::validate_prepared_transition(transition).is_ok()
        && transition.transition.body.author_registration
            == acceptance.request.promoter_registration
        && transition.transition.body.resolutions == entry.resolution_dependencies
        && entry.author_owner_grant == acceptance.request.promoter_owner_grant
        && entry.stream_id == *author_stream
        && entry.seq == *seq
        && entry.previous_hash == *previous_hash
        && matches!(
            &entry.change,
            super::membership::MembershipChange::SetMember {
                user_pubkey,
                role: StoreMembershipRoleGrant::Owner {
                    recovery: super::membership::OwnerRecoveryAnchorRef::Promotion {
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

fn prepared_candidate_has_closed_shape(candidate: &PreparedStoreOperationCommit) -> bool {
    candidate.validate_closed_shape().is_ok()
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
    prepared_candidate_has_closed_shape(candidate)
        && candidate.commit.author_registration == acceptance.request.promoter_registration
        && candidate.commit.control()
            == Some(&super::store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
        && operations.acknowledgement.is_none()
        && operations.device_join_attempt_decisions.is_empty()
        && operations.device_join_outcomes.is_empty()
        && operations.device_join_cleanup_receipts.is_empty()
        && operations.provider_access_grants.is_empty()
        && operations.provider_access_withdrawals.is_empty()
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
        let Some(super::store_commit::StoreControl { transition }) = candidate.commit.control()
        else {
            return false;
        };
        let super::membership::MembershipChange::SetMember { wrapped_key, .. } =
            &publication.entry.change
        else {
            return false;
        };
        let prepared_transition = PreparedMembershipTransition {
            entry: publication.entry.clone(),
            entry_ref: publication.entry_ref.clone(),
            entry_object: publication.entry_object.clone(),
            transition: transition.clone(),
        };
        super::invite::validate_prepared_publication(publication).is_ok()
            && transition_matches_acceptance(&prepared_transition, wrapped_key, acceptance)
            && merge_candidate_matches_finalization(candidate, &prepared_transition, acceptance)
            && prepared_transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            && matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
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
    nonactivation: &super::remote_object::CandidateNonactivation,
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
            && publication.entry_object.reference()
                == receipt_publication.entry_object.reference()
            && publication.head == receipt_publication.head
            && publication.head_ref == receipt_publication.head_ref
            && publication.head_object.reference()
                == receipt_publication.head_object.reference()
    )
}

impl OwnerPromotionJournal {
    pub(crate) fn promotion_id(&self) -> OwnerPromotionId {
        self.promotion_id
    }

    pub(crate) fn target_state_key(&self) -> Result<String, OwnerPromotionError> {
        target_key(&self.target)
    }

    pub(crate) fn validate_id(
        &self,
        expected: OwnerPromotionId,
    ) -> Result<(), OwnerPromotionError> {
        self.validate_contents()?;
        if self.promotion_id != expected {
            return Err(OwnerPromotionError::Protocol(
                "promotion journal is stored under another identity".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_target_key(&self, expected: &str) -> Result<(), OwnerPromotionError> {
        self.validate_contents()?;
        if self.target_state_key()? != expected {
            return Err(OwnerPromotionError::Protocol(
                "promotion journal is stored under another target".to_string(),
            ));
        }
        Ok(())
    }

    fn into_predecessor(
        self,
    ) -> Result<(OwnerPromotionJournalPredecessor, OwnerPromotionJournalState), OwnerPromotionError>
    {
        self.validate_contents()?;
        let previous_value = serde_json::to_string(&self).map_err(|error| {
            OwnerPromotionError::Protocol(format!("serialize Owner-promotion predecessor: {error}"))
        })?;
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

    fn status(&self) -> OwnerPromotionStatus {
        match &self.state {
            OwnerPromotionJournalState::Allocated => OwnerPromotionStatus::Preparing {
                member_registration: self.target.clone(),
            },
            OwnerPromotionJournalState::RequestPrepared { request, .. } => {
                OwnerPromotionStatus::RequestPending {
                    request: request.clone(),
                }
            }
            OwnerPromotionJournalState::AwaitingAcceptance {
                request,
                activation,
            } => OwnerPromotionStatus::AwaitingAcceptance {
                request: request.clone(),
                activation: activation.clone(),
            },
            OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                OwnerPromotionStatus::AcceptanceReady {
                    acceptance: acceptance.clone(),
                }
            }
            OwnerPromotionJournalState::MergeMembershipPrepared { acceptance, .. }
            | OwnerPromotionJournalState::MergeHeadPrepared { acceptance, .. } => {
                OwnerPromotionStatus::FinalizationPending {
                    acceptance: acceptance.clone(),
                }
            }
            OwnerPromotionJournalState::Finalized { membership, .. } => {
                OwnerPromotionStatus::Finalized {
                    membership: membership.clone(),
                }
            }
            OwnerPromotionJournalState::Nonactivated { request, .. } => {
                OwnerPromotionStatus::Nonactivated {
                    request: request.clone(),
                }
            }
            OwnerPromotionJournalState::Stale {
                acceptance, reason, ..
            } => OwnerPromotionStatus::Stale {
                acceptance: acceptance.clone(),
                reason: reason.clone(),
            },
        }
    }

    fn validate_contents(&self) -> Result<(), OwnerPromotionError> {
        let valid = match &self.state {
            OwnerPromotionJournalState::Allocated => true,
            OwnerPromotionJournalState::RequestPrepared { request, candidate } => {
                request_has_closed_shape(self.promotion_id, &self.target, request)
                    && prepared_candidate_is_exact_request(candidate, request)
            }
            OwnerPromotionJournalState::AwaitingAcceptance {
                request,
                activation,
            } => {
                request_has_closed_shape(self.promotion_id, &self.target, request)
                    && activation.commit.coord.validate().is_ok()
            }
            OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                acceptance_has_closed_shape(self.promotion_id, &self.target, acceptance)
            }
            OwnerPromotionJournalState::MergeMembershipPrepared {
                acceptance,
                wrapped_key,
                transition,
            } => {
                acceptance_has_closed_shape(self.promotion_id, &self.target, acceptance)
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
                acceptance_has_closed_shape(self.promotion_id, &self.target, acceptance)
                    && wrapped_key_matches_acceptance(wrapped_key, acceptance)
                    && transition_matches_acceptance(transition, &wrapped_key.reference, acceptance)
                    && merge_candidate_matches_finalization(candidate, transition, acceptance)
                    && super::invite::validate_prepared_publication(publication).is_ok()
                    && transition.entry == publication.entry
                    && transition.entry_ref == publication.entry_ref
                    && transition
                        .transition
                        .matches_head(&publication.head, &publication.head_ref)
                    && matches!(
                        &publication.head.activation,
                        super::membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
            }
            OwnerPromotionJournalState::Finalized {
                acceptance,
                membership,
                receipt,
            } => {
                acceptance_has_closed_shape(self.promotion_id, &self.target, acceptance)
                    && finalization_receipt_matches_acceptance(receipt, acceptance)
                    && finalization_receipt_matches_membership(receipt, membership)
            }
            OwnerPromotionJournalState::Nonactivated {
                request,
                nonactivation,
            } => {
                request_has_closed_shape(self.promotion_id, &self.target, request)
                    && nonactivation_matches_request(nonactivation, request)
            }
            OwnerPromotionJournalState::Stale {
                acceptance,
                reason,
                evidence,
            } => {
                acceptance_has_closed_shape(self.promotion_id, &self.target, acceptance)
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
                            },
                        ) => stale_candidate_evidence_matches(nonactivation, receipt, acceptance),
                        _ => false,
                    }
            }
        };
        if !valid {
            return Err(OwnerPromotionError::Protocol(
                "promotion journal state violates its closed protocol invariants".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_begin(&self) -> Result<(), OwnerPromotionError> {
        self.validate_contents()?;
        if !matches!(self.state, OwnerPromotionJournalState::Allocated) {
            return Err(OwnerPromotionError::Protocol(
                "promotion journal begins in a non-initial state".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_acceptance_begin(&self) -> Result<(), OwnerPromotionError> {
        self.validate_contents()?;
        if !matches!(
            self.state,
            OwnerPromotionJournalState::AcceptanceReady { .. }
        ) {
            return Err(OwnerPromotionError::Protocol(
                "candidate promotion journal must begin with its signed acceptance".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_transition(
        &self,
        next: &OwnerPromotionJournal,
    ) -> Result<(), OwnerPromotionError> {
        self.validate_contents()?;
        next.validate_contents()?;
        if self.promotion_id != next.promotion_id || self.target != next.target {
            return Err(OwnerPromotionError::Protocol(
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
                        super::membership::MembershipHeadActivation::StoreCommit { commit }
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
                        super::membership::MembershipHeadActivation::StoreCommit { commit }
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
                        super::membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
            }
            (
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance,
                    publication,
                    candidate,
                    ..
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
                        } if nonactivation_matches_candidate(candidate, nonactivation)
                            && receipt_matches_merge_preparation(
                                receipt,
                                candidate,
                                publication,
                            )
                    )
            }
            _ => false,
        };
        if !valid {
            return Err(OwnerPromotionError::Protocol(
                "promotion journal transition skips or reverses protocol state".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_failed_attempt_replacement(
        &self,
        replacement: &OwnerPromotionJournal,
    ) -> Result<(), OwnerPromotionError> {
        self.validate_contents()?;
        replacement.validate_begin()?;
        if self.target != replacement.target || self.promotion_id == replacement.promotion_id {
            return Err(OwnerPromotionError::Protocol(
                "promotion retry must retain its target and use a fresh identity".to_string(),
            ));
        }
        if !matches!(
            self.state,
            OwnerPromotionJournalState::Nonactivated { .. }
                | OwnerPromotionJournalState::Stale { .. }
        ) {
            return Err(OwnerPromotionError::Protocol(
                "only a failed promotion attempt can be replaced".to_string(),
            ));
        }
        Ok(())
    }
}

struct OwnerPromotionJournalPredecessor {
    promotion_id: OwnerPromotionId,
    target: StoreDeviceRegistrationRef,
    previous_value: String,
}

impl OwnerPromotionJournalPredecessor {
    fn transition_to(
        &self,
        next: &OwnerPromotionJournal,
        remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    ) -> Result<OwnerPromotionJournalTransition, OwnerPromotionError> {
        let previous: OwnerPromotionJournal =
            serde_json::from_str(&self.previous_value).map_err(|error| {
                OwnerPromotionError::Protocol(format!(
                    "parse exact Owner-promotion predecessor: {error}"
                ))
            })?;
        previous.validate_transition(next)?;
        let next_value = serde_json::to_string(next).map_err(|error| {
            OwnerPromotionError::Protocol(format!("serialize Owner-promotion successor: {error}"))
        })?;
        Ok(OwnerPromotionJournalTransition {
            journal_key: format!("owner_promotion/{}", self.promotion_id),
            target_key: target_key(&self.target)?,
            previous_value: self.previous_value.clone(),
            next_value,
            remote_objects,
        })
    }
}

async fn advance_owner_promotion_journal(
    db: &Database,
    previous: OwnerPromotionJournalPredecessor,
    next: OwnerPromotionJournal,
) -> Result<(OwnerPromotionJournalPredecessor, OwnerPromotionJournalState), OwnerPromotionError> {
    let remote_objects = match &next.state {
        OwnerPromotionJournalState::MergeHeadPrepared {
            wrapped_key,
            transition,
            publication,
            candidate,
            ..
        } => {
            candidate.merge_owner_promotion_remote_objects(transition, publication, wrapped_key)?
        }
        _ => Vec::new(),
    };
    let transition = previous.transition_to(&next, remote_objects)?;
    let (successor, state) = next.into_predecessor()?;
    db.advance_owner_promotion_journal(transition)
        .await
        .map_err(|error| {
            OwnerPromotionError::Protocol(format!("advance exact Owner-promotion journal: {error}"))
        })?;
    Ok((successor, state))
}

pub(crate) struct OwnerPromotionJournalTransition {
    journal_key: String,
    target_key: String,
    previous_value: String,
    next_value: String,
    remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
}

impl OwnerPromotionJournalTransition {
    pub(crate) fn into_values(
        self,
    ) -> (
        String,
        String,
        String,
        String,
        Vec<super::remote_object::RemoteObjectRecord>,
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

fn target_key(target: &StoreDeviceRegistrationRef) -> Result<String, OwnerPromotionError> {
    let bytes = serde_json::to_vec(target).map_err(|error| {
        OwnerPromotionError::Protocol(format!("serialize promotion target: {error}"))
    })?;
    Ok(format!(
        "{TARGET_PREFIX}{}",
        super::store_commit::ObjectHash::digest(&bytes)
    ))
}

async fn load_current_merge_membership(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<MembershipChain, OwnerPromotionError> {
    let founder = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?
        .ok_or_else(|| OwnerPromotionError::Protocol("Store founder is absent".to_string()))?;
    let loaded =
        super::membership_ops::load_current_exact_chain(storage, root, Some(&founder), Some(db))
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()));
    loaded
}

fn exact_merge_member_grant(
    membership: &MembershipChain,
    member_pubkey: &str,
) -> Result<MembershipGrantId, OwnerPromotionError> {
    let grants = membership.active_grant_ids(member_pubkey);
    let Some(grant) = grants.iter().next() else {
        return Err(OwnerPromotionError::Protocol(
            "promotion target has no active Member grant".to_string(),
        ));
    };
    if grants.len() != 1
        || membership
            .active_grant(grant)
            .is_none_or(|record| record.role != StoreMembershipRoleGrant::Member)
    {
        return Err(OwnerPromotionError::Protocol(
            "promotion target does not have exactly one active Member grant".to_string(),
        ));
    }
    Ok(grant.clone())
}

async fn resume_request_publication(
    db: &Database,
    storage: &dyn SyncStorage,
    journal: OwnerPromotionJournal,
) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
    let (previous, state) = journal.into_predecessor()?;
    resume_request_publication_state(db, storage, previous, state).await
}

async fn resume_request_publication_state(
    db: &Database,
    storage: &dyn SyncStorage,
    mut previous: OwnerPromotionJournalPredecessor,
    mut state: OwnerPromotionJournalState,
) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
    loop {
        match state {
            OwnerPromotionJournalState::Allocated => {
                return Err(OwnerPromotionError::Protocol(
                    "promotion request allocation has not been prepared".to_string(),
                ));
            }
            OwnerPromotionJournalState::RequestPrepared { request, candidate } => {
                let head = candidate.head_ref();
                let outcome =
                    super::store_outbound::publish_prepared_store_operation(db, storage, candidate)
                        .await?;
                let next_state = match outcome {
                    StoreOperationPublicationOutcome::Activated(commit) => {
                        let activation = OwnerPromotionRequestActivation { commit, head };
                        OwnerPromotionJournalState::AwaitingAcceptance {
                            request,
                            activation,
                        }
                    }
                    StoreOperationPublicationOutcome::RepreparedCandidate(candidate) => {
                        OwnerPromotionJournalState::RequestPrepared { request, candidate }
                    }
                    StoreOperationPublicationOutcome::NonactivatedCandidate {
                        nonactivation,
                        ..
                    } => OwnerPromotionJournalState::Nonactivated {
                        request,
                        nonactivation: nonactivation.into_durable(),
                    },
                    StoreOperationPublicationOutcome::Nonactivated(_) => {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion request lost without exact nonactivation evidence"
                                .to_string(),
                        ));
                    }
                    StoreOperationPublicationOutcome::Reprepared => {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion request unexpectedly used acknowledgement reprepare"
                                .to_string(),
                        ));
                    }
                };
                let next = OwnerPromotionJournal {
                    promotion_id: previous.promotion_id,
                    target: previous.target.clone(),
                    state: next_state,
                };
                (previous, state) = advance_owner_promotion_journal(db, previous, next).await?;
            }
            OwnerPromotionJournalState::AwaitingAcceptance { request, .. }
            | OwnerPromotionJournalState::Nonactivated { request, .. } => return Ok(request),
            OwnerPromotionJournalState::AcceptanceReady { acceptance }
            | OwnerPromotionJournalState::MergeMembershipPrepared { acceptance, .. }
            | OwnerPromotionJournalState::MergeHeadPrepared { acceptance, .. }
            | OwnerPromotionJournalState::Finalized { acceptance, .. }
            | OwnerPromotionJournalState::Stale { acceptance, .. } => {
                return Ok(*acceptance.request);
            }
        }
    }
}

pub async fn begin_owner_promotion(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity: &UserKeypair,
    member_registration: StoreDeviceRegistrationRef,
) -> Result<OwnerPromotionRequest, OwnerPromotionError> {
    let (allocated, failed_attempt) = if let Some(existing) = db
        .load_owner_promotion_target(target_key(&member_registration)?)
        .await?
    {
        if matches!(&existing.state, OwnerPromotionJournalState::Allocated) {
            (Some(existing), None)
        } else if matches!(
            &existing.state,
            OwnerPromotionJournalState::Nonactivated { .. }
                | OwnerPromotionJournalState::Stale { .. }
        ) {
            (None, Some(existing))
        } else {
            return resume_request_publication(db, storage, existing).await;
        }
    } else {
        (None, None)
    };
    let root = db
        .local_store_root_ref()
        .await?
        .ok_or_else(|| OwnerPromotionError::Protocol("Store root is absent".to_string()))?;
    let member = super::store_objects::load_registration_ref(storage, &root, &member_registration)
        .await
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
    let membership = load_current_merge_membership(db, storage, &root).await?;
    let plan = super::store_outbound::prepare_store_operation_commit(
        db,
        storage,
        super::store_outbound::StoreOperationPreparation::new(&membership),
        device_id,
        identity,
    )
    .await?;
    let member_grant = exact_merge_member_grant(&membership, &member.value.author_pubkey)?;
    let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
        OwnerPromotionError::Protocol("promotion author is not an Owner".to_string())
    })?;
    let reusable =
        membership.reusable_author_streams(&plan.registration().author_pubkey, &owner_grant);
    let author_stream = db
        .select_membership_author_stream(&plan.registration().author_pubkey, &owner_grant, reusable)
        .await?;
    let (seq, previous_hash) = membership
        .next_stream_position(
            &plan.registration().author_pubkey,
            &owner_grant,
            author_stream,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let finalization = OwnerPromotionFinalization {
        author_stream,
        seq,
        previous_hash,
    };
    let allocation = match allocated {
        Some(allocation) => allocation,
        None => {
            let allocation = OwnerPromotionJournal {
                promotion_id: OwnerPromotionId::from_generated(db.new_write_id().to_string()),
                target: member_registration.clone(),
                state: OwnerPromotionJournalState::Allocated,
            };
            match failed_attempt {
                Some(previous) => {
                    db.replace_failed_owner_promotion_journal(previous, allocation)
                        .await?
                }
                None => {
                    db.begin_owner_promotion_journal(target_key(&allocation.target)?, allocation)
                        .await?
                }
            }
        }
    };
    let promotion_id = allocation.promotion_id;
    let request = plan.sign_owner_promotion_request(
        promotion_id,
        member_registration.clone(),
        member.value.author_pubkey,
        member_grant,
        finalization,
        identity,
    )?;
    let candidate = super::store_outbound::prepare_store_operation_candidate(
        db,
        storage,
        plan,
        StoreOperationBatch::OwnerPromotionRequest(request.clone()),
    )
    .await?;
    let prepared = OwnerPromotionJournal {
        promotion_id,
        target: member_registration,
        state: OwnerPromotionJournalState::RequestPrepared {
            request: request.clone(),
            candidate: Box::new(candidate),
        },
    };
    let (previous, _) = allocation.into_predecessor()?;
    let (previous, state) = advance_owner_promotion_journal(db, previous, prepared).await?;
    resume_request_publication_state(db, storage, previous, state).await
}

pub async fn accept_owner_promotion(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity: &UserKeypair,
    request: OwnerPromotionRequest,
) -> Result<OwnerPromotionAcceptance, OwnerPromotionError> {
    if let Some(existing) = db
        .load_owner_promotion_journal(request.promotion_id)
        .await?
    {
        if let OwnerPromotionJournalState::AcceptanceReady { acceptance }
        | OwnerPromotionJournalState::MergeMembershipPrepared { acceptance, .. }
        | OwnerPromotionJournalState::MergeHeadPrepared { acceptance, .. }
        | OwnerPromotionJournalState::Finalized { acceptance, .. }
        | OwnerPromotionJournalState::Stale { acceptance, .. } = existing.state
        {
            if acceptance.request.as_ref() == &request {
                return Ok(acceptance);
            }
        }
        return Err(OwnerPromotionError::Protocol(
            "promotion id is already bound to another journal state".to_string(),
        ));
    }
    let (root, registration_ref, registration, _) =
        super::store_outbound::load_local_store_authority(db, device_id, identity).await?;
    if registration_ref != request.member_registration
        || registration.author_pubkey != request.member_pubkey
    {
        return Err(OwnerPromotionError::Protocol(
            "promotion request targets another local device".to_string(),
        ));
    }
    let protocol = super::store_objects::load_store_protocol_root(storage, &root)
        .await
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
    let live = storage.provider_binding().await?;
    if live.store != protocol.value.descriptor.provider || live.device != registration.provider {
        return Err(OwnerPromotionError::Protocol(
            "live storage principal differs from the Store and candidate registration".to_string(),
        ));
    }
    let activation =
        super::store_engine::find_owner_promotion_request_activation(storage, &root, &request)
            .await
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let membership_stream = StreamActivation::grant_authorized_stream_id(
        root.store_root_hash,
        &registration_ref,
        &request.intended_owner_grant,
        StreamAnchorDomain::StoreMembership,
    );
    let membership_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let membership_prefix = membership_head_slot_prefix(
        &request.member_pubkey,
        &request.intended_owner_grant,
        membership_stream,
        1,
    );
    let membership_slot = storage
        .allocate_protocol_slot(&membership_context, &membership_prefix, ".json")
        .await?;
    let recovery_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let recovery_prefix = owner_recovery_semantic_prefix(
        &request.member_pubkey,
        request.intended_owner_grant.clone(),
        1,
    );
    let recovery_slot = storage
        .allocate_protocol_slot(&recovery_context, &recovery_prefix, ".json")
        .await?;
    let anchors = OwnerPromotionAnchors {
        membership: GrantStreamAnchor::StoreMembership {
            first_slot: membership_slot,
        },
        recovery: GrantStreamAnchor::OwnerRecovery {
            first_slot: recovery_slot,
        },
    };
    let acceptance = OwnerPromotionAcceptance::signed(
        request.clone(),
        activation,
        anchors,
        &registration,
        identity,
    )
    .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    super::store_engine::verify_owner_promotion_acceptance(storage, &root, &acceptance)
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
    let journal = OwnerPromotionJournal {
        promotion_id: request.promotion_id,
        target: request.member_registration.clone(),
        state: OwnerPromotionJournalState::AcceptanceReady {
            acceptance: acceptance.clone(),
        },
    };
    db.begin_owner_promotion_acceptance_journal(journal).await?;
    Ok(acceptance)
}

fn prepare_promotion_wrap<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    store_id: &'a str,
    recipient: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    membership: &'a MembershipChain,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<PreparedWrappedStoreKey, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let refs = membership
            .wrapped_key_authority_for(&keys::public_key_hex(identity))
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let authorized = Box::pin(super::invite::load_authorized_owner_keyring(
            storage,
            root.store_root_hash,
            identity,
            store_id,
            &refs,
            encryption,
        ))
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let recipient_key = super::invite::ed25519_hex_to_x25519(recipient)
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let value = super::invite::signed_wrapped_key(
            store_id,
            recipient,
            &recipient_key,
            &authorized,
            identity,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        Box::pin(super::wrapped_store_key::prepare_wrapped_store_key(
            storage,
            root.store_root_hash,
            recipient,
            value,
        ))
        .await
        .map_err(OwnerPromotionError::from)
    })
}

pub fn finalize_owner_promotion<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    acceptance: OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<StoreMembershipStateRef, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let root = db
            .local_store_root_ref()
            .await?
            .ok_or_else(|| OwnerPromotionError::Protocol("Store root is absent".to_string()))?;
        Box::pin(super::store_engine::verify_owner_promotion_acceptance(
            storage,
            &root,
            &acceptance,
        ))
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let promoter = load_owner_promotion_remote_promoter(
            storage,
            &root,
            &acceptance.request.promoter_registration,
        )
        .await?;
        loop {
            let resumed = resume_owner_promotion_finalization(
                db,
                storage,
                device_id,
                identity,
                encryption,
                root.clone(),
                acceptance.clone(),
            )
            .await?;
            match resumed {
                OwnerPromotionResumeOutcome::Complete(membership) => return Ok(membership),
                OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending } => {
                    match activate_owner_promotion_merge_head(
                        db, storage, &root, &promoter, &previous, pending,
                    )
                    .await?
                    {
                        MergeHeadPublication::Continue(next) => {
                            advance_owner_promotion_journal(db, previous, next).await?;
                        }
                        MergeHeadPublication::DurablyComplete { membership } => {
                            return Ok(membership);
                        }
                        MergeHeadPublication::Stale {
                            journal: next,
                            reason,
                        } => {
                            advance_owner_promotion_journal(db, previous, next).await?;
                            return Err(OwnerPromotionError::Stale(Box::new(reason)));
                        }
                    }
                }
            }
        }
    })
}

fn load_owner_promotion_promoter<'a>(
    db: &'a Database,
    device_id: &'a str,
    identity: &'a UserKeypair,
    root: &'a StoreRootRef,
    acceptance: &'a OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<super::store_commit::StoreDeviceRegistration, OwnerPromotionError>,
            > + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let (promoter_root, promoter_ref, promoter, _) = Box::pin(
            super::store_outbound::load_local_store_authority(db, device_id, identity),
        )
        .await?;
        if promoter_root != *root
            || promoter_ref != acceptance.request.promoter_registration
            || promoter.author_pubkey != keys::public_key_hex(identity)
        {
            return Err(OwnerPromotionError::Protocol(
                "promotion finalizer is not the request promoter".to_string(),
            ));
        }
        Ok(promoter)
    })
}

enum OwnerPromotionPreparation {
    Continue(OwnerPromotionJournal),
    Stale {
        journal: OwnerPromotionJournal,
        reason: OwnerPromotionStaleReason,
    },
}

fn prepare_owner_promotion_finalization<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    root: &'a StoreRootRef,
    journal: &'a OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionPreparation, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    let author_stream = acceptance.request.finalization.author_stream;
    let seq = acceptance.request.finalization.seq;
    prepare_merge_owner_promotion_finalization(
        db,
        storage,
        device_id,
        identity,
        encryption,
        root,
        journal,
        acceptance,
        author_stream,
        seq,
    )
}

fn prepare_merge_owner_promotion_finalization<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    root: &'a StoreRootRef,
    journal: &'a OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
    author_stream: super::causal_grants::AuthorStreamId,
    seq: u64,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionPreparation, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let promoter =
            load_owner_promotion_promoter(db, device_id, identity, root, &acceptance).await?;
        let membership = Box::pin(load_current_merge_membership(db, storage, root)).await?;
        if let Some(winner) = membership.head_refs().iter().find(|head| {
            head.coord.author_pubkey == promoter.author_pubkey
                && head.coord.author_owner_grant == acceptance.request.promoter_owner_grant
                && head.coord.stream_id == author_stream
                && head.coord.seq >= seq
        }) {
            let reason = OwnerPromotionStaleReason::MergeFinalizationPointOccupied {
                winner: winner.clone(),
            };
            let next = OwnerPromotionJournal {
                promotion_id: journal.promotion_id,
                target: journal.target.clone(),
                state: OwnerPromotionJournalState::Stale {
                    acceptance,
                    reason: reason.clone(),
                    evidence: Box::new(OwnerPromotionStaleEvidence::BeforePublication),
                },
            };
            return Ok(OwnerPromotionPreparation::Stale {
                journal: next,
                reason,
            });
        }
        let wrapped_key = prepare_promotion_wrap(
            storage,
            root,
            membership.store_id().ok_or_else(|| {
                OwnerPromotionError::Protocol("membership Store id is absent".to_string())
            })?,
            &acceptance.request.member_pubkey,
            identity,
            encryption,
            &membership,
        )
        .await?;
        let candidate = super::store_objects::load_registration_ref(
            storage,
            root,
            &acceptance.request.member_registration,
        )
        .await
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?;
        let entry = membership
            .signed_finalize_owner_promotion_in_stream(
                root,
                &promoter,
                &candidate.value,
                acceptance.clone(),
                identity,
                wrapped_key.reference.clone(),
                db.hlc().now().to_string(),
            )
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let transition = Box::pin(super::invite::prepare_membership_transition(
            storage,
            db,
            root.store_root_hash,
            &membership,
            entry,
            identity,
        ))
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let next = OwnerPromotionJournal {
            promotion_id: journal.promotion_id,
            target: journal.target.clone(),
            state: OwnerPromotionJournalState::MergeMembershipPrepared {
                acceptance,
                wrapped_key,
                transition: Box::new(transition),
            },
        };
        Ok(OwnerPromotionPreparation::Continue(next))
    })
}

fn prepare_merge_store_candidate<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    root: &'a StoreRootRef,
    journal: &'a OwnerPromotionJournalPredecessor,
    acceptance: OwnerPromotionAcceptance,
    wrapped_key: PreparedWrappedStoreKey,
    transition: Box<PreparedMembershipTransition>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionJournal, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        super::invite::publish_prepared_merge_membership_authority(
            storage,
            root.store_root_hash,
            &transition,
            std::slice::from_ref(&wrapped_key),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let membership = Box::pin(load_current_merge_membership(db, storage, root)).await?;
        let plan = Box::pin(super::store_outbound::prepare_store_operation_commit(
            db,
            storage,
            super::store_outbound::StoreOperationPreparation::new(&membership),
            device_id,
            identity,
        ))
        .await?;
        let OwnerPromotionAnchors {
            membership: membership_anchor,
            recovery,
        } = &acceptance.anchors;
        let mut stream_activations = vec![
            StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.request.member_registration.clone(),
                acceptance.request.intended_owner_grant.clone(),
                membership_anchor.clone(),
            ),
            StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.request.member_registration.clone(),
                acceptance.request.intended_owner_grant.clone(),
                recovery.clone(),
            ),
        ];
        stream_activations.sort();
        let mut candidate = Box::pin(super::store_outbound::prepare_store_operation_candidate(
            db,
            storage,
            plan,
            StoreOperationBatch::MergeMembershipActivation {
                transition: transition.transition.clone(),
                stream_activations,
            },
        ))
        .await?;
        let publication = super::invite::finish_membership_transition(
            storage,
            db,
            root.store_root_hash,
            transition.as_ref().clone(),
            super::membership::MembershipHeadActivation::StoreCommit {
                commit: candidate.reference.clone(),
            },
            identity,
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        candidate
            .attach_merge_membership_proof(storage, &publication, None, identity)
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let next = OwnerPromotionJournal {
            promotion_id: journal.promotion_id,
            target: journal.target.clone(),
            state: OwnerPromotionJournalState::MergeHeadPrepared {
                acceptance,
                wrapped_key,
                transition,
                publication: Box::new(publication),
                candidate: Box::new(candidate),
            },
        };
        Ok(next)
    })
}

enum MergeHeadPublication {
    Continue(OwnerPromotionJournal),
    DurablyComplete {
        membership: StoreMembershipStateRef,
    },
    Stale {
        journal: OwnerPromotionJournal,
        reason: OwnerPromotionStaleReason,
    },
}

struct PublishedOwnerPromotionMergeHead {
    acceptance: Box<OwnerPromotionAcceptance>,
    wrapped_key: Box<PreparedWrappedStoreKey>,
    transition: Box<PreparedMembershipTransition>,
    publication: Box<PreparedMembershipPublication>,
    candidate: Box<PreparedStoreOperationCommit>,
}

fn load_owner_promotion_remote_promoter<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    registration: &'a StoreDeviceRegistrationRef,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<super::store_commit::StoreDeviceRegistration, OwnerPromotionError>,
            > + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        Box::pin(super::store_objects::load_registration_ref(
            storage,
            root,
            registration,
        ))
        .await
        .map(|loaded| loaded.value)
        .map_err(|error| OwnerPromotionError::Storage(error.to_string()))
    })
}

fn activate_owner_promotion_merge_head<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    promoter: &'a super::store_commit::StoreDeviceRegistration,
    previous: &'a OwnerPromotionJournalPredecessor,
    published: Box<PublishedOwnerPromotionMergeHead>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<MergeHeadPublication, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let PublishedOwnerPromotionMergeHead {
            acceptance,
            wrapped_key,
            transition,
            publication,
            candidate,
        } = *published;
        let candidate_ref = candidate.reference.clone();
        let candidate_commit = Box::new(candidate.commit.clone());
        let receipt_candidate = candidate.clone();
        super::invite::publish_prepared_merge_membership_authority(
            storage,
            root.store_root_hash,
            &transition,
            std::slice::from_ref(&wrapped_key),
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let membership = finalized_merge_membership_ref(
            storage,
            root,
            &candidate_ref,
            &candidate_commit,
            &transition,
            &publication,
        )
        .await?;
        if membership
            .heads
            .binary_search(&publication.head_ref)
            .is_err()
        {
            return Err(OwnerPromotionError::Protocol(
                "activated promotion head is absent from current membership".to_string(),
            ));
        }
        let finalized = OwnerPromotionJournal {
            promotion_id: previous.promotion_id,
            target: previous.target.clone(),
            state: OwnerPromotionJournalState::Finalized {
                acceptance: *acceptance.clone(),
                membership: membership.clone(),
                receipt: Box::new(OwnerPromotionFinalizationReceipt {
                    candidate: receipt_candidate.clone(),
                    publication: publication.clone(),
                }),
            },
        };
        let journal_transition = previous.transition_to(&finalized, Vec::new())?;
        let remote_objects = candidate.merge_owner_promotion_remote_objects(
            &transition,
            &publication,
            &wrapped_key,
        )?;
        for object in [&transition.entry_ref.object, &wrapped_key.reference.object] {
            let remote = remote_objects
                .iter()
                .find(|remote| remote.object() == object)
                .cloned()
                .ok_or_else(|| {
                    OwnerPromotionError::Protocol(
                        "Owner-promotion completion omits a published membership authority"
                            .to_string(),
                    )
                })?;
            db.mark_remote_object_uploaded(remote)
                .await
                .map_err(|error| {
                    OwnerPromotionError::Protocol(format!(
                        "record published Owner-promotion membership authority: {error}"
                    ))
                })?;
        }
        let outcome = Box::pin(super::invite::publish_prepared_merge_membership_activation(
            db,
            storage,
            root,
            promoter,
            &transition,
            &publication,
            candidate,
            super::store_outbound::StoreMembershipJournalCompletion::OwnerPromotion {
                transition: journal_transition,
                remote_objects,
            },
        ))
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        match outcome {
            StoreOperationPublicationOutcome::Activated(commit) => {
                if commit != candidate_ref {
                    return Err(OwnerPromotionError::Protocol(
                        "Merge promotion activated another prepared candidate".to_string(),
                    ));
                }
                Ok(MergeHeadPublication::DurablyComplete { membership })
            }
            StoreOperationPublicationOutcome::RepreparedCandidate(candidate) => {
                let next = OwnerPromotionJournal {
                    promotion_id: previous.promotion_id,
                    target: previous.target.clone(),
                    state: OwnerPromotionJournalState::MergeHeadPrepared {
                        acceptance: *acceptance,
                        wrapped_key: *wrapped_key,
                        transition,
                        publication,
                        candidate,
                    },
                };
                Ok(MergeHeadPublication::Continue(next))
            }
            StoreOperationPublicationOutcome::NonactivatedCandidate { nonactivation, .. } => {
                let reason = OwnerPromotionStaleReason::MergeActivationRejected;
                let next = OwnerPromotionJournal {
                    promotion_id: previous.promotion_id,
                    target: previous.target.clone(),
                    state: OwnerPromotionJournalState::Stale {
                        acceptance: *acceptance,
                        reason: reason.clone(),
                        evidence: Box::new(OwnerPromotionStaleEvidence::Candidate {
                            nonactivation: nonactivation.into_durable(),
                            receipt: Box::new(OwnerPromotionFinalizationReceipt {
                                candidate: receipt_candidate,
                                publication,
                            }),
                        }),
                    },
                };
                Ok(MergeHeadPublication::Stale {
                    journal: next,
                    reason,
                })
            }
            StoreOperationPublicationOutcome::Nonactivated(_) => {
                Err(OwnerPromotionError::Protocol(
                    "promotion finalization lost without exact nonactivation evidence".to_string(),
                ))
            }
            StoreOperationPublicationOutcome::Reprepared => Err(OwnerPromotionError::Protocol(
                "promotion finalization used acknowledgement reprepare".to_string(),
            )),
        }
    })
}

enum OwnerPromotionResumeOutcome {
    Complete(StoreMembershipStateRef),
    PublishMergeHead {
        previous: OwnerPromotionJournalPredecessor,
        pending: Box<PublishedOwnerPromotionMergeHead>,
    },
}

fn resume_owner_promotion_finalization<'a>(
    db: &'a Database,
    storage: &'a dyn SyncStorage,
    device_id: &'a str,
    identity: &'a UserKeypair,
    encryption: &'a EncryptionService,
    root: StoreRootRef,
    acceptance: OwnerPromotionAcceptance,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<OwnerPromotionResumeOutcome, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let existing = db
            .load_owner_promotion_journal(acceptance.request.promotion_id)
            .await?;
        if let Some(existing) = &existing {
            if let OwnerPromotionJournalState::Finalized { membership, .. } = &existing.state {
                return Ok(OwnerPromotionResumeOutcome::Complete(membership.clone()));
            }
        }
        let journal = existing.ok_or(OwnerPromotionError::NotFound(
            acceptance.request.promotion_id,
        ))?;
        if journal.target != acceptance.request.member_registration {
            return Err(OwnerPromotionError::Protocol(
                "promotion journal targets another registration".to_string(),
            ));
        }
        match &journal.state {
            OwnerPromotionJournalState::AwaitingAcceptance {
                request: persisted, ..
            } if persisted != acceptance.request.as_ref() => {
                return Err(OwnerPromotionError::Protocol(
                    "promotion finalization differs from its persisted request".to_string(),
                ));
            }
            OwnerPromotionJournalState::AcceptanceReady {
                acceptance: persisted,
            }
            | OwnerPromotionJournalState::MergeMembershipPrepared {
                acceptance: persisted,
                ..
            }
            | OwnerPromotionJournalState::MergeHeadPrepared {
                acceptance: persisted,
                ..
            }
            | OwnerPromotionJournalState::Finalized {
                acceptance: persisted,
                ..
            }
            | OwnerPromotionJournalState::Stale {
                acceptance: persisted,
                ..
            } if persisted != &acceptance => {
                return Err(OwnerPromotionError::Protocol(
                    "promotion finalization differs from its persisted acceptance".to_string(),
                ));
            }
            _ => {}
        }
        let (mut previous, mut state) = journal.into_predecessor()?;
        loop {
            match state {
                OwnerPromotionJournalState::AwaitingAcceptance { request, .. } => {
                    if request != *acceptance.request {
                        return Err(OwnerPromotionError::Protocol(
                            "promotion finalization differs from its persisted request".to_string(),
                        ));
                    }
                    let next = OwnerPromotionJournal {
                        promotion_id: previous.promotion_id,
                        target: previous.target.clone(),
                        state: OwnerPromotionJournalState::AcceptanceReady {
                            acceptance: acceptance.clone(),
                        },
                    };
                    (previous, state) = advance_owner_promotion_journal(db, previous, next).await?;
                }
                OwnerPromotionJournalState::AcceptanceReady { acceptance } => {
                    let preparation = prepare_owner_promotion_finalization(
                        db, storage, device_id, identity, encryption, &root, &previous, acceptance,
                    )
                    .await?;
                    match preparation {
                        OwnerPromotionPreparation::Continue(next) => {
                            (previous, state) =
                                advance_owner_promotion_journal(db, previous, next).await?;
                        }
                        OwnerPromotionPreparation::Stale {
                            journal: next,
                            reason,
                        } => {
                            advance_owner_promotion_journal(db, previous, next).await?;
                            return Err(OwnerPromotionError::Stale(Box::new(reason)));
                        }
                    }
                }
                OwnerPromotionJournalState::MergeMembershipPrepared {
                    acceptance,
                    wrapped_key,
                    transition,
                } => {
                    let next = prepare_merge_store_candidate(
                        db,
                        storage,
                        device_id,
                        identity,
                        &root,
                        &previous,
                        acceptance,
                        wrapped_key,
                        transition,
                    )
                    .await?;
                    (previous, state) = advance_owner_promotion_journal(db, previous, next).await?;
                }
                OwnerPromotionJournalState::MergeHeadPrepared {
                    acceptance,
                    wrapped_key,
                    transition,
                    publication,
                    candidate,
                } => {
                    let pending = Box::new(PublishedOwnerPromotionMergeHead {
                        acceptance: Box::new(acceptance),
                        wrapped_key: Box::new(wrapped_key),
                        transition,
                        publication,
                        candidate,
                    });
                    return Ok(OwnerPromotionResumeOutcome::PublishMergeHead { previous, pending });
                }
                OwnerPromotionJournalState::Finalized { membership, .. } => {
                    return Ok(OwnerPromotionResumeOutcome::Complete(membership));
                }
                OwnerPromotionJournalState::Stale { reason, .. } => {
                    return Err(OwnerPromotionError::Stale(Box::new(reason)))
                }
                OwnerPromotionJournalState::Allocated
                | OwnerPromotionJournalState::RequestPrepared { .. }
                | OwnerPromotionJournalState::Nonactivated { .. } => {
                    return Err(OwnerPromotionError::RequestNotActivated)
                }
            }
        }
    })
}

fn finalized_merge_membership_ref<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    candidate_ref: &'a super::store_commit::StoreBatchCommitRef,
    candidate: &'a super::store_commit::StoreBatchCommit,
    transition: &'a PreparedMembershipTransition,
    publication: &'a PreparedMembershipPublication,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<StoreMembershipStateRef, OwnerPromotionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        if candidate.control()
            != Some(&super::store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == candidate_ref
            )
        {
            return Err(OwnerPromotionError::Protocol(
                "activated Owner promotion differs from its exact membership transition"
                    .to_string(),
            ));
        }
        let predecessor = &candidate.membership_state;
        let root_value = super::store_objects::load_store_protocol_root(storage, root)
            .await
            .map_err(|error| OwnerPromotionError::Storage(error.to_string()))?
            .value;
        let mut membership = super::membership_ops::load_anchored_chain_at_exact_heads(
            storage,
            root,
            &root_value.descriptor.founder_pubkey,
            &predecessor.heads,
            &predecessor.resolutions,
        )
        .await
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let super::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
            return Err(OwnerPromotionError::Protocol(
                "Owner promotion predecessor membership is conflicted".to_string(),
            ));
        };
        let exact_predecessor = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            candidate.device_state.recovery().to_vec(),
            resolved.state_hash,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        if exact_predecessor != candidate.membership_state {
            return Err(OwnerPromotionError::Protocol(
                "Owner promotion candidate membership differs from its exact predecessor"
                    .to_string(),
            ));
        }
        membership
            .add_entry(transition.entry.clone())
            .and_then(|()| membership.activate_head_ref(publication.head_ref.clone()))
            .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?;
        let super::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
            return Err(OwnerPromotionError::Protocol(
                "finalized Owner promotion produced conflicted membership".to_string(),
            ));
        };
        let super::membership::MembershipChange::SetMember {
            user_pubkey,
            role:
                StoreMembershipRoleGrant::Owner {
                    recovery: super::membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
                },
            grant_id,
            ..
        } = &transition.entry.change
        else {
            return Err(OwnerPromotionError::Protocol(
                "Merge Owner promotion entry does not add an Owner recovery stream".to_string(),
            ));
        };
        let mut recovery = candidate.device_state.recovery().to_vec();
        if recovery
            .iter()
            .any(|cursor| &cursor.owner_grant == grant_id)
        {
            return Err(OwnerPromotionError::Protocol(
                "Merge Owner promotion recovery stream already exists".to_string(),
            ));
        }
        recovery.push(super::store_commit::OwnerRecoveryCursor {
            owner_grant: grant_id.clone(),
            position: super::store_commit::OwnerRecoveryPosition::BeforeFirst {
                activation: super::store_commit::OwnerRecoveryActivationId::derive(
                    root,
                    user_pubkey,
                    grant_id,
                    acceptance.anchors.recovery(),
                )
                .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))?,
            },
        });
        StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            recovery,
            resolved.state_hash,
        )
        .map_err(|error| OwnerPromotionError::Protocol(error.to_string()))
    })
}

pub async fn owner_promotion_status(
    db: &Database,
    promotion_id: OwnerPromotionId,
) -> Result<OwnerPromotionStatus, OwnerPromotionError> {
    db.load_owner_promotion_journal(promotion_id)
        .await?
        .map(|journal| journal.status())
        .ok_or(OwnerPromotionError::NotFound(promotion_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn second_merge_owner_promotion_verifies_existing_promotion_history() {
        let founder_db = crate::sync::test_helpers::open_test_db();
        let founder = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &founder_db,
            "successive-owner-promotions",
            founder.clone(),
        )
        .await
        .expect("create Merge Store");
        let first_owner = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let encryption = EncryptionService::from_key([42; 32]);
        for member in [&first_owner, &second_owner] {
            super::super::membership_ops::invite_member(
                &store.storage,
                store.home.as_ref(),
                &founder,
                &super::super::hlc::Hlc::new("successive-owner-promotions".to_string()),
                &keys::public_key_hex(member),
                None,
                super::super::membership::MemberRole::Member,
                &encryption,
                store.storage.store_id(),
                "Merge Store",
                &founder_db,
            )
            .await
            .expect("invite Member identity");
        }

        let first_owner_db = crate::sync::test_helpers::open_test_db();
        let second_owner_db = crate::sync::test_helpers::open_test_db();
        crate::sync::test_helpers::install_active_device_fixture(
            &store,
            &founder_db,
            &first_owner_db,
            &first_owner,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate first Owner device");
        crate::sync::test_helpers::install_active_device_fixture(
            &store,
            &founder_db,
            &second_owner_db,
            &second_owner,
            "2026-07-21T00:01:00Z",
        )
        .await
        .expect("activate second Owner device");
        crate::sync::test_helpers::promote_active_member_fixture(
            &store,
            &founder_db,
            &first_owner_db,
            &founder,
            &first_owner,
            &encryption,
        )
        .await
        .expect("promote first Owner");

        let membership = crate::sync::pull::load_cycle_membership(&store.storage, &second_owner_db)
            .await
            .expect("load second Owner membership");
        let (_temp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let pull = crate::sync::store_engine::pull_store_commits(
            &second_owner_db,
            second_owner_db.synced_tables(),
            &store.storage,
            store.root.store_root_hash,
            &store_dir,
            membership
                .chain
                .as_ref()
                .expect("opened Store has membership"),
            Some(&second_owner),
        )
        .await
        .expect("pull second Owner through the first promotion");
        assert!(pull.held_positions.is_empty());

        crate::sync::test_helpers::promote_active_member_fixture(
            &store,
            &founder_db,
            &second_owner_db,
            &founder,
            &second_owner,
            &encryption,
        )
        .await
        .expect("promote second Owner");

        let membership = load_current_merge_membership(&founder_db, &store.storage, &store.root)
            .await
            .expect("load membership after successive promotions");
        assert!(membership.is_owner_now(&keys::public_key_hex(&first_owner)));
        assert!(membership.is_owner_now(&keys::public_key_hex(&second_owner)));
    }

    #[tokio::test]
    async fn merge_owner_promotion_activates_through_its_store_bound_head_and_persists_exact_receipt(
    ) {
        let owner_db = crate::sync::test_helpers::open_test_db();
        let owner = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &owner_db,
            "merge-owner-promotion",
            owner.clone(),
        )
        .await
        .expect("create Merge Store");
        let member = UserKeypair::generate();
        let encryption = EncryptionService::from_key([42; 32]);
        super::super::membership_ops::invite_member(
            &store.storage,
            store.home.as_ref(),
            &owner,
            &super::super::hlc::Hlc::new("owner-device".to_string()),
            &keys::public_key_hex(&member),
            None,
            super::super::membership::MemberRole::Member,
            &encryption,
            store.storage.store_id(),
            "Merge Store",
            &owner_db,
        )
        .await
        .expect("invite Member identity");
        let member_db = crate::sync::test_helpers::open_test_db();
        crate::sync::test_helpers::install_active_device_fixture(
            &store,
            &owner_db,
            &member_db,
            &member,
            "2026-07-20T00:00:00Z",
        )
        .await
        .expect("activate Member device");
        let member_device_id = member_db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read Member device id")
            .expect("Member device id");
        let (_, member_registration, _, _) =
            super::super::store_outbound::load_local_store_authority(
                &member_db,
                &member_device_id,
                &member,
            )
            .await
            .expect("load Member registration");

        Box::pin(crate::sync::test_helpers::promote_active_member_fixture(
            &store,
            &owner_db,
            &member_db,
            &owner,
            &member,
            &encryption,
        ))
        .await
        .expect("activate Owner promotion");

        assert!(
            member_db
                .load_owner_promotion_target(target_key(&member_registration).unwrap())
                .await
                .expect("load candidate target index")
                .is_none(),
            "the accepting candidate does not own the initiating Owner's target index"
        );

        let membership = load_current_merge_membership(&owner_db, &store.storage, &store.root)
            .await
            .expect("load activated membership");
        assert!(membership.is_owner_now(&keys::public_key_hex(&member)));
        let promoted_head = membership
            .head_refs()
            .iter()
            .find(|reference| reference.coord.author_pubkey == keys::public_key_hex(&owner))
            .expect("promoter stream head");
        let opened = super::super::membership_ops::load_exact_membership_head(
            &store.storage,
            &store.root,
            promoted_head,
        )
        .await
        .expect("load activated promotion head");
        assert!(matches!(
            opened.activation,
            super::super::membership::MembershipHeadActivation::StoreCommit { .. }
        ));

        let mut journal = owner_db
            .load_owner_promotion_target(target_key(&member_registration).unwrap())
            .await
            .expect("load finalized promotion journal")
            .expect("finalized promotion journal exists");
        let OwnerPromotionJournalState::Finalized {
            membership: state,
            receipt,
            ..
        } = &mut journal.state
        else {
            panic!("promotion journal is finalized with Merge membership")
        };
        let publication = &receipt.publication;
        let exact_head = publication.head_ref.clone();
        let index = state
            .heads
            .binary_search(&exact_head)
            .expect("finalized membership contains the exact published head");
        let mut substituted = exact_head;
        substituted.head_hash =
            super::super::store_commit::ObjectHash::digest(b"substituted same-coordinate head");
        state.heads[index] = substituted;
        state.heads.sort();
        let encoded =
            serde_json::to_string(&journal).expect("serialize substituted receipt journal");
        owner_db
            .set_protocol_state(
                &format!("owner_promotion/{}", journal.promotion_id),
                &encoded,
            )
            .await
            .expect("install substituted receipt journal");

        assert!(owner_db
            .load_owner_promotion_journal(journal.promotion_id)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn journal_load_rejects_substituted_request_or_prepared_commit_bytes() {
        let owner_db = crate::sync::test_helpers::open_test_db();
        let owner = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &owner_db,
            "corrupt-owner-promotion-request",
            owner.clone(),
        )
        .await
        .expect("create Merge Store");
        let member = UserKeypair::generate();
        let encryption = EncryptionService::from_key([42; 32]);
        super::super::membership_ops::invite_member(
            &store.storage,
            store.home.as_ref(),
            &owner,
            &super::super::hlc::Hlc::new("owner-device".to_string()),
            &keys::public_key_hex(&member),
            None,
            super::super::membership::MemberRole::Member,
            &encryption,
            store.storage.store_id(),
            "Merge Store",
            &owner_db,
        )
        .await
        .expect("invite Member identity");
        let member_db = crate::sync::test_helpers::open_test_db();
        crate::sync::test_helpers::install_active_device_fixture(
            &store,
            &owner_db,
            &member_db,
            &member,
            "2026-07-20T00:00:00Z",
        )
        .await
        .expect("activate Member device");
        let owner_device_id = owner_db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .expect("read Owner device id")
            .expect("Owner device id");
        let (_, member_registration, _, _) =
            super::super::store_outbound::load_local_store_authority(
                &member_db,
                &member_db
                    .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                    .await
                    .expect("read Member device id")
                    .expect("Member device id"),
                &member,
            )
            .await
            .expect("load Member registration");
        store.home.fail_exact_create_before_call(1);
        begin_owner_promotion(
            &owner_db,
            &store.storage,
            &owner_device_id,
            &owner,
            member_registration.clone(),
        )
        .await
        .expect_err("interrupted publication retains RequestPrepared");
        let journal = owner_db
            .load_owner_promotion_target(target_key(&member_registration).unwrap())
            .await
            .expect("load prepared request journal")
            .expect("prepared request journal exists");
        let mut substituted_request = journal.clone();
        let OwnerPromotionJournalState::RequestPrepared { request, .. } =
            &mut substituted_request.state
        else {
            panic!("interrupted request remains RequestPrepared")
        };
        request.member_grant = MembershipGrantId(super::super::store_commit::ObjectHash::digest(
            b"another exact Member grant",
        ));
        let encoded =
            serde_json::to_string(&substituted_request).expect("serialize corrupt request journal");
        owner_db
            .set_protocol_state(
                &format!("owner_promotion/{}", journal.promotion_id),
                &encoded,
            )
            .await
            .expect("install corrupt id journal");
        owner_db
            .set_protocol_state(&target_key(&journal.target).unwrap(), &encoded)
            .await
            .expect("install corrupt target journal");

        assert!(owner_db
            .load_owner_promotion_journal(journal.promotion_id)
            .await
            .is_err());

        let mut substituted_bytes = journal;
        let OwnerPromotionJournalState::RequestPrepared { candidate, .. } =
            &mut substituted_bytes.state
        else {
            panic!("interrupted request remains RequestPrepared")
        };
        let bytes = b"another exact prepared object".to_vec();
        let reference = super::super::storage::ExactObjectRef::new(
            candidate.prepared.reference().slot().clone(),
            bytes.len() as u64,
            super::super::store_commit::ObjectHash::digest(&bytes),
        );
        candidate.prepared =
            super::super::storage::PreparedExactObject::new(reference.clone(), bytes)
                .expect("prepare substituted exact object");
        candidate.reference.object = reference;
        let encoded = serde_json::to_string(&substituted_bytes)
            .expect("serialize substituted prepared bytes journal");
        owner_db
            .set_protocol_state(
                &format!("owner_promotion/{}", substituted_bytes.promotion_id),
                &encoded,
            )
            .await
            .expect("install substituted id journal");
        owner_db
            .set_protocol_state(&target_key(&substituted_bytes.target).unwrap(), &encoded)
            .await
            .expect("install substituted target journal");

        assert!(owner_db
            .load_owner_promotion_journal(substituted_bytes.promotion_id)
            .await
            .is_err());
    }
}
