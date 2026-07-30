use super::pull::*;
use super::verified_history::{
    load_membership_at_exact_heads_with_verified_activations, MergeHistoryVerifier,
    VerifiedMergeConflictResolutionActivation, VerifiedMergeMembershipPrefix,
};
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::*;
use crate::protocol::store_commit::{
    ack_slot_prefix, device_exclusion_outcome_semantic_prefix,
    device_exclusion_proposal_semantic_prefix, device_join_attempt_semantic_prefix,
    device_join_outcome_semantic_prefix, founder_registration_semantic_prefix,
    package_semantic_prefix, provider_access_grant_semantic_prefix, registration_semantic_prefix,
    snapshot_slot_prefix, DeviceJoinAttemptRef, DeviceJoinOutcome, DeviceJoinOutcomeRef,
    SnapshotMeta, StoreAck, StoreAckRef, StoreDeviceExclusionOutcomeRef,
    StoreDeviceExclusionProposal, StoreDeviceExclusionProposalRef, StoreDeviceHeadRef,
    StoreSnapshotRef,
};
use crate::storage::{
    decode_protocol_object, run_blocking_object_verification, verify_store_root, StoreObjectError,
    VerifiedObject,
};
use crate::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use crate::sync::store::StoreError;
use crate::sync::store::{
    reclaim_authorization_semantic_prefix, reclaim_evidence_semantic_prefix,
    reclaim_receipt_semantic_prefix, ReclaimAuthorization, ReclaimAuthorizationRef,
    ReclaimEvidence, ReclaimReceipt, ReclaimReceiptRef,
};
use std::collections::BTreeMap;

pub(crate) struct StoreCommitVerifier<'a> {
    storage: &'a dyn SyncStorage,
    root: StoreRootRef,
    verified_root: VerifiedObject<StoreProtocolRoot>,
    commits: BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit>,
}

struct ExactAnnouncementPath {
    next_slot: crate::storage::cloud::ObjectSlot,
    accepted_head: Option<StoreDeviceHeadRef>,
    commits: Vec<StoreBatchCommitRef>,
}

#[derive(Debug)]
pub(crate) struct VerifiedReclaimAuthorization {
    pub(crate) authorization: VerifiedObject<ReclaimAuthorization>,
    pub(crate) evidence: VerifiedObject<ReclaimEvidence>,
}

#[derive(Debug)]
pub(crate) struct VerifiedReclaimReceipt {
    pub(crate) receipt: VerifiedObject<ReclaimReceipt>,
    pub(crate) executor: StoreDeviceRegistration,
}

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn load_membership_at_verified_prefix(
        &self,
        heads: &[crate::protocol::membership::MembershipHeadRef],
        resolutions: &[crate::protocol::membership::StoreMembershipConflictResolutionRef],
        verified_activations: &VerifiedMergeMembershipPrefix,
        pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        load_membership_at_exact_heads_with_verified_activations(
            self,
            heads,
            resolutions,
            verified_activations,
            pending_resolution,
        )
        .await
    }

    pub(super) fn from_verified_root(
        _authority: super::history::HistoryConstructionAuthority,
        storage: &'a dyn SyncStorage,
        root: &StoreRootRef,
        verified_root: VerifiedObject<StoreProtocolRoot>,
    ) -> Result<Self, StoreProtocolError> {
        let verified_reference = StoreRootRef {
            store_root_id: verified_root.value.descriptor.store_root_id(),
            store_root_hash: verified_root.semantic_hash,
            object: verified_root.object.clone(),
        };
        if &verified_reference != root {
            return Err(StoreProtocolError::Malformed(
                "verified Store root belongs to another exact reference".to_string(),
            ));
        }
        Ok(Self {
            storage,
            root: root.clone(),
            verified_root,
            commits: BTreeMap::new(),
        })
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
        if let Some(previous) = previous {
            if previous.value().author_registration != *registration_ref
                || previous.author() != registration
            {
                return Err(StoreError::InvalidOutbound(
                    "verified Store commit author differs from its announcement registration"
                        .to_string(),
                ));
            }
        }
        let path = self
            .load_exact_announcement_path(
                registration_ref,
                registration,
                previous.map(VerifiedStoreBatchCommit::reference),
            )
            .await?;
        for reference in &path.commits {
            let loaded;
            let verified =
                if let Some(previous) = previous.filter(|commit| reference == commit.reference()) {
                    previous
                } else {
                    loaded = self.load_ref(reference).await?;
                    &loaded
                };
            if verified.reference() != reference
                || verified.author() != registration
                || verified.value().author_registration != *registration_ref
            {
                return Err(StoreError::InvalidOutbound(
                    "verified Store announcement history belongs to another author".to_string(),
                ));
            }
        }
        Ok((path.next_slot, path.accepted_head))
    }

    async fn load_exact_announcement_path(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        previous: Option<&StoreBatchCommitRef>,
    ) -> Result<ExactAnnouncementPath, StoreError> {
        let DeviceStreamAnchor::StoreAnnouncements { first_slot } = &registration.store_commits
        else {
            return Err(StoreError::InvalidOutbound(
                "Merge registration has no Store announcement anchor".to_string(),
            ));
        };
        let Some(target) = previous else {
            return Ok(ExactAnnouncementPath {
                next_slot: first_slot.clone(),
                accepted_head: None,
                commits: Vec::new(),
            });
        };
        let expected_stream = StreamActivation::device_authorized_stream_id(
            self.root.store_root_hash,
            registration_ref,
            StreamAnchorDomain::StoreAnnouncements,
        );
        if target.coord.stream_id != expected_stream {
            return Err(StoreError::InvalidOutbound(
                "local predecessor belongs to another Store announcement stream".to_string(),
            ));
        }
        let activation = registration
            .store_announcement_activation(registration_ref)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            .activation_id();
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let mut slot = first_slot.clone();
        let mut predecessor: Option<StoreDeviceHeadRef> = None;
        let mut commits = Vec::new();
        for sequence in 1..=target.coord.sequence() {
            let prefix = head_slot_prefix(&registration.device_id.to_string(), sequence);
            let (bytes, object) = self
                .storage
                .read_protocol_slot(&context, &slot, &prefix)
                .await
                .map_err(StoreObjectError::from)?;
            let verify_bytes = bytes.clone();
            let expected_registration = registration_ref.clone();
            let expected_registration_value = registration.clone();
            let store_root_hash = self.root.store_root_hash;
            let expected_predecessor = predecessor
                .as_ref()
                .map(|reference| reference.object.clone());
            let head = run_blocking_object_verification(
                &prefix,
                &object,
                Box::new(move || {
                    let unverified: StoreDeviceHead =
                        serde_json::from_slice(&verify_bytes).map_err(|error| {
                            StoreProtocolError::Malformed(error.to_string())
                        })?;
                    if unverified.author_registration != expected_registration
                        || unverified.successor.activation != activation
                        || unverified.successor.predecessor != expected_predecessor
                    {
                        return Err(StoreProtocolError::Malformed(format!(
                            "local Store head {sequence} does not extend its exact activated predecessor"
                        )));
                    }
                    StoreDeviceHead::parse_at(
                        &verify_bytes,
                        store_root_hash,
                        &expected_registration_value,
                        &unverified.commit,
                    )
                }),
            )
            .await?;
            let is_target = sequence == target.coord.sequence();
            if is_target && head.commit != *target {
                return Err(StoreError::MergeAnnouncementOccupied {
                    expected: Box::new(target.clone()),
                    actual: Box::new(head.commit),
                });
            }
            commits.push(head.commit.clone());
            let reference = StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object,
            };
            if is_target {
                return Ok(ExactAnnouncementPath {
                    next_slot: head.successor.next_slot,
                    accepted_head: Some(reference),
                    commits,
                });
            }
            slot = head.successor.next_slot;
            predecessor = Some(reference);
        }
        Err(StoreError::InvalidOutbound(
            "local Store predecessor traversal ended early".to_string(),
        ))
    }

    pub(super) async fn verify_terminal_candidate_head(
        &mut self,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<crate::protocol::remote_object::VerifiedCandidateHead, StorePullError> {
        let storage = self.storage;
        let root = self.root.clone();
        let candidate_ref = candidate.reference();
        let candidate_commit = candidate.value();
        let candidate_author = candidate.author();
        if candidate_head.commit != *candidate_ref
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
            candidate_ref,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let verified_predecessor = match candidate_commit.order.predecessor() {
            Some(predecessor) => Some(self.load_ref(predecessor).await?),
            None => None,
        };
        candidate_head_object.verify(&candidate_head.to_bytes())?;
        let (candidate_slot, predecessor_head) = self
            .exact_next_announcement_slot(
                &candidate_commit.author_registration,
                candidate_author,
                verified_predecessor.as_ref(),
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
            candidate_ref.coord.sequence(),
        );
        match storage
            .read_protocol_slot(&context, &candidate_slot, &candidate_prefix)
            .await
        {
            Err(StorageError::NotFound(_)) => Ok(
                crate::protocol::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                    object: candidate_head_object.clone(),
                },
            ),
            Ok((bytes, object))
                if bytes == candidate_head.to_bytes() && object == *candidate_head_object =>
            {
                Ok(
                    crate::protocol::remote_object::VerifiedCandidateHead::ExactLateCandidate {
                        object: candidate_head_object.clone(),
                    },
                )
            }
            Ok((bytes, object)) => {
                object.verify(&bytes)?;
                let unverified: StoreDeviceHead =
                    serde_json::from_slice(&bytes).map_err(|error| {
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
                let competing_commit = self.load_ref(&unverified.commit).await?;
                if competing_commit.author() != candidate_author {
                    return Err(StorePullError::Database(
                        "competing terminal candidate belongs to another author".to_string(),
                    ));
                }
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
                    crate::protocol::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                        object: candidate_head_object.clone(),
                    },
                )
            }
            Err(error) => Err(StorePullError::Storage(error)),
        }
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
    ) -> Result<crate::protocol::remote_object::VerifiedCandidateNonactivation, StorePullError>
    {
        let activation_commit_ref = activation_commit.reference();
        let activation_commit_value = activation_commit.value();
        let candidate_ref = candidate.reference();
        let candidate_commit = candidate.value();
        let verified_activation_head = StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        };
        if activation_head.commit != *activation_commit_ref
            || locator.activation_head() != &verified_activation_head
            || !activation_commit_value
                .device_exclusion_outcomes()
                .contains(&StoreDeviceExclusionOutcomeRef::Excluded(
                    locator.exclusion().clone(),
                ))
            || !super::verified_history::registration::device_state_has_active_registration(
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
        if exact_cut != Some(&StoreHistoryCut(locator.accepted_cut().clone())) {
            return Err(StorePullError::Database(
                "author exclusion locator differs from the verified outcome cutoff".to_string(),
            ));
        }
        if candidate_head.commit != *candidate_ref
            || candidate_head.author_registration != locator.exclusion().proposal.target
            || candidate_commit.author_registration != candidate_head.author_registration
        {
            return Err(StorePullError::Database(
                "candidate head differs from the excluded author and exact candidate".to_string(),
            ));
        }
        let verified_candidate_head = self
            .verify_terminal_candidate_head(candidate, candidate_head, candidate_head_object)
            .await?;
        let durable = crate::protocol::remote_object::CandidateNonactivation::from_durable_parts(
            candidate_ref,
            candidate_commit,
            crate::protocol::remote_object::CandidateNonactivationProof::AuthorExclusion {
                exclusion: locator.exclusion().clone(),
                accepted_cut: locator.accepted_cut().clone(),
                activation_head: verified_activation_head,
            },
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        crate::protocol::remote_object::VerifiedCandidateNonactivation::from_verified_author_exclusion(
            durable,
            candidate_ref.clone(),
            verified_candidate_head,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))
    }

    pub(crate) async fn load_registration(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix = registration_slot_semantic_prefix(&reference.object)?;
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await?;
        let verify_bytes = bytes.clone();
        let expected_root = self.root.clone();
        let expected_reference = reference.clone();
        let pinned_root = self.verified_root.value.clone();
        let value = run_blocking_object_verification(
            &semantic_prefix,
            &reference.object,
            Box::new(move || {
                verify_opened_registration(
                    &verify_bytes,
                    &expected_root,
                    &expected_reference,
                    &pinned_root,
                )
            }),
        )
        .await?;
        Ok(VerifiedObject {
            value,
            bytes,
            semantic_hash: reference.registration_hash,
            object: reference.object.clone(),
        })
    }

    pub(crate) async fn load_founder_registration(
        &self,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix =
            founder_registration_semantic_prefix(self.verified_root.value.descriptor.creation_id);
        let (bytes, object) = self
            .storage
            .read_protocol_slot(
                &context,
                &self.verified_root.value.descriptor.founder_registration,
                &semantic_prefix,
            )
            .await?;
        let verify_bytes = bytes.clone();
        let verify_object = object.clone();
        let verify_root = self.root.clone();
        let verify_root_value = self.verified_root.value.clone();
        let (value, reference) = run_blocking_object_verification(
            &semantic_prefix,
            &object,
            Box::new(move || {
                let unverified: StoreDeviceRegistration = serde_json::from_slice(&verify_bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                let reference =
                    StoreDeviceRegistrationRef::from_registration(&unverified, verify_object);
                let value = verify_opened_registration(
                    &verify_bytes,
                    &verify_root,
                    &reference,
                    &verify_root_value,
                )?;
                Ok((value, reference))
            }),
        )
        .await?;
        Ok(VerifiedObject {
            value,
            bytes,
            semantic_hash: reference.registration_hash,
            object,
        })
    }

    async fn load_exact_object<T>(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
        semantic_hash: ObjectHash,
        verify: impl FnOnce(&[u8]) -> Result<T, StoreProtocolError> + Send + 'static,
    ) -> Result<VerifiedObject<T>, StoreObjectError>
    where
        T: Send + 'static,
    {
        let bytes = self
            .storage
            .read_protocol_object(context, object, semantic_prefix)
            .await?;
        let verify_bytes = bytes.clone();
        let value = run_blocking_object_verification(
            semantic_prefix,
            object,
            Box::new(move || verify(&verify_bytes)),
        )
        .await?;
        Ok(VerifiedObject {
            value,
            bytes,
            semantic_hash,
            object: object.clone(),
        })
    }

    pub(crate) async fn load_provider_access_grant(
        &self,
        reference: &crate::protocol::provider::StoreMemberProviderAccessGrantRef,
        administrator: &StoreDeviceRegistration,
    ) -> Result<
        VerifiedObject<crate::protocol::provider::StoreMemberProviderAccessGrant>,
        StoreObjectError,
    > {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::ProviderAccessGrant,
        );
        let semantic_prefix = provider_access_grant_semantic_prefix(&reference.grant_id);
        let expected = reference.clone();
        let administrator = administrator.clone();
        let store = self.verified_root.value.descriptor.provider.clone();
        self.load_exact_object(
            &context,
            &reference.object,
            &semantic_prefix,
            reference.grant_hash,
            move |bytes| {
                let grant: crate::protocol::provider::StoreMemberProviderAccessGrant =
                    decode_protocol_object(bytes)?;
                expected
                    .verify(&grant)
                    .and_then(|()| grant.verify(&store, &administrator))
                    .map_err(|_| StoreProtocolError::ProviderAccessMismatch)?;
                Ok(grant)
            },
        )
        .await
    }

    pub(crate) async fn load_owner_signed_device_join_attempt(
        &self,
        reference: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<DeviceJoinAttempt>, StoreObjectError> {
        let (bytes, semantic_prefix) = self.read_device_join_attempt(reference).await?;
        self.verify_device_join_attempt(reference, owner, bytes, semantic_prefix)
            .await
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
        let (bytes, semantic_prefix) = self.read_device_join_attempt(reference).await?;
        let unverified: DeviceJoinAttempt =
            decode_protocol_object(&bytes).map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.clone(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        let owner = self
            .load_registration(&unverified.owner_registration)
            .await?;
        let attempt = self
            .verify_device_join_attempt(reference, &owner.value, bytes, semantic_prefix)
            .await?;
        Ok((attempt, owner))
    }

    async fn read_device_join_attempt(
        &self,
        reference: &DeviceJoinAttemptRef,
    ) -> Result<(Vec<u8>, String), StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let semantic_prefix = device_join_attempt_semantic_prefix(reference.attempt_id);
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await?;
        Ok((bytes, semantic_prefix))
    }

    async fn verify_device_join_attempt(
        &self,
        reference: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
        bytes: Vec<u8>,
        semantic_prefix: String,
    ) -> Result<VerifiedObject<DeviceJoinAttempt>, StoreObjectError> {
        let expected = reference.clone();
        let owner = owner.clone();
        let verify_bytes = bytes.clone();
        let value = run_blocking_object_verification(
            &semantic_prefix,
            &reference.object,
            Box::new(move || DeviceJoinAttempt::parse_at(&verify_bytes, &expected, &owner)),
        )
        .await?;
        Ok(VerifiedObject {
            value,
            bytes,
            semantic_hash: reference.attempt_hash,
            object: reference.object.clone(),
        })
    }

    pub(crate) async fn load_device_join_outcome(
        &self,
        reference: &DeviceJoinOutcomeRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<DeviceJoinOutcome>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let semantic_prefix = device_join_outcome_semantic_prefix(reference.attempt().attempt_id);
        let expected_hash = match reference {
            DeviceJoinOutcomeRef::Activated { outcome_hash, .. }
            | DeviceJoinOutcomeRef::Cancelled { outcome_hash, .. } => *outcome_hash,
        };
        let expected = reference.clone();
        let expected_owner = owner.clone();
        let expected_store_root_hash = self.root.store_root_hash;
        self.load_exact_object(
            &context,
            reference.object(),
            &semantic_prefix,
            expected_hash,
            move |bytes| {
                let outcome: DeviceJoinOutcome = decode_protocol_object(bytes)?;
                verify_store_root(expected_store_root_hash, outcome.store_root_hash)?;
                outcome
                    .owner_registration
                    .verify_registration(&expected_owner)?;
                if !crate::keys::verify_signature_hex(
                    &expected_owner.device_signing_pubkey,
                    &outcome.signature,
                    &outcome.canonical_signed_bytes(),
                ) {
                    return Err(StoreProtocolError::InvalidSignature);
                }
                expected.verify_outcome(&outcome)?;
                Ok(outcome)
            },
        )
        .await
    }

    pub(crate) async fn load_device_exclusion_proposal(
        &self,
        reference: &StoreDeviceExclusionProposalRef,
    ) -> Result<VerifiedDeviceExclusionProposal, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionProposal,
        );
        let semantic_prefix = device_exclusion_proposal_semantic_prefix(
            reference.target.device_id,
            reference.proposal_id,
            reference.proposal_hash,
        );
        let expected = reference.clone();
        let expected_store_root_hash = self.root.store_root_hash;
        let opened = self
            .load_exact_object(
                &context,
                &reference.object,
                &semantic_prefix,
                reference.proposal_hash,
                move |bytes| {
                    let proposal: StoreDeviceExclusionProposal = decode_protocol_object(bytes)?;
                    expected.verify_proposal(&proposal)?;
                    verify_store_root(expected_store_root_hash, proposal.store_root_hash)?;
                    Ok(proposal)
                },
            )
            .await?;
        let target = self.load_registration(&opened.value.target).await?.value;
        let owner = self
            .load_registration(&opened.value.owner_registration)
            .await?
            .value;
        let verified =
            StoreDeviceExclusionProposal::parse_at(&opened.bytes, reference, &target, &owner)
                .map_err(|source| StoreObjectError::InvalidObject {
                    semantic_prefix,
                    key: reference.object.slot().logical_key().to_string(),
                    source: Box::new(source),
                })?;
        Ok(VerifiedDeviceExclusionProposal {
            reference: reference.clone(),
            object: VerifiedObject {
                value: verified,
                ..opened
            },
            target,
            owner,
        })
    }

    pub(crate) async fn load_device_exclusion_outcome(
        &self,
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: &VerifiedDeviceExclusionProposal,
    ) -> Result<VerifiedDeviceExclusionOutcome, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        );
        let semantic_prefix = device_exclusion_outcome_semantic_prefix(
            proposal.object.value.target.device_id,
            proposal.object.value.proposal_id,
        );
        let expected = reference.clone();
        let opened = self
            .load_exact_object(
                &context,
                reference.object(),
                &semantic_prefix,
                reference.outcome_hash(),
                move |bytes| {
                    let outcome: StoreDeviceExclusionOutcome = decode_protocol_object(bytes)?;
                    if outcome.outcome_hash() != expected.outcome_hash()
                        || outcome.proposal() != expected.proposal()
                    {
                        return Err(StoreProtocolError::DeviceStateMismatch);
                    }
                    Ok(outcome)
                },
            )
            .await?;
        let owner_ref = match &opened.value {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => &exclusion.owner_registration,
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                &cancellation.owner_registration
            }
        };
        let owner = self.load_registration(owner_ref).await?.value;
        let verified = StoreDeviceExclusionOutcome::parse_at(
            &opened.bytes,
            reference,
            &proposal.object.value,
            &proposal.target,
            &owner,
        )
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix,
            key: reference.object().slot().logical_key().to_string(),
            source: Box::new(source),
        })?;
        Ok(VerifiedDeviceExclusionOutcome {
            object: VerifiedObject {
                value: verified,
                ..opened
            },
            owner,
        })
    }

    pub(crate) async fn load_store_ack(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<StoreAck>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let semantic_prefix =
            ack_slot_prefix(&registration.device_id.to_string(), reference.sequence);
        let expected_root = self.root.clone();
        let expected = reference.clone();
        let expected_registration = registration.clone();
        self.load_exact_object(
            &context,
            &reference.object,
            &semantic_prefix,
            reference.ack_hash,
            move |bytes| {
                StoreAck::parse_at(bytes, &expected_root, &expected, &expected_registration)
            },
        )
        .await
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &StoreSnapshotRef,
    ) -> Result<(StoreSnapshotRef, SnapshotMeta), StoreObjectError> {
        let prefix =
            snapshot_slot_prefix(&registration.device_id.to_string(), reference.generation);
        if registration_ref.device_id != registration.device_id {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "Store snapshot registration reference names another device".to_string(),
                )),
            });
        }
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let expected_root = self.root.clone();
        let expected_registration_ref = registration_ref.clone();
        let expected_registration = registration.clone();
        let expected_reference = reference.clone();
        let opened = self
            .load_exact_object(
                &context,
                &reference.object,
                &prefix,
                reference.snapshot_hash,
                move |bytes| {
                    super::writer::snapshot::verify_store_snapshot_bytes(
                        &expected_root,
                        &expected_registration_ref,
                        &expected_registration,
                        &expected_reference,
                        bytes,
                    )
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
                },
            )
            .await?;
        Ok((reference.clone(), opened.value))
    }

    pub(crate) async fn load_device_join_attempt_evidence(
        &self,
        reference: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<LoadedDeviceJoinAttemptEvidence, StorePullError> {
        let attempt = self
            .load_owner_signed_device_join_attempt(reference, owner)
            .await?;
        self.validate_device_join_attempt_evidence(attempt, owner)
            .await
    }

    pub(crate) async fn validate_device_join_attempt_evidence(
        &self,
        attempt: VerifiedObject<DeviceJoinAttempt>,
        owner: &StoreDeviceRegistration,
    ) -> Result<LoadedDeviceJoinAttemptEvidence, StorePullError> {
        if attempt.value.store_root != self.root.clone() {
            return Err(StorePullError::Database(
                "device join attempt names another Store root".to_string(),
            ));
        }
        let offer = &attempt.value.provider_approval.request.offer;
        let administrator = self
            .load_registration(&offer.provider_admin.administrator)
            .await?
            .value;
        attempt
            .value
            .provider_approval
            .verify(&self.verified_root, owner, &administrator)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(LoadedDeviceJoinAttemptEvidence { attempt })
    }

    pub(crate) async fn load_reclaim_authorization(
        &self,
        reference: &ReclaimAuthorizationRef,
    ) -> Result<VerifiedReclaimAuthorization, StoreObjectError> {
        let evidence_context = ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimEvidence,
        );
        let evidence_prefix = reclaim_evidence_semantic_prefix(reference.evidence.evidence_hash);
        let expected_evidence = reference.evidence.clone();
        let expected_store_root_hash = self.root.store_root_hash;
        let evidence = self
            .load_exact_object(
                &evidence_context,
                &reference.evidence.object,
                &evidence_prefix,
                reference.evidence.evidence_hash,
                move |bytes| {
                    let evidence: ReclaimEvidence = decode_protocol_object(bytes)?;
                    expected_evidence.verify(&evidence)?;
                    verify_store_root(expected_store_root_hash, evidence.store_root_hash)?;
                    Ok(evidence)
                },
            )
            .await?;
        let authorization_context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimAuthorization,
        );
        let authorization_prefix =
            reclaim_authorization_semantic_prefix(reference.authorization_hash);
        let owner_pubkey = evidence.value.author_pubkey.clone();
        let expected_authorization = reference.clone();
        let expected_store_root_hash = self.root.store_root_hash;
        let authorization = self
            .load_exact_object(
                &authorization_context,
                &reference.object,
                &authorization_prefix,
                reference.authorization_hash,
                move |bytes| {
                    let authorization: ReclaimAuthorization = decode_protocol_object(bytes)?;
                    expected_authorization.verify(&authorization, &owner_pubkey)?;
                    verify_store_root(expected_store_root_hash, authorization.store_root_hash)?;
                    Ok(authorization)
                },
            )
            .await?;
        if authorization.value.target != evidence.value.claim.target() {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: authorization_prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "reclaim authorization target differs from its exact evidence".to_string(),
                )),
            });
        }
        Ok(VerifiedReclaimAuthorization {
            authorization,
            evidence,
        })
    }

    pub(crate) async fn load_reclaim_receipt(
        &self,
        reference: &ReclaimReceiptRef,
    ) -> Result<VerifiedReclaimReceipt, StoreObjectError> {
        self.load_reclaim_authorization(&reference.authorization)
            .await?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimReceipt,
        );
        let prefix = reclaim_receipt_semantic_prefix(reference.receipt_hash);
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &prefix)
            .await?;
        let unverified: ReclaimReceipt =
            serde_json::from_slice(&bytes).map_err(|error| StoreObjectError::InvalidObject {
                semantic_prefix: prefix.clone(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(error.to_string())),
            })?;
        let executor = self.load_registration(&unverified.executor).await?.value;
        let receipt = reference
            .verify(&unverified, &executor)
            .and_then(|()| {
                verify_store_root(self.root.store_root_hash, unverified.store_root_hash)?;
                Ok(unverified)
            })
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: prefix,
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        Ok(VerifiedReclaimReceipt {
            receipt: VerifiedObject {
                value: receipt,
                bytes,
                semantic_hash: reference.receipt_hash,
                object: reference.object.clone(),
            },
            executor,
        })
    }

    pub(crate) async fn load_store_ack_predecessor(
        &self,
        successor_ref: &StoreAckRef,
        successor: &StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<Option<(StoreAckRef, VerifiedObject<StoreAck>)>, StoreObjectError> {
        if successor.registration != successor_ref.registration
            || successor.sequence != successor_ref.sequence
        {
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: ack_slot_prefix(
                    &registration.device_id.to_string(),
                    successor_ref.sequence,
                ),
                key: successor_ref.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "Store acknowledgement differs from its exact reference".to_string(),
                )),
            });
        }
        let Some(object) = successor.successor.predecessor.as_ref() else {
            return Ok(None);
        };
        let sequence =
            successor
                .sequence
                .checked_sub(1)
                .ok_or_else(|| StoreObjectError::InvalidObject {
                    semantic_prefix: ack_slot_prefix(&registration.device_id.to_string(), 0),
                    key: object.slot().logical_key().to_string(),
                    source: Box::new(StoreProtocolError::InvalidAckSequence(0)),
                })?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let semantic_prefix = ack_slot_prefix(&registration.device_id.to_string(), sequence);
        let bytes = self
            .storage
            .read_protocol_object(&context, object, &semantic_prefix)
            .await?;
        let ack_hash = StoreAck::semantic_hash_from_bytes(&bytes).map_err(|source| {
            StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.clone(),
                key: object.slot().logical_key().to_string(),
                source: Box::new(source),
            }
        })?;
        let reference = StoreAckRef {
            registration: successor_ref.registration.clone(),
            sequence,
            ack_hash,
            object: object.clone(),
        };
        let value =
            StoreAck::parse_at(&bytes, &self.root, &reference, registration).map_err(|source| {
                StoreObjectError::InvalidObject {
                    semantic_prefix,
                    key: object.slot().logical_key().to_string(),
                    source: Box::new(source),
                }
            })?;
        Ok(Some((
            reference.clone(),
            VerifiedObject {
                value,
                bytes,
                semantic_hash: reference.ack_hash,
                object: reference.object.clone(),
            },
        )))
    }

    pub(crate) async fn load_owner_recovery_node(
        &self,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<VerifiedObject<OwnerRecoveryNode>, StoreObjectError> {
        let semantic_prefix = owner_recovery_semantic_prefix(
            &reference.owner_pubkey,
            reference.owner_grant.clone(),
            reference.sequence,
        );
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let expected_root = self.root.clone();
        let expected = reference.clone();
        self.load_exact_object(
            &context,
            &reference.object,
            &semantic_prefix,
            reference.node_hash,
            move |bytes| OwnerRecoveryNode::parse_at(bytes, &expected_root, &expected),
        )
        .await
    }

    pub(crate) async fn load_head(
        &self,
        reference: &StoreDeviceHeadRef,
        registration: &StoreDeviceRegistration,
        commit: &StoreBatchCommitRef,
    ) -> Result<VerifiedObject<StoreDeviceHead>, StoreObjectError> {
        let semantic_prefix =
            head_slot_prefix(&registration.device_id.to_string(), commit.coord.sequence());
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let expected = reference.clone();
        let expected_registration = registration.clone();
        let expected_commit = commit.clone();
        let store_root_hash = self.root.store_root_hash;
        self.load_exact_object(
            &context,
            &reference.object,
            &semantic_prefix,
            reference.head_hash,
            move |bytes| {
                let head = StoreDeviceHead::parse_at(
                    bytes,
                    store_root_hash,
                    &expected_registration,
                    &expected_commit,
                )?;
                let actual = head.head_hash();
                if actual != expected.head_hash {
                    return Err(StoreProtocolError::ObjectHashMismatch {
                        expected: expected.head_hash,
                        actual,
                    });
                }
                Ok(head)
            },
        )
        .await
    }

    pub(crate) async fn load_store_package(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<VerifiedObject<Vec<u8>>>, StoreObjectError> {
        let verified = self.load_ref(reference).await?;
        let commit = verified.value();
        let stream_id = verified.reference().coord.stream_id.to_string();
        let Some(package) = commit.store_package() else {
            return Ok(None);
        };
        let semantic_prefix = package_semantic_prefix(
            commit.candidate_family(),
            &stream_id,
            commit.seq(),
            package.content_hash,
        );
        let context = ProtocolObjectContext::store_encrypted(
            commit.store_root_hash,
            ProtocolObjectDomain::StorePackage,
        );
        let expected_commit = commit.clone();
        self.load_exact_object(
            &context,
            &package.object,
            &semantic_prefix,
            package.content_hash,
            move |bytes| {
                expected_commit.verify_store_package(bytes)?;
                Ok(bytes.to_vec())
            },
        )
        .await
        .map(Some)
    }

    pub(super) async fn load_ref(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        if let Some(commit) = self.commits.get(reference) {
            return Ok(commit.clone());
        }
        let verified = self.load_verified_commit(reference).await?;
        self.commits.insert(reference.clone(), verified.clone());
        Ok(verified)
    }

    pub(crate) async fn authenticate_bytes(
        &mut self,
        reference: &StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        if let Some(commit) = self.commits.get(reference) {
            if commit.value().to_bytes() == bytes {
                return Ok(commit.clone());
            }
            return Err(StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(StoreProtocolError::Malformed(
                    "supplied Store commit bytes differ from the authenticated operation input"
                        .to_string(),
                )),
            });
        }
        let verified = self.verify_commit_bytes(reference, bytes.to_vec()).await?;
        self.commits.insert(reference.clone(), verified.clone());
        Ok(verified)
    }

    async fn load_verified_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        let semantic_prefix = semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            ProtocolObjectDomain::StoreCommit,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await
            .map_err(StoreObjectError::Storage)?;
        self.verify_commit_bytes(reference, bytes).await
    }

    async fn verify_commit_bytes(
        &self,
        reference: &StoreBatchCommitRef,
        bytes: Vec<u8>,
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        let semantic_prefix = semantic_prefix_from_exact_object(&reference.object, ".json")
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: "Store candidate commit".to_string(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
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
        let author = self.load_registration(&author_reference).await?.value;
        let expected_reference = reference.clone();
        let expected_author = author.clone();
        let store_root_hash = self.root.store_root_hash;
        let verify_bytes = bytes;
        run_blocking_object_verification(
            &semantic_prefix,
            &reference.object,
            Box::new(move || {
                VerifiedStoreBatchCommit::parse(
                    &verify_bytes,
                    store_root_hash,
                    &expected_reference,
                    &expected_author,
                )
            }),
        )
        .await
    }

    pub(crate) fn verified_root(&self) -> &StoreProtocolRoot {
        &self.verified_root.value
    }

    pub(crate) fn verified_root_object(&self) -> &VerifiedObject<StoreProtocolRoot> {
        &self.verified_root
    }

    pub(crate) fn storage(&self) -> &'a dyn SyncStorage {
        self.storage
    }

    pub(crate) fn root(&self) -> &StoreRootRef {
        &self.root
    }

    pub(crate) fn remember(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        if commit.store_root_hash() != self.root.store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: self.root.store_root_hash,
                actual: commit.store_root_hash(),
            });
        }
        self.commits.insert(commit.reference().clone(), commit);
        Ok(())
    }
}

fn registration_slot_semantic_prefix(object: &ExactObjectRef) -> Result<String, StoreObjectError> {
    object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .map(str::to_string)
        .ok_or_else(|| StoreObjectError::InvalidObject {
            semantic_prefix: object.slot().logical_key().to_string(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(
                "device registration slot has no .json suffix".to_string(),
            )),
        })
}

fn verify_opened_registration(
    bytes: &[u8],
    root: &StoreRootRef,
    reference: &StoreDeviceRegistrationRef,
    pinned_root: &StoreProtocolRoot,
) -> Result<StoreDeviceRegistration, StoreProtocolError> {
    let registration = StoreDeviceRegistration::parse_at(bytes, root, reference.device_id)?;
    reference.verify_registration(&registration)?;
    let expected_prefix = match &registration.origin {
        StoreDeviceRegistrationOrigin::Founder { creation_id }
            if *creation_id == pinned_root.descriptor.creation_id
                && registration.provider
                    == pinned_root.descriptor.founder_provider_admin.provider
                && reference.object.slot() == &pinned_root.descriptor.founder_registration =>
        {
            founder_registration_semantic_prefix(*creation_id)
        }
        StoreDeviceRegistrationOrigin::Founder { .. } => {
            return Err(StoreProtocolError::InvalidFounder);
        }
        _ if reference.object.slot() != &pinned_root.descriptor.founder_registration => {
            registration_semantic_prefix(&reference.device_id.to_string())
        }
        _ => return Err(StoreProtocolError::InvalidFounder),
    };
    let actual_prefix = reference
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StoreProtocolError::Malformed(
                "device registration slot has no .json suffix".to_string(),
            )
        })?;
    if actual_prefix != expected_prefix {
        return Err(StoreProtocolError::Malformed(
            "device registration exact slot does not match its signed origin".to_string(),
        ));
    }
    Ok(registration)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CommitCoverageError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
}

pub(crate) async fn load_provider_access_activation(
    history_verifier: &mut MergeHistoryVerifier<'_>,
    access: &crate::protocol::provider::ActivatedStoreMemberProviderAccessGrant,
    administrator: &StoreDeviceRegistration,
) -> Result<VerifiedStoreBatchCommit, StorePullError> {
    let grant = history_verifier
        .load_provider_access_grant(&access.grant_ref, administrator)
        .await?;
    if grant.value != access.grant {
        return Err(StorePullError::Database(
            "device provider approval embeds a different access grant than its exact reference"
                .to_string(),
        ));
    }
    let activation = history_verifier.load_ref(&access.activation).await?;
    if activation.value().provider_access_grants() != std::slice::from_ref(&access.grant_ref)
        || activation.value().author_registration != access.grant.administrator
        || activation.author() != administrator
    {
        return Err(StorePullError::Database(
            "device provider approval activation is not the administrator's exact sole access grant"
                .to_string(),
        ));
    }
    Ok(activation)
}

pub(crate) struct LoadedDeviceJoinAttemptEvidence {
    pub(crate) attempt: VerifiedObject<DeviceJoinAttempt>,
}
