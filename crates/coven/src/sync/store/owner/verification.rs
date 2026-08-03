use super::pull::*;
use super::verified_history::predecessor_verifies_owner;
use super::verified_history::registration::{
    device_state_has_active_registration, device_state_has_pending_proposal,
    registration_attempt_error, RegistrationLoadError,
};
use crate::protocol::membership::{MembershipChain, MembershipChange, MembershipHeadRef};
use crate::protocol::remote_object;
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
use std::collections::{BTreeMap, BTreeSet};

mod membership;
pub(crate) use membership::StoreMembershipObjectVerifier;

pub(super) enum DeviceStateResolver<'a> {
    Database(&'a crate::database::StoreDatabase),
    Loaded {
        genesis: &'a ResolvedStoreDeviceState,
        states: &'a BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    },
}

impl DeviceStateResolver<'_> {
    async fn resolve(
        &self,
        reference: &StoreDeviceStateRef,
    ) -> Result<ResolvedStoreDeviceState, RegistrationLoadError> {
        let state = match self {
            DeviceStateResolver::Database(database) => {
                return database
                    .resolved_store_device_state(reference)
                    .await
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()));
            }
            DeviceStateResolver::Loaded { genesis, states } => {
                let frontier = &reference.frontier().0;
                if frontier.is_empty() {
                    (*genesis).clone()
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
        };
        if state.state_hash != reference.state_hash() || state.recovery != reference.recovery() {
            return Err(RegistrationLoadError::Invalid(
                "device state differs from its exact predecessor snapshots".to_string(),
            ));
        }
        Ok(state)
    }
}

pub(crate) struct StoreCommitVerifier<'a> {
    storage: &'a dyn SyncStorage,
    root: super::super::protocol_root::VerifiedStoreRoot,
    commits: BTreeMap<StoreBatchCommitRef, VerifiedStoreBatchCommit>,
}

pub(crate) struct VerifiedMergeMembershipClosure {
    objects: crate::database::VerifiedMergeMembershipObjects,
    remote_objects: Vec<remote_object::RemoteObjectRecord>,
    pub(crate) proof: RetainedMergeMembershipProof,
}

impl VerifiedMergeMembershipClosure {
    pub(super) fn objects(&self) -> &crate::database::VerifiedMergeMembershipObjects {
        &self.objects
    }

    pub(super) fn into_remote_objects(self) -> Vec<remote_object::RemoteObjectRecord> {
        self.remote_objects
    }
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
    pub(super) fn store_root_hash(&self) -> ObjectHash {
        self.root.reference().store_root_hash
    }

    pub(crate) fn membership_objects(&self) -> StoreMembershipObjectVerifier<'_, 'a> {
        StoreMembershipObjectVerifier::new(self)
    }

    pub(crate) async fn verified_merge_membership_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
        let Some(StoreControl { transition }) = commit.control() else {
            return Ok(None);
        };
        let entry = self
            .membership_objects()
            .load_entry(&transition.body.entry)
            .await
            .map_err(StorePullError::Object)?;
        let coord = &transition.body.entry.coord;
        let loaded_head = self
            .membership_objects()
            .load_head_at_slot(
                &transition.head_slot,
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
                coord.seq,
            )
            .await
            .map_err(StorePullError::Object)?;
        let head_bytes = loaded_head.bytes;
        let head_object = loaded_head.object;
        let head = loaded_head.value;
        let head_ref = MembershipHeadRef {
            coord: head.entry_coord(),
            head_hash: head.head_hash(),
            object: head_object,
        };
        let objects = crate::database::VerifiedMergeMembershipObjects::verify(
            commit,
            commit_ref,
            &entry.value,
            &head,
            head_ref.clone(),
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let family = commit.candidate_family();
        let resolution = match &entry.value.change {
            MembershipChange::ResolutionActivation { resolution } => Some(resolution.clone()),
            _ => None,
        };
        let resolution_loaded = if let Some(resolution) = &resolution {
            let loaded = self
                .membership_objects()
                .load_resolution(resolution)
                .await
                .map_err(StorePullError::Object)?;
            Some((loaded.bytes, loaded.value))
        } else {
            None
        };
        let remote_objects = activated_merge_membership_remote_objects(
            family,
            &objects,
            MembershipAuthorityBytes::new(entry.bytes.clone(), entry.bytes),
            MembershipAuthorityBytes::new(head_bytes.clone(), head_bytes),
            resolution_loaded
                .as_ref()
                .map(|(bytes, _)| MembershipAuthorityBytes::new(bytes.clone(), bytes.clone())),
            commit_ref,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let resolution_value = resolution_loaded.map(|(_, value)| value);
        let proof = RetainedMergeMembershipProof {
            commit: commit_ref.clone(),
            commit_value: commit.clone(),
            announcement: None,
            entry: transition.body.entry.clone(),
            entry_value: entry.value,
            head: head_ref,
            head_value: head,
            resolution,
            resolution_value,
        };
        Ok(Some(VerifiedMergeMembershipClosure {
            objects,
            remote_objects,
            proof,
        }))
    }

    pub(super) fn from_verified_root(
        _authority: super::HistoryConstructionAuthority,
        storage: &'a dyn SyncStorage,
        root: super::super::protocol_root::VerifiedStoreRoot,
    ) -> Self {
        Self {
            storage,
            root,
            commits: BTreeMap::new(),
        }
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
            self.root.reference().store_root_hash,
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
            self.root.reference().store_root_hash,
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
            let store_root_hash = self.root.reference().store_root_hash;
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
        let root = self.root.reference().clone();
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix = registration_slot_semantic_prefix(&reference.object)?;
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await?;
        let verify_bytes = bytes.clone();
        let expected_root = self.root.reference().clone();
        let expected_reference = reference.clone();
        let pinned_root = self.root.protocol().clone();
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

    pub(crate) async fn verify_owner_recovery_activation(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<
        Option<(
            crate::protocol::membership::MembershipGrantId,
            OwnerRecoveryActivationId,
        )>,
        StorePullError,
    > {
        let mut recoveries = commit.stream_activations().iter().filter_map(|activation| {
            let StreamActivation::GrantAuthorized {
                author_registration,
                grant_id,
                anchor: anchor @ GrantStreamAnchor::OwnerRecovery { .. },
                ..
            } = activation
            else {
                return None;
            };
            Some((author_registration, grant_id, anchor))
        });
        let Some((registration_ref, grant_id, anchor)) = recoveries.next() else {
            return Ok(None);
        };
        if recoveries.next().is_some() {
            return Err(StorePullError::Database(
                "Store commit activates more than one Owner recovery stream".to_string(),
            ));
        }
        let registration = self.load_registration(registration_ref).await?;
        OwnerRecoveryActivationId::derive(
            self.root.reference(),
            &registration.value.author_pubkey,
            grant_id,
            anchor,
        )
        .map(|activation| Some((grant_id.clone(), activation)))
        .map_err(|error| StorePullError::Database(error.to_string()))
    }

    pub(crate) async fn discover_owner_recoveries(
        &self,
        membership: &MembershipChain,
    ) -> Result<Vec<ReferencedStoreDeviceRegistration>, StorePullError> {
        let protocol = &self.root.protocol();
        if membership
            .active_owner_grant(&protocol.descriptor.founder_pubkey)
            .as_ref()
            != Some(&protocol.descriptor.founder_grant)
        {
            return Ok(Vec::new());
        }
        let GrantStreamAnchor::OwnerRecovery { first_slot } = &protocol.descriptor.founder_recovery
        else {
            return Err(StorePullError::Database(
                "Store founder recovery authority has no recovery stream".into(),
            ));
        };
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let mut slot = first_slot.clone();
        let mut predecessor: Option<OwnerRecoveryNodeRef> = None;
        let mut sequence = 1_u64;
        let mut recovered = Vec::new();
        loop {
            let prefix = owner_recovery_semantic_prefix(
                &protocol.descriptor.founder_pubkey,
                protocol.descriptor.founder_grant.clone(),
                sequence,
            );
            let (bytes, object) = match self
                .storage
                .read_protocol_slot(&context, &slot, &prefix)
                .await
            {
                Ok(opened) => opened,
                Err(StorageError::NotFound(_)) => break,
                Err(error) => return Err(StoreObjectError::Storage(error).into()),
            };
            let unverified: OwnerRecoveryNode =
                serde_json::from_slice(&bytes).map_err(|error| {
                    StorePullError::Database(format!("Owner recovery node: {error}"))
                })?;
            let reference = OwnerRecoveryNodeRef {
                owner_pubkey: unverified.owner_pubkey.clone(),
                owner_grant: unverified.owner_grant.clone(),
                sequence: unverified.sequence,
                node_hash: unverified.node_hash(),
                object,
            };
            let node = OwnerRecoveryNode::parse_at(&bytes, self.root.reference(), &reference)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            if reference.owner_pubkey != protocol.descriptor.founder_pubkey
                || reference.owner_grant != protocol.descriptor.founder_grant
                || reference.sequence != sequence
                || node.predecessor != predecessor
                || !predecessor_verifies_owner(
                    membership,
                    &node.membership,
                    &node.owner_pubkey,
                    &node.owner_grant,
                )
            {
                return Err(StorePullError::Database(
                    "Owner recovery stream differs from its root-anchored authority".into(),
                ));
            }
            let registration = self
                .load_registration(&node.readiness.registration)
                .await?
                .value;
            let initial_ack = self
                .load_store_ack(&node.readiness.initial_ack, &registration)
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
            recovered.push(
                ReferencedStoreDeviceRegistration::verified(
                    node.readiness.registration.clone(),
                    registration,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?,
            );
            slot = node.next_slot.clone();
            predecessor = Some(reference);
            sequence = sequence.checked_add(1).ok_or_else(|| {
                StorePullError::Database("Owner recovery sequence overflow".into())
            })?;
        }
        Ok(recovered)
    }

    pub(crate) async fn load_active_registrations(
        &self,
        state: &ResolvedStoreDeviceState,
    ) -> Result<BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>, StorePullError> {
        let mut active = BTreeMap::new();
        for (device_id, record) in &state.devices {
            if !matches!(record.status, StoreDeviceStatus::Active) {
                continue;
            }
            let registration = self.load_registration(&record.registration).await?;
            if registration.value.device_id != *device_id {
                return Err(StorePullError::Database(
                    "resolved Store device state names another exact registration".to_string(),
                ));
            }
            active.insert(
                *device_id,
                ReferencedStoreDeviceRegistration::verified(
                    record.registration.clone(),
                    registration.value,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?,
            );
        }
        Ok(active)
    }

    pub(crate) async fn verify_canonical_owner_registration(
        &self,
        state: &ResolvedStoreDeviceState,
        owner_pubkey: &str,
        selected: &StoreDeviceRegistrationRef,
    ) -> Result<(), StorePullError> {
        let active = self.load_active_registrations(state).await?;
        let canonical = active
            .values()
            .filter(|registration| registration.value().author_pubkey == owner_pubkey)
            .map(ReferencedStoreDeviceRegistration::reference)
            .min();
        if canonical != Some(selected) {
            return Err(StorePullError::Database(
                "conflict-resolution acceptance does not use the canonical active Owner registration"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn load_founder_registration(
        &self,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix =
            founder_registration_semantic_prefix(self.root.protocol().descriptor.creation_id);
        let (bytes, object) = self
            .storage
            .read_protocol_slot(
                &context,
                &self.root.protocol().descriptor.founder_registration,
                &semantic_prefix,
            )
            .await?;
        let verify_bytes = bytes.clone();
        let verify_object = object.clone();
        let verify_root = self.root.reference().clone();
        let verify_root_value = self.root.protocol().clone();
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::ProviderAccessGrant,
        );
        let semantic_prefix = provider_access_grant_semantic_prefix(&reference.grant_id);
        let expected = reference.clone();
        let administrator = administrator.clone();
        let store = self.root.protocol().descriptor.provider.clone();
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
            self.root.reference().store_root_hash,
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let semantic_prefix = device_join_outcome_semantic_prefix(reference.attempt().attempt_id);
        let expected_hash = match reference {
            DeviceJoinOutcomeRef::Activated { outcome_hash, .. }
            | DeviceJoinOutcomeRef::Cancelled { outcome_hash, .. } => *outcome_hash,
        };
        let expected = reference.clone();
        let expected_owner = owner.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionProposal,
        );
        let semantic_prefix = device_exclusion_proposal_semantic_prefix(
            reference.target.device_id,
            reference.proposal_id,
            reference.proposal_hash,
        );
        let expected = reference.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
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
            self.root.reference().store_root_hash,
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let semantic_prefix =
            ack_slot_prefix(&registration.device_id.to_string(), reference.sequence);
        let expected_root = self.root.reference().clone();
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

    pub(crate) async fn predecessor_activates_acknowledgement(
        &mut self,
        order: &StoreCommitOrder,
        expected: &StoreAckRef,
        ack: &StoreAck,
    ) -> Result<bool, StorePullError> {
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
            let commit = self.load_ref(&reference).await?;
            if commit.value().acknowledgement() == Some(expected) {
                let predecessor_cut = commit
                    .value()
                    .order
                    .predecessor_cut()
                    .map_err(|error| StorePullError::Database(error.to_string()))?;
                return Ok(commit.value().author_registration == expected.registration
                    && ack.registration == expected.registration
                    && ack.store_cut == predecessor_cut
                    && ack.device_state == commit.value().device_state);
            }
            pending.extend(commit.value().order.predecessor.iter().cloned());
            pending.extend(commit.value().order.dependencies.values().cloned());
        }
        Ok(false)
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let expected_root = self.root.reference().clone();
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

    pub(crate) async fn load_store_snapshot_stream(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<Vec<crate::database::PublishedStoreSnapshot>, super::writer::snapshot::SnapshotError>
    {
        let mut slot = match &registration.snapshots {
            DeviceStreamAnchor::StoreSnapshots { first_slot } => first_slot.clone(),
            _ => {
                return Err(super::writer::snapshot::SnapshotError::PublicationState(
                    "local Store registration has no snapshot stream anchor".to_string(),
                ));
            }
        };
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let mut generation = 0_u64;
        let mut predecessor = None;
        let mut snapshots = Vec::new();
        loop {
            let prefix = snapshot_slot_prefix(&registration.device_id.to_string(), generation);
            let (bytes, object) = match self
                .storage
                .read_protocol_slot(&context, &slot, &prefix)
                .await
            {
                Ok(value) => value,
                Err(StorageError::NotFound(_)) => break,
                Err(error) => {
                    return Err(super::writer::snapshot::SnapshotError::Bucket(error));
                }
            };
            let expected_root = self.root.reference().clone();
            let expected_registration_ref = registration_ref.clone();
            let expected_registration = registration.clone();
            let expected_object = object.clone();
            let (reference, meta) = run_blocking_object_verification(
                &prefix,
                &object,
                Box::new(move || {
                    let semantic_hash = SnapshotMeta::semantic_hash_from_bytes(&bytes)
                        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                    let reference = StoreSnapshotRef {
                        generation,
                        snapshot_hash: semantic_hash,
                        object: expected_object,
                    };
                    let meta = super::writer::snapshot::verify_store_snapshot_bytes(
                        &expected_root,
                        &expected_registration_ref,
                        &expected_registration,
                        &reference,
                        &bytes,
                    )
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                    Ok((reference, meta))
                }),
            )
            .await
            .map_err(super::writer::snapshot::SnapshotError::StoreObject)?;
            if meta.predecessor != predecessor {
                return Err(super::writer::snapshot::SnapshotError::Parse(
                    "Store snapshot stream has an invalid exact predecessor".to_string(),
                ));
            }
            let successor_slot = meta.successor.next_slot.clone();
            slot = successor_slot.clone();
            predecessor = Some(reference.clone());
            snapshots.push(crate::database::PublishedStoreSnapshot {
                reference,
                successor_slot,
                meta,
            });
            generation = generation.checked_add(1).ok_or_else(|| {
                super::writer::snapshot::SnapshotError::Parse(
                    "Store snapshot generation overflow".to_string(),
                )
            })?;
        }
        Ok(snapshots)
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
        if attempt.value.store_root != self.root.reference().clone() {
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
            .verify(self.root.object(), owner, &administrator)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(LoadedDeviceJoinAttemptEvidence { attempt })
    }

    pub(crate) async fn load_reclaim_authorization(
        &self,
        reference: &ReclaimAuthorizationRef,
    ) -> Result<VerifiedReclaimAuthorization, StoreObjectError> {
        let evidence_context = ProtocolObjectContext::store_encrypted(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreReclaimEvidence,
        );
        let evidence_prefix = reclaim_evidence_semantic_prefix(reference.evidence.evidence_hash);
        let expected_evidence = reference.evidence.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreReclaimAuthorization,
        );
        let authorization_prefix =
            reclaim_authorization_semantic_prefix(reference.authorization_hash);
        let owner_pubkey = evidence.value.author_pubkey.clone();
        let expected_authorization = reference.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
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
            self.root.reference().store_root_hash,
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
                verify_store_root(
                    self.root.reference().store_root_hash,
                    unverified.store_root_hash,
                )?;
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
            self.root.reference().store_root_hash,
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
        let value = StoreAck::parse_at(&bytes, self.root.reference(), &reference, registration)
            .map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix,
                key: object.slot().logical_key().to_string(),
                source: Box::new(source),
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let expected_root = self.root.reference().clone();
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
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let expected = reference.clone();
        let expected_registration = registration.clone();
        let expected_commit = commit.clone();
        let store_root_hash = self.root.reference().store_root_hash;
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
            self.root.reference().store_root_hash,
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
        let store_root_hash = self.root.reference().store_root_hash;
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

    pub(super) async fn load_commit_device_operations(
        &mut self,
        resolver: Option<&DeviceStateResolver<'_>>,
        commit: &StoreBatchCommit,
        predecessor_state: &ResolvedStoreDeviceState,
        predecessor_membership: Option<&MembershipChain>,
    ) -> Result<VerifiedStoreDeviceOperations, RegistrationLoadError> {
        if commit.device_exclusion_proposals().is_empty()
            && commit.device_exclusion_outcomes().is_empty()
        {
            return VerifiedStoreDeviceOperations::without_exclusions(commit)
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()));
        }
        let predecessor = predecessor_membership.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device exclusion activation has no exact predecessor membership authority"
                    .to_string(),
            )
        })?;
        let mut proposals = Vec::with_capacity(commit.device_exclusion_proposals().len());
        for reference in commit.device_exclusion_proposals() {
            let opened = self
                .load_device_exclusion_proposal(reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let proposal = &opened.object.value;
            if proposal.frozen_device_state != commit.device_state
                || !device_state_has_active_registration(predecessor_state, &proposal.target)
                || !device_state_has_active_registration(
                    predecessor_state,
                    &proposal.owner_registration,
                )
                || !predecessor_verifies_owner(
                    predecessor,
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
            proposals.push(RetainedStoreDeviceExclusionProposal::from_verified(&opened));
        }
        let mut outcomes = Vec::with_capacity(commit.device_exclusion_outcomes().len());
        for reference in commit.device_exclusion_outcomes() {
            if !device_state_has_pending_proposal(predecessor_state, reference.proposal()) {
                return Err(RegistrationLoadError::Invalid(
                    "device exclusion outcome does not resolve an exact pending proposal"
                        .to_string(),
                ));
            }
            let proposal = self
                .load_device_exclusion_proposal(reference.proposal())
                .await
                .map_err(RegistrationLoadError::Object)?;
            let outcome = self
                .load_device_exclusion_outcome(reference, &proposal)
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
                || !predecessor_verifies_owner(
                    predecessor,
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
            match (&outcome.object.value, reference) {
                (
                    StoreDeviceExclusionOutcome::Cancelled(_),
                    StoreDeviceExclusionOutcomeRef::Cancelled(_),
                ) => {}
                (
                    StoreDeviceExclusionOutcome::Excluded(exclusion),
                    StoreDeviceExclusionOutcomeRef::Excluded(_),
                ) => {
                    let StoreDeviceExclusionProof {
                        frozen_device_state,
                        remaining_device_acks,
                        cutoff,
                    } = &exclusion.proof;
                    if frozen_device_state != &proposal.object.value.frozen_device_state {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proof names another frozen device state".to_string(),
                        ));
                    }
                    let resolver = resolver.ok_or_else(|| {
                        RegistrationLoadError::Invalid(
                            "Merge device exclusion proof has no materialized state resolver"
                                .to_string(),
                        )
                    })?;
                    let frozen = resolver
                        .resolve(&proposal.object.value.frozen_device_state)
                        .await?;
                    if !device_state_has_active_registration(&frozen, &proposal.object.value.target)
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion proposal frozen state does not contain its active target"
                                .to_string(),
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
                    let target_stream = StreamActivation::device_authorized_stream_id(
                        self.root.reference().store_root_hash,
                        &proposal.object.value.target,
                        StreamAnchorDomain::StoreAnnouncements,
                    );
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
                                "device exclusion proof repeats a remaining registration"
                                    .to_string(),
                            ));
                        }
                        let registration = self
                            .load_registration(&required_record.registration)
                            .await
                            .map_err(RegistrationLoadError::Object)?
                            .value;
                        let ack = self
                            .load_store_ack(reference, &registration)
                            .await
                            .map_err(RegistrationLoadError::Object)?
                            .value;
                        if !self
                            .predecessor_activates_acknowledgement(&commit.order, reference, &ack)
                            .await
                            .map_err(registration_attempt_error)?
                        {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement is not activated in the outcome predecessor"
                                    .to_string(),
                            ));
                        }
                        let ack_state = resolver.resolve(&ack.device_state).await?;
                        if !device_state_has_pending_proposal(&ack_state, &proposal.reference) {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement does not observe the pending proposal"
                                    .to_string(),
                            ));
                        }
                        let freeze = ack
                            .exclusions
                            .proposal_freezes
                            .iter()
                            .find(|freeze| freeze.proposal == proposal.reference)
                            .ok_or_else(|| {
                                RegistrationLoadError::Invalid(
                                    "device exclusion proof acknowledgement omits the exact proposal freeze"
                                        .to_string(),
                                )
                            })?;
                        let target_cut = &freeze.target_cut.0;
                        if target_cut.len() > 1
                            || target_cut.keys().any(|stream| stream != &target_stream)
                        {
                            return Err(RegistrationLoadError::Invalid(
                                "device exclusion proof acknowledgement includes a non-target stream"
                                    .to_string(),
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
                                            "device exclusion proof target cuts fork at one sequence"
                                                .to_string(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    if certified != required.into_keys().collect()
                        || cutoff != &StoreHistoryCut(joined)
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
                    let predecessor_target = predecessor_cut
                        .0
                        .get(&target_stream)
                        .map(|reference| BTreeMap::from([(target_stream, reference.clone())]));
                    let target_predecessor_cut =
                        StoreHistoryCut(predecessor_target.unwrap_or_default());
                    if !cutoff.frontier().covers(&target_predecessor_cut.frontier()) {
                        return Err(RegistrationLoadError::Invalid(
                            "device exclusion outcome predecessor advances the target beyond its certified cutoff"
                                .to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(RegistrationLoadError::Invalid(
                        "device exclusion outcome variant differs from its exact reference"
                            .to_string(),
                    ))
                }
            }
            outcomes.push(
                RetainedStoreDeviceExclusionOutcome::from_verified(
                    reference,
                    RetainedStoreDeviceExclusionProposal::from_verified(&proposal),
                    &outcome,
                )
                .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?,
            );
        }
        RetainedStoreDeviceOperations::from_sources(proposals, outcomes)
            .verify_for(self.root.reference(), commit)
            .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))
    }

    pub(crate) async fn read_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StoreObjectError> {
        self.storage
            .read_protocol_object(context, object, semantic_prefix)
            .await
            .map_err(StoreObjectError::from)
    }

    pub(crate) async fn read_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, ExactObjectRef), StorageError> {
        self.storage
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    pub(crate) fn remember(
        &mut self,
        commit: VerifiedStoreBatchCommit,
    ) -> Result<(), StoreProtocolError> {
        if commit.store_root_hash() != self.root.reference().store_root_hash {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: self.root.reference().store_root_hash,
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

pub(crate) struct LoadedDeviceJoinAttemptEvidence {
    pub(crate) attempt: VerifiedObject<DeviceJoinAttempt>,
}
