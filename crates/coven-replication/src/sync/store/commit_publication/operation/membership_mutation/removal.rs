use super::{
    decode_membership_mutation, exact_owned_remote, MembershipMutationError,
    MembershipMutationPlan, MembershipMutationProgress, ReplacementWrappedKey,
    RevokeMembershipPublication, RevokeMutationPlan,
};
use coven_keys::encryption::{self, EncryptionService};
use coven_keys::keys;
use coven_protocol::membership::{
    self, AuthorStreamId, MemberRole, MembershipChain, MembershipChange,
};
use coven_storage as cloud_storage;
use coven_storage::cloud::{CloudAccessOutcome, CloudAccessState, RevokeOutcome};

pub(crate) struct AuthorizedMembershipRevocation<'operation, 'storage, 'input> {
    operation:
        &'operation mut crate::sync::store::commit_publication::AuthorizedWriterOperation<'storage>,
    chain: &'input mut MembershipChain,
    revokee_pubkey: &'input str,
    store_id: &'input str,
    timestamp: &'input str,
    current_encryption: &'input EncryptionService,
    pending_rotation: &'input dyn cloud_storage::CloudSyncRotationStateAccess,
    _permit: coven_database::store::MembershipMutationPermit,
}

/// Build a removal that revokes provider access, rotates the Store key, and
/// publishes the signed membership change as one durable operation.
///
/// An Owner removal composes a Store candidate against this device's next stream
/// position and returns it for staging; the turn that claimed the position is
/// released with the plan, and the publication below takes its own. A writer that
/// takes the position in between is not a lost operation: publication reads the
/// occupant, verifies it, and returns a nonactivated candidate, which
/// the revocation operation ends on — restoring provider access, deleting what
/// the candidate published, and clearing the staged mutation, so the next removal
/// composes at the position that follows.
impl<'operation, 'storage, 'input> AuthorizedMembershipRevocation<'operation, 'storage, 'input> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn begin(
        operation: &'operation mut crate::sync::store::commit_publication::AuthorizedWriterOperation<'storage>,
        chain: &'input mut MembershipChain,
        revokee_pubkey: &'input str,
        store_id: &'input str,
        timestamp: &'input str,
        current_encryption: &'input EncryptionService,
        pending_rotation: &'input dyn cloud_storage::CloudSyncRotationStateAccess,
    ) -> Self {
        let permit = operation.membership_mutation_permit().await;
        Self {
            operation,
            chain,
            revokee_pubkey,
            store_id,
            timestamp,
            current_encryption,
            pending_rotation,
            _permit: permit,
        }
    }

    async fn build_revoke_mutation(
        &mut self,
        stream_id: AuthorStreamId,
    ) -> Result<RevokeMutationPlan, MembershipMutationError> {
        let chain = self.chain.clone();
        let revokee_pubkey = self.revokee_pubkey;
        let store_id = self.store_id;
        let timestamp = self.timestamp;
        let current_encryption = self.current_encryption;
        if chain.store_id() != Some(store_id) {
            return Err(MembershipMutationError::InvalidDurableMutation(format!(
                "membership chain store {:?} differs from requested store {store_id:?}",
                chain.store_id()
            )));
        }
        let members = chain.current_members();
        if !members.iter().any(|(pubkey, _)| pubkey == revokee_pubkey) {
            return Err(MembershipMutationError::NotAMember(
                revokee_pubkey.to_string(),
            ));
        }
        let current_owners = members
            .iter()
            .filter(|(pubkey, role)| pubkey != revokee_pubkey && *role == MemberRole::Owner)
            .map(|(pubkey, _)| pubkey.clone())
            .collect::<Vec<_>>();
        if current_owners.is_empty() {
            return Err(MembershipMutationError::LastOwner);
        }
        let current_keyring = self
            .operation
            .open_keyring_or_for_membership(&chain, current_encryption)
            .await?;
        let new_keyring = current_keyring
            .with_appended_generation(
                current_keyring
                    .current_generation()
                    .checked_add(1)
                    .ok_or_else(|| {
                        MembershipMutationError::Crypto("store key generation overflow".to_string())
                    })?,
                encryption::generate_random_key(),
            )
            .map_err(MembershipMutationError::Encryption)?;
        let remaining_members = members
            .iter()
            .filter(|(pubkey, _)| pubkey != revokee_pubkey)
            .cloned()
            .collect::<Vec<_>>();
        let mut wraps = Vec::with_capacity(remaining_members.len());
        for (recipient, _) in remaining_members {
            let recipient_key = keys::ed25519_hex_to_x25519_public_key(&recipient)?;
            wraps.push(ReplacementWrappedKey {
                prepared: self
                    .operation
                    .prepare_replacement_wrapped_key(
                        store_id,
                        &recipient,
                        &recipient_key,
                        &new_keyring,
                    )
                    .await?,
            });
        }
        wraps.sort_by(|left, right| left.prepared.reference.cmp(&right.prepared.reference));
        let wrapped_keys = wraps
            .iter()
            .map(|wrap| wrap.prepared.reference.clone())
            .collect();
        let publication = if chain.is_owner_now(revokee_pubkey) {
            let plan = self
                .operation
                .prepare_plan()
                .await
                .map_err(MembershipMutationError::from)?;
            let entry = self.operation.sign_owner_barrier_removal(
                &chain,
                stream_id,
                revokee_pubkey.to_string(),
                wrapped_keys,
                plan.device_state().clone(),
                timestamp.to_string(),
            )?;
            let transition = self
                .operation
                .prepare_membership_transition(&chain, entry)
                .await?;
            let mut candidate = self
                .operation
                .prepare_candidate(
                plan,
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::MergeMembershipActivation {
                    transition: transition.transition.clone(),
                    stream_activations: Vec::new(),
                },
            )
            .await
            .map_err(MembershipMutationError::from)?;
            let head = self
                .operation
                .finish_membership_transition(
                    transition.clone(),
                    membership::MembershipHeadActivation::StoreCommit {
                        commit: candidate.reference.clone(),
                    },
                )
                .await?;
            self.operation
                .attach_membership_proof(&mut candidate, &head)?;
            RevokeMembershipPublication::StoreActivated {
                transition: Box::new(transition),
                candidate: Box::new(candidate),
                publication: Box::new(head),
            }
        } else {
            let entry = self.operation.sign_direct_removal(
                &chain,
                stream_id,
                revokee_pubkey.to_string(),
                wrapped_keys,
                timestamp.to_string(),
            )?;
            RevokeMembershipPublication::Direct {
                publication: Box::new(
                    self.operation
                        .prepare_membership_publication(&chain, entry)
                        .await?,
                ),
            }
        };
        let provider_account_email = chain
            .current_member_provider_email(revokee_pubkey)
            .map(str::to_string);
        Ok(RevokeMutationPlan {
            publication,
            revokee_pubkey: revokee_pubkey.to_string(),
            desired_access: CloudAccessState::Absent {
                member_pubkey: revokee_pubkey.to_string(),
                provider_account_email: provider_account_email.clone(),
            },
            prior_access: CloudAccessState::Present {
                member_pubkey: revokee_pubkey.to_string(),
                provider_account_email,
            },
            wraps,
            keyring_payload: new_keyring
                .to_keyring_payload()
                .map_err(MembershipMutationError::Encryption)?,
        })
    }

    pub(crate) async fn execute(mut self) -> Result<EncryptionService, MembershipMutationError> {
        let (mut plan, mut progress, intent_hash) = match self
            .operation
            .outbound_membership_mutation()
            .await?
        {
            Some(row) => {
                let intent_hash = row.intent_hash;
                let (pending, progress) = decode_membership_mutation(row)?;
                let MembershipMutationPlan::Revoke(plan) = pending else {
                    return Err(MembershipMutationError::PendingMutation(
                        "an admission is pending".to_string(),
                    ));
                };
                if !plan.matches_request(
                    &self.operation.writer_pubkey(),
                    self.revokee_pubkey,
                    self.store_id,
                ) {
                    return Err(MembershipMutationError::PendingMutation(
                        "the pending removal has different immutable inputs".to_string(),
                    ));
                }
                (plan, progress, intent_hash)
            }
            None => {
                let is_current = self
                    .chain
                    .current_members()
                    .iter()
                    .any(|(pubkey, _)| pubkey == self.revokee_pubkey);
                let was_removed = self.chain.entries().iter().any(|entry| {
                    matches!(
                        &entry.change,
                        MembershipChange::RemoveMember { user_pubkey, .. }
                            if user_pubkey == self.revokee_pubkey
                    )
                });
                if !is_current && was_removed {
                    if !self
                        .chain
                        .current_members()
                        .into_iter()
                        .any(|(_, role)| role == MemberRole::Owner)
                    {
                        return Err(MembershipMutationError::LastOwner);
                    }
                    let keyring = self
                        .operation
                        .open_keyring_for_membership(self.chain)
                        .await?;
                    match self
                        .operation
                        .set_membership_access(CloudAccessState::Absent {
                            member_pubkey: self.revokee_pubkey.to_string(),
                            provider_account_email: None,
                        })
                        .await?
                    {
                        CloudAccessOutcome::Absent(RevokeOutcome::Revoked) => {}
                        CloudAccessOutcome::Absent(RevokeOutcome::Unsupported) => {
                            tracing::info!(
                                "cloud provider offers no per-member credential revocation; chain revocation and store key rotation protect later content",
                            );
                        }
                        CloudAccessOutcome::Present(_) => {
                            return Err(MembershipMutationError::Crypto(
                                "provider returned present outcome for absent access request"
                                    .to_string(),
                            ));
                        }
                    }
                    return Ok(keyring);
                }
                let stream_id = self
                    .operation
                    .select_membership_author_stream(self.chain)
                    .await?;
                let plan = Box::pin(self.build_revoke_mutation(stream_id)).await?;
                plan.validate_closed_shape()?;
                let encoded = MembershipMutationPlan::Revoke(plan.clone()).encode()?;
                let progress = MembershipMutationProgress::Pending;
                let progress_bytes = progress.encode()?;
                let pending_generation =
                    EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                        .map_err(MembershipMutationError::Encryption)?
                        .current_generation();
                let intent_hash = self
                    .operation
                    .stage_membership_mutation(
                        encoded,
                        progress_bytes,
                        plan.candidate_remote_objects()?,
                        Some(pending_generation),
                    )
                    .await?;
                (plan, progress, intent_hash)
            }
        };
        let Self {
            operation,
            chain,
            pending_rotation,
            _permit,
            ..
        } = self;
        let pending_generation =
            EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                .map_err(MembershipMutationError::Encryption)?
                .current_generation();
        match &progress {
            MembershipMutationProgress::RevokeActivated { .. } => {
                pending_rotation.mark_committed_mutation(pending_generation, intent_hash)
            }
            _ => pending_rotation.mark_candidate(pending_generation, intent_hash),
        }
        .map_err(MembershipMutationError::RotationState)?;
        let mut persistence = operation.membership_mutation_persistence(intent_hash);
        plan.validate_closed_shape()?;
        if matches!(
            progress,
            MembershipMutationProgress::AdmissionGranted { .. }
        ) {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "removal carries admission progress".to_string(),
            ));
        }
        let mut validated_chain = chain.with_exact_entry(plan.publication.entry())?;
        if let MembershipMutationProgress::RevokeActivated { candidate } = &progress {
            let (expected, publication) = match &plan.publication {
                RevokeMembershipPublication::Direct { publication } => (None, publication),
                RevokeMembershipPublication::StoreActivated {
                    candidate,
                    publication,
                    ..
                } => (Some(&candidate.reference), publication),
            };
            if candidate.as_ref() != expected {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "membership activation names another candidate".to_string(),
                ));
            }
            validated_chain.activate_head_ref(publication.head_ref.clone())?;
            *chain = validated_chain;
            return EncryptionService::from_keyring_payload(plan.keyring_payload)
                .map_err(MembershipMutationError::Encryption);
        }
        if let MembershipMutationProgress::RevokeCandidateNonactivating { nonactivation } =
            &progress
        {
            let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication
            else {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "direct removal carries Store-candidate nonactivation".to_string(),
                ));
            };
            nonactivation
                .validate()
                .map_err(MembershipMutationError::from)?;
            if nonactivation
                .reference()
                .map_err(MembershipMutationError::from)?
                != candidate.reference
            {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "membership nonactivation names another candidate".to_string(),
                ));
            }
            persistence.finish_nonactivating_revoke(&plan).await?;
            let generation = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                .map_err(MembershipMutationError::Encryption)?
                .current_generation();
            pending_rotation
                .remove_candidate(generation, persistence.intent_hash())
                .map_err(MembershipMutationError::RotationState)?;
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership removal candidate did not activate".to_string(),
            ));
        }
        let publication = plan.publication.publication().clone();
        let author = operation
            .verify_membership_publication_author(&publication)
            .await?;
        let keyring = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
            .map_err(MembershipMutationError::Encryption)?;
        let remaining = validated_chain.current_members();
        if remaining.len() != plan.wraps.len() {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "planned replacement wraps do not cover every remaining member exactly once"
                    .to_string(),
            ));
        }
        let mut planned_recipients = std::collections::BTreeSet::new();
        for wrapped in &plan.wraps {
            let reference = &wrapped.prepared.reference;
            if !planned_recipients.insert(reference.recipient_pubkey.clone())
                || !remaining
                    .iter()
                    .any(|(member_pubkey, _)| member_pubkey == &reference.recipient_pubkey)
            {
                return Err(MembershipMutationError::InvalidDurableMutation(format!(
                    "planned replacement wrap has duplicate or non-member recipient {}",
                    reference.recipient_pubkey
                )));
            }
            let envelope = wrapped.prepared.validate()?;
            if envelope.generation != keyring.current_generation()
                || envelope.author_pubkey != publication.entry.author_pubkey
                || envelope
                    .verify_and_unwrap(
                        &publication.entry.store_id,
                        &reference.recipient_pubkey,
                        std::iter::once(publication.entry.author_pubkey.as_str()),
                    )
                    .is_err()
            {
                return Err(MembershipMutationError::InvalidDurableMutation(format!(
                "planned replacement wrap for {} is not bound to the exact removal, generation, recipient, and author",
                reference.recipient_pubkey
            )));
            }
        }
        let authority_refs = match &publication.entry.change {
            MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
            _ => {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "planned removal publication is not a removal".to_string(),
                ))
            }
        };
        let planned_refs = plan
            .wraps
            .iter()
            .map(|wrap| wrap.prepared.reference.clone())
            .collect::<Vec<_>>();
        if authority_refs != &planned_refs {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "planned removal authority differs from its exact wrapped keys".to_string(),
            ));
        }
        let remote_objects = plan.candidate_remote_objects()?;
        for wrapped in &plan.wraps {
            if let Some(remotes) = &remote_objects {
                let expected = exact_owned_remote(remotes, &wrapped.prepared.reference.object)?;
                persistence
                    .mark_remote_object_uploaded(expected.into_record())
                    .await?;
            }
        }
        let prepared_wraps = plan
            .wraps
            .iter()
            .map(|wrap| wrap.prepared.clone())
            .collect::<Vec<_>>();
        match &plan.publication {
            RevokeMembershipPublication::Direct { .. } => {
                operation
                    .publish_direct_membership_authority(&plan.wraps, &publication)
                    .await?;
            }
            RevokeMembershipPublication::StoreActivated { transition, .. } => {
                operation
                    .publish_membership_authority(transition, &prepared_wraps)
                    .await?;
            }
        }
        if let Some(remotes) = &remote_objects {
            let expected = exact_owned_remote(remotes, &publication.entry_ref.object)?;
            persistence
                .mark_remote_object_uploaded(expected.into_record())
                .await?;
        }
        match operation
            .set_membership_access(plan.desired_access.clone())
            .await?
        {
            CloudAccessOutcome::Absent(_) => {}
            CloudAccessOutcome::Present(_) => {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "provider returned present outcome for absent access request".to_string(),
                ))
            }
        }
        if matches!(progress, MembershipMutationProgress::Pending) {
            progress = MembershipMutationProgress::RevokeAccessRemoved;
            persistence.record_progress(&progress).await?;
        }
        match plan.publication.clone() {
            RevokeMembershipPublication::Direct { publication } => {
                operation
                    .publish_direct_membership_head(&publication, &author)
                    .await?;
                validated_chain.activate_head_ref(publication.head_ref.clone())?;
                persistence
                    .record_direct_revoke_activation(keyring.current_generation())
                    .await?;
                pending_rotation
                    .mark_committed_mutation(
                        keyring.current_generation(),
                        persistence.intent_hash(),
                    )
                    .map_err(MembershipMutationError::RotationState)?;
                *chain = validated_chain;
                Ok(keyring)
            }
            RevokeMembershipPublication::StoreActivated {
                transition,
                mut candidate,
                publication,
            } => {
                // Every publication attempt re-derives this from the candidate it
                // is about to publish, and a reprepare changes the candidate.
                let candidate_remotes =
                    |candidate: &coven_protocol::prepared_commit::PreparedStoreOperationCommit| {
                        candidate
                            .merge_membership_activation_remote_objects(
                                &transition,
                                &publication,
                                &prepared_wraps,
                            )
                            .map_err(MembershipMutationError::from)
                    };
                let initial_remotes = candidate_remotes(&candidate)?;
                operation
                    .upload_commit(&candidate)
                    .await
                    .map_err(MembershipMutationError::from)?;
                persistence
                    .mark_remote_object_uploaded(
                        exact_owned_remote(&initial_remotes, &candidate.reference.object)?
                            .into_record(),
                    )
                    .await?;
                loop {
                    let previous_candidate = candidate.as_ref().clone();
                    let current_remotes = candidate_remotes(&candidate)?;
                    let outcome = operation
                    .publish_membership_activation(
                        &transition,
                        &publication,
                        candidate.clone(),
                        coven_protocol::membership_mutation::StoreMembershipJournalCompletion::RotationMutation {
                        intent_hash: persistence.intent_hash(),
                        progress_bytes: MembershipMutationProgress::RevokeActivated {
                            candidate: Some(candidate.reference.clone()),
                        }
                        .encode()?,
                        generation: keyring.current_generation(),
                        remote_objects: current_remotes
                            .iter()
                            .map(|remote| remote.record().clone())
                            .collect(),
                    },
                    )
                    .await?;
                    match outcome {
                    crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::Activated(reference) => {
                        if reference != candidate.reference {
                            return Err(MembershipMutationError::InvalidDurableMutation(
                                "membership removal activated another Store candidate".to_string(),
                            ));
                        }
                        validated_chain.activate_head_ref(publication.head_ref.clone())?;
                        pending_rotation
                            .mark_committed_mutation(
                                keyring.current_generation(),
                                persistence.intent_hash(),
                            )
                            .map_err(MembershipMutationError::RotationState)?;
                        *chain = validated_chain;
                        return Ok(keyring);
                    }
                    crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::RepreparedCandidate(
                        replacement,
                    ) => {
                        if replacement.reference != candidate.reference {
                            return Err(MembershipMutationError::InvalidDurableMutation(
                                "membership removal reprepare changed its signed candidate"
                                    .to_string(),
                            ));
                        }
                        let previous_remotes = candidate_remotes(&previous_candidate)?;
                        candidate = replacement;
                        let replacement_remotes = candidate_remotes(&candidate)?;
                        let previous_head = previous_candidate.head_ref();
                        let replacement_head = candidate.head_ref();
                        plan.publication = RevokeMembershipPublication::StoreActivated {
                            transition: transition.clone(),
                            candidate: candidate.clone(),
                            publication: publication.clone(),
                        };
                        let plan_bytes = MembershipMutationPlan::Revoke(plan.clone()).encode()?;
                        let (previous_intent_hash, replacement_intent_hash) = persistence
                            .adopt_candidate_head(
                                plan_bytes,
                                exact_owned_remote(&previous_remotes, &previous_head.object)?
                                    .into_record(),
                                exact_owned_remote(&replacement_remotes, &replacement_head.object)?,
                                Some(keyring.current_generation()),
                            )
                            .await?;
                        pending_rotation
                            .replace_candidate_mutation(
                                keyring.current_generation(),
                                previous_intent_hash,
                                replacement_intent_hash,
                            )
                            .map_err(MembershipMutationError::RotationState)?;
                    }
                    crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::NonactivatedCandidate {
                        candidate: returned,
                        nonactivation,
                    } => {
                        if *returned != *candidate {
                            return Err(MembershipMutationError::InvalidDurableMutation(
                                "membership removal nonactivation returned another candidate"
                                    .to_string(),
                            ));
                        }
                        let verified = *nonactivation;
                        persistence
                            .begin_nonactivating_revoke(&plan, verified)
                            .await?;
                        pending_rotation
                            .remove_candidate(
                                keyring.current_generation(),
                                persistence.intent_hash(),
                            )
                            .map_err(MembershipMutationError::RotationState)?;
                        return Err(MembershipMutationError::InvalidDurableMutation(
                            "membership removal candidate did not activate".to_string(),
                        ));
                    }
                    crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::Nonactivated(
                        reference,
                    ) => {
                        return Err(MembershipMutationError::InvalidDurableMutation(format!(
                            "membership removal candidate {} lost without exact evidence",
                            reference.commit_hash
                        )))
                    }
                    crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::Reprepared => {
                        return Err(MembershipMutationError::InvalidDurableMutation(
                            "membership removal returned acknowledgement-only reprepare state"
                                .to_string(),
                        ))
                    }
                }
                }
            }
        }
    }
}
