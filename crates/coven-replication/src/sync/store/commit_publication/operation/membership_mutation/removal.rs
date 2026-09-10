use super::{
    decode_membership_mutation, exact_owned_remote, MembershipMutationError,
    MembershipMutationPlan, MembershipMutationProgress, MembershipRevocation,
    PreparedMembershipActivation, ReplacementWrappedKey, RevokeMutationPlan,
};
use coven_keys::encryption::{self, EncryptionService};
use coven_keys::keys;
use coven_protocol::membership::{MemberRole, StoreAuthorityChange};
use coven_storage as cloud_storage;
use coven_storage::cloud::{CloudAccessOutcome, CloudAccessState, RevokeOutcome};

pub(crate) struct AuthorizedMembershipRevocation<'operation, 'storage, 'input> {
    operation:
        &'operation mut crate::sync::store::commit_publication::AuthorizedWriterOperation<'storage>,
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
/// The operation keeps its author turn through durable candidate staging.
/// Once staged, the active publication reservation protects that exact candidate
/// while provider access and key rotation proceed and publication takes its turn.
impl<'operation, 'storage, 'input> AuthorizedMembershipRevocation<'operation, 'storage, 'input> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn begin(
        operation: &'operation mut crate::sync::store::commit_publication::AuthorizedWriterOperation<'storage>,
        revokee_pubkey: &'input str,
        store_id: &'input str,
        timestamp: &'input str,
        current_encryption: &'input EncryptionService,
        pending_rotation: &'input dyn cloud_storage::CloudSyncRotationStateAccess,
    ) -> Self {
        let permit = operation.membership_mutation_permit().await;
        Self {
            operation,
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
        plan: &crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<RevokeMutationPlan, MembershipMutationError> {
        let chain = plan.membership().clone();
        let stream_id = self
            .operation
            .select_membership_author_stream(&chain)
            .await?;
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
            .keyrings
            .open_or(&chain, current_encryption)
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
        let entry = if chain.is_owner_now(revokee_pubkey) {
            self.operation.sign_owner_barrier_removal(
                &chain,
                stream_id,
                revokee_pubkey.to_string(),
                wrapped_keys,
                plan.device_state().clone(),
                timestamp.to_string(),
            )?
        } else {
            self.operation.sign_member_removal(
                &chain,
                stream_id,
                revokee_pubkey.to_string(),
                wrapped_keys,
                timestamp.to_string(),
            )?
        };
        let transition = self
            .operation
            .prepare_membership_transition(&chain, entry)
            .await?;
        let mut candidate = self.operation.prepare_candidate_for_write(
            plan,
            crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::MergeMembershipActivation {
                transition: transition.transition.clone(), stream_activations: Vec::new(),
            },
            write_id,
        ).await.map_err(MembershipMutationError::from)?;
        let publication = self
            .operation
            .finish_store_membership_transition(transition.clone(), candidate.reference.clone())
            .await?;
        self.operation
            .attach_membership_proof(&mut candidate, &publication)?;
        let publication = PreparedMembershipActivation {
            transition: Box::new(transition),
            candidate: Box::new(candidate),
            publication: Box::new(publication),
        };
        let provider_account_email = chain
            .current_member_provider_email(revokee_pubkey)
            .map(str::to_string);
        Ok(RevokeMutationPlan {
            publication,
            revokee_pubkey: revokee_pubkey.to_string(),
            desired_access: CloudAccessState::Absent {
                member_pubkey: revokee_pubkey.to_string(),
                provider_account_email,
            },
            wraps,
            keyring_payload: new_keyring
                .to_keyring_payload()
                .map_err(MembershipMutationError::Encryption)?,
        })
    }

    pub(crate) async fn execute(mut self) -> Result<MembershipRevocation, MembershipMutationError> {
        let (mut plan, mut progress, mut intent_hash) = match self
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
                self.operation
                    .refresh_membership_publication()
                    .await
                    .map_err(MembershipMutationError::from)?;
                self.operation.membership.ensure_resolved()?;
                let is_current = self
                    .operation
                    .membership
                    .current_members()
                    .iter()
                    .any(|(pubkey, _)| pubkey == self.revokee_pubkey);
                let was_removed = self.operation.membership.entries().iter().any(|entry| {
                    matches!(
                        &entry.change,
                        StoreAuthorityChange::RemoveMember { user_pubkey, .. }
                            if user_pubkey == self.revokee_pubkey
                    )
                });
                if !is_current && was_removed {
                    if !self
                        .operation
                        .membership
                        .current_members()
                        .into_iter()
                        .any(|(_, role)| role == MemberRole::Owner)
                    {
                        return Err(MembershipMutationError::LastOwner);
                    }
                    let keyring = self
                        .operation
                        .keyrings
                        .open(&self.operation.membership)
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
                    return Ok(MembershipRevocation::AlreadyRemoved(keyring));
                }
                let operation_plan = self
                    .operation
                    .prepare_plan()
                    .await
                    .map_err(MembershipMutationError::from)?;
                let write_id = self.operation.database.new_store_write_id();
                let plan = Box::pin(self.build_revoke_mutation(&operation_plan, write_id)).await?;
                plan.validate_closed_shape()?;
                let encoded = MembershipMutationPlan::Revoke(plan.clone()).encode()?;
                let progress = MembershipMutationProgress::Pending;
                let progress_bytes = progress.encode()?;
                let intent_hash = self
                    .operation
                    .stage_membership_mutation(
                        encoded,
                        progress_bytes,
                        plan.candidate_remote_objects()?,
                        (*plan.publication.candidate).clone(),
                    )
                    .await?;
                (plan, progress, intent_hash)
            }
        };
        loop {
            let active = self.operation.database.active_store_publication().await?;
            let continuing = active.as_ref().is_some_and(|active| {
                active.owner() == &coven_database::ActiveStorePublicationOwner::MembershipMutation
                    && (active.is_awaiting_preparation()
                        || active.membership_abandonment().is_some())
            });
            if !continuing {
                match self
                    .execute_plan(plan.clone(), progress.clone(), intent_hash)
                    .await
                {
                    Ok(keyring) => return Ok(MembershipRevocation::Activated(keyring)),
                    Err(error)
                        if super::publication_predecessor_changed(
                            &error,
                            &plan.publication.entry().coord(),
                        ) => {}
                    Err(error) => return Err(error),
                }
            }
            let active = self
                .operation
                .abandon_membership_candidate(
                    intent_hash,
                    &plan.publication.candidate,
                    &plan.publication.publication,
                )
                .await?;
            let operation_plan = self.operation.prepare_plan().await?;
            if !operation_plan
                .membership()
                .current_members()
                .iter()
                .any(|(pubkey, _)| pubkey == self.revokee_pubkey)
            {
                let keyring = self
                    .operation
                    .keyrings
                    .open(operation_plan.membership())
                    .await?;
                match self
                    .operation
                    .set_membership_access(plan.desired_access.clone())
                    .await?
                {
                    CloudAccessOutcome::Absent(_) => {}
                    CloudAccessOutcome::Present(_) => {
                        return Err(MembershipMutationError::InvalidDurableMutation(
                            "provider returned present for an accepted member removal".into(),
                        ))
                    }
                }
                self.operation
                    .database
                    .complete_satisfied_membership_mutation(
                        intent_hash,
                        active,
                        operation_plan.publication_previous().record().clone(),
                        operation_plan.membership().clone(),
                    )
                    .await?;
                self.pending_rotation
                    .install_durable_gate(self.operation.database.load_rotation_gate().await?);
                return Ok(MembershipRevocation::AlreadyRemoved(keyring));
            }
            let replacement = Box::pin(self.build_revoke_mutation(
                &operation_plan,
                plan.publication.candidate.commit.write_id.clone(),
            ))
            .await?;
            replacement.validate_closed_shape()?;
            intent_hash = self
                .operation
                .database
                .replace_membership_candidate_mutation(
                    intent_hash,
                    active,
                    (*replacement.publication.candidate).clone(),
                    MembershipMutationPlan::Revoke(replacement.clone()).encode()?,
                    MembershipMutationProgress::Pending.encode()?,
                    replacement.candidate_remote_objects()?,
                )
                .await?;
            self.pending_rotation
                .install_durable_gate(self.operation.database.load_rotation_gate().await?);
            drop(operation_plan);
            plan = replacement;
            let row = self
                .operation
                .outbound_membership_mutation()
                .await?
                .ok_or_else(|| {
                    MembershipMutationError::InvalidDurableMutation(
                        "reprepared membership mutation lost its durable journal".into(),
                    )
                })?;
            if row.intent_hash != intent_hash {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "reprepared membership mutation changed its durable owner".into(),
                ));
            }
            let (_, retained_progress) = decode_membership_mutation(row)?;
            progress = retained_progress;
        }
    }

    async fn execute_plan(
        &mut self,
        plan: RevokeMutationPlan,
        mut progress: MembershipMutationProgress,
        intent_hash: coven_protocol::store_commit::ObjectHash,
    ) -> Result<EncryptionService, MembershipMutationError> {
        let operation = &mut *self.operation;
        let pending_rotation = self.pending_rotation;

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
        let persistence = operation.membership_mutation_persistence(intent_hash);
        plan.validate_closed_shape()?;
        if matches!(
            progress,
            MembershipMutationProgress::AdmissionGranted { .. }
                | MembershipMutationProgress::AdmissionActivated { .. }
        ) {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "removal carries admission progress".to_string(),
            ));
        }
        let mut validated_chain = operation
            .membership
            .with_exact_entry(plan.publication.entry())?;
        if let MembershipMutationProgress::RevokeActivated { candidate } = &progress {
            let publication = &plan.publication.publication;
            if candidate != &plan.publication.candidate.reference {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "membership activation names another candidate".to_string(),
                ));
            }
            validated_chain.activate_head_ref(publication.head_ref.clone())?;
            operation.membership = validated_chain;
            return EncryptionService::from_keyring_payload(plan.keyring_payload)
                .map_err(MembershipMutationError::Encryption);
        }
        let publication = plan.publication.publication.clone();
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
            StoreAuthorityChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
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
        let prepared_wraps = plan
            .wraps
            .iter()
            .map(|wrap| wrap.prepared.clone())
            .collect::<Vec<_>>();
        operation
            .publish_membership_authority(&plan.publication.transition, &prepared_wraps)
            .await?;
        for wrapped in &plan.wraps {
            persistence
                .mark_remote_object_uploaded(
                    exact_owned_remote(&remote_objects, &wrapped.prepared.reference.object)?
                        .into_record(),
                )
                .await?;
        }
        persistence
            .mark_remote_object_uploaded(
                exact_owned_remote(&remote_objects, &publication.entry_ref.object)?.into_record(),
            )
            .await?;
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
        let PreparedMembershipActivation {
            transition,
            candidate,
            publication,
        } = plan.publication;
        let reference = operation
            .publish_membership_activation(
                &transition,
                &publication,
                candidate.clone(),
                coven_protocol::membership_mutation::StoreMembershipJournalCompletion::Mutation {
                    intent_hash: persistence.intent_hash(),
                    progress_bytes: MembershipMutationProgress::RevokeActivated {
                        candidate: candidate.reference.clone(),
                    }
                    .encode()?,
                    remote_objects: remote_objects
                        .into_iter()
                        .map(|remote| remote.into_record())
                        .collect(),
                },
            )
            .await?;
        if reference != candidate.reference {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership removal accepted another Store candidate".to_string(),
            ));
        }
        pending_rotation
            .mark_committed_mutation(keyring.current_generation(), persistence.intent_hash())
            .map_err(MembershipMutationError::RotationState)?;
        Ok(keyring)
    }
}
