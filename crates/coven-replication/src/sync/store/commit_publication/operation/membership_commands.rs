use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invite_member(
        &mut self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        encryption: &coven_keys::encryption::EncryptionService,
        store_id: &str,
        store_name: &str,
    ) -> Result<
        coven_storage::join_code::InviteCode,
        crate::sync::store::membership::MembershipOpsError,
    > {
        if role == coven_protocol::membership::MemberRole::Owner {
            return Err(crate::sync::store::membership::MembershipOpsError::Invite(
                crate::sync::store::membership::InviteError::Membership(
                    coven_protocol::membership::MembershipError::OwnerPromotionRequired,
                ),
            ));
        }
        if public_key_hex == self.writer.author_pubkey() {
            return Err(crate::sync::store::membership::MembershipOpsError::SelfInvite);
        }
        self.resolved_membership()?;
        let root = self.store_root().clone();
        let protocol_store_id = root.store_root_id.to_string();
        let invite_timestamp = self.database.stamp();
        let (join_info, wrapped_key, validated_chain) = async {
            let storage = self.storage.clone();
            let database = self.database.clone();
            let chain = self.membership.clone();
            let _mutation = database.membership_mutation_permit().await;
            let (plan, mut progress, intent_hash) =
                match database.outbound_membership_mutation().await? {
                Some(row) => {
                    let intent_hash = row.intent_hash;
                    let (pending, progress) = decode_membership_mutation(row)?;
                    let MembershipMutationPlan::Invite(plan) = pending else {
                        return Err(crate::sync::store::membership::InviteError::PendingMutation(
                            "a member removal is pending".to_string(),
                        ));
                    };
                    if !plan.matches_request(
                        &self.writer_pubkey(),
                        public_key_hex,
                        invitee_email,
                        &role,
                        &protocol_store_id,
                    ) {
                        return Err(crate::sync::store::membership::InviteError::PendingMutation(
                            "the pending invitation has different immutable inputs".to_string(),
                        ));
                    }
                    (plan, progress, intent_hash)
                }
                None => {
                    let stream_id = self.select_membership_author_stream(&chain).await?;
                    let invitee_x25519_pk =
                        coven_keys::keys::ed25519_hex_to_x25519_public_key(public_key_hex)?;
                    let authorized_keyring = self
                        .open_keyring_or_for_membership(&chain, encryption)
                        .await?;
                    let signed = self
                        .writer
                        .seal_keyring_for_member(
                            protocol_store_id.clone(),
                            public_key_hex.to_string(),
                            invitee_x25519_pk,
                            authorized_keyring,
                        )
                        .await?;
                    let wrapped_key = self.prepare_wrapped_key(public_key_hex, signed).await?;
                    let entry = self.writer.sign_set_member(
                        &chain,
                        stream_id,
                        public_key_hex.to_string(),
                        invitee_email.map(str::to_string),
                        role.clone(),
                        wrapped_key.reference.clone(),
                        invite_timestamp.clone(),
                    )?;
                    let publication = self
                        .prepare_membership_publication(&chain, entry)
                        .await?;
                    let plan = InviteMutationPlan {
                        publication,
                        invitee_pubkey: public_key_hex.to_string(),
                        invitee_email: invitee_email.map(str::to_string),
                        role,
                        desired_access: coven_storage::cloud::CloudAccessState::Present {
                            member_pubkey: public_key_hex.to_string(),
                            provider_account_email: invitee_email.map(str::to_string),
                        },
                        wrapped_key,
                    };
                    let encoded = MembershipMutationPlan::Invite(plan.clone()).encode()?;
                    let progress = MembershipMutationProgress::Pending;
                    let intent_hash = database
                        .stage_membership_mutation(
                            encoded,
                            progress.encode()?,
                            None,
                        )
                        .await?;
                    (plan, progress, intent_hash)
                }
                };
        plan.publication.validate()?;
        let mut validated_chain = chain.with_exact_entry(&plan.publication.entry)?;
        let author = self
            .verify_membership_publication_author(&plan.publication)
            .await?;
        let wrapped = plan.wrapped_key.validate()?;
        let authority_matches = matches!(
            &plan.publication.entry.change,
            coven_protocol::membership::MembershipChange::SetMember { wrapped_key, .. }
                if wrapped_key == &plan.wrapped_key.reference
        );
        if !authority_matches
            || wrapped.author_pubkey != plan.publication.entry.author_pubkey
            || wrapped
                .verify_and_unwrap(
                    &plan.publication.entry.store_id,
                    &plan.invitee_pubkey,
                    std::iter::once(plan.publication.entry.author_pubkey.as_str()),
                )
                .is_err()
        {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "planned invitation wrap is not bound to its exact entry, recipient, and author"
                        .to_string(),
                ),
            );
        }
        let outcome = storage
            .set_member_access(plan.desired_access.clone())
            .await?;
        let coven_storage::cloud::CloudAccessOutcome::Present(observed_join_info) = outcome else {
            return Err(
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "provider returned absent outcome for present access request".to_string(),
                ),
            );
        };
        let persistence = self.membership_mutation_persistence(intent_hash);
        let join_info = match progress {
            MembershipMutationProgress::Pending => {
                progress = MembershipMutationProgress::InviteGranted {
                    join_info: observed_join_info.clone(),
                };
                persistence.record_progress(&progress).await?;
                observed_join_info
            }
            MembershipMutationProgress::InviteGranted { join_info } => {
                if join_info != observed_join_info {
                    return Err(
                        crate::sync::store::membership::InviteError::InvalidDurableMutation(
                            "provider returned different join information while verifying persisted access"
                                .to_string(),
                        ),
                    );
                }
                join_info
            }
            MembershipMutationProgress::RevokeAccessRemoved
            | MembershipMutationProgress::RevokeCandidateNonactivating { .. }
            | MembershipMutationProgress::ResolutionCandidateNonactivating { .. }
            | MembershipMutationProgress::RevokeActivated { .. }
            | MembershipMutationProgress::ResolutionActivated { .. } => {
                return Err(
                    crate::sync::store::membership::InviteError::InvalidDurableMutation(
                        "invitation carries member-removal progress".to_string(),
                    ),
                );
            }
        };
        storage
            .as_ref()
            .create_protocol_object(&plan.wrapped_key.object)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        storage
            .as_ref()
            .create_protocol_object(&plan.publication.prepared_entry()?)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        self.membership_objects()
            .load_entry(&plan.publication.entry_ref)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        storage
            .as_ref()
            .create_protocol_object(&plan.publication.prepared_head()?)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        self.membership_objects()
            .load_head_for_registration(&plan.publication.head_ref, &author)
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(error.to_string())
            })?;
        validated_chain.activate_head_ref(plan.publication.head_ref.clone())?;
        persistence.complete().await?;
        let wrapped_key = plan.wrapped_key.reference;
        Ok::<_, crate::sync::store::membership::InviteError>((
            join_info,
            wrapped_key,
            validated_chain,
        ))
        }
        .await?;
        self.membership = validated_chain;
        let owner_pubkey = self
            .membership
            .founder_pubkey()
            .ok_or(crate::sync::store::membership::MembershipOpsError::ChainHasNoFounder)?
            .to_string();
        if self.protocol_root().descriptor.store_root_id() != root.store_root_id
            || self.protocol_root().descriptor.founder_pubkey != owner_pubkey
        {
            return Err(crate::sync::store::membership::MembershipOpsError::Chain(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "Store protocol root differs from the invite authority".to_string(),
                ),
            ));
        }
        Ok(coven_storage::join_code::InviteCode {
            v: coven_storage::join_code::INVITE_CODE_VERSION,
            store_id: store_id.to_string(),
            store_name: store_name.to_string(),
            join_info,
            owner_pubkey,
            wrapped_key,
            store_root: root,
            membership_floor: coven_protocol::membership::MembershipFloor(
                self.membership.head_refs().to_vec(),
            ),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn remove_member(
        &mut self,
        public_key_hex: &str,
        current_encryption: &coven_keys::encryption::EncryptionService,
        master_keys: &dyn coven_keys::keys::MasterKeyCustody,
        cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
    ) -> Result<String, crate::sync::store::membership::MembershipOpsError> {
        let timestamp = self.database.stamp();
        let new_key = self
            .revoke_member_without_local_adoption(
                public_key_hex,
                &timestamp,
                current_encryption,
                pending_rotation,
            )
            .await?;
        let generation = new_key.current_generation();
        let fingerprint = cipher
            .adopt_key_rotation(&new_key, master_keys)
        .map_err(|source| {
            crate::sync::store::membership::MembershipOpsError::RotationCommittedAdoptionFailed {
                source,
            }
        })?;
        self.complete_revoke_rotation_adoption(pending_rotation, generation)
            .await?;
        Ok(fingerprint)
    }

    pub(super) async fn revoke_member_without_local_adoption(
        &mut self,
        public_key_hex: &str,
        timestamp: &str,
        current_encryption: &coven_keys::encryption::EncryptionService,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
    ) -> Result<
        coven_keys::encryption::EncryptionService,
        crate::sync::store::membership::MembershipOpsError,
    > {
        let mut membership = self.resolved_membership()?.clone();
        let store_id = self.store_root().store_root_id.to_string();
        let new_key = membership_mutation::AuthorizedMembershipRevocation::begin(
            self,
            &mut membership,
            public_key_hex,
            &store_id,
            timestamp,
            current_encryption,
            pending_rotation,
        )
        .await
        .execute()
        .await?;
        self.membership = membership;
        Ok(new_key)
    }

    pub(super) async fn complete_revoke_rotation_adoption(
        &self,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
        adopted_generation: u64,
    ) -> Result<(), crate::sync::store::membership::InviteError> {
        let _mutation = self.database.membership_mutation_permit().await;
        let row = self
            .database
            .outbound_membership_mutation()
            .await?
            .ok_or_else(|| {
                crate::sync::store::membership::InviteError::InvalidDurableMutation(
                    "activated removal journal is absent during key adoption".to_string(),
                )
            })?;
        let intent_hash =
            membership_mutation::validate_revoke_rotation_adoption(row, adopted_generation)?;
        let gate = self
            .database
            .complete_local_rotation_adoption(intent_hash, adopted_generation)
            .await?;
        pending_rotation.install_durable_gate(gate);
        Ok(())
    }

    pub(super) async fn build_resolution_mutation(
        &mut self,
        chain: &MembershipChain,
        conflict_hash: store_commit::ObjectHash,
        selection: membership::MembershipConflictSelection,
        created_at: &str,
    ) -> Result<ResolveMutationPlan, InviteError> {
        let base = self
            .prepare_conflict_resolution_plan(chain.head_refs())
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let chain = base.membership().clone();
        let membership::MembershipStatus::Conflict(conflict) = chain.status() else {
            return Err(MembershipError::Conflict.into());
        };
        let current = match conflict {
            membership::MembershipConflict::ConcurrentMemberAssignments {
                conflict_hash, ..
            }
            | membership::MembershipConflict::RevocationCycle { conflict_hash, .. } => {
                *conflict_hash
            }
        };
        if current != conflict_hash {
            return Err(MembershipError::InvalidConflictResolution.into());
        }
        let resolver_pubkey = self.writer.author_pubkey();
        let replacement_grant =
            membership::derive_store_resolution_grant(&conflict_hash, &resolver_pubkey);
        let stream_id = base.grant_authorized_stream_id(
            &replacement_grant,
            store_commit::StreamAnchorDomain::StoreMembership,
        );
        let membership_context = ProtocolObjectContext::signed_plaintext(
            base.root().store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let membership_slot = self
            .storage
            .as_ref()
            .allocate_protocol_slot(
                &membership_context,
                &membership_head_slot_prefix(&resolver_pubkey, &replacement_grant, stream_id, 1),
                ".json",
            )
            .await?;
        let recovery_context = ProtocolObjectContext::signed_plaintext(
            base.root().store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let recovery_slot = self
            .storage
            .as_ref()
            .allocate_protocol_slot(
                &recovery_context,
                &store_commit::owner_recovery_semantic_prefix(
                    &resolver_pubkey,
                    replacement_grant.clone(),
                    1,
                ),
                ".json",
            )
            .await?;
        let membership = store_commit::GrantStreamAnchor::StoreMembership {
            first_slot: membership_slot,
        };
        let recovery = store_commit::GrantStreamAnchor::OwnerRecovery {
            first_slot: recovery_slot,
        };
        let resolution = base.sign_conflict_resolution(
            &chain,
            selection,
            replacement_grant,
            membership,
            recovery,
        )?;
        let resolution_bytes = serde_json::to_vec(&resolution).map_err(|error| {
            InviteError::InvalidDurableMutation(format!("serialize membership resolution: {error}"))
        })?;
        let resolution_context = ProtocolObjectContext::signed_plaintext(
            base.root().store_root_hash,
            ProtocolObjectDomain::StoreMembershipResolution,
        );
        let resolution_hash = resolution.resolution_hash();
        let resolution_prefix = store_commit::membership_resolution_semantic_prefix(
            conflict_hash,
            &resolver_pubkey,
            resolution_hash,
        );
        let resolution_slot = self
            .storage
            .as_ref()
            .allocate_protocol_slot(&resolution_context, &resolution_prefix, ".json")
            .await?;
        let resolution_object = self.storage.as_ref().prepare_protocol_object(
            &resolution_context,
            resolution_slot,
            &resolution_prefix,
            resolution_bytes,
        )?;
        let reference = resolution.resolution_ref(resolution_object.reference().clone());
        let mut resolved_chain = chain.clone();
        resolved_chain.apply_resolutions(
            base.root().store_root_hash,
            &[(reference.clone(), resolution.clone())],
        )?;
        let entry = base.sign_conflict_resolution_activation(
            &resolved_chain,
            stream_id,
            reference.clone(),
            &resolution,
            created_at.to_string(),
        )?;
        let transition = self
            .prepare_membership_transition(&resolved_chain, entry)
            .await?;
        let operation_plan = base
            .finish(&resolved_chain, &reference)
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let mut stream_activations = vec![
            store_commit::StreamActivation::grant_authorized(
                resolution.store_root_hash,
                resolution.replacement_acceptance.owner_registration.clone(),
                resolution.replacement_grant.clone(),
                resolution.replacement_acceptance.membership.clone(),
            ),
            store_commit::StreamActivation::grant_authorized(
                resolution.store_root_hash,
                resolution.replacement_acceptance.owner_registration.clone(),
                resolution.replacement_grant.clone(),
                resolution.replacement_acceptance.recovery.clone(),
            ),
        ];
        stream_activations.sort();
        let mut candidate = self
            .prepare_candidate(
                operation_plan,
                commit_plan::StoreOperationBatch::MergeMembershipActivation {
                    transition: transition.transition.clone(),
                    stream_activations,
                },
            )
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let publication = self
            .finish_membership_transition(
                transition.clone(),
                membership::MembershipHeadActivation::StoreCommit {
                    commit: candidate.reference.clone(),
                },
            )
            .await?;
        self.attach_merge_membership_proof(&mut candidate, &publication, Some(&resolution))
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let plan = ResolveMutationPlan {
            resolution,
            reference,
            transition: Box::new(transition),
            candidate: Box::new(candidate),
            publication: Box::new(publication),
        };
        plan.validate_closed_shape()?;
        Ok(plan)
    }

    pub(crate) async fn resolve_membership_conflict(
        &mut self,
        choice: &coven_protocol::membership::MembershipConflictChoice,
        created_at: &str,
    ) -> Result<
        coven_protocol::membership::StoreMembershipConflictResolutionRef,
        crate::sync::store::membership::MembershipOpsError,
    > {
        let mut membership = self.membership.clone();
        let valid_choice = match (membership.status(), choice.selection()) {
            (
                coven_protocol::membership::MembershipStatus::Conflict(
                    coven_protocol::membership::MembershipConflict::ConcurrentMemberAssignments {
                        conflict_hash,
                        conflicting_grants,
                        ..
                    },
                ),
                coven_protocol::membership::MembershipConflictSelection::MemberAssignment { grant },
            ) => conflict_hash == &choice.conflict_hash() && conflicting_grants.contains_key(grant),
            (
                coven_protocol::membership::MembershipStatus::Conflict(
                    coven_protocol::membership::MembershipConflict::RevocationCycle {
                        conflict_hash,
                        maximal_valid_branches,
                        ..
                    },
                ),
                coven_protocol::membership::MembershipConflictSelection::RevocationBranch { heads },
            ) => {
                conflict_hash == &choice.conflict_hash()
                    && maximal_valid_branches
                        .iter()
                        .any(|branch| branch.heads == *heads)
            }
            _ => false,
        };
        if !valid_choice {
            return Err(crate::sync::store::membership::InviteError::Membership(
                coven_protocol::membership::MembershipError::InvalidConflictResolution,
            )
            .into());
        }
        let conflict_hash = choice.conflict_hash();
        let selection = choice.selection().clone();
        let database = self.database.clone();
        let signer_pubkey = self.writer.author_pubkey();
        let _mutation = database.membership_mutation_permit().await;
        let (mut plan, progress, intent_hash) = match database
            .outbound_membership_mutation()
            .await
            .map_err(InviteError::from)?
        {
            Some(row) => {
                let intent_hash = row.intent_hash;
                let (pending, progress) = decode_membership_mutation(row)?;
                let MembershipMutationPlan::Resolve(plan) = pending else {
                    return Err(InviteError::PendingMutation(
                        "another membership mutation is pending".to_string(),
                    )
                    .into());
                };
                if plan.resolution.conflict_hash != conflict_hash
                    || plan.resolution.resolver_pubkey != signer_pubkey
                    || plan.resolution.selection != selection
                {
                    return Err(InviteError::PendingMutation(
                        "the pending resolution has different immutable inputs".to_string(),
                    )
                    .into());
                }
                (plan, progress, intent_hash)
            }
            None => {
                let plan = self
                    .build_resolution_mutation(&membership, conflict_hash, selection, created_at)
                    .await?;
                let bytes = MembershipMutationPlan::Resolve(plan.clone()).encode()?;
                let progress = MembershipMutationProgress::Pending;
                let intent_hash = database
                    .stage_membership_candidate_mutation(
                        bytes,
                        progress.encode()?,
                        plan.remote_objects()?,
                        None,
                    )
                    .await
                    .map_err(InviteError::from)?;
                (plan, progress, intent_hash)
            }
        };
        let mut persistence = self.membership_mutation_persistence(intent_hash);
        plan.validate_closed_shape()?;
        if let MembershipMutationProgress::ResolutionCandidateNonactivating { nonactivation } =
            &progress
        {
            if nonactivation
                .reference()
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
                != plan.candidate.reference
            {
                return Err(InviteError::InvalidDurableMutation(
                    "resolution nonactivation names another candidate".to_string(),
                )
                .into());
            }
            persistence.finish_nonactivating_resolution(&plan).await?;
            return Err(InviteError::InvalidDurableMutation(
                "membership resolution candidate did not activate".to_string(),
            )
            .into());
        }
        if let MembershipMutationProgress::ResolutionActivated { candidate } = &progress {
            if candidate != &plan.candidate.reference {
                return Err(InviteError::InvalidDurableMutation(
                    "resolution activation names another candidate".to_string(),
                )
                .into());
            }
            membership
                .apply_resolutions(
                    plan.resolution.store_root_hash,
                    &[(plan.reference.clone(), plan.resolution.clone())],
                )
                .map_err(InviteError::from)?;
            membership
                .add_entry(plan.publication.entry.clone())
                .map_err(InviteError::from)?;
            membership
                .activate_head_ref(plan.publication.head_ref.clone())
                .map_err(InviteError::from)?;
            self.membership = membership;
            return Ok(plan.reference);
        }
        if !matches!(progress, MembershipMutationProgress::Pending) {
            return Err(InviteError::InvalidDurableMutation(
                "membership resolution carries another mutation's progress".to_string(),
            )
            .into());
        }
        membership
            .apply_resolutions(
                plan.resolution.store_root_hash,
                &[(plan.reference.clone(), plan.resolution.clone())],
            )
            .map_err(InviteError::from)?;
        membership
            .add_entry(plan.publication.entry.clone())
            .map_err(InviteError::from)?;
        let remotes = plan.remote_objects()?;
        self.storage
            .as_ref()
            .create_protocol_object(&plan.prepared_resolution()?)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        self.membership_objects()
            .load_resolution(&plan.reference)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        persistence
            .mark_remote_object_uploaded(
                exact_owned_remote(&remotes, &plan.reference.object)?.into_record(),
            )
            .await?;
        self.publish_membership_authority(&plan.transition, &[])
            .await?;
        persistence
            .mark_remote_object_uploaded(
                exact_owned_remote(&remotes, &plan.transition.entry_ref.object)?.into_record(),
            )
            .await?;
        self.upload_commit(&plan.candidate)
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        persistence
            .mark_remote_object_uploaded(
                exact_owned_remote(&remotes, &plan.candidate.reference.object)?.into_record(),
            )
            .await?;
        loop {
            let previous = plan.candidate.as_ref().clone();
            let current_remotes = plan.remote_objects()?;
            let outcome = self
                .publish_membership_activation(
                    &plan.transition,
                    &plan.publication,
                    plan.candidate.clone(),
                    coven_protocol::membership_mutation::StoreMembershipJournalCompletion::Mutation {
                        intent_hash: persistence.intent_hash(),
                        progress_bytes: MembershipMutationProgress::ResolutionActivated {
                            candidate: plan.candidate.reference.clone(),
                        }
                        .encode()?,
                        remote_objects: current_remotes
                            .iter()
                            .map(|remote| remote.record().clone())
                            .collect(),
                    },
                )
                .await?;
            match outcome {
                commit_plan::StoreOperationPublicationOutcome::Activated(reference)
                    if reference == plan.candidate.reference =>
                {
                    membership
                        .activate_head_ref(plan.publication.head_ref.clone())
                        .map_err(InviteError::from)?;
                    self.membership = membership;
                    return Ok(plan.reference);
                }
                commit_plan::StoreOperationPublicationOutcome::RepreparedCandidate(replacement)
                    if replacement.reference == plan.candidate.reference =>
                {
                    let previous_remotes = plan.remote_objects()?;
                    let previous_head = previous.head_ref();
                    plan.candidate = replacement;
                    let replacement_remotes = plan.remote_objects()?;
                    let replacement_head = plan.candidate.head_ref();
                    let bytes = MembershipMutationPlan::Resolve(plan.clone()).encode()?;
                    persistence
                        .adopt_candidate_head(
                            bytes,
                            exact_owned_remote(&previous_remotes, &previous_head.object)?
                                .into_record(),
                            exact_owned_remote(&replacement_remotes, &replacement_head.object)?,
                            None,
                        )
                        .await?;
                }
                commit_plan::StoreOperationPublicationOutcome::NonactivatedCandidate {
                    candidate,
                    nonactivation,
                } if candidate.as_ref() == plan.candidate.as_ref() => {
                    persistence
                        .begin_nonactivating_resolution(&plan, *nonactivation)
                        .await?;
                    return Err(InviteError::InvalidDurableMutation(
                        "membership resolution candidate did not activate".to_string(),
                    )
                    .into());
                }
                _ => {
                    return Err(InviteError::InvalidDurableMutation(
                        "membership resolution returned an inapplicable publication outcome"
                            .to_string(),
                    )
                    .into())
                }
            }
        }
    }
}
