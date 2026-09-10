use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
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
        let outcome = self
            .revoke_member_without_local_adoption(
                public_key_hex,
                &timestamp,
                current_encryption,
                pending_rotation,
            )
            .await?;
        let new_key = match &outcome {
            membership_mutation::MembershipRevocation::Activated(keyring)
            | membership_mutation::MembershipRevocation::AlreadyRemoved(keyring) => keyring,
        };
        let generation = new_key.current_generation();
        let adopted = cipher
            .adopt_key_rotation(new_key, master_keys)
        .map_err(|source| {
            crate::sync::store::membership::MembershipOpsError::RotationCommittedAdoptionFailed {
                source,
            }
        })?;
        match outcome {
            membership_mutation::MembershipRevocation::Activated(_) => {
                self.complete_revoke_rotation_adoption(pending_rotation, generation)
                    .await?;
            }
            membership_mutation::MembershipRevocation::AlreadyRemoved(_) => {
                let _mutation = self.database.membership_mutation_permit().await;
                if self
                    .database
                    .load_rotation_gate()
                    .await
                    .map_err(MembershipMutationError::from)?
                    .is_some()
                {
                    let gate = self
                        .database
                        .complete_peer_rotation_adoption(generation)
                        .await
                        .map_err(MembershipMutationError::from)?;
                    pending_rotation.install_durable_gate(gate);
                }
            }
        }
        Ok(adopted.fingerprint().to_string())
    }

    pub(super) async fn revoke_member_without_local_adoption(
        &mut self,
        public_key_hex: &str,
        timestamp: &str,
        current_encryption: &coven_keys::encryption::EncryptionService,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
    ) -> Result<
        membership_mutation::MembershipRevocation,
        crate::sync::store::membership::MembershipOpsError,
    > {
        self.resolved_membership()?;
        let store_id = self.store_root().store_root_id.to_string();
        let new_key = membership_mutation::AuthorizedMembershipRevocation::begin(
            self,
            public_key_hex,
            &store_id,
            timestamp,
            current_encryption,
            pending_rotation,
        )
        .await
        .execute()
        .await?;
        Ok(new_key)
    }

    pub(super) async fn complete_revoke_rotation_adoption(
        &self,
        pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
        adopted_generation: u64,
    ) -> Result<(), crate::sync::store::membership::MembershipMutationError> {
        let _mutation = self.database.membership_mutation_permit().await;
        let row = self
            .database
            .outbound_membership_mutation()
            .await?
            .ok_or_else(|| {
                crate::sync::store::membership::MembershipMutationError::InvalidDurableMutation(
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

    async fn prepare_resolution_mutation(
        &mut self,
        chain: &MembershipChain,
        conflict_hash: store_commit::ObjectHash,
        selection: membership::MembershipConflictSelection,
        created_at: &str,
    ) -> Result<(ResolveMutationPlan, store_commit::ObjectHash), MembershipMutationError> {
        let base = self
            .prepare_conflict_resolution_plan(chain.head_refs())
            .await
            .map_err(MembershipMutationError::from)?;
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
        let resolution_bytes =
            serde_json::to_vec(&resolution).map_err(MembershipMutationError::Json)?;
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
            .map_err(MembershipMutationError::from)?;
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
                &operation_plan,
                commit_plan::StoreOperationBatch::MergeMembershipActivation {
                    transition: transition.transition.clone(),
                    stream_activations,
                },
            )
            .await
            .map_err(MembershipMutationError::from)?;
        let publication = self
            .finish_store_membership_transition(transition.clone(), candidate.reference.clone())
            .await?;
        self.attach_merge_membership_proof(&mut candidate, &publication, Some(&resolution))
            .map_err(MembershipMutationError::from)?;
        let plan = ResolveMutationPlan {
            resolution,
            reference,
            transition: Box::new(transition),
            candidate: Box::new(candidate),
            publication: Box::new(publication),
        };
        plan.validate_closed_shape()?;
        let bytes = MembershipMutationPlan::Resolve(plan.clone()).encode()?;
        let intent_hash = self
            .database
            .stage_membership_candidate_mutation(
                bytes,
                MembershipMutationProgress::Pending.encode()?,
                plan.remote_objects()?,
                (*plan.candidate).clone(),
            )
            .await
            .map_err(MembershipMutationError::from)?;
        Ok((plan, intent_hash))
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
            return Err(
                crate::sync::store::membership::MembershipMutationError::Membership(
                    coven_protocol::membership::MembershipError::InvalidConflictResolution,
                )
                .into(),
            );
        }
        let conflict_hash = choice.conflict_hash();
        let selection = choice.selection().clone();
        let database = self.database.clone();
        let signer_pubkey = self.writer.author_pubkey();
        let _mutation = database.membership_mutation_permit().await;
        let (plan, progress, intent_hash) = match database
            .outbound_membership_mutation()
            .await
            .map_err(MembershipMutationError::from)?
        {
            Some(row) => {
                let intent_hash = row.intent_hash;
                let (pending, progress) = decode_membership_mutation(row)?;
                let MembershipMutationPlan::Resolve(plan) = pending else {
                    return Err(MembershipMutationError::PendingMutation(
                        "another membership mutation is pending".to_string(),
                    )
                    .into());
                };
                if plan.resolution.conflict_hash != conflict_hash
                    || plan.resolution.resolver_pubkey != signer_pubkey
                    || plan.resolution.selection != selection
                {
                    return Err(MembershipMutationError::PendingMutation(
                        "the pending resolution has different immutable inputs".to_string(),
                    )
                    .into());
                }
                (plan, progress, intent_hash)
            }
            None => {
                let (plan, intent_hash) = self
                    .prepare_resolution_mutation(&membership, conflict_hash, selection, created_at)
                    .await?;
                (plan, MembershipMutationProgress::Pending, intent_hash)
            }
        };
        let persistence = self.membership_mutation_persistence(intent_hash);
        plan.validate_closed_shape()?;
        if let MembershipMutationProgress::ResolutionActivated { candidate } = &progress {
            if candidate != &plan.candidate.reference {
                return Err(MembershipMutationError::InvalidDurableMutation(
                    "resolution activation names another candidate".to_string(),
                )
                .into());
            }
            membership
                .apply_resolutions(
                    plan.resolution.store_root_hash,
                    &[(plan.reference.clone(), plan.resolution.clone())],
                )
                .map_err(MembershipMutationError::from)?;
            membership
                .add_entry(plan.publication.entry.clone())
                .map_err(MembershipMutationError::from)?;
            membership
                .activate_head_ref(plan.publication.head_ref.clone())
                .map_err(MembershipMutationError::from)?;
            self.membership = membership;
            return Ok(plan.reference);
        }
        if !matches!(progress, MembershipMutationProgress::Pending) {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership resolution carries another mutation's progress".to_string(),
            )
            .into());
        }
        membership
            .apply_resolutions(
                plan.resolution.store_root_hash,
                &[(plan.reference.clone(), plan.resolution.clone())],
            )
            .map_err(MembershipMutationError::from)?;
        membership
            .add_entry(plan.publication.entry.clone())
            .map_err(MembershipMutationError::from)?;
        let remotes = plan.remote_objects()?;
        self.storage
            .as_ref()
            .create_protocol_object(&plan.prepared_resolution()?)
            .await
            .map_err(MembershipMutationError::from)?;
        self.membership_objects()
            .load_resolution(&plan.reference)
            .await
            .map_err(MembershipMutationError::from)?;
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
            .map_err(MembershipMutationError::from)?;
        persistence
            .mark_remote_object_uploaded(
                exact_owned_remote(&remotes, &plan.candidate.reference.object)?.into_record(),
            )
            .await?;
        let current_remotes = plan.remote_objects()?;
        let reference = self
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
        if reference != plan.candidate.reference {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership resolution accepted another Store candidate".to_string(),
            )
            .into());
        }
        Ok(plan.reference)
    }
}
