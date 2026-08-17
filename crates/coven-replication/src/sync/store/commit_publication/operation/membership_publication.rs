use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(super) async fn outbound_membership_mutation(
        &self,
    ) -> Result<Option<coven_database::DurableMembershipMutation>, MembershipMutationError> {
        self.database
            .outbound_membership_mutation()
            .await
            .map_err(MembershipMutationError::from)
    }

    pub(super) async fn stage_membership_mutation(
        &self,
        plan_bytes: Vec<u8>,
        progress_bytes: Vec<u8>,
        remote_objects: Option<Vec<coven_protocol::remote_object::ClosedRemoteObject>>,
        pending_rotation_generation: Option<u64>,
    ) -> Result<coven_protocol::store_commit::ObjectHash, MembershipMutationError> {
        match remote_objects {
            Some(remote_objects) => self
                .database
                .stage_membership_candidate_mutation(
                    plan_bytes,
                    progress_bytes,
                    remote_objects,
                    pending_rotation_generation,
                )
                .await
                .map_err(MembershipMutationError::from),
            None => self
                .database
                .stage_membership_mutation(plan_bytes, progress_bytes, pending_rotation_generation)
                .await
                .map_err(MembershipMutationError::from),
        }
    }

    pub(super) fn membership_mutation_persistence(
        &self,
        intent_hash: coven_protocol::store_commit::ObjectHash,
    ) -> MutationPersistence {
        MutationPersistence::new(
            self.database.clone(),
            std::sync::Arc::clone(self.storage),
            intent_hash,
        )
    }

    pub(super) async fn publish_direct_membership_authority(
        &mut self,
        wraps: &[ReplacementWrappedKey],
        publication: &PreparedMembershipPublication,
    ) -> Result<(), MembershipMutationError> {
        for wrapped in wraps {
            self.storage
                .as_ref()
                .create_protocol_object(&wrapped.prepared.object)
                .await
                .map_err(MembershipMutationError::from)?;
            load_wrapped_store_key(
                self.storage.as_ref(),
                self.store_root().store_root_hash,
                &wrapped.prepared.reference,
            )
            .await?;
        }
        self.storage
            .as_ref()
            .create_protocol_object(&publication.prepared_entry()?)
            .await
            .map_err(MembershipMutationError::from)?;
        self.membership_objects()
            .load_entry(&publication.entry_ref)
            .await
            .map_err(MembershipMutationError::from)?;
        Ok(())
    }

    pub(super) async fn publish_direct_membership_head(
        &mut self,
        publication: &PreparedMembershipPublication,
        author: &coven_protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<(), MembershipMutationError> {
        self.storage
            .as_ref()
            .create_protocol_object(&publication.prepared_head()?)
            .await
            .map_err(MembershipMutationError::from)?;
        self.membership_objects()
            .load_head_for_registration(&publication.head_ref, author)
            .await
            .map_err(MembershipMutationError::from)?;
        Ok(())
    }

    pub(crate) async fn prepare_membership_transition(
        &mut self,
        chain: &MembershipChain,
        entry: MembershipEntry,
    ) -> Result<PreparedMembershipTransition, MembershipMutationError> {
        let root = self.store_root().clone();
        let storage = self.storage.as_ref();
        let (_, entry_ref) =
            store_objects::prepare_membership_entry(storage, root.store_root_hash, &entry)
                .await
                .map_err(MembershipMutationError::from)?;
        let coord = entry.coord();
        let predecessor = chain
            .head_ref_for_stream(
                &coord.author_pubkey,
                &coord.author_owner_grant,
                coord.stream_id,
            )
            .cloned();
        let current_slot = match predecessor.as_ref() {
            Some(reference) => {
                let loaded = self
                    .writer
                    .load_membership_head(self.membership_objects(), reference)
                    .await
                    .map_err(MembershipMutationError::from)?;
                loaded.value.body.successor.next_slot.clone()
            }
            None => match chain.membership_anchor(&coord.author_owner_grant) {
                Some(store_commit::GrantStreamAnchor::StoreMembership { first_slot }) => {
                    first_slot.clone()
                }
                Some(
                    store_commit::GrantStreamAnchor::OwnerRecovery { .. }
                    | store_commit::GrantStreamAnchor::CircleControl { .. }
                    | store_commit::GrantStreamAnchor::CircleRoster { .. }
                    | store_commit::GrantStreamAnchor::CircleMetadata { .. },
                ) => {
                    return Err(MembershipMutationError::InvalidDurableMutation(format!(
                        "Owner grant {} uses another domain's anchor as its membership stream",
                        coord.author_owner_grant
                    )));
                }
                None => {
                    return Err(MembershipMutationError::InvalidDurableMutation(format!(
                        "Owner grant {} has no activated membership stream anchor",
                        coord.author_owner_grant
                    )));
                }
            },
        };
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
            MembershipMutationError::InvalidDurableMutation(
                "membership head sequence overflow".to_string(),
            )
        })?;
        let next_prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            next_sequence,
        );
        let next_slot = storage
            .allocate_protocol_slot(&context, &next_prefix, ".json")
            .await?;
        let anchor = chain
            .membership_anchor(&coord.author_owner_grant)
            .ok_or_else(|| {
                MembershipMutationError::InvalidDurableMutation(format!(
                    "Owner grant {} has no activated membership stream anchor",
                    coord.author_owner_grant
                ))
            })?;
        let transition = self.writer.build_membership_transition(
            root.store_root_hash,
            &entry,
            entry_ref.clone(),
            predecessor,
            anchor.clone(),
            next_slot,
            current_slot,
        )?;
        Ok(PreparedMembershipTransition {
            entry,
            entry_ref,
            transition,
        })
    }

    pub(crate) async fn prepare_membership_publication(
        &mut self,
        chain: &MembershipChain,
        entry: MembershipEntry,
    ) -> Result<PreparedMembershipPublication, MembershipMutationError> {
        let prepared = self.prepare_membership_transition(chain, entry).await?;
        self.finish_membership_transition(prepared, membership::MembershipHeadActivation::Direct)
            .await
    }

    pub(crate) async fn finish_membership_transition(
        &mut self,
        prepared: PreparedMembershipTransition,
        activation: membership::MembershipHeadActivation,
    ) -> Result<PreparedMembershipPublication, MembershipMutationError> {
        let root = self.store_root().clone();
        let head =
            self.writer
                .sign_membership_head(&prepared.entry, &prepared.transition, activation)?;
        let coord = prepared.entry.coord();
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let head_prefix = membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        );
        let head_bytes = serde_json::to_vec(&head).map_err(MembershipMutationError::Json)?;
        let head_object = self.storage.as_ref().prepare_protocol_object(
            &context,
            prepared.transition.head_slot.clone(),
            &head_prefix,
            head_bytes,
        )?;
        let head_ref = MembershipHeadRef {
            coord,
            head_hash: head.head_hash(),
            object: head_object.reference().clone(),
        };
        let publication = PreparedMembershipPublication {
            entry: prepared.entry,
            entry_ref: prepared.entry_ref,
            head,
            head_ref,
        };
        publication.validate()?;
        Ok(publication)
    }

    pub(crate) async fn publish_membership_authority(
        &mut self,
        transition: &PreparedMembershipTransition,
        wraps: &[PreparedWrappedStoreKey],
    ) -> Result<(), MembershipMutationError> {
        transition.validate()?;
        let expected_wraps: Vec<&WrappedStoreKeyRef> = match &transition.entry.change {
            MembershipChange::SetMember { wrapped_key, .. } => vec![wrapped_key],
            MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.iter().collect(),
            MembershipChange::Founder { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => Vec::new(),
        };
        if expected_wraps.len() != wraps.len()
            || expected_wraps
                .iter()
                .zip(wraps)
                .any(|(expected, prepared)| **expected != prepared.reference)
        {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "prepared Merge membership wraps differ from their exact transition".to_string(),
            ));
        }
        for prepared in wraps {
            prepared.validate()?;
            self.storage
                .as_ref()
                .create_protocol_object(&prepared.object)
                .await
                .map_err(MembershipMutationError::from)?;
            load_wrapped_store_key(
                self.storage.as_ref(),
                self.store_root().store_root_hash,
                &prepared.reference,
            )
            .await?;
        }
        self.storage
            .as_ref()
            .create_protocol_object(&transition.prepared_entry()?)
            .await
            .map_err(MembershipMutationError::from)?;
        self.membership_objects()
            .load_entry(&transition.entry_ref)
            .await
            .map_err(MembershipMutationError::from)?;
        Ok(())
    }

    pub(crate) async fn publish_membership_activation(
        &mut self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        completion: coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
    ) -> Result<commit_plan::StoreOperationPublicationOutcome, MembershipMutationError> {
        transition.validate()?;
        publication.validate()?;
        candidate
            .validate_closed_shape()
            .map_err(MembershipMutationError::PreparedCommit)?;
        if candidate.commit.control()
            != Some(&store_commit::StoreControl {
                transition: transition.transition.clone(),
            })
            || !transition
                .transition
                .matches_head(&publication.head, &publication.head_ref)
            || !matches!(
                &publication.head.activation,
                membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &candidate.reference
            )
            || !self.writer.verify_membership_head(&publication.head)
        {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "prepared Merge membership head differs from its exact Store activation"
                    .to_string(),
            ));
        }
        self.storage
            .as_ref()
            .create_protocol_object(&publication.prepared_head()?)
            .await
            .map_err(MembershipMutationError::from)?;
        self.writer
            .load_membership_head(self.membership_objects(), &publication.head_ref)
            .await
            .map_err(MembershipMutationError::from)?;
        let database = self.database.clone();
        database
            .mark_remote_object_uploaded(
                completion
                    .remote_object(&publication.head_ref.object)
                    .map_err(MembershipMutationError::from)?,
            )
            .await?;
        let membership_objects = VerifiedMergeMembershipObjects::verify(
            &candidate.commit,
            &candidate.reference,
            &transition.entry,
            &publication.head,
            publication.head_ref.clone(),
        )?;
        let _authorship = database.author_own_stream().await;
        self.publish_prepared(candidate, Some(membership_objects), Some(completion))
            .await
            .map_err(MembershipMutationError::from)
    }
}
