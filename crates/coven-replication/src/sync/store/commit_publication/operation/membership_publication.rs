use super::*;

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(crate) async fn refresh_membership_publication(&mut self) -> Result<(), StoreError> {
        let pulled = self
            .writer
            .install_current_publication(&mut self.history, &mut self.membership)
            .await?;
        if !pulled.held_positions.is_empty() {
            return Err(StoreError::PublicationHeld(pulled.held_positions));
        }
        Ok(())
    }

    pub(super) async fn abandon_membership_candidate(
        &mut self,
        intent_hash: store_commit::ObjectHash,
        original: &commit_plan::PreparedStoreOperationCommit,
    ) -> Result<coven_database::ActiveStorePublication, MembershipMutationError> {
        let publication = original.prepared_membership_publication()?;
        publication.candidate_object_refs(&original.commit, &original.reference)?;
        let active = self
            .database
            .active_store_publication()
            .await?
            .ok_or_else(|| {
                MembershipMutationError::InvalidDurableMutation(
                    "membership abandonment has no reserved publication".into(),
                )
            })?;
        if active.owner() != &coven_database::ActiveStorePublicationOwner::MembershipMutation {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "membership abandonment found another publication owner".into(),
            ));
        }
        if !active.is_awaiting_preparation() {
            let abandonment = match active.membership_abandonment() {
                Some(candidate) => candidate.clone(),
                None => {
                    self.refresh_membership_publication().await?;
                    let plan = self.prepare_plan().await?;
                    let abandonment = self
                        .prepare_replacement_candidate(
                            &plan,
                            commit_plan::StoreOperationBatch::AbandonCandidates(vec![
                                store_commit::CandidateCleanupManifest {
                                    candidate: store_commit::StoreBatchCommitDeletionTarget {
                                        coord: original.reference.coord.clone(),
                                        object: original.reference.object.clone(),
                                        canonical_signed_bytes: original.commit.to_bytes(),
                                    },
                                },
                            ]),
                            original,
                        )
                        .await?;
                    self.database
                        .stage_membership_candidate_abandonment(
                            intent_hash,
                            active,
                            original.clone(),
                            abandonment.clone(),
                        )
                        .await?;
                    abandonment
                }
            };
            let bytes = abandonment.commit.to_bytes();
            let remote = coven_protocol::remote_object::RemoteObjectRecord::candidate_commit(
                abandonment.reference.clone(),
                &bytes,
                &bytes,
            )?
            .into_record();
            let database = self.database.clone();
            let _authorship = database.author_own_stream().await;
            self.publish_prepared(
                Box::new(abandonment), None,
                Some(coven_protocol::membership_mutation::StoreMembershipJournalCompletion::MembershipCandidateAbandoned {
                    intent_hash,
                    original: Box::new(original.clone()),
                    remote_objects: vec![remote],
                }),
            ).await?;
        }
        let active = self
            .database
            .active_store_publication()
            .await?
            .ok_or_else(|| {
                MembershipMutationError::InvalidDurableMutation(
                    "accepted abandonment lost its continuation reservation".into(),
                )
            })?;
        if !active.is_awaiting_preparation() {
            return Err(MembershipMutationError::InvalidDurableMutation(
                "accepted abandonment did not advance its continuation reservation".into(),
            ));
        }
        crate::sync::store::authorization::retire_store_write_candidates(
            &self.database,
            self.storage.as_ref(),
            active,
        )
        .await?;
        self.database
            .active_store_publication()
            .await?
            .ok_or_else(|| {
                MembershipMutationError::InvalidDurableMutation(
                    "membership cleanup lost its continuation reservation".into(),
                )
            })
    }

    pub(crate) async fn prepare_authority_change(
        &mut self,
        chain: &MembershipChain,
        change: StoreAuthorityChange,
    ) -> Result<PreparedMembershipTransition, MembershipMutationError> {
        let stream = self.select_membership_author_stream(chain).await?;
        let entry =
            self.writer
                .sign_authority_change(chain, stream, change, self.database.stamp())?;
        self.prepare_membership_transition(chain, entry).await
    }

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
        remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
        candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<coven_protocol::store_commit::ObjectHash, MembershipMutationError> {
        self.database
            .stage_membership_candidate_mutation(
                plan_bytes,
                progress_bytes,
                remote_objects,
                candidate,
            )
            .await
            .map_err(MembershipMutationError::from)
    }

    pub(super) fn membership_mutation_persistence(
        &self,
        intent_hash: coven_protocol::store_commit::ObjectHash,
    ) -> MutationPersistence {
        MutationPersistence::new(self.database.clone(), intent_hash)
    }

    pub(crate) async fn prepare_membership_transition(
        &mut self,
        chain: &MembershipChain,
        entry: MembershipEntry,
    ) -> Result<PreparedMembershipTransition, MembershipMutationError> {
        self.history
            .prepare_membership_transition(
                &self.writer.membership_publication_signer(),
                chain,
                entry,
            )
            .await
    }

    pub(crate) async fn finish_store_membership_transition(
        &mut self,
        prepared: PreparedMembershipTransition,
        commit: store_commit::StoreBatchCommitRef,
    ) -> Result<PreparedMembershipPublication, MembershipMutationError> {
        self.history
            .finish_store_membership_transition(
                &self.writer.membership_publication_signer(),
                prepared,
                commit,
            )
            .await
    }

    pub(crate) async fn publish_membership_authority(
        &mut self,
        candidate: &commit_plan::PreparedStoreOperationCommit,
        remotes: &[coven_protocol::remote_object::ClosedRemoteObject],
    ) -> Result<(), MembershipMutationError> {
        use coven_protocol::objects::PreparedExactObject;
        use coven_protocol::remote_object::ClosedRemoteObject;

        let publication = candidate.prepared_membership_publication()?;
        let expected_wraps: &[WrappedStoreKeyRef] = match &publication.entry.change {
            StoreAuthorityChange::SetMember { wrapped_key, .. } => {
                std::slice::from_ref(wrapped_key)
            }
            StoreAuthorityChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
            StoreAuthorityChange::Founder { .. }
            | StoreAuthorityChange::DeviceRegistrationActivation { .. }
            | StoreAuthorityChange::DeviceExclusionProposal { .. }
            | StoreAuthorityChange::DeviceExclusionOutcome { .. }
            | StoreAuthorityChange::ProviderAdmin
            | StoreAuthorityChange::ResolutionActivation { .. } => &[],
        };
        let prepare_exact = |reference: &coven_protocol::objects::ExactObjectRef| -> Result<
            (ClosedRemoteObject, PreparedExactObject),
            MembershipMutationError,
        > {
            let remote = exact_owned_remote(remotes, reference)?;
            let bytes = remote.stored_bytes().ok_or_else(|| {
                MembershipMutationError::InvalidDurableMutation(format!(
                    "membership candidate lacks stored bytes for exact object {}",
                    reference.slot().logical_key(),
                ))
            })?;
            let prepared = PreparedExactObject::new(reference.clone(), bytes.to_vec())?;
            Ok((remote, prepared))
        };
        let (entry_remote, entry) = prepare_exact(&publication.entry_ref.object)?;
        let wraps = expected_wraps
            .iter()
            .map(|reference| {
                let (remote, object) = prepare_exact(&reference.object)?;
                let prepared = PreparedWrappedStoreKey {
                    reference: reference.clone(),
                    object,
                };
                prepared.validate()?;
                Ok((remote, prepared))
            })
            .collect::<Result<Vec<_>, MembershipMutationError>>()?;

        for (remote, prepared) in wraps {
            self.storage
                .as_ref()
                .create_protocol_object(&prepared.object)
                .await?;
            load_wrapped_store_key(
                self.storage.as_ref(),
                self.store_root().store_root_hash,
                &prepared.reference,
            )
            .await?;
            self.database
                .mark_remote_object_uploaded(remote.into_record())
                .await?;
        }
        self.storage.as_ref().create_protocol_object(&entry).await?;
        self.membership_objects()
            .load_entry(&publication.entry_ref)
            .await?;
        self.database
            .mark_remote_object_uploaded(entry_remote.into_record())
            .await?;
        Ok(())
    }

    pub(super) async fn finalize_membership_head_acceptance(
        &mut self,
        commit: &store_commit::VerifiedStoreBatchCommit,
        proof: &store_commit::RetainedMergeMembershipProof,
        publication: &coven_database::StoreCommitPublicationOutcome,
    ) -> Result<coven_protocol::remote_object::RemoteObjectRecord, StoreError> {
        self.history
            .finalize_membership_head_acceptance(
                &self.writer.membership_publication_signer(),
                commit,
                proof,
                publication,
            )
            .await
    }

    pub(crate) async fn publish_membership_activation(
        &mut self,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        completion: coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
    ) -> Result<store_commit::StoreBatchCommitRef, MembershipMutationError> {
        let authorship = self.database.author_own_stream().await;
        self.publish_membership_activation_with_authorship(candidate, completion, &authorship)
            .await
    }

    pub(crate) async fn publish_membership_activation_with_authorship(
        &mut self,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        completion: coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        _authorship: &coven_database::OwnStreamAuthorship,
    ) -> Result<store_commit::StoreBatchCommitRef, MembershipMutationError> {
        let publication = candidate
            .prepared_membership_publication()
            .map_err(MembershipMutationError::PreparedCommit)?;
        if !self.writer.verify_membership_head(&publication.head) {
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
            &publication.entry,
            &publication.head,
            publication.head_ref.clone(),
        )?;
        self.publish_prepared(candidate, Some(membership_objects), Some(completion))
            .await
            .map(|accepted| accepted.commit_ref().clone())
            .map_err(MembershipMutationError::from)
    }
}
