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
        publication: &PreparedMembershipPublication,
    ) -> Result<coven_database::ActiveStorePublication, MembershipMutationError> {
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
                    publication: Box::new(publication.clone()),
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
        transition: &PreparedMembershipTransition,
        wraps: &[PreparedWrappedStoreKey],
    ) -> Result<(), MembershipMutationError> {
        transition.validate()?;
        let expected_wraps: Vec<&WrappedStoreKeyRef> = match &transition.entry.change {
            StoreAuthorityChange::SetMember { wrapped_key, .. } => vec![wrapped_key],
            StoreAuthorityChange::RemoveMember { wrapped_keys, .. } => {
                wrapped_keys.iter().collect()
            }
            StoreAuthorityChange::Founder { .. }
            | StoreAuthorityChange::DeviceRegistrationActivation { .. }
            | StoreAuthorityChange::DeviceExclusionProposal { .. }
            | StoreAuthorityChange::DeviceExclusionOutcome { .. }
            | StoreAuthorityChange::ProviderAdmin
            | StoreAuthorityChange::ResolutionActivation { .. } => Vec::new(),
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
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        completion: coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
    ) -> Result<store_commit::StoreBatchCommitRef, MembershipMutationError> {
        let authorship = self.database.author_own_stream().await;
        self.publish_membership_activation_with_authorship(
            transition,
            publication,
            candidate,
            completion,
            &authorship,
        )
        .await
    }

    pub(crate) async fn publish_membership_activation_with_authorship(
        &mut self,
        transition: &PreparedMembershipTransition,
        publication: &PreparedMembershipPublication,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        completion: coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        _authorship: &coven_database::OwnStreamAuthorship,
    ) -> Result<store_commit::StoreBatchCommitRef, MembershipMutationError> {
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
                membership::MembershipHeadActivation::StoreCommit { commit, .. }
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
        self.publish_prepared(candidate, Some(membership_objects), Some(completion))
            .await
            .map(|accepted| accepted.commit_ref().clone())
            .map_err(MembershipMutationError::from)
    }
}
