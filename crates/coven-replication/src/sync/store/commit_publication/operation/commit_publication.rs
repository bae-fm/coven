use super::*;

pub(crate) enum StoreOperationPublicationOutcome {
    Accepted(coven_database::AcceptedStoreCommitEvidence),
    SnapshotRetired(coven_protocol::store_commit::AcceptedStoreSnapshotRef),
}

impl StoreOperationPublicationOutcome {
    fn require_accepted(self) -> Result<coven_database::AcceptedStoreCommitEvidence, StoreError> {
        match self {
            Self::Accepted(accepted) => Ok(accepted),
            Self::SnapshotRetired(_) => Err(StoreError::InvalidOutbound(
                "Store operation requires candidate replacement after snapshot retirement".into(),
            )),
        }
    }
}

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(crate) async fn prepare_publication_boundary(
        &mut self,
    ) -> Result<coven_database::ObservedStorePublication, StoreError> {
        match self.database.store_current_publication().await? {
            coven_database::StorePublicationBoundary::Observed(observed) => Ok(observed),
            coven_database::StorePublicationBoundary::AcceptedPrefix(_) => {
                self.refresh_membership_publication().await?;
                Ok(self
                    .database
                    .store_current_publication()
                    .await?
                    .require_observed()?
                    .clone())
            }
        }
    }

    pub(crate) async fn prepare_plan(
        &mut self,
    ) -> Result<commit_plan::StoreOperationCommitPlan, StoreError> {
        let authorship = self.database.author_own_stream().await;
        self.prepare_plan_with_authorship(authorship).await
    }

    pub(crate) async fn prepare_plan_with_authorship(
        &mut self,
        authorship: coven_database::OwnStreamAuthorship,
    ) -> Result<commit_plan::StoreOperationCommitPlan, StoreError> {
        let root = self.store_root().clone();
        let author = self.writer.author_pubkey();
        let stream_id = self.announcement_stream_id();
        self.prepare_publication_boundary().await?;
        let base = authorship.local_commit_base(stream_id).await?;
        let (authorship, state) = base.into_parts();
        let (previous, frontier, membership, publication) = state.into_parts();
        let publication_previous = publication.require_observed()?.clone();
        let candidate_membership_heads = membership.head_refs;
        if publication_previous.record().store_root_hash != root.store_root_hash {
            return Err(StoreError::InvalidOutbound(
                "Store publication boundary belongs to another Store root".to_string(),
            ));
        }
        let dependencies = coven_protocol::store_commit::CommitFrontier::from_refs(frontier)
            .map(|frontier| frontier.commits().clone())
            .map_err(StoreError::from)?;
        let seq = commit_plan::next_store_sequence(previous.as_ref())?;
        let coord = coven_protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence: seq,
        };
        let order = coven_protocol::store_commit::StoreCommitOrder {
            seq,
            predecessor: previous,
            dependencies,
        };
        let authorization = self
            .writer
            .authorize_retained_preparation(&self.history, &order, &candidate_membership_heads)
            .await
            .map_err(StoreError::from)?;
        self.membership = authorization.membership.clone();
        let owner_grant = authorization.membership.active_owner_grant(&author);
        let predecessor = authorization
            .membership
            .write_grant_authority(&author)
            .ok_or_else(|| {
                StoreError::InvalidOutbound(format!(
                    "Merge Store operation author {author} has no active write grant"
                ))
            })?;
        Ok(commit_plan::StoreOperationCommitPlan::new(
            authorship,
            Arc::clone(&self.writer),
            root,
            coord,
            order,
            publication_previous,
            authorization.membership_state,
            authorization.device_state_ref,
            predecessor,
            owner_grant,
            authorization.membership,
            authorization.device_state,
        ))
    }

    pub(super) fn membership_authority(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
    ) -> Result<coven_protocol::membership::MembershipCoord, StoreError> {
        let writer = self.writer.author_pubkey();
        let predecessor = membership.write_grant_authority(&writer).ok_or_else(|| {
            StoreError::Preparation(crate::sync::store::StorePreparationError::Gate(format!(
                "Store writer {writer} has no active membership grant"
            )))
        })?;
        Ok(predecessor)
    }

    pub(crate) async fn activate_uploaded(
        &mut self,
        uploaded: commit_plan::UploadedStoreOperationActivation,
    ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, StoreError> {
        self.publish_uploaded(uploaded, None, None)
            .await
            .map(|accepted| accepted.commit_ref().clone())
    }

    pub(crate) async fn publish_prepared(
        &mut self,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        membership_objects: Option<coven_database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<coven_database::AcceptedStoreCommitEvidence, StoreError> {
        self.publish_prepared_attempt(candidate, membership_objects, membership_completion)
            .await?
            .require_accepted()
    }

    pub(crate) async fn publish_prepared_attempt(
        &mut self,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        membership_objects: Option<coven_database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<StoreOperationPublicationOutcome, StoreError> {
        let verified_commit = self
            .history
            .authenticate_commit_bytes(&candidate.reference, &candidate.commit.to_bytes())
            .await?;
        if let Some(accepted) = self
            .database
            .installed_store_commit_evidence(verified_commit.clone())
            .await?
        {
            return self
                .complete_installed_operation(
                    &candidate,
                    verified_commit,
                    accepted,
                    membership_completion,
                )
                .await
                .map(StoreOperationPublicationOutcome::Accepted);
        }
        let uploaded = self
            .upload_authenticated(candidate, verified_commit)
            .await?;
        self.publish_uploaded_attempt(uploaded, membership_objects, membership_completion)
            .await
    }

    pub(crate) async fn upload_prepared(
        &mut self,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
    ) -> Result<commit_plan::UploadedStoreOperationActivation, StoreError> {
        let verified_commit = self
            .history
            .authenticate_commit_bytes(&candidate.reference, &candidate.commit.to_bytes())
            .await?;
        self.upload_authenticated(candidate, verified_commit).await
    }

    async fn upload_authenticated(
        &mut self,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        verified_commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<commit_plan::UploadedStoreOperationActivation, StoreError> {
        candidate.commit.retained_operation_objects()?;
        let reference = candidate.reference.clone();
        let commit = verified_commit.value();
        let circle_activations = if commit.control().is_some() {
            self.history
                .verify_membership_control(&verified_commit)
                .await
                .map_err(StoreError::from)?
        } else {
            coven_protocol::circle_activation::VerifiedCircleActivations::none(commit, &reference)
                .map_err(StoreError::from)?
        };
        self.upload_commit(&candidate).await?;
        Ok(commit_plan::UploadedStoreOperationActivation {
            candidate,
            verified_commit,
            circle_activations,
        })
    }

    pub(crate) async fn publish_uploaded(
        &mut self,
        uploaded: commit_plan::UploadedStoreOperationActivation,
        membership_objects: Option<coven_database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<coven_database::AcceptedStoreCommitEvidence, StoreError> {
        self.publish_uploaded_attempt(uploaded, membership_objects, membership_completion)
            .await?
            .require_accepted()
    }

    async fn publish_uploaded_attempt(
        &mut self,
        uploaded: commit_plan::UploadedStoreOperationActivation,
        membership_objects: Option<coven_database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<StoreOperationPublicationOutcome, StoreError> {
        let commit_plan::UploadedStoreOperationActivation {
            mut candidate,
            verified_commit,
            circle_activations,
        } = uploaded;
        if let Some(accepted) = self
            .database
            .installed_store_commit_evidence(verified_commit.clone())
            .await?
        {
            return self
                .complete_installed_operation(
                    &candidate,
                    verified_commit,
                    accepted,
                    membership_completion,
                )
                .await
                .map(StoreOperationPublicationOutcome::Accepted);
        }
        self.database
            .mark_candidate_commit_uploaded(candidate.reference.clone())
            .await?;
        let accepted_publication = match self
            .publish_store_commit_publication(&verified_commit)
            .await?
        {
            crate::sync::store::authorization::history::publication::StoreCommitPublicationAttemptOutcome::SnapshotRetired(snapshot) => {
                return Ok(StoreOperationPublicationOutcome::SnapshotRetired(snapshot));
            }
            outcome => outcome.require_published()?,
        };
        let accepted_publication = match accepted_publication {
            coven_database::StoreCommitPublicationOutcome::Installed(accepted) => {
                return self
                    .complete_installed_operation(
                        &candidate,
                        verified_commit,
                        accepted,
                        membership_completion,
                    )
                    .await
                    .map(StoreOperationPublicationOutcome::Accepted);
            }
            accepted => accepted,
        };
        let database = self.database.clone();
        let root = self.store_root().clone();
        let commit = verified_commit.value().clone();
        let history_evidence = candidate.history_evidence.clone();
        let membership_heads = &commit.membership_state.heads;
        let authorization = self
            .history
            .authorize_retained_outbound(
                &commit.order,
                membership_heads,
                &commit.author_registration,
            )
            .await
            .map_err(StoreError::from)?;
        let device_operations = self
            .history
            .load_local_device_operations(
                &verified_commit,
                &authorization.membership,
                &authorization.device_state_ref,
                authorization.device_state,
            )
            .await
            .map_err(StoreError::from)?;
        let accepted: coven_database::AcceptedStoreCommitEvidence = match &accepted_publication {
            coven_database::StoreCommitPublicationOutcome::Accepted { interval, .. } => {
                interval.accepted_commit(&verified_commit)?.into()
            }
            coven_database::StoreCommitPublicationOutcome::Installed(evidence) => evidence.clone(),
        };
        let (operation_object_ids, membership_completion) = self
            .finalize_store_operation_authority(
                &candidate,
                &verified_commit,
                &accepted_publication,
                membership_completion,
            )
            .await?;
        let registrations = candidate
            .registration_activation
            .take()
            .into_iter()
            .collect::<Vec<_>>();
        let membership_after = if let Some(proof) = &history_evidence.membership_proof {
            // A competing publication may have refreshed this chain while the
            // candidate kept its original predecessor. Extend the current chain.
            let mut membership = self.membership.clone();
            membership = membership
                .with_exact_entry(&proof.entry_value)
                .map_err(MembershipMutationError::from)?;
            if !membership.covers_heads(std::slice::from_ref(&proof.head)) {
                membership
                    .activate_head_ref(proof.head.clone())
                    .map_err(MembershipMutationError::from)?;
            }
            Some(membership)
        } else {
            None
        };
        let materialization = database
            .materialize_published_store_operation(
                root,
                verified_commit,
                registrations,
                device_operations,
                circle_activations,
                accepted_publication,
                history_evidence,
                membership_objects,
                operation_object_ids,
                membership_completion,
            )
            .await?;
        if let Some(materialization) = materialization {
            self.history
                .admit_materialized_publication(&materialization)
                .map_err(StoreError::from)?;
            if let Some(membership) = membership_after {
                self.membership = membership;
            }
        } else if membership_after.is_some() {
            // A different pull installed this control and may have advanced
            // further. Its current accepted chain owns the resulting authority.
            self.refresh_installed_membership().await?;
        }
        Ok(StoreOperationPublicationOutcome::Accepted(accepted))
    }

    async fn finalize_store_operation_authority(
        &mut self,
        candidate: &commit_plan::PreparedStoreOperationCommit,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        publication: &coven_database::StoreCommitPublicationOutcome,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<
        (
            Option<Vec<coven_protocol::store_commit::ObjectHash>>,
            Option<coven_protocol::membership_mutation::StoreMembershipJournalCompletion>,
        ),
        StoreError,
    > {
        let reference = verified_commit.reference();
        let mut membership_completion = membership_completion;
        if let Some(completion) = &mut membership_completion {
            if let coven_protocol::membership_mutation::StoreMembershipJournalCompletion::MembershipCandidateAbandoned { original, .. } = completion {
                original.prepared_membership_publication()?.candidate_object_refs(&original.commit, &original.reference)?;
                coven_protocol::remote_object::CandidateNonactivation::validate_durable_shape(
                    &original.reference,
                    &original.commit,
                    coven_protocol::remote_object::CandidateNonactivationProof::AcceptedAbandonment {
                        abandonment: coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
                            coord: candidate.reference.coord.clone(),
                            object: candidate.reference.object.clone(),
                            canonical_signed_bytes: candidate.commit.to_bytes(),
                        },
                    },
                )?;
            } else {
            let proof = candidate
                .history_evidence
                .membership_proof
                .as_ref()
                .ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "membership finalization omits its prepared authority proof".into(),
                    )
                })?;
            let result = self
                .finalize_membership_head_acceptance(verified_commit, proof, publication)
                .await?;
            completion.retain_acceptance_result(result);
            }
        }
        let operation_object_ids = if membership_completion.is_none() {
            Some(
                std::iter::once(coven_protocol::remote_object::remote_object_id(
                    &reference.object,
                ))
                .chain(
                    candidate
                        .commit
                        .retained_operation_objects()?
                        .iter()
                        .map(coven_protocol::remote_object::remote_object_id),
                )
                .chain(
                    candidate
                        .history_evidence
                        .acknowledgement
                        .iter()
                        .flat_map(|proof| {
                            proof.predecessors.iter().map(|(reference, _)| {
                                coven_protocol::remote_object::remote_object_id(&reference.object)
                            })
                        }),
                )
                .collect(),
            )
        } else {
            None
        };
        if let Some(completion) = &membership_completion {
            let completion_ids = completion
                .object_refs()
                .iter()
                .map(coven_protocol::remote_object::remote_object_id)
                .collect::<std::collections::BTreeSet<_>>();
            if completion_ids.is_empty()
                || !completion_ids.contains(&coven_protocol::remote_object::remote_object_id(
                    &reference.object,
                ))
            {
                return Err(StoreError::InvalidOutbound(
                    "membership journal completion does not cover its exact Store candidate"
                        .to_string(),
                ));
            }
        }
        Ok((operation_object_ids, membership_completion))
    }

    async fn complete_installed_operation(
        &mut self,
        candidate: &commit_plan::PreparedStoreOperationCommit,
        verified_commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
        accepted: coven_database::AcceptedStoreCommitEvidence,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<coven_database::AcceptedStoreCommitEvidence, StoreError> {
        candidate.validate_closed_shape()?;
        let (operation_object_ids, membership_completion) = self
            .finalize_store_operation_authority(
                candidate,
                &verified_commit,
                &coven_database::StoreCommitPublicationOutcome::Installed(accepted.clone()),
                membership_completion,
            )
            .await?;
        let accepted = self
            .database
            .complete_installed_store_operation(
                verified_commit,
                accepted,
                candidate.history_evidence.clone(),
                operation_object_ids,
                membership_completion,
            )
            .await?;
        if candidate.history_evidence.membership_proof.is_some() {
            self.refresh_installed_membership().await?;
        }
        Ok(accepted)
    }

    async fn refresh_installed_membership(&mut self) -> Result<(), StoreError> {
        let founder = self.protocol_root().descriptor.founder_pubkey.clone();
        self.membership = self
            .history
            .load_current_membership(&founder)
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation(
                    "load installed membership authority",
                    error,
                )
            })?;
        Ok(())
    }

    pub(crate) async fn prepare_candidate(
        &mut self,
        plan: &commit_plan::StoreOperationCommitPlan,
        batch: commit_plan::StoreOperationBatch,
    ) -> Result<commit_plan::PreparedStoreOperationCommit, StoreError> {
        self.prepare_candidate_for_write(plan, batch, self.database.new_store_write_id())
            .await
    }

    pub(crate) async fn prepare_replacement_candidate(
        &mut self,
        plan: &commit_plan::StoreOperationCommitPlan,
        batch: commit_plan::StoreOperationBatch,
        previous: &commit_plan::PreparedStoreOperationCommit,
    ) -> Result<commit_plan::PreparedStoreOperationCommit, StoreError> {
        if plan.coord() != &previous.reference.coord
            || !plan.is_local_registration(&previous.commit.author_registration)
        {
            return Err(StoreError::InvalidOutbound(
                "replacement operation differs from its reserved author position".into(),
            ));
        }
        self.prepare_candidate_for_write(plan, batch, previous.commit.write_id.clone())
            .await
    }

    pub(crate) async fn prepare_candidate_for_write(
        &mut self,
        plan: &commit_plan::StoreOperationCommitPlan,
        batch: commit_plan::StoreOperationBatch,
        write_id: coven_protocol::write::WriteId,
    ) -> Result<commit_plan::PreparedStoreOperationCommit, StoreError> {
        let storage = self.storage.as_ref();
        let acknowledgement_evidence = match &batch {
            commit_plan::StoreOperationBatch::Acknowledgement {
                reference, value, ..
            } => Some((reference.clone(), value.clone())),
            _ => None,
        };
        let retained_registration_evidence = match &batch {
            commit_plan::StoreOperationBatch::JoinActivation { registration, .. } => {
                vec![registration.registration().clone()]
            }
            commit_plan::StoreOperationBatch::SamePrincipalDeviceJoin { registration, .. } => {
                vec![registration.registration().clone()]
            }
            _ => Vec::new(),
        };
        let retained_device_operations = match &batch {
            commit_plan::StoreOperationBatch::DeviceExclusionProposal { proposal, .. } => Some(
                coven_protocol::store_commit::RetainedStoreDeviceOperations::from_sources(
                    vec![proposal.clone()],
                    Vec::new(),
                ),
            ),
            commit_plan::StoreOperationBatch::DeviceExclusionOutcome { outcome, .. } => Some(
                coven_protocol::store_commit::RetainedStoreDeviceOperations::from_sources(
                    Vec::new(),
                    vec![outcome.clone()],
                ),
            ),
            _ => None,
        };
        let (commit, registration_activation) = plan.sign_batch(write_id, batch)?;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            plan.root().store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let stream_id = plan.coord().stream_id.to_string();
        let prefix = coven_protocol::store_commit::commit_semantic_prefix(
            commit.candidate_family(),
            &stream_id,
            commit.seq(),
            commit.commit_hash(),
        );
        let slot = storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let prepared = storage
            .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let verified_commit =
            plan.verify_prepared_commit(&commit.to_bytes(), prepared.reference().clone())?;
        let common = commit_plan::PreparedStoreOperationCommon {
            reference: verified_commit.reference().clone(),
            commit,
            registration_activation,
        };
        let acknowledgement = match acknowledgement_evidence {
            Some((reference, value)) => Some(
                self.history
                    .retain_acknowledgement(&verified_commit, reference, value)
                    .map_err(StoreError::from)?,
            ),
            None => None,
        };
        let merge_history_evidence =
            crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence {
                registrations: retained_registration_evidence,
                acknowledgement,
                membership_proof: None,
            };
        let registrations = common
            .registration_activation
            .as_ref()
            .map(|activation| vec![activation.clone()])
            .unwrap_or_default();
        let device_operations = match retained_device_operations {
            Some(retained) => retained
                .verify_for(plan.root(), &common.commit)
                .map_err(StoreError::from)?,
            None => {
                coven_protocol::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
                    &common.commit,
                )
                .map_err(StoreError::from)?
            }
        };
        let state_after = Box::pin(self.history.derive_local_post_device_state(
            &common.commit,
            plan.predecessor_state().clone(),
            &registrations,
            device_operations,
        ))
        .await
        .map_err(StoreError::from)?;
        let successor = self
            .history
            .prepare_merge_history_successor(
                &verified_commit,
                plan.membership(),
                None,
                plan.predecessor_state(),
                &state_after,
                merge_history_evidence,
            )
            .await
            .map_err(StoreError::from)?;
        let publication_entry = self
            .writer
            .sign_store_publication_entry(plan.publication_previous(), &verified_commit)
            .map_err(StoreError::from)?;
        let publication_prefix =
            coven_protocol::store_commit::store_publication_entry_semantic_prefix(
                &publication_entry,
            );
        let publication_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            common.commit.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StorePublicationEntry,
        );
        let publication_slot = storage
            .allocate_protocol_slot(&publication_context, &publication_prefix, ".json")
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let prepared_publication = storage
            .prepare_protocol_object(
                &publication_context,
                publication_slot,
                &publication_prefix,
                publication_entry.to_bytes(),
            )
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let replacement = self
            .writer
            .advance_store_publication(
                plan.publication_previous(),
                &publication_entry,
                &prepared_publication,
                &verified_commit,
            )
            .map_err(StoreError::from)?;
        Ok(commit_plan::PreparedStoreOperationCommit {
            common,
            publication: coven_protocol::prepared_commit::PreparedStorePublication {
                previous: plan.publication_previous().record().clone(),
                previous_version: plan.publication_previous().version().clone(),
                entry: publication_entry,
                entry_object: prepared_publication.reference().clone(),
                replacement,
            },
            history_evidence: successor.history_evidence,
        })
    }
}
