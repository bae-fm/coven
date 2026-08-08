use super::*;
use coven_storage::VerifiedObjectWrites;

impl<'storage> AuthorizedWriterOperation<'storage> {
    pub(super) async fn reject_excluded_merge_candidate(
        &self,
        candidate: &coven_protocol::store_commit::StoreBatchCommitRef,
        author: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<(), StoreError> {
        if self
            .database
            .author_exclusion_activation_for_candidate(
                self.history.root().clone(),
                candidate.clone(),
                author.clone(),
            )
            .await?
            .is_some()
        {
            return Err(StoreError::AuthorExcluded {
                device_id: author.device_id,
            });
        }
        Ok(())
    }

    pub(crate) async fn prepare_plan(
        &mut self,
    ) -> Result<commit_plan::StoreOperationCommitPlan, StoreError> {
        let root = self.store_root().clone();
        let candidate_membership_heads = self.membership.head_refs().to_vec();
        let author = self.writer.author_pubkey();
        let stream_id = self.announcement_stream_id();
        let base = self.database.local_commit_base(stream_id).await?;
        let previous = base.predecessor;
        let dependencies = coven_protocol::store_commit::CommitFrontier::from_refs(base.frontier)
            .map(|frontier| frontier.commits().clone())
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
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
            .authorize_retained_outbound(&self.history, &order, &candidate_membership_heads)
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
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
            commit_plan::StoreOperationPlanCommon::new(
                base.authorship,
                Arc::clone(&self.writer),
                root,
                coord,
                order,
                authorization.membership_state,
                authorization.device_state_ref,
                coven_protocol::store_commit::StoreOperationMembershipAuthority { predecessor },
                owner_grant,
            ),
            authorization.membership,
            authorization.device_state,
        ))
    }

    pub(super) fn membership_authority(
        &self,
        membership: &coven_protocol::membership::MembershipChain,
    ) -> Result<coven_protocol::store_commit::StoreOperationMembershipAuthority, StoreError> {
        let writer = self.writer.author_pubkey();
        let predecessor = membership.write_grant_authority(&writer).ok_or_else(|| {
            StoreError::Preparation(crate::sync::store::StorePreparationError::Gate(format!(
                "Store writer {writer} has no active membership grant"
            )))
        })?;
        Ok(coven_protocol::store_commit::StoreOperationMembershipAuthority { predecessor })
    }

    pub(crate) async fn prepare_conflict_resolution_plan(
        &mut self,
        candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
    ) -> Result<MergeConflictResolutionCommitPlan, StoreError> {
        let root = self.store_root().clone();
        let stream_id = self.announcement_stream_id();
        let base = self.database.local_commit_base(stream_id).await?;
        let previous = base.predecessor;
        let dependencies = coven_protocol::store_commit::CommitFrontier::from_refs(base.frontier)
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let seq = commit_plan::next_store_sequence(previous.as_ref())?;
        let coord = coven_protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence: seq,
        };
        let order = coven_protocol::store_commit::StoreCommitOrder {
            seq,
            predecessor: previous,
            dependencies: dependencies.0,
        };
        let authorization = self
            .writer
            .authorize_retained_conflict_resolution(
                &self.history,
                &order,
                candidate_membership_heads,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        Ok(MergeConflictResolutionCommitPlan::new(
            base.authorship,
            Arc::clone(&self.writer),
            root,
            coord,
            order,
            authorization,
        ))
    }

    pub(crate) async fn prepare_candidate(
        &mut self,
        plan: commit_plan::StoreOperationCommitPlan,
        batch: commit_plan::StoreOperationBatch,
    ) -> Result<commit_plan::PreparedStoreOperationCommit, StoreError> {
        self.prepare_candidate_borrowed(&plan, batch).await
    }

    pub(crate) async fn activate(
        &mut self,
        plan: commit_plan::StoreOperationCommitPlan,
        batch: commit_plan::StoreOperationBatch,
    ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, StoreError> {
        let prepared = self.prepare_candidate_borrowed(&plan, batch).await?;
        match self.publish_prepared(Box::new(prepared), None, None).await? {
            commit_plan::StoreOperationPublicationOutcome::Activated(reference) => Ok(reference),
            commit_plan::StoreOperationPublicationOutcome::Nonactivated(reference) => {
                Err(StoreError::InvalidOutbound(format!(
                    "Store operation candidate {} did not activate",
                    reference.commit_hash
                )))
            }
            commit_plan::StoreOperationPublicationOutcome::Reprepared => {
                Err(StoreError::InvalidOutbound(
                    "Store operation was reprepared during immediate activation".to_string(),
                ))
            }
            commit_plan::StoreOperationPublicationOutcome::RepreparedCandidate(_) => {
                Err(StoreError::InvalidOutbound(
                    "Store operation adopted a published head for a candidate composed in this call"
                        .to_string(),
                ))
            }
            commit_plan::StoreOperationPublicationOutcome::NonactivatedCandidate { .. } => {
                Err(StoreError::ActivationConflict)
            }
        }
    }

    pub(crate) async fn publish_prepared(
        &mut self,
        candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        membership_objects: Option<coven_database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<commit_plan::StoreOperationPublicationOutcome, StoreError> {
        let retained_operation_objects = candidate
            .commit
            .retained_operation_objects()
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let head = candidate.head.clone();
        let head_object = candidate.head_object.clone();
        let history_summary = candidate.history_summary.clone();
        self.publish(
            commit_plan::PreparedStoreOperationActivation {
                candidate,
                retained_operation_objects,
            },
            head,
            head_object,
            history_summary,
            membership_objects,
            membership_completion,
        )
        .await
    }

    pub(super) async fn publish(
        &mut self,
        mut activation: commit_plan::PreparedStoreOperationActivation,
        head: coven_protocol::store_commit::StoreDeviceHead,
        head_object: coven_protocol::objects::ExactObjectRef,
        history_summary: coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        membership_objects: Option<coven_database::VerifiedMergeMembershipObjects>,
        membership_completion: Option<
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        >,
    ) -> Result<commit_plan::StoreOperationPublicationOutcome, StoreError> {
        let database = self.database.clone();
        let root = self.store_root().clone();
        let reference = activation.candidate.reference.clone();
        let verified_commit = self
            .history
            .authenticate_commit_bytes(&reference, &activation.candidate.commit.to_bytes())
            .await?;
        let commit = verified_commit.value().clone();
        let circle_activations = if commit.control().is_some() {
            self.history
                .verify_membership_control(&verified_commit)
                .await
                .map_err(StoreError::InvalidOutbound)?
        } else {
            coven_protocol::circle_activation::VerifiedCircleActivations::none(&commit, &reference)
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
        };
        self.upload_commit(&activation.candidate).await?;
        let membership_heads = &commit.membership_state.heads;
        let authorization = self
            .history
            .authorize_retained_outbound(
                &commit.order,
                membership_heads,
                &commit.author_registration,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let device_operations = self
            .history
            .load_local_device_operations(
                &verified_commit,
                &authorization.membership,
                &authorization.device_state_ref,
                authorization.device_state,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let has_tracked_remote_objects =
            !activation.retained_operation_objects.is_empty() || membership_completion.is_some();
        if has_tracked_remote_objects {
            database
                .mark_candidate_commit_uploaded(reference.clone())
                .await
                .map_err(|error| {
                    StoreError::InvalidOutbound(format!("record uploaded Store candidate: {error}"))
                })?;
        }
        let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            commit.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = coven_protocol::store_commit::head_slot_prefix(
            &commit.author_registration.device_id.to_string(),
            commit.seq(),
        );
        // The head's bytes are what it serializes to, so the upload rebuilds
        // them under the object the candidate names; `new` re-checks them
        // against that reference before any of them leave this device.
        let prepared_head =
            coven_protocol::objects::PreparedExactObject::new(head_object.clone(), head.to_bytes())
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
        match self
            .storage
            .as_ref()
            .create_protocol_object(&prepared_head)
            .await
        {
            Ok(()) => {}
            Err(coven_protocol::objects::StorageError::SlotCollision(_)) => {
                return self
                    .resolve_head_collision(
                        activation.candidate,
                        verified_commit,
                        reference,
                        head,
                        head_object,
                        head_prefix,
                    )
                    .await;
            }
            Err(error) => {
                return Err(coven_protocol::objects::StoreObjectError::from(error).into());
            }
        }
        self.storage
            .verify_readback(&head_context, &head_object, &head_prefix, &head.to_bytes())
            .await
            .map_err(StoreError::readback)?;
        let activation_head = coven_protocol::store_commit::StoreDeviceHeadRef {
            head_hash: head.head_hash(),
            object: head_object.clone(),
        };
        let operation_object_ids = if has_tracked_remote_objects {
            database
                .mark_store_head_uploaded(activation_head.clone())
                .await
                .map_err(|error| {
                    StoreError::InvalidOutbound(format!("record uploaded Store head: {error}"))
                })?;
            membership_completion.is_none().then(|| {
                std::iter::once(coven_protocol::remote_object::remote_object_id(
                    &reference.object,
                ))
                .chain(
                    activation
                        .retained_operation_objects
                        .iter()
                        .map(coven_protocol::remote_object::remote_object_id),
                )
                .chain(std::iter::once(
                    coven_protocol::remote_object::remote_object_id(&head_object),
                ))
                .collect::<Vec<_>>()
            })
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
                || !completion_ids.contains(&coven_protocol::remote_object::remote_object_id(
                    &head_object,
                ))
            {
                return Err(StoreError::InvalidOutbound(
                    "membership journal completion does not cover its exact Store candidate"
                        .to_string(),
                ));
            }
        }
        let registrations = activation
            .candidate
            .registration_activation
            .take()
            .into_iter()
            .collect::<Vec<_>>();
        database
            .materialize_published_store_operation(
                root,
                verified_commit,
                registrations,
                device_operations,
                circle_activations,
                head,
                activation_head.object,
                history_summary,
                membership_objects,
                operation_object_ids,
                membership_completion,
            )
            .await?;
        Ok(commit_plan::StoreOperationPublicationOutcome::Activated(
            reference,
        ))
    }

    pub(super) async fn prepare_candidate_borrowed(
        &mut self,
        plan: &commit_plan::StoreOperationCommitPlan,
        batch: commit_plan::StoreOperationBatch,
    ) -> Result<commit_plan::PreparedStoreOperationCommit, StoreError> {
        let storage = self.storage.as_ref();
        let acknowledgement_evidence = match &batch {
            commit_plan::StoreOperationBatch::Acknowledgement {
                reference, value, ..
            } => Some((reference.clone(), value.clone())),
            _ => None,
        };
        let retained_registration_evidence = match &batch {
            commit_plan::StoreOperationBatch::Outcome {
                registration: Some(registration),
                ..
            } => vec![registration.registration().clone()],
            _ => Vec::new(),
        };
        let retained_device_operations = match &batch {
            commit_plan::StoreOperationBatch::DeviceExclusionProposal(proposal) => Some(
                coven_protocol::store_commit::RetainedStoreDeviceOperations::from_sources(
                    vec![proposal.clone()],
                    Vec::new(),
                ),
            ),
            commit_plan::StoreOperationBatch::DeviceExclusionOutcome(outcome) => Some(
                coven_protocol::store_commit::RetainedStoreDeviceOperations::from_sources(
                    Vec::new(),
                    vec![outcome.clone()],
                ),
            ),
            _ => None,
        };
        let (commit, registration_activation) =
            plan.sign_batch(self.database.new_store_write_id(), batch)?;
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
                plan.retain_acknowledgement(
                    &self.history,
                    &common.reference,
                    &common.commit,
                    reference,
                    value,
                )
                .await
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
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
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
            None => {
                coven_protocol::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
                    &common.commit,
                )
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?
            }
        };
        let state_after = Box::pin(self.history.derive_local_post_device_state(
            &common.commit,
            plan.predecessor_state().clone(),
            &registrations,
            device_operations,
        ))
        .await
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            common.commit.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let device_id = plan.device_id().to_string();
        let successor = self
            .history
            .prepare_merge_history_successor(
                &verified_commit,
                plan.membership(),
                None,
                state_after,
                merge_history_evidence,
            )
            .await
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let next_prefix = coven_protocol::store_commit::head_slot_prefix(
            &device_id,
            commit_plan::successor_store_sequence(common.commit.seq())?,
        );
        let next_slot = storage
            .allocate_protocol_slot(&head_context, &next_prefix, ".json")
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        let head = plan.sign_device_head(
            common.reference.clone(),
            successor.summary.digest(),
            coven_protocol::store_commit::SuccessorLink {
                activation: plan.announcement_activation_id()?,
                predecessor: successor.predecessor_head.map(|reference| reference.object),
                next_slot,
            },
        )?;
        let head_prefix =
            coven_protocol::store_commit::head_slot_prefix(&device_id, common.commit.seq());
        let prepared_head = storage
            .prepare_protocol_object(
                &head_context,
                successor.head_slot,
                &head_prefix,
                head.to_bytes(),
            )
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        Ok(commit_plan::PreparedStoreOperationCommit {
            common,
            head,
            head_object: prepared_head.reference().clone(),
            history_summary: successor.summary,
        })
    }

    pub(crate) async fn finish_nonactivating_acknowledgement(
        &self,
        acknowledgement: coven_protocol::store_commit::StoreAckRef,
    ) -> Result<(), StoreError> {
        let target = self
            .database
            .acknowledgement_cleanup_target(acknowledgement.clone())
            .await?;
        crate::sync::store::authorization::delete_candidate_cleanup_targets::<StoreError>(
            self.storage.as_ref(),
            &self.database,
            target,
        )
        .await?;
        self.database
            .complete_nonactivating_acknowledgement(acknowledgement)
            .await?;
        Ok(())
    }

    pub(super) async fn resolve_head_collision(
        &mut self,
        mut candidate: Box<commit_plan::PreparedStoreOperationCommit>,
        commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
        head: coven_protocol::store_commit::StoreDeviceHead,
        head_object: coven_protocol::objects::ExactObjectRef,
        head_prefix: String,
    ) -> Result<commit_plan::StoreOperationPublicationOutcome, StoreError> {
        let database = self.database.clone();
        let observation = self
            .history
            .merge_conflict()
            .observe_occupied_merge_head(&head, &commit, head_object.slot(), &head_prefix)
            .await?;
        if observation.winner().commit == reference {
            let (winner, winner_prepared) = observation.into_head();
            if let Some(acknowledgement) = commit.acknowledgement().cloned() {
                database
                    .adopt_acknowledgement_head(acknowledgement, winner, winner_prepared)
                    .await?;
                return Ok(commit_plan::StoreOperationPublicationOutcome::Reprepared);
            }
            candidate.adopt_merge_head(winner, winner_prepared.reference().clone())?;
            return Ok(
                commit_plan::StoreOperationPublicationOutcome::RepreparedCandidate(candidate),
            );
        }
        let registration = database
            .activated_store_device_registration(commit.author_registration.clone())
            .await?;
        let nonactivation = observation
            .verified_nonactivation(
                coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
                    coord: reference.coord.clone(),
                    object: reference.object.clone(),
                    canonical_signed_bytes: commit.to_bytes(),
                },
                registration.value(),
            )
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let Some(acknowledgement) = commit.acknowledgement().cloned() else {
            return Ok(
                commit_plan::StoreOperationPublicationOutcome::NonactivatedCandidate {
                    candidate,
                    nonactivation: Box::new(nonactivation),
                },
            );
        };
        database
            .begin_acknowledgement_nonactivation(acknowledgement.clone(), nonactivation)
            .await?;
        self.finish_nonactivating_acknowledgement(acknowledgement)
            .await?;
        Ok(commit_plan::StoreOperationPublicationOutcome::Nonactivated(
            reference,
        ))
    }
}
