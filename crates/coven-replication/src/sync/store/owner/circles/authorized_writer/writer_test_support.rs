use super::*;

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_create_for_test(
        &mut self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<CircleOperationJournal, CircleOperationError> {
        self.preparer().prepare_create(metadata_stamp, name).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn publish_prepared_operation_for_test(
        &mut self,
        operation_id: &coven_protocol::circle::CircleOperationId,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        self.publisher().publish(operation_id, routing_key).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_circle_object_for_test(
        &mut self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: Vec<u8>,
    ) -> Result<coven_protocol::objects::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object(context, semantic_prefix, extension, bytes)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn prepare_circle_object_at_for_test(
        &mut self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        bytes: Vec<u8>,
    ) -> Result<coven_protocol::objects::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object_at(context, slot, semantic_prefix, bytes)
    }

    #[cfg(test)]
    pub(crate) async fn resign_merge_journal_with_reference_for_test(
        &mut self,
        journal: &mut CircleOperationJournal,
        reference: coven_protocol::store_commit::CircleControlRef,
        mutate_commit: impl FnOnce(&mut coven_protocol::store_commit::StoreBatchCommit),
    ) -> Result<(), CircleOperationError> {
        let old_commit = journal.commit()?;
        let coord = journal.operation().commit_ref.coord.clone();
        let mut commit = self.local_writer.sign_circle_commit_for_test(
            &old_commit,
            coord.clone(),
            reference,
            old_commit.stream_activations().to_vec(),
        )?;
        mutate_commit(&mut commit);
        self.local_writer.resign_store_commit_for_test(&mut commit);
        let coven_protocol::store_commit::StoreCommitCoord { stream_id, .. } = coord.clone();
        let commit_prepared = self
            .preparer()
            .prepare_circle_object(
                &ProtocolObjectContext::signed_plaintext(
                    commit.store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                ),
                &coven_protocol::store_commit::commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    commit.seq(),
                    commit.commit_hash(),
                ),
                ".json",
                commit.to_bytes(),
            )
            .await?;
        let commit_ref = coven_protocol::store_commit::StoreBatchCommitRef::from_commit(
            &commit,
            coord,
            commit_prepared.reference().clone(),
        )
        .map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "build replacement Circle commit reference: {error}"
            ))
        })?;
        let old_head = journal.operation().policy.head.clone();
        let history_summary = journal.operation().policy.history_summary.clone();
        let head = self
            .local_writer
            .sign_device_head(
                commit.store_root_hash,
                commit_ref.clone(),
                history_summary.digest(),
                old_head.successor.clone(),
            )
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "sign replacement Circle Store head: {error}"
                ))
            })?;
        let head_slot = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle operation has no prepared Store head".to_string(),
                )
            })?
            .reference()
            .slot()
            .clone();
        let head_prepared = self.preparer().prepare_circle_object_at(
            &ProtocolObjectContext::signed_plaintext(
                commit.store_root_hash,
                ProtocolObjectDomain::StoreHead,
            ),
            head_slot,
            &coven_protocol::store_commit::head_slot_prefix(
                &commit.author_registration.device_id.to_string(),
                commit.seq(),
            ),
            head.to_bytes(),
        )?;
        let operation = journal.operation_mut();
        operation.commit_bytes = commit.to_bytes();
        operation.commit_ref = commit_ref;
        operation
            .prepared_objects
            .insert("store-commit".to_string(), commit_prepared);
        operation
            .prepared_objects
            .insert("store-head".to_string(), head_prepared);
        operation.policy = CircleOperationPolicy {
            head,
            history_summary,
        };
        operation.uploaded.clear();
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn sign_circle_commit_for_test(
        &self,
        old_commit: &coven_protocol::store_commit::StoreBatchCommit,
        coord: coven_protocol::store_commit::StoreCommitCoord,
        reference: coven_protocol::store_commit::CircleControlRef,
        stream_activations: Vec<coven_protocol::store_commit::StreamActivation>,
    ) -> Result<coven_protocol::store_commit::StoreBatchCommit, CircleOperationError> {
        self.local_writer.sign_circle_commit_for_test(
            old_commit,
            coord,
            reference,
            stream_activations,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_circle_activation_objects_for_test(
        &mut self,
        draft: coven_protocol::circle::CircleTransitionDraft,
        history: &CircleTransitionHistory,
        candidate_family: coven_protocol::store_commit::CandidateFamilyId,
    ) -> Result<
        (
            coven_protocol::circle::PreparedCircleTransition,
            coven_protocol::store_commit::CircleActivationObjects,
            std::collections::BTreeMap<String, coven_protocol::objects::PreparedExactObject>,
            Option<coven_protocol::objects::ExactObjectRef>,
            Vec<coven_protocol::store_commit::StreamActivation>,
        ),
        CircleOperationError,
    > {
        self.preparer()
            .prepare_circle_activation_objects(draft, history, &[], candidate_family)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn load_complete_circle_epoch_close_responses_for_test(
        &mut self,
        control: &coven_protocol::circle::PreparedCircleControl,
    ) -> Result<
        Option<
            Vec<(
                coven_protocol::circle::CircleEpochCloseSettlement,
                coven_protocol::circle::CircleEpochCloseResponseSlotValue,
            )>,
        >,
        CircleOperationError,
    > {
        self.load_complete_circle_epoch_close_responses(control)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn resolution_request_for_test(
        &self,
        circle_id: CircleId,
        chosen: &coven_protocol::circle::CircleControlCoord,
        conflicting_branches: Vec<coven_protocol::circle::CircleControlCoord>,
    ) -> Result<CircleOperationRequest, CircleOperationError> {
        let retained_branches = self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
            .ok_or(CircleOperationError::NotConflicted { circle_id })?;
        self.resolution_request(circle_id, chosen, &retained_branches, conflicting_branches)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn begin_circle_epoch_close_cancellation_for_test(
        &mut self,
        circle_id: CircleId,
    ) -> Result<coven_protocol::circle::CircleOperationId, CircleOperationError> {
        self.begin_circle_epoch_close_cancellation(circle_id).await
    }
}
