use super::*;

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
    #[cfg(test)]
    pub(crate) async fn prepare_create_for_test(
        &mut self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<PreparedCircleJournal, CircleOperationError> {
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

    #[cfg(test)]
    pub(crate) async fn resign_merge_journal_with_reference_for_test(
        &mut self,
        journal: &mut CircleOperationJournal,
        reference: coven_protocol::store_commit::CircleControlRef,
        mutate_commit: impl FnOnce(&mut coven_protocol::store_commit::StoreBatchCommit),
    ) -> Result<(), CircleOperationError> {
        let old_commit = journal.commit()?;
        let coord = journal.operation().commit_ref().coord.clone();
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
            .prepare_circle_object_for_test(
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
            coord.clone(),
            commit_prepared.reference().clone(),
        )
        .map_err(CircleOperationError::from)?;
        let verified_commit = self.local_writer.verify_prepared_circle_commit(
            &commit.to_bytes(),
            commit.store_root_hash,
            coord,
            commit_prepared.reference().clone(),
        )?;
        let old_publication = journal.operation().store_commit.publication.clone();
        let previous = coven_database::ObservedStorePublication::from_parts(
            old_publication.previous.clone(),
            old_publication.previous_version.clone(),
        );
        let publication_entry = self
            .local_writer
            .sign_store_publication_entry(&previous, &verified_commit)
            .map_err(CircleOperationError::from)?;
        let publication_prepared = self
            .prepare_circle_object_for_test(
                &ProtocolObjectContext::signed_plaintext(
                    commit.store_root_hash,
                    ProtocolObjectDomain::StorePublicationEntry,
                ),
                &coven_protocol::store_commit::store_publication_entry_semantic_prefix(
                    &publication_entry,
                ),
                ".json",
                publication_entry.to_bytes(),
            )
            .await?;
        let publication_replacement = self
            .local_writer
            .advance_store_publication(
                &previous,
                &publication_entry,
                &publication_prepared,
                &verified_commit,
            )
            .map_err(CircleOperationError::from)?;
        // The commit spool must exist before its journal references it.
        self.database
            .install_payload_for_test(commit_prepared.stored_bytes().to_vec())
            .await
            .map_err(CircleOperationError::from)?;
        let operation = journal.operation_mut();
        operation.store_commit.common.commit = commit;
        operation.store_commit.common.reference = commit_ref;
        operation.store_commit.publication =
            coven_protocol::prepared_commit::PreparedStorePublication {
                previous: old_publication.previous,
                previous_version: old_publication.previous_version,
                entry: publication_entry,
                entry_object: publication_prepared.reference().clone(),
                replacement: publication_replacement,
            };
        operation.prepared_objects.insert(
            "store-commit".to_string(),
            commit_prepared.reference().clone(),
        );
        journal.uploaded.clear();
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
