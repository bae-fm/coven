use super::commands::{
    CircleAddMemberRequest, CircleCancelEpochCloseRequest, CircleDeleteRequest,
    CircleFinalizeEpochCloseRequest, CircleOperationRequest, CircleRemoveMemberRequest,
    CircleRenameRequest, CircleResolveControlRequest, CircleResolveLosingBranch,
};
use super::*;
use crate::protocol::circle::{
    circle_epoch_close_response_semantic_prefix, CircleControlState, CircleEpochCloseExclusionRef,
    CircleEpochCloseResponseRef, CircleEpochCloseResponseSlotValue, CircleEpochCloseSettlement,
    CircleId, CircleRole, PreparedCircleControl,
};
use crate::protocol::objects::{
    ProtocolObjectContext, ProtocolObjectDomain, StorageError, StoreObjectError,
};
use crate::protocol::store_commit::CommitFrontier;

pub(crate) struct AuthorizedCircleWriter<'writer, 'storage> {
    writer: &'writer mut AuthorizedWriterOperation<'storage>,
    database: crate::database::StoreDatabase,
    storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
    store_dir: &'storage crate::store_dir::StoreDir,
    root: crate::protocol::store_commit::StoreRootRef,
    membership: crate::protocol::membership::MembershipChain,
    local_writer: std::sync::Arc<crate::sync::store::owner::writer::LocalStoreWriter>,
}

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        writer: &'writer mut AuthorizedWriterOperation<'storage>,
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
        store_dir: &'storage crate::store_dir::StoreDir,
        root: crate::protocol::store_commit::StoreRootRef,
        membership: crate::protocol::membership::MembershipChain,
        local_writer: std::sync::Arc<crate::sync::store::owner::writer::LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            store_dir,
            root,
            membership,
            local_writer,
        }
    }

    pub(super) fn publisher(&mut self) -> publication::CircleCandidatePublisher<'_, 'storage> {
        let database = self.database.clone();
        let storage = self.storage.clone();
        let membership = self.membership.clone();
        let history = self.writer.circle_history();
        publication::CircleCandidatePublisher::new(
            database,
            storage,
            membership,
            std::sync::Arc::clone(&self.local_writer),
            history,
        )
    }

    pub(super) fn preparer(&mut self) -> preparation::CircleCandidatePreparer<'_, 'storage> {
        let announcement_stream_id = self.writer.announcement_stream_id();
        let database = self.database.clone();
        let membership = self.membership.clone();
        let root = self.root.clone();
        let storage = self.storage.clone();
        let history = self.writer.circle_history();
        preparation::CircleCandidatePreparer::new(
            announcement_stream_id,
            database,
            membership,
            root,
            storage,
            std::sync::Arc::clone(&self.local_writer),
            history,
        )
    }

    #[cfg(test)]
    pub(crate) async fn prepare_create_for_test(
        &mut self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<CircleOperationJournal, CircleOperationError> {
        self.preparer().prepare_create(metadata_stamp, name).await
    }

    #[cfg(test)]
    pub(crate) async fn publish_prepared_operation_for_test(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        self.publisher().publish(operation_id, routing_key).await
    }

    #[cfg(test)]
    pub(crate) async fn prepare_circle_object_for_test(
        &mut self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: Vec<u8>,
    ) -> Result<crate::protocol::objects::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object(context, semantic_prefix, extension, bytes)
            .await
    }

    #[cfg(test)]
    pub(crate) fn prepare_circle_object_at_for_test(
        &mut self,
        context: &crate::protocol::objects::ProtocolObjectContext,
        slot: crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        bytes: Vec<u8>,
    ) -> Result<crate::protocol::objects::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object_at(context, slot, semantic_prefix, bytes)
    }

    #[cfg(test)]
    pub(crate) async fn resign_merge_journal_with_reference_for_test(
        &mut self,
        journal: &mut CircleOperationJournal,
        reference: crate::protocol::store_commit::CircleControlRef,
        mutate_commit: impl FnOnce(&mut crate::protocol::store_commit::StoreBatchCommit),
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
        let crate::protocol::store_commit::StoreCommitCoord { stream_id, .. } = coord.clone();
        let commit_prepared = self
            .preparer()
            .prepare_circle_object(
                &ProtocolObjectContext::signed_plaintext(
                    commit.store_root_hash,
                    ProtocolObjectDomain::StoreCommit,
                ),
                &crate::protocol::store_commit::commit_semantic_prefix(
                    commit.candidate_family(),
                    &stream_id.to_string(),
                    commit.seq(),
                    commit.commit_hash(),
                ),
                ".json",
                commit.to_bytes(),
            )
            .await?;
        let commit_ref = crate::protocol::store_commit::StoreBatchCommitRef::from_commit(
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
                old_head.successor,
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
            &crate::protocol::store_commit::head_slot_prefix(
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

    #[cfg(test)]
    pub(crate) fn sign_circle_commit_for_test(
        &self,
        old_commit: &crate::protocol::store_commit::StoreBatchCommit,
        coord: crate::protocol::store_commit::StoreCommitCoord,
        reference: crate::protocol::store_commit::CircleControlRef,
        stream_activations: Vec<crate::protocol::store_commit::StreamActivation>,
    ) -> Result<crate::protocol::store_commit::StoreBatchCommit, CircleOperationError> {
        self.local_writer.sign_circle_commit_for_test(
            old_commit,
            coord,
            reference,
            stream_activations,
        )
    }

    #[cfg(test)]
    pub(crate) async fn prepare_circle_activation_objects_for_test(
        &mut self,
        draft: crate::protocol::circle::CircleTransitionDraft,
        history: &CircleTransitionHistory,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
    ) -> Result<
        (
            crate::protocol::circle::PreparedCircleTransition,
            crate::protocol::store_commit::CircleActivationObjects,
            std::collections::BTreeMap<String, crate::protocol::objects::PreparedExactObject>,
            Option<crate::protocol::objects::ExactObjectRef>,
            Vec<crate::protocol::store_commit::StreamActivation>,
        ),
        CircleOperationError,
    > {
        self.preparer()
            .prepare_circle_activation_objects(draft, history, &[], candidate_family)
            .await
    }

    pub(crate) async fn publish_circle_epoch_close_responses(
        &mut self,
    ) -> Result<(), CircleOperationError> {
        let controls = self.database.closing_circle_controls().await?;
        if controls.is_empty() {
            return Ok(());
        }
        let root = self.root.clone();
        let storage = self.storage.clone();
        let frontier = CommitFrontier::from_refs(self.database.materialized_frontier().await?)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        for control in controls {
            let CircleControlState::EpochClose(close) = control.value.state() else {
                return Err(CircleOperationError::InvalidState(
                    "closing Circle state contains an active control".to_string(),
                ));
            };
            let Some(participant) = self.local_writer.local_circle_close_participant(close) else {
                tracing::debug!(
                    circle_id = %control.value.circle_id,
                    close_id = %close.close_id,
                    "local device is not a participant in the Circle epoch close"
                );
                continue;
            };
            let response = self
                .local_writer
                .sign_circle_epoch_close_response(&control, frontier.clone())?;
            let response_device_id = response.registration.device_id;
            let prefix = circle_epoch_close_response_semantic_prefix(
                control.value.circle_id,
                close.close_id,
                response_device_id,
            );
            let context = ProtocolObjectContext::store_encrypted(
                root.store_root_hash,
                ProtocolObjectDomain::CircleEpochCloseResponse,
            );
            let prepared = storage
                .prepare_protocol_object(
                    &context,
                    participant.response_slot.clone(),
                    &prefix,
                    CircleEpochCloseResponseSlotValue::Response(response).to_bytes(),
                )
                .map_err(StoreObjectError::from)?;
            match storage.create_protocol_object(&prepared).await {
                Ok(()) | Err(StorageError::SlotCollision(_)) => {}
                Err(error) => return Err(StoreObjectError::from(error).into()),
            }
            let (winner_bytes, _) = storage
                .read_prepared_protocol_slot(&context, &participant.response_slot, &prefix)
                .await
                .map_err(StoreObjectError::from)?;
            match CircleEpochCloseResponseSlotValue::parse(&winner_bytes)? {
                CircleEpochCloseResponseSlotValue::Response(winner) => {
                    if !self
                        .local_writer
                        .verify_local_circle_epoch_close_response(&winner, &control)
                    {
                        return Err(CircleOperationError::InvalidState(format!(
                            "Circle epoch-close response slot for device {} holds an unverifiable response",
                            response_device_id
                        )));
                    }
                }
                CircleEpochCloseResponseSlotValue::Exclusion(exclusion) => {
                    if !exclusion.verify_for(&control) {
                        return Err(CircleOperationError::InvalidState(format!(
                            "Circle epoch-close exclusion for device {} holds an unverifiable exclusion",
                            response_device_id
                        )));
                    }
                    tracing::debug!(
                        circle_id = %control.value.circle_id,
                        close_id = %close.close_id,
                        device_id = %response_device_id,
                        "local device was excluded from the Circle epoch close before it responded"
                    );
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn finalize_ready_circle_epoch_closes(
        &mut self,
        metadata_stamp: &str,
        routing_encryption: &crate::encryption::EncryptionService,
    ) -> Result<(), CircleOperationError> {
        let journals = self.database.waiting_circle_operations().await?;
        if journals.is_empty() {
            return Ok(());
        }
        for mut journal in journals {
            let member_pubkey = match &journal.intent {
                CircleOperationIntent::RemoveMember { member_pubkey } => member_pubkey.clone(),
                _ => {
                    return Err(CircleOperationError::Journal(format!(
                        "Circle operation {} waits for close responses without a removal intent",
                        journal.operation_id
                    )));
                }
            };
            let identity_pubkey = self.local_writer.author_pubkey();
            let (current, activation_commit_ref) = self
                .database
                .circle_closing_context(journal.circle_id, &identity_pubkey)
                .await?;
            let CircleControlState::EpochClose(close) = current.control.value.state() else {
                return Err(CircleOperationError::InvalidState(
                    "Circle close-finalization context is active".to_string(),
                ));
            };
            if close.close_id
                != crate::protocol::circle::CircleEpochCloseId::from_operation_id(
                    &journal.operation_id,
                )
            {
                return Err(CircleOperationError::Journal(format!(
                    "Circle operation {} differs from its close id",
                    journal.operation_id
                )));
            }
            let Some(settlements) = self
                .load_complete_circle_epoch_close_responses(&current.control)
                .await?
            else {
                continue;
            };
            let cutoff = settlements
                .iter()
                .filter_map(|(settlement, _)| settlement.response_frontier())
                .try_fold(close.provisional_frontier.clone(), |cutoff, frontier| {
                    cutoff
                        .join(frontier.clone())
                        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))
                })?;
            let bootstrap = self
                .snapshots()
                .capture_circle_snapshot_at_cutoff(routing_encryption, journal.circle_id, cutoff)
                .await?;
            let activation = self
                .history()
                .load_commit(&activation_commit_ref)
                .await
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            let activation_commit = activation.value();
            if activation_commit.candidate_family() != current.candidate_family {
                return Err(CircleOperationError::InvalidState(format!(
                    "Circle {} closing state differs from its activating Store commit",
                    journal.circle_id
                )));
            }
            let reference = activation_commit
                .circle_controls()
                .iter()
                .find(|reference| {
                    reference.circle_id() == journal.circle_id
                        && reference.control() == &current.control.coord
                })
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle {} closing control is absent from its activating Store commit",
                        journal.circle_id
                    ))
                })?;
            let keyring = match &current.access.disposition {
                crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
                crate::protocol::circle::CircleAccessDisposition::Inactive => {
                    return Err(CircleOperationError::InvalidState(
                        "Circle close finalization lost its retained keyring".to_string(),
                    ));
                }
            };
            let roster_chain = self
                .history()
                .activations()
                .load_control_roster_chain(&activation, reference, &current.control, keyring)
                .await?;
            let intent = journal
                .operation()
                .creation
                .close_intent
                .clone()
                .ok_or_else(|| {
                    CircleOperationError::Journal(format!(
                        "Circle operation {} lost its close intent",
                        journal.operation_id
                    ))
                })?;
            let prepared = self
                .preparer()
                .prepare_request(CircleOperationRequest::FinalizeEpochClose(Box::new(
                    CircleFinalizeEpochCloseRequest {
                        operation_id: journal.operation_id.clone(),
                        circle_id: journal.circle_id,
                        member_pubkey,
                        metadata_stamp: metadata_stamp.to_string(),
                        current,
                        previous_control: reference.clone(),
                        roster_chain,
                        intent,
                        responses: settlements
                            .into_iter()
                            .map(|(settlement, _)| settlement)
                            .collect(),
                        bootstrap,
                    },
                )))
                .await?;
            if prepared.operation_id != journal.operation_id
                || prepared.circle_id != journal.circle_id
                || prepared.intent != journal.intent
            {
                return Err(CircleOperationError::Journal(format!(
                    "Circle operation {} finalization changed its durable identity",
                    journal.operation_id
                )));
            }
            journal.begin_finalization(prepared.operation().clone())?;
            self.database
                .begin_circle_operation_finalization(journal.clone())
                .await?;
            let routing_key = crate::protocol::circle::derive_row_routing_key(
                routing_encryption,
                self.root.store_root_hash,
            )
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
            self.publisher()
                .publish(&journal.operation_id, Some(&routing_key))
                .await?;
        }
        Ok(())
    }

    async fn load_complete_circle_epoch_close_responses(
        &self,
        control: &PreparedCircleControl,
    ) -> Result<
        Option<
            Vec<(
                CircleEpochCloseSettlement,
                CircleEpochCloseResponseSlotValue,
            )>,
        >,
        CircleOperationError,
    > {
        let CircleControlState::EpochClose(close) = control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle close-response collection received an active control".to_string(),
            ));
        };
        let context = ProtocolObjectContext::store_encrypted(
            control.value.store_root_hash,
            ProtocolObjectDomain::CircleEpochCloseResponse,
        );
        let mut settlements = Vec::with_capacity(close.participants.len());
        for participant in &close.participants {
            let prefix = circle_epoch_close_response_semantic_prefix(
                control.value.circle_id,
                close.close_id,
                participant.registration.device_id,
            );
            let (bytes, object) = match self
                .storage
                .read_protocol_slot(&context, &participant.response_slot, &prefix)
                .await
            {
                Ok(response) => response,
                Err(StorageError::NotFound(_)) => return Ok(None),
                Err(error) => return Err(StoreObjectError::from(error).into()),
            };
            let slot_value = CircleEpochCloseResponseSlotValue::parse(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "Circle epoch-close response slot for device {} failed to parse: {error}",
                    participant.registration.device_id
                ))
            })?;
            let settlement = match &slot_value {
                CircleEpochCloseResponseSlotValue::Response(response) => {
                    let registration = self
                        .database
                        .activated_store_device_registration(participant.registration.clone())
                        .await?;
                    if !response.verify_for(control, registration.value()) {
                        return Err(CircleOperationError::InvalidState(format!(
                            "Circle epoch-close response from device {} failed verification",
                            participant.registration.device_id
                        )));
                    }
                    CircleEpochCloseSettlement::Response(
                        CircleEpochCloseResponseRef::from_response(response, object).map_err(
                            |error| {
                                CircleOperationError::InvalidState(format!(
                                    "Circle epoch-close response from device {} has an invalid exact reference: {error}",
                                    participant.registration.device_id
                                ))
                            },
                        )?,
                    )
                }
                CircleEpochCloseResponseSlotValue::Exclusion(exclusion) => {
                    if !exclusion.verify_for(control)
                        || exclusion.excluded != participant.registration
                    {
                        return Err(CircleOperationError::InvalidState(format!(
                            "Circle epoch-close exclusion for device {} failed verification",
                            participant.registration.device_id
                        )));
                    }
                    CircleEpochCloseSettlement::Exclusion(
                        CircleEpochCloseExclusionRef::from_exclusion(exclusion, object).map_err(
                            |error| {
                                CircleOperationError::InvalidState(format!(
                                    "Circle epoch-close exclusion for device {} has an invalid exact reference: {error}",
                                    participant.registration.device_id
                                ))
                            },
                        )?,
                    )
                }
            };
            settlements.push((settlement, slot_value));
        }
        Ok(Some(settlements))
    }

    #[cfg(test)]
    pub(crate) async fn load_complete_circle_epoch_close_responses_for_test(
        &mut self,
        control: &crate::protocol::circle::PreparedCircleControl,
    ) -> Result<
        Option<
            Vec<(
                crate::protocol::circle::CircleEpochCloseSettlement,
                crate::protocol::circle::CircleEpochCloseResponseSlotValue,
            )>,
        >,
        CircleOperationError,
    > {
        self.load_complete_circle_epoch_close_responses(control)
            .await
    }

    pub(crate) async fn stage_acknowledgements(
        &self,
        frontier: &crate::protocol::store_commit::CommitFrontier,
        sync_time: &str,
    ) -> Result<(), StoreAckError> {
        let inputs = self
            .database
            .circle_acknowledgement_publication_inputs()
            .await?;
        if inputs.is_empty() {
            return Ok(());
        }
        for input in inputs {
            let previous = self
                .database
                .latest_published_circle_ack(input.circle_id)
                .await?;
            if previous.as_ref().is_some_and(|previous| {
                &previous.store_cut == frontier && previous.control == input.control
            }) {
                tracing::debug!(
                    circle_id = %input.circle_id,
                    "skip Circle acknowledgement: accepted frontier and control unchanged"
                );
                continue;
            }
            let (sequence, predecessor) = match &previous {
                Some(previous) => (
                    previous.reference.sequence.checked_add(1).ok_or_else(|| {
                        StoreAckError::InvalidOutbound(
                            "Circle acknowledgement sequence overflow".to_string(),
                        )
                    })?,
                    Some(previous.reference.object.clone()),
                ),
                None => (1, None),
            };
            let context = input.access.protocol_context(
                self.root.store_root_hash,
                ProtocolObjectDomain::CircleAcknowledgement,
            );
            let semantic_prefix = self
                .local_writer
                .circle_ack_semantic_prefix(input.circle_id, sequence);
            let current_slot = match &previous {
                Some(previous) => previous.successor_slot.clone(),
                None => self
                    .storage
                    .allocate_protocol_slot(&context, &semantic_prefix, ".json")
                    .await
                    .map_err(StoreObjectError::from)?,
            };
            let next_slot = self
                .storage
                .allocate_protocol_slot(
                    &context,
                    &self.local_writer.circle_ack_semantic_prefix(
                        input.circle_id,
                        sequence.checked_add(1).ok_or_else(|| {
                            StoreAckError::InvalidOutbound(
                                "Circle acknowledgement sequence overflow".to_string(),
                            )
                        })?,
                    ),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let acknowledgement = self
                .local_writer
                .sign_circle_acknowledgement(
                    self.root.store_root_hash,
                    input.circle_id,
                    sequence,
                    frontier.clone(),
                    input.control,
                    input.epoch_id,
                    input.access.key_fingerprint(),
                    input.seeded_from,
                    sync_time.to_owned(),
                    predecessor,
                    next_slot,
                )
                .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
            let prepared = self
                .storage
                .prepare_protocol_object(
                    &context,
                    current_slot,
                    &semantic_prefix,
                    acknowledgement.to_bytes(),
                )
                .map_err(StoreObjectError::from)?;
            self.database
                .stage_circle_ack(acknowledgement, prepared)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn publish_acknowledgement_objects(
        &self,
        outbound: &crate::database::OutboundStoreAck,
        candidate: &crate::sync::store::operations::PreparedStoreOperationCommit,
    ) -> Result<(), StoreAckError> {
        for circle in &outbound.circle_acknowledgements {
            if let Err(error) = self
                .storage
                .create_protocol_object(&circle.ack.prepared)
                .await
            {
                if matches!(
                    error,
                    crate::protocol::objects::StorageError::SlotCollision(_)
                ) {
                    return Err(StoreAckError::InvalidOutbound(format!(
                        "Circle acknowledgement slot {} holds different bytes",
                        circle.reference.object.slot().logical_key()
                    )));
                }
                return Err(StoreObjectError::from(error).into());
            }
            let remote = candidate
                .circle_acknowledgement_remote_objects(&circle.ack)?
                .into_iter()
                .find(|remote| remote.object() == &circle.reference.object)
                .ok_or_else(|| {
                    StoreAckError::InvalidOutbound(
                        "prepared activation does not own its Circle acknowledgement object"
                            .to_string(),
                    )
                })?;
            self.database.mark_remote_object_uploaded(remote).await?;
        }
        Ok(())
    }

    pub(crate) fn snapshots(&mut self) -> snapshots::CircleSnapshotWriter<'_, 'storage> {
        snapshots::CircleSnapshotWriter::new(
            self.writer,
            self.database.clone(),
            self.storage.clone(),
            self.store_dir,
            self.root.clone(),
            std::sync::Arc::clone(&self.local_writer),
        )
    }

    fn history(&mut self) -> VerifiedCircleHistory<'_, 'storage> {
        self.writer.circle_history()
    }

    /// A deleted Circle is terminal: every lifecycle command refuses it with a
    /// typed reason rather than a generic missing-authoring-state error.
    async fn ensure_not_deleted(&self, circle_id: CircleId) -> Result<(), CircleOperationError> {
        if self.database.circle_is_deleted(circle_id).await? {
            return Err(CircleOperationError::Deleted { circle_id });
        }
        Ok(())
    }

    async fn current_authoring_context(
        &mut self,
        circle_id: CircleId,
    ) -> Result<
        (
            CircleAuthoringState,
            crate::protocol::store_commit::VerifiedStoreBatchCommit,
            crate::protocol::store_commit::CircleControlRef,
        ),
        CircleOperationError,
    > {
        self.ensure_not_deleted(circle_id).await?;
        let identity_pubkey = self.local_writer.author_pubkey();
        let (current, activation_commit_ref) = self
            .database
            .circle_authoring_context(circle_id, &identity_pubkey)
            .await?;
        let activation_commit = self
            .history()
            .load_commit(&activation_commit_ref)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if activation_commit.value().candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .value()
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .cloned()
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        Ok((current, activation_commit, reference))
    }

    async fn current_delete_context(
        &mut self,
        circle_id: CircleId,
    ) -> Result<
        (
            CircleAuthoringState,
            crate::protocol::store_commit::CircleControlRef,
        ),
        CircleOperationError,
    > {
        let identity_pubkey = self.local_writer.author_pubkey();
        let (current, activation_commit_ref) = self
            .database
            .circle_delete_context(circle_id, &identity_pubkey)
            .await?;
        let activation_commit = self
            .history()
            .load_commit(&activation_commit_ref)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        if activation_commit.value().candidate_family() != current.candidate_family {
            return Err(CircleOperationError::InvalidState(format!(
                "Circle {circle_id} current state differs from its activating Store commit"
            )));
        }
        let reference = activation_commit
            .value()
            .circle_controls()
            .iter()
            .find(|reference| {
                reference.circle_id() == circle_id && reference.control() == &current.control.coord
            })
            .cloned()
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} current control is absent from its activating Store commit"
                ))
            })?;
        Ok((current, reference))
    }

    pub(crate) async fn create_circle(
        &mut self,
        metadata_stamp: &str,
        name: &str,
    ) -> Result<CircleId, CircleOperationError> {
        let journal = self.preparer().prepare_create(metadata_stamp, name).await?;
        let circle_id = journal.circle_id();
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await?;
        Ok(circle_id)
    }

    pub(crate) async fn rename_circle(
        &mut self,
        metadata_stamp: &str,
        circle_id: CircleId,
        name: &str,
    ) -> Result<(), CircleOperationError> {
        let (current, _, reference) = self.current_authoring_context(circle_id).await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::Rename(Box::new(
                CircleRenameRequest {
                    circle_id,
                    name: name.to_string(),
                    metadata_stamp: metadata_stamp.to_string(),
                    current,
                    previous_control: reference,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle rename changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await
    }

    pub(crate) async fn remove_circle_member(
        &mut self,
        circle_id: CircleId,
        member_pubkey: String,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        let (current, activation_commit, reference) =
            self.current_authoring_context(circle_id).await?;
        let keyring = match &current.access.disposition {
            crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::protocol::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member removal requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = self
            .history()
            .activations()
            .load_control_roster_chain(&activation_commit, &reference, &current.control, keyring)
            .await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::RemoveMember(Box::new(
                CircleRemoveMemberRequest {
                    circle_id,
                    member_pubkey,
                    current,
                    previous_control: reference,
                    roster_chain,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member removal changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await?;
        Ok(operation_id)
    }

    pub(crate) async fn resolve_circle_control(
        &mut self,
        circle_id: CircleId,
        chosen: crate::protocol::circle::CircleControlCoord,
    ) -> Result<(), CircleOperationError> {
        let branches = self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
            .ok_or(CircleOperationError::NotConflicted { circle_id })?;
        let request = self
            .resolution_request(circle_id, &chosen, &branches, branches.clone())
            .await?;
        let journal = self.preparer().prepare_request(request).await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle control resolution changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher().publish(&operation_id, None).await
    }

    async fn resolution_request(
        &self,
        circle_id: CircleId,
        chosen: &crate::protocol::circle::CircleControlCoord,
        retained_branches: &[crate::protocol::circle::CircleControlCoord],
        conflicting_branches: Vec<crate::protocol::circle::CircleControlCoord>,
    ) -> Result<CircleOperationRequest, CircleOperationError> {
        self.ensure_not_deleted(circle_id).await?;
        if !retained_branches.contains(chosen) {
            return Err(CircleOperationError::ChosenBranchNotRetained { circle_id });
        }
        let identity_pubkey = self.local_writer.author_pubkey();
        let chosen_activation = self
            .database
            .verified_circle_activation(self.root.clone(), circle_id, chosen.clone())
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle {circle_id} conflict omits retained authority for the chosen branch"
                ))
            })?;
        if matches!(
            chosen_activation.control.value.state(),
            CircleControlState::EpochClose(_)
        ) {
            return Err(CircleOperationError::ResolveToClosingBranch { circle_id });
        }
        let chosen_state =
            retained_branch_authoring_state(circle_id, &identity_pubkey, &chosen_activation)?;
        let previous_control = chosen_activation.reference.clone();
        let mut losing_branches = Vec::new();
        for branch in retained_branches {
            if branch == chosen {
                continue;
            }
            let activation = self
                .database
                .verified_circle_activation(self.root.clone(), circle_id, branch.clone())
                .await?
                .ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle {circle_id} conflict omits retained authority for a losing branch"
                    ))
                })?;
            let selected_metadata = losing_branch_selected_metadata(circle_id, &activation)?;
            losing_branches.push(CircleResolveLosingBranch {
                reference: activation.reference.clone(),
                selected_metadata,
            });
        }
        Ok(CircleOperationRequest::ResolveControl(Box::new(
            CircleResolveControlRequest {
                circle_id,
                chosen: chosen_state,
                previous_control,
                losing_branches,
                conflicting_branches,
            },
        )))
    }

    #[cfg(test)]
    pub(super) async fn resolution_request_for_test(
        &self,
        circle_id: CircleId,
        chosen: &crate::protocol::circle::CircleControlCoord,
        conflicting_branches: Vec<crate::protocol::circle::CircleControlCoord>,
    ) -> Result<CircleOperationRequest, CircleOperationError> {
        let retained_branches = self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
            .ok_or(CircleOperationError::NotConflicted { circle_id })?;
        self.resolution_request(circle_id, chosen, &retained_branches, conflicting_branches)
            .await
    }

    pub(crate) async fn cancel_circle_epoch_close(
        &mut self,
        circle_id: CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        let operation_id = self
            .begin_circle_epoch_close_cancellation(circle_id)
            .await?;
        self.publisher().publish(&operation_id, None).await?;
        Ok(operation_id)
    }

    async fn begin_circle_epoch_close_cancellation(
        &mut self,
        circle_id: CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        self.ensure_not_deleted(circle_id).await?;
        let identity_pubkey = self.local_writer.author_pubkey();
        let mut journal = self
            .database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|journal| journal.circle_id() == circle_id)
            .ok_or(CircleOperationError::NoCloseToCancel { circle_id })?;
        let member_pubkey = match &journal.intent {
            CircleOperationIntent::RemoveMember { member_pubkey } => member_pubkey.clone(),
            _ => {
                return Err(CircleOperationError::Journal(format!(
                    "Circle operation {} waits for close responses without a removal intent",
                    journal.operation_id
                )));
            }
        };
        let close_id =
            crate::protocol::circle::CircleEpochCloseId::from_operation_id(&journal.operation_id);
        let (current, reference) = match self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
        {
            None => {
                let (current, _) = self
                    .database
                    .circle_closing_context(circle_id, &identity_pubkey)
                    .await?;
                let CircleControlState::EpochClose(close) = current.control.value.state() else {
                    return Err(CircleOperationError::InvalidState(
                        "Circle close-cancellation context is active".to_string(),
                    ));
                };
                if close.close_id != close_id {
                    return Err(CircleOperationError::Journal(format!(
                        "Circle operation {} differs from its close id",
                        journal.operation_id
                    )));
                }
                let activation = self
                    .database
                    .verified_circle_activation(
                        self.root.clone(),
                        circle_id,
                        current.control.coord.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(format!(
                            "Circle {circle_id} closing control has no retained activation"
                        ))
                    })?;
                if activation.control != current.control {
                    return Err(CircleOperationError::InvalidState(format!(
                        "Circle {circle_id} closing state differs from its retained activation"
                    )));
                }
                (current, activation.reference)
            }
            Some(branches) => {
                let mut selected = None;
                for branch in branches {
                    let activation = self
                        .database
                        .verified_circle_activation(self.root.clone(), circle_id, branch)
                        .await?
                        .ok_or_else(|| {
                            CircleOperationError::InvalidState(format!(
                                "Circle {circle_id} conflict omits a retained branch activation"
                            ))
                        })?;
                    let CircleControlState::EpochClose(close) = activation.control.value.state()
                    else {
                        continue;
                    };
                    if close.close_id != close_id {
                        continue;
                    }
                    let current =
                        retained_branch_authoring_state(circle_id, &identity_pubkey, &activation)?;
                    if selected
                        .replace((current, activation.reference.clone()))
                        .is_some()
                    {
                        return Err(CircleOperationError::InvalidState(format!(
                            "Circle {circle_id} conflict repeats close {close_id}"
                        )));
                    }
                }
                selected.ok_or_else(|| {
                    CircleOperationError::InvalidState(format!(
                        "Circle {circle_id} conflict does not retain local close {close_id}"
                    ))
                })?
            }
        };
        let CircleControlState::EpochClose(close) = current.control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle close-cancellation context is active".to_string(),
            ));
        };
        if close.close_id
            != crate::protocol::circle::CircleEpochCloseId::from_operation_id(&journal.operation_id)
        {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} differs from its close id",
                journal.operation_id
            )));
        }
        let prepared = self
            .preparer()
            .prepare_request(CircleOperationRequest::CancelEpochClose(Box::new(
                CircleCancelEpochCloseRequest {
                    operation_id: journal.operation_id.clone(),
                    circle_id,
                    member_pubkey,
                    current,
                    previous_control: reference,
                },
            )))
            .await?;
        if prepared.operation_id != journal.operation_id
            || prepared.circle_id != circle_id
            || prepared.intent != journal.intent
        {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} cancellation changed its durable identity",
                journal.operation_id
            )));
        }
        journal.begin_finalization(prepared.operation().clone())?;
        self.database
            .begin_circle_operation_finalization(journal.clone())
            .await?;
        Ok(journal.operation_id)
    }

    #[cfg(test)]
    pub(crate) async fn begin_circle_epoch_close_cancellation_for_test(
        &mut self,
        circle_id: CircleId,
    ) -> Result<crate::protocol::circle::CircleOperationId, CircleOperationError> {
        self.begin_circle_epoch_close_cancellation(circle_id).await
    }

    pub(crate) async fn exclude_circle_close_device(
        &mut self,
        circle_id: CircleId,
        excluded_device_id: crate::protocol::store_commit::StoreDeviceId,
    ) -> Result<(), CircleOperationError> {
        let identity_pubkey = self.local_writer.author_pubkey();
        let journal = self
            .database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|journal| journal.circle_id() == circle_id)
            .ok_or(CircleOperationError::NoCloseToExclude { circle_id })?;
        let (current, _) = self
            .database
            .circle_closing_context(circle_id, &identity_pubkey)
            .await?;
        let CircleControlState::EpochClose(close) = current.control.value.state() else {
            return Err(CircleOperationError::InvalidState(
                "Circle device-exclusion context is active".to_string(),
            ));
        };
        if close.close_id
            != crate::protocol::circle::CircleEpochCloseId::from_operation_id(&journal.operation_id)
        {
            return Err(CircleOperationError::Journal(format!(
                "Circle operation {} differs from its close id",
                journal.operation_id
            )));
        }
        let participant = close
            .participants
            .iter()
            .find(|participant| participant.registration.device_id == excluded_device_id)
            .ok_or(CircleOperationError::DeviceNotACloseParticipant {
                circle_id,
                device_id: excluded_device_id,
            })?;
        let exclusion = self.local_writer.sign_circle_epoch_close_exclusion(
            &current.control,
            participant.registration.clone(),
        )?;
        let prefix = crate::protocol::circle::circle_epoch_close_response_semantic_prefix(
            circle_id,
            close.close_id,
            excluded_device_id,
        );
        let context = ProtocolObjectContext::store_encrypted(
            current.control.value.store_root_hash,
            ProtocolObjectDomain::CircleEpochCloseResponse,
        );
        let prepared = self
            .storage
            .prepare_protocol_object(
                &context,
                participant.response_slot.clone(),
                &prefix,
                CircleEpochCloseResponseSlotValue::Exclusion(exclusion).to_bytes(),
            )
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        match self.storage.create_protocol_object(&prepared).await {
            Ok(()) | Err(StorageError::SlotCollision(_)) => {}
            Err(error) => {
                return Err(crate::protocol::objects::StoreObjectError::from(error).into())
            }
        }
        let (winner_bytes, _) = self
            .storage
            .read_prepared_protocol_slot(&context, &participant.response_slot, &prefix)
            .await
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        match CircleEpochCloseResponseSlotValue::parse(&winner_bytes)? {
            CircleEpochCloseResponseSlotValue::Exclusion(winner) => {
                if !winner.verify_for(&current.control) {
                    return Err(CircleOperationError::InvalidState(
                        "published Circle epoch-close exclusion failed verification".to_string(),
                    ));
                }
            }
            CircleEpochCloseResponseSlotValue::Response(response) => {
                let registration = self
                    .database
                    .activated_store_device_registration(participant.registration.clone())
                    .await?;
                if !response.verify_for(&current.control, registration.value()) {
                    return Err(CircleOperationError::InvalidState(
                        "Circle epoch-close response slot holds an unverifiable response"
                            .to_string(),
                    ));
                }
                tracing::debug!(
                    circle_id = %circle_id,
                    close_id = %close.close_id,
                    device_id = %excluded_device_id,
                    "device responded before exclusion; adopting its response"
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn retry_circle_operation(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<(), CircleOperationError> {
        let journal = self
            .database
            .circle_operation(operation_id)
            .await?
            .ok_or_else(|| {
                CircleOperationError::Journal(format!("circle operation {operation_id} is absent"))
            })?;
        if !matches!(
            journal.state(),
            crate::protocol::circle::CircleOperationState::Blocked { .. }
        ) {
            return Err(CircleOperationError::NotBlocked {
                operation_id: operation_id.clone(),
            });
        }
        self.database.unblock_circle_operation(operation_id).await?;
        let routing_key = routing_encryption
            .map(|encryption| {
                crate::protocol::circle::derive_row_routing_key(
                    encryption,
                    journal.operation().creation.control.value.store_root_hash,
                )
            })
            .transpose()
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        self.publisher()
            .publish(operation_id, routing_key.as_ref())
            .await
    }

    pub(crate) async fn delete_circle(
        &mut self,
        circle_id: CircleId,
    ) -> Result<(), CircleOperationError> {
        if self
            .database
            .circle_control_conflict_branches(circle_id)
            .await?
            .is_some()
        {
            return Err(CircleOperationError::Conflicted { circle_id });
        }
        if self.database.circle_is_deleted(circle_id).await? {
            return Err(CircleOperationError::Deleted { circle_id });
        }
        let (current, reference) = self.current_delete_context(circle_id).await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::Delete(Box::new(
                CircleDeleteRequest {
                    circle_id,
                    current,
                    previous_control: reference,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle deletion changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        let superseded = self
            .database
            .waiting_circle_operations()
            .await?
            .into_iter()
            .find(|waiting| waiting.circle_id() == circle_id)
            .map(|waiting| waiting.operation_id.clone());
        match superseded {
            Some(superseded) => {
                self.database
                    .insert_circle_operation_superseding(journal, superseded)
                    .await?
            }
            None => self.database.insert_circle_operation(journal).await?,
        }
        self.publisher().publish(&operation_id, None).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn add_circle_member(
        &mut self,
        circle_id: CircleId,
        member_pubkey: String,
        role: CircleRole,
        bootstrap: crate::sync::store::SnapshotCut,
        routing_key: &crate::protocol::circle::RowRoutingKey,
    ) -> Result<(), CircleOperationError> {
        let (current, activation_commit, reference) =
            self.current_authoring_context(circle_id).await?;
        let keyring = match &current.access.disposition {
            crate::protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
            crate::protocol::circle::CircleAccessDisposition::Inactive => {
                return Err(CircleOperationError::InvalidState(
                    "Circle member addition requires active local access".to_string(),
                ));
            }
        };
        let roster_chain = self
            .history()
            .activations()
            .load_control_roster_chain(&activation_commit, &reference, &current.control, keyring)
            .await?;
        let journal = self
            .preparer()
            .prepare_request(CircleOperationRequest::AddMember(Box::new(
                CircleAddMemberRequest {
                    circle_id,
                    member_pubkey,
                    role,
                    bootstrap,
                    current,
                    previous_control: reference,
                    roster_chain,
                },
            )))
            .await?;
        if journal.circle_id() != circle_id {
            return Err(CircleOperationError::InvalidState(
                "prepared Circle member addition changed Circle identity".to_string(),
            ));
        }
        let operation_id = journal.operation_id.clone();
        self.database.insert_circle_operation(journal).await?;
        self.publisher()
            .publish(&operation_id, Some(routing_key))
            .await
    }

    pub(crate) async fn resume_circle_operations(
        &mut self,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<(), CircleOperationError> {
        let database = self.database.clone();
        for operation_id in database.discarding_circle_operations().await? {
            self.history()
                .cleanup_operation_candidate(&operation_id)
                .await
                .map_err(|error| {
                    CircleOperationError::InvalidState(format!(
                        "Circle operation {operation_id} discard cleanup: {error}"
                    ))
                })?;
            database
                .finish_circle_operation_discard(&operation_id)
                .await?;
        }
        while let Some(journal) = database.oldest_pending_circle_operation().await? {
            if !journal.is_publishable() {
                return Err(CircleOperationError::Journal(format!(
                    "pending circle operation {} contains a blocked payload",
                    journal.circle_id()
                )));
            }
            match self
                .publisher()
                .publish(&journal.operation_id, routing_key)
                .await
            {
                Ok(()) | Err(CircleOperationError::Blocked { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

fn retained_branch_authoring_state(
    circle_id: CircleId,
    identity_pubkey: &str,
    activation: &crate::sync::store::circle_controls::VerifiedCircleReference,
) -> Result<CircleAuthoringState, CircleOperationError> {
    let access = activation.local_access.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch has no local access"
        ))
    })?;
    let active = access.active.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch has no active access"
        ))
    })?;
    if access.leaf.value.recipient_pubkey != identity_pubkey {
        return Err(CircleOperationError::InvalidState(format!(
            "Circle {circle_id} retained branch belongs to another local identity"
        )));
    }
    Ok(CircleAuthoringState {
        candidate_family: access.leaf.value.candidate_family,
        control: activation.control.clone(),
        access: access.leaf.value.clone(),
        roster: active.roster.clone(),
        metadata: active.metadata.clone(),
    })
}

fn losing_branch_selected_metadata(
    circle_id: CircleId,
    activation: &crate::sync::store::circle_controls::VerifiedCircleReference,
) -> Result<crate::protocol::circle::CircleMetadata, CircleOperationError> {
    activation
        .local_access
        .as_ref()
        .and_then(|access| access.active.as_ref())
        .map(|active| active.metadata.clone())
        .ok_or_else(|| {
            CircleOperationError::InvalidState(format!(
                "Circle {circle_id} control resolution requires active access to every branch"
            ))
        })
}
