use super::*;

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
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
        routing_encryption: &coven_keys::encryption::EncryptionService,
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
                != coven_protocol::circle::CircleEpochCloseId::from_operation_id(
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
                coven_protocol::circle::CircleAccessDisposition::Active { keyring, .. } => keyring,
                coven_protocol::circle::CircleAccessDisposition::Inactive => {
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
            if prepared.journal.operation_id != journal.operation_id
                || prepared.journal.circle_id != journal.circle_id
                || prepared.journal.intent != journal.intent
            {
                return Err(CircleOperationError::Journal(format!(
                    "Circle operation {} finalization changed its durable identity",
                    journal.operation_id
                )));
            }
            journal.begin_finalization(prepared.journal.operation().clone())?;
            self.database
                .begin_circle_operation_finalization(journal.clone(), prepared.prepared_objects)
                .await?;
            let routing_key = coven_protocol::circle::derive_row_routing_key(
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

    pub(crate) async fn load_complete_circle_epoch_close_responses(
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
}
