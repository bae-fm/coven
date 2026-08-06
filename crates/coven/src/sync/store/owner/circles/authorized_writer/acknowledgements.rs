use super::*;

impl<'writer, 'storage> AuthorizedCircleWriter<'writer, 'storage> {
    pub(crate) async fn stage_acknowledgements(
        &self,
        frontier: &coven_protocol::store_commit::CommitFrontier,
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
        candidate: &coven_protocol::prepared_commit::PreparedStoreOperationCommit,
    ) -> Result<(), StoreAckError> {
        for circle in &outbound.circle_acknowledgements {
            if let Err(error) = self
                .storage
                .create_protocol_object(&circle.ack.prepared)
                .await
            {
                if matches!(
                    error,
                    coven_protocol::objects::StorageError::SlotCollision(_)
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
}
