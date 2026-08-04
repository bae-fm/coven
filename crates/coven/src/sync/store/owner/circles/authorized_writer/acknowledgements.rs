use super::*;
use crate::protocol::store_commit::CommitFrontier;
use crate::storage::{ProtocolObjectDomain, StoreObjectError};
use crate::sync::store::operations;

pub(super) struct CircleAcknowledgementWriter<'operation, 'writer, 'storage> {
    owner: &'operation AuthorizedCircleWriter<'writer, 'storage>,
}

impl<'operation, 'writer, 'storage> CircleAcknowledgementWriter<'operation, 'writer, 'storage> {
    pub(super) fn new(owner: &'operation AuthorizedCircleWriter<'writer, 'storage>) -> Self {
        Self { owner }
    }

    pub(super) async fn stage(
        &self,
        frontier: &CommitFrontier,
        sync_time: &str,
    ) -> Result<(), StoreAckError> {
        let inputs = self
            .owner
            .database
            .circle_acknowledgement_publication_inputs()
            .await?;
        if inputs.is_empty() {
            return Ok(());
        }
        let root = self.owner.root.clone();
        for input in inputs {
            let previous = self
                .owner
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
                root.store_root_hash,
                ProtocolObjectDomain::CircleAcknowledgement,
            );
            let semantic_prefix = self
                .owner
                .local_writer
                .circle_ack_semantic_prefix(input.circle_id, sequence);
            let current_slot = match &previous {
                Some(previous) => previous.successor_slot.clone(),
                None => self
                    .owner
                    .storage
                    .allocate_protocol_slot(&context, &semantic_prefix, ".json")
                    .await
                    .map_err(StoreObjectError::from)?,
            };
            let next_slot = self
                .owner
                .storage
                .allocate_protocol_slot(
                    &context,
                    &self.owner.local_writer.circle_ack_semantic_prefix(
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
            let ack = self
                .owner
                .local_writer
                .sign_circle_acknowledgement(
                    root.store_root_hash,
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
                .owner
                .storage
                .prepare_protocol_object(&context, current_slot, &semantic_prefix, ack.to_bytes())
                .map_err(StoreObjectError::from)?;
            self.owner.database.stage_circle_ack(ack, prepared).await?;
        }
        Ok(())
    }

    pub(super) async fn publish_objects(
        &self,
        outbound: &crate::database::OutboundStoreAck,
        candidate: &operations::PreparedStoreOperationCommit,
    ) -> Result<(), StoreAckError> {
        for circle in &outbound.circle_acknowledgements {
            if let Err(error) = self
                .owner
                .storage
                .create_protocol_object(&circle.ack.prepared)
                .await
            {
                if matches!(error, crate::storage::StorageError::SlotCollision(_)) {
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
            self.owner
                .database
                .mark_remote_object_uploaded(remote)
                .await?;
        }
        Ok(())
    }
}
