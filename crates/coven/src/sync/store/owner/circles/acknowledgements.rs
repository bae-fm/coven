use crate::protocol::store_commit::{
    circle_ack_slot_prefix, CircleAck, CommitFrontier, DeviceStreamAnchor, StreamActivation,
    SuccessorLink,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, StoreObjectError};
use crate::sync::store::operations;

use super::super::StoreAckError;
use super::AuthorizedWriterOperation;

pub(crate) struct CircleAcknowledgementWriter<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
}

impl<'operation, 'storage> CircleAcknowledgementWriter<'operation, 'storage> {
    pub(super) fn new(writer: &'operation mut AuthorizedWriterOperation<'storage>) -> Self {
        Self { writer }
    }

    pub(crate) async fn stage(
        &self,
        frontier: &CommitFrontier,
        sync_time: &str,
    ) -> Result<(), StoreAckError> {
        let inputs = self
            .writer
            .database()
            .circle_acknowledgement_publication_inputs()
            .await?;
        if inputs.is_empty() {
            return Ok(());
        }
        let device_id = self.writer.local_device_id().to_string();
        let root = self.writer.store_root().clone();
        let (registration_ref, _, device_signer) = self.writer.registration();
        let registration_ref = registration_ref.clone();
        let device_signer = device_signer.clone();
        for input in inputs {
            let previous = self
                .writer
                .database()
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
            let context = ProtocolObjectContext::circle(
                root.store_root_hash,
                ProtocolObjectDomain::CircleAcknowledgement,
                input.epoch_encryption,
            );
            let semantic_prefix = circle_ack_slot_prefix(input.circle_id, &device_id, sequence);
            let current_slot = match &previous {
                Some(previous) => previous.successor_slot.clone(),
                None => self
                    .writer
                    .storage()
                    .allocate_protocol_slot(&context, &semantic_prefix, ".json")
                    .await
                    .map_err(StoreObjectError::from)?,
            };
            let next_slot = self
                .writer
                .storage()
                .allocate_protocol_slot(
                    &context,
                    &circle_ack_slot_prefix(
                        input.circle_id,
                        &device_id,
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
            let stream_first_slot = crate::storage::cloud::ObjectSlot::logical(format!(
                "{}.json",
                circle_ack_slot_prefix(input.circle_id, &device_id, 1)
            ))
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
            let activation = StreamActivation::device_authorized(
                root.store_root_hash,
                registration_ref.clone(),
                DeviceStreamAnchor::CircleAcknowledgements {
                    circle_id: input.circle_id,
                    first_slot: stream_first_slot,
                },
            )
            .activation_id();
            let ack = CircleAck::signed(
                root.store_root_hash,
                input.circle_id,
                registration_ref.clone(),
                sequence,
                frontier.clone(),
                input.control,
                input.epoch_id,
                input.key_fingerprint,
                input.seeded_from,
                sync_time.to_owned(),
                SuccessorLink {
                    activation,
                    predecessor,
                    next_slot,
                },
                &device_signer,
            )
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
            let prepared = self
                .writer
                .storage()
                .prepare_protocol_object(&context, current_slot, &semantic_prefix, ack.to_bytes())
                .map_err(StoreObjectError::from)?;
            self.writer
                .database()
                .stage_circle_ack(ack, prepared)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn publish_objects(
        &self,
        outbound: &crate::database::OutboundStoreAck,
        candidate: &operations::PreparedStoreOperationCommit,
    ) -> Result<(), StoreAckError> {
        for circle in &outbound.circle_acknowledgements {
            if let Err(error) = self
                .writer
                .storage()
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
            self.writer
                .database()
                .mark_remote_object_uploaded(remote)
                .await?;
        }
        Ok(())
    }
}
