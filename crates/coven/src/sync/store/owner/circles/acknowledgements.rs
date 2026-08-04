use crate::protocol::store_commit::{circle_ack_slot_prefix, CircleAck, CommitFrontier};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, StoreObjectError};
use crate::sync::store::operations;

use super::StoreAckError;
pub(crate) struct CircleAcknowledgementWriter {
    database: crate::database::StoreDatabase,
    storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
    root: crate::protocol::store_commit::StoreRootRef,
    local_writer: std::sync::Arc<crate::sync::store::owner::writer::LocalStoreWriter>,
}

pub(crate) struct CircleAcknowledgementReader<'operation, 'storage> {
    database: &'operation crate::database::StoreDatabase,
    storage: &'storage dyn crate::storage::SyncStorage,
    root: &'operation crate::protocol::store_commit::StoreRootRef,
}

impl<'operation, 'storage> CircleAcknowledgementReader<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation crate::database::StoreDatabase,
        storage: &'storage dyn crate::storage::SyncStorage,
        root: &'operation crate::protocol::store_commit::StoreRootRef,
    ) -> Self {
        Self {
            database,
            storage,
            root,
        }
    }

    pub(crate) async fn load(
        &self,
        reference: &crate::protocol::store_commit::CircleAckRef,
    ) -> Result<CircleAck, StoreAckError> {
        let access = self
            .database
            .circle_package_access(
                self.root.clone(),
                reference.circle_id,
                reference.control.clone(),
            )
            .await?
            .ok_or_else(|| {
                StoreAckError::InvalidOutbound(format!(
                    "Circle {} acknowledgement key is not resolvable from its exact control",
                    reference.circle_id
                ))
            })?;
        let author = self
            .database
            .activated_store_device_registration(reference.registration.clone())
            .await?;
        let context = ProtocolObjectContext::circle(
            self.root.store_root_hash,
            ProtocolObjectDomain::CircleAcknowledgement,
            access.into_encryption(),
        );
        let semantic_prefix = circle_ack_slot_prefix(
            reference.circle_id,
            &author.value().device_id.to_string(),
            reference.sequence,
        );
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await
            .map_err(StoreObjectError::from)?;
        CircleAck::parse_at(&bytes, self.root, reference, author.value())
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))
    }

    pub(crate) async fn stable_dominating(
        &self,
        circle_id: crate::protocol::circle::CircleId,
        snapshot_cut: &CommitFrontier,
    ) -> Result<Option<Vec<crate::protocol::store_commit::CircleAckRef>>, StoreAckError> {
        let devices = self
            .database
            .active_circle_access_devices(circle_id)
            .await?;
        if devices.is_empty() {
            return Ok(None);
        }
        let mut acknowledgements = Vec::new();
        for device_id in devices {
            let Some(reference) = self
                .database
                .activated_circle_ack(circle_id, device_id)
                .await?
            else {
                return Ok(None);
            };
            let acknowledgement = self.load(&reference).await?;
            if !acknowledgement.store_cut.covers(snapshot_cut) {
                return Ok(None);
            }
            acknowledgements.push(reference);
        }
        acknowledgements.sort();
        Ok(Some(acknowledgements))
    }
}

impl CircleAcknowledgementWriter {
    pub(super) fn new(
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<dyn crate::storage::SyncStorage>,
        root: crate::protocol::store_commit::StoreRootRef,
        local_writer: std::sync::Arc<crate::sync::store::owner::writer::LocalStoreWriter>,
    ) -> Self {
        Self {
            database,
            storage,
            root,
            local_writer,
        }
    }

    pub(crate) async fn stage(
        &self,
        frontier: &CommitFrontier,
        sync_time: &str,
    ) -> Result<(), StoreAckError> {
        let inputs = self
            .database
            .circle_acknowledgement_publication_inputs()
            .await?;
        if inputs.is_empty() {
            return Ok(());
        }
        let root = self.root.clone();
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
            let context = ProtocolObjectContext::circle(
                root.store_root_hash,
                ProtocolObjectDomain::CircleAcknowledgement,
                input.epoch_encryption,
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
            let ack = self
                .local_writer
                .sign_circle_acknowledgement(
                    root.store_root_hash,
                    input.circle_id,
                    sequence,
                    frontier.clone(),
                    input.control,
                    input.epoch_id,
                    input.key_fingerprint,
                    input.seeded_from,
                    sync_time.to_owned(),
                    predecessor,
                    next_slot,
                )
                .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
            let prepared = self
                .storage
                .prepare_protocol_object(&context, current_slot, &semantic_prefix, ack.to_bytes())
                .map_err(StoreObjectError::from)?;
            self.database.stage_circle_ack(ack, prepared).await?;
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
            self.database.mark_remote_object_uploaded(remote).await?;
        }
        Ok(())
    }
}
