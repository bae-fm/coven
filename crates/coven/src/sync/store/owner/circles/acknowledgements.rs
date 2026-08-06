use coven_protocol::objects::{ProtocolObjectDomain, StoreObjectError};
use coven_protocol::store_commit::{circle_ack_slot_prefix, CircleAck, CommitFrontier};

use super::StoreAckError;

pub(crate) struct CircleAcknowledgementReader<'operation, 'storage> {
    database: &'operation coven_database::StoreDatabase,
    storage: &'storage dyn crate::storage::SyncStorage,
    root: &'operation coven_protocol::store_commit::StoreRootRef,
}

impl<'operation, 'storage> CircleAcknowledgementReader<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation coven_database::StoreDatabase,
        storage: &'storage dyn crate::storage::SyncStorage,
        root: &'operation coven_protocol::store_commit::StoreRootRef,
    ) -> Self {
        Self {
            database,
            storage,
            root,
        }
    }

    pub(crate) async fn load(
        &self,
        reference: &coven_protocol::store_commit::CircleAckRef,
    ) -> Result<CircleAck, StoreAckError> {
        let access = self
            .database
            .circle_epoch_access(
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
        let context = access.protocol_context(
            self.root.store_root_hash,
            ProtocolObjectDomain::CircleAcknowledgement,
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
        circle_id: coven_protocol::circle::CircleId,
        snapshot_cut: &CommitFrontier,
    ) -> Result<Option<Vec<coven_protocol::store_commit::CircleAckRef>>, StoreAckError> {
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
