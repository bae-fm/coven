use coven_protocol::objects::{ProtocolObjectDomain, StoreObjectError};
use coven_protocol::store_commit::{circle_ack_slot_prefix, CircleAck, CommitFrontier};

use super::StoreAckError;
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;

pub(crate) struct CircleAcknowledgementReader<'operation, 'storage> {
    database: &'operation coven_database::StoreDatabase,
    storage: &'storage dyn coven_storage::CloudSyncObjectStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> CircleAcknowledgementReader<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation coven_database::StoreDatabase,
        storage: &'storage dyn coven_storage::CloudSyncObjectStorage,
        history: &'operation mut MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }

    pub(crate) async fn load(
        &self,
        reference: &coven_protocol::store_commit::CircleAckRef,
    ) -> Result<CircleAck, StoreAckError> {
        let access = self
            .database
            .circle_epoch_access(
                self.history.verified_root().reference().clone(),
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
            self.history.verified_root().reference().store_root_hash,
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
        CircleAck::parse_at(
            &bytes,
            self.history.verified_root().reference(),
            reference,
            author.value(),
        )
        .map_err(StoreAckError::from)
    }

    pub(crate) async fn stable_dominating(
        &mut self,
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
            if !acknowledgement.store_cut.covers(snapshot_cut)
                && !self
                    .history
                    .history_has_only_acknowledgements(
                        &coven_protocol::store_commit::StoreHistoryCut::from_commits(
                            acknowledgement.store_cut.0.clone(),
                        ),
                        &coven_protocol::store_commit::StoreHistoryCut::from_commits(
                            snapshot_cut.0.clone(),
                        ),
                    )
                    .await
                    .map_err(crate::sync::store::StoreError::from)?
            {
                return Ok(None);
            }
            acknowledgements.push(reference);
        }
        acknowledgements.sort();
        Ok(Some(acknowledgements))
    }
}
