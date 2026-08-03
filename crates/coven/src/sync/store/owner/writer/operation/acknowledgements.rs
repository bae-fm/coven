use super::snapshot;
use super::*;
use crate::protocol::store_commit::{ack_slot_prefix, DeviceStreamAnchor, StoreAck, SuccessorLink};
use crate::storage::StoreObjectError;
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain};

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreAckError {
    #[error("database: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("outbound Store acknowledgement is invalid: {0}")]
    InvalidOutbound(String),
    #[error("Store acknowledgement activation: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store acknowledgement snapshot: {0}")]
    Snapshot(#[from] snapshot::SnapshotError),
}

impl From<crate::database::DbError> for StoreAckError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

fn acknowledgement_first_slot(
    registration: &crate::protocol::store_commit::StoreDeviceRegistration,
) -> Result<&crate::storage::cloud::ObjectSlot, StoreAckError> {
    match &registration.acknowledgements {
        DeviceStreamAnchor::StoreAcknowledgements { first_slot } => Ok(first_slot),
        _ => Err(StoreAckError::InvalidOutbound(
            "local Store registration has no acknowledgement stream anchor".to_string(),
        )),
    }
}

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn stage_and_publish_ack(
        &mut self,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        Box::pin(self.drain_acknowledgements())
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("publish queued Store acknowledgement", error)
            })?;
        let frontier = CommitFrontier::from_refs(
            self.database
                .materialized_frontier()
                .await
                .map_err(|error| format!("read Store acknowledgement frontier: {error}"))?,
        )
        .map_err(|error| format!("shape Store acknowledgement frontier: {error}"))?;
        Box::pin(self.stage_acknowledgement(frontier.clone(), sync_time.to_owned()))
            .await
            .map_err(|error| format!("stage Store acknowledgement: {error}"))?;
        Box::pin(
            self.circles()
                .acknowledgements()
                .stage(&frontier, sync_time),
        )
        .await
        .map_err(|error| format!("stage Circle acknowledgements: {error}"))?;
        Box::pin(self.drain_acknowledgements())
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
        Ok(())
    }

    pub(crate) async fn stage_acknowledgement(
        &mut self,
        frontier: CommitFrontier,
        sync_time: String,
    ) -> Result<StoreAck, StoreAckError> {
        let commits = frontier.commits();
        let device_id = self.local_device_id().to_string();
        let root = self.store_root().clone();
        let registration_ref = self.writer.registration_ref().clone();
        let registration = self.writer.registration().clone();
        let device_signer = self.writer.device_signer.clone();
        let history_cut =
            crate::protocol::store_commit::StoreHistoryCut::from_commits(commits.clone());
        let (device_state, _) = self
            .database
            .store_device_state_for_history_cut(&history_cut)
            .await?;
        let snapshot = self
            .select_acknowledgement_snapshot(&frontier, &device_state)
            .await?;
        let exclusions = crate::protocol::store_commit::StoreAckExclusionState {
            proposal_freezes: self.database.store_device_exclusion_freezes().await?,
        };
        if self.database.oldest_outbound_store_ack().await?.is_some() {
            return Err(StoreAckError::InvalidOutbound(
                "a prior acknowledgement remains queued".to_string(),
            ));
        }
        let previous = self.database.latest_local_store_ack().await?;
        let (sequence, predecessor, current_slot) = match previous {
            Some(previous) => (
                previous.reference.sequence.checked_add(1).ok_or_else(|| {
                    StoreAckError::InvalidOutbound(
                        "Store acknowledgement sequence overflow".to_string(),
                    )
                })?,
                Some(previous.reference.object),
                previous.successor_slot,
            ),
            None => (1, None, acknowledgement_first_slot(&registration)?.clone()),
        };
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let semantic_prefix = ack_slot_prefix(&device_id, sequence);
        let next_slot = self
            .storage
            .allocate_protocol_slot(
                &context,
                &ack_slot_prefix(
                    &device_id,
                    sequence.checked_add(1).ok_or_else(|| {
                        StoreAckError::InvalidOutbound(
                            "Store acknowledgement sequence overflow".to_string(),
                        )
                    })?,
                ),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let activation = registration
            .store_acknowledgement_activation(&registration_ref)
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?
            .activation_id();
        let acknowledgement = StoreAck::signed(
            root.store_root_hash,
            registration_ref,
            sequence,
            history_cut,
            device_state,
            snapshot,
            exclusions,
            sync_time,
            SuccessorLink {
                activation,
                predecessor,
                next_slot,
            },
            &device_signer,
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
            .stage_store_ack(acknowledgement.clone(), prepared)
            .await?;
        Ok(acknowledgement)
    }

    pub(crate) async fn drain_acknowledgements(&mut self) -> Result<u64, StoreAckError> {
        let device_id = self.local_device_id().to_string();
        let mut published = 0_u64;
        while let Some(outbound) = self.database.oldest_outbound_store_ack().await? {
            if let Some(activated) = self
                .database
                .activated_store_ack(&outbound.reference.registration)
                .await?
            {
                if activated == outbound.reference {
                    self.database
                        .complete_outbound_store_ack(outbound.reference)
                        .await?;
                    published = published.checked_add(1).ok_or_else(|| {
                        StoreAckError::Database("ack publish count exceeded u64".into())
                    })?;
                    continue;
                }
                if activated.sequence >= outbound.reference.sequence {
                    return Err(StoreAckError::InvalidOutbound(
                        "queued Store acknowledgement differs from the activated exact ref"
                            .to_string(),
                    ));
                }
            }
            let candidate = match outbound.activation.clone() {
                crate::database::OutboundStoreAckActivation::AwaitingCandidate => {
                    let plan = self.prepare_plan().await?;
                    plan.common()
                        .validate_acknowledgement(&outbound.ack.value)?;
                    let candidate = Box::pin(self.prepare_candidate(
                        plan,
                        crate::sync::store::operations::StoreOperationBatch::Acknowledgement {
                            reference: outbound.reference.clone(),
                            value: outbound.ack.value.clone(),
                            circle_acknowledgements: outbound.circle_acknowledgements.clone(),
                        },
                    ))
                    .await?;
                    self.database
                        .prepare_acknowledgement_activation(outbound.reference.clone(), candidate)
                        .await?;
                    continue;
                }
                crate::database::OutboundStoreAckActivation::Prepared(candidate) => candidate,
                crate::database::OutboundStoreAckActivation::Nonactivating(_) => {
                    self.finish_nonactivating_acknowledgement(outbound.reference)
                        .await?;
                    published = published.checked_add(1).ok_or_else(|| {
                        StoreAckError::Database("ack publish count exceeded u64".into())
                    })?;
                    continue;
                }
            };
            let context = ProtocolObjectContext::signed_plaintext(
                outbound.ack.value.store_root_hash,
                ProtocolObjectDomain::StoreAck,
            );
            if let Err(error) = self
                .storage
                .create_protocol_object(&outbound.ack.prepared)
                .await
            {
                if !matches!(error, crate::storage::StorageError::SlotCollision(_)) {
                    return Err(StoreObjectError::from(error).into());
                }
                let semantic_prefix = ack_slot_prefix(&device_id, outbound.reference.sequence);
                let (winner_bytes, winner_prepared) = self
                    .storage
                    .read_prepared_protocol_slot(
                        &context,
                        outbound.reference.object.slot(),
                        &semantic_prefix,
                    )
                    .await
                    .map_err(StoreObjectError::from)?;
                self.database
                    .adopt_outbound_store_ack_slot_winner(
                        outbound.reference.clone(),
                        winner_bytes,
                        winner_prepared,
                    )
                    .await?;
                continue;
            }
            let opened = self
                .storage
                .read_protocol_object(
                    &context,
                    &outbound.reference.object,
                    &ack_slot_prefix(&device_id, outbound.reference.sequence),
                )
                .await
                .map_err(StoreObjectError::from)?;
            if opened != outbound.ack.bytes {
                return Err(StoreAckError::InvalidOutbound(
                    "Store acknowledgement exact readback differs from prepared bytes".to_string(),
                ));
            }
            let acknowledgement_remote = candidate
                .acknowledgement_remote_objects(&outbound.ack)?
                .into_iter()
                .find(|remote| remote.object() == &outbound.reference.object)
                .ok_or_else(|| {
                    StoreAckError::InvalidOutbound(
                        "prepared activation does not own its acknowledgement object".to_string(),
                    )
                })?;
            self.database
                .mark_remote_object_uploaded(acknowledgement_remote)
                .await?;
            self.circles()
                .acknowledgements()
                .publish_objects(&outbound, &candidate)
                .await?;
            let _authorship = self.database.author_own_stream().await;
            let publication =
                Box::pin(self.publish_prepared(Box::new(candidate), None, None)).await?;
            match publication
            {
                crate::sync::store::operations::StoreOperationPublicationOutcome::Activated(_) => {
                    self.database
                        .complete_outbound_store_ack(outbound.reference)
                        .await?;
                }
                crate::sync::store::operations::StoreOperationPublicationOutcome::Nonactivated(_) => {}
                crate::sync::store::operations::StoreOperationPublicationOutcome::Reprepared => {
                    continue;
                }
                crate::sync::store::operations::StoreOperationPublicationOutcome::RepreparedCandidate(_)
                | crate::sync::store::operations::StoreOperationPublicationOutcome::NonactivatedCandidate { .. } => {
                    return Err(StoreAckError::InvalidOutbound(
                        "acknowledgement publication returned non-acknowledgement conflict state"
                            .to_string(),
                    ));
                }
            }
            published = published
                .checked_add(1)
                .ok_or_else(|| StoreAckError::Database("ack publish count exceeded u64".into()))?;
        }
        Ok(published)
    }
}

#[cfg(test)]
mod tests;
