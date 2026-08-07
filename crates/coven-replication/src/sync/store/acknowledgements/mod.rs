//! Store and Circle acknowledgement publication.

mod circle;

pub(crate) use circle::CircleAcknowledgementReader;

use super::snapshots as snapshot;
use super::{AuthorizedWriterOperation, StoreError};
use crate::sync::cycle::SyncCycleFailure;
use crate::sync::store::commit_publication::LocalStoreWriter;
use coven_database::StoreDatabase;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{ack_slot_prefix, CommitFrontier, StoreAck, SuccessorLink};
use coven_storage::{SyncStorage, VerifiedObjectWrites};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum StoreAckError {
    #[error("database: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("Store protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("published Store acknowledgement count has no representable successor")]
    PublishCountExhausted,
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("outbound Store acknowledgement is invalid: {0}")]
    InvalidOutbound(String),
    #[error("Store acknowledgement activation: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store acknowledgement snapshot: {0}")]
    Snapshot(#[from] snapshot::SnapshotError),
}

pub(crate) struct AuthorizedAcknowledgements<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: Arc<dyn SyncStorage>,
    local_writer: Arc<LocalStoreWriter>,
}

impl<'operation, 'storage> AuthorizedAcknowledgements<'operation, 'storage> {
    pub(crate) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        local_writer: Arc<LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            local_writer,
        }
    }

    pub(crate) async fn stage_and_publish(
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
            self.writer
                .circles()
                .stage_acknowledgements(&frontier, sync_time),
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
        let device_id = self.writer.local_device_id().to_string();
        let root = self.writer.store_root().clone();
        let history_cut =
            coven_protocol::store_commit::StoreHistoryCut::from_commits(commits.clone());
        let (device_state, _) = self
            .database
            .store_device_state_for_history_cut(&history_cut)
            .await?;
        let snapshot = self
            .writer
            .select_acknowledgement_snapshot(&frontier, &device_state)
            .await?;
        let exclusions = coven_protocol::store_commit::StoreAckExclusionState {
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
            None => (1, None, self.local_writer.first_acknowledgement_slot()),
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
        let activation = self
            .local_writer
            .acknowledgement_activation_id()
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
        let acknowledgement = self
            .local_writer
            .sign_device_acknowledgement(
                root.store_root_hash,
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
        let device_id = self.writer.local_device_id().to_string();
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
                    published = published
                        .checked_add(1)
                        .ok_or(StoreAckError::PublishCountExhausted)?;
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
                coven_database::OutboundStoreAckActivation::AwaitingCandidate => {
                    let plan = self.writer.prepare_plan().await?;
                    plan.common()
                        .validate_acknowledgement(&outbound.ack.value)?;
                    let candidate = Box::pin(self.writer.prepare_candidate(
                        plan,
                        crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Acknowledgement {
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
                coven_database::OutboundStoreAckActivation::Prepared(candidate) => candidate,
                coven_database::OutboundStoreAckActivation::Nonactivating(_) => {
                    self.writer
                        .finish_nonactivating_acknowledgement(outbound.reference)
                        .await?;
                    published = published
                        .checked_add(1)
                        .ok_or(StoreAckError::PublishCountExhausted)?;
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
                if !matches!(
                    error,
                    coven_protocol::objects::StorageError::SlotCollision(_)
                ) {
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
            self.storage
                .verify_readback(
                    &context,
                    &outbound.reference.object,
                    &ack_slot_prefix(&device_id, outbound.reference.sequence),
                    &outbound.ack.bytes,
                )
                .await
                .map_err(|error| StoreAckError::Outbound(StoreError::readback(error)))?;
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
            self.writer
                .circles()
                .publish_acknowledgement_objects(&outbound, &candidate)
                .await?;
            let _authorship = self.database.author_own_stream().await;
            let publication = Box::pin(self.writer.publish_prepared(
                Box::new(candidate),
                None,
                None,
            ))
            .await?;
            match publication
            {
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::Activated(_) => {
                    self.database
                        .complete_outbound_store_ack(outbound.reference)
                        .await?;
                }
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::Nonactivated(_) => {}
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::Reprepared => {
                    continue;
                }
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::RepreparedCandidate(_)
                | crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::NonactivatedCandidate { .. } => {
                    return Err(StoreAckError::InvalidOutbound(
                        "acknowledgement publication returned non-acknowledgement conflict state"
                            .to_string(),
                    ));
                }
            }
            published = published
                .checked_add(1)
                .ok_or(StoreAckError::PublishCountExhausted)?;
        }
        Ok(published)
    }
}

#[cfg(test)]
mod tests;

impl From<coven_protocol::prepared_commit::PreparedCommitError> for StoreAckError {
    fn from(error: coven_protocol::prepared_commit::PreparedCommitError) -> Self {
        StoreAckError::InvalidOutbound(error.to_string())
    }
}
