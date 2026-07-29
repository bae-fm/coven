use super::snapshot;
use super::*;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store::operations;
use crate::sync::store_commit::{
    ack_slot_prefix, circle_ack_slot_prefix, CircleAck, DeviceStreamAnchor, StoreAck,
    StoreAckExclusionState, StoreHistoryCut, StoreRootRef, StreamActivation, SuccessorLink,
};
use crate::sync::store_objects::StoreObjectError;

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

struct ResolvedStoreAckPlan {
    root: StoreRootRef,
    registration_ref: crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: crate::sync::store_commit::StoreDeviceRegistration,
    device_signer: UserKeypair,
    device_id: String,
    history_cut: StoreHistoryCut,
    device_state: crate::sync::store_commit::StoreDeviceStateRef,
    snapshot: Option<crate::sync::store_commit::StoreSnapshotLocator>,
    exclusions: StoreAckExclusionState,
    last_sync: String,
}

async fn stage_resolved_store_ack(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    plan: ResolvedStoreAckPlan,
) -> Result<StoreAck, StoreAckError> {
    if database.oldest_outbound_store_ack().await?.is_some() {
        return Err(StoreAckError::InvalidOutbound(
            "a prior acknowledgement remains queued".to_string(),
        ));
    }
    let previous = database.latest_local_store_ack().await?;
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
        None => (
            1,
            None,
            acknowledgement_first_slot(&plan.registration)?.clone(),
        ),
    };
    let context = ProtocolObjectContext::signed_plaintext(
        plan.root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let semantic_prefix = ack_slot_prefix(&plan.device_id, sequence);
    let next_slot = storage
        .allocate_protocol_slot(
            &context,
            &ack_slot_prefix(
                &plan.device_id,
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
    let activation = plan
        .registration
        .store_acknowledgement_activation(&plan.registration_ref)
        .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?
        .activation_id();
    let acknowledgement = StoreAck::signed(
        plan.root.store_root_hash,
        plan.registration_ref,
        sequence,
        plan.history_cut,
        plan.device_state,
        plan.snapshot,
        plan.exclusions,
        plan.last_sync,
        SuccessorLink {
            activation,
            predecessor,
            next_slot,
        },
        &plan.device_signer,
    )
    .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
    let prepared = storage
        .prepare_protocol_object(
            &context,
            current_slot,
            &semantic_prefix,
            acknowledgement.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    database
        .stage_store_ack(acknowledgement.clone(), prepared)
        .await?;
    Ok(acknowledgement)
}

/// Create every pending Circle acknowledgement object at its reserved slot and
/// record it uploaded so its activating Store commit owns it. The ciphertext was
/// sealed at staging and `create_protocol_object` verifies the exact stored
/// bytes, so no epoch key is needed here. A slot occupied by different bytes is a
/// create-once violation on this device's per-Circle stream; it fails loud to the
/// drain, which retries the whole publication.
async fn publish_circle_acknowledgement_objects(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    outbound: &crate::database::OutboundStoreAck,
    candidate: &operations::PreparedStoreOperationCommit,
) -> Result<(), StoreAckError> {
    for circle in &outbound.circle_acknowledgements {
        if let Err(error) = storage.create_protocol_object(&circle.ack.prepared).await {
            if matches!(error, crate::sync::storage::StorageError::SlotCollision(_)) {
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
        database.mark_remote_object_uploaded(remote).await?;
    }
    Ok(())
}

async fn publish_acknowledgement_object(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    outbound: &crate::database::OutboundStoreAck,
    candidate: &operations::PreparedStoreOperationCommit,
) -> Result<bool, StoreAckError> {
    let context = ProtocolObjectContext::signed_plaintext(
        outbound.ack.value.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    if let Err(error) = storage.create_protocol_object(&outbound.ack.prepared).await {
        if !matches!(error, crate::sync::storage::StorageError::SlotCollision(_)) {
            return Err(StoreObjectError::from(error).into());
        }
        let semantic_prefix = ack_slot_prefix(device_id, outbound.reference.sequence);
        let (winner_bytes, winner_prepared) = storage
            .read_prepared_protocol_slot(
                &context,
                outbound.reference.object.slot(),
                &semantic_prefix,
            )
            .await
            .map_err(StoreObjectError::from)?;
        database
            .adopt_outbound_store_ack_slot_winner(
                outbound.reference.clone(),
                winner_bytes,
                winner_prepared,
            )
            .await?;
        return Ok(false);
    }
    let opened = storage
        .read_protocol_object(
            &context,
            &outbound.reference.object,
            &ack_slot_prefix(device_id, outbound.reference.sequence),
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
    database
        .mark_remote_object_uploaded(acknowledgement_remote)
        .await?;
    Ok(true)
}

fn acknowledgement_first_slot(
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
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
            self.database()
                .materialized_frontier()
                .await
                .map_err(|error| format!("read Store acknowledgement frontier: {error}"))?,
        )
        .map_err(|error| format!("shape Store acknowledgement frontier: {error}"))?;
        Box::pin(self.stage_acknowledgement(frontier.clone(), sync_time.to_owned()))
            .await
            .map_err(|error| format!("stage Store acknowledgement: {error}"))?;
        Box::pin(self.stage_circle_acknowledgements(&frontier, sync_time))
            .await
            .map_err(|error| format!("stage Circle acknowledgements: {error}"))?;
        Box::pin(self.drain_acknowledgements())
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
        Ok(())
    }

    /// Stage one Circle acknowledgement for every Circle whose private state this
    /// device currently holds active access to. Each names the device's exact
    /// accepted Store frontier, the activated control/epoch its projection
    /// derives from, and the retained bootstrap coverage it was seeded from,
    /// sealed to the Circle epoch key. Re-staging is skipped when neither the
    /// accepted frontier nor the control advanced past the last published
    /// acknowledgement. The Circle acknowledgements ride the same activating
    /// Store commit as the Store acknowledgement through the shared drain.
    pub(crate) async fn stage_circle_acknowledgements(
        &self,
        frontier: &CommitFrontier,
        sync_time: &str,
    ) -> Result<(), StoreAckError> {
        let inputs = self
            .database()
            .circle_acknowledgement_publication_inputs()
            .await?;
        if inputs.is_empty() {
            return Ok(());
        }
        let device_id = self.local_device_id().to_string();
        let root = self.store_root().clone();
        let (registration_ref, _, device_signer) = self.registration();
        let registration_ref = registration_ref.clone();
        let device_signer = device_signer.clone();
        for input in inputs {
            let previous = self
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
                    .storage()
                    .allocate_protocol_slot(&context, &semantic_prefix, ".json")
                    .await
                    .map_err(StoreObjectError::from)?,
            };
            let next_slot = self
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
                .storage()
                .prepare_protocol_object(&context, current_slot, &semantic_prefix, ack.to_bytes())
                .map_err(StoreObjectError::from)?;
            self.database().stage_circle_ack(ack, prepared).await?;
        }
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
        let (registration_ref, registration, device_signer) = self.registration();
        let registration_ref = registration_ref.clone();
        let registration = registration.clone();
        let device_signer = device_signer.clone();
        let history_cut = crate::sync::store_commit::StoreHistoryCut::from_commits(commits.clone());
        let (device_state, _) = self
            .database()
            .store_device_state_for_history_cut(&history_cut)
            .await?;
        let snapshot = self
            .history()
            .select_acknowledgement_snapshot(&frontier, &device_state)
            .await?;
        let exclusions = crate::sync::store_commit::StoreAckExclusionState {
            proposal_freezes: self.database().store_device_exclusion_freezes().await?,
        };
        stage_resolved_store_ack(
            self.database(),
            self.storage(),
            ResolvedStoreAckPlan {
                root,
                registration_ref,
                registration,
                device_signer,
                device_id,
                history_cut,
                device_state,
                snapshot,
                exclusions,
                last_sync: sync_time,
            },
        )
        .await
    }

    pub(crate) async fn drain_acknowledgements(&mut self) -> Result<u64, StoreAckError> {
        let device_id = self.local_device_id().to_string();
        let mut published = 0_u64;
        while let Some(outbound) = self.database().oldest_outbound_store_ack().await? {
            if let Some(activated) = self
                .database()
                .activated_store_ack(&outbound.reference.registration)
                .await?
            {
                if activated == outbound.reference {
                    self.database()
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
                    self.database()
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
            if !publish_acknowledgement_object(
                self.database(),
                self.storage(),
                &device_id,
                &outbound,
                &candidate,
            )
            .await?
            {
                continue;
            }
            publish_circle_acknowledgement_objects(
                self.storage(),
                self.database(),
                &outbound,
                &candidate,
            )
            .await?;
            let _authorship = self.database().author_own_stream().await;
            let publication =
                Box::pin(self.publish_prepared(Box::new(candidate), None, None)).await?;
            match publication
            {
                crate::sync::store::operations::StoreOperationPublicationOutcome::Activated(_) => {
                    self.database()
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
