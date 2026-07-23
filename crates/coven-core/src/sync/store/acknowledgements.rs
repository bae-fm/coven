use super::database::StoreDatabase;
use super::*;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store_commit::{
    ack_slot_prefix, DeviceStreamAnchor, StoreAck, StoreAckExclusionState, StoreHistoryCut,
    SuccessorLink,
};
use crate::sync::store_objects::StoreObjectError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreAckError {
    #[error("database: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("Store acknowledgement protocol state {0:?} is absent")]
    MissingState(&'static str),
    #[error("outbound Store acknowledgement is invalid: {0}")]
    InvalidOutbound(String),
    #[error("Store acknowledgement activation: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store acknowledgement snapshot: {0}")]
    Snapshot(#[from] crate::sync::store::snapshot::SnapshotError),
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
    let db = database.sqlite();
    if db.oldest_outbound_store_ack().await?.is_some() {
        return Err(StoreAckError::InvalidOutbound(
            "a prior acknowledgement remains queued".to_string(),
        ));
    }
    let previous = db.latest_local_store_ack().await?;
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
    db.stage_store_ack(acknowledgement.clone(), prepared)
        .await?;
    Ok(acknowledgement)
}

async fn publish_acknowledgement_object(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    outbound: &crate::database::OutboundStoreAck,
    candidate: &operations::PreparedStoreOperationCommit,
) -> Result<bool, StoreAckError> {
    let db = database.sqlite();
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
        db.adopt_outbound_store_ack_slot_winner(
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

pub(super) async fn finish_nonactivating_acknowledgement(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    acknowledgement: crate::sync::store_commit::StoreAckRef,
) -> Result<(), crate::sync::store::StoreError> {
    let db = database.sqlite();
    if let Some(target) = database
        .acknowledgement_cleanup_target(acknowledgement.clone())
        .await?
    {
        crate::sync::store_objects::delete_exact_object(storage, &target.object).await?;
        db.mark_candidate_cleanup_absent(target.object).await?;
    }
    database
        .complete_nonactivating_acknowledgement(acknowledgement)
        .await?;
    Ok(())
}

impl AuthorizedStore<'_> {
    pub(crate) async fn stage_and_publish_ack(
        &self,
        identity: &UserKeypair,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        Box::pin(self.drain_acknowledgements(identity))
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
        Box::pin(self.stage_acknowledgement(frontier, sync_time.to_owned(), identity))
            .await
            .map_err(|error| format!("stage Store acknowledgement: {error}"))?;
        Box::pin(self.drain_acknowledgements(identity))
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
        Ok(())
    }

    pub(crate) async fn stage_acknowledgement(
        &self,
        frontier: CommitFrontier,
        sync_time: String,
        identity: &UserKeypair,
    ) -> Result<StoreAck, StoreAckError> {
        let commits = frontier.commits();
        let device_id = self
            .db()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or(StoreAckError::MissingState(
                crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            ))?;
        let (root, registration_ref, registration, device_signer) =
            crate::sync::store::operations::load_local_store_authority(
                self.database(),
                &device_id,
                identity,
            )
            .await
            .map_err(|error| StoreAckError::InvalidOutbound(error.to_string()))?;
        let history_cut = crate::sync::store_commit::StoreHistoryCut::from_commits(commits.clone());
        let (device_state, _) = self
            .database()
            .store_device_state_for_history_cut(&history_cut)
            .await?;
        let snapshot = self
            .select_acknowledgement_snapshot(&root, &frontier, &device_state)
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

    async fn select_acknowledgement_snapshot(
        &self,
        root: &StoreRootRef,
        frontier: &CommitFrontier,
        device_state: &crate::sync::store_commit::StoreDeviceStateRef,
    ) -> Result<Option<crate::sync::store_commit::StoreSnapshotLocator>, StoreAckError> {
        let registrations = self
            .database()
            .activated_store_device_registration_records()
            .await?;
        let mut candidates = Vec::new();
        for (registration_ref, registration) in registrations {
            for snapshot in crate::sync::store::snapshot::load_store_snapshot_stream(
                self.storage(),
                root,
                &registration_ref,
                &registration,
            )
            .await?
            {
                if !frontier.covers(&snapshot.meta.coverage)
                    || snapshot.meta.state.devices.state_hash() != device_state.state_hash()
                    || snapshot.meta.state.devices.recovery() != device_state.recovery()
                {
                    continue;
                }
                pull::verify_snapshot_for_acknowledgement(self.storage(), root, &snapshot)
                    .await
                    .map_err(|error| {
                        crate::sync::store::snapshot::SnapshotError::UnauthorizedAuthor(
                            error.to_string(),
                        )
                    })?;
                candidates.push(snapshot);
            }
        }
        Ok(
            crate::sync::store::snapshot::select_maximal_store_snapshot(candidates).map(
                |snapshot| crate::sync::store_commit::StoreSnapshotLocator {
                    author_registration: snapshot.meta.author_registration,
                    snapshot: snapshot.reference,
                },
            ),
        )
    }

    pub(crate) async fn drain_acknowledgements(
        &self,
        identity: &UserKeypair,
    ) -> Result<u64, StoreAckError> {
        let device_id = self
            .db()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or(StoreAckError::MissingState(
                crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            ))?;
        let mut published = 0_u64;
        while let Some(outbound) = self.db().oldest_outbound_store_ack().await? {
            if let Some(activated) = self
                .db()
                .activated_store_ack(&outbound.reference.registration)
                .await?
            {
                if activated == outbound.reference {
                    self.db()
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
                    let plan = operations::prepare_plan(
                        self.database(),
                        self.storage(),
                        self.membership(),
                        &device_id,
                        identity,
                    )
                    .await?;
                    plan.common()
                        .validate_acknowledgement(&outbound.ack.value)?;
                    let candidate = Box::pin(operations::prepare_candidate(
                        self.database(),
                        self.storage(),
                        plan,
                        crate::sync::store::operations::StoreOperationBatch::Acknowledgement {
                            reference: outbound.reference.clone(),
                            value: outbound.ack.value.clone(),
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
                    finish_nonactivating_acknowledgement(
                        self.database(),
                        self.storage(),
                        outbound.reference,
                    )
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
            match Box::pin(operations::publish_prepared(
                self.database(),
                self.storage(),
                Box::new(candidate),
                None,
                None,
            ))
            .await?
            {
                crate::sync::store::operations::StoreOperationPublicationOutcome::Activated(_) => {
                    self.db()
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
