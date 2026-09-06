//! Store and Circle acknowledgement publication.

mod circle;

pub(crate) use circle::CircleAcknowledgementReader;

use super::snapshots as snapshot;
use super::{AuthorizedWriterOperation, StoreError};
use crate::sync::cycle::SyncCycleFailure;
use crate::sync::store::commit_publication::LocalStoreWriter;
use crate::sync::store::commit_verification::merge_history::SelectedReplayBaselineRetirement;
use coven_database::StoreDatabase;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    ack_slot_prefix, CommitFrontier, StoreAck, StoreSnapshotLocator, SuccessorLink,
};
use coven_storage::CloudSyncObjectStorage;
use std::sync::Arc;
use tracing::debug;

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
    #[error("outbound Store acknowledgement prepared commit: {0}")]
    PreparedCommit(#[from] coven_protocol::prepared_commit::PreparedCommitError),
    #[error("Store acknowledgement activation: {0}")]
    Outbound(#[from] StoreError),
    #[error("Store acknowledgement sync cycle: {0}")]
    SyncCycle(#[source] Box<crate::sync::cycle::SyncCycleFailure>),
    #[error("Store acknowledgement writer authorization: {0}")]
    WriterAuthorization(#[source] Box<crate::sync::store::StoreWriterAuthorizationError>),
    #[error("Store acknowledgement snapshot: {0}")]
    Snapshot(#[from] snapshot::SnapshotError),
}

impl From<crate::sync::cycle::SyncCycleFailure> for StoreAckError {
    fn from(error: crate::sync::cycle::SyncCycleFailure) -> Self {
        Self::SyncCycle(Box::new(error))
    }
}

impl From<crate::sync::store::StoreWriterAuthorizationError> for StoreAckError {
    fn from(error: crate::sync::store::StoreWriterAuthorizationError) -> Self {
        Self::WriterAuthorization(Box::new(error))
    }
}

pub struct StagedStoreAcknowledgement {
    pub acknowledgement: Option<StoreAck>,
}

/// What standing on this device's acknowledged snapshot did, or why it did
/// nothing.
///
/// A decline is a value rather than a swallowed nothing for the same reason the
/// reclaim report's is: a stage that speaks only when it acts is
/// indistinguishable from one that is not running, and this one spent weeks
/// looking exactly like that on a live store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayBaselineAdvance {
    Advanced(coven_database::AdvancedReplayBaseline),
    Declined(ReplayBaselineDecline),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayBaselineDecline {
    /// This device has published no acknowledgement naming a snapshot, so it
    /// has never said it holds one.
    NoAcknowledgedSnapshot,
    /// The device that authored the acknowledged snapshot is no longer an
    /// activated registration, so its stream is not this device's to read.
    SnapshotAuthorInactive { generation: u64 },
    /// The acknowledged snapshot is gone from its author's stream.
    SnapshotUnavailable { generation: u64 },
    /// The acknowledged snapshot did not verify as installable now.
    SnapshotRejected { generation: u64 },
    /// A current writer has not published an acknowledgement whose Store cut
    /// covers this snapshot.
    MissingWriterAcknowledgement {
        generation: u64,
        member: String,
        device_id: String,
    },
    /// Current membership has not been named by accepted Store history, so its
    /// writer set cannot yet license retirement.
    MembershipNotAccepted { generation: u64 },
    /// Current accepted history applies a commit outside the snapshot cut
    /// before a commit inside it, so the cut cannot become a replay baseline.
    NonPrefixCut { generation: u64 },
    /// The steady state: the baseline already restates everything the
    /// acknowledged snapshot does.
    BaselineAtCoverage { generation: u64 },
}

impl ReplayBaselineDecline {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoAcknowledgedSnapshot => "this device has acknowledged no snapshot",
            Self::SnapshotAuthorInactive { .. } => "the acknowledged snapshot's author is inactive",
            Self::SnapshotUnavailable { .. } => "the acknowledged snapshot is gone from its stream",
            Self::SnapshotRejected { .. } => "the acknowledged snapshot did not verify",
            Self::MissingWriterAcknowledgement { .. } => {
                "a current writer has not crossed the acknowledged snapshot"
            }
            Self::MembershipNotAccepted { .. } => {
                "current membership is not yet in accepted Store history"
            }
            Self::NonPrefixCut { .. } => {
                "the acknowledged snapshot is not a prefix of accepted replay order"
            }
            Self::BaselineAtCoverage { .. } => "the baseline already covers it",
        }
    }

    pub fn generation(&self) -> Option<u64> {
        match self {
            Self::NoAcknowledgedSnapshot => None,
            Self::SnapshotAuthorInactive { generation }
            | Self::SnapshotUnavailable { generation }
            | Self::SnapshotRejected { generation }
            | Self::MissingWriterAcknowledgement { generation, .. }
            | Self::MembershipNotAccepted { generation }
            | Self::NonPrefixCut { generation }
            | Self::BaselineAtCoverage { generation } => Some(*generation),
        }
    }
}

pub(crate) struct AuthorizedAcknowledgements<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: Arc<dyn CloudSyncObjectStorage>,
    local_writer: Arc<LocalStoreWriter>,
}

impl<'operation, 'storage> AuthorizedAcknowledgements<'operation, 'storage> {
    pub(crate) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        local_writer: Arc<LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            local_writer,
        }
    }

    /// Publish anything queued, then acknowledge where this device now stands.
    /// Local replay retirement runs independently after acknowledgement
    /// publication, once every current writer has crossed the selected cut.
    pub(crate) async fn stage_and_publish(
        &mut self,
        sync_time: &str,
        settled: &crate::sync::store::SettledCycle,
    ) -> Result<(), SyncCycleFailure> {
        Box::pin(self.drain_acknowledgements())
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("publish queued Store acknowledgement", error)
            })?;
        let frontier =
            CommitFrontier::from_refs(self.database.materialized_frontier().await.map_err(
                |error| SyncCycleFailure::operation("read Store acknowledgement frontier", error),
            )?)
            .map_err(|error| {
                SyncCycleFailure::operation("shape Store acknowledgement frontier", error)
            })?;
        // Circle acknowledgements first: an outbound Store acknowledgement is what
        // carries them to the cloud, so the Store one below has to know whether
        // any are waiting before it decides it has nothing to say.
        Box::pin(
            self.writer
                .circles()
                .stage_acknowledgements(&frontier, sync_time),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("stage Circle acknowledgements", error))?;
        let StagedStoreAcknowledgement { acknowledgement } =
            Box::pin(self.stage_acknowledgement_against(
                frontier.clone(),
                sync_time.to_owned(),
                Some(settled),
            ))
            .await
            .map_err(|error| SyncCycleFailure::operation("stage Store acknowledgement", error))?;
        if let Some(acknowledgement) = &acknowledgement {
            debug!(
                sequence = acknowledgement.sequence,
                snapshot = acknowledgement
                    .snapshot
                    .as_ref()
                    .map(|locator| locator.snapshot.generation),
                "Staged a Store acknowledgement"
            );
        }
        Box::pin(self.drain_acknowledgements())
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
        Ok(())
    }

    /// Stand on the snapshot this device has already acknowledged.
    ///
    /// Its own cycle stage, not a step of publishing an acknowledgement,
    /// because the licence is the statement the device has already made and not
    /// the act of making another. A device with nothing new to say never stages
    /// one; a device whose store has moved past every published snapshot can no
    /// longer name one to stage; and both of those describe a device with a full
    /// retained history to retire. Reading the licence out of what a device is
    /// about to say finds nothing in exactly those cases.
    ///
    /// Idempotent: adopting a cut the baseline already holds retires nothing,
    /// and the ordinary answer once a device has caught up is
    /// [`ReplayBaselineDecline::BaselineAtCoverage`], reached without reading
    /// anything from the provider.
    pub(crate) async fn stand_on_acknowledged_snapshot(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<ReplayBaselineAdvance, StoreAckError> {
        let registration = self.writer.local_registration_ref().clone();
        let members = self.writer.membership().clone();
        let resolved = self
            .writer
            .resolve_acknowledged_snapshot(&registration, &members)
            .await?;
        let selected = match resolved {
            Ok(selected) => selected,
            Err(decline) => return Ok(ReplayBaselineAdvance::Declined(decline)),
        };
        let generation = selected.snapshot.reference.generation;
        let advanced = match self.advance_over(Some(selected), routing_encryption).await {
            Ok(advanced) => advanced,
            Err(StoreAckError::Database(coven_database::DbError::ReplayRetirementCutNotPrefix)) => {
                return Ok(ReplayBaselineAdvance::Declined(
                    ReplayBaselineDecline::NonPrefixCut { generation },
                ));
            }
            Err(error) => return Err(error),
        };
        match advanced {
            Some(advanced) => Ok(ReplayBaselineAdvance::Advanced(advanced)),
            // The cut this snapshot covers does not move the baseline forward,
            // which the coverage check above did not catch: the baseline is at
            // or past it by a route the coverage comparison did not see.
            None => Ok(ReplayBaselineAdvance::Declined(
                ReplayBaselineDecline::BaselineAtCoverage { generation },
            )),
        }
    }

    async fn snapshot_it_will_name(
        &mut self,
        frontier: &CommitFrontier,
        device_state: &coven_protocol::store_commit::StoreDeviceStateRef,
        settled: Option<&crate::sync::store::SettledCycle>,
    ) -> Result<Option<StoreSnapshotLocator>, StoreAckError> {
        // Which snapshot a device could acknowledge next is settled by the same
        // local facts reclaim's answer is, and reaching it means reading every
        // activated device's snapshot stream. A device with nothing new to say
        // asks that question every cycle and gets the same answer, so the
        // answer is remembered against the facts it was reached from and the
        // read is skipped until one of them moves.
        let inputs = match settled {
            Some(settled) => {
                let inputs =
                    crate::sync::store::CycleInputs::read(&self.database, self.writer.membership())
                        .await?;
                if let Some(remembered) = settled.acknowledgeable_snapshot(&inputs) {
                    return Ok(remembered);
                }
                Some((settled, inputs))
            }
            None => None,
        };
        let selected = self
            .writer
            .select_acknowledgement_snapshot(frontier, device_state)
            .await?;
        if let Some((settled, inputs)) = &inputs {
            settled.record_acknowledgeable_snapshot(
                inputs.clone(),
                selected.as_ref().map(|selected| StoreSnapshotLocator {
                    author_registration: selected.snapshot.meta.author_registration.clone(),
                    snapshot: selected.snapshot.reference.clone(),
                }),
            );
        }
        let Some(selected) = selected else {
            return Ok(None);
        };
        let locator = StoreSnapshotLocator {
            author_registration: selected.snapshot.meta.author_registration.clone(),
            snapshot: selected.snapshot.reference.clone(),
        };
        Ok(Some(locator))
    }

    async fn advance_over(
        &mut self,
        snapshot: Option<SelectedReplayBaselineRetirement>,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<Option<coven_database::AdvancedReplayBaseline>, StoreAckError> {
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        Ok(self
            .database
            .advance_snapshot_replay_baseline(
                self.writer.store_root().clone(),
                snapshot.verified,
                routing_encryption.cloned(),
            )
            .await?)
    }

    /// Stage this device's acknowledgement of `frontier`, unless the one it
    /// already published still says the same thing.
    ///
    /// Publishing an acknowledgement appends a commit, so an acknowledgement that
    /// asserts nothing new still lands in every device's history, every retained
    /// materialization, and every snapshot taken afterwards. Without a guard the
    /// device acknowledges its own acknowledgement and a Store where nothing is
    /// happening grows one commit per device per sync cycle, without end.
    ///
    /// [`StoreAckAssertion`] is what an acknowledgement claims; the rest of it —
    /// the sequence, the wall clock, the links to its neighbours — differs by
    /// construction and says nothing. The one subtlety is the frontier: an
    /// acknowledgement cannot cover the commit that carries it, so the standing
    /// state records that commit and the comparison treats it as covered.
    /// Anything else in the frontier having moved is new material to acknowledge.
    ///
    /// Returns the acknowledgement it staged, or `None` when the standing one
    /// still holds, alongside what advancing the baseline retired.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn stage_acknowledgement(
        &mut self,
        frontier: CommitFrontier,
        sync_time: String,
    ) -> Result<StagedStoreAcknowledgement, StoreAckError> {
        self.stage_acknowledgement_against(frontier, sync_time, None)
            .await
    }

    async fn stage_acknowledgement_against(
        &mut self,
        frontier: CommitFrontier,
        sync_time: String,
        settled: Option<&crate::sync::store::SettledCycle>,
    ) -> Result<StagedStoreAcknowledgement, StoreAckError> {
        let history_cut =
            coven_protocol::store_commit::StoreHistoryCut::from_commits(frontier.commits().clone());
        let (device_state, _) = self
            .database
            .store_device_state_for_history_cut(&history_cut)
            .await?;
        let previous = self.database.latest_local_store_ack().await?;
        let snapshot = self
            .snapshot_it_will_name(&frontier, &device_state, settled)
            .await?;
        let acknowledgement = self
            .say_acknowledgement(history_cut, device_state, previous, snapshot, sync_time)
            .await?;
        Ok(StagedStoreAcknowledgement { acknowledgement })
    }

    /// Stage the acknowledgement itself after selecting the snapshot it names.
    /// Publishing the durable promise does not retire local replay inputs;
    /// retirement later requires every current writer to have crossed the cut.
    async fn say_acknowledgement(
        &mut self,
        history_cut: coven_protocol::store_commit::StoreHistoryCut,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        previous: Option<coven_database::PublishedStoreAck>,
        snapshot: Option<StoreSnapshotLocator>,
        sync_time: String,
    ) -> Result<Option<StoreAck>, StoreAckError> {
        let device_id = self.writer.local_device_id().to_string();
        let root = self.writer.store_root().clone();
        let exclusions = coven_protocol::store_commit::StoreAckExclusionState {
            proposal_freezes: self.database.store_device_exclusion_freezes().await?,
        };
        if self.database.oldest_outbound_store_ack().await?.is_some() {
            return Err(StoreAckError::InvalidOutbound(
                "a prior acknowledgement remains queued".to_string(),
            ));
        }
        let assertion = self.local_writer.device_acknowledgement_assertion(
            history_cut,
            device_state.clone(),
            snapshot,
            exclusions,
        );
        let membership_state =
            coven_protocol::circle_control::StoreMembershipStateRef::from_membership(
                self.writer.membership(),
                device_state.recovery().to_vec(),
            )?;
        // A queued Circle acknowledgement travels to the cloud inside the Store
        // acknowledgement's commit, so one waiting is reason enough to publish
        // even when this device has nothing of its own left to say.
        let carries_circle_acknowledgements = self.database.outbound_circle_acks_pending().await?;
        let standing_still_holds = previous
            .as_ref()
            .and_then(|previous| previous.standing.as_ref())
            .is_some_and(|standing| {
                standing.still_holds(&assertion)
                    && standing
                        .activating_commit
                        .as_ref()
                        .and_then(|commit| self.writer.accepted_commit_membership_state(commit))
                        == Some(&membership_state)
            });
        if !carries_circle_acknowledgements && standing_still_holds {
            debug!("skip Store acknowledgement: the standing one still holds");
            return Ok(None);
        }
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
            .map_err(StoreAckError::from)?;
        let acknowledgement = self
            .local_writer
            .sign_device_acknowledgement(
                root.store_root_hash,
                sequence,
                assertion,
                sync_time,
                SuccessorLink {
                    activation,
                    predecessor,
                    next_slot,
                },
            )
            .map_err(StoreAckError::from)?;
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
        Ok(Some(acknowledgement))
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
                if activated.reference == outbound.reference {
                    self.database
                        .complete_outbound_store_ack(
                            outbound.reference,
                            activated.activating_commit,
                        )
                        .await?;
                    published = published
                        .checked_add(1)
                        .ok_or(StoreAckError::PublishCountExhausted)?;
                    continue;
                }
                if activated.reference.sequence >= outbound.reference.sequence {
                    return Err(StoreAckError::InvalidOutbound(
                        "queued Store acknowledgement differs from the activated exact ref"
                            .to_string(),
                    ));
                }
            }
            let candidate = match outbound.activation.clone() {
                coven_database::OutboundStoreAckActivation::AwaitingCandidate => {
                    let plan = self.writer.prepare_plan().await?;
                    plan.validate_acknowledgement(&outbound.ack.value)?;
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
            let semantic_prefix = ack_slot_prefix(&device_id, outbound.reference.sequence);
            if let Err(error) = self
                .storage
                .create_verified_protocol_object(
                    &context,
                    &outbound.ack.prepared,
                    &semantic_prefix,
                    &outbound.ack.bytes,
                )
                .await
            {
                if !matches!(
                    error,
                    coven_protocol::objects::StorageError::SlotCollision(_)
                ) {
                    return Err(StoreObjectError::from(error).into());
                }
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
                .mark_remote_object_uploaded(acknowledgement_remote.into_record())
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
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationPublicationOutcome::Activated(activating_commit) => {
                    self.database
                        .complete_outbound_store_ack(outbound.reference, activating_commit)
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
