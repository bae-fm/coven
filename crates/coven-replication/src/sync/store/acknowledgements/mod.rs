//! Store and Circle acknowledgement publication.

mod circle;

pub(crate) use circle::CircleAcknowledgementReader;

use super::snapshots as snapshot;
use super::{AuthorizedWriterOperation, StoreError};
use crate::sync::cycle::SyncCycleFailure;
use crate::sync::store::commit_publication::LocalStoreWriter;
use crate::sync::store::commit_verification::merge_history::SelectedStoreSnapshot;
use coven_database::StoreDatabase;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{ack_slot_prefix, CommitFrontier, StoreAck, SuccessorLink};
use coven_storage::CloudSyncObjectStorage;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, thiserror::Error)]
pub enum StoreAckError {
    #[error("Store acknowledgement membership: {0}")]
    Membership(#[from] coven_protocol::membership::MembershipError),
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

/// What standing on the latest accepted snapshot did, or why it did nothing.
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
    /// Accepted Store history contains no snapshot boundary.
    NoAcceptedSnapshot,
    /// Current accepted history applies a commit outside the snapshot cut
    /// before a commit inside it, so the cut cannot become a replay baseline.
    NonPrefixCut {
        snapshot: coven_protocol::store_commit::StoreSnapshotRef,
    },
    /// The steady state: the baseline already restates everything the
    /// accepted snapshot does.
    BaselineAtCoverage {
        snapshot: coven_protocol::store_commit::StoreSnapshotRef,
    },
}

impl ReplayBaselineDecline {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoAcceptedSnapshot => "accepted Store history contains no snapshot",
            Self::NonPrefixCut { .. } => {
                "the accepted snapshot is not a prefix of accepted replay order"
            }
            Self::BaselineAtCoverage { .. } => "the baseline already covers it",
        }
    }

    pub fn snapshot(&self) -> Option<&coven_protocol::store_commit::StoreSnapshotRef> {
        match self {
            Self::NoAcceptedSnapshot => None,
            Self::NonPrefixCut { snapshot } | Self::BaselineAtCoverage { snapshot } => {
                Some(snapshot)
            }
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
    /// publication, from the installed accepted snapshot boundary.
    pub(crate) async fn stage_and_publish(
        &mut self,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        self.drain_acknowledgements().await.map_err(|error| {
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
            Box::pin(self.stage_acknowledgement(frontier.clone(), sync_time.to_owned()))
                .await
                .map_err(|error| {
                    SyncCycleFailure::operation("stage Store acknowledgement", error)
                })?;
        if let Some(acknowledgement) = &acknowledgement {
            debug!(
                sequence = acknowledgement.sequence,
                "Staged a Store acknowledgement"
            );
        }
        self.drain_acknowledgements()
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
        Ok(())
    }

    /// Stand on the latest installed accepted snapshot.
    ///
    /// Idempotent: adopting a cut the baseline already holds retires nothing,
    /// and the ordinary answer once a device has caught up is
    /// [`ReplayBaselineDecline::BaselineAtCoverage`], reached without reading
    /// anything from the provider.
    pub(crate) async fn stand_on_accepted_snapshot(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<ReplayBaselineAdvance, StoreAckError> {
        let resolved = self.writer.resolve_accepted_snapshot().await?;
        let selected = match resolved {
            Ok(selected) => selected,
            Err(decline) => return Ok(ReplayBaselineAdvance::Declined(decline)),
        };
        let snapshot = selected.snapshot.reference.clone();
        let advanced = match self.advance_over(selected, routing_encryption).await {
            Ok(advanced) => advanced,
            Err(StoreAckError::Database(coven_database::DbError::ReplayRetirementCutNotPrefix)) => {
                return Ok(ReplayBaselineAdvance::Declined(
                    ReplayBaselineDecline::NonPrefixCut { snapshot },
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
                ReplayBaselineDecline::BaselineAtCoverage { snapshot },
            )),
        }
    }

    async fn advance_over(
        &mut self,
        snapshot: SelectedStoreSnapshot,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<Option<coven_database::AdvancedReplayBaseline>, StoreAckError> {
        Ok(self
            .database
            .advance_snapshot_replay_baseline(
                self.writer.store_root().clone(),
                snapshot.verified,
                routing_encryption.cloned(),
                self.local_writer.local_membership(self.writer.membership()),
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
    /// [`coven_protocol::store_commit::StoreAckAssertion`] is what an acknowledgement claims; the rest of it —
    /// the sequence, the wall clock, the links to its neighbours — differs by
    /// construction and says nothing. The one subtlety is the frontier: an
    /// acknowledgement cannot cover the commit that carries it, so the standing
    /// state records that commit and the comparison treats it as covered.
    /// For other advances, verified history distinguishes new work from peer
    /// acknowledgements so idle devices do not acknowledge each other forever.
    ///
    /// Returns the acknowledgement it staged, or `None` when the standing one
    /// still holds. Baseline retirement runs independently.
    pub(crate) async fn stage_acknowledgement(
        &mut self,
        frontier: CommitFrontier,
        sync_time: String,
    ) -> Result<StagedStoreAcknowledgement, StoreAckError> {
        let history_cut =
            coven_protocol::store_commit::StoreHistoryCut::from_commits(frontier.commits().clone());
        let (device_state, _) = self
            .database
            .store_device_state_for_history_cut(&history_cut)
            .await?;
        let previous = self.database.latest_local_store_ack().await?;
        let acknowledgement = self
            .say_acknowledgement(history_cut, device_state, previous, sync_time)
            .await?;
        Ok(StagedStoreAcknowledgement { acknowledgement })
    }

    /// Stage the observation without retiring local replay inputs.
    /// A separate stage adopts the accepted snapshot boundary.
    async fn say_acknowledgement(
        &mut self,
        history_cut: coven_protocol::store_commit::StoreHistoryCut,
        device_state: coven_protocol::store_commit::StoreDeviceStateRef,
        previous: Option<coven_database::PublishedStoreAck>,
        sync_time: String,
    ) -> Result<Option<StoreAck>, StoreAckError> {
        let device_id = self.writer.local_device_id().to_string();
        let root = self.writer.store_root().clone();
        if self.database.oldest_outbound_store_ack().await?.is_some() {
            return Err(StoreAckError::InvalidOutbound(
                "a prior acknowledgement remains queued".to_string(),
            ));
        }
        let assertion = self
            .local_writer
            .device_acknowledgement_assertion(history_cut, device_state);
        // A queued Circle acknowledgement travels to the cloud inside the Store
        // acknowledgement's commit, so one waiting is reason enough to publish
        // even when this device has nothing of its own left to say.
        let carries_circle_acknowledgements = self.database.outbound_circle_acks_pending().await?;
        let standing_still_holds = match previous
            .as_ref()
            .and_then(|previous| previous.standing.as_ref())
        {
            Some(standing) if standing.still_holds(&assertion) => true,
            Some(standing) if standing.assertion.same_state_as(&assertion) => self
                .writer
                .history_has_only_acknowledgements(
                    &standing.assertion.store_cut,
                    &assertion.store_cut,
                )
                .await
                .map_err(StoreError::from)?,
            Some(_) | None => false,
        };
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
        let mut authorship = self.database.author_own_stream().await;
        let device_id = self.writer.local_device_id().to_string();
        let mut published = 0_u64;
        while let Some(outbound) = self.database.oldest_outbound_store_ack().await? {
            if let Some(active) = self.database.active_store_publication().await? {
                if active.owner()
                    == &coven_database::ActiveStorePublicationOwner::StoreAcknowledgement
                {
                    crate::sync::store::authorization::retire_store_write_candidates(
                        &self.database,
                        self.storage.as_ref(),
                        active,
                    )
                    .await?;
                }
            }
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
                if activated.reference.sequence > outbound.reference.sequence {
                    return Err(StoreAckError::InvalidOutbound(
                        "queued Store acknowledgement differs from the activated exact ref"
                            .to_string(),
                    ));
                }
            }
            let candidate = match outbound.activation.clone() {
                coven_database::OutboundStoreAckActivation::AwaitingCandidate
                | coven_database::OutboundStoreAckActivation::Created => {
                    let plan = self.writer.prepare_plan_with_authorship(authorship).await?;
                    let created = matches!(
                        outbound.activation,
                        coven_database::OutboundStoreAckActivation::Created
                    );
                    if created
                        && self
                            .database
                            .activated_store_ack(&outbound.reference.registration)
                            .await?
                            .is_some_and(|activated| activated.reference == outbound.reference)
                    {
                        authorship = plan.into_authorship();
                        continue;
                    }
                    let (reference, acknowledgement) = if created {
                        if outbound.ack.value.store_cut != plan.predecessor_cut()?
                            || &outbound.ack.value.device_state != plan.device_state()
                        {
                            self.prepare_acknowledgement_successor(&outbound, &plan)
                                .await?
                        } else {
                            (outbound.reference.clone(), outbound.ack.clone())
                        }
                    } else {
                        self.prepare_acknowledgement_object(
                            &outbound,
                            &plan,
                            outbound.reference.sequence,
                            outbound.reference.object.slot().clone(),
                            outbound.ack.value.successor.clone(),
                        )?
                    };
                    plan.validate_acknowledgement(&acknowledgement.value)?;
                    let mut candidate = Box::pin(self.writer.prepare_candidate(
                    &plan,
                    crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Acknowledgement {
                        reference: reference.clone(),
                        value: acknowledgement.value.clone(),
                        circle_acknowledgements: outbound.circle_acknowledgements.clone(),
                    },
                ))
                .await?;
                    if created && reference != outbound.reference {
                        candidate
                            .history_evidence
                            .acknowledgement
                            .as_mut()
                            .ok_or_else(|| {
                                StoreAckError::InvalidOutbound(
                                    "prepared acknowledgement omits its retained proof".into(),
                                )
                            })?
                            .predecessors =
                            vec![(outbound.reference.clone(), outbound.ack.value.clone())];
                        candidate.validate_closed_shape()?;
                    }
                    let claimed = self
                        .database
                        .prepare_acknowledgement_activation(
                            outbound.reference.clone(),
                            acknowledgement,
                            candidate,
                        )
                        .await?;
                    if !claimed {
                        return Ok(published);
                    }
                    authorship = plan.into_authorship();
                    continue;
                }
                coven_database::OutboundStoreAckActivation::Prepared(candidate) => candidate,
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
            let outcome = Box::pin(self.writer.publish_prepared_attempt(
                Box::new(candidate.clone()),
                None,
                None,
            ))
            .await?;
            let accepted = match outcome {
            crate::sync::store::commit_publication::operation::StoreOperationPublicationOutcome::Accepted(accepted) => accepted,
            crate::sync::store::commit_publication::operation::StoreOperationPublicationOutcome::SnapshotRetired(snapshot) => {
                authorship = self.replace_snapshot_acknowledgement(&outbound, &candidate, snapshot, authorship).await?;
                continue;
            }
        };
            self.database
                .complete_outbound_store_ack(outbound.reference, accepted.commit_ref().clone())
                .await?;
            published = published
                .checked_add(1)
                .ok_or(StoreAckError::PublishCountExhausted)?;
        }
        Ok(published)
    }

    async fn prepare_acknowledgement_successor(
        &self,
        outbound: &coven_database::OutboundStoreAck,
        plan: &crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreAckRef,
            coven_protocol::objects::ExactProtocolObject<StoreAck>,
        ),
        StoreAckError,
    > {
        let sequence = outbound.reference.sequence.checked_add(1).ok_or_else(|| {
            StoreAckError::InvalidOutbound("Store acknowledgement sequence overflow".into())
        })?;
        let following = sequence.checked_add(1).ok_or_else(|| {
            StoreAckError::InvalidOutbound("Store acknowledgement sequence overflow".into())
        })?;
        let context = ProtocolObjectContext::signed_plaintext(
            plan.root().store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let next_slot = self
            .storage
            .allocate_protocol_slot(
                &context,
                &ack_slot_prefix(&self.writer.local_device_id().to_string(), following),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        self.prepare_acknowledgement_object(
            outbound,
            plan,
            sequence,
            outbound.ack.value.successor.next_slot.clone(),
            SuccessorLink {
                activation: outbound.ack.value.successor.activation,
                predecessor: Some(outbound.reference.object.clone()),
                next_slot,
            },
        )
    }

    fn prepare_acknowledgement_object(
        &self,
        outbound: &coven_database::OutboundStoreAck,
        plan: &crate::sync::store::commit_publication::operation::commit_plan::StoreOperationCommitPlan,
        sequence: u64,
        slot: coven_protocol::objects::ObjectSlot,
        successor: SuccessorLink,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreAckRef,
            coven_protocol::objects::ExactProtocolObject<StoreAck>,
        ),
        StoreAckError,
    > {
        let value = self.local_writer.sign_device_acknowledgement(
            plan.root().store_root_hash,
            sequence,
            self.local_writer.device_acknowledgement_assertion(
                plan.predecessor_cut()?,
                plan.device_state().clone(),
            ),
            outbound.ack.value.last_sync.clone(),
            successor,
        )?;
        let bytes = value.to_bytes();
        let prepared = self
            .storage
            .prepare_protocol_object(
                &ProtocolObjectContext::signed_plaintext(
                    plan.root().store_root_hash,
                    ProtocolObjectDomain::StoreAck,
                ),
                slot,
                &ack_slot_prefix(&self.writer.local_device_id().to_string(), sequence),
                bytes.clone(),
            )
            .map_err(StoreObjectError::from)?;
        let reference = coven_protocol::store_commit::StoreAckRef {
            registration: value.registration.clone(),
            sequence,
            ack_hash: value.ack_hash(),
            object: prepared.reference().clone(),
        };
        Ok((
            reference,
            coven_protocol::objects::ExactProtocolObject {
                value,
                bytes,
                prepared,
            },
        ))
    }

    async fn replace_snapshot_acknowledgement(
        &mut self,
        outbound: &coven_database::OutboundStoreAck,
        previous: &coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        snapshot: coven_protocol::store_commit::AcceptedStoreSnapshotRef,
        authorship: coven_database::OwnStreamAuthorship,
    ) -> Result<coven_database::OwnStreamAuthorship, StoreAckError> {
        let plan = self.writer.prepare_plan_with_authorship(authorship).await?;
        let (reference, acknowledgement) = self
            .prepare_acknowledgement_successor(outbound, &plan)
            .await?;
        let coven_protocol::objects::ExactProtocolObject {
            value,
            bytes,
            prepared,
        } = acknowledgement;
        plan.validate_acknowledgement(&value)?;
        let mut candidate = self.writer.prepare_replacement_candidate(
            &plan,
            crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::Acknowledgement {
                reference, value: value.clone(),
                circle_acknowledgements: outbound.circle_acknowledgements.clone(),
            },
            previous,
        ).await?;
        let previous_proof = previous
            .history_evidence
            .acknowledgement
            .as_ref()
            .ok_or_else(|| {
                StoreAckError::InvalidOutbound(
                    "queued acknowledgement omits its retained proof".into(),
                )
            })?;
        let retained = candidate
            .history_evidence
            .acknowledgement
            .as_mut()
            .ok_or_else(|| {
                StoreAckError::InvalidOutbound(
                    "replacement acknowledgement omits its retained proof".into(),
                )
            })?;
        retained.predecessors = previous_proof.proof_objects().cloned().collect();
        candidate.validate_closed_shape()?;
        self.database
            .replace_acknowledgement_activation(
                outbound.reference.clone(),
                snapshot,
                coven_protocol::objects::ExactProtocolObject {
                    value,
                    bytes,
                    prepared,
                },
                candidate,
            )
            .await?;
        Ok(plan.into_authorship())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod idle_tests;
