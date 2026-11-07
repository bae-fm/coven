//! One journal owns validation, prerequisite artifacts, claims and undo.

use crate::sequence_sync::OperationState;
use crate::{
    sequence_sync::snapshot_capture::{Capture, SnapshotArtifact, SnapshotRequest},
    sequence_sync::{next_slot, SequenceSync, Session},
    slot_object, OperationOutcome, OperationRejection, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::operation_kind;
use coven_database::{
    ClaimAttempt, ClaimSettlement, ClaimWork, Journal, NewOperation, OperationId, OperationRecord,
};
use coven_format::{
    admin::StoreChange,
    sealed_key::{CopyBox, KeyCopyPrefix},
    sealed_slot::{SlotKind, SlotObjectPrefix},
    sequence::{Purpose, SlotFrame, SlotHeader},
};
use coven_merge::{Audience, Slot};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use super::sequence_copies::{required_copies, route};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Request {
    Change(#[serde(with = "change")] StoreChange),
    Snapshot(SnapshotRequest),
    Key(super::sequence_keys::KeyPreparation),
    /// One key copy this device's member owes a current member (§12.1).
    Copy(#[serde(with = "route")] KeyCopyPrefix),
}
#[derive(Serialize, Deserialize)]
pub(crate) enum OperationStep {
    Preparing,
    Copy(#[serde(with = "route")] KeyCopyPrefix),
    Claiming,
    CreatorCopy(#[serde(with = "coven_database::slot_json")] Slot),
    Capturing(Capture),
    Uploading(SnapshotArtifact),
    Announcing(SnapshotArtifact),
    Activating {
        artifact: SnapshotArtifact,
        #[serde(with = "coven_database::slot_json")]
        announcement: Slot,
    },
    Applied(#[serde(with = "coven_database::slot_json")] Slot),
    SnapshotApplied {
        artifact: SnapshotArtifact,
        #[serde(with = "coven_database::slot_json")]
        slot: Slot,
    },
    Satisfied,
}
#[derive(Serialize, Deserialize)]
pub(crate) enum OperationProgress {
    Running(OperationStep),
    Undo { step: OperationStep, reason: OperationRejection },
}
#[derive(Serialize, Deserialize)]
pub(crate) struct SequenceOperation {
    pub(crate) request: Request,
    pub(crate) progress: OperationProgress,
}
impl OperationState for SequenceOperation {}

impl SequenceOperation {
    /// The journal row that starts this operation, before its first step.
    pub(crate) fn journaled(&self, started_by: &str) -> Result<NewOperation, SyncError> {
        Ok(NewOperation {
            kind: operation_kind::SEQUENCE.into(),
            data: serde_json::to_vec(self)?,
            started_by: started_by.into(),
            pauses_writes: false,
        })
    }

    /// Journal the operation as one the app's write asked for.
    pub(crate) fn start(
        self,
        journal: &Journal<'_>,
        started_by: &str,
    ) -> Result<OperationId, SyncError> {
        let kind = self.kind().listed();
        Ok(journal.start_app_operation(&self.journaled(started_by)?, &kind)?)
    }

    /// An administrative change, before its first step.
    pub(crate) fn change(change: StoreChange) -> Self {
        Self {
            request: Request::Change(change),
            progress: OperationProgress::Running(OperationStep::Preparing),
        }
    }

    pub(crate) fn kind(&self) -> crate::OperationKind {
        match &self.request {
            Request::Key(target) => target.kind(),
            Request::Copy(_) => crate::OperationKind::SendKeyCopy,
            Request::Change(change) => operation_kind(change),
            Request::Snapshot(request) => match request.purpose {
                Purpose::Ordinary => crate::OperationKind::WriteSnapshot,
                Purpose::Raise(_) => crate::OperationKind::SchemaChange,
                Purpose::Reset => crate::OperationKind::Reset,
            },
        }
    }
}

impl SequenceSync {
    /// Administrative requests retain their target across contention and reopening.
    pub async fn administer(
        self: &std::sync::Arc<Self>,
        change: StoreChange,
    ) -> Result<(), SyncError> {
        self.administer_at(change).await.map(drop)
    }

    /// Administer `change`, answering the slot its entry landed at, or
    /// `None` when the applied state already had it. A store's creation
    /// lands at slot 1, though its key copy lands after it.
    pub async fn administer_at(
        self: &std::sync::Arc<Self>,
        change: StoreChange,
    ) -> Result<Option<Slot>, SyncError> {
        self.submit(Request::Change(change)).await
    }

    /// Catch up, settle this device's open claim and run its journaled
    /// operations to their ends, as every new operation does first (§11.4).
    /// A storage setup a crash cut past its point of no return finishes a
    /// creation this way.
    pub async fn settle_operations(self: &std::sync::Arc<Self>) -> Result<(), SyncError> {
        let owner = self.clone();
        let settling = async move {
            let mut session = owner.session.lock().await;
            let member =
                owner.member.read()?.ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            let mut ring = owner.ring(&mut session, &member).await?;
            owner.read_forward(&mut session, &mut ring, &member).await?;
            owner.settle_open(&mut session, &mut ring, &member).await?;
            owner.resume_administration(&mut session, &mut ring, &member).await
        };
        coven_foundation::task::spawn_completing(
            "settle operations",
            self.database.bugs().clone(),
            settling,
        )
        .await?
    }

    /// Register this installation for its member once the applied history
    /// binds the member's current pair (§13). `replaces` names this member's
    /// earlier installation that a reset supersedes.
    pub async fn register_device(
        self: &std::sync::Arc<Self>,
        name: String,
        replaces: Option<coven_foundation::id_source::DeviceId>,
    ) -> Result<(), SyncError> {
        self.submit(Request::Change(StoreChange::AddDevice { name, replaces })).await.map(drop)
    }

    /// Capture the counted audience prefix, upload it, then claim its announcement.
    pub async fn snapshot(
        self: &std::sync::Arc<Self>,
        audience: Audience,
    ) -> Result<Slot, SyncError> {
        self.submit(Request::Snapshot(SnapshotRequest { audience, purpose: Purpose::Ordinary }))
            .await
            .map(|slot| slot.expect("snapshot completion has a counted announcement"))
    }

    /// Publish a prepared reset and activate it through the same operation journal.
    pub async fn reset(self: &std::sync::Arc<Self>, audience: Audience) -> Result<(), SyncError> {
        self.submit(Request::Snapshot(SnapshotRequest { audience, purpose: Purpose::Reset }))
            .await
            .map(drop)
    }

    /// Publish the captured breaking migration and activate its audience version.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn raise_schema(
        self: &std::sync::Arc<Self>,
        audience: Audience,
        version: u32,
    ) -> Result<(), SyncError> {
        self.submit(Request::Snapshot(SnapshotRequest {
            audience,
            purpose: Purpose::Raise(version),
        }))
        .await
        .map(drop)
    }

    async fn submit(
        self: &std::sync::Arc<Self>,
        request: Request,
    ) -> Result<Option<Slot>, SyncError> {
        let owner = self.clone();
        // The owned task survives cancellation of the app's waiter, including
        // requests that the provider cannot cancel once sent.
        let submitting = async move {
            let mut session = owner.session.lock().await;
            let member =
                owner.member.read()?.ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            let mut ring = owner.ring(&mut session, &member).await?;
            owner.read_forward(&mut session, &mut ring, &member).await?;
            owner.settle_open(&mut session, &mut ring, &member).await?;
            owner.resume_administration(&mut session, &mut ring, &member).await?;
            if let Request::Change(StoreChange::AddDevice { .. }) = &request {
                super::sequence_bootstrap::restorable(owner.admission(&session, &member), &member)?;
                // A registration journaled before a restart has just ended:
                // landed, this device is registered and needs no other.
                if session.history.state.device_acts_for(&owner.device, &member.member()) {
                    return Ok(None);
                }
            }
            // Recording a key the journal already records runs that record
            // now: a second one's claim would meet the first's open claim
            // (§14.3).
            if let Request::Change(StoreChange::SetAccess { member: recorded, access }) = &request {
                if let Some(id) = owner.access_operation(*recorded, access).await? {
                    owner.retry_operation(id).await?;
                    return owner
                        .run_administration(&mut session, &mut ring, &member, id)
                        .await
                        .and_then(crate::operation_outcome::into_slot);
                }
            }
            let creation = matches!(request, Request::Change(StoreChange::CreateStore { .. }));
            match owner
                .start_request(&mut session, &mut ring, &member, request, "operation")
                .await
                .and_then(crate::operation_outcome::into_slot)
            {
                // Another creation claimed slot 1 first (§10).
                Err(SyncError::Rejected(_)) if creation => {
                    Err(coven_storage::StorageFailure::StoreExists.into())
                }
                result => result,
            }
        };
        coven_foundation::task::spawn_completing(
            "submit an operation",
            self.database.bugs().clone(),
            submitting,
        )
        .await?
    }

    /// Journal `request` as a new sequence operation, before its first step.
    pub(crate) async fn start_request_row(
        &self,
        request: Request,
        started_by: &str,
    ) -> Result<OperationId, SyncError> {
        let work = SequenceOperation {
            request,
            progress: OperationProgress::Running(OperationStep::Preparing),
        };
        Ok(self.database.start_operation(work.journaled(started_by)?).await?)
    }

    pub(crate) async fn start_request(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        request: Request,
        started_by: &str,
    ) -> Result<OperationOutcome, SyncError> {
        if !self.validate_request(session, member, &request)?? {
            return Ok(OperationOutcome::Satisfied);
        }
        let id = self.start_request_row(request, started_by).await?;
        self.run_administration(session, ring, member, id).await
    }

    fn operation_header(
        &self,
        session: &Session,
        member: &MemberIdentity,
        claim: u64,
    ) -> SlotHeader {
        SlotHeader { device: self.device, claim, author: member.member(), after: session.through }
    }

    /// Whether `request` still has work to claim, or the applied history's
    /// rejection of it. Validating a change needs the slot it would take,
    /// and none follows the last one.
    pub(crate) fn validate_request(
        &self,
        session: &Session,
        member: &MemberIdentity,
        request: &Request,
    ) -> Result<Result<bool, OperationRejection>, SyncError> {
        Ok(match request {
            Request::Key(_) => unreachable!("key preparation supplies its public commitment first"),
            Request::Change(change) => session
                .history
                .validate(
                    &self.operation_header(session, member, 1),
                    next_slot(session.through)?,
                    change,
                )
                .map(|effect| effect.is_some()),
            Request::Copy(route) => Ok(session.history.copy_wanted(route, member.member())),
            Request::Snapshot(request) => self.validate_snapshot(session, member, request),
        })
    }

    /// Whether a snapshot `request` still has work to claim, or the applied
    /// history's rejection of it.
    fn validate_snapshot(
        &self,
        session: &Session,
        member: &MemberIdentity,
        request: &SnapshotRequest,
    ) -> Result<bool, OperationRejection> {
        // Nobody raises a deleted circle: a raise started before the
        // deletion applied finds nothing to claim, and adopts that
        // state as it would an established version (§15.9, §18.3).
        if matches!(request.purpose, Purpose::Raise(_))
            && session.history.state.deleted(&request.audience)
        {
            return Ok(false);
        }
        let state = &session.history.state;
        let author = member.member();
        if state.active_member(&author).is_none() || !state.device_acts_for(&self.device, &author) {
            return Err(OperationRejection::NotAllowed);
        }
        // A snapshot of a gone circle meets a gone target, as every
        // change aimed at it does (§10.4).
        if let Audience::Circle(circle) = &request.audience {
            if state.gone_circle(circle) {
                return Err(OperationRejection::TargetGone(crate::EntryTarget::Circle(*circle)));
            }
        }
        if !crate::effects::belongs(state, &request.audience, &author)
            || (request.purpose == Purpose::Reset
                && request.audience == Audience::Store
                && !state.active_admin(&author))
        {
            return Err(OperationRejection::NotAllowed);
        }
        // A raise that finds its version established adopts that
        // state and claims nothing (§18.3).
        if let Purpose::Raise(version) = request.purpose {
            return Ok(session.history.state.reached(&request.audience) < version);
        }
        Ok(true)
    }

    /// Whether a journaled operation already carries `request`'s kind of work.
    pub(crate) async fn journaled(
        &self,
        carries: impl Fn(&Request) -> bool,
    ) -> Result<bool, SyncError> {
        for record in self.database.operations().await? {
            if record.kind == operation_kind::SEQUENCE
                && carries(&serde_json::from_slice::<SequenceOperation>(&record.data)?.request)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Coven's own work started in a pass: once journaled, its failure is the
    /// operation's to report and retry under its backoff, and the pass goes
    /// on (E5). A failure before journaling, or one stopping this
    /// installation, ends the pass.
    pub(crate) async fn own_work(
        &self,
        result: Result<(), SyncError>,
        carries: impl Fn(&Request) -> bool,
    ) -> Result<(), SyncError> {
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.state.check()?;
                if self.journaled(carries).await? {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(crate) async fn resume_administration(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let records = self.database.operations().await?;
        self.waits.retain_operations(&records.iter().map(|r| r.id).collect());
        for record in records {
            if record.kind == operation_kind::SEQUENCE
                && !parent_owned(&record)
                && self.operation_ready(&record, ring).await?
            {
                // Its outcome is the journal's to keep: one the app's write
                // journaled is listed until the app acknowledges it (§11.4).
                let outcome = self.run_administration(session, ring, member, record.id).await?;
                tracing::debug!(id = ?record.id, ?outcome, "resumed operation ended");
            }
        }
        Ok(())
    }

    pub(crate) async fn run_administration(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<OperationOutcome, SyncError> {
        let record = self.database.operation(id).await?.ok_or(SyncError::NotPending(id))?;
        if parent_owned(&record) {
            // The parent records the failure and owns its retry scope (§11.4).
            return self.advance_administration(session, ring, member, id).await;
        }
        let scope = self.waits.begin_attempt(id);
        let result = scope.run(self.advance_administration(session, ring, member, id)).await;
        if let Err(error) = &result {
            if let Some(record) = self.database.operation(id).await? {
                let work: SequenceOperation = serde_json::from_slice(&record.data)?;
                self.record_operation_error(
                    id,
                    work.kind(),
                    crate::sequence_sync::OperationFailure::Returned(error),
                    ring,
                )
                .await?;
            }
        } else {
            self.waits.operation_advanced(id);
        }
        result
    }

    async fn advance_administration(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<OperationOutcome, SyncError> {
        loop {
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(coven_database::DbError::OperationChanged(id))?;
            let work: SequenceOperation = serde_json::from_slice(&record.data)?;
            let SequenceOperation { request, progress } = work;
            let mut step = match progress {
                OperationProgress::Undo { step, reason } => {
                    self.abandon_settled_claim(id).await?;
                    if let Err(cleanup) = self.undo_step(&step).await {
                        return Err(SyncError::cleanup(reason.into(), cleanup));
                    }
                    return self.finish(&record, OperationOutcome::Rejected(reason)).await;
                }
                OperationProgress::Running(step) => step,
            };
            match &step {
                OperationStep::Applied(slot) => {
                    return self.finish(&record, OperationOutcome::Landed(*slot)).await
                }
                OperationStep::Satisfied => {
                    return self.finish(&record, OperationOutcome::Satisfied).await
                }
                OperationStep::SnapshotApplied { artifact, slot } => {
                    self.artifact_file(&artifact.capture.name)?.remove()?;
                    return self.finish(&record, OperationOutcome::Landed(*slot)).await;
                }
                _ => (),
            }
            if let Request::Key(target) = request {
                self.prepare_audience_key(session, ring, member, &record, target).await?;
                continue;
            }
            if let Some(claim) = self.database.current_claim().await? {
                if claim.work != ClaimWork::Operation(id) {
                    return Err(coven_database::DbError::ClaimPending.into());
                }
                match self.settle_claim(session, ring, member, &claim).await? {
                    ClaimSettlement::Landed => {
                        let slot = claim
                            .attempts
                            .last()
                            .expect("claim attempt")
                            .slot_number()
                            .expect("slot");
                        step = match step {
                            OperationStep::Copy(_) => OperationStep::Preparing,
                            OperationStep::Claiming
                                if matches!(
                                    request,
                                    Request::Change(StoreChange::CreateStore { .. })
                                ) =>
                            {
                                OperationStep::CreatorCopy(slot)
                            }
                            OperationStep::Claiming => OperationStep::Applied(slot),
                            OperationStep::CreatorCopy(creation) => {
                                OperationStep::Applied(creation)
                            }
                            OperationStep::Announcing(artifact)
                                if artifact.capture.purpose == Purpose::Ordinary =>
                            {
                                OperationStep::SnapshotApplied { artifact, slot }
                            }
                            OperationStep::Announcing(artifact) => {
                                OperationStep::Activating { artifact, announcement: slot }
                            }
                            OperationStep::Activating { artifact, .. } => {
                                OperationStep::SnapshotApplied { artifact, slot }
                            }
                            _ => unreachable!("a publication step owns each claim"),
                        };
                        self.database
                            .finish_claim(Some(
                                SequenceOperation {
                                    request,
                                    progress: OperationProgress::Running(step),
                                }
                                .update(&record)?,
                            ))
                            .await?;
                        continue;
                    }
                    ClaimSettlement::NotLanded => {
                        if claim
                            .attempts
                            .last()
                            .expect("attempt")
                            .slot_number()
                            .expect("slot")
                            .get()
                            > session.through
                        {
                            match self.read_forward(session, ring, member).await {
                                Ok(())
                                | Err(SyncError::Removed | SyncError::MemberKeysReplaced { .. }) => {
                                }
                                Err(error) => return Err(error),
                            }
                            if matches!(step, OperationStep::Copy(_) | OperationStep::Claiming) {
                                step = OperationStep::Preparing;
                            }
                        }
                    }
                    ClaimSettlement::DeviceIdInUse => {
                        return Err(self
                            .stop_copy(crate::DeviceIdEvidence::UnattemptedSlot {
                                slot: claim
                                    .attempts
                                    .last()
                                    .expect("attempt")
                                    .slot_number()
                                    .expect("slot"),
                            })
                            .await)
                    }
                    _ => unreachable!("settled create"),
                }
            }
            let validation = if matches!(step, OperationStep::CreatorCopy(_)) {
                self.check_binding(session, member)?;
                Ok(true)
            } else {
                self.validate_request(session, member, &request)?
            };
            match validation {
                Ok(false) => {
                    self.abandon_settled_claim(id).await?;
                    self.undo_step(&step).await?;
                    self.database
                        .advance_operation(
                            SequenceOperation {
                                request,
                                progress: OperationProgress::Running(OperationStep::Satisfied),
                            }
                            .update(&record)?,
                        )
                        .await?;
                    continue;
                }
                Err(reason) => {
                    self.database
                        .advance_operation(
                            SequenceOperation {
                                request,
                                progress: OperationProgress::Undo { step, reason },
                            }
                            .update(&record)?,
                        )
                        .await?;
                    continue;
                }
                Ok(true) => (),
            }
            if matches!(step, OperationStep::Preparing) {
                step = match &request {
                    Request::Key(_) => unreachable!("key preparation has advanced"),
                    Request::Copy(route) => OperationStep::Copy(route.clone()),
                    Request::Change(change) => {
                        match required_copies(&session.history, ring, change, member.member())
                            .into_iter()
                            .find(|route| !session.history.has_copy(route))
                        {
                            Some(route) => OperationStep::Copy(route),
                            None => OperationStep::Claiming,
                        }
                    }
                    Request::Snapshot(request) => {
                        let number = self.database.reserve_snapshot_number().await?.get();
                        OperationStep::Capturing(Capture {
                            id: coven_format::admin::SnapshotId {
                                audience: request.audience.clone(),
                                device: self.device,
                                number,
                            },
                            purpose: request.purpose,
                            key: session.history.sending_key(
                                ring,
                                member.member(),
                                &request.audience,
                            )?,
                            name: format!("snapshot-{number}.sealed"),
                        })
                    }
                };
                self.database
                    .advance_operation(
                        SequenceOperation { request, progress: OperationProgress::Running(step) }
                            .update(&record)?,
                    )
                    .await?;
                continue;
            }
            match step {
                OperationStep::Capturing(capture) => {
                    // No snapshot is taken at a version the history doesn't
                    // hold while this device's raise waits (§16.2).
                    let progress = match self.capture_snapshot(&capture, ring, member).await {
                        Err(SyncError::Database(coven_database::DbError::RaiseWaiting)) => {
                            OperationProgress::Undo {
                                step: OperationStep::Capturing(capture),
                                reason: OperationRejection::NotAllowed,
                            }
                        }
                        // A raise whose capture moved to a later version has
                        // nothing left to raise; that capture raises it (§18.5).
                        Ok(None) => OperationProgress::Running(OperationStep::Satisfied),
                        Ok(Some(artifact)) => {
                            OperationProgress::Running(OperationStep::Uploading(artifact))
                        }
                        Err(error) => return Err(error),
                    };
                    self.database
                        .advance_operation(SequenceOperation { request, progress }.update(&record)?)
                        .await?;
                }
                OperationStep::Uploading(artifact) => {
                    self.upload_snapshot(record, request, artifact).await?;
                }
                OperationStep::Announcing(ref artifact)
                | OperationStep::Activating { ref artifact, .. }
                    if !session.history.preparation_current(&artifact.announcement()) =>
                {
                    // No activation has landed. Its counted announcement
                    // remains history while its unusable body is reclaimed.
                    self.undo_step(&step).await?;
                    self.database
                        .advance_operation(
                            SequenceOperation {
                                request,
                                progress: OperationProgress::Running(OperationStep::Preparing),
                            }
                            .update(&record)?,
                        )
                        .await?;
                }
                _ => self.claim_step(session, ring, member, &record, &request, &step).await?,
            }
        }
    }

    /// End the operation's journal row with its outcome, which the app's
    /// record of it keeps in the same transaction (§11.4). An operation an
    /// invite started stays journaled at its last step: the invite names it,
    /// and ends it in the transaction that moves the invite on or ends the
    /// invite, so a restart never finds one without the other (§13.2).
    async fn finish(
        &self,
        record: &OperationRecord,
        outcome: OperationOutcome,
    ) -> Result<OperationOutcome, SyncError> {
        if !parent_owned(record) {
            self.database.finish_operation_with_outcome(record.id, outcome.clone()).await?;
        }
        Ok(outcome)
    }

    /// The settlement of the open `claim`: an attempt not yet read is read,
    /// and one whose slot is still empty is sent again with its exact bytes.
    pub(crate) async fn settle_claim(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        claim: &coven_database::Claim,
    ) -> Result<ClaimSettlement, SyncError> {
        let settlement = match claim.settlement {
            ClaimSettlement::Unknown => {
                self.settle_open(session, ring, member).await?.expect("open claim")
            }
            value => value,
        };
        if settlement == ClaimSettlement::Retry {
            return self.publish_current(session, ring, member).await;
        }
        Ok(settlement)
    }

    async fn claim_step(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        record: &OperationRecord,
        request: &Request,
        step: &OperationStep,
    ) -> Result<(), SyncError> {
        let path = self.path(next_slot(session.through)?);
        let store_key = match request {
            Request::Change(StoreChange::CreateStore { key, .. }) => *key,
            _ => session.history.metadata_key(ring)?,
        };
        let (frame, kind) = match step {
            OperationStep::Copy(_) | OperationStep::CreatorCopy(_) => {
                let route = match step {
                    OperationStep::Copy(route) => route.clone(),
                    OperationStep::CreatorCopy(_) => {
                        let Request::Change(StoreChange::CreateStore { key, admin, .. }) = request
                        else {
                            unreachable!("creation owns its first key copy");
                        };
                        KeyCopyPrefix {
                            audience: Audience::Store,
                            key: *key,
                            member: member.member(),
                            sealing: admin.sealing,
                        }
                    }
                    _ => unreachable!("copy step"),
                };
                let context = SlotObjectPrefix::copy_context(
                    coven_format::FormatVersion::CURRENT,
                    store_key,
                    &route,
                )?;
                let sealed = match route.audience {
                    Audience::Store => coven_crypto::seal_store_key(
                        ring.store_key(route.key)?,
                        &route.sealing,
                        path.as_str(),
                        &context,
                    )?,
                    Audience::Circle(circle) => coven_crypto::seal_circle_key(
                        ring.circle_key(circle, route.key)?,
                        &route.sealing,
                        path.as_str(),
                        &context,
                    )?,
                };
                (SlotFrame::KeyCopy, SlotKind::KeyCopy(CopyBox { routing: route, sealed }))
            }
            OperationStep::Claiming => {
                let Request::Change(change) = request else { unreachable!("plain admin claim") };
                (SlotFrame::Admin(change.clone()), SlotKind::Admin)
            }
            OperationStep::Announcing(artifact) => {
                (SlotFrame::Announcement(artifact.announcement()), SlotKind::Announcement)
            }
            OperationStep::Activating { artifact, announcement } => {
                let change = match artifact.capture.purpose {
                    Purpose::Raise(version) => {
                        StoreChange::RaiseSchema { version, announcement: *announcement }
                    }
                    Purpose::Reset => StoreChange::Reset { announcement: *announcement },
                    Purpose::Ordinary => unreachable!("ordinary snapshots need no activation"),
                };
                session.history.validate(
                    &self.operation_header(session, member, 1),
                    next_slot(session.through)?,
                    &change,
                )?;
                (SlotFrame::Admin(change), SlotKind::Admin)
            }
            _ => unreachable!("a publication step"),
        };
        let held = ring.clone();
        let author = member.clone();
        let device = self.device;
        let after = session.through;
        let seal = move |claim| {
            let bytes = slot_object::seal_bounded(
                &path,
                &SlotHeader { device, claim, author: author.member(), after },
                &frame,
                SlotObjectPrefix {
                    format: coven_format::FormatVersion::CURRENT,
                    key: store_key,
                    kind,
                },
                &held,
                &author,
            )?;
            Ok::<_, SyncError>(ClaimAttempt { path, bytes: Cursor::new(bytes) })
        };
        if let Some(claim) = self.database.current_claim().await? {
            self.database.retry_claim(seal(claim.number)?).await?;
        } else {
            self.database.begin_claim(ClaimWork::Operation(record.id), seal).await?;
        }
        self.publish_current(session, ring, member).await?;
        Ok(())
    }

    async fn undo_step(&self, step: &OperationStep) -> Result<(), SyncError> {
        let artifact = match step {
            OperationStep::Capturing(capture) => {
                self.artifact_file(&capture.name)?.remove()?;
                return Ok(());
            }
            OperationStep::Uploading(artifact)
            | OperationStep::Announcing(artifact)
            | OperationStep::Activating { artifact, .. } => artifact,
            _ => return Ok(()),
        };
        if let Some(session) = &artifact.session {
            self.storage.abort_upload(session).await?;
        }
        let path = crate::snapshot_object::path(self.store(), &artifact.capture.id)?;
        self.storage.delete(&path).await?;
        if self.storage.status(&path).await?.is_some() {
            return Err(coven_storage::StorageFailure::Protocol
                .with_source("snapshot remains after deletion")
                .into());
        }
        self.artifact_file(&artifact.capture.name)?.remove()?;
        Ok(())
    }

    async fn abandon_settled_claim(&self, id: OperationId) -> Result<(), SyncError> {
        if let Some(claim) = self.database.current_claim().await? {
            if claim.work == ClaimWork::Operation(id) {
                self.database.finish_claim(None).await?;
            }
        }
        Ok(())
    }
}
#[cfg(test)]
impl SequenceSync {
    /// Run one of the app's journal steps in a write of its own, then a
    /// pass, and return the operation's outcome as `coven_operations` lists
    /// it; `None` when the step journaled nothing.
    pub(crate) async fn journal_and_pass(
        &self,
        step: impl FnOnce(&coven_database::SqlContext<'_, '_>) -> Result<Option<OperationId>, SyncError>
            + Send
            + 'static,
    ) -> Result<Option<OperationOutcome>, SyncError> {
        let journaled = self.database.write_as(move |sql| step(&sql)).await?;
        let Some(id) = journaled else {
            return Ok(None);
        };
        self.pass().await?;
        let outcome = self
            .database
            .read(move |sql| {
                Ok::<_, coven_database::DbError>(sql.query_row(
                    "SELECT outcome FROM coven_operations WHERE operation=?1",
                    [id.0],
                    |row| row.get::<_, OperationOutcome>(0),
                )?)
            })
            .await?;
        Ok(Some(outcome))
    }

    /// Make a circle as the app's write step does, and run the pass that
    /// lands it, returning its rejection as the error.
    pub(crate) async fn create_circle(
        &self,
        name: String,
    ) -> Result<coven_foundation::id_source::CircleId, SyncError> {
        let steps = self.operation_steps();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let outcome = self
            .journal_and_pass(move |sql| {
                let (circle, operation) = steps.create_circle(sql, name)?;
                sender.send(circle).expect("the caller waits");
                Ok(Some(operation))
            })
            .await?
            .expect("making a circle always journals");
        crate::operation_outcome::into_slot(outcome)?;
        Ok(receiver.await.expect("the step sent the circle"))
    }
}

mod change {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &StoreChange,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        value.encode().map_err(serde::ser::Error::custom)?.serialize(output)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<StoreChange, D::Error> {
        StoreChange::decode(&Vec::<u8>::deserialize(input)?).map_err(serde::de::Error::custom)
    }
}
fn parent_owned(record: &OperationRecord) -> bool {
    record.started_by == operation_kind::INVITE
}

fn operation_kind(change: &StoreChange) -> crate::OperationKind {
    use crate::OperationKind as K;
    match change {
        StoreChange::CreateStore { .. } => K::CreateStore,
        StoreChange::RenameStore { .. } => K::RenameStore,
        StoreChange::AddMember { .. }
        | StoreChange::AddInviteShare { .. }
        | StoreChange::EndInviteShare { .. } => K::Invite,
        StoreChange::RemoveMember { .. } => K::RemoveMember,
        StoreChange::ChangeRole { .. } => K::SetRole,
        StoreChange::SetAccess { .. } => K::SetAccess,
        StoreChange::ReplaceMemberKeys { .. } => K::ReplaceMemberKeys,
        StoreChange::AddDevice { .. } => K::AddDevice,
        StoreChange::RemoveDevice { .. } => K::RemoveDevice,
        StoreChange::CreateCircle { .. } => K::CreateCircle,
        StoreChange::RenameCircle { .. } => K::RenameCircle,
        StoreChange::DeleteCircle { .. } => K::DeleteCircle,
        StoreChange::AddCircleMember { .. } => K::AddCircleMember,
        StoreChange::RemoveCircleMember { .. } => K::RemoveCircleMember,
        StoreChange::RotateKey { .. } => K::RotateKey,
        StoreChange::RaiseSchema { .. } => K::SchemaChange,
        StoreChange::Reset { .. } => K::Reset,
    }
}

#[cfg(test)]
#[path = "sequence_operations_tests.rs"]
mod tests;
