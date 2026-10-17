//! One journal owns validation, prerequisite artifacts, claims and undo.

use crate::{
    history::History,
    sequence_sync::snapshot_capture::{Capture, SnapshotArtifact, SnapshotRequest},
    sequence_sync::{SequenceSync, Session},
    slot_object, OperationRejection, SyncError,
};
use coven_crypto::{MemberIdentity, StoreKeyring};
use coven_database::{
    ClaimAttempt, ClaimSettlement, ClaimWork, NewOperation, OperationId, OperationRecord,
    OperationUpdate,
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

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Request {
    Change(#[serde(with = "change")] StoreChange),
    Snapshot(SnapshotRequest),
}
#[derive(Serialize, Deserialize)]
pub(crate) enum Step {
    Preparing,
    InstallingKey(Vec<u8>),
    Copy(#[serde(with = "route")] KeyCopyPrefix),
    Claiming,
    CreatorCopy(u64),
    Capturing(Capture),
    Uploading(SnapshotArtifact),
    Announcing(SnapshotArtifact),
    Activating {
        artifact: SnapshotArtifact,
        announcement: u64,
    },
    Applied(u64),
    SnapshotApplied {
        artifact: SnapshotArtifact,
        slot: u64,
    },
    Satisfied,
}
#[derive(Serialize, Deserialize)]
pub(crate) enum Progress {
    Running(Step),
    Undo {
        step: Step,
        reason: OperationRejection,
    },
}
#[derive(Serialize, Deserialize)]
pub(crate) struct SequenceOperation {
    pub(crate) request: Request,
    pub(crate) progress: Progress,
}
impl SequenceOperation {
    pub(crate) fn update(&self, record: &OperationRecord) -> Result<OperationUpdate, SyncError> {
        Ok(OperationUpdate {
            id: record.id,
            previous: record.last_step,
            last_step: record
                .last_step
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?,
            data: serde_json::to_vec(self)?,
        })
    }
    fn kind(&self) -> crate::OperationKind {
        match &self.request {
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
        self.submit(Request::Change(change)).await.map(drop)
    }

    /// Capture the counted audience prefix, upload it, then claim its announcement.
    pub async fn snapshot(
        self: &std::sync::Arc<Self>,
        audience: Audience,
    ) -> Result<Slot, SyncError> {
        self.submit(Request::Snapshot(SnapshotRequest {
            audience,
            purpose: Purpose::Ordinary,
        }))
        .await
        .map(|slot| Slot(slot.expect("snapshot completion has a counted announcement")))
    }

    /// Publish a prepared reset and activate it through the same operation journal.
    pub async fn reset(self: &std::sync::Arc<Self>, audience: Audience) -> Result<(), SyncError> {
        self.submit(Request::Snapshot(SnapshotRequest {
            audience,
            purpose: Purpose::Reset,
        }))
        .await
        .map(drop)
    }

    /// Publish the captured breaking migration and activate its audience version.
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
    ) -> Result<Option<u64>, SyncError> {
        let owner = self.clone();
        // The owned task survives cancellation of the app's waiter, including
        // requests that the provider cannot cancel once sent.
        tokio::spawn(async move {
            let mut session = owner.session.lock().await;
            let member = owner
                .member
                .read()?
                .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            let mut ring = owner
                .keys
                .read()?
                .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            owner.read_forward(&mut session, &mut ring, &member).await?;
            owner.settle_open(&mut session, &mut ring, &member).await?;
            owner
                .resume_administration(&mut session, &mut ring, &member)
                .await?;
            owner
                .start_request(&mut session, &mut ring, &member, request, "operation")
                .await
        })
        .await
        .map_err(|error| SyncError::disk(std::io::Error::other(error)))?
    }

    pub(crate) async fn start_request(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        request: Request,
        started_by: &str,
    ) -> Result<Option<u64>, SyncError> {
        if !self.validate_request(session, member, &request)? {
            return Ok(None);
        }
        let work = SequenceOperation {
            request,
            progress: Progress::Running(Step::Preparing),
        };
        let id = self
            .database
            .start_operation(NewOperation {
                kind: "sequence".into(),
                data: serde_json::to_vec(&work)?,
                started_by: started_by.into(),
                pauses_writes: false,
            })
            .await?;
        self.run_administration(session, ring, member, id).await
    }

    fn operation_header(
        &self,
        session: &Session,
        member: &MemberIdentity,
        claim: u64,
    ) -> SlotHeader {
        SlotHeader {
            device: self.device,
            claim,
            author: member.member(),
            after: session.through,
        }
    }

    fn validate_request(
        &self,
        session: &Session,
        member: &MemberIdentity,
        request: &Request,
    ) -> Result<bool, OperationRejection> {
        match request {
            Request::Change(change) => session
                .history
                .validate(
                    &self.operation_header(session, member, 1),
                    Slot(session.through + 1),
                    change,
                )
                .map(|effect| effect.is_some()),
            Request::Snapshot(request) => {
                if !crate::effects::belongs(
                    &session.history.state,
                    &request.audience,
                    &member.member(),
                ) || !session
                    .history
                    .state
                    .devices
                    .get(&self.device)
                    .is_some_and(|device| device.active() && device.member == member.member())
                    || (request.purpose == Purpose::Reset
                        && request.audience == Audience::Store
                        && session.history.state.members[&member.member()].role
                            != coven_format::admin::MemberRole::Admin)
                {
                    return Err(OperationRejection::NotAllowed);
                }
                Ok(true)
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
        self.waits
            .retain_operations(&records.iter().map(|r| r.id).collect());
        for record in records {
            if record.kind == "sequence" && self.operation_ready(&record, ring).await? {
                self.run_administration(session, ring, member, record.id)
                    .await?;
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
    ) -> Result<Option<u64>, SyncError> {
        let scope = self.waits.begin_attempt(id);
        let result = scope
            .run(self.advance_administration(session, ring, member, id))
            .await;
        if let Err(error) = &result {
            if let Some(record) = self.database.operation(id).await? {
                let work: SequenceOperation = serde_json::from_slice(&record.data)?;
                self.record_operation_error(id, work.kind(), error, ring)
                    .await?;
            }
        } else {
            self.waits.clear_evidence(id);
            self.waits
                .forget(coven_database::RetrySubject::Operation(id));
        }
        result
    }

    async fn advance_administration(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        id: OperationId,
    ) -> Result<Option<u64>, SyncError> {
        loop {
            let record = self
                .database
                .operation(id)
                .await?
                .ok_or(coven_database::DbError::OperationChanged(id))?;
            let work: SequenceOperation = serde_json::from_slice(&record.data)?;
            let SequenceOperation { request, progress } = work;
            let mut step = match progress {
                Progress::Undo { step, reason } => {
                    self.abandon_settled_claim(id).await?;
                    if let Err(cleanup) = self.undo_step(&step).await {
                        return Err(SyncError::Cleanup {
                            operation: Box::new(SyncError::Rejected(reason)),
                            cleanup: Box::new(cleanup),
                        });
                    }
                    self.database.finish_operation(id).await?;
                    return Err(SyncError::Rejected(reason));
                }
                Progress::Running(step) => step,
            };
            match &step {
                Step::Applied(slot) => {
                    self.database.finish_operation(id).await?;
                    return Ok(Some(*slot));
                }
                Step::Satisfied => {
                    self.database.finish_operation(id).await?;
                    return Ok(None);
                }
                Step::SnapshotApplied { artifact, slot } => {
                    self.artifact_file(&artifact.capture.name)?.remove()?;
                    self.database.finish_operation(id).await?;
                    return Ok(Some(*slot));
                }
                _ => (),
            }
            if let Some(claim) = self.database.current_claim().await? {
                if claim.work != ClaimWork::Operation(id) {
                    return Err(coven_database::DbError::ClaimPending.into());
                }
                let settlement = match claim.settlement {
                    ClaimSettlement::Unknown => self
                        .settle_open(session, ring, member)
                        .await?
                        .expect("open claim"),
                    value => value,
                };
                let settlement = if settlement == ClaimSettlement::Retry {
                    self.publish_current(session, ring, member).await?
                } else {
                    settlement
                };
                match settlement {
                    ClaimSettlement::Landed => {
                        let slot = claim
                            .attempts
                            .last()
                            .expect("claim attempt")
                            .slot_number()
                            .expect("slot")
                            .0;
                        step = match step {
                            Step::Copy(_) => Step::Preparing,
                            Step::Claiming
                                if matches!(
                                    request,
                                    Request::Change(StoreChange::CreateStore { .. })
                                ) =>
                            {
                                Step::CreatorCopy(slot)
                            }
                            Step::Claiming => Step::Applied(slot),
                            Step::CreatorCopy(creation) => Step::Applied(creation),
                            Step::Announcing(artifact)
                                if artifact.capture.purpose == Purpose::Ordinary =>
                            {
                                Step::SnapshotApplied { artifact, slot }
                            }
                            Step::Announcing(artifact) => Step::Activating {
                                artifact,
                                announcement: slot,
                            },
                            Step::Activating { artifact, .. } => {
                                Step::SnapshotApplied { artifact, slot }
                            }
                            _ => unreachable!("a publication step owns each claim"),
                        };
                        self.database
                            .finish_claim(Some(
                                SequenceOperation {
                                    request,
                                    progress: Progress::Running(step),
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
                            .0
                            > session.through
                        {
                            match self.read_forward(session, ring, member).await {
                                Ok(())
                                | Err(SyncError::Removed | SyncError::MemberKeysReplaced { .. }) => {
                                }
                                Err(error) => return Err(error),
                            }
                            if matches!(step, Step::Copy(_) | Step::Claiming) {
                                step = Step::Preparing;
                            }
                        }
                    }
                    ClaimSettlement::DeviceIdInUse => {
                        return Err(self.state.stop_copy(
                            crate::DeviceIdEvidence::UnattemptedSlot {
                                slot: claim
                                    .attempts
                                    .last()
                                    .expect("attempt")
                                    .slot_number()
                                    .expect("slot"),
                            },
                        ))
                    }
                    _ => unreachable!("settled create"),
                }
            }
            let validation = if matches!(step, Step::CreatorCopy(_)) {
                self.check_binding(session, member)?;
                Ok(true)
            } else {
                self.validate_request(session, member, &request)
            };
            match validation {
                Ok(false) => {
                    self.abandon_settled_claim(id).await?;
                    self.undo_step(&step).await?;
                    self.database
                        .advance_operation(
                            SequenceOperation {
                                request,
                                progress: Progress::Running(Step::Satisfied),
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
                                progress: Progress::Undo { step, reason },
                            }
                            .update(&record)?,
                        )
                        .await?;
                    continue;
                }
                Ok(true) => (),
            }
            if let Step::InstallingKey(bytes) = &step {
                let prepared = StoreKeyring::from_secret_bytes(bytes)?;
                for id in prepared.store_key_ids() {
                    ring.insert_store_key(prepared.store_key(id)?.clone())?;
                }
                if let Request::Change(
                    StoreChange::CreateCircle { circle, .. }
                    | StoreChange::RotateKey {
                        audience: Audience::Circle(circle),
                        ..
                    },
                ) = &request
                {
                    for id in prepared.circle_key_ids(*circle) {
                        ring.insert_circle_key(prepared.circle_key(*circle, id)?.clone())?;
                    }
                }
                self.keys.persist(ring)?;
                self.database
                    .advance_operation(
                        SequenceOperation {
                            request,
                            progress: Progress::Running(Step::Preparing),
                        }
                        .update(&record)?,
                    )
                    .await?;
                continue;
            }
            if matches!(step, Step::Preparing) {
                step = match &request {
                    Request::Change(change) => match required_copies(&session.history, change, member.member()).into_iter().find(|route|
                        !session.history.copies.iter().any(|copy| matches!(&copy.prefix.kind, SlotKind::KeyCopy(copy) if &copy.routing == route))) {
                        Some(route) => Step::Copy(route), None => Step::Claiming,
                    },
                    Request::Snapshot(request) => {
                        if let Purpose::Raise(version) = request.purpose {
                            self.database.prepare_raise_capture(request.audience.clone(), version).await?
                                .ok_or(SyncError::Rejected(OperationRejection::NotAllowed))?;
                        }
                        let number = self.database.reserve_snapshot_number().await?.get();
                        Step::Capturing(Capture {id: coven_format::admin::SnapshotId {audience: request.audience.clone(),device: self.device,number},
                            purpose: request.purpose, key: session.history.sending_key(ring, member.member(), &request.audience)?, name: format!("snapshot-{number}.sealed")})
                    },
                };
                self.database
                    .advance_operation(
                        SequenceOperation {
                            request,
                            progress: Progress::Running(step),
                        }
                        .update(&record)?,
                    )
                    .await?;
                continue;
            }
            match step {
                Step::Capturing(capture) => {
                    let artifact = self.capture_snapshot(&capture, ring, member).await?;
                    self.database
                        .advance_operation(
                            SequenceOperation {
                                request,
                                progress: Progress::Running(Step::Uploading(artifact)),
                            }
                            .update(&record)?,
                        )
                        .await?;
                }
                Step::Uploading(artifact) => {
                    self.upload_snapshot(record, request, artifact).await?;
                }
                Step::Announcing(ref artifact) | Step::Activating { ref artifact, .. }
                    if !session
                        .history
                        .preparation_current(&artifact.announcement()) =>
                {
                    // No activation has landed. Its counted announcement
                    // remains history while its unusable body is reclaimed.
                    self.undo_step(&step).await?;
                    self.database
                        .advance_operation(
                            SequenceOperation {
                                request,
                                progress: Progress::Running(Step::Preparing),
                            }
                            .update(&record)?,
                        )
                        .await?;
                }
                _ => {
                    self.claim_step(session, ring, member, &record, &request, &step)
                        .await?
                }
            }
        }
    }

    async fn claim_step(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
        record: &OperationRecord,
        request: &Request,
        step: &Step,
    ) -> Result<(), SyncError> {
        let path = self.path(
            session
                .through
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?,
        )?;
        let store_key = match request {
            Request::Change(StoreChange::CreateStore { key, .. }) => *key,
            _ => session.history.metadata_key(ring)?,
        };
        let (frame, kind) = match step {
            Step::Copy(_) | Step::CreatorCopy(_) => {
                let route = match step {
                    Step::Copy(route) => route.clone(),
                    Step::CreatorCopy(_) => {
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
                (
                    SlotFrame::KeyCopy,
                    SlotKind::KeyCopy(CopyBox {
                        routing: route,
                        sealed,
                    }),
                )
            }
            Step::Claiming => {
                let Request::Change(change) = request else {
                    unreachable!("plain admin claim")
                };
                (SlotFrame::Admin(change.clone()), SlotKind::Admin)
            }
            Step::Announcing(artifact) => (
                SlotFrame::Announcement(artifact.announcement()),
                SlotKind::Announcement,
            ),
            Step::Activating {
                artifact,
                announcement,
            } => {
                let change = match artifact.capture.purpose {
                    Purpose::Raise(version) => StoreChange::RaiseSchema {
                        version,
                        announcement: Slot(*announcement),
                    },
                    Purpose::Reset => StoreChange::Reset {
                        announcement: Slot(*announcement),
                    },
                    Purpose::Ordinary => unreachable!("ordinary snapshots need no activation"),
                };
                session.history.validate(
                    &self.operation_header(session, member, 1),
                    Slot(session.through + 1),
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
                &SlotHeader {
                    device,
                    claim,
                    author: author.member(),
                    after,
                },
                &frame,
                SlotObjectPrefix {
                    format: coven_format::FormatVersion::CURRENT,
                    key: store_key,
                    kind,
                },
                &held,
                &author,
            )
            .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))?;
            Ok(ClaimAttempt {
                path,
                bytes: Cursor::new(bytes),
            })
        };
        if let Some(claim) = self.database.current_claim().await? {
            self.database.retry_claim(seal(claim.number)?).await?;
        } else {
            self.database
                .begin_claim(ClaimWork::Operation(record.id), seal)
                .await?;
        }
        self.publish_current(session, ring, member).await?;
        Ok(())
    }

    async fn undo_step(&self, step: &Step) -> Result<(), SyncError> {
        let artifact = match step {
            Step::Capturing(capture) => {
                self.artifact_file(&capture.name)?.remove()?;
                return Ok(());
            }
            Step::Uploading(artifact)
            | Step::Announcing(artifact)
            | Step::Activating { artifact, .. } => artifact,
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
mod change {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &StoreChange,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .encode()
            .map_err(serde::ser::Error::custom)?
            .serialize(output)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<StoreChange, D::Error> {
        StoreChange::decode(&Vec::<u8>::deserialize(input)?).map_err(serde::de::Error::custom)
    }
}
mod route {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &KeyCopyPrefix,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        (
            &value.audience.to_string(),
            value.key,
            value.member,
            value.sealing.as_bytes(),
        )
            .serialize(output)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<KeyCopyPrefix, D::Error> {
        let (audience, key, member, sealing): (String, _, _, [u8; 32]) =
            Deserialize::deserialize(input)?;
        let audience = if audience == "store" {
            Audience::Store
        } else {
            Audience::Circle(coven_foundation::id_source::CircleId(
                audience.parse().map_err(serde::de::Error::custom)?,
            ))
        };
        Ok(KeyCopyPrefix {
            audience,
            key,
            member,
            sealing: coven_crypto::SealingKey::from_bytes(sealing),
        })
    }
}

fn required_copies(
    history: &History,
    change: &StoreChange,
    author: coven_foundation::id_source::MemberId,
) -> Vec<KeyCopyPrefix> {
    match change {
        StoreChange::RotateKey { audience, key, .. } => history.routes(audience, *key),
        StoreChange::CreateCircle { circle, key, .. } => vec![KeyCopyPrefix {
            audience: Audience::Circle(*circle),
            key: *key,
            member: author,
            sealing: history.state.members[&author].keys.sealing,
        }],
        StoreChange::AddCircleMember { circle, member } => history
            .introductions
            .iter()
            .filter(|intro| intro.audience == Audience::Circle(*circle))
            .map(|intro| KeyCopyPrefix {
                audience: intro.audience.clone(),
                key: intro.key,
                member: *member,
                sealing: history.state.members[member].keys.sealing,
            })
            .collect(),
        StoreChange::AddMember { member, keys, .. }
        | StoreChange::ReplaceMemberKeys { member, keys, .. } => history
            .introductions
            .iter()
            .filter(|intro| {
                intro.audience == Audience::Store
                    || crate::effects::belongs(&history.state, &intro.audience, member)
            })
            .map(|intro| KeyCopyPrefix {
                audience: intro.audience.clone(),
                key: intro.key,
                member: *member,
                sealing: keys.sealing,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn operation_kind(change: &StoreChange) -> crate::OperationKind {
    use crate::OperationKind as K;
    match change {
        StoreChange::CreateStore { .. } => K::CreateStore,
        StoreChange::AddMember { .. } => K::Invite,
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
