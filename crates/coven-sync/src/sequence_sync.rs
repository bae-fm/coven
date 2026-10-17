//! Read, apply and claim the shared sequence in one serialized installation.

use crate::{
    chunk_reader::PartPlaintext,
    floor_object::FloorObject,
    history::{History, KeyCopy},
    object_input::ObjectInput,
    slot_object::{self, ReceivedSlot, SlotBody},
    snapshot_object, PendingReason, PendingRecord, PendingSubject, Prerequisite, Refusal,
    SyncError, SyncState, SyncStatus,
};
use coven_crypto::{custody::KeySession, MemberIdentity, StoreKeyring};
use coven_database::{Database, IncomingSlot, SlotContent};
use coven_format::{floor::OriginalSlot, sequence::SlotFrame};
use coven_foundation::{
    files::StoreDir,
    id_source::{DeviceId, StoreId},
};
use coven_merge::{Audience, Slot};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use std::{num::NonZeroU64, sync::Arc};
use tokio::sync::Mutex;

pub(crate) struct Session {
    history: History,
    through: u64,
    floor: Option<FloorObject>,
    blocked: Option<PendingRecord>,
    /// The next slot a member without store keys inspects for its copies.
    scanned: u64,
    /// The retired devices whose keys the last stray-key listing judged.
    swept: Option<std::collections::BTreeSet<DeviceId>>,
}

/// Owns sequence progress. App commits use the database independently; every
/// publisher shares this owner's serialization and actual blocked state.
pub struct SequenceSync {
    database: Database,
    storage: Arc<dyn Storage>,
    keys: Arc<KeySession<StoreKeyring>>,
    member: Arc<KeySession<MemberIdentity>>,
    directory: StoreDir,
    device: DeviceId,
    state: Arc<SyncState>,
    clock: coven_foundation::clock::ClockRef,
    ids: coven_foundation::id_source::IdSourceRef,
    waits: Arc<crate::RetryWaits>,
    files: crate::Files,
    session: Mutex<Session>,
    /// Admin access an operation holds in memory until its key is made (§4).
    admin_access: std::sync::Mutex<
        std::collections::BTreeMap<
            coven_database::OperationId,
            Arc<dyn coven_storage::AccessKeyAdmin>,
        >,
    >,
}

impl SequenceSync {
    /// Reconstruct historical bindings once from committed originals. It
    /// performs no provider request and grants no sending authority.
    pub async fn open(
        database: Database,
        storage: Arc<dyn Storage>,
        directory: StoreDir,
        device: DeviceId,
        keys: Arc<KeySession<StoreKeyring>>,
        member: Arc<KeySession<MemberIdentity>>,
        state: Arc<SyncState>,
        clock: coven_foundation::clock::ClockRef,
        ids: coven_foundation::id_source::IdSourceRef,
        waits: Arc<crate::RetryWaits>,
        files: crate::Files,
    ) -> Result<Self, SyncError> {
        let originals = database.original_slots().await?;
        let through = database.applied_slot().await?;
        let identity = member
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        // An installation still loading its first keys has applied nothing.
        let history = match keys.read()? {
            Some(mut ring) => History::restore(&originals, directory.id(), &mut ring, &identity)?,
            None if originals.is_empty() => History::default(),
            None => return Err(coven_storage::StorageFailure::MemberKeysMissing.into()),
        };
        let blocked = database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .find(|record| {
                record.reported_by == device
                    && record.subject == PendingSubject::Slot(Slot(through.saturating_add(1)))
                    && !matches!(record.reason, PendingReason::WritesRefused { .. })
            });
        if let Some(record) = &blocked {
            state.set(SyncStatus::Blocked(Box::new(record.clone())));
        }
        Ok(Self {
            database,
            storage,
            keys,
            member,
            directory,
            device,
            state,
            clock,
            ids,
            waits,
            files,
            session: Mutex::new(Session {
                history,
                through,
                floor: None,
                blocked,
                scanned: 1,
                swept: None,
            }),
            admin_access: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        })
    }

    pub(crate) fn store(&self) -> StoreId {
        self.directory.id()
    }
    pub(crate) fn path(&self, slot: u64) -> Result<ObjectPath, SyncError> {
        Ok(ObjectPath::slot(
            self.store(),
            NonZeroU64::new(slot).ok_or(SyncError::SequenceExhausted)?,
        ))
    }

    /// Reach the current read end, publish this pass's captured eligible queue,
    /// then perform the effects of its applied administrative state.
    pub async fn pass(&self) -> Result<SyncStatus, SyncError> {
        let mut session = self.session.lock().await;
        let waiting = self.database.waiting_snapshot_headers().await?;
        let first = waiting.first().map(|write| write.header.position);
        let captured = waiting.last().map(|write| write.header.position.number);
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        if let (Some(through), Some(first)) = (captured, first) {
            // The queue's first write names a wait for its sealing key; a
            // later send that succeeds settles that condition.
            let subject = PendingSubject::Write(coven_merge::WriteId {
                device: first.device,
                number: first.number,
            });
            match self
                .send_writes(&mut session, &mut ring, &member, through)
                .await
            {
                Ok(()) => self.database.clear_pending(subject).await?,
                Err(SyncError::KeyUnavailable(key)) => {
                    let audience = session
                        .history
                        .introductions
                        .iter()
                        .find(|intro| intro.key == key)
                        .map(|intro| intro.audience.clone())
                        .unwrap_or(Audience::Store);
                    self.database
                        .record_pending(PendingRecord {
                            subject,
                            reason: PendingReason::KeyUnavailable { audience, key },
                            reported_by: self.device,
                        })
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        self.resume_administration(&mut session, &mut ring, &member)
            .await?;
        // Provider work, rotation and retention send for this installation;
        // one still loading before its registration sends none of them.
        if self.registered(&session) {
            self.resume_access(&mut session, &mut ring, &member).await?;
            self.resume_invites(&mut session, &mut ring, &member)
                .await?;
            self.resume_member_keys(&mut session, &mut ring).await?;
            self.rotate_exposed(&mut session, &mut ring, &member)
                .await?;
            self.retain_sequence(&mut session, &mut ring, &member)
                .await?;
            self.retain_snapshots(&session).await?;
            self.request_snapshots(&mut session, &mut ring, &member)
                .await?;
            self.sweep_stray_access_keys(&mut session, &member).await?;
            self.retain_files(&session, &ring, &member).await?;
            self.files.sync_files().await?;
        }
        let status = SyncStatus::Synced {
            through: session.through,
        };
        self.state.set(status.clone());
        Ok(status)
    }

    /// Whether the applied history registers this installation as active.
    pub(crate) fn registered(&self, session: &Session) -> bool {
        session
            .history
            .state
            .devices
            .get(&self.device)
            .is_some_and(|device| device.active())
    }

    pub(crate) async fn fetch(&self, path: &ObjectPath) -> Result<Option<ObjectInput>, SyncError> {
        let response = match self.storage.stream_object(path, None).await {
            Ok(response) => response,
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        ObjectInput::receive_response(&self.database, self.storage.as_ref(), path, response)
            .await
            .map(Some)
    }

    /// Exponential existence checks bracket the end; binary checks narrow it.
    /// These observations never supply data or advance the applied prefix.
    async fn locate_end(&self, start: u64) -> Result<u64, SyncError> {
        let mut low = start;
        let mut step = 1u64;
        let high = loop {
            let Some(next) = start.checked_add(step) else {
                break u64::MAX;
            };
            if self.storage.status(&self.path(next)?).await?.is_none() {
                break next;
            }
            low = next;
            let Some(next_step) = step.checked_mul(2) else {
                break u64::MAX;
            };
            step = next_step;
        };
        let mut high = high;
        while high - low > 1 {
            let middle = low + (high - low) / 2;
            if self.storage.status(&self.path(middle)?).await?.is_some() {
                low = middle;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    pub(crate) async fn read_forward(
        &self,
        session: &mut Session,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if let SyncStatus::Stopped(_) = &*self.state.subscribe().borrow() {
            return self.state.check();
        }
        if let Some(record) = &session.blocked {
            if !self
                .database
                .pending_can_retry::<PendingRecord>(record.subject.clone())
                .await?
            {
                return Err(SyncError::Pending(Box::new(record.clone())));
            }
        }
        loop {
            let first = session
                .through
                .checked_add(1)
                .ok_or(SyncError::SequenceExhausted)?;
            let mut next = first;
            let mut known_end = session.through;
            let mut virtual_history = session.history.clone();
            let mut tentative_ring = ring.clone();
            let mut slots = Vec::new();
            let mut failure = None;
            let floor = loop {
                loop {
                    let path = self.path(next)?;
                    let input = match self.fetch(&path).await {
                        Ok(Some(input)) => input,
                        Ok(None) => {
                            if next <= known_end {
                                failure = Some((
                                    Slot(next),
                                    crate::write_object::damaged(
                                        &path,
                                        Refusal::Parse { cause: None },
                                    ),
                                ));
                            }
                            break;
                        }
                        Err(error) => {
                            failure = Some((Slot(next), error));
                            break;
                        }
                    };
                    let received = match slot_object::open(
                        input.local_bytes().await?,
                        &path,
                        &tentative_ring,
                        &virtual_history,
                        member.member(),
                    ) {
                        Ok(slot) => slot,
                        Err(error) => {
                            failure = Some((Slot(next), error));
                            break;
                        }
                    };
                    if let Err(error) = self.check_copy(&received, &path).await {
                        failure = Some((Slot(next), error));
                        break;
                    }
                    let metadata = !received.writes();
                    let incoming = virtual_history.consume(received, member.member());
                    if let Err(error) = virtual_history.acquire(&mut tentative_ring, member) {
                        failure = Some((Slot(next), error));
                        break;
                    }
                    slots.push((incoming, metadata.then(|| virtual_history.clone()), input));
                    known_end = known_end.max(next);
                    if slots.len() == 32 {
                        known_end = self.locate_end(next).await?;
                    }
                    next = next.checked_add(1).ok_or(SyncError::SequenceExhausted)?;
                }
                // The final floor observation follows every input read. A
                // floor written after the ending miss may name new slots: read
                // them before deciding that a hole or signer is missing.
                let floor = FloorObject::read(
                    &self.database,
                    self.storage.as_ref(),
                    &ObjectPath::floor(self.store()),
                    session.floor.as_ref(),
                    &mut tentative_ring,
                    member,
                )
                .await?;
                if floor
                    .as_ref()
                    .is_some_and(|floor| floor.header.first.0 > first)
                {
                    break floor;
                }
                if failure.is_none() && floor.as_ref().is_some_and(|floor| floor.header.at >= next)
                {
                    known_end = floor.as_ref().expect("observed floor").header.at;
                    continue;
                }
                if let Some(floor) = &floor {
                    self.authenticate_floor(floor, &virtual_history, &mut tentative_ring, member)
                        .await?;
                }
                break floor;
            };
            if floor
                .as_ref()
                .is_some_and(|floor| floor.header.first.0 > first)
            {
                for (_, _, input) in slots {
                    input.release().await?;
                }
                self.reload_floor(session, floor.expect("floor passed first"), ring, member)
                    .await?;
                continue;
            }
            session.floor = floor;
            for (incoming, history, input) in slots {
                let at = incoming.slot;
                if let Err(error) = self
                    .apply(
                        session,
                        incoming,
                        history.unwrap_or_else(|| session.history.clone()),
                        &tentative_ring,
                        member,
                    )
                    .await
                {
                    return Err(self.block(session, at, error).await);
                }
                input.release().await?;
            }
            *ring = tentative_ring;
            if let Some((slot, error)) = failure {
                return Err(self.block(session, slot, error).await);
            }
            if next <= known_end {
                let error =
                    crate::write_object::damaged(&self.path(next)?, Refusal::Parse { cause: None });
                return Err(self.block(session, Slot(next), error).await);
            }
            session.blocked = None;
            self.check_binding(session, member)?;
            self.state.set(SyncStatus::Synced {
                through: session.through,
            });
            return Ok(());
        }
    }

    pub(crate) async fn check_copy(
        &self,
        received: &ReceivedSlot,
        path: &ObjectPath,
    ) -> Result<(), SyncError> {
        let received = &received.envelope;
        if received.header.device == self.device
            && !self
                .database
                .attempted_slot(received.header.claim, path.clone(), received.digest)
                .await?
        {
            return Err(self
                .state
                .stop_copy(crate::DeviceIdEvidence::UnattemptedSlot {
                    slot: Slot(received.header.after + 1),
                }));
        }
        Ok(())
    }

    pub(crate) fn check_binding(
        &self,
        session: &Session,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if let Some(device) = session.history.state.devices.get(&self.device) {
            match device.state {
                coven_database::DeviceState::Removed => {
                    self.state
                        .set(SyncStatus::Stopped(crate::SyncFailure::Removed));
                    return Err(SyncError::Removed);
                }
                coven_database::DeviceState::Replaced => {
                    return Err(self.state.stop_copy(crate::DeviceIdEvidence::Replaced))
                }
                coven_database::DeviceState::Active => (),
            }
            // This member's own replacement binds its kept candidate before
            // custody installs it (§18.1).
            let current = session.history.state.members[&device.member].keys;
            if current != member.keys().public_keys()
                && member.candidate().map(|keys| keys.public_keys()) != Some(current)
            {
                return Err(SyncError::MemberKeysReplaced {
                    member: device.member,
                });
            }
        }
        self.state.registered(
            session
                .history
                .state
                .devices
                .get(&self.device)
                .is_some_and(|device| device.active()),
        );
        Ok(())
    }

    pub(crate) async fn apply(
        &self,
        session: &mut Session,
        mut incoming: IncomingSlot<coven_database::DownloadedWriteStream<PartPlaintext>>,
        history: History,
        ring: &StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let at = incoming.slot;
        if let SlotContent::Admin {
            effect: Some(effect),
            ..
        } = &mut incoming.content
        {
            effect.provider =
                history.provider_changes(member.member(), &self.invited_accounts().await?)?;
            if !effect.reload.is_empty() {
                let required = effect.reload.clone();
                // Persist the audience gates before reading any replacement body.
                self.database
                    .apply_slot::<PendingRecord, _, _>(incoming, || Ok(()), None)
                    .await?;
                return self
                    .reload_activation(session, at, &required, history, ring, member)
                    .await;
            }
        }
        self.keys.persist(ring)?;
        let result = self
            .database
            .apply_slot::<PendingRecord, _, _>(incoming, || Ok(()), None)
            .await
            .map_err(|error| {
                crate::write_object::database_failure(
                    &self.path(at.0).expect("positive slot"),
                    error,
                )
            })?;
        match result {
            coven_database::ApplyOutcome::Applied
            | coven_database::ApplyOutcome::AlreadyApplied => {
                session.through = at.0;
                session.history = history;
                self.check_binding(session, member)?;
                Ok(())
            }
            coven_database::ApplyOutcome::Waiting(wait) => {
                Err(SyncError::Pending(Box::new(PendingRecord {
                    subject: PendingSubject::Slot(at),
                    reason: match wait {
                        coven_database::WriteWait::Reload(audience) => {
                            PendingReason::Waits(Prerequisite::Reload(audience))
                        }
                        coven_database::WriteWait::SchemaVersion(version) => {
                            PendingReason::UpdateRequired(crate::RequiredUpdate::AppSchema {
                                version,
                            })
                        }
                        _ => PendingReason::Waits(Prerequisite::SequenceCatchUp),
                    },
                    reported_by: self.device,
                })))
            }
        }
    }

    async fn block(&self, session: &mut Session, slot: Slot, error: SyncError) -> SyncError {
        if let SyncError::DeviceIdInUse(evidence) = error {
            return self.state.stop_copy(evidence);
        }
        if matches!(error, SyncError::Removed) {
            self.state
                .set(SyncStatus::Stopped(crate::SyncFailure::Removed));
            return error;
        }
        let reason = match &error {
            SyncError::Pending(record) => record.reason.clone(),
            SyncError::KeyUnavailable(key) => PendingReason::KeyUnavailable {
                audience: Audience::Store,
                key: *key,
            },
            SyncError::UpdateRequired(update) => PendingReason::UpdateRequired(*update),
            SyncError::Damaged(object) => PendingReason::Refused(object.failure.clone()),
            SyncError::Storage(error) => PendingReason::Storage {
                provider: self.storage.config().provider(),
                failure: error.failure(),
            },
            SyncError::MemberKeysReplaced { member } => {
                PendingReason::MemberKeysReplaced { member: *member }
            }
            _ => PendingReason::Failed(error.local_failure()),
        };
        let record = PendingRecord {
            subject: PendingSubject::Slot(slot),
            reason,
            reported_by: self.device,
        };
        session.blocked = Some(record.clone());
        self.state
            .set(SyncStatus::Blocked(Box::new(record.clone())));
        match self.database.record_pending(record).await {
            Ok(()) => error,
            Err(cleanup) => SyncError::Cleanup {
                operation: Box::new(error),
                cleanup: Box::new(cleanup.into()),
            },
        }
    }
}

impl History {
    pub(crate) fn restore(
        originals: &[OriginalSlot],
        store: StoreId,
        ring: &mut StoreKeyring,
        member: &MemberIdentity,
    ) -> Result<Self, SyncError> {
        for original in originals {
            snapshot_object::open_routing(original, store, ring, member)?;
        }
        let mut history = Self::default();
        for original in originals {
            let path = ObjectPath::slot(
                store,
                NonZeroU64::new(original.slot.0).expect("positive original slot"),
            );
            let received =
                slot_object::open_bounded(original.object.clone(), &path, ring, &history)?;
            history.consume(received, member.member());
        }
        history.acquire(ring, member)?;
        Ok(history)
    }

    pub(crate) fn consume(
        &mut self,
        received: ReceivedSlot,
        member: coven_foundation::id_source::MemberId,
    ) -> IncomingSlot<coven_database::DownloadedWriteStream<PartPlaintext>> {
        let (received, body) = received.into_parts();
        let slot = Slot(received.header.after + 1);
        let content = match body {
            SlotBody::Writes(writes) => SlotContent::Writes(writes),
            SlotBody::Bounded {
                frame: SlotFrame::Admin(change),
                original,
            } => {
                self.note_named_key(&change);
                let effect = match self.validate(&received.header, slot, &change) {
                    Ok(mut effect) => {
                        if let Some(effect) = &mut effect {
                            if let Some(boundary) = &effect.boundary {
                                if crate::effects::belongs(
                                    &effect.state,
                                    &boundary.snapshot().audience,
                                    &member,
                                ) {
                                    effect.reload.push(boundary.snapshot().audience.clone());
                                }
                            }
                            for circle in effect
                                .state
                                .readable_circles(&member)
                                .difference(&self.state.readable_circles(&member))
                            {
                                if self.state.circles.contains_key(circle) {
                                    effect.reload.push(Audience::Circle(*circle));
                                }
                            }
                            self.accept_admin(slot, &received.header, effect);
                        }
                        effect
                    }
                    Err(reason) => {
                        tracing::debug!(?slot, ?reason, "consumed refused administrative slot");
                        None
                    }
                };
                SlotContent::Admin { original, effect }
            }
            SlotBody::Bounded {
                frame: SlotFrame::KeyCopy,
                original,
            } => {
                self.copies.push(KeyCopy {
                    path: received.path,
                    prefix: received.prefix,
                });
                SlotContent::KeyCopy(original)
            }
            SlotBody::Bounded {
                frame: SlotFrame::Announcement(announcement),
                original,
            } => {
                if self.announcement_allowed(slot, &received.header, &announcement) {
                    self.announcements.insert(slot, announcement);
                } else {
                    tracing::debug!(
                        ?slot,
                        "consumed stale or unauthorized snapshot announcement"
                    );
                }
                SlotContent::Announcement(original)
            }
        };
        IncomingSlot {
            slot,
            header: received.header,
            digest: received.digest,
            content,
        }
    }

    fn announcement_allowed(
        &self,
        slot: Slot,
        header: &coven_format::sequence::SlotHeader,
        announcement: &coven_format::sequence::SnapshotAnnouncement,
    ) -> bool {
        if !crate::effects::belongs(&self.state, &announcement.snapshot.audience, &header.author)
            || !self
                .state
                .devices
                .get(&header.device)
                .is_some_and(|device| device.active() && device.member == header.author)
            || announcement.snapshot.device != header.device
            || announcement.covers >= slot.0
        {
            return false;
        }
        let previous = self
            .announcements
            .values()
            .filter(|prior| prior.snapshot.audience == announcement.snapshot.audience)
            .map(|prior| prior.covers)
            .max();
        let boundary = self
            .boundaries
            .iter()
            .filter(|prior| prior.boundary.snapshot().audience == announcement.snapshot.audience)
            .map(|prior| prior.boundary.entry().0)
            .max();
        previous.is_none_or(|covers| announcement.covers >= covers)
            && boundary.is_none_or(|at| announcement.covers >= at)
    }
}

#[path = "access_keys.rs"]
pub(crate) mod access_keys;
#[path = "sequence_access.rs"]
mod sequence_access;
#[path = "sequence_access_keys.rs"]
mod sequence_access_keys;
#[path = "sequence_bootstrap.rs"]
pub(crate) mod sequence_bootstrap;
#[path = "sequence_claim.rs"]
mod sequence_claim;
#[path = "sequence_credentials.rs"]
mod sequence_credentials;
#[path = "sequence_files.rs"]
mod sequence_files;
#[path = "sequence_invites.rs"]
mod sequence_invites;
#[path = "sequence_join.rs"]
pub(crate) mod sequence_join;
#[path = "sequence_keys.rs"]
mod sequence_keys;
#[path = "sequence_member_keys.rs"]
mod sequence_member_keys;
#[path = "sequence_operations.rs"]
pub(crate) mod sequence_operations;
#[path = "sequence_reload.rs"]
pub(crate) mod sequence_reload;
#[path = "sequence_retention.rs"]
pub(crate) mod sequence_retention;
#[path = "sequence_retry.rs"]
mod sequence_retry;
#[path = "sequence_snapshots.rs"]
mod sequence_snapshots;
#[path = "snapshot_capture.rs"]
pub(crate) mod snapshot_capture;
#[path = "snapshot_file_scan.rs"]
mod snapshot_file_scan;
#[path = "snapshot_upload.rs"]
mod snapshot_upload;
#[path = "sync_loop.rs"]
pub(crate) mod sync_loop;

#[cfg(test)]
#[path = "sequence_sync_tests.rs"]
mod tests;
