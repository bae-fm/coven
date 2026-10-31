//! Read, apply and claim the shared sequence in one serialized installation.

use crate::{
    chunk_reader::PartPlaintext,
    floor_object::FloorObject,
    history::{History, KeyCopy},
    object_input::ObjectInput,
    slot_object::{self, ReceivedSlot, SlotBody},
    snapshot_object, PendingReason, PendingRecord, PendingSubject, Prerequisite, SequenceStatus,
    SyncError, SyncState,
};
use coven_crypto::{
    custody::{KeySession, PersonEntries},
    MemberIdentity, StoreKeyring,
};
use coven_database::{Database, IncomingSlot, LoadStep, SlotContent};
use coven_format::{floor::OriginalSlot, sequence::SlotFrame};
use coven_foundation::progress::Progress;
use coven_foundation::{
    files::StoreDir,
    id_source::{DeviceId, StoreId},
};
use coven_merge::{Audience, Slot};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use std::{num::NonZeroU64, sync::Arc};
use tokio::sync::{watch, Mutex};

pub(crate) struct Session {
    history: History,
    through: u64,
    floor: Option<FloorObject>,
    /// The floor revision `read_forward` last authenticated. Reading it back
    /// unchanged needs no second check: the history through its `at`, which
    /// the check reads, never changes once applied.
    checked_floor: Option<coven_storage::StoredObject>,
    blocked: Option<PendingRecord>,
    /// The next slot a member without store keys inspects for its copies.
    scanned: u64,
    /// Committed originals wait for store keys before their history is
    /// reconstructed: custody held none when this owner opened.
    locked: bool,
}

/// Owns sequence progress. App commits use the database independently; every
/// publisher shares this owner's serialization and actual blocked state.
#[derive(Clone)]
pub struct SequenceSync {
    database: Database,
    storage: Arc<dyn Storage>,
    keys: Arc<KeySession<StoreKeyring>>,
    member: Arc<KeySession<MemberIdentity>>,
    /// The person's restore code and S3 key: the code's store name follows
    /// applied renames (§13.1), and both are deleted when they leave (§14.1).
    person: Arc<PersonEntries>,
    /// The store's recorded location, for the code that replaces an
    /// unreadable saved entry (§13.1).
    settings: Arc<coven_storage::StorageSettings>,
    directory: StoreDir,
    device: DeviceId,
    state: Arc<SyncState>,
    clock: coven_foundation::clock::ClockRef,
    ids: coven_foundation::id_source::IdSourceRef,
    waits: Arc<crate::RetryWaits>,
    files: crate::Files,
    /// The steps `unlock` reports as it loads the store (§13.4).
    progress: watch::Sender<Option<Progress<LoadStep>>>,
    session: Arc<Mutex<Session>>,
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
        person: Arc<PersonEntries>,
        settings: Arc<coven_storage::StorageSettings>,
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
        // Without store keys in custody, history waits for this member's
        // copies in storage (§12.1).
        let (history, locked) = match keys.read()? {
            Some(mut ring) => {
                let history = History::restore(&originals, directory.id(), &mut ring, &identity)?;
                keys.persist(&ring)?;
                (history, false)
            }
            None => (History::default(), !originals.is_empty()),
        };
        let blocked = database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .find(|record| {
                record.subject == PendingSubject::Slot(Slot(through.saturating_add(1)))
                    && !matches!(record.reason, PendingReason::WritesRefused { .. })
            });
        if let Some(record) = &blocked {
            state.set(SequenceStatus::Blocked(Box::new(record.clone())));
        }
        let sync = Self {
            database,
            storage,
            keys,
            member,
            person,
            settings,
            directory,
            device,
            state,
            clock,
            ids,
            waits,
            files,
            progress: watch::channel(None).0,
            session: Arc::new(Mutex::new(Session {
                history,
                through,
                floor: None,
                checked_floor: None,
                blocked,
                scanned: 1,
                locked,
            })),
        };
        sync.publish_invites().await?;
        Ok(sync)
    }

    /// Use candidate credentials while retaining this installation's exact
    /// progress and serialization. Failed setup can keep the old provider
    /// without replaying history the candidate already applied.
    pub fn reconnect(&self, storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            ..self.clone()
        }
    }

    /// The steps `unlock` reports as it loads the store from storage, each
    /// with how much of it is done (§13.4). Passes report none.
    pub fn subscribe_progress(&self) -> watch::Receiver<Option<Progress<LoadStep>>> {
        self.progress.subscribe()
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
    pub async fn pass(&self) -> Result<SequenceStatus, SyncError> {
        let mut session = self.session.lock().await;
        // What earlier passes' slots left for this device's files goes out
        // in this pass, in a write of this device's own (§17.2).
        self.database.settle_files().await?;
        let waiting = self.database.waiting_snapshot_headers().await?;
        let first = waiting.first().map(|write| write.header.position);
        // What the queue lets out, if anything: a raise's capture and a
        // bound upload's hold keep later writes waiting (§6.3).
        let sendable = self
            .database
            .sendable_through()
            .await?
            .zip(first)
            .filter(|(through, first)| *through >= first.number);
        let member = self
            .member
            .read()?
            .ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        if let Some((through, first)) = sendable {
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
                        .record_pending(PendingRecord::new(
                            subject,
                            PendingReason::KeyUnavailable { audience, key },
                        ))
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
            let member = self
                .member
                .read()?
                .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            self.rotate_exposed(&mut session, &mut ring, &member)
                .await?;
            self.publish_raises(&mut session, &mut ring, &member)
                .await?;
            // A snapshot announced here is named by this pass's floor write.
            self.request_snapshots(&mut session, &mut ring, &member)
                .await?;
            self.retain_sequence(&mut session, &mut ring, &member)
                .await?;
            self.retain_snapshots(&mut session, &mut ring, &member)
                .await?;
            self.retain_files(&session, &ring, &member).await?;
        }
        let status = SequenceStatus::Synced {
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

    /// A slot's bytes: the ones already received for it if this device holds
    /// them whole, since a slot is created once and never changes, or else
    /// the provider's. None when the provider has no such slot.
    pub(crate) async fn fetch(&self, path: &ObjectPath) -> Result<Option<ObjectInput>, SyncError> {
        match self.held_slot(path).await? {
            Some(input) => Ok(Some(input)),
            None => self.read_slot(path).await,
        }
    }

    /// The slot's bytes if this device already holds all of them.
    pub(crate) async fn held_slot(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<ObjectInput>, SyncError> {
        match ObjectInput::retained(&self.database, path).await? {
            Some(input) if input.held().await? => Ok(Some(input)),
            _ => Ok(None),
        }
    }

    /// The slot's bytes from the provider, None when it has no such slot.
    pub(crate) async fn read_slot(
        &self,
        path: &ObjectPath,
    ) -> Result<Option<ObjectInput>, SyncError> {
        let response = match self.storage.stream_object(path, None).await {
            Ok(response) => response,
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        ObjectInput::receive_response(&self.database, self.storage.as_ref(), path, response)
            .await
            .map(Some)
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
                        .set(SequenceStatus::Stopped(crate::SyncFailure::Removed));
                    return Err(SyncError::Removed);
                }
                coven_database::DeviceState::Replaced => {
                    return Err(self.state.stop_copy(crate::DeviceIdEvidence::Replaced))
                }
                coven_database::DeviceState::Active => (),
            }
            // This member's own replacement binds its kept candidate before
            // custody installs it (§11.5).
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

    /// Bring the person's entries to `next` before committing the
    /// slots or snapshot that reach it from `applied`, so a failure returns to
    /// the caller with nothing committed; the retried pass or call reads the
    /// same slots and repeats it.
    ///
    /// The person leaves the store when their member's removal applies, by
    /// any admin including themselves. Their saved restore code and S3 key
    /// then serve no device of theirs, so this device deletes them: from
    /// iCloud Keychain for all their Apple devices, or from this phone's Block
    /// Store (§14.1). From then on the keychain refuses to write them again.
    /// Removing only a device leaves them: the person keeps their other
    /// devices. Otherwise a rename, this device's or another's, rewrites the
    /// saved code's store name, unless the entry's is newer (§13.1).
    pub(crate) fn follow_person_entries(
        &self,
        applied: &History,
        next: &History,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let left = |history: &History| crate::effects::left(&history.state, &member.member());
        if left(next) {
            if !left(applied) {
                self.person.delete_entries()?;
            }
            return Ok(());
        }
        let named = |history: &History| history.state.store.as_ref().map(crate::SavedName::of);
        match named(next) {
            Some(name) if named(applied) != Some(name.clone()) => {
                crate::follow_name(&self.person, &name, || {
                    let location = self.settings.read()?;
                    crate::replacement_code(&next.state, Some(member), location.as_ref())
                })
            }
            _ => Ok(()),
        }
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
        // A reload's fixed target already installed this slot.
        if at.0 <= session.through {
            return Ok(());
        }
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
                Err(SyncError::Pending(Box::new(PendingRecord::new(
                    PendingSubject::Slot(at),
                    match wait {
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
                ))))
            }
        }
    }

    /// Every variant is matched by name, so a new one is classified here.
    #[deny(clippy::wildcard_enum_match_arm)]
    async fn block(&self, session: &mut Session, slot: Slot, error: SyncError) -> SyncError {
        if let SyncError::DeviceIdInUse(evidence) = error {
            return self.state.stop_copy(evidence);
        }
        if matches!(error, SyncError::Removed) {
            self.state
                .set(SequenceStatus::Stopped(crate::SyncFailure::Removed));
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
                answered: error.answered(),
            },
            SyncError::MemberKeysReplaced { member } => {
                PendingReason::MemberKeysReplaced { member: *member }
            }
            SyncError::Failed { .. }
            | SyncError::Cleanup { .. }
            | SyncError::NoStorage
            | SyncError::StoreOwner
            | SyncError::InvitationChanged
            | SyncError::NotPending(_)
            | SyncError::ReloadPending(_)
            | SyncError::SnapshotPending(_)
            | SyncError::OperationData(_)
            | SyncError::SequenceExhausted
            | SyncError::Database(_)
            | SyncError::SecureStorage(_)
            | SyncError::Crypto(_)
            | SyncError::Format(_)
            | SyncError::Key(_)
            | SyncError::KeyAlreadyUsed(_)
            | SyncError::Rejected(_)
            | SyncError::Removed
            | SyncError::DeviceIdInUse(_)
            | SyncError::WrongStore { .. } => PendingReason::Failed(error.local_failure()),
        };
        let record = PendingRecord::new(PendingSubject::Slot(slot), reason);
        session.blocked = Some(record.clone());
        self.state
            .set(SequenceStatus::Blocked(Box::new(record.clone())));
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
            SlotBody::Writes { size, writes } => SlotContent::Writes { size, writes },
            SlotBody::Bounded {
                frame: SlotFrame::Admin(change),
                original,
            } => {
                let effect = match self.validate(&received.header, slot, &change) {
                    Ok(mut effect) => {
                        if let Some(effect) = &mut effect {
                            if self.state.store.is_some()
                                && !crate::effects::belongs(&self.state, &Audience::Store, &member)
                                && crate::effects::belongs(&effect.state, &Audience::Store, &member)
                            {
                                effect.reload.push(Audience::Store);
                            }
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

#[path = "operation_state.rs"]
mod operation_state;
pub(crate) use operation_state::OperationState;
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
#[path = "sequence_read.rs"]
mod sequence_read;
#[path = "sequence_reload.rs"]
pub(crate) mod sequence_reload;
#[path = "sequence_retention.rs"]
pub(crate) mod sequence_retention;
#[path = "sequence_retry.rs"]
mod sequence_retry;
pub(crate) use sequence_retry::OperationFailure;
#[path = "sequence_snapshots.rs"]
mod sequence_snapshots;
#[path = "slot_reading.rs"]
mod slot_reading;
use slot_reading::SlotReading;
#[path = "slot_reads.rs"]
mod slot_reads;
pub(crate) use slot_reads::SlotReads;
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
pub(crate) mod tests;
