//! Read, apply and claim the shared sequence in one serialized installation.

use crate::{
    chunk_reader::PartPlaintext,
    floor_object::FloorObject,
    history::{History, KeyCopy},
    object_input::ObjectInput,
    slot_object::{self, ReceivedSlot, SlotBody},
    snapshot_object, PendingReason, PendingRecord, PendingSubject, Prerequisite, SequenceState,
    SequenceStatus, SyncError,
};
use coven_crypto::{
    custody::{KeySession, PersonEntries},
    MemberIdentity, StoreKeyring,
};
use coven_database::operation_kind;
use coven_database::{
    Database, IncomingSlot, LoadStep, NewOperation, ProviderAccessChange, SlotContent,
};
use coven_format::{
    floor::OriginalSlot,
    sealed_slot::SlotKind,
    sequence::{Purpose, SlotFrame},
};
use coven_foundation::{
    files::StoreDir,
    id_source::{DeviceId, StoreId},
};
use coven_merge::{Audience, Slot};
use coven_storage::{ObjectPath, Storage, StorageFailure};
use std::sync::Arc;
use tokio::sync::Mutex;

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
    scanned: Slot,
    /// How far the scan for the circle key a stuck slot waits on got, while
    /// this owner is open (§5.4, §12.1).
    stuck_scan: Option<sequence_bootstrap::StuckScan>,
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
    /// The store's directory, whose recorded location is read for the code
    /// that replaces an unreadable saved entry (§13.1).
    directory: StoreDir,
    device: DeviceId,
    state: Arc<SequenceState>,
    clock: coven_foundation::clock::ClockRef,
    ids: coven_foundation::id_source::IdSourceRef,
    waits: Arc<crate::RetryWaits>,
    files: crate::Files,
    /// The steps `unlock` reports as it loads the store (§13.4).
    progress: coven_foundation::progress::ProgressChannel<LoadStep>,
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
        state: Arc<SequenceState>,
        clock: coven_foundation::clock::ClockRef,
        ids: coven_foundation::id_source::IdSourceRef,
        waits: Arc<crate::RetryWaits>,
        files: crate::Files,
    ) -> Result<Self, SyncError> {
        let originals = database.original_slots().await?;
        let through = database.applied_slot().await?;
        let identity = member.read()?.ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        // A slot binding the member to a pair recorded here is one this
        // installation can sign with; any other stops it (§12.3).
        database
            .hold_member_keys(
                std::iter::once(identity.keys().public_keys())
                    .chain(identity.candidate().map(|keys| keys.public_keys()))
                    .collect(),
            )
            .await?;
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
        let blocked = database.pending::<PendingRecord>().await?.into_iter().find(|record| {
            next_slot(through).is_ok_and(|next| record.subject == PendingSubject::Slot(next))
                && !matches!(record.reason, PendingReason::WritesRefused { .. })
        });
        if let Some(record) = &blocked {
            state.set(SequenceStatus::Blocked(Box::new(record.clone())));
        }
        Ok(Self {
            database,
            storage,
            keys,
            member,
            person,
            directory,
            device,
            state,
            clock,
            ids,
            waits,
            files,
            progress: coven_foundation::progress::ProgressChannel::new(),
            session: Arc::new(Mutex::new(Session {
                history,
                through,
                floor: None,
                checked_floor: None,
                blocked,
                scanned: Slot::FIRST,
                stuck_scan: None,
                locked,
            })),
        })
    }

    /// Decide this device's slot 1 in its database, as creating the store
    /// does, and rebuild the session from the applied slots as opening does.
    #[cfg(test)]
    pub(crate) async fn decide_creation(
        &self,
        name: &str,
        device_name: &str,
        ring: &StoreKeyring,
    ) -> Result<(), SyncError> {
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        crate::decide_creation(
            &self.database,
            crate::Creation {
                store: self.store(),
                name: name.into(),
                device: self.device,
                device_name: device_name.into(),
                member: &member,
                ring,
            },
        )
        .await?;
        let originals = self.database.original_slots().await?;
        let identity = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.keys.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let history = History::restore(&originals, self.store(), &mut ring, &identity)?;
        let mut session = self.session.lock().await;
        session.through = self.database.applied_slot().await?;
        session.history = history;
        Ok(())
    }

    /// Use candidate credentials while retaining this installation's exact
    /// progress and serialization. Failed setup can keep the old provider
    /// without replaying history the candidate already applied.
    pub fn reconnect(&self, storage: Arc<dyn Storage>) -> Self {
        Self { storage, ..self.clone() }
    }

    /// The steps `unlock` reports as it loads the store from storage, each
    /// with how much of it is done (§13.4). Passes report none.
    pub fn subscribe_progress(&self) -> coven_foundation::progress::ProgressWatch<LoadStep> {
        self.progress.watch()
    }

    pub(crate) fn store(&self) -> StoreId {
        self.directory.id()
    }

    /// The app's write steps over this owner's own fields, for tests that
    /// run a step beside the pass.
    #[cfg(test)]
    pub(crate) fn operation_steps(&self) -> crate::OperationSteps {
        // Custody holding no storage credentials: these tests give an
        // invite's key in the call.
        let keychain = coven_crypto::custody::Keychain::in_memory("operation steps").unwrap();
        let credentials = crate::DeviceCredentials::new(
            Arc::new(coven_crypto::custody::StoreKeychain::new(keychain.clone(), self.store())),
            Arc::new(coven_crypto::custody::PersonEntries::new(keychain, self.store())),
            self.directory.clone(),
            None,
        );
        crate::OperationSteps::new(
            self.device,
            self.member.clone(),
            self.ids.clone(),
            self.clock.clone(),
            self.directory.clone(),
            Arc::new(credentials),
            self.waits.clone(),
            self.state.clone(),
        )
    }
    pub(crate) fn path(&self, slot: Slot) -> ObjectPath {
        ObjectPath::slot(self.store(), slot.into())
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
        let member = self.member.read()?.ok_or(StorageFailure::MemberKeysMissing)?;
        let mut ring = self.ring(&mut session, &member).await?;
        self.read_forward(&mut session, &mut ring, &member).await?;
        self.check_admitted(&session, &member).await?;
        self.settle_open(&mut session, &mut ring, &member).await?;
        self.resume_rebuilds(&mut session, &ring, &member).await?;
        if let Some((through, first)) = sendable {
            // The queue's first write names a wait for its sealing key; a
            // later send that succeeds settles that condition.
            let subject = PendingSubject::Write(coven_merge::WriteId {
                device: first.device,
                number: first.number,
            });
            match self.send_writes(&mut session, &mut ring, &member, through).await {
                Ok(()) => self.database.clear_pending(subject).await?,
                Err(SyncError::KeyUnavailable { audience, key }) => {
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
        self.resume_administration(&mut session, &mut ring, &member).await?;
        // Provider work, rotation and retention send for this installation;
        // one still loading before its registration sends none of them.
        if self.registered(&session) {
            self.resume_access(&mut session, &mut ring, &member).await?;
            self.resume_invites(&mut session, &mut ring, &member).await?;
            self.resume_pairings(&session).await?;
            self.end_gone_requests(&session, &member).await?;
            self.resume_member_keys(&mut session, &mut ring).await?;
            let member =
                self.member.read()?.ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
            self.rotate_exposed(&mut session, &mut ring, &member).await?;
            self.send_owed_copies(&mut session, &mut ring, &member).await?;
            self.publish_raises(&mut session, &mut ring, &member).await?;
            // A snapshot announced here is named by this pass's floor write.
            self.request_snapshots(&mut session, &mut ring, &member).await?;
            self.retain_sequence(&mut session, &mut ring, &member).await?;
            self.retain_snapshots(&mut session, &mut ring, &member).await?;
            self.retain_files(&session, &ring, &member).await?;
        }
        let status = SequenceStatus::Synced { through: session.through };
        self.state.set(status.clone());
        Ok(status)
    }

    /// Whether the applied history registers this installation as active.
    pub(crate) fn registered(&self, session: &Session) -> bool {
        session.history.state.devices.get(&self.device).is_some_and(|device| device.active())
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

    /// A refusal judges the stored slot, not one read of it, and nothing
    /// checks a slot's transfer: the slot whose kept copy a check refused is
    /// read again, and the refusal stands only when the second read returns
    /// the same bytes (§19.1). Different bytes mean a fault on the way
    /// changed a read: a refusal recorded over the copy ends, the copy is
    /// dropped so the next pass reads the slot again, and this pass fails
    /// with `Corrupted`.
    async fn confirm_refusal(&self, session: &mut Session, slot: Slot) -> Result<(), SyncError> {
        let path = self.path(slot);
        let Some(kept) = self.held_slot(&path).await? else {
            return Ok(());
        };
        if kept.same_as_stored(self.storage.as_ref()).await? {
            return Ok(());
        }
        self.database.clear_pending(PendingSubject::Slot(slot)).await?;
        session.blocked = None;
        kept.discard().await?;
        Err(crate::object_input::corrupted(&path))
    }

    /// Stop for good on history showing another copy of this installation
    /// using its device id (§19.3), and end what this copy's app journaled
    /// and the invites it made: none would ever run, and no slot applies
    /// this stop to end them (§14.1). A failed write fails the handle with
    /// its cause, which every call reports.
    pub(crate) async fn stop_copy(&self, evidence: crate::DeviceIdEvidence) -> SyncError {
        let stopped = self.state.stop_copy(evidence);
        let stop = match evidence {
            crate::DeviceIdEvidence::Replaced => coven_database::DeviceStop::Replaced,
            crate::DeviceIdEvidence::UnattemptedSlot { .. } => coven_database::DeviceStop::Copied,
        };
        if let Err(error) = self.database.end_for_good(stop).await {
            tracing::error!("a stopped copy's operations stay journaled: {error}");
        }
        stopped
    }

    /// Whether the applied history ever bound this member id to the pair
    /// this installation holds. A store's history binds it before it has
    /// applied anything else, slot 1 its creator's and a restore's loading
    /// a pair history bound, except a store a join published once it found
    /// its admission, whose first load applies that admission (§13.2).
    pub(crate) fn pair_bound(&self, session: &Session, member: &MemberIdentity) -> bool {
        let keys = member.keys().public_keys();
        session
            .history
            .bindings
            .iter()
            .any(|binding| binding.member == member.member() && binding.keys == keys)
    }

    /// Stop for good once a store's history, read to the end of the
    /// sequence, binds its pair nowhere: the admission a join published it
    /// on was refused, and its device never became a member (§13.2, §14.1).
    /// Only a pass, and the first load of a store a join published, check
    /// this: every store a pass runs on has a pair its history binds first,
    /// but a joined store's, while a creation that loses slot 1 never gets a
    /// pass. No slot applies this stop, so it ends what the app journaled
    /// and the invites the device made, as a stopped copy does; every pass
    /// finds it again. A failed write fails the handle with its cause, which
    /// every call reports.
    pub(crate) async fn check_admitted(
        &self,
        session: &Session,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        if session.through == 0 || self.pair_bound(session, member) {
            return Ok(());
        }
        self.state.set(SequenceStatus::Stopped(crate::SyncFailure::NotAdmitted));
        if let Err(error) =
            self.database.end_for_good(coven_database::DeviceStop::NotAdmitted).await
        {
            tracing::error!("a store never admitted keeps its operations journaled: {error}");
        }
        Err(SyncError::NotAdmitted)
    }

    pub(crate) async fn check_copy(
        &self,
        received: &ReceivedSlot,
        path: &ObjectPath,
    ) -> Result<(), SyncError> {
        let received = received.envelope();
        if received.header.device == self.device
            && !self
                .database
                .attempted_slot(received.header.claim, path.clone(), received.digest)
                .await?
        {
            return Err(self
                .stop_copy(crate::DeviceIdEvidence::UnattemptedSlot { slot: received.slot() })
                .await);
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
                    self.state.set(SequenceStatus::Stopped(crate::SyncFailure::Removed));
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
                return Err(SyncError::MemberKeysReplaced { member: device.member });
            }
        }
        self.state.registered(
            session.history.state.devices.get(&self.device).is_some_and(|device| device.active()),
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
                    let location = coven_storage::recorded_config(&self.directory)?;
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
        if at.get() <= session.through {
            return Ok(());
        }
        let mut reloads = Vec::new();
        if let SlotContent::Admin { effect: Some(effect), .. } = &mut incoming.content {
            effect.provider_access = history.provider_access(member.member())?;
            // A circle below this device's version waits for its rebuild
            // instead of reloading (§18.3).
            let breaking = self.database.breaking_version().await?;
            reloads = effect
                .reload
                .iter()
                .filter(|audience| !history.state.rebuilt_by(audience, breaking))
                .cloned()
                .collect();
        }
        // A reload persists the audience gates before any replacement body is
        // read, and keeps the keys after it loads.
        if reloads.is_empty() {
            self.keys.persist(ring)?;
        }
        let result = self
            .database
            .apply_slot::<PendingRecord, _, _>(incoming, history.writers(self.device), || Ok(()))
            .await
            .map_err(|error| crate::write_object::database_failure(&self.path(at), error))?;
        match (result, reloads.is_empty()) {
            // The database applied the slot whole: no audience in it needs
            // replacement data, and a circle below this device's version
            // waits for its rebuild instead (§18.3).
            (coven_database::ApplyOutcome::Applied, _)
            | (coven_database::ApplyOutcome::AlreadyApplied, true) => {
                if !reloads.is_empty() {
                    self.keys.persist(ring)?;
                }
                session.through = at.get();
                session.history = history;
                self.check_binding(session, member)?;
                Ok(())
            }
            (coven_database::ApplyOutcome::AlreadyApplied, false)
            | (
                coven_database::ApplyOutcome::Waiting(coven_database::WriteWait::Reload(_)),
                false,
            ) => self.reload_activation(session, at, &reloads, history, ring, member).await,
            (coven_database::ApplyOutcome::Waiting(wait), _) => {
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
                        // Missing sources arrive with a later slot's write.
                        coven_database::WriteWait::Sources(_) => {
                            PendingReason::Waits(Prerequisite::SequenceCatchUp)
                        }
                    },
                ))))
            }
        }
    }

    /// Every variant is matched by name, so a new one is classified here.
    #[deny(clippy::wildcard_enum_match_arm)]
    async fn block(&self, session: &mut Session, slot: Slot, error: SyncError) -> SyncError {
        if let SyncError::DeviceIdInUse(evidence) = error {
            return self.stop_copy(evidence).await;
        }
        if matches!(error, SyncError::Removed) {
            self.state.set(SequenceStatus::Stopped(crate::SyncFailure::Removed));
            return error;
        }
        let path = self.path(slot);
        let refused = matches!(&error, SyncError::Damaged(object) if object.path == path.as_str());
        if refused {
            if let Err(failed) = self.confirm_refusal(session, slot).await {
                return failed;
            }
        }
        let reason = slot_block_reason(&error, self.storage.config().provider());
        let subject = PendingSubject::Slot(slot);
        self.waits.log_failure(&subject, &reason, error.cause());
        let cause = blocked_cause(session, &subject, &error);
        let record = PendingRecord { subject, reason, cause };
        session.blocked = Some(record.clone());
        self.state.set(SequenceStatus::Blocked(Box::new(record.clone())));
        match self.database.record_pending(record).await {
            Ok(()) => error,
            Err(cleanup) => SyncError::cleanup(error, cleanup.into()),
        }
    }
}

/// The cause a blocked slot's record keeps: the text of the error that
/// blocked it. A pass that stops at the same wait again has no error of its
/// own, so the record keeps the one this run met for it, if any.
/// Why a slot blocks on `error`: the reason it gives any request, except a
/// rejection. A slot's block isn't this device's own request, so a
/// rejection there isn't the app's to answer, and `Never` would leave the
/// sequence with no recourse; it stays a local operation failure (§19.1).
fn slot_block_reason(error: &SyncError, provider: coven_storage::CloudProvider) -> PendingReason {
    match error {
        SyncError::Rejected(_) => PendingReason::Failed(crate::LocalFailure::Operation),
        error => error.pending_reason(provider),
    }
}

fn blocked_cause(session: &Session, slot: &PendingSubject, error: &SyncError) -> Option<String> {
    if let SyncError::Pending(wait) = error {
        if wait.subject == *slot {
            return session
                .blocked
                .as_ref()
                .filter(|blocked| **blocked == **wait)
                .and_then(|blocked| blocked.cause.clone());
        }
    }
    Some(error.cause().to_string())
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
            let path = original.path(store);
            let received =
                slot_object::open_bounded(original.object.clone(), &path, ring, &history)?;
            history.consume(received, member.member());
        }
        history.acquire(ring, member)?;
        Ok(history)
    }

    /// The audience an admin change makes this member read anew: the store on
    /// the member's admission, the boundary's audience on a raise or reset, a
    /// circle the member is added to. One change names at most one.
    fn reload_of(
        &self,
        effect: &coven_database::AdminEffect,
        member: &coven_foundation::id_source::MemberId,
    ) -> Option<Audience> {
        use coven_format::admin::StoreChange::{AddCircleMember, AdmitMember, RaiseSchema, Reset};
        match &effect.change {
            AdmitMember { .. }
                if self.state.store.is_some()
                    && !crate::effects::belongs(&self.state, &Audience::Store, member)
                    && crate::effects::belongs(&effect.state, &Audience::Store, member) =>
            {
                Some(Audience::Store)
            }
            RaiseSchema { .. } | Reset { .. } => effect
                .boundary
                .as_ref()
                .map(|boundary| boundary.snapshot().audience.clone())
                .filter(|audience| crate::effects::belongs(&effect.state, audience, member)),
            AddCircleMember { circle, .. }
                if self.state.circles.contains_key(circle)
                    && effect.state.readable_circles(member).contains(circle)
                    && !self.state.readable_circles(member).contains(circle) =>
            {
                Some(Audience::Circle(*circle))
            }
            _ => None,
        }
    }

    pub(crate) fn consume(
        &mut self,
        received: ReceivedSlot,
        member: coven_foundation::id_source::MemberId,
    ) -> IncomingSlot<coven_database::DownloadedWriteStream<PartPlaintext>> {
        let (received, body) = received.into_parts();
        let slot = received.slot();
        let content = match body {
            SlotBody::Writes { size, writes } => SlotContent::Writes { size, writes },
            SlotBody::Bounded { frame: SlotFrame::Admin(change), original } => {
                // An admission's copies stand in history whether or not its
                // entry counts: a refused one still disclosed them (§12.2).
                let validated = match &received.prefix.kind {
                    SlotKind::Admission(copies) => {
                        self.validate_admission(&received.header, slot, &change, copies)
                    }
                    _ => self.validate(&received.header, slot, &change),
                };
                self.copies.extend(KeyCopy::all(&received.path, &received.prefix));
                let effect = match validated {
                    Ok(mut effect) => {
                        if let Some(effect) = &mut effect {
                            effect.reload = self.reload_of(effect, &member);
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
            SlotBody::Bounded { frame: SlotFrame::KeyCopy, original } => {
                self.copies.extend(KeyCopy::all(&received.path, &received.prefix));
                SlotContent::KeyCopy(original)
            }
            SlotBody::Bounded { frame: SlotFrame::Announcement(announcement), original } => {
                let accepted = if self.announcement_allowed(slot, &received.header, &announcement) {
                    self.announcements.insert(slot, announcement.clone());
                    Some(announcement)
                } else {
                    tracing::debug!(?slot, "consumed stale or unauthorized snapshot announcement");
                    None
                };
                SlotContent::Announcement { original, accepted }
            }
        };
        IncomingSlot { slot, header: received.header, digest: received.digest, content }
    }

    fn announcement_allowed(
        &self,
        slot: Slot,
        header: &coven_format::sequence::SlotHeader,
        announcement: &coven_format::sequence::SnapshotAnnouncement,
    ) -> bool {
        if !crate::effects::belongs(&self.state, &announcement.snapshot.audience, &header.author)
            || !self.state.device_acts_for(&header.device, &header.author)
            || announcement.snapshot.device != header.device
            || announcement.covers >= slot.get()
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
            .map(|prior| prior.boundary.entry().get())
            .max();
        // Covering the latest raise, the audience's version at the coverage
        // is its version now (§16.3).
        let reached = match announcement.purpose {
            Purpose::Raise(version) => version,
            Purpose::Ordinary | Purpose::Reset => {
                self.state.reached(&announcement.snapshot.audience)
            }
        };
        previous.is_none_or(|covers| announcement.covers >= covers)
            && boundary.is_none_or(|at| announcement.covers >= at)
            && announcement.breaking_version <= reached
    }
}

/// The journal row and intention for one provider account; the caller sets
/// whether history already shows it granted.
pub(crate) fn account_change(
    account: &str,
    intended: bool,
    needs_invitation: bool,
) -> Result<ProviderAccessChange, SyncError> {
    Ok(ProviderAccessChange {
        operation: NewOperation {
            kind: operation_kind::PROVIDER_ACCESS.into(),
            data: serde_json::to_vec(account)?,
            started_by: operation_kind::COVEN.into(),
            pauses_writes: false,
        },
        account: account.to_owned(),
        intended,
        initially_granted: false,
        needs_invitation,
    })
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
#[path = "sequence_copies.rs"]
pub(crate) mod sequence_copies;
#[path = "sequence_credentials.rs"]
mod sequence_credentials;
#[path = "sequence_cut_off.rs"]
mod sequence_cut_off;
#[path = "sequence_files.rs"]
mod sequence_files;
#[path = "sequence_gone_requests.rs"]
mod sequence_gone_requests;
#[path = "sequence_gone_storage.rs"]
mod sequence_gone_storage;
#[path = "sequence_invites.rs"]
pub(crate) mod sequence_invites;
#[path = "sequence_join.rs"]
pub(crate) mod sequence_join;
#[path = "sequence_keys.rs"]
mod sequence_keys;
#[path = "sequence_member_keys.rs"]
mod sequence_member_keys;
#[path = "sequence_pairings.rs"]
pub(crate) mod sequence_pairings;
pub(crate) use sequence_member_keys::registered;
#[path = "sequence_operations.rs"]
pub(crate) mod sequence_operations;
#[path = "sequence_own_membership.rs"]
mod sequence_own_membership;
pub use sequence_own_membership::{registration_journaled, removal_journaled};
#[path = "sequence_raises.rs"]
mod sequence_raises;
#[path = "sequence_read.rs"]
mod sequence_read;
pub(crate) use sequence_read::next_slot;
#[path = "sequence_reload.rs"]
pub(crate) mod sequence_reload;
#[path = "sequence_retention.rs"]
pub(crate) mod sequence_retention;
#[path = "sequence_retry.rs"]
mod sequence_retry;
pub(crate) use sequence_retry::{make_retry_due, OperationFailure};
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
#[path = "snapshot_staged.rs"]
mod snapshot_staged;
#[path = "snapshot_upload.rs"]
mod snapshot_upload;
#[path = "sync_loop.rs"]
pub(crate) mod sync_loop;

#[cfg(test)]
#[path = "sequence_sync_tests.rs"]
pub(crate) mod tests;
