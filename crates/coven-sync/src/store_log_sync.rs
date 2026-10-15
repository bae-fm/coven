//! The owner of the store-log storage step; no background tasks or sync loop.

use crate::{
    store_log_keys as keys, store_log_object as object, DamagedObject, Refusal, SyncError,
};
use coven_crypto::{custody::KeySession, MemberIdentity, StoreKeyring};
use coven_database::{Database, LocalStoreLog, StoreLog};
use coven_format::{
    sealed_key::SealedKey,
    store_log::{StoreChange, StoreLogEntry},
    value::EntryId,
};
use coven_foundation::{
    clock::ClockRef,
    id_source::{IdSourceRef, KeyId},
};
use coven_merge::Audience;
use coven_storage::{ObjectPath, Storage, StorageFailure};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Owns store-log, key and snapshot synchronization capabilities (§20.2).
/// Calls require exclusive access so one install never authors or applies two
/// entries concurrently through this owner. The database also checks stale replay.
pub struct StoreLogSync {
    #[cfg(test)]
    received_entry_decodes: Option<Arc<std::sync::atomic::AtomicUsize>>,
    download_limit: std::num::NonZeroUsize,
    /// Storage-time samples, shared with the pass's device-log owner, which
    /// checks its write misses against them (§15).
    finality: Arc<std::sync::Mutex<crate::finality::Observation>>,
    reads: crate::pass_reads::PassReads,
    send: Arc<crate::SendGate>,
    storage: Option<Arc<dyn Storage>>,
    database: Database,
    store_keys: Arc<KeySession<StoreKeyring>>,
    member_keys: Arc<KeySession<MemberIdentity>>,
    clock: ClockRef,
    ids: IdSourceRef,
    directory: coven_foundation::files::StoreDir,
    /// The storage account's admin access, held only by the operation that
    /// needs it and never written anywhere (§4).
    admin_access:
        std::collections::BTreeMap<crate::OperationId, Arc<dyn coven_storage::AccessKeyAdmin>>,
    /// Operation and joining retry waits, and the provider cooldown (§16.5).
    waits: Arc<crate::RetryWaits>,
    /// Commits that can satisfy waiting operations' prerequisites.
    prerequisites: std::sync::Mutex<coven_database::DatabaseChanges>,
    /// This drive's one `join-requests/` listing, serving every open
    /// invite's due check, or the failure it met (SP step 5).
    join_requests: Option<Result<operation_invites::RequestListing, StorageFailure>>,
}

impl StoreLogSync {
    /// Compose the step from retained capabilities; none are handed to callers.
    pub fn new(
        storage: Arc<dyn Storage>,
        database: Database,
        store_keys: Arc<KeySession<StoreKeyring>>,
        member_keys: Arc<KeySession<MemberIdentity>>,
        clock: ClockRef,
        send: Arc<crate::SendGate>,
        ids: IdSourceRef,
        directory: coven_foundation::files::StoreDir,
        waits: Arc<crate::RetryWaits>,
    ) -> Self {
        Self {
            #[cfg(test)]
            received_entry_decodes: None,
            download_limit: std::num::NonZeroUsize::MIN,
            finality: Arc::default(),
            reads: crate::pass_reads::PassReads::default(),
            send,
            waits,
            prerequisites: std::sync::Mutex::new(database.operation_prerequisite_changes()),
            storage: Some(storage),
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
            admin_access: Default::default(),
            join_requests: None,
        }
    }

    /// Compose a disconnected store that can read local state and resume retained work.
    pub fn disconnected(
        database: Database,
        store_keys: Arc<KeySession<StoreKeyring>>,
        member_keys: Arc<KeySession<MemberIdentity>>,
        clock: ClockRef,
        send: Arc<crate::SendGate>,
        ids: IdSourceRef,
        directory: coven_foundation::files::StoreDir,
        waits: Arc<crate::RetryWaits>,
    ) -> Self {
        Self {
            #[cfg(test)]
            received_entry_decodes: None,
            download_limit: std::num::NonZeroUsize::MIN,
            finality: Arc::default(),
            reads: crate::pass_reads::PassReads::default(),
            send,
            waits,
            prerequisites: std::sync::Mutex::new(database.operation_prerequisite_changes()),
            storage: None,
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
            admin_access: Default::default(),
            join_requests: None,
        }
    }

    /// Bound simultaneous membership and key-copy requests in subsequent scans.
    pub fn set_discovery_download_limit(&mut self, limit: std::num::NonZeroUsize) {
        self.download_limit = limit;
    }

    /// Share reads and return evidence during this pass. Sending authority was
    /// already injected into both owners at the composition root.
    pub(crate) fn begin_pass(
        &self,
        writes: &mut crate::DeviceLogSync,
    ) -> crate::pass_reads::ReadScope {
        writes.share_reads(self.reads.clone());
        writes.share_observation(self.finality.clone());
        writes.set_download_limit(self.download_limit);
        self.reads.enter()
    }

    pub(crate) fn invalidate_sends(&self) {
        self.send.invalidate();
    }

    pub(crate) fn send_stopped(&self) -> bool {
        self.send.stopped().is_some()
    }

    pub(crate) fn copy_observation(&self) -> Result<Vec<keys::ObservedCopy>, SyncError> {
        self.send.observation()
    }

    /// Publish entries, replay downloads, acquire keys and resume snapshot work.
    /// A missing dependency or sealed copy waits solely in storage for a later
    /// call; each waiting or refused object is its own subject's pending record.
    pub async fn sync_store_log(&mut self) -> Result<(), SyncError> {
        let _reads = self.reads.enter();
        self.step().await?;
        self.schedule_version_changes().await?;
        self.resume_snapshots().await
    }

    /// Catch up, then settle key work and the reserved entry in that order:
    /// discovery always precedes publication.
    async fn step(&self) -> Result<(), SyncError> {
        if self.catch_up_membership().await? {
            self.match_keys().await?;
            self.settle_queued_entry().await?;
        }
        Ok(())
    }

    /// Phase 1 of a pass: discover and apply the store log and read key
    /// copies and identity, publishing nothing. Its completion serves every
    /// operation of this pass in place of their own catch-up (SP step 5).
    pub(crate) async fn catch_up_membership(&self) -> Result<bool, SyncError> {
        let complete = self.scan_store_log(false).await?;
        // A complete phase installed its own catch-up, so `can_send` judges
        // this phase, never an earlier one that is still fresh.
        self.reads.keep_membership(complete && self.send.can_send());
        Ok(complete)
    }

    /// Phase 2's key work: publish reserved copies, then share historical
    /// keys with current members who lack them. Without a fresh completed
    /// first phase, sharing waits (SP step 2).
    pub(crate) async fn match_keys(&self) -> Result<(), SyncError> {
        if !self.send.can_send() {
            return Ok(());
        }
        let local = self.database.local_store_log().await?;
        let ring = self.store_keys.read()?;
        self.share_keys(&local.log, &ring).await
    }

    /// SP step 5 begins with the reserved store-log entry, whose number
    /// cannot be abandoned, before any operation reserves another. Callers
    /// settle it only after a completed first phase; the send gate checks
    /// that phase's freshness before every request.
    pub(crate) async fn settle_queued_entry(&self) -> Result<(), SyncError> {
        let mut local = self.database.local_store_log().await?;
        if local.upload.is_none() {
            return Ok(());
        }
        let member = self.operation_member()?;
        let mut ring = self.store_keys.read()?;
        // Only an admission entry publishes without sending authority; its
        // binding must be observed before any ordinary send (§10).
        let admission = !self.send.can_send();
        self.publish(&mut local, &member, &mut ring).await?;
        if admission && local.upload.is_none() {
            self.scan_store_log_once(true).await?;
        }
        Ok(())
    }

    /// Fix and publish a new entry. Existing queued entries go first. The change's
    /// key introductions name material already in custody and commit its hash.
    /// Administrative callers must complete online catch-up before calling this
    /// authoring primitive. After a failed upload, call `sync_store_log` to retry
    /// that same entry.
    pub async fn make_and_upload_entry(
        &mut self,
        change: StoreChange,
    ) -> Result<EntryId, SyncError> {
        let _scope = self.reads.enter();
        self.reads.reset_discovery();
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        let mut local = self.database.local_store_log().await?;
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let mut ring = self.store_keys.read()?;
        self.check_stopped(&local, &member)?;
        self.update_keys(&local.log, &member, &mut ring).await?;
        self.publish(&mut local, &member, &mut ring).await?;
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        if let StoreChange::CreateStore { store, .. } = &change {
            if *store != local.store {
                return Err(SyncError::WrongStore {
                    expected: local.store,
                    actual: *store,
                });
            }
        }
        keys::check_shared_keys(&local.log, &change, &ring)?;
        let bootstrap = !local.log.replay.state.devices.contains_key(&local.device)
            && (matches!(&change, StoreChange::CreateStore { .. })
                || matches!(&change, StoreChange::AddDevice { device, .. } if *device == local.device));
        let observed = if matches!(change, StoreChange::CreateStore { .. }) {
            Vec::new()
        } else {
            self.send.observation()?
        };
        let author = member.clone();
        let id = self
            .database
            .prepare_store_log(member.member(), change, move |log, entry, copies| {
                keys::seal(copies, log, entry, ring.as_ref(), &author, &observed)
            })
            .await?;
        // Re-read the committed queue, so the first attempt uses the same path as
        // every retry and never depends on a callback's transient return value.
        local = self.database.local_store_log().await?;
        let mut ring = self.store_keys.read()?;
        self.publish(&mut local, &member, &mut ring).await?;
        if let Some(coven_database::EntryOutcome::Dropped(reason)) =
            local.log.replay.entries.get(&id)
        {
            return Err(SyncError::Rejected(reason.clone()));
        }
        if bootstrap {
            // Admission's exact-path exception supplies no ordinary authority.
            // Observe the published binding before allowing subsequent sends.
            self.scan_store_log_once(true).await?;
        }
        Ok(id)
    }

    /// A complete catch-up: every received entry applied and this pass's
    /// key-copy observation complete (§11).
    /// Within a pass, its completed first phase is the catch-up; one that
    /// did not complete, or has expired, waits for the next pass (SP steps 2
    /// and 5). Outside a pass, read the store log and key copies now.
    async fn catch_up(&self) -> Result<(), SyncError> {
        match self.reads.membership() {
            Some(true) if self.send.can_send() => Ok(()),
            Some(_) => Err(StorageFailure::CatchUpRequired.into()),
            None => self.scan_store_log(true).await.map(|_| ()),
        }
    }

    /// Settle an entry whose complete bytes failed a check. A newer format is
    /// not a verdict: it is returned as the entry's wait for that update, and
    /// its log stops there as it would at a refusal (§19.1).
    async fn stuck_entry(
        &self,
        entry: EntryId,
        path: &ObjectPath,
        failure: Refusal,
    ) -> Result<Option<crate::PendingReason>, SyncError> {
        if let Some(update) = failure.required_update() {
            tracing::debug!(
                path = path.as_str(),
                ?update,
                "store-log entry needs a newer coven"
            );
            return Ok(Some(crate::PendingReason::UpdateRequired(update)));
        }
        crate::pending_logs::record_refusal(
            &self.database,
            coven_database::LogRefusal {
                object: coven_database::LogObject::Entry(entry),
                failure: (&failure).into(),
            },
        )
        .await?;
        tracing::warn!(path = path.as_str(), error = %failure, "store log is stuck");
        Ok(None)
    }

    fn check_stopped(
        &self,
        local: &LocalStoreLog,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let state = &local.log.replay.state;
        if state
            .members
            .get(&member.member())
            .is_some_and(|m| m.removed)
            || state
                .devices
                .get(&local.device)
                .is_some_and(|d| d.removed())
        {
            self.send.invalidate();
            return Err(SyncError::Removed);
        }
        // Held credentials a kept replacement retired cannot author anything;
        // the installation keeps its device id and queues (§10).
        if crate::effects::member(state, &member.member())
            .is_some_and(|held| held.keys != member.keys().public_keys())
        {
            return Err(SyncError::MemberKeysReplaced {
                member: member.member(),
            });
        }
        Ok(())
    }

    async fn publish(
        &self,
        local: &mut LocalStoreLog,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<(), SyncError> {
        if self.can_publish(local).await? {
            self.publish_queued(local, member, ring).await?;
        }
        Ok(())
    }

    async fn can_publish(&self, local: &LocalStoreLog) -> Result<bool, SyncError> {
        if let Some(upload) = &local.upload {
            // Recovery can retain a fixed entry before restoring its past.
            // Download that past before publishing or replaying the entry.
            if !object::ready(&local.log, &upload.entry, local.store).map_err(|failure| {
                SyncError::Damaged(DamagedObject {
                    path: object::path(self.directory.id(), upload.entry.position).into(),
                    failure,
                })
            })? {
                tracing::debug!(entry = ?upload.entry.position, "queued entry waits for its recorded past");
                return Ok(false);
            }
            if let Some(id) = self.pending_reload().await? {
                tracing::debug!(?id, "store-log publication waits for snapshot reload");
                return Ok(false);
            }
            for record in self.database.operations().await? {
                if !self.operation_can_retry(&record).await?
                    && crate::operation_data::Data::read(&record)?
                        .entry()?
                        .is_some_and(|entry| entry.position == upload.entry.position)
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn publish_queued(
        &self,
        local: &mut LocalStoreLog,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<(), SyncError> {
        if let Some(entry) = self.upload_queued(local, member, ring).await? {
            // Observe all landing witnesses after publication, before retaining
            // this entry's author-view checks.
            let publications = crate::store_log_landing::observe_publication(
                &self.database,
                &self.reads,
                self.download_limit,
                self.directory.id(),
                self.storage.as_deref(),
                &local.log,
                entry.position,
            )
            .await?;
            self.apply(local, entry, &publications, member, ring)
                .await?;
        }
        Ok(())
    }

    async fn upload_queued(
        &self,
        local: &LocalStoreLog,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<Option<StoreLogEntry>, SyncError> {
        if let Some(upload) = &local.upload {
            if let StoreChange::CreateStore { key_hash, .. } = &upload.entry.change {
                if upload.sealing.keys.is_empty()
                    && !keys::holds(ring.as_ref(), &Audience::Store, upload.sealing.key)
                {
                    // Recovering an unpublished creation needs this member's
                    // copy of its key: a missing or refused copy is that key
                    // copy's record and fails publication loudly (§19.1).
                    let key = match keys::read_store_key(
                        &self.database,
                        &self.reads,
                        self.download_limit,
                        self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                        self.directory.id(),
                        upload.sealing.key,
                        Some(key_hash),
                        member,
                    )
                    .await
                    {
                        Ok(key) => key,
                        Err(error) => {
                            let reason = match &error {
                                SyncError::Damaged(damage) => {
                                    crate::PendingReason::Refused(damage.failure.clone())
                                }
                                SyncError::KeyUnavailable(key) => {
                                    crate::PendingReason::KeyUnavailable {
                                        audience: Audience::Store,
                                        key: *key,
                                    }
                                }
                                _ => return Err(error),
                            };
                            let record = self
                                .record_key_condition(
                                    &Audience::Store,
                                    upload.sealing.key,
                                    member,
                                    Some(reason),
                                )
                                .await?
                                .expect("a recorded condition");
                            return Err(SyncError::Pending(Box::new(record)));
                        }
                    };
                    match ring {
                        Some(ring) => ring.insert_store_key(key)?,
                        None => *ring = Some(StoreKeyring::new(key)),
                    }
                    self.store_keys
                        .persist(ring.as_ref().expect("recovered creation key"))?;
                }
            }
            let bytes = object::seal_upload(
                self.directory.id(),
                &local.log,
                upload,
                ring.as_ref(),
                member,
            )?;
            let _bootstrap = self.bootstrap_publication(local, upload, member)?;
            object::upload(
                self.directory.id(),
                self.storage.as_deref(),
                &self.database,
                &self.send,
                upload,
                &bytes,
            )
            .await?;
            return Ok(Some(upload.entry.clone()));
        }
        Ok(None)
    }

    fn bootstrap_publication<'a>(
        &'a self,
        local: &LocalStoreLog,
        upload: &coven_database::StoreLogUpload,
        member: &MemberIdentity,
    ) -> Result<Option<crate::PublicationScope<'a>>, SyncError> {
        let membership = match &upload.entry.change {
            StoreChange::CreateStore { store, admin, .. }
                if local.log.replay.state.store.is_none()
                    && *store == local.store
                    && upload.entry.author == member.member()
                    && *admin == member.keys().public_keys() =>
            {
                false
            }
            StoreChange::AddDevice { device, .. }
                if *device == local.device
                    && !local.log.replay.state.devices.contains_key(device) =>
            {
                true
            }
            _ => return Ok(None),
        };
        if upload.entry.author != member.member() {
            return Err(coven_database::DropReason::NotAllowed.into());
        }
        let paths = std::iter::once(object::path(local.store, upload.entry.position))
            .chain(
                upload
                    .sealing
                    .keys
                    .iter()
                    .map(|copy| copy.path(local.store)),
            )
            .collect();
        Ok(Some(self.send.bootstrap(paths, membership)))
    }

    async fn apply(
        &self,
        local: &mut LocalStoreLog,
        entry: StoreLogEntry,
        publications: &BTreeMap<EntryId, coven_format::value::StorageTime>,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<(), SyncError> {
        let (entry, replay) = crate::replay_entry(&local.log, entry, publications);
        self.apply_batch(local, vec![entry], replay, member).await?;
        self.update_keys(&local.log, member, ring).await
    }

    async fn apply_batch(
        &self,
        local: &mut LocalStoreLog,
        entries: Vec<coven_database::ReplayEntry>,
        replay: coven_database::StoreLogReplay,
        member: &MemberIdentity,
    ) -> Result<(), SyncError> {
        let mut operations = Vec::new();
        let records = self.database.operations().await?;
        let data = records
            .iter()
            .map(crate::operation_data::Data::read)
            .collect::<Result<Vec<_>, _>>()?;
        let next = StoreLog {
            entries: local.log.entries.iter().chain(&entries).cloned().collect(),
            replay: replay.clone(),
            horizon: local.log.horizon,
        };
        let provider_access = self.access_changes(&next, &data)?;
        let deletions = crate::access_keys::deletions(&local.log.entries, &entries, &replay)?;
        let before = &local.log.replay.state;
        let after = &replay.state;
        let mut changed: BTreeSet<_> = before
            .schema
            .keys()
            .chain(after.schema.keys())
            .chain(before.resets.keys())
            .chain(after.resets.keys())
            .filter(|audience| {
                before.schema.get(*audience) != after.schema.get(*audience)
                    || before.resets.get(*audience) != after.resets.get(*audience)
            })
            .cloned()
            .collect();
        // A circle readable again may have parts this device skipped under
        // positions it has since passed. The database requires its reload in the
        // replay's transaction; record the operation that performs it there too.
        changed.extend(
            local
                .log
                .returning_circles(after, &member.member())
                .into_iter()
                .map(Audience::Circle),
        );
        if !changed.is_empty() {
            operations.push(
                crate::operation_data::Data::Snapshots(crate::snapshot_data::SnapshotTask {
                    job: crate::snapshot_data::SnapshotJob::Reload {
                        scope: crate::snapshot_data::ReloadScope::Changed(
                            changed.into_iter().collect(),
                        ),
                        files: None,
                    },
                    temporary: Vec::new(),
                })
                .new_operation("coven")?,
            );
        }
        let mut updates = Vec::new();
        for record in records {
            let data = crate::operation_data::Data::read(&record)?;
            if self.operation_can_retry(&record).await?
                && record.last_step < data.entry_step_number(4)
                && data.entry()?.is_some_and(|fixed| {
                    entries
                        .iter()
                        .any(|entry| fixed.position == entry.entry.position)
                })
            {
                updates.push(data.update(&record, data.entry_step_number(4))?);
            }
        }
        self.database
            .apply_store_log_batch::<crate::PendingRecord>(
                local.log.clone(),
                entries,
                replay,
                operations,
                updates,
                deletions,
                provider_access,
            )
            .await?;
        *local = self.database.local_store_log().await?;
        self.check_stopped(local, member)
    }

    /// Open newly needed keys from the completed catch-up's copies. Without a
    /// fresh completion there is nothing current to open them from.
    async fn update_keys(
        &self,
        log: &StoreLog,
        member: &MemberIdentity,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<(), SyncError> {
        if !self.send.can_send() {
            return Ok(());
        }
        self.acquire_observed_keys(log, member, ring, &self.send.observation()?)
            .await
    }

    /// Record this pass's key-copy reading for its senders, then share every
    /// historical key a current audience member lacks. An incomplete reading
    /// is recorded as such; it blocks the work that needs a complete one.
    async fn share_keys(
        &self,
        log: &StoreLog,
        ring: &Option<StoreKeyring>,
    ) -> Result<(), SyncError> {
        // Reserved copies precede all new sharing decisions. An observed duplicate
        // or removed recipient must never erase a writer's already reserved number.
        object::upload_keys(
            self.directory.id(),
            self.storage.as_deref(),
            &self.database,
            &self.send,
        )
        .await?;
        let needs = crate::key_introductions::needed(log);
        if needs.is_empty() {
            return Ok(());
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let copies = self.send.observation()?;
        for (audience, key) in needs.keys().cloned() {
            if !crate::key_introductions::accepted(ring.as_ref(), &needs, &audience, key) {
                tracing::debug!(?audience, ?key, "historical key has not arrived");
                continue;
            }
            let ring = ring.as_ref().expect("held key has a keyring");
            for (member, recipient) in keys::recipients(log, &audience) {
                if copies.iter().any(|copy| {
                    copy.prefix.audience == audience
                        && copy.prefix.key == key
                        && copy.prefix.member == *member
                }) {
                    tracing::debug!(?audience, ?key, ?member, "key copy already observed");
                    continue;
                }
                let recipient = *recipient;
                let member = *member;
                match audience {
                    Audience::Store => {
                        let key = ring.store_key(key)?.clone();
                        self.database
                            .prepare_key_upload(move |path| {
                                keys::store_copy(path, &key, &member, &recipient)
                            })
                            .await?;
                    }
                    Audience::Circle(circle) => {
                        let key = ring.circle_key(circle, key)?.clone();
                        self.database
                            .prepare_key_upload(move |path| {
                                keys::circle_copy(path, &key, &member, &recipient)
                            })
                            .await?;
                    }
                }
                object::upload_keys(
                    self.directory.id(),
                    Some(storage),
                    &self.database,
                    &self.send,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn read(&self, path: &ObjectPath) -> Result<Option<Vec<u8>>, SyncError> {
        match self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .read(path)
            .await
        {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.failure() == StorageFailure::NotFound => {
                tracing::debug!(path = path.as_str(), "object has not arrived");
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "store_log_sync_tests.rs"]
pub(crate) mod tests;

#[path = "operation_access.rs"]
mod operation_access;
#[path = "operation_access_keys.rs"]
mod operation_access_keys;
#[path = "operation_calls.rs"]
mod operation_calls;
#[path = "operation_invites.rs"]
mod operation_invites;
#[path = "operation_minting.rs"]
mod operation_minting;
#[path = "operation_pending.rs"]
mod operation_pending;
#[path = "operation_steps.rs"]
mod operation_steps;

#[path = "schema_sync.rs"]
mod schema_sync;
#[path = "snapshots.rs"]
mod snapshots;

#[path = "bootstrap.rs"]
mod bootstrap;
#[path = "storage_setup.rs"]
mod storage_setup;

pub use bootstrap::JoinOutcome;

#[path = "store_log_finality.rs"]
mod store_log_finality;

#[path = "store_log_batch.rs"]
mod store_log_batch;
#[path = "store_log_identity.rs"]
mod store_log_identity;

#[path = "key_acquisition.rs"]
mod key_acquisition;

#[path = "key_rotation.rs"]
mod key_rotation;
