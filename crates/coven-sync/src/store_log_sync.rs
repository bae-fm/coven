//! The owner of the store-log storage step; no background tasks or sync loop.

use crate::{
    store_log_keys as keys, store_log_object as object, DamagedObject, Refusal, SyncError,
    SyncFailure,
};
use coven_crypto::{custody::KeySession, MemberKeys, StoreKeyring};
use coven_database::{Database, LocalStoreLog, StoreLog};
use coven_format::{
    sealed_key::SealedKey,
    sealed_single::SingleChunkObject,
    store_log::{StoreChange, StoreLogEntry},
    value::EntryId,
};
use coven_foundation::{
    clock::ClockRef,
    id_source::{IdSourceRef, KeyId},
};
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, Storage, StorageFailure};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Owns store-log, key and snapshot synchronization capabilities (§20.2).
/// Calls require exclusive access so one install never authors or applies two
/// entries concurrently through this owner. The database also checks stale replay.
pub struct StoreLogSync {
    #[cfg(test)]
    received_entry_decodes: Option<Arc<std::sync::atomic::AtomicUsize>>,
    finality: std::sync::Mutex<crate::finality::Observation>,
    reads: crate::pass_reads::PassReads,
    storage: Option<Arc<dyn Storage>>,
    database: Database,
    store_keys: Arc<KeySession<StoreKeyring>>,
    member_keys: Arc<KeySession<MemberKeys>>,
    clock: ClockRef,
    ids: IdSourceRef,
    directory: coven_foundation::files::StoreDir,
}

impl StoreLogSync {
    /// Compose the step from retained capabilities; none are handed to callers.
    pub fn new(
        storage: Arc<dyn Storage>,
        database: Database,
        store_keys: Arc<KeySession<StoreKeyring>>,
        member_keys: Arc<KeySession<MemberKeys>>,
        clock: ClockRef,
        ids: IdSourceRef,
        directory: coven_foundation::files::StoreDir,
    ) -> Self {
        Self {
            #[cfg(test)]
            received_entry_decodes: None,
            finality: std::sync::Mutex::default(),
            reads: crate::pass_reads::PassReads::default(),
            storage: Some(storage),
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
        }
    }

    /// Compose a disconnected store that can read local state and resume retained work.
    pub fn disconnected(
        database: Database,
        store_keys: Arc<KeySession<StoreKeyring>>,
        member_keys: Arc<KeySession<MemberKeys>>,
        clock: ClockRef,
        ids: IdSourceRef,
        directory: coven_foundation::files::StoreDir,
    ) -> Self {
        Self {
            #[cfg(test)]
            received_entry_decodes: None,
            finality: std::sync::Mutex::default(),
            reads: crate::pass_reads::PassReads::default(),
            storage: None,
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
        }
    }

    /// Share metadata and checked references with device-log downloads until
    /// this pass ends. Standalone calls retain their own scoped reads.
    pub(crate) fn begin_pass(
        &self,
        writes: &mut crate::DeviceLogSync,
    ) -> crate::pass_reads::ReadScope {
        writes.share_reads(self.reads.clone());
        self.reads.enter()
    }

    /// Publish entries, replay downloads, acquire keys and resume snapshot work.
    /// A missing dependency or sealed copy waits solely in storage for a later call.
    /// Returns skipped damage so connecting storage can reject incomplete key setup.
    pub async fn sync_store_log(&mut self) -> Result<Vec<DamagedObject>, SyncFailure> {
        let _reads = self.reads.enter();
        let mut damages = self.step().await.map_err(SyncFailure::from)?;
        self.schedule_version_changes()
            .await
            .map_err(SyncFailure::from)?;
        damages.extend(self.resume_snapshots().await.map_err(SyncFailure::from)?);
        Ok(damages)
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
        if let Some(id) = self.pending_reload().await? {
            return Err(SyncError::ReloadPending(id));
        }
        let mut local = self.database.local_store_log().await?;
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let mut ring = self.store_keys.read()?;
        let mut damages = Vec::new();
        self.check_stopped(&local, &member)?;
        self.update_keys(&local.log, &member, &mut ring, &mut damages)
            .await?;
        self.publish(&mut local, &member, &mut ring, &mut damages)
            .await?;
        if let Some(damaged) = damages.into_iter().next() {
            return Err(damaged.into());
        }
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
        let mut damages = Vec::new();
        let observed = keys::read_copies(
            self.storage.as_deref().ok_or(SyncError::NoStorage)?,
            local.store,
            &mut damages,
        )
        .await?;
        if let Some(damaged) = damages.into_iter().next() {
            return Err(damaged.into());
        }
        let author = member.clone();
        let id = self
            .database
            .prepare_store_log(member.member_id(), change, move |log, entry, copies| {
                keys::seal(copies, log, entry, ring.as_ref(), &author, &observed)
            })
            .await?;
        // Re-read the committed queue, so the first attempt uses the same path as
        // every retry and never depends on a callback's transient return value.
        local = self.database.local_store_log().await?;
        let mut ring = self.store_keys.read()?;
        let mut damages = Vec::new();
        self.publish(&mut local, &member, &mut ring, &mut damages)
            .await?;
        if let Some(damaged) = damages.into_iter().next() {
            return Err(damaged.into());
        }
        if let Some(coven_database::EntryOutcome::Dropped(reason)) =
            local.log.replay.entries.get(&id)
        {
            return Err(SyncError::Rejected(reason.clone()));
        }
        Ok(id)
    }

    async fn catch_up(&self) -> Result<(), SyncError> {
        let damages = self.scan_store_log(true).await?;
        if let Some(damage) = damages.into_iter().next() {
            return Err(damage.into());
        }
        Ok(())
    }

    async fn step(&self) -> Result<Vec<DamagedObject>, SyncError> {
        self.scan_store_log(false).await
    }

    async fn scan_store_log_once(
        &self,
        require_complete: bool,
    ) -> Result<(Vec<DamagedObject>, bool), SyncError> {
        let through = self
            .finality
            .lock()
            .expect("finality lock poisoned")
            .through();
        let mut local = self.database.local_store_log().await?;
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let mut ring = self.store_keys.read()?;
        let mut damages = Vec::new();
        self.check_stopped(&local, &member)?;
        self.update_keys(&local.log, &member, &mut ring, &mut damages)
            .await?;
        self.publish(&mut local, &member, &mut ring, &mut damages)
            .await?;
        let mut paths = self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .list(&ObjectPrefix::store_logs(self.directory.id()))
            .await?;
        // Listing discovers writers. Exact status reads establish each writer's
        // terminal miss; a listed suffix beyond a gap cannot certify finality.
        let writers: BTreeSet<_> = local
            .log
            .entries
            .iter()
            .map(|entry| entry.entry.position.device)
            .chain(local.log.replay.state.devices.keys().copied())
            .chain(
                paths
                    .iter()
                    .filter_map(|object| object.path.store_log_position().map(|p| p.0)),
            )
            .collect();
        for device in writers {
            let mut number = local
                .log
                .entries
                .iter()
                .filter(|entry| entry.entry.position.device == device)
                .map(|entry| entry.entry.position.number)
                .max()
                .unwrap_or(0);
            loop {
                number = number.checked_add(1).ok_or_else(|| {
                    StorageFailure::Protocol.with_source("store-log sequence exhausted")
                })?;
                let path = object::path(local.store, EntryId { device, number });
                let Some(stored) = self
                    .storage
                    .as_deref()
                    .ok_or(SyncError::NoStorage)?
                    .status(&path)
                    .await?
                else {
                    break;
                };
                crate::store_log_landing::add_observation(&mut paths, stored)?;
            }
        }
        let publications = crate::store_log_landing::publication_times(&local.log, &paths)?;
        for object in &paths {
            self.finality
                .lock()
                .expect("finality lock poisoned")
                .observe(object);
        }
        let mut entries = BTreeMap::new();
        for stored in paths {
            let (device, number) = stored.path.store_log_position().ok_or(
                coven_storage::StorageFailure::Protocol
                    .with_source("store-log listing returned another layout"),
            )?;
            entries.insert(
                EntryId {
                    device,
                    number: number.get(),
                },
                stored,
            );
        }
        let stuck = crate::pending_logs::local_refusals(&self.database).await?;
        if require_complete {
            if let Some(refusal) = stuck
                .iter()
                .find(|refusal| matches!(refusal.object, coven_database::LogObject::Entry(_)))
            {
                return Err(crate::pending_logs::required(&self.database, *refusal).await?);
            }
        }
        entries.retain(|id, _| {
            !stuck
                .iter()
                .any(|record| record.blocks(coven_database::LogObject::Entry(*id)))
        });
        let mut pending_devices = BTreeSet::new();
        let mut cache = BTreeMap::new();
        loop {
            let mut advanced = false;
            for (id, stored) in &entries {
                let path = &stored.path;
                if pending_devices.contains(&id.device) || local.log.replay.entries.contains_key(id)
                {
                    continue;
                }
                if id.number > 1
                    && !local.log.replay.entries.contains_key(&EntryId {
                        number: id.number - 1,
                        ..*id
                    })
                {
                    continue;
                }
                if !cache.contains_key(id) {
                    let Some(bytes) = self.read_entry(stored, require_complete).await? else {
                        pending_devices.insert(id.device);
                        continue;
                    };
                    cache.insert(*id, bytes);
                }
                let envelope = match SingleChunkObject::decode(&cache[id]) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        self.stuck_entry(*id, path, Refusal::from(error)).await?;
                        pending_devices.insert(id.device);
                        continue;
                    }
                };
                let SingleChunkObject::StoreLog { key, .. } = &envelope else {
                    self.stuck_entry(
                        *id,
                        path,
                        crate::write_object::invalid("expected a store-log envelope"),
                    )
                    .await?;
                    pending_devices.insert(id.device);
                    continue;
                };
                if !self
                    .acquire(&Audience::Store, *key, &member, &mut ring, &mut damages)
                    .await?
                {
                    if require_complete {
                        if let Some(damage) = damages.into_iter().next() {
                            return Err(damage.into());
                        }
                        return Err(SyncError::KeyUnavailable(*key));
                    }
                    continue;
                }
                #[cfg(test)]
                if let Some(counter) = &self.received_entry_decodes {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let entry = match object::open(
                    &envelope,
                    path,
                    ring.as_ref().expect("acquired keyring").store_key(*key)?,
                ) {
                    Ok(entry) => entry,
                    Err(failure) => {
                        self.stuck_entry(*id, path, failure).await?;
                        pending_devices.insert(id.device);
                        continue;
                    }
                };
                match object::ready(&local.log, &entry, local.store) {
                    Ok(true) => {
                        self.apply(
                            &mut local,
                            entry,
                            &publications,
                            &member,
                            &mut ring,
                            &mut damages,
                        )
                        .await?
                    }
                    Ok(false) => continue,
                    Err(failure) => {
                        self.stuck_entry(*id, path, failure).await?;
                        pending_devices.insert(id.device);
                        continue;
                    }
                }
                advanced = true;
            }
            if !advanced {
                break;
            }
        }
        if require_complete {
            if let Some(refusal) = crate::pending_logs::local_refusals(&self.database)
                .await?
                .into_iter()
                .find(|refusal| matches!(refusal.object, coven_database::LogObject::Entry(_)))
            {
                return Err(crate::pending_logs::required(&self.database, refusal).await?);
            }
            if let Some(id) = entries.keys().find(|id| {
                !local.log.replay.entries.contains_key(id) && !pending_devices.contains(&id.device)
            }) {
                return Err(StorageFailure::NotFound
                    .with_source(format!(
                        "store-log catch-up is missing an entry or its prerequisites at {}",
                        object::path(self.directory.id(), *id).as_str()
                    ))
                    .into());
            }
        }
        let complete_through = |through: Option<std::time::Duration>| {
            let includes = |time: &std::time::SystemTime| {
                through.is_none_or(|end| {
                    time.duration_since(std::time::UNIX_EPOCH)
                        .expect("nonnegative provider time")
                        <= end
                })
            };
            publications
                .iter()
                .filter(|(_, time)| includes(time))
                .all(|(id, _)| local.log.replay.entries.contains_key(id))
                && stuck.iter().all(|refusal| match refusal.object {
                    coven_database::LogObject::Entry(id) => {
                        publications.get(&id).is_some_and(|time| !includes(time))
                    }
                    coven_database::LogObject::Write(_) => true,
                })
        };
        let complete = complete_through(None);
        if let Some((through, unit)) = through {
            if complete_through(Some(through)) {
                if let Some(horizon) = crate::finality::horizon(&local.log, through, unit) {
                    if local.log.horizon != Some(horizon) {
                        self.database
                            .advance_store_log_horizon(local.log, horizon)
                            .await?;
                    }
                }
            }
        }
        Ok((damages, complete))
    }

    async fn read_entry(
        &self,
        object: &coven_storage::StoredObject,
        require_present: bool,
    ) -> Result<Option<Vec<u8>>, SyncError> {
        let bytes = self.read(&object.path).await?;
        if require_present && bytes.is_none() {
            return Err(StorageFailure::NotFound
                .with_source(format!(
                    "store-log catch-up could not read {}",
                    object.path.as_str()
                ))
                .into());
        }
        if bytes
            .as_ref()
            .is_some_and(|bytes| bytes.len() as u64 != object.size)
        {
            return Err(StorageFailure::Protocol
                .with_source("store-log read length differs from listing")
                .into());
        }
        Ok(bytes)
    }

    async fn stuck_entry(
        &self,
        entry: EntryId,
        path: &ObjectPath,
        failure: Refusal,
    ) -> Result<(), SyncError> {
        if let Refusal::Parse { cause: Some(error) } = &failure {
            if crate::error::newer_format(error.as_ref()) {
                return Err(SyncFailure::UpdateRequired(
                    crate::error::required_format(error.as_ref()).expect("newer format"),
                )
                .into());
            }
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
        Ok(())
    }

    fn check_stopped(&self, local: &LocalStoreLog, member: &MemberKeys) -> Result<(), SyncError> {
        let state = &local.log.replay.state;
        if state
            .members
            .get(&member.member_id())
            .is_some_and(|m| m.removed)
            || state.devices.get(&local.device).is_some_and(|d| d.removed)
        {
            return Err(SyncFailure::Removed.into());
        }
        Ok(())
    }

    async fn publish(
        &self,
        local: &mut LocalStoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<(), SyncError> {
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
                return Ok(());
            }
            if let Some(id) = self.pending_reload().await? {
                tracing::debug!(?id, "store-log publication waits for snapshot reload");
                return Ok(());
            }
            for record in self.database.operations().await? {
                if !self.operation_can_retry(&record).await?
                    && crate::operation_data::Data::read(&record)?
                        .entry()?
                        .is_some_and(|entry| entry.position == upload.entry.position)
                {
                    return Ok(());
                }
            }
        }
        self.publish_queued(local, member, ring, damages).await
    }

    async fn publish_queued(
        &self,
        local: &mut LocalStoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<(), SyncError> {
        if let Some(upload) = local.upload.take() {
            if let StoreChange::CreateStore { key_hash, .. } = &upload.entry.change {
                if upload.sealing.keys.is_empty()
                    && !keys::holds(ring.as_ref(), &Audience::Store, upload.sealing.key)
                {
                    let key = keys::read_store_key(
                        self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                        self.directory.id(),
                        upload.sealing.key,
                        member,
                    )
                    .await?;
                    key.check_hash(key_hash)?;
                    match ring {
                        Some(ring) => ring.insert_store_key(key)?,
                        None => *ring = Some(StoreKeyring::new(key)),
                    }
                    self.store_keys
                        .persist(ring.as_ref().expect("recovered creation key"))?;
                }
            }
            let bytes = object::seal_upload(self.directory.id(), &upload, ring.as_ref(), member)?;
            object::upload(
                self.directory.id(),
                self.storage.as_deref(),
                &self.database,
                &upload,
                &bytes,
            )
            .await?;
            // Observe all landing witnesses after publication, before retaining
            // this entry's author-view checks.
            let publications = crate::store_log_landing::observe_publication(
                self.directory.id(),
                self.storage.as_deref(),
                &local.log,
                upload.entry.position,
            )
            .await?;
            self.apply(local, upload.entry, &publications, member, ring, damages)
                .await?;
        }
        Ok(())
    }

    async fn apply(
        &self,
        local: &mut LocalStoreLog,
        entry: StoreLogEntry,
        publications: &BTreeMap<EntryId, std::time::SystemTime>,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<(), SyncError> {
        let (entry, replay) = crate::replay_entry(&local.log, entry, publications);
        let mut operations = self
            .removal_work(&local.log, &entry, &replay, member)
            .await?;
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
        // A device advances shared write headers while it cannot read a circle.
        // Restored membership must reload those skipped parts before continuing
        // from that frontier. Record the reload with the membership replay.
        for (circle, current) in &after.circles {
            if !after.circle_deletions.contains_key(circle)
                && current.members.contains(&member.member_id())
                && before.circles.get(circle).is_some_and(|previous| {
                    !before.circle_deletions.contains_key(circle)
                        && !previous.members.contains(&member.member_id())
                })
            {
                changed.insert(Audience::Circle(*circle));
            }
        }
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
        for record in self.database.operations().await? {
            let data = crate::operation_data::Data::read(&record)?;
            if self.operation_can_retry(&record).await?
                && record.last_step < data.entry_step_number(4)
                && data
                    .entry()?
                    .is_some_and(|fixed| fixed.position == entry.entry.position)
            {
                updates.push(data.update(&record, data.entry_step_number(4))?);
            }
        }
        self.database
            .apply_store_log_operations::<crate::PendingRecord>(
                entry.clone(),
                replay.clone(),
                operations,
                updates,
            )
            .await?;
        local.log.entries.push(entry);
        local.log.entries.sort_by_key(|e| e.entry.timestamp);
        local.log.replay = replay;
        self.check_stopped(local, member)?;
        self.update_keys(&local.log, member, ring, damages).await
    }

    async fn update_keys(
        &self,
        log: &StoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<(), SyncError> {
        for (audience, key) in keys::needed(log) {
            self.acquire(&audience, key, member, ring, damages).await?;
        }
        self.share_keys(log, ring, damages).await
    }

    async fn share_keys(
        &self,
        log: &StoreLog,
        ring: &Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<(), SyncError> {
        // Reserved copies precede all new sharing decisions. An observed duplicate
        // or removed recipient must never erase a writer's already reserved number.
        object::upload_keys(self.directory.id(), self.storage.as_deref(), &self.database).await?;
        let needs = keys::needed(log);
        if needs.is_empty() {
            return Ok(());
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let copies = keys::read_copies(storage, self.directory.id(), damages).await?;
        for (audience, key) in needs {
            if !keys::holds(ring.as_ref(), &audience, key) {
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
                let member = member.clone();
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
                object::upload_keys(self.directory.id(), Some(storage), &self.database).await?;
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

    async fn acquire(
        &self,
        audience: &Audience,
        key: KeyId,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<bool, SyncError> {
        if keys::holds(ring.as_ref(), audience, key) {
            return Ok(true);
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        for copy in keys::read_copies(storage, self.directory.id(), damages).await? {
            if copy.prefix.audience != *audience
                || copy.prefix.key != key
                || copy.prefix.member != member.member_id()
            {
                continue;
            }
            let path = copy.path;
            if damages.iter().any(|damage| damage.path == path.as_str()) {
                continue;
            }
            let envelope = SealedKey::decode(&copy.bytes)?;
            let opened = match audience {
                Audience::Store => envelope
                    .open_store_key(member, path.as_str())
                    .map(|opened| {
                        if opened.id() != key {
                            return Err(Refusal::WrongIdentity { cause: None });
                        }
                        match ring {
                            Some(ring) => {
                                ring.insert_store_key(opened)
                                    .map_err(|e| Refusal::WrongIdentity {
                                        cause: Some(Arc::new(e)),
                                    })
                            }
                            None => {
                                *ring = Some(StoreKeyring::new(opened));
                                Ok(())
                            }
                        }
                    }),
                Audience::Circle(circle) => {
                    envelope
                        .open_circle_key(member, path.as_str())
                        .map(|opened| {
                            if opened.id() != key || opened.circle() != *circle {
                                return Err(Refusal::WrongIdentity { cause: None });
                            }
                            // A circle introduction follows an opened store-log entry.
                            ring.as_mut()
                                .expect("circle key follows an opened store key")
                                .insert_circle_key(opened)
                                .map_err(|e| Refusal::WrongIdentity {
                                    cause: Some(Arc::new(e)),
                                })
                        })
                }
            };
            match opened {
                Err(error @ coven_crypto::CryptoError::KeyIdentity) => {
                    Self::damage(
                        damages,
                        &path,
                        Refusal::WrongIdentity {
                            cause: Some(Arc::new(error)),
                        },
                    )?;
                    continue;
                }
                Err(error) => {
                    Self::damage(
                        damages,
                        &path,
                        Refusal::Decryption {
                            cause: Some(Arc::new(error)),
                        },
                    )?;
                    continue;
                }
                Ok(Err(failure)) => {
                    Self::damage(damages, &path, failure)?;
                    continue;
                }
                Ok(Ok(())) => {
                    self.store_keys
                        .persist(ring.as_ref().expect("opened keyring"))?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub(crate) fn damage(
        damages: &mut Vec<DamagedObject>,
        path: &ObjectPath,
        failure: Refusal,
    ) -> Result<(), SyncError> {
        if let Refusal::Parse { cause: Some(error) } = &failure {
            if crate::error::newer_format(error.as_ref()) {
                return Err(SyncFailure::UpdateRequired(
                    crate::error::required_format(error.as_ref()).expect("newer format"),
                )
                .into());
            }
        }
        tracing::warn!(path = path.as_str(), error = %failure, "damaged object; will reread on sync");
        if !damages.iter().any(|object| object.path == path.as_str()) {
            damages.push(DamagedObject {
                path: path.as_str().to_owned(),
                failure,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "store_log_sync_tests.rs"]
pub(crate) mod tests;

#[path = "operation_calls.rs"]
mod operation_calls;
#[path = "operation_invites.rs"]
mod operation_invites;
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
