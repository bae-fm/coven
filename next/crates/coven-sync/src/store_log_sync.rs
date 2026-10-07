//! The owner of the store-log storage step; no background tasks or sync loop.

use crate::{
    store_log_keys as keys, store_log_object as object, DamagedObject, DroppedEntry,
    ObjectCheckFailure, SyncError, SyncFailure, SyncReport,
};
use coven_crypto::{
    custody::{MemberKeyCustody, StoreKeyCustody},
    seal_circle_key, seal_store_key, CryptoError, MemberKeys, SealedKey, StoreKeyring,
};
use coven_database::{Database, EntryOutcome, LocalStoreLog, StoreLog};
use coven_format::{
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
use std::time::UNIX_EPOCH;

/// Owns store-log, key and snapshot synchronization capabilities (§21.2).
/// Calls require exclusive access so one install never authors or applies two
/// entries concurrently through this owner. The database also checks stale replay.
pub struct StoreLogSync {
    storage: Option<Arc<dyn Storage>>,
    database: Database,
    store_keys: Arc<dyn StoreKeyCustody>,
    member_keys: Arc<dyn MemberKeyCustody>,
    clock: ClockRef,
    ids: IdSourceRef,
    directory: coven_foundation::files::StoreDir,
}

impl StoreLogSync {
    /// Compose the step from retained capabilities; none are handed to callers.
    pub fn new(
        storage: Arc<dyn Storage>,
        database: Database,
        store_keys: Arc<dyn StoreKeyCustody>,
        member_keys: Arc<dyn MemberKeyCustody>,
        clock: ClockRef,
        ids: IdSourceRef,
        directory: coven_foundation::files::StoreDir,
    ) -> Self {
        Self {
            storage: Some(storage),
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
        }
    }

    /// Compose a store that can journal operations before storage connects.
    pub fn disconnected(
        database: Database,
        store_keys: Arc<dyn StoreKeyCustody>,
        member_keys: Arc<dyn MemberKeyCustody>,
        clock: ClockRef,
        ids: IdSourceRef,
        directory: coven_foundation::files::StoreDir,
    ) -> Self {
        Self {
            storage: None,
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
        }
    }

    /// Publish entries, replay downloads, acquire keys and resume snapshot work.
    /// A missing dependency or sealed copy waits solely in storage for a later call.
    pub async fn sync_store_log(&mut self) -> Result<SyncReport, SyncFailure> {
        let mut report = self.step().await.map_err(SyncFailure::from)?;
        self.schedule_version_changes()
            .await
            .map_err(SyncFailure::from)?;
        let snapshots = self.resume_snapshots().await.map_err(SyncFailure::from)?;
        report.damaged_objects.extend(snapshots.damaged_objects);
        let operations = self.operation_report().await.map_err(SyncFailure::from)?;
        report.blocked_operations = operations.blocked_operations;
        report.access_keys_to_delete = operations.access_keys_to_delete;
        Ok(report)
    }

    /// Fix and publish a new entry. Existing queued bytes go first. The change's
    /// key ids are fresh names; this owner generates and seals their material.
    /// After a failed upload, call `sync_store_log` to retry that same entry.
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
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
        let mut ring = self.store_keys.unlock()?;
        let mut report = SyncReport::default();
        self.check_stopped(&local, &member)?;
        self.update_keys(&local.log, &member, &mut ring, &mut report)
            .await?;
        self.publish(&mut local, &member, &mut ring, &mut report)
            .await?;
        if let Some(damaged) = report.damaged_objects.into_iter().next() {
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
        let author = member.clone();
        let id = self
            .database
            .prepare_store_log(member.member_id(), change, move |log, entry| {
                keys::seal(log, entry, ring.as_ref(), &author)
            })
            .await?;
        // Re-read the committed bytes, so the first attempt uses the same path as
        // every retry and never depends on a callback's transient return value.
        local = self.database.local_store_log().await?;
        let mut ring = self.store_keys.unlock()?;
        let mut report = SyncReport::default();
        self.publish(&mut local, &member, &mut ring, &mut report)
            .await?;
        if let Some(damaged) = report.damaged_objects.into_iter().next() {
            return Err(damaged.into());
        }
        Ok(id)
    }

    async fn step(&self) -> Result<SyncReport, SyncError> {
        let mut local = self.database.local_store_log().await?;
        let member = self
            .member_keys
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
        let mut ring = self.store_keys.unlock()?;
        let mut report = SyncReport::default();
        self.check_stopped(&local, &member)?;
        self.update_keys(&local.log, &member, &mut ring, &mut report)
            .await?;
        self.publish(&mut local, &member, &mut ring, &mut report)
            .await?;
        let paths = self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .list(&ObjectPrefix::store_logs())
            .await?;
        let mut entries = BTreeMap::new();
        for stored in paths {
            let path = stored.path;
            let (device, number) =
                path.store_log_position()
                    .ok_or(coven_storage::StorageError::Protocol(
                        "store-log listing returned another layout",
                    ))?;
            entries.insert(
                EntryId {
                    device,
                    number: number.get(),
                },
                path,
            );
        }
        let mut blocked = BTreeSet::new();
        let mut cache = BTreeMap::new();
        let mut origins = Vec::new();
        for (id, path) in entries.iter().filter(|(id, _)| id.number == 1) {
            let Some(bytes) = self.read(path).await? else {
                continue;
            };
            match SingleChunkObject::decode(&bytes) {
                Ok(object) => match object::check_origin(&object, path) {
                    Ok(()) => {
                        if let SingleChunkObject::StoreLog {
                            origin: Some(origin),
                            ..
                        } = object
                        {
                            origins.push(origin);
                        }
                    }
                    Err(failure) => {
                        Self::damage(&mut report, path, failure)?;
                        blocked.insert(id.device);
                    }
                },
                Err(error) => {
                    Self::damage(
                        &mut report,
                        path,
                        ObjectCheckFailure::Parse(Arc::new(error)),
                    )?;
                    blocked.insert(id.device);
                }
            }
            cache.insert(*id, bytes);
        }
        let own = local
            .log
            .entries
            .iter()
            .find_map(|e| object::origin(&e.entry))
            .or_else(|| origins.iter().find(|o| o.store == local.store).cloned());
        if let Some(own) = own {
            for origin in origins {
                if origin.store != local.store {
                    if origin.timestamp < own.timestamp {
                        return Err(SyncFailure::LocationTaken.into());
                    }
                    blocked.insert(origin.timestamp.device());
                }
            }
        }
        let now = match self.clock.now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis(),
            Err(_) => 0,
        };
        loop {
            let mut advanced = false;
            for (id, path) in &entries {
                if blocked.contains(&id.device) || local.log.replay.entries.contains_key(id) {
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
                    let Some(bytes) = self.read(path).await? else {
                        blocked.insert(id.device);
                        continue;
                    };
                    cache.insert(*id, bytes);
                }
                let envelope = match SingleChunkObject::decode(&cache[id]) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        Self::damage(
                            &mut report,
                            path,
                            ObjectCheckFailure::Parse(Arc::new(error)),
                        )?;
                        blocked.insert(id.device);
                        continue;
                    }
                };
                if let Err(failure) = object::check_origin(&envelope, path) {
                    Self::damage(&mut report, path, failure)?;
                    blocked.insert(id.device);
                    continue;
                }
                let SingleChunkObject::StoreLog { key, origin, .. } = &envelope else {
                    unreachable!("checked store-log envelope")
                };
                if origin
                    .as_ref()
                    .is_some_and(|origin| origin.store != local.store)
                {
                    blocked.insert(id.device);
                    continue;
                }
                if !self
                    .acquire(&Audience::Store, *key, &member, &mut ring, &mut report)
                    .await?
                {
                    continue;
                }
                let entry = match object::open(
                    &envelope,
                    path,
                    ring.as_ref().expect("acquired keyring").store_key(*key)?,
                ) {
                    Ok(entry) => entry,
                    Err(failure) => {
                        Self::damage(&mut report, path, failure)?;
                        blocked.insert(id.device);
                        continue;
                    }
                };
                if u128::from(entry.timestamp.milliseconds()) > now + 300_000 {
                    continue;
                }
                match object::ready(&local.log, &entry, local.store) {
                    Ok(true) => {
                        self.apply(&mut local, entry, &member, &mut ring, &mut report)
                            .await?
                    }
                    Ok(false) => continue,
                    Err(failure) => {
                        Self::damage(&mut report, path, failure)?;
                        blocked.insert(id.device);
                        continue;
                    }
                }
                advanced = true;
            }
            if !advanced {
                break;
            }
        }
        report.dropped_entries = local
            .log
            .entries
            .iter()
            .filter(|e| e.entry.author == member.member_id())
            .filter_map(|e| match &local.log.replay.entries[&e.entry.position] {
                EntryOutcome::Kept => None,
                EntryOutcome::Dropped(reason) => Some(DroppedEntry {
                    entry: e.entry.position,
                    change: (&e.entry.change).into(),
                    reason: reason.clone(),
                }),
            })
            .collect();
        Ok(report)
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
        if state
            .format
            .values()
            .any(|version| version.number > coven_format::FORMAT_VERSION)
        {
            return Err(SyncFailure::UpdateRequired.into());
        }
        Ok(())
    }

    async fn publish(
        &self,
        local: &mut LocalStoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        if let Some(upload) = &local.upload {
            // Recovery can retain a fixed entry before restoring its past.
            // Download that past before publishing or replaying the entry.
            if !object::ready(&local.log, &upload.entry, local.store).map_err(|failure| {
                SyncError::Damaged(DamagedObject {
                    path: object::path(upload.entry.position).into(),
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
                if record.failure.is_some()
                    && crate::operation_data::Data::read(&record)?
                        .entry()?
                        .is_some_and(|entry| entry.position == upload.entry.position)
                {
                    return Ok(());
                }
            }
        }
        self.publish_queued(local, member, ring, report).await
    }

    async fn publish_queued(
        &self,
        local: &mut LocalStoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        if let Some(upload) = local.upload.take() {
            for key in upload.sealed.keys {
                self.storage
                    .as_deref()
                    .ok_or(SyncError::NoStorage)?
                    .create_once(
                        &ObjectPath::parse(&key.path).map_err(coven_storage::StorageError::from)?,
                        &key.bytes,
                    )
                    .await?;
            }
            self.storage
                .as_deref()
                .ok_or(SyncError::NoStorage)?
                .create_once(&object::path(upload.entry.position), &upload.sealed.bytes)
                .await?;
            self.apply(local, upload.entry, member, ring, report)
                .await?;
        }
        Ok(())
    }

    async fn apply(
        &self,
        local: &mut LocalStoreLog,
        entry: StoreLogEntry,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        let (entry, replay) = crate::replay_entry(&local.log, entry);
        let mut operations = self
            .removal_work(&local.log, &entry, &replay, member)
            .await?;
        let before = &local.log.replay.state;
        let after = &replay.state;
        let changed: BTreeSet<_> = before
            .schema
            .keys()
            .chain(after.schema.keys())
            .chain(before.format.keys())
            .chain(after.format.keys())
            .chain(before.resets.keys())
            .chain(after.resets.keys())
            .filter(|audience| {
                before.schema.get(*audience) != after.schema.get(*audience)
                    || before.format.get(*audience) != after.format.get(*audience)
                    || before.resets.get(*audience) != after.resets.get(*audience)
            })
            .cloned()
            .collect();
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
            if record.failure.is_none()
                && record.last_step < data.entry_step_number(4)
                && data
                    .entry()?
                    .is_some_and(|fixed| fixed.position == entry.entry.position)
            {
                updates.push(data.update(&record, data.entry_step_number(4))?);
            }
        }
        self.database
            .apply_store_log_operations(entry.clone(), replay.clone(), operations, updates)
            .await?;
        local.log.entries.push(entry);
        local.log.entries.sort_by_key(|e| e.entry.timestamp);
        local.log.replay = replay;
        self.check_stopped(local, member)?;
        self.update_keys(&local.log, member, ring, report).await
    }

    async fn update_keys(
        &self,
        log: &StoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        for (audience, key) in keys::needed(log) {
            self.acquire(&audience, key, member, ring, report).await?;
        }
        self.share_dropped_keys(log, ring).await
    }

    async fn share_dropped_keys(
        &self,
        log: &StoreLog,
        ring: &Option<StoreKeyring>,
    ) -> Result<(), SyncError> {
        for (audience, key) in keys::dropped_removal_keys(log) {
            if !keys::holds(ring, &audience, key) {
                tracing::debug!(?audience, ?key, "dropped removal's key has not arrived");
                continue;
            }
            let ring = ring.as_ref().expect("held key has a keyring");
            for (member, recipient) in keys::recipients(log, &audience) {
                let path = keys::path(&audience, key, member);
                if self.read(&path).await?.is_none() {
                    let recipient = *recipient;
                    let sealed_path = path.clone();
                    let bytes = match audience {
                        Audience::Store => {
                            let key = ring.store_key(key)?.clone();
                            self.database
                                .prepare_key_upload(path.as_str().into(), move || {
                                    Ok::<_, SyncError>(seal_store_key(
                                        &key,
                                        &recipient,
                                        sealed_path.as_str(),
                                    )?)
                                })
                                .await?
                        }
                        Audience::Circle(circle) => {
                            let key = ring.circle_key(circle, key)?.clone();
                            self.database
                                .prepare_key_upload(path.as_str().into(), move || {
                                    Ok::<_, SyncError>(seal_circle_key(
                                        &key,
                                        &recipient,
                                        sealed_path.as_str(),
                                    )?)
                                })
                                .await?
                        }
                    };
                    // This path may have another writer: any first copy of the
                    // same key counts, even when its random sealed bytes differ.
                    self.storage
                        .as_deref()
                        .ok_or(SyncError::NoStorage)?
                        .create_once(&path, &bytes)
                        .await?;
                }
                self.database.complete_key_upload(path.into()).await?;
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
        report: &mut SyncReport,
    ) -> Result<bool, SyncError> {
        if keys::holds(ring, audience, key) {
            return Ok(true);
        }
        let path = keys::path(audience, key, &member.member_id());
        if report
            .damaged_objects
            .iter()
            .any(|damage| damage.path == path.as_str())
        {
            return Ok(false);
        }
        let Some(bytes) = self.read(&path).await? else {
            return Ok(false);
        };
        if let Err(error) = SealedKey::decode(&bytes) {
            Self::damage(report, &path, ObjectCheckFailure::Parse(Arc::new(error)))?;
            return Ok(false);
        }
        let opened = match audience {
            Audience::Store => member.open_store_key(path.as_str(), &bytes).map(|opened| {
                if opened.id() != key {
                    return Err(object::invalid("sealed key identity disagrees with path"));
                }
                match ring {
                    Some(ring) => ring
                        .insert_store_key(opened)
                        .map_err(|e| ObjectCheckFailure::Parse(Arc::new(e))),
                    None => {
                        *ring = Some(StoreKeyring::new(opened));
                        Ok(())
                    }
                }
            }),
            Audience::Circle(circle) => {
                member.open_circle_key(path.as_str(), &bytes).map(|opened| {
                    if opened.id() != key || opened.circle() != *circle {
                        return Err(object::invalid(
                            "sealed circle key identity disagrees with path",
                        ));
                    }
                    // A circle introduction follows an opened store-log entry.
                    ring.as_mut()
                        .expect("circle key follows an opened store key")
                        .insert_circle_key(opened)
                        .map_err(|e| ObjectCheckFailure::Parse(Arc::new(e)))
                })
            }
        };
        match opened {
            Err(error) => {
                Self::damage(report, &path, ObjectCheckFailure::Decryption(error))?;
                Ok(false)
            }
            Ok(Err(failure)) => {
                Self::damage(report, &path, failure)?;
                Ok(false)
            }
            Ok(Ok(())) => {
                self.store_keys
                    .persist(ring.as_ref().expect("opened keyring"))?;
                Ok(true)
            }
        }
    }

    fn damage(
        report: &mut SyncReport,
        path: &ObjectPath,
        failure: ObjectCheckFailure,
    ) -> Result<(), SyncError> {
        if let ObjectCheckFailure::Parse(error) = &failure {
            if matches!(
                error.downcast_ref::<coven_format::Error>(),
                Some(coven_format::Error::UnsupportedVersion(version)) if *version > coven_format::FORMAT_VERSION
            ) || matches!(
                error.downcast_ref::<CryptoError>(),
                Some(CryptoError::UnsupportedVersion(version)) if *version > coven_format::FORMAT_VERSION
            ) {
                return Err(SyncFailure::UpdateRequired.into());
            }
        }
        if !report
            .damaged_objects
            .iter()
            .any(|object| object.path == path.as_str())
        {
            report.damaged_objects.push(DamagedObject {
                path: path.as_str().to_owned(),
                failure,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "store_log_sync_tests.rs"]
mod tests;

#[path = "operation_calls.rs"]
mod operation_calls;
#[path = "operation_invites.rs"]
mod operation_invites;
#[path = "operation_steps.rs"]
mod operation_steps;

#[path = "schema_sync.rs"]
mod schema_sync;
#[path = "snapshots.rs"]
mod snapshots;

#[path = "bootstrap.rs"]
mod bootstrap;

pub use bootstrap::JoinOutcome;
