//! The owner of the store-log storage step; no background tasks or sync loop.

use crate::{
    store_log_keys as keys, store_log_object as object, DamagedObject, Refusal, SyncError,
};
use coven_crypto::{custody::KeySession, MemberKeys, StoreKeyring};
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
    storage: Option<Arc<dyn Storage>>,
    database: Database,
    store_keys: Arc<KeySession<StoreKeyring>>,
    member_keys: Arc<KeySession<MemberKeys>>,
    clock: ClockRef,
    ids: IdSourceRef,
    directory: coven_foundation::files::StoreDir,
    /// The storage account's admin access, held only by the operation that
    /// needs it and never written anywhere (§4).
    admin_access:
        std::collections::BTreeMap<crate::OperationId, Arc<dyn coven_storage::AccessKeyAdmin>>,
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
            download_limit: std::num::NonZeroUsize::MIN,
            finality: Arc::default(),
            reads: crate::pass_reads::PassReads::default(),
            storage: Some(storage),
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
            admin_access: Default::default(),
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
            download_limit: std::num::NonZeroUsize::MIN,
            finality: Arc::default(),
            reads: crate::pass_reads::PassReads::default(),
            storage: None,
            database,
            store_keys,
            member_keys,
            clock,
            ids,
            directory,
            admin_access: Default::default(),
        }
    }

    /// Bound simultaneous membership and key-copy requests in subsequent scans.
    pub fn set_discovery_download_limit(&mut self, limit: std::num::NonZeroUsize) {
        self.download_limit = limit;
    }

    /// Share metadata and checked references with device-log downloads until
    /// this pass ends. Standalone calls retain their own scoped reads.
    pub(crate) fn begin_pass(
        &self,
        writes: &mut crate::DeviceLogSync,
    ) -> crate::pass_reads::ReadScope {
        writes.share_reads(self.reads.clone());
        writes.share_observation(self.finality.clone());
        writes.set_download_limit(self.download_limit);
        self.reads.enter()
    }

    /// Publish entries, replay downloads, acquire keys and resume snapshot work.
    /// A missing dependency or sealed copy waits solely in storage for a later call.
    /// Returns skipped damage so connecting storage can reject incomplete key setup.
    pub async fn sync_store_log(&mut self) -> Result<Vec<DamagedObject>, SyncError> {
        let _reads = self.reads.enter();
        let mut damages = self.step().await?;
        self.schedule_version_changes().await?;
        damages.extend(self.resume_snapshots().await?);
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
            &self.database,
            &self.reads,
            self.download_limit,
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

    async fn stuck_entry(
        &self,
        entry: EntryId,
        path: &ObjectPath,
        failure: Refusal,
    ) -> Result<(), SyncError> {
        if let Refusal::Parse { cause: Some(error) } = &failure {
            if crate::error::newer_format(error.as_ref()) {
                return Err(SyncError::UpdateRequired(
                    crate::error::required_format(error.as_ref()).expect("newer format"),
                ));
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
            return Err(SyncError::Removed);
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
        if self.can_publish(local).await? {
            self.publish_queued(local, member, ring, damages).await?;
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
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
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
            self.apply(local, entry, &publications, member, ring, damages)
                .await?;
        }
        Ok(())
    }

    async fn upload_queued(
        &self,
        local: &LocalStoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
    ) -> Result<Option<StoreLogEntry>, SyncError> {
        if let Some(upload) = &local.upload {
            if let StoreChange::CreateStore { key_hash, .. } = &upload.entry.change {
                if upload.sealing.keys.is_empty()
                    && !keys::holds(ring.as_ref(), &Audience::Store, upload.sealing.key)
                {
                    let key = keys::read_store_key(
                        &self.database,
                        &self.reads,
                        self.download_limit,
                        self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                        self.directory.id(),
                        upload.sealing.key,
                        Some(key_hash),
                        member,
                    )
                    .await?;
                    match ring {
                        Some(ring) => ring.insert_store_key(key)?,
                        None => *ring = Some(StoreKeyring::new(key)),
                    }
                    self.store_keys
                        .persist(ring.as_ref().expect("recovered creation key"))?;
                }
            }
            let bytes = object::seal_upload(self.directory.id(), upload, ring.as_ref(), member)?;
            object::upload(
                self.directory.id(),
                self.storage.as_deref(),
                &self.database,
                upload,
                &bytes,
            )
            .await?;
            return Ok(Some(upload.entry.clone()));
        }
        Ok(None)
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
        self.apply_batch(local, vec![entry], replay, member).await?;
        self.update_keys(&local.log, member, ring, damages).await
    }

    async fn apply_batch(
        &self,
        local: &mut LocalStoreLog,
        entries: Vec<coven_database::ReplayEntry>,
        replay: coven_database::StoreLogReplay,
        member: &MemberKeys,
    ) -> Result<(), SyncError> {
        let mut operations = self
            .removal_work(&local.log, &entries, &replay, member)
            .await?;
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
        // A device advances shared write headers while it cannot read a circle.
        // Restored membership must reload those skipped parts before continuing
        // from that frontier. Record the reload with the membership replay.
        for (circle, current) in &after.circles {
            if !after.circle_deletions.contains_key(circle)
                && current.members.contains(&member.member_id())
                && before.circles.get(circle).is_some_and(|previous| {
                    before.circle_deletions.contains_key(circle)
                        || !previous.members.contains(&member.member_id())
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
            )
            .await?;
        *local = self.database.local_store_log().await?;
        self.check_stopped(local, member)
    }

    async fn update_keys(
        &self,
        log: &StoreLog,
        member: &MemberKeys,
        ring: &mut Option<StoreKeyring>,
        damages: &mut Vec<DamagedObject>,
    ) -> Result<(), SyncError> {
        self.acquire_keys(log, member, ring, damages).await?;
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
        if let Some(writer) =
            object::upload_keys(self.directory.id(), self.storage.as_deref(), &self.database)
                .await?
        {
            self.reads
                .forget_misses(coven_database::LogKind::KeyCopy, writer);
        }
        let needs = crate::key_introductions::needed(log);
        if needs.is_empty() {
            return Ok(());
        }
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let copies = keys::read_copies(
            &self.database,
            &self.reads,
            self.download_limit,
            storage,
            self.directory.id(),
            damages,
        )
        .await?;
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

    pub(crate) fn damage(
        damages: &mut Vec<DamagedObject>,
        path: &ObjectPath,
        failure: Refusal,
    ) -> Result<(), SyncError> {
        if let Refusal::Parse { cause: Some(error) } = &failure {
            if crate::error::newer_format(error.as_ref()) {
                return Err(SyncError::UpdateRequired(
                    crate::error::required_format(error.as_ref()).expect("newer format"),
                ));
            }
        }
        tracing::warn!(path = path.as_str(), error = %failure, "retained object failed validation");
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

#[path = "key_acquisition.rs"]
mod key_acquisition;
