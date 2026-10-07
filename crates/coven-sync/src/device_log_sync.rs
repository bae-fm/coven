//! One upload, download or position-posting step; scheduling belongs to the caller.

use crate::{DeviceActivity, SyncError, SyncFailure, SyncResults, WaitingWrite};
use coven_crypto::custody::{MemberKeyCustody, StoreKeyCustody};
use coven_database::{ApplyOutcome, Database, DbError, WriteWait};
use coven_format::{
    objects::{Fingerprint, PostedPositions},
    sealed_single::SingleChunkPrefix,
    Object,
};
use coven_merge::{Audience, WriteId};
use coven_storage::{ObjectPath, ObjectPrefix, Storage};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[path = "write_download.rs"]
mod download;
#[cfg(test)]
#[path = "device_log_sync_tests.rs"]
mod tests;
#[path = "write_upload.rs"]
mod upload;

/// Device-log transfer and position publication using the capabilities chosen
/// by the opening composition root. Calls on one owner are serialized by `&mut
/// self`; each step completes its database work before returning.
pub struct DeviceLogSync {
    reads: crate::pass_reads::PassReads,
    storage: Option<Arc<dyn Storage>>,
    database: Database,
    store_keys: Arc<dyn StoreKeyCustody>,
    member_keys: Arc<dyn MemberKeyCustody>,
}

impl DeviceLogSync {
    /// Retain the provider, database and custody selected for this store.
    pub fn new(
        storage: Arc<dyn Storage>,
        database: Database,
        store_keys: Arc<dyn StoreKeyCustody>,
        member_keys: Arc<dyn MemberKeyCustody>,
    ) -> Self {
        Self {
            reads: crate::pass_reads::PassReads::default(),
            storage: Some(storage),
            database,
            store_keys,
            member_keys,
        }
    }

    /// Compose device-log work before a provider has connected.
    pub fn disconnected(
        database: Database,
        store_keys: Arc<dyn StoreKeyCustody>,
        member_keys: Arc<dyn MemberKeyCustody>,
    ) -> Self {
        Self {
            reads: crate::pass_reads::PassReads::default(),
            storage: None,
            database,
            store_keys,
            member_keys,
        }
    }

    pub(crate) fn set_storage(&mut self, storage: Option<Arc<dyn Storage>>) {
        self.storage = storage;
    }

    pub(crate) fn share_reads(&mut self, reads: crate::pass_reads::PassReads) {
        self.reads = reads;
    }

    /// Publish queued writes in number order, fixing the sealing keys before the first
    /// request and retaining provider sessions across interruptions. Returns the
    /// positions confirmed stored. A newer store schema stops this step; a local
    /// breaking migration waits for its store-log raise without recording an attempt.
    pub async fn upload_writes(&mut self) -> Result<Vec<WriteId>, SyncError> {
        let mut uploaded = Vec::new();
        loop {
            let local = self.database.local_store_log().await?;
            let state = self.database.sync_state(Vec::new()).await?;
            let member = self
                .member_keys
                .unlock()?
                .ok_or(SyncError::MissingMemberKeys)?;
            crate::write_seal::check_member(&local.log, &member, local.device)?;
            crate::write_seal::check_upload_version(&local.log, state.schema_version)?;
            if state.breaking_version
                > local
                    .log
                    .replay
                    .state
                    .schema
                    .get(&Audience::Store)
                    .map_or(0, |v| v.number)
            {
                break;
            }
            let store = local
                .log
                .replay
                .state
                .store
                .as_ref()
                .expect("checked store membership");
            let ring = self
                .store_keys
                .unlock()?
                .ok_or(SyncError::KeyUnavailable(store.key))?;
            let selected_ring = ring.clone();
            let signer = member.clone();
            let next = self
                .database
                .prepare_write_upload(move |log, schema, header| {
                    crate::write_seal::select(log, schema, header, &selected_ring, &signer)
                        .map_err(|e| DbError::SyncStream(Box::new(e)))
                })
                .await
                .map_err(|error| match error {
                    DbError::SyncStream(source) => match source.downcast::<SyncError>() {
                        Ok(error) => *error,
                        Err(source) => DbError::SyncStream(source).into(),
                    },
                    error => error.into(),
                })?;
            let Some(write) = next else {
                break;
            };
            self.send_write(write, ring, member).await?;
            self.database.upload_succeeded(write).await?;
            uploaded.push(write);
        }
        Ok(uploaded)
    }

    /// List device logs and apply ready writes until no further listed write can
    /// advance. Damage blocks that device's successors, while independent devices
    /// continue. A later call rereads damaged objects and retains waiting start times.
    pub async fn download_writes(&mut self) -> Result<SyncResults, SyncError> {
        let _reads = self.reads.enter();
        let local = self.database.local_store_log().await?;
        let state = self.database.sync_state(Vec::new()).await?;
        let member = self
            .member_keys
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
        crate::write_seal::check_member(&local.log, &member, local.device)?;
        let key = local
            .log
            .replay
            .state
            .store
            .as_ref()
            .expect("checked membership")
            .key;
        let ring = self
            .store_keys
            .unlock()?
            .ok_or(SyncError::KeyUnavailable(key))?;
        let mut objects = BTreeMap::new();
        let mut devices: BTreeSet<_> = local.log.replay.state.devices.keys().copied().collect();
        for object in self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .list(&ObjectPrefix::device_logs())
            .await?
        {
            let (device, number) = object.path.write_position().expect("device-log prefix");
            devices.insert(device);
            let write = WriteId {
                device,
                number: number.get(),
            };
            if !state.positions.covers(write) {
                objects.insert(write, object);
            }
        }
        let mut report = SyncResults::default();
        let mut damaged = BTreeSet::new();
        let mut waiting = BTreeMap::new();
        let mut positions = state.positions;
        let mut replays = crate::replay_cache::ReplayCache::new(&local.log);
        loop {
            let mut progressed = false;
            for (write, object) in &objects {
                if positions.covers(*write) {
                    continue;
                }
                let previous = WriteId {
                    number: write.number - 1,
                    ..*write
                };
                if write.number > 1 && !positions.covers(previous) {
                    waiting.insert(*write, vec![previous]);
                    continue;
                }
                if damaged.contains(&write.device) {
                    continue;
                }
                match self
                    .receive_write(object, &ring, &local.log, &mut replays, &member.member_id())
                    .await
                {
                    Ok(ApplyOutcome::Applied | ApplyOutcome::AlreadyApplied) => {
                        match positions.0.iter_mut().find(|p| p.device == write.device) {
                            Some(position) => *position = *write,
                            None => {
                                positions.0.push(*write);
                                positions.0.sort_by_key(|p| p.device);
                            }
                        }
                        waiting.remove(write);
                        progressed = true;
                    }
                    Ok(ApplyOutcome::Waiting(reason)) => {
                        waiting.insert(
                            *write,
                            match reason {
                                WriteWait::Writes(writes) => writes,
                                _ => Vec::new(),
                            },
                        );
                    }
                    Err(SyncError::Damaged(object)) => {
                        damaged.insert(write.device);
                        report.damaged_objects.push(object);
                    }
                    Err(SyncError::KeyUnavailable(_)) => {
                        waiting.insert(*write, Vec::new());
                    }
                    Err(error) => return Err(error),
                }
            }
            if !progressed {
                break;
            }
        }
        let since = self
            .database
            .waiting_writes(waiting.keys().copied().collect())
            .await?;
        report.waiting = since
            .into_iter()
            .map(|(write, since)| WaitingWrite {
                write,
                waiting_for: waiting.remove(&write).expect("recorded waiting write"),
                since,
            })
            .collect();
        report.devices = devices
            .into_iter()
            .filter(|id| *id != local.device)
            .map(|device| DeviceActivity {
                device,
                applied_through: positions
                    .0
                    .iter()
                    .find(|p| p.device == device)
                    .map_or(0, |p| p.number),
            })
            .collect();
        let comparison = self.compare_fingerprints().await?;
        report.disagreements = comparison.disagreements;
        report.damaged_objects.extend(comparison.damaged_objects);
        Ok(report)
    }

    /// Replace this device's authenticated D8 object from one committed database
    /// state. Returns false while any local write awaits upload: clipping only
    /// the position would publish fingerprints that still include those writes.
    pub async fn post_positions(&mut self) -> Result<bool, SyncError> {
        let member = self
            .member_keys
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
        let Some(positions) = self.positions_for(&member).await? else {
            return Ok(false);
        };
        let key = positions
            .fingerprints
            .iter()
            .find(|f| f.audience == Audience::Store)
            .expect("store fingerprint")
            .key;
        let ring = self
            .store_keys
            .unlock()?
            .ok_or(SyncError::KeyUnavailable(key))?;
        let path = ObjectPath::positions(positions.device);
        let prefix = SingleChunkPrefix::PostedPositions(key);
        let sealed = ring.store_key(key)?.derive().seal_object_chunk(
            path.as_str(),
            &prefix.encode()?,
            0,
            0,
            &Object::PostedPositions(positions).encode()?,
        )?;
        let mut bytes = prefix.encode_chunk(&sealed)?;
        let mut hash = coven_crypto::ObjectHasher::new();
        hash.update(&bytes);
        bytes.extend_from_slice(member.sign_object(path.as_str(), &hash.finish()).as_bytes());
        self.storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .replace(&path, &bytes)
            .await?;
        Ok(true)
    }

    /// Compare this committed state with every other device's posted state.
    /// Only equal write positions, store-log positions, schema versions and
    /// fingerprint keys are comparable. Damage counts as no post (§19.1).
    /// No database state is changed and no recovery operation is started.
    pub async fn compare_fingerprints(&mut self) -> Result<SyncResults, SyncError> {
        let mut report = SyncResults::default();
        let Some(own) = self.current_positions().await? else {
            return Ok(report);
        };
        let ring = self.store_keys.unlock()?;
        let log = self.database.local_store_log().await?.log;
        for object in self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .list(&ObjectPrefix::positions())
            .await?
        {
            if object.path.device() == Some(own.device) {
                continue;
            }
            let bytes = match self
                .storage
                .as_deref()
                .ok_or(SyncError::NoStorage)?
                .read(&object.path)
                .await
            {
                Ok(bytes) => bytes,
                Err(coven_storage::StorageError::NotFound) => {
                    tracing::debug!(
                        path = object.path.as_str(),
                        "positions disappeared after listing"
                    );
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let peer =
                match crate::posted_positions::open(&bytes, &object.path, ring.as_ref(), &log) {
                    Ok(peer) => peer,
                    Err(SyncError::Damaged(object)) => {
                        report.damaged_objects.push(object);
                        continue;
                    }
                    Err(SyncError::KeyUnavailable(key)) => {
                        tracing::debug!(
                            path = object.path.as_str(),
                            ?key,
                            "positions key is unavailable"
                        );
                        continue;
                    }
                    Err(error) => return Err(error),
                };
            if own.writes != peer.writes
                || own.store_log != peer.store_log
                || own.schema_version != peer.schema_version
            {
                continue;
            }
            for fingerprint in &own.fingerprints {
                if peer.fingerprints.iter().any(|other| {
                    other.audience == fingerprint.audience
                        && other.key == fingerprint.key
                        && other.bytes != fingerprint.bytes
                }) {
                    let mut devices = [own.device, peer.device];
                    devices.sort();
                    report.disagreements.push(crate::Disagreement {
                        devices,
                        audience: fingerprint.audience.clone(),
                        positions: own.writes.0.clone(),
                        store_log: own.store_log.0.clone(),
                        schema_version: own.schema_version,
                    });
                }
            }
        }
        report
            .disagreements
            .sort_by(|a, b| (&a.devices, &a.audience).cmp(&(&b.devices, &b.audience)));
        Ok(report)
    }

    pub(crate) async fn current_positions(&self) -> Result<Option<PostedPositions>, SyncError> {
        let member = self
            .member_keys
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
        self.positions_for(&member).await
    }

    async fn positions_for(
        &self,
        member: &coven_crypto::MemberKeys,
    ) -> Result<Option<PostedPositions>, SyncError> {
        let local = self.database.local_store_log().await?;
        let store = local
            .log
            .replay
            .state
            .store
            .as_ref()
            .ok_or(SyncFailure::Removed)?;
        let ring = self
            .store_keys
            .unlock()?
            .ok_or(SyncError::KeyUnavailable(store.key))?;
        let mut selected = vec![(Audience::Store, store.key)];
        for (circle, state) in &local.log.replay.state.circles {
            if state.deleted || !state.members.contains(&member.member_id()) {
                continue;
            }
            let audience = Audience::Circle(*circle);
            match crate::write_seal::current(&local.log, &ring, &audience, &member.member_id()) {
                Ok(key) => selected.push((audience, key)),
                Err(SyncError::KeyUnavailable(_)) => (),
                Err(error) => return Err(error),
            }
        }
        let hashers = selected
            .iter()
            .map(|(audience, key)| {
                Ok((
                    audience.clone(),
                    crate::write_seal::derive(&ring, audience, *key)?.fingerprint_hasher(),
                ))
            })
            .collect::<Result<_, SyncError>>()?;
        let state = self.database.sync_state(hashers).await?;
        crate::write_seal::check_member(&local.log, member, local.device)?;
        if state.uploads_pending {
            return Ok(None);
        }
        if local.log.positions() != state.store_log {
            return Err(DbError::StoreLogEntriesChanged.into());
        }
        let fingerprints = selected
            .into_iter()
            .zip(state.fingerprints)
            .map(|((audience, key), (actual, bytes))| {
                assert_eq!(audience, actual);
                Fingerprint {
                    audience,
                    key,
                    bytes,
                }
            })
            .collect();
        Ok(Some(PostedPositions {
            device: state.device,
            writes: state.positions,
            store_log: state.store_log,
            schema_version: state.schema_version,
            fingerprints,
        }))
    }
}
