//! One upload, download or position-posting step; scheduling belongs to the caller.

use crate::{DeviceActivity, SyncError, SyncFailure, SyncReport, WaitingWrite};
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
    storage: Arc<dyn Storage>,
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
            storage,
            database,
            store_keys,
            member_keys,
        }
    }

    /// Publish queued writes in number order, fixing the seal before the first
    /// request and retaining provider sessions across interruptions. Returns the
    /// positions confirmed stored. A newer store schema stops this step; a local
    /// breaking migration waits for its store-log raise without attempting a seal.
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
            let next = self
                .database
                .prepare_write_upload(move |log, schema, upload, emit| {
                    crate::write_seal::seal(log, schema, upload, &ring, &member, emit)
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
            self.send_write(write).await?;
            self.database.upload_succeeded(write).await?;
            uploaded.push(write);
        }
        Ok(uploaded)
    }

    /// List device logs and apply ready writes until no further listed write can
    /// advance. Damage blocks that device's successors, while independent devices
    /// continue. A later call rereads damaged objects and retains waiting start times.
    pub async fn download_writes(&mut self) -> Result<SyncReport, SyncError> {
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
        for object in self.storage.list(&ObjectPrefix::device_logs()).await? {
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
        let mut report = SyncReport::default();
        let mut damaged = BTreeSet::new();
        let mut waiting = BTreeMap::new();
        let mut positions = state.positions;
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
                    .receive_write(object, &ring, &local.log, &member.member_id())
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
        Ok(report)
    }

    /// Replace this device's authenticated D8 object from one committed database
    /// state. Returns false while any local write awaits upload: clipping only
    /// the position would publish fingerprints that still include those writes.
    pub async fn post_positions(&mut self) -> Result<bool, SyncError> {
        let local = self.database.local_store_log().await?;
        let member = self
            .member_keys
            .unlock()?
            .ok_or(SyncError::MissingMemberKeys)?;
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
        crate::write_seal::check_member(&local.log, &member, local.device)?;
        if state.uploads_pending {
            return Ok(false);
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
        let object = Object::PostedPositions(PostedPositions {
            device: state.device,
            writes: state.positions,
            store_log: state.store_log,
            schema_version: state.schema_version,
            fingerprints,
        });
        let path = ObjectPath::positions(state.device);
        let prefix = SingleChunkPrefix::PostedPositions(store.key);
        let sealed = ring.store_key(store.key)?.derive().seal_object_chunk(
            path.as_str(),
            &prefix.encode()?,
            0,
            0,
            &object.encode()?,
        )?;
        self.storage
            .replace(&path, &prefix.encode_chunk(&sealed)?)
            .await?;
        Ok(true)
    }
}
