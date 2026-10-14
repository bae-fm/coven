//! Receiving and committing a causally ready store-log batch.

use super::*;
use crate::{PendingReason, PendingRecord, PendingSubject, Prerequisite};
use coven_format::sealed_single::SingleChunkObject;

impl StoreLogSync {
    /// One complete scan of the received store log. Each received entry that
    /// cannot apply yet is its own subject's record, replaced by this scan: its
    /// missing key, its newer format, or the first entry of its recorded past
    /// that has not applied (§19.1). Returns whether every observed entry applied.
    pub(super) async fn scan_store_log_once(
        &self,
        require_complete: bool,
    ) -> Result<bool, SyncError> {
        self.send.begin();
        let _scope = self.reads.enter();
        self.reads.reset_discovery();
        let (through, sample) = {
            let finality = self.finality.lock().expect("finality lock poisoned");
            (finality.through(), finality.sample_time())
        };
        let mut local = self.database.local_store_log().await?;
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let mut ring = self.store_keys.read()?;
        self.check_stopped(&local, &member)?;
        self.acquire_keys(&local.log, &member, &mut ring).await?;
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let mut paths = Vec::new();
        let mut writers =
            crate::discovery::writers(&self.database, &self.reads, storage, local.store).await?;
        let stuck = crate::pending_logs::local_refusals(&self.database).await?;
        if require_complete {
            if let Some(refusal) = stuck
                .iter()
                .find(|r| matches!(r.object, coven_database::LogObject::Entry(_)))
            {
                return Err(crate::pending_logs::required(&self.database, *refusal).await?);
            }
        }
        let mut probed = BTreeSet::new();
        let mut pending_entries = BTreeSet::new();
        let mut bytes = BTreeMap::new();
        let mut opened = BTreeMap::<EntryId, StoreLogEntry>::new();
        let mut missing_keys = BTreeMap::new();
        let mut waits = BTreeMap::new();
        let mut entries = BTreeMap::new();
        loop {
            let unseen = writers.difference(&probed).copied().collect();
            for received in crate::discovery::scan(
                &self.database,
                &self.reads,
                storage,
                local.store,
                coven_database::LogKind::Entry,
                &unseen,
                self.download_limit,
            )
            .await?
            {
                let (device, number) = received
                    .metadata
                    .path
                    .store_log_position()
                    .ok_or(StorageFailure::InvalidPath)?;
                bytes.insert(
                    EntryId {
                        device,
                        number: number.get(),
                    },
                    received.bytes,
                );
                crate::store_log_landing::add_observation(&mut paths, received.metadata)?;
            }
            crate::discovery::scan(
                &self.database,
                &self.reads,
                storage,
                local.store,
                coven_database::LogKind::KeyCopy,
                &unseen,
                self.download_limit,
            )
            .await?;
            probed.extend(&writers);
            for stored in &paths {
                let (device, number) = stored.path.store_log_position().ok_or(
                    StorageFailure::Protocol
                        .with_source("store-log observation returned another layout"),
                )?;
                entries.insert(
                    EntryId {
                        device,
                        number: number.get(),
                    },
                    stored.clone(),
                );
                self.finality
                    .lock()
                    .expect("finality lock poisoned")
                    .observe(stored);
            }
            for (id, stored) in &entries {
                if local.log.replay.entries.contains_key(id)
                    || opened.contains_key(id)
                    || pending_entries.contains(id)
                    || stuck
                        .iter()
                        .any(|record| record.blocks(coven_database::LogObject::Entry(*id)))
                {
                    continue;
                }
                if id.number > 1 {
                    let prior = EntryId {
                        number: id.number - 1,
                        ..*id
                    };
                    if !local.log.replay.entries.contains_key(&prior)
                        && !opened.contains_key(&prior)
                    {
                        continue;
                    }
                }
                let envelope = match SingleChunkObject::decode(&bytes[id]) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        if let Some(wait) = self
                            .stuck_entry(*id, &stored.path, Refusal::from(error))
                            .await?
                        {
                            waits.insert(*id, wait);
                        }
                        pending_entries.insert(*id);
                        continue;
                    }
                };
                let SingleChunkObject::StoreLog { key, .. } = &envelope else {
                    self.stuck_entry(*id, &stored.path, Refusal::WrongIdentity { cause: None })
                        .await?;
                    pending_entries.insert(*id);
                    continue;
                };
                let scanned: Vec<_> = opened.values().collect();
                let entry = match self
                    .open_entry(
                        &local.log,
                        &scanned,
                        &envelope,
                        &stored.path,
                        *key,
                        &member,
                        &mut ring,
                    )
                    .await?
                {
                    super::key_acquisition::EntryOpening::Opened(entry) => entry,
                    super::key_acquisition::EntryOpening::WaitsForKey => {
                        missing_keys.insert(*id, *key);
                        continue;
                    }
                    super::key_acquisition::EntryOpening::Refused(failure) => {
                        if let Some(wait) = self.stuck_entry(*id, &stored.path, failure).await? {
                            waits.insert(*id, wait);
                        }
                        pending_entries.insert(*id);
                        continue;
                    }
                };
                #[cfg(test)]
                if let Some(counter) = &self.received_entry_decodes {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                missing_keys.remove(id);
                opened.insert(*id, entry);
            }
            for entry in opened.values() {
                writers.extend(entry.had_read.0.iter().map(|id| id.device));
                if let StoreChange::AddDevice {
                    device, replaces, ..
                } = &entry.change
                {
                    writers.insert(*device);
                    writers.extend(replaces);
                }
            }
            if writers.len() == probed.len() {
                break;
            }
        }
        // Discovery, including writers learned from these entries, precedes
        // immutable landing checks. Every author is checked against its own past.
        let publications = crate::store_log_landing::publication_times(&local.log, &paths)?;
        if opened.keys().any(|id| !publications.contains_key(id)) {
            return Err(StorageFailure::NotFound
                .with_source("published store-log entry is absent from exact status")
                .into());
        }
        let mut batch = crate::replay::ReplayBatch::new(&local.log);
        let mut ordered: Vec<_> = opened.values().collect();
        ordered.sort_by_key(|entry| entry.timestamp);
        loop {
            let mut advanced = false;
            for entry in &ordered {
                let id = entry.position;
                if batch.contains(id) || pending_entries.contains(&id) {
                    continue;
                }
                match batch.push(entry, &publications, local.store) {
                    Ok(true) => advanced = true,
                    Ok(false) => (),
                    Err(failure) => {
                        if let Some(wait) =
                            self.stuck_entry(id, &entries[&id].path, failure).await?
                        {
                            waits.insert(id, wait);
                        }
                        pending_entries.insert(id);
                    }
                }
            }
            if !advanced {
                break;
            }
        }
        // An opened entry left out of the batch waits for the first entry of
        // its recorded past that has not applied.
        for entry in &ordered {
            let id = entry.position;
            if batch.contains(id) || pending_entries.contains(&id) {
                continue;
            }
            match object::first_missing(entry, |past| batch.contains(past)) {
                Some(past) => {
                    waits.insert(
                        id,
                        PendingReason::Waits(Prerequisite::Object(object::path(local.store, past))),
                    );
                }
                // Its whole recorded past applied while the store's creation
                // has not: that creation must check this entry when it arrives.
                None => tracing::debug!(?id, "entry waits for the store's creation"),
            }
        }
        for (id, key) in &missing_keys {
            waits.insert(
                *id,
                PendingReason::KeyUnavailable {
                    audience: Audience::Store,
                    key: *key,
                },
            );
        }
        // One behind a stopped entry of its own log is not read again, so it
        // has no record of its own (§19.1).
        waits.retain(|id, _| {
            !pending_entries
                .iter()
                .any(|stop| stop.device == id.device && stop.number < id.number)
        });
        if let Some((ready, replay)) = batch.finish() {
            self.apply_batch(&mut local, ready, replay, &member).await?;
        }
        self.replace_entry_waits(&local, waits).await?;
        if require_complete {
            if let Some(key) = missing_keys.values().next() {
                return Err(SyncError::KeyUnavailable(*key));
            }
            if let Some(refusal) = crate::pending_logs::local_refusals(&self.database)
                .await?
                .into_iter()
                .find(|refusal| matches!(refusal.object, coven_database::LogObject::Entry(_)))
            {
                return Err(crate::pending_logs::required(&self.database, refusal).await?);
            }
            if let Some(id) = entries.keys().find(|id| {
                !local.log.replay.entries.contains_key(id) && !pending_entries.contains(id)
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
        let mut unreadable = None;
        if complete {
            self.acquire_keys(&local.log, &member, &mut ring).await?;
            let reading = keys::read_copies(
                &self.database,
                &self.reads,
                self.download_limit,
                storage,
                local.store,
            )
            .await?;
            let state = &local.log.replay.state;
            let binding = match state.members.get(&member.member_id()) {
                Some(held) if held.removed => crate::send_gate::Binding::Retired,
                Some(held) if held.sealing == member.sealing_public_key() => {
                    match state.devices.get(&local.device) {
                        Some(device) if device.removed => crate::send_gate::Binding::Retired,
                        Some(device) if device.member == member.member_id() => {
                            crate::send_gate::Binding::Device
                        }
                        None => crate::send_gate::Binding::Member,
                        Some(_) => crate::send_gate::Binding::Absent,
                    }
                }
                _ => crate::send_gate::Binding::Absent,
            };
            // A copy without a readable prefix leaves the reading incomplete:
            // it supplies no sending authority (§11).
            match reading.unreadable {
                None => self.send.complete(binding, reading.copies)?,
                Some(damage) => unreadable = Some(damage),
            }
            self.finality
                .lock()
                .expect("finality lock poisoned")
                .complete_catch_up(sample);
        } else {
            self.acquire_keys(&local.log, &member, &mut ring).await?;
        }
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
        // A catch-up that must be complete fails loudly on that copy.
        match unreadable {
            Some(damage) if require_complete => Err(damage.into()),
            _ => Ok(complete),
        }
    }

    /// This completed scan decides every entry wait this device records:
    /// received entries that cannot apply yet, and its own queued entry whose
    /// recorded past has not applied. An entry that applied, or no longer
    /// waits, loses its record; refusals and drops are not waits (§19.1).
    async fn replace_entry_waits(
        &self,
        local: &coven_database::LocalStoreLog,
        mut waits: BTreeMap<EntryId, PendingReason>,
    ) -> Result<(), SyncError> {
        if let Some(upload) = &local.upload {
            if let Some(past) = object::first_missing(&upload.entry, |past| {
                local.log.replay.entries.contains_key(&past)
            }) {
                waits.insert(
                    upload.entry.position,
                    PendingReason::Waits(Prerequisite::Object(object::path(local.store, past))),
                );
            }
        }
        let records = waits
            .into_iter()
            .filter(|(id, _)| !local.log.replay.entries.contains_key(id))
            .map(|(id, reason)| PendingRecord {
                subject: PendingSubject::Entry(id),
                reason,
                reported_by: local.device,
            })
            .collect();
        self.database
            .replace_local_pending(
                |record: &PendingRecord| {
                    matches!(record.subject, PendingSubject::Entry(_))
                        && matches!(
                            record.reason,
                            PendingReason::Waits(_)
                                | PendingReason::KeyUnavailable { .. }
                                | PendingReason::UpdateRequired(_)
                        )
                },
                records,
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "store_log_batch_tests.rs"]
mod tests;
