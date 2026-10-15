//! Receiving and committing a causally ready store-log batch.

use super::*;
use crate::{PendingReason, PendingRecord, PendingSubject, Prerequisite};
use coven_format::sealed_single::SingleChunkObject;

impl StoreLogSync {
    /// One complete scan of the received store log. Each received entry that
    /// cannot apply yet is its own subject's record, replaced by this scan: its
    /// missing key, its newer format, or the first entry of its recorded past
    /// that has not applied (§19.1). Returns whether the observation completed:
    /// every observed entry applied and every key copy's clear prefix decoded.
    /// Only then does it supply sending authority and the catch-up time (§11).
    pub(super) async fn scan_store_log_once(
        &self,
        require_complete: bool,
    ) -> Result<bool, SyncError> {
        if self.send.stopped().is_some() {
            return Err(self.record_device_reset().await);
        }
        self.send.begin();
        // Reservations precede the scan: received own entries must not hide
        // that storage was ahead of this copy (§10).
        let reserved = self.database.identity_reservations().await?;
        let mut listed_entries = 0;
        let _scope = self.reads.enter();
        self.reads.reset_discovery();
        let (through, sample) = {
            let finality = self.finality.lock().expect("finality lock poisoned");
            (finality.through(), finality.sample_time())
        };
        let mut local = self.database.local_store_log().await?;
        self.reads.forget_applied(&local.log);
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
        let mut opened = BTreeMap::<EntryId, object::OpenedEntry>::new();
        let mut tentative = super::key_acquisition::Tentative::default();
        // Entries opened with a tentative candidate, by its introducing entry.
        let mut introducers = BTreeMap::<EntryId, EntryId>::new();
        let mut missing_keys = BTreeMap::new();
        let mut waits = BTreeMap::new();
        let mut entries = BTreeMap::new();
        loop {
            let unseen: Vec<_> = writers.difference(&probed).copied().collect();
            // Applied entries live in the decoded log; only later input is read.
            for received in crate::discovery::scan(
                &self.database,
                &self.reads,
                storage,
                local.store,
                coven_database::LogKind::Entry,
                &crate::discovery::past_applied(&local.log, unseen.iter().copied()),
                self.download_limit,
            )
            .await?
            {
                let (device, number) = received
                    .metadata
                    .path
                    .store_log_position()
                    .ok_or(StorageFailure::InvalidPath)?;
                if device == local.device {
                    listed_entries = listed_entries.max(number.get());
                }
                bytes.insert(
                    EntryId {
                        device,
                        number: number.get(),
                    },
                    received.bytes,
                );
                crate::store_log_landing::add_observation(&mut paths, received.metadata)?;
            }
            keys::refresh_copies(
                &self.database,
                &self.reads,
                self.download_limit,
                storage,
                local.store,
                unseen.iter().copied(),
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
                    .observe(stored, self.clock.elapsed());
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
                // A key wait does not stop its log: the next entry is read
                // and records its own first unmet condition (§19.1,
                // storelog-data `Reading.stops`).
                if id.number > 1 {
                    let prior = EntryId {
                        number: id.number - 1,
                        ..*id
                    };
                    if !local.log.replay.entries.contains_key(&prior)
                        && !opened.contains_key(&prior)
                        && !missing_keys.contains_key(&prior)
                    {
                        continue;
                    }
                }
                // An entry opened by an earlier scan is not decrypted again.
                if let Some(entry) = self.reads.opened(*id) {
                    missing_keys.remove(id);
                    tentative.opened(&entry.entry);
                    opened.insert(*id, entry);
                    continue;
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
                let entry = match self
                    .open_entry(
                        &local.log,
                        &mut tentative,
                        &envelope,
                        &stored.path,
                        *key,
                        &member,
                        &mut ring,
                    )
                    .await?
                {
                    super::key_acquisition::EntryOpening::Opened(entry) => {
                        self.reads.keep_opened(entry.clone());
                        entry
                    }
                    super::key_acquisition::EntryOpening::Tentative(entry, introducer) => {
                        introducers.insert(*id, introducer);
                        entry
                    }
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
                tentative.opened(&entry.entry);
                opened.insert(*id, entry);
            }
            for entry in opened.values().map(|opened| &opened.entry) {
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
        // A number beyond the reservations is storage ahead, whatever its
        // bytes. Undecodable bytes within them are provider damage, never
        // another copy's object: the copy reading below reports them and the
        // phase waits (§11, `OwnCopyDamage.unreadable_is_damage`).
        let listed_copies = self.reads.with_copies(|copies| copies.newest(local.device));
        // Entry and key-copy misses following this scan's sample supply two
        // of a retired device's three drain reads (§6, SP step 2).
        let drains = match sample {
            Some(sample) => self
                .database
                .retirement_finalities()
                .await?
                .into_iter()
                .filter(|finality| {
                    finality.drain_permitted(sample)
                        && self
                            .reads
                            .reached_miss(coven_database::LogKind::Entry, finality.device)
                        && self
                            .reads
                            .reached_miss(coven_database::LogKind::KeyCopy, finality.device)
                })
                .map(|finality| (finality.device, sample))
                .collect(),
            None => BTreeMap::new(),
        };
        self.reads.keep_drains(drains);
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
        ordered.sort_by_key(|opened| opened.entry.timestamp);
        // An entry opened with another entry's tentative candidate applies
        // only once that introduction has applied and accepts the candidate:
        // an unchecked candidate authorizes no other data (§11,
        // `KeyPhases.admitsDependent`). The introducing entry itself is
        // checked with it.
        let unaccepted = |batch: &crate::replay::ReplayBatch, id: EntryId| {
            introducers
                .get(&id)
                .filter(|introducer| **introducer != id)
                .map(|introducer| (introducer, tentative.candidate(*introducer)))
                .filter(|(introducer, candidate)| {
                    !batch.contains(**introducer)
                        || crate::key_introductions::hash_for(
                            &crate::key_introductions::introductions_of(batch.entries()),
                            &Audience::Store,
                            candidate.id(),
                        ) != Some(candidate.hash())
                })
                .map(|(_, candidate)| candidate.id())
        };
        loop {
            let mut advanced = false;
            for opened in &ordered {
                let id = opened.entry.position;
                if batch.contains(id)
                    || pending_entries.contains(&id)
                    || unaccepted(&batch, id).is_some()
                {
                    continue;
                }
                match batch.push(opened, &publications, local.store) {
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
        for entry in ordered.iter().map(|opened| &opened.entry) {
            let id = entry.position;
            if batch.contains(id) || pending_entries.contains(&id) {
                continue;
            }
            if let Some(key) = unaccepted(&batch, id) {
                missing_keys.insert(id, key);
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
        let complete_through = |through: Option<coven_format::value::StorageTime>| {
            let includes =
                |time: &coven_format::value::StorageTime| through.is_none_or(|end| *time <= end);
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
            // Each completed reading decides this device's records of
            // undecodable copies: a copy that now reads, or is no longer
            // read, loses its record (§19.1).
            self.database
                .replace_local_pending(
                    |record: &PendingRecord| {
                        matches!(record.subject, PendingSubject::KeyCopyPath { .. })
                    },
                    reading.damage_records(local.device),
                )
                .await?;
            let state = &local.log.replay.state;
            let binding = match state.members.get(&member.member()) {
                Some(held) if held.removed => crate::send_gate::Binding::Retired,
                Some(held) if held.keys == member.keys().public_keys() => {
                    match state.devices.get(&local.device) {
                        Some(device) if device.removed() => crate::send_gate::Binding::Retired,
                        Some(device) if device.member == member.member() => {
                            crate::send_gate::Binding::Device
                        }
                        None => crate::send_gate::Binding::Member,
                        Some(_) => crate::send_gate::Binding::Absent,
                    }
                }
                _ => crate::send_gate::Binding::Absent,
            };
            if binding == crate::send_gate::Binding::Device {
                match self
                    .identity_gate(&local, &reserved, listed_entries, listed_copies)
                    .await
                {
                    crate::identity::Gate::Send => {}
                    crate::identity::Gate::Pending(error) => return Err(error),
                    crate::identity::Gate::Reset(reason) => {
                        self.send.stop(reason);
                        return Err(self.record_device_reset().await);
                    }
                    crate::identity::Gate::Stopped => return Err(SyncError::Removed),
                }
            }
            // A copy without a readable prefix leaves the reading incomplete:
            // it supplies no sending authority (§11).
            match reading.first_damage() {
                None => {
                    self.send.complete(binding, reading.copies)?;
                    self.finality
                        .lock()
                        .expect("finality lock poisoned")
                        .complete_catch_up(sample);
                }
                Some(damage) => unreadable = Some(damage),
            }
        } else {
            self.acquire_keys(&local.log, &member, &mut ring).await?;
        }
        if let (Some((through, unit)), Some(sample)) = (through, sample) {
            if complete_through(Some(through)) {
                if let Some(horizon) = crate::finality::horizon(&local.log, through, unit) {
                    if local.log.horizon != Some(horizon) {
                        self.database
                            .advance_store_log_horizon(local.log, horizon, sample)
                            .await?;
                    }
                }
            }
        }
        // A catch-up that must be complete fails loudly on that copy; the
        // pass's first phase is incomplete, so an earlier catch-up that is
        // still fresh cannot stand in for it (SP step 2).
        match unreadable {
            Some(damage) if require_complete => Err(damage.into()),
            Some(_) => Ok(false),
            None => Ok(complete),
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
