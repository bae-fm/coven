//! One upload, download or position-posting step; scheduling belongs to the caller.

use crate::{PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_crypto::{custody::KeySession, MemberKeys, StoreKeyring};
use coven_database::{ApplyOutcome, Database, DbError, LogObject, LogRefusal};
use coven_format::{
    objects::{Fingerprint, PostedPositions},
    sealed_single::SingleChunkPrefix,
    Object,
};
use coven_foundation::clock::ClockRef;
use coven_merge::{Audience, WriteId};
use coven_storage::{ObjectPath, ObjectPrefix, Storage};
use std::collections::BTreeMap;
use std::sync::Arc;

#[path = "write_download.rs"]
mod download;
#[cfg(test)]
#[path = "device_log_sync_tests.rs"]
pub(crate) mod tests;
#[path = "write_upload.rs"]
mod upload;

/// What one write discovery established (§15, SP step 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteDiscovery {
    /// Every log reached a miss the recent-return rule trusts, and every write
    /// found applied. Only then can the pass certify a return checkpoint.
    pub complete: bool,
    /// A miss could not be trusted, or required history is missing: snapshot
    /// discovery must come before further misses are used.
    pub needs_snapshots: bool,
}

/// Probe a writer's next numbers by exact status, fetching ahead in parallel
/// up to `limit` after a hit and scheduling nothing past the first miss or a
/// refused number `stop`. Returns the contiguous hits and the missed number,
/// none when the refusal ended the log first. A failed probe fails the call
/// after in-flight requests settle; it is never a miss.
async fn next_writes(
    storage: &dyn Storage,
    store: coven_foundation::id_source::StoreId,
    writer: coven_foundation::id_source::DeviceId,
    after: u64,
    stop: Option<u64>,
    limit: std::num::NonZeroUsize,
) -> Result<(Vec<(WriteId, coven_storage::StoredObject)>, Option<u64>), SyncError> {
    use futures_util::{stream::FuturesUnordered, StreamExt};
    let mut running = FuturesUnordered::new();
    let mut next = after;
    let mut width = 1;
    let mut miss = None::<u64>;
    let mut failed = None;
    let mut found = BTreeMap::new();
    loop {
        while miss.is_none() && failed.is_none() && running.len() < width {
            let number = next.checked_add(1).ok_or_else(|| {
                coven_storage::StorageFailure::Protocol.with_source("write numbers exhausted")
            })?;
            if stop.is_some_and(|stop| number >= stop) {
                break;
            }
            next = number;
            let path = crate::write_seal::path(
                store,
                WriteId {
                    device: writer,
                    number,
                },
            );
            running.push(async move { (number, storage.status(&path).await) });
        }
        let Some((number, result)) = running.next().await else {
            break;
        };
        match result {
            Ok(Some(object)) => {
                found.insert(number, object);
                width = limit.get();
            }
            Ok(None) => miss = Some(miss.map_or(number, |known| known.min(number))),
            Err(error) => {
                failed.get_or_insert(error);
            }
        }
    }
    if let Some(error) = failed {
        return Err(error.into());
    }
    Ok((
        found
            .into_iter()
            .filter(|(number, _)| miss.is_none_or(|miss| *number < miss))
            .map(|(number, object)| {
                (
                    WriteId {
                        device: writer,
                        number,
                    },
                    object,
                )
            })
            .collect(),
        miss,
    ))
}

/// Device-log transfer and position publication using the capabilities chosen
/// by the opening composition root. Calls on one owner are serialized by `&mut
/// self`; each step completes its database work before returning.
pub struct DeviceLogSync {
    reads: crate::pass_reads::PassReads,
    /// The pass's storage-time evidence; without it no write miss is trusted.
    observation: Option<Arc<std::sync::Mutex<crate::finality::Observation>>>,
    /// Sleep-inclusive monotonic time for judging each miss when it is used.
    clock: ClockRef,
    download_limit: std::num::NonZeroUsize,
    /// This pass's key-copy observation (§11).
    copies: crate::key_introductions::PassObservation,
    storage: Option<Arc<dyn Storage>>,
    database: Database,
    store_keys: Arc<KeySession<StoreKeyring>>,
    member_keys: Arc<KeySession<MemberKeys>>,
}

impl DeviceLogSync {
    /// Retain the provider, database and custody selected for this store.
    pub fn new(
        storage: Arc<dyn Storage>,
        database: Database,
        store_keys: Arc<KeySession<StoreKeyring>>,
        member_keys: Arc<KeySession<MemberKeys>>,
        clock: ClockRef,
    ) -> Self {
        Self {
            reads: crate::pass_reads::PassReads::default(),
            observation: None,
            clock,
            download_limit: std::num::NonZeroUsize::MIN,
            copies: Default::default(),
            storage: Some(storage),
            database,
            store_keys,
            member_keys,
        }
    }

    /// Compose device-log work before a provider has connected.
    pub fn disconnected(
        database: Database,
        store_keys: Arc<KeySession<StoreKeyring>>,
        member_keys: Arc<KeySession<MemberKeys>>,
        clock: ClockRef,
    ) -> Self {
        Self {
            reads: crate::pass_reads::PassReads::default(),
            observation: None,
            clock,
            download_limit: std::num::NonZeroUsize::MIN,
            copies: Default::default(),
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

    pub(crate) fn share_observation(
        &mut self,
        observation: Arc<std::sync::Mutex<crate::finality::Observation>>,
    ) {
        self.observation = Some(observation);
    }

    pub(crate) fn set_download_limit(&mut self, limit: std::num::NonZeroUsize) {
        self.download_limit = limit;
    }

    pub(crate) fn share_copies(&mut self, copies: crate::key_introductions::PassObservation) {
        self.copies = copies;
    }

    /// The pass's complete key-copy observation, or a fresh one outside a pass (§11).
    async fn copy_observation(
        &self,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<crate::key_introductions::CopyObservation, SyncError> {
        match self.copies.current() {
            Some(observation) => Ok(observation),
            None => {
                crate::store_log_keys::observe(
                    &self.database,
                    &self.reads,
                    self.download_limit,
                    self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                    store,
                )
                .await
            }
        }
    }

    /// Publish queued writes in number order, fixing the sealing keys before the first
    /// request and retaining provider sessions across interruptions. Returns the
    /// positions confirmed stored. A newer store schema stops this step; a local
    /// breaking migration waits for its store-log raise without recording an attempt.
    /// A write with an audience lacking a usable key records that wait and stops
    /// this log's sends, later numbers included; the pass goes on (§11).
    pub async fn upload_writes(&mut self) -> Result<Vec<WriteId>, SyncError> {
        let mut uploaded = Vec::new();
        let mut waits: Vec<_> = self
            .database
            .pending::<PendingRecord>()
            .await?
            .into_iter()
            .filter(|record| {
                matches!(record.subject, PendingSubject::Write(write) if write.device == record.reported_by)
                    && matches!(record.reason, PendingReason::KeyUnavailable { .. })
            })
            .collect();
        loop {
            let local = self.database.local_store_log().await?;
            let state = self.database.sync_state(Vec::new()).await?;
            let member = self
                .member_keys
                .read()?
                .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
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
            let store_key = crate::key_introductions::newest(&local.log, &Audience::Store)
                .expect("checked store membership");
            let ring = self
                .store_keys
                .read()?
                .ok_or(SyncError::KeyUnavailable(store_key))?;
            let selected_ring = ring.clone();
            let signer = member.clone();
            let observation = self.copy_observation(local.store).await?;
            let prepared = self
                .database
                .prepare_write_upload(move |log, schema, header| {
                    crate::write_seal::select(
                        log,
                        schema,
                        header,
                        &selected_ring,
                        &signer,
                        &observation,
                    )
                    .map_err(|e| DbError::SyncStream(Box::new(e)))
                })
                .await
                .map_err(|error| match error {
                    DbError::SyncStream(source) => match source.downcast::<SyncError>() {
                        Ok(error) => *error,
                        Err(source) => DbError::SyncStream(source).into(),
                    },
                    error => error.into(),
                });
            let next = match prepared {
                Ok(next) => next,
                Err(error) => {
                    let Some(wait) = crate::write_seal::key_wait(&error) else {
                        return Err(error);
                    };
                    tracing::debug!(subject = ?wait.subject, reason = ?wait.reason, "write waits for a key");
                    if !waits.contains(wait) {
                        self.database.record_pending(wait.clone()).await?;
                    }
                    break;
                }
            };
            let Some(write) = next else {
                break;
            };
            // Its keys are now fixed and held: any recorded key wait is over.
            if let Some(index) = waits
                .iter()
                .position(|wait| wait.subject == PendingSubject::Write(write))
            {
                self.database
                    .clear_pending(waits.remove(index).subject)
                    .await?;
            }
            self.send_write(local.store, write, ring, member).await?;
            self.database.upload_succeeded(write).await?;
            uploaded.push(write);
        }
        Ok(uploaded)
    }

    /// GET each device's next write number, fetching ahead on a hit, and apply
    /// ready writes until none can advance. Each miss is judged by the recent-
    /// return rule when it is used (§15); an untrusted miss, or a missing number
    /// another write needs, asks the pass for snapshot discovery first.
    /// Permanent failures are recorded once and stop that log; independent logs
    /// continue. Waiting prerequisites and failed reads remain retryable.
    pub async fn download_writes(&mut self) -> Result<WriteDiscovery, SyncError> {
        let _reads = self.reads.enter();
        let local = self.database.local_store_log().await?;
        let state = self.database.sync_state(Vec::new()).await?;
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        crate::write_seal::check_member(&local.log, &member, local.device)?;
        let key = crate::key_introductions::newest(&local.log, &Audience::Store)
            .expect("checked membership");
        let ring = self
            .store_keys
            .read()?
            .ok_or(SyncError::KeyUnavailable(key))?;
        let saved = self.database.return_checkpoint().await?;
        let storage = self.storage.as_deref().ok_or(SyncError::NoStorage)?;
        let refused = crate::pending_logs::local_refusals(&self.database).await?;
        let mut discovery = WriteDiscovery {
            complete: true,
            needs_snapshots: false,
        };
        let mut objects = BTreeMap::new();
        let mut ends = BTreeMap::new();
        // Every device that ever registered, including removed and replaced
        // ones, and this device's own log for its identity read.
        let writers: std::collections::BTreeSet<_> = local
            .log
            .replay
            .state
            .devices
            .keys()
            .copied()
            .chain([local.device])
            .collect();
        for writer in writers {
            // A refused write stops its log: nothing at or past it is read.
            let stop = refused
                .iter()
                .filter_map(|record| match record.object {
                    LogObject::Write(id) if id.device == writer => Some(id.number),
                    _ => None,
                })
                .min();
            let after = state
                .positions
                .0
                .iter()
                .find(|position| position.device == writer)
                .map_or(0, |position| position.number);
            if stop.is_some() {
                discovery.complete = false;
            }
            let (found, end) = next_writes(
                storage,
                local.store,
                writer,
                after,
                stop,
                self.download_limit,
            )
            .await?;
            objects.extend(found);
            let Some(miss) = end else {
                continue;
            };
            ends.insert(writer, miss);
            if !self.observation.as_ref().is_some_and(|observation| {
                crate::recent_return::miss_is_recent(observation, self.clock.as_ref(), saved)
            }) {
                discovery.needs_snapshots = true;
            }
        }
        let mut stuck = refused;
        let mut waits = BTreeMap::new();
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
                    continue;
                }
                if stuck
                    .iter()
                    .any(|record| record.blocks(LogObject::Write(*write)))
                {
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
                        progressed = true;
                    }
                    Ok(ApplyOutcome::Waiting(reason)) => {
                        tracing::debug!(?write, ?reason, "write is waiting for its prerequisites");
                        waits.insert(*write, reason);
                    }
                    Err(SyncError::Damaged(damage)) => {
                        stuck.push(
                            crate::write_object::record_damage(
                                &self.database,
                                self.storage.as_deref().ok_or(SyncError::NoStorage)?,
                                object,
                                &damage.failure,
                            )
                            .await?,
                        );
                    }
                    Err(SyncError::KeyUnavailable(key)) => {
                        tracing::debug!(?write, ?key, "write key is unavailable");
                    }
                    Err(error) => return Err(error),
                }
            }
            if !progressed {
                break;
            }
        }
        for write in objects.keys() {
            if positions.covers(*write) {
                continue;
            }
            discovery.complete = false;
            // A cause past its writer's miss is required history: only a fresh
            // snapshot catalog can certify its deletion (§15).
            if let Some(coven_database::WriteWait::Writes(needed)) = waits.get(write) {
                if needed.iter().any(|cause| {
                    ends.get(&cause.device)
                        .is_some_and(|miss| cause.number >= *miss)
                }) {
                    discovery.needs_snapshots = true;
                }
            }
        }
        if discovery.needs_snapshots {
            discovery.complete = false;
        }
        self.compare_fingerprints().await?;
        Ok(discovery)
    }

    /// Replace this device's authenticated D8 object from one committed database
    /// state. Returns false while any local write awaits upload: clipping only
    /// the position would publish fingerprints that still include those writes.
    pub async fn post_positions(&mut self) -> Result<bool, SyncError> {
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let local = self.database.local_store_log().await?;
        let Some(positions) = self.positions_for(&member, &local).await? else {
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
            .read()?
            .ok_or(SyncError::KeyUnavailable(key))?;
        let path = ObjectPath::positions(local.store, positions.device);
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
    /// Signed peer reports about our objects are retained independently of agreement.
    async fn compare_fingerprints(&mut self) -> Result<(), SyncError> {
        let own = self.current_positions().await?;
        let ring = self.store_keys.read()?;
        let local = self.database.local_store_log().await?;
        let log = local.log;
        let state = self.database.sync_state(Vec::new()).await?;
        let mut reports = Vec::new();
        for object in self
            .storage
            .as_deref()
            .ok_or(SyncError::NoStorage)?
            .list(&ObjectPrefix::positions(local.store))
            .await?
        {
            if object.path.device() == Some(local.device) {
                continue;
            }
            let input = crate::object_input::ObjectInput::observe(&self.database, &object).await?;
            let bytes = match input
                .whole(self.storage.as_deref().ok_or(SyncError::NoStorage)?)
                .await
            {
                Ok(bytes) => bytes,
                Err(SyncError::Storage(error))
                    if error.failure() == coven_storage::StorageFailure::NotFound =>
                {
                    tracing::debug!(
                        path = object.path.as_str(),
                        "positions disappeared after listing"
                    );
                    continue;
                }
                Err(error) => return Err(error),
            };
            let peer =
                match crate::posted_positions::open(&bytes, &object.path, ring.as_ref(), &log) {
                    Ok(peer) => peer,
                    Err(SyncError::Damaged(object)) => {
                        tracing::warn!(error = %object, "damaged positions count as not posted");
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
            reports.extend(
                peer.pending
                    .iter()
                    .filter_map(LogRefusal::from_report)
                    .filter(|record| {
                        record.object.device() == local.device
                            && match record.object {
                                LogObject::Write(id) => state.positions.covers(id),
                                LogObject::Entry(id) => state.store_log.covers(id),
                            }
                    })
                    .map(|record| (peer.device, record)),
            );
            let Some(own) = &own else { continue };
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
                    tracing::warn!(
                        device = ?own.device,
                        peer = ?peer.device,
                        audience = ?fingerprint.audience,
                        "fingerprints disagree at matching positions and schema"
                    );
                }
            }
        }
        self.database
            .replace_pending_reports(
                reports
                    .into_iter()
                    .map(|(peer, record)| crate::PendingRecord::log_refusal(record, peer))
                    .collect(),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn current_positions(&self) -> Result<Option<PostedPositions>, SyncError> {
        let member = self
            .member_keys
            .read()?
            .ok_or(coven_storage::StorageFailure::MemberKeysMissing)?;
        let local = self.database.local_store_log().await?;
        self.positions_for(&member, &local).await
    }

    async fn positions_for(
        &self,
        member: &coven_crypto::MemberKeys,
        local: &coven_database::LocalStoreLog,
    ) -> Result<Option<PostedPositions>, SyncError> {
        let store_key = crate::key_introductions::newest(&local.log, &Audience::Store)
            .ok_or(SyncError::Removed)?;
        let ring = self
            .store_keys
            .read()?
            .ok_or(SyncError::KeyUnavailable(store_key))?;
        // Positions are replaceable metadata, not first attempts; fingerprints
        // use the newest key this device reads each audience with.
        let mut selected = vec![(
            Audience::Store,
            crate::write_seal::readable_key(&local.log, &ring, &Audience::Store)?,
        )];
        for (circle, state) in &local.log.replay.state.circles {
            if local.log.replay.state.circle_deletions.contains_key(circle)
                || !state.members.contains(&member.member_id())
            {
                continue;
            }
            let audience = Audience::Circle(*circle);
            match crate::write_seal::readable_key(&local.log, &ring, &audience) {
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
            pending: crate::pending_logs::reports(&self.database).await?,
            device: state.device,
            writes: state.positions,
            store_log: state.store_log,
            schema_version: state.schema_version,
            fingerprints,
        }))
    }
}
