//! One upload, download or position-posting step; scheduling belongs to the caller.

use crate::{PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_crypto::{custody::KeySession, MemberIdentity, StoreKeyring};
use coven_database::{ApplyOutcome, Database, DbError, LogObject, LogRefusal};
use coven_foundation::clock::ClockRef;
use coven_merge::{Audience, WriteId};
use coven_storage::Storage;
use std::collections::BTreeMap;
use std::sync::Arc;

#[path = "write_download.rs"]
mod download;
#[path = "write_drain.rs"]
mod drain;
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
    send: Arc<crate::SendGate>,
    storage: Option<Arc<dyn Storage>>,
    database: Database,
    store_keys: Arc<KeySession<StoreKeyring>>,
    member_keys: Arc<KeySession<MemberIdentity>>,
}

impl DeviceLogSync {
    /// Retain the provider, database and custody selected for this store.
    pub fn new(
        storage: Arc<dyn Storage>,
        database: Database,
        store_keys: Arc<KeySession<StoreKeyring>>,
        member_keys: Arc<KeySession<MemberIdentity>>,
        clock: ClockRef,
        send: Arc<crate::SendGate>,
    ) -> Self {
        Self {
            reads: crate::pass_reads::PassReads::default(),
            observation: None,
            clock,
            download_limit: std::num::NonZeroUsize::MIN,
            send,
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
        member_keys: Arc<KeySession<MemberIdentity>>,
        clock: ClockRef,
        send: Arc<crate::SendGate>,
    ) -> Self {
        Self {
            reads: crate::pass_reads::PassReads::default(),
            observation: None,
            clock,
            download_limit: std::num::NonZeroUsize::MIN,
            send,
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

    /// Publish queued writes in number order, fixing the sealing keys before the first
    /// request and retaining provider sessions across interruptions. Returns the
    /// positions confirmed stored. A newer store schema stops this step; a local
    /// breaking migration waits for its store-log raise without recording an attempt.
    /// A write with an audience lacking a usable key records that wait and stops
    /// this log's sends, later numbers included; the pass goes on (§11).
    pub async fn upload_writes(&mut self) -> Result<Vec<WriteId>, SyncError> {
        self.upload(true).await
    }

    /// Retry only already attempted writes, with their fixed bytes. A pass
    /// whose first phase did not complete makes no first attempt (SP step 2);
    /// attempted writes precede every untried one in the queue.
    pub(crate) async fn retry_attempted_writes(&mut self) -> Result<Vec<WriteId>, SyncError> {
        self.upload(false).await
    }

    async fn upload(&mut self, first_attempts: bool) -> Result<Vec<WriteId>, SyncError> {
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
            if !first_attempts
                && !self
                    .database
                    .read_oldest_upload(|upload| Ok::<_, SyncError>(upload.keys.is_some()))
                    .await
                    .map_err(|error| match error {
                        coven_database::UploadReadError::Database(error) => error.into(),
                        coven_database::UploadReadError::Consumer(error) => error,
                    })?
                    .unwrap_or(false)
            {
                break;
            }
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
            let observation = self.send.observation()?;
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
        // A drained device is never probed again; its discovered writes come
        // from retained observations. A retired device whose entry and copy
        // logs reached misses after a qualifying sample this pass completes its
        // drain with a trusted write miss here (§6, SP step 6).
        let drained = self.database.drained_devices().await?;
        let drain_reads = self.reads.drains();
        let draining: BTreeMap<_, _> = self
            .database
            .retirement_finalities()
            .await?
            .into_iter()
            .filter_map(|finality| {
                let sample = *drain_reads.get(&finality.device)?;
                finality
                    .drain_permitted(sample)
                    .then_some((finality.device, (finality, sample)))
            })
            .collect();
        let mut drains = Vec::new();
        // Every other device that ever registered, including removed and
        // replaced ones. The first phase reads this device's own next write.
        let writers: std::collections::BTreeSet<_> = local
            .log
            .replay
            .state
            .devices
            .keys()
            .copied()
            .filter(|device| *device != local.device)
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
            if drained.contains(&writer) {
                objects.extend(
                    self.retained_writes(local.store, writer, after, stop)
                        .await?,
                );
                continue;
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
            } else if let (Some(drain), None) = (draining.get(&writer), stop) {
                drains.push((*drain, miss));
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
                    .receive_write(
                        object,
                        &ring,
                        &local.log,
                        &mut replays,
                        &member.member(),
                        local.device,
                    )
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
                    // Waits and refusals are already the write's pending record;
                    // the pass continues with independent logs (§19.1).
                    Ok(ApplyOutcome::Waiting(reason)) => {
                        tracing::debug!(?write, ?reason, "write is waiting for its prerequisites");
                        waits.insert(*write, reason);
                    }
                    Err(SyncError::Damaged(damage)) => stuck.push(LogRefusal {
                        object: LogObject::Write(*write),
                        failure: (&damage.failure).into(),
                    }),
                    Err(SyncError::KeyUnavailable(key)) => {
                        tracing::debug!(?write, ?key, "write key is unavailable");
                    }
                    Err(SyncError::UpdateRequired(update)) => {
                        tracing::debug!(?write, ?update, "write needs a newer coven");
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
        for ((drain, sample), miss) in drains {
            self.finish_drain(drain, sample, miss, &objects, &positions, &waits)
                .await?;
        }
        self.compare_fingerprints().await?;
        Ok(discovery)
    }
}

#[path = "position_publication.rs"]
mod positions;
