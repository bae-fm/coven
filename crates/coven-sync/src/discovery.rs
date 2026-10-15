//! Next-number reads retain complete input before advancing a discovery cursor.

use crate::SyncError;
use coven_database::{Database, LogInput, LogKind};
use coven_foundation::id_source::{DeviceId, StoreId};
use coven_storage::{ObjectPath, ObjectRead, Storage, StorageFailure};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

/// Completed reads are committed together, bounding the bytes held in memory.
const RETAIN_BATCH: usize = 16 * 1024 * 1024;

/// The writers to probe in this observation: the immediate store-log folders
/// listed once per observation, and every writer known locally. Folders are
/// remembered, so a writer survives a terminal miss and a later listing omission.
pub(crate) async fn writers(
    database: &Database,
    reads: &crate::pass_reads::PassReads,
    storage: &dyn Storage,
    store: StoreId,
) -> Result<BTreeSet<DeviceId>, SyncError> {
    let listed = match reads.writers() {
        Some(writers) => writers,
        None => {
            let writers: BTreeSet<_> = storage.list_writers(store).await?.into_iter().collect();
            reads.keep_writers(writers.clone());
            writers
        }
    };
    let mut writers = database.known_log_writers().await?;
    let unknown: Vec<_> = listed.difference(&writers).copied().collect();
    if !unknown.is_empty() {
        database.remember_log_writers(unknown).await?;
    }
    writers.extend(listed);
    Ok(writers)
}

pub(crate) fn path(
    store: StoreId,
    kind: LogKind,
    writer: DeviceId,
    number: NonZeroU64,
) -> ObjectPath {
    match kind {
        LogKind::Entry => ObjectPath::store_log(store, writer, number),
        LogKind::KeyCopy => ObjectPath::key_copy(store, writer, number),
    }
}

/// Each writer's numbers past its newest applied entry, keeping that entry:
/// the decoded log holds the rest, and the newest one's observation keeps
/// the storage-time sample a restarted session starts from.
pub(crate) fn past_applied(
    log: &coven_database::StoreLog,
    writers: impl IntoIterator<Item = DeviceId>,
) -> BTreeMap<DeviceId, u64> {
    let applied: BTreeMap<_, _> = log
        .positions()
        .0
        .into_iter()
        .map(|id| (id.device, id.number))
        .collect();
    writers
        .into_iter()
        .map(|writer| {
            let number = applied.get(&writer).copied().unwrap_or(0);
            (writer, number.saturating_sub(1))
        })
        .collect()
}

/// The retained input of each writer past the number named with it.
pub(crate) async fn retained(
    database: &Database,
    store: StoreId,
    kind: LogKind,
    after: &BTreeMap<DeviceId, u64>,
) -> Result<BTreeMap<(DeviceId, NonZeroU64), ObjectRead>, SyncError> {
    database
        .log_inputs(kind, after.iter().map(|(w, n)| (*w, *n)).collect())
        .await?
        .into_iter()
        .map(|input| {
            let read = ObjectRead {
                metadata: serde_json::from_slice(&input.observation)?,
                bytes: input.bytes,
            };
            if read.metadata.path != path(store, kind, input.writer, input.number)
                || read.metadata.size != read.bytes.len() as u64
            {
                return Err(coven_database::DbError::DamagedDatabase.into());
            }
            Ok(((input.writer, input.number), read))
        })
        .collect()
}

/// Read each writer's log through its next miss. Returns the retained input
/// past the number each writer names, and every new read.
pub(crate) async fn scan(
    database: &Database,
    reads: &crate::pass_reads::PassReads,
    storage: &dyn Storage,
    store: StoreId,
    kind: LogKind,
    writers: &BTreeMap<DeviceId, u64>,
    limit: std::num::NonZeroUsize,
) -> Result<Vec<ObjectRead>, SyncError> {
    let mut input = retained(database, store, kind, writers).await?;
    let positions: BTreeMap<_, _> = database.log_downloads(kind).await?.into_iter().collect();
    // A refused entry stops its writer's log: nothing at or past it is read.
    // Its number is retained, so the next read would be past it.
    let refused: BTreeSet<DeviceId> = match kind {
        LogKind::Entry => crate::pending_logs::local_refusals(database)
            .await?
            .into_iter()
            .filter_map(|refusal| match refusal.object {
                coven_database::LogObject::Entry(entry) => Some(entry.device),
                coven_database::LogObject::Write(_) => None,
            })
            .collect(),
        LogKind::KeyCopy => BTreeSet::new(),
    };
    // A completed drain ends discovery; its retained history is in `input`.
    let drained = database.drained_devices().await?;
    for &writer in writers.keys() {
        if refused.contains(&writer) || drained.contains(&writer) {
            continue;
        }
        let after = positions.get(&writer).copied().unwrap_or(0);
        scan_tail(
            database, reads, storage, store, kind, writer, after, limit, &mut input,
        )
        .await?;
    }
    Ok(input.into_values().collect())
}

/// An invitation's tail starts at its promised number without claiming that the
/// historical prefix has been received. Cached tail input is reused by both scans.
pub(crate) async fn scan_tail(
    database: &Database,
    reads: &crate::pass_reads::PassReads,
    storage: &dyn Storage,
    store: StoreId,
    kind: LogKind,
    writer: DeviceId,
    mut after: u64,
    limit: std::num::NonZeroUsize,
    input: &mut BTreeMap<(DeviceId, NonZeroU64), ObjectRead>,
) -> Result<(), SyncError> {
    use futures_util::{stream::FuturesUnordered, StreamExt};
    let mut running = FuturesUnordered::new();
    let mut stopped = false;
    let mut failed = None;
    let mut width = 1;
    let mut received = Vec::new();
    let mut held = 0;
    loop {
        while !stopped && running.len() < width {
            let Some(number) = after.checked_add(1).and_then(NonZeroU64::new) else {
                failed = Some(StorageFailure::Protocol.with_source("log sequence exhausted"));
                stopped = true;
                break;
            };
            if reads.missed(kind, writer, number.get()) {
                stopped = true;
                break;
            }
            after = number.get();
            if input.contains_key(&(writer, number)) {
                width = limit.get();
                continue;
            }
            let path = path(store, kind, writer, number);
            running.push(async move { (number, storage.read_object(&path, None).await) });
        }
        let Some((number, result)) = running.next().await else {
            break;
        };
        match result {
            Ok(read) => {
                held += read.bytes.len();
                received.push(LogInput {
                    writer,
                    number,
                    observation: serde_json::to_vec(&read.metadata)?,
                    bytes: read.bytes.clone(),
                });
                input.insert((writer, number), read);
                width = limit.get();
                if held >= RETAIN_BATCH {
                    database
                        .keep_log_inputs(kind, std::mem::take(&mut received))
                        .await?;
                    held = 0;
                }
            }
            Err(error) => {
                stopped = true;
                if error.failure() == StorageFailure::NotFound {
                    reads.keep_miss(kind, writer, number.get());
                }
                if error.failure() != StorageFailure::NotFound && failed.is_none() {
                    failed = Some(error);
                }
            }
        }
    }
    // Completed reads are retained even when another request failed.
    if !received.is_empty() {
        database.keep_log_inputs(kind, received).await?;
    }
    match failed {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
pub(crate) mod tests;
