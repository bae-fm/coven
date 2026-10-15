//! Each writer's next write numbers, read by GET: a hit is the write itself,
//! retained whole before anything applies it, and a 404 is the miss (§6, SP
//! step 6).

use super::DeviceLogSync;
use crate::object_input::ObjectInput;
use crate::SyncError;
use coven_database::Database;
use coven_foundation::id_source::{DeviceId, StoreId};
use coven_merge::WriteId;
use coven_storage::{ObjectPath, Storage, StorageFailure, StoredObject};
use std::collections::BTreeMap;

/// One writer's write log as this discovery reads it.
pub(super) struct WriteLog {
    pub(super) writer: DeviceId,
    /// The last number read, contiguously from the applied position: its
    /// input is retained whole.
    after: u64,
    /// A refused number: nothing at or past it is read (§19.1).
    pub(super) stop: Option<u64>,
    /// Requests in the next window: one until a hit, then the transfer limit.
    width: usize,
    /// A drained writer is never probed; its retained input is all it has.
    drained: bool,
}

/// How one window of a log ended.
pub(super) enum End {
    /// The first number that storage does not hold.
    Miss(u64),
    /// A refused number, or the end of a drained writer's retained input.
    Stopped,
}

/// The contiguous writes one window read, and how the log ended if it did.
pub(super) struct Window {
    pub(super) found: Vec<(WriteId, StoredObject)>,
    pub(super) end: Option<End>,
}

impl WriteLog {
    pub(super) fn new(writer: DeviceId, applied: u64, stop: Option<u64>, drained: bool) -> Self {
        Self {
            writer,
            after: applied,
            stop,
            width: 1,
            drained,
        }
    }
}

/// Read one number: complete a partially retained input from its retained
/// prefix, or GET it whole. `None` is the miss; a failure is never one.
async fn fetch(
    database: &Database,
    storage: &dyn Storage,
    path: ObjectPath,
    partial: Option<ObjectInput>,
) -> Result<Option<StoredObject>, SyncError> {
    let input = match partial {
        // A prefix retained from a header read or an interrupted GET. Its
        // object gone at this read is the miss too, as for a whole GET.
        Some(input) => match input.receive(storage).await {
            Ok(()) => input,
            Err(SyncError::Storage(error)) if error.failure() == StorageFailure::NotFound => {
                return Ok(None)
            }
            Err(error) => return Err(error),
        },
        None => match storage.stream_object(&path, None).await {
            Ok(response) => {
                ObjectInput::receive_response(database, storage, &path, response).await?
            }
            Err(error) if error.failure() == StorageFailure::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        },
    };
    Ok(Some(input.object().clone()))
}

impl DeviceLogSync {
    /// Read the next window of `log`: numbers whose input is already retained
    /// whole cost no request, and at most the log's width of requests run in
    /// parallel. Nothing is scheduled past the first miss or the log's stop.
    /// Every hit is retained before this returns, including hits past a miss
    /// that were already in flight; a failed read fails the call after the
    /// others settle, keeping what they received.
    pub(super) async fn read_window(
        &self,
        storage: &dyn Storage,
        store: StoreId,
        log: &mut WriteLog,
    ) -> Result<Window, SyncError> {
        use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
        let mut running = FuturesUnordered::new();
        let mut found = BTreeMap::new();
        let mut stopped = false;
        let mut next = log.after;
        while running.len() < log.width {
            let number = next
                .checked_add(1)
                .ok_or_else(|| StorageFailure::Protocol.with_source("write numbers exhausted"))?;
            if log.stop.is_some_and(|stop| number >= stop) {
                stopped = true;
                break;
            }
            let path = crate::write_seal::path(
                store,
                WriteId {
                    device: log.writer,
                    number,
                },
            );
            let retained = ObjectInput::retained(&self.database, &path).await?;
            if retained.is_none() && log.drained {
                stopped = true;
                break;
            }
            next = number;
            match retained {
                Some(input) if input.received().await? => {
                    found.insert(number, input.object().clone());
                }
                partial => running.push(
                    fetch(&self.database, storage, path, partial).map(move |read| (number, read)),
                ),
            }
        }
        let mut miss = None::<u64>;
        let mut failed = None;
        while let Some((number, read)) = running.next().await {
            match read {
                Ok(Some(object)) => {
                    found.insert(number, object);
                }
                Ok(None) => miss = Some(miss.map_or(number, |known| known.min(number))),
                Err(error) => {
                    failed.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failed {
            return Err(error);
        }
        let found: Vec<_> = found
            .into_iter()
            .filter(|(number, _)| miss.is_none_or(|miss| *number < miss))
            .map(|(number, object)| {
                (
                    WriteId {
                        device: log.writer,
                        number,
                    },
                    object,
                )
            })
            .collect();
        if let Some((last, _)) = found.last() {
            log.after = last.number;
            log.width = self.download_limit.get();
        }
        let end = match miss {
            Some(miss) => Some(End::Miss(miss)),
            None => stopped.then_some(End::Stopped),
        };
        Ok(Window { found, end })
    }
}

#[cfg(test)]
#[path = "write_discovery_tests.rs"]
mod tests;
