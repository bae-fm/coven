//! First publication times observed before consuming store-log entries.

use crate::SyncError;
use coven_database::{DbError, StoreLog};
use coven_format::value::EntryId;
use coven_storage::Storage;
use std::{collections::BTreeMap, time::SystemTime};

/// Observe every landing witness after publishing a local entry, including on
/// a retry whose first publication preceded this call.
pub(crate) async fn observe_publication(
    database: &coven_database::Database,
    reads: &crate::pass_reads::PassReads,
    limit: std::num::NonZeroUsize,
    store: coven_foundation::id_source::StoreId,
    storage: Option<&dyn Storage>,
    log: &StoreLog,
    entry: EntryId,
) -> Result<BTreeMap<EntryId, SystemTime>, SyncError> {
    // Entries landing before this publication decide its lateness. Other
    // writers' key-copy observations from this catch-up still stand; this
    // device's own log gained the entry's prerequisite copies.
    reads.reset_discovery_of(coven_database::LogKind::Entry);
    reads.forget_misses(coven_database::LogKind::KeyCopy, entry.device);
    let storage = storage.ok_or(SyncError::NoStorage)?;
    let path = crate::store_log_object::path(store, entry);
    let published = storage.status(&path).await?.ok_or_else(|| {
        coven_storage::StorageFailure::NotFound
            .with_source("published store-log entry is absent from exact status")
    })?;
    let mut writers = crate::discovery::writers(database, reads, storage, store).await?;
    writers.insert(entry.device);
    let mut objects = crate::discovery::scan(
        database,
        reads,
        storage,
        store,
        coven_database::LogKind::Entry,
        &writers,
        limit,
    )
    .await?
    .into_iter()
    .map(|read| read.metadata)
    .collect();
    add_observation(&mut objects, published)?;
    publication_times(log, &objects)
}

/// Exact status and a retained read must identify the same immutable publication.
pub(crate) fn add_observation(
    objects: &mut Vec<coven_storage::StoredObject>,
    observed: coven_storage::StoredObject,
) -> Result<(), SyncError> {
    if let Some(listed) = objects.iter().find(|object| object.path == observed.path) {
        if listed.object_id != observed.object_id
            || listed.revision != observed.revision
            || listed.stored_at != observed.stored_at
            || listed.size != observed.size
        {
            let (device, number) = observed
                .path
                .store_log_position()
                .ok_or(coven_storage::StorageFailure::InvalidPath)?;
            return Err(DbError::StoreLogEntryChanged(EntryId {
                device,
                number: number.get(),
            })
            .into());
        }
    } else {
        objects.push(observed);
    }
    Ok(())
}

/// Landing witnesses come from received input, not kept replay or decoded arrival order. Retained publications keep their original times.
pub(crate) fn publication_times(
    log: &StoreLog,
    objects: &[coven_storage::StoredObject],
) -> Result<BTreeMap<EntryId, SystemTime>, SyncError> {
    let mut times = BTreeMap::new();
    for stored in objects {
        let (device, number) = stored.path.store_log_position().ok_or(
            coven_storage::StorageFailure::Protocol
                .with_source("store-log observation returned another layout"),
        )?;
        times.insert(
            EntryId {
                device,
                number: number.get(),
            },
            stored.stored_at,
        );
    }
    for applied in &log.entries {
        if times
            .insert(applied.entry.position, applied.stored_at)
            .is_some_and(|time| time != applied.stored_at)
        {
            return Err(DbError::StoreLogEntryChanged(applied.entry.position).into());
        }
    }
    Ok(times)
}

#[cfg(test)]
#[path = "store_log_landing_tests.rs"]
mod tests;
