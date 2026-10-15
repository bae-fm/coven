//! First publication times observed before consuming store-log entries.

use crate::SyncError;
use coven_database::StoreLog;
use coven_format::value::EntryId;
use coven_format::value::StorageTime;
use coven_storage::Storage;
use std::collections::BTreeMap;

/// Observe every landing witness after publishing a local entry, including on
/// a retry whose first publication preceded this call. Entries landing before
/// this publication decide its lateness; the catch-up that permitted it
/// retained them, so discovery does not restart (SP step 4). The entry's own
/// create reply is retained with them; without one, exact status supplies it.
pub(crate) async fn observe_publication(
    database: &coven_database::Database,
    reads: &crate::pass_reads::PassReads,
    store: coven_foundation::id_source::StoreId,
    storage: Option<&dyn Storage>,
    log: &StoreLog,
    entry: EntryId,
) -> Result<BTreeMap<EntryId, StorageTime>, SyncError> {
    // This device's own log gained the entry's prerequisite copies.
    reads.forget_misses(coven_database::LogKind::KeyCopy, entry.device);
    let path = crate::store_log_object::path(store, entry);
    let mut objects: Vec<_> =
        crate::discovery::retained(database, store, coven_database::LogKind::Entry)
            .await?
            .into_values()
            .map(|read| read.metadata)
            .collect();
    if !objects.iter().any(|object| object.path == path) {
        let storage = storage.ok_or(SyncError::NoStorage)?;
        let published = storage.status(&path).await?.ok_or_else(|| {
            coven_storage::StorageFailure::NotFound
                .with_source("published store-log entry is absent from exact status")
        })?;
        objects.push(published);
    }
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
            return Err(changed_publication(&observed.path));
        }
    } else {
        objects.push(observed);
    }
    Ok(())
}

/// Storage reported other metadata for one immutable publication: a provider
/// protocol failure, never a local database one (§3.1, §4).
fn changed_publication(path: &coven_storage::ObjectPath) -> SyncError {
    coven_storage::StorageFailure::Protocol
        .with_source(format!(
            "store-log entry {} changed its immutable publication metadata",
            path.as_str()
        ))
        .into()
}

/// Landing witnesses come from received input, not kept replay or decoded arrival order. Retained publications keep their original times.
pub(crate) fn publication_times(
    log: &StoreLog,
    objects: &[coven_storage::StoredObject],
) -> Result<BTreeMap<EntryId, StorageTime>, SyncError> {
    let mut listed = BTreeMap::new();
    for stored in objects {
        let (device, number) = stored.path.store_log_position().ok_or(
            coven_storage::StorageFailure::Protocol
                .with_source("store-log observation returned another layout"),
        )?;
        listed.insert(
            EntryId {
                device,
                number: number.get(),
            },
            stored,
        );
    }
    let mut times: BTreeMap<_, _> = listed
        .iter()
        .map(|(id, stored)| (*id, stored.stored_at))
        .collect();
    for applied in &log.entries {
        if let Some(stored) = listed.get(&applied.entry.position) {
            if stored.stored_at != applied.stored_at {
                return Err(changed_publication(&stored.path));
            }
        }
        times.insert(applied.entry.position, applied.stored_at);
    }
    Ok(times)
}

#[cfg(test)]
#[path = "store_log_landing_tests.rs"]
mod tests;
