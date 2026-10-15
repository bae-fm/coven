//! First publication times observed before consuming store-log entries.

use crate::SyncError;
use coven_database::StoreLog;
use coven_format::value::EntryId;
use coven_format::value::StorageTime;
use coven_storage::Storage;
use std::collections::BTreeMap;

/// Observe every landing witness after publishing a local entry, including on
/// a retry whose first publication preceded this call. Every entry stored
/// more than 30 days before this publication decides its lateness, as it does
/// for every reader (§9). The latest complete catch-up received every entry
/// through `received_through` (its `T′`, SP step 2), so a publication no more
/// than 30 days after that is judged from the retained input without
/// restarting discovery (SP step 4). A later one, or one with no such
/// catch-up, reads every writer's log again. The entry's own create reply is
/// retained with them; without one, exact status supplies it.
pub(crate) async fn observe_publication(
    database: &coven_database::Database,
    reads: &crate::pass_reads::PassReads,
    limit: std::num::NonZeroUsize,
    store: coven_foundation::id_source::StoreId,
    storage: Option<&dyn Storage>,
    log: &StoreLog,
    entry: EntryId,
    received_through: Option<StorageTime>,
) -> Result<BTreeMap<EntryId, StorageTime>, SyncError> {
    // This device's own log gained the entry's prerequisite copies.
    reads.forget_misses(coven_database::LogKind::KeyCopy, entry.device);
    let path = crate::store_log_object::path(store, entry);
    let mut writers = database.known_log_writers().await?;
    writers.insert(entry.device);
    let mut objects: Vec<_> = crate::discovery::retained(
        database,
        store,
        coven_database::LogKind::Entry,
        &crate::discovery::past_applied(log, writers),
    )
    .await?
    .into_values()
    .map(|read| read.metadata)
    .collect();
    let published = match objects.iter().find(|object| object.path == path) {
        Some(object) => object.stored_at,
        None => {
            let storage = storage.ok_or(SyncError::NoStorage)?;
            let published = storage.status(&path).await?.ok_or_else(|| {
                coven_storage::StorageFailure::NotFound
                    .with_source("published store-log entry is absent from exact status")
            })?;
            let stored_at = published.stored_at;
            objects.push(published);
            stored_at
        }
    };
    // Only entries stored strictly before this cutoff can make it late.
    let received = match published.checked_sub(crate::replay::ENTRY_WINDOW) {
        Some(cutoff) => received_through.is_some_and(|through| cutoff <= through),
        None => true,
    };
    if !received {
        // This listing follows the publication, so §4's listing guarantee
        // makes every entry stored more than an hour before it visible.
        let storage = storage.ok_or(SyncError::NoStorage)?;
        reads.reset_discovery_of(coven_database::LogKind::Entry);
        let mut writers = crate::discovery::writers(database, reads, storage, store).await?;
        writers.insert(entry.device);
        for read in crate::discovery::scan(
            database,
            reads,
            storage,
            store,
            coven_database::LogKind::Entry,
            &crate::discovery::past_applied(log, writers),
            limit,
        )
        .await?
        {
            add_observation(&mut objects, read.metadata)?;
        }
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
