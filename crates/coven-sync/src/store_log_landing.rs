//! First publication times observed before consuming store-log entries.

use crate::SyncError;
use coven_database::{DbError, StoreLog};
use coven_format::value::EntryId;
use coven_storage::Storage;
use std::{collections::BTreeMap, time::SystemTime};

/// Observe every landing witness after publishing a local entry, including on
/// a retry whose first publication preceded this call.
pub(crate) async fn observe_publication(
    store: coven_foundation::id_source::StoreId,
    storage: Option<&dyn Storage>,
    log: &StoreLog,
    entry: EntryId,
) -> Result<BTreeMap<EntryId, SystemTime>, SyncError> {
    let objects = storage
        .ok_or(SyncError::NoStorage)?
        .list(&coven_storage::ObjectPrefix::store_logs(store))
        .await?;
    let times = publication_times(log, &objects)?;
    if !times.contains_key(&entry) {
        return Err(coven_storage::StorageFailure::NotFound
            .with_source("published store-log entry is absent from its listing")
            .into());
    }
    Ok(times)
}

/// Landing witnesses come from the complete listing, not the kept replay or
/// decoded arrival order. Retained publications keep their original times.
pub(crate) fn publication_times(
    log: &StoreLog,
    objects: &[coven_storage::StoredObject],
) -> Result<BTreeMap<EntryId, SystemTime>, SyncError> {
    let mut times = BTreeMap::new();
    for stored in objects {
        let (device, number) = stored.path.store_log_position().ok_or(
            coven_storage::StorageFailure::Protocol
                .with_source("store-log listing returned another layout"),
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
