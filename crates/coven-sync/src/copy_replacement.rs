//! Replacing a key copy the provider damaged (§11): only its publisher can,
//! from the bytes it confirmed; readers skip a copy its writer's kept entry
//! marks replaced.

use super::*;
use crate::operation_data::{Data, EntryWork, Intent};
use crate::store_log_keys::{CopyReading, ObservedCopy};
use coven_foundation::id_source::DeviceId;
use std::num::NonZeroU64;

/// Every key copy a kept entry of its writer marks replaced, as (writer,
/// number). The entry names numbers only, in the log of the device holding
/// it, so no device can mark another writer's copy (§11).
pub(super) fn replaced_copies(log: &StoreLog) -> BTreeSet<(DeviceId, NonZeroU64)> {
    log.entries
        .iter()
        .filter(|replayed| {
            log.replay.entries.get(&replayed.entry.position)
                == Some(&coven_database::EntryOutcome::Kept)
        })
        .filter_map(|replayed| match replayed.entry.change {
            StoreChange::ReplaceKeyCopy { damaged, .. } => {
                Some((replayed.entry.position.device, NonZeroU64::new(damaged)?))
            }
            _ => None,
        })
        .collect()
}

impl CopyReading {
    /// Settle the undecodable copies this device can account for (§11). A
    /// copy a kept entry of its writer marks replaced is skipped: it is
    /// damage, and its replacement carries what it held
    /// (`CopyReplacement.marked_is_damage`). This device's own confirmed copy
    /// is observed as the exact bytes it confirmed, which are what the copy
    /// held (`CopyReplacement.own_record_matches`); those numbers are
    /// returned, since this device replaces them. Every other undecodable
    /// copy still leaves the reading incomplete.
    pub(super) async fn settle(
        &mut self,
        log: &StoreLog,
        database: &coven_database::Database,
        own: DeviceId,
    ) -> Result<Vec<NonZeroU64>, SyncError> {
        let replaced = replaced_copies(log);
        let mut own_damage = Vec::new();
        let mut unreadable = Vec::new();
        for (path, failure) in std::mem::take(&mut self.unreadable) {
            let Some((writer, number)) = path.key_copy_position() else {
                unreadable.push((path, failure));
                continue;
            };
            if replaced.contains(&(writer, number)) {
                continue;
            }
            if writer == own {
                if let Some(bytes) = database.confirmed_key_copy(number).await? {
                    let prefix = coven_format::sealed_key::SealedKey::decode(&bytes)?
                        .prefix()
                        .clone();
                    self.copies.push(ObservedCopy {
                        path,
                        prefix,
                        bytes,
                    });
                    own_damage.push(number);
                    continue;
                }
            }
            unreadable.push((path, failure));
        }
        self.unreadable = unreadable;
        self.copies
            .sort_by_key(|copy| copy.path.key_copy_position());
        Ok(own_damage)
    }
}

impl StoreLogSync {
    /// Start one durable replacement for each of this device's confirmed
    /// copies the provider damaged, unless one is already running (§11).
    pub(super) async fn schedule_copy_replacements(
        &self,
        damaged: &[NonZeroU64],
    ) -> Result<(), SyncError> {
        if damaged.is_empty() {
            return Ok(());
        }
        let mut running = BTreeSet::new();
        for record in self.database.operations().await? {
            if let Data::Entry(EntryWork {
                intent: Intent::ReplaceKeyCopy { damaged },
                ..
            }) = Data::read(&record)?
            {
                running.insert(damaged);
            }
        }
        for number in damaged.iter().filter(|number| !running.contains(number)) {
            tracing::warn!(number = number.get(), "replacing a damaged own key copy");
            self.database
                .start_operation(
                    Data::Entry(EntryWork {
                        intent: Intent::ReplaceKeyCopy { damaged: *number },
                        entry: None,
                    })
                    .new_operation("coven")?,
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "copy_replacement_tests.rs"]
mod tests;
