//! A retired writer's drain completes with its write log (§6, SP step 6).

use super::DeviceLogSync;
use crate::SyncError;
use coven_database::DbError;
use coven_merge::WriteId;
use std::collections::BTreeMap;

impl DeviceLogSync {
    /// A drained writer's writes past `after`, from retained observations, up
    /// to its first unobserved number or its refused write.
    pub(super) async fn retained_writes(
        &self,
        store: coven_foundation::id_source::StoreId,
        writer: coven_foundation::id_source::DeviceId,
        mut after: u64,
        stop: Option<u64>,
    ) -> Result<Vec<(WriteId, coven_storage::StoredObject)>, SyncError> {
        let mut found = Vec::new();
        while let Some(number) = after.checked_add(1) {
            if stop.is_some_and(|stop| number >= stop) {
                break;
            }
            let write = WriteId {
                device: writer,
                number,
            };
            let path = crate::write_seal::path(store, write);
            let Some(observation) = self
                .database
                .retained_observation(path.as_str().to_owned())
                .await?
            else {
                break;
            };
            let object: coven_storage::StoredObject =
                serde_json::from_slice(&observation).map_err(|_| DbError::DamagedDatabase)?;
            found.push((write, object));
            after = number;
        }
        Ok(found)
    }

    /// Mark a drain once its write log reached a trusted miss after the
    /// qualifying sample, with every write found before it retained. A cause
    /// past that miss is a required gap: the drain stays pending (§6).
    pub(super) async fn finish_drain(
        &self,
        drain: coven_database::RetirementFinality,
        sample: coven_format::value::StorageTime,
        miss: u64,
        objects: &BTreeMap<WriteId, coven_storage::StoredObject>,
        positions: &coven_format::value::WritePositions,
        waits: &BTreeMap<WriteId, coven_database::WriteWait>,
    ) -> Result<(), SyncError> {
        let required = waits.values().any(|wait| match wait {
            coven_database::WriteWait::Writes(needed) => needed
                .iter()
                .any(|cause| cause.device == drain.device && cause.number >= miss),
            _ => false,
        });
        if required {
            tracing::debug!(device = ?drain.device, "retired log has a required gap; drain waits");
            return Ok(());
        }
        for (write, object) in objects {
            if write.device == drain.device && !positions.covers(*write) {
                crate::object_input::ObjectInput::observe(&self.database, object).await?;
            }
        }
        if self.database.mark_drained(drain, sample).await? {
            tracing::debug!(device = ?drain.device, "retired device drained");
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "write_drain_tests.rs"]
mod tests;
