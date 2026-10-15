//! A retired writer's drain completes with its write log (§6, SP step 6).

use super::DeviceLogSync;
use crate::SyncError;
use coven_merge::WriteId;
use std::collections::BTreeMap;

impl DeviceLogSync {
    /// Mark a drain once its write log reached a trusted miss after the
    /// qualifying sample; every write read before that miss is retained whole,
    /// since discovery retains each hit before it returns. A cause past that
    /// miss is a required gap: the drain stays pending (§6).
    pub(super) async fn finish_drain(
        &self,
        drain: coven_database::RetirementFinality,
        sample: coven_format::value::StorageTime,
        miss: u64,
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
        if self.database.mark_drained(drain, sample).await? {
            tracing::debug!(device = ?drain.device, "retired device drained");
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "write_drain_tests.rs"]
mod tests;
