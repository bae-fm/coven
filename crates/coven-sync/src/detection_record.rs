//! The record of an S3 detection run (§11.6, §20.7): kept before the run's
//! first probe, and ended once a cleanup has confirmed its probes and its
//! unfinished multipart uploads gone. A run a crash cut leaves its record,
//! which hands the run to the next detection at that bucket, or
//! to the store's next connection there, to clean up after.

use crate::SyncError;
use coven_database::{
    operation_kind::S3_DETECTION, Database, NewOperation, OperationId, OperationRecord,
};
use coven_storage::StorageConfig;
use serde::{Deserialize, Serialize};

/// What a run's record holds.
#[derive(Serialize, Deserialize)]
struct DetectionWork {
    /// Where its probes go under the store's `.coven-check` prefix.
    run: uuid::Uuid,
    /// The location it probed, as detection was given it.
    location: StorageConfig,
}

/// One detection run's record, as committed.
pub struct DetectionRecord {
    id: OperationId,
    work: DetectionWork,
}

impl DetectionRecord {
    /// Record `run` at `location` before its first probe.
    pub async fn start(
        database: &Database,
        location: &StorageConfig,
        run: uuid::Uuid,
    ) -> Result<Self, SyncError> {
        let work = DetectionWork { run, location: location.clone() };
        let id = database
            .start_operation(NewOperation {
                kind: S3_DETECTION.into(),
                data: serde_json::to_vec(&work)?,
                started_by: S3_DETECTION.into(),
                pauses_writes: false,
            })
            .await?;
        Ok(Self { id, work })
    }

    /// The recorded runs whose probes sit in `location`'s bucket: the same
    /// bucket at the same endpoint and region, whatever profile reached them.
    pub async fn at(database: &Database, location: &StorageConfig) -> Result<Vec<Self>, SyncError> {
        let mut records = Vec::new();
        for record in database.operations().await? {
            if record.kind != S3_DETECTION {
                continue;
            }
            let work: DetectionWork = serde_json::from_slice(&record.data)?;
            if work.location.same_s3_bucket(location) {
                records.push(Self { id: record.id, work });
            }
        }
        Ok(records)
    }

    /// The run this records.
    pub fn run(&self) -> uuid::Uuid {
        self.work.run
    }

    /// End the record, once a cleanup has confirmed the run's probes and
    /// uploads gone.
    pub async fn end(&self, database: &Database) -> Result<(), SyncError> {
        Ok(database.finish_operation(self.id).await?)
    }

    /// The detection runs a replaced installation's journal still owes
    /// cleanup for, as new rows for its replacement's journal (§19.3).
    pub fn carried(records: &[OperationRecord]) -> Vec<NewOperation> {
        records
            .iter()
            .filter(|record| record.kind == S3_DETECTION)
            .map(|record| NewOperation {
                kind: S3_DETECTION.into(),
                data: record.data.clone(),
                started_by: S3_DETECTION.into(),
                pauses_writes: false,
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "detection_record_tests.rs"]
mod tests;
