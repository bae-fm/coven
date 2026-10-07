//! Store-log replay, keys, file transfers and durable operations (§§9–18).

mod conflicts;
mod device_log_sync;
mod effects;
mod replay;
mod write_seal;

pub use replay::{replay, replay_entry};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

mod error;
mod report;
mod store_log_keys;
mod store_log_object;
mod store_log_sync;
pub use coven_database::DropReason;
pub use device_log_sync::DeviceLogSync;
pub use error::{SyncError, SyncFailure};
pub use report::{
    DamagedObject, DeviceActivity, DroppedEntry, ObjectCheckFailure, StoreLogChange, SyncReport,
    WaitingWrite,
};
pub use store_log_sync::StoreLogSync;

mod operation_types;
pub use operation_types::*;
mod operation_data;
mod operations;
pub use operations::Operations;
mod files;
pub use files::{
    DrainOutcome, EagerCacheFillStatus, FileRangeStream, FileReadError, FileStream, Files,
    PinProgress, QueuedUpload, RecordedUploadFailure, RowsPinnedLiveQuery, UploadFailure,
    UploadFailures, UploadPhase, UploadQueue, UploadsLiveQuery,
};
