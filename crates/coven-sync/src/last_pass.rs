//! How the last pass ended (E5), as `coven_sync_status.last_pass` keeps it:
//! one of the facts the sync state is computed from (§19.1).

use crate::{DeviceIdEvidence, PendingRecord, SyncFailure};
use coven_database::DbError;
use coven_storage::{StorageError, StorageFailure};
use std::time::SystemTime;

/// How the last pass that reached storage, or failed trying, ended (E5), as
/// `coven_sync_status.last_pass` holds it. It outlives each connection:
/// opening keeps it, and the running loop writes it as each pass ends. An
/// error is kept as its text, which names every cause below it with secrets
/// and URLs already cut, so a pass read after the process that ran it ended
/// still says why.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LastPass {
    /// The pass reached its observed end; other subjects may still be pending.
    Synced,
    /// The request preventing the pass received no usable provider response.
    Offline {
        /// The network failure, as the provider adapter classified it.
        failure: StorageFailure,
        /// The failure's text.
        error: String,
    },
    /// This slot invokes §5.4's stuck-device rule.
    Blocked {
        /// The slot that cannot apply.
        #[serde(with = "coven_database::slot_json")]
        slot: coven_merge::Slot,
        /// Why it cannot apply.
        reason: crate::PendingReason,
        /// The text of the error that blocked the slot: the cause the
        /// slot's pending record keeps. A device that opens already blocked
        /// has none until a pass of its own meets the error.
        error: Option<String>,
    },
    /// Storage refused the whole pass, or coven's own operation failed.
    Failed {
        /// What failed.
        failure: StatusFailure,
        /// The failure's text.
        error: String,
    },
}

/// What stopped a pass as a whole, as `LastPass` keeps it: a `SyncFailure`
/// without the error objects, whose text it holds beside it (E5).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StatusFailure {
    /// A schema or stored object format is newer than this implementation.
    UpdateRequired(crate::RequiredUpdate),
    /// This device or its member has been removed.
    Removed,
    /// Another copy uses this device's id; this copy sends nothing more (§19.3).
    DeviceIdInUse(DeviceIdEvidence),
    /// The provider refused or failed a request, as its adapter classified it.
    Storage(StorageFailure),
    /// The pass finished, but this work of coven's own failed its last
    /// attempt; it retries under backoff.
    Operation(PendingRecord),
    /// Any other failure.
    Other,
}

impl LastPass {
    /// How a pass that failed as a whole ended.
    pub fn failed(failure: impl Into<SyncFailure>) -> Self {
        let failure = failure.into();
        let error = failure.to_string();
        let failure = match failure {
            SyncFailure::UpdateRequired(update) => StatusFailure::UpdateRequired(update),
            SyncFailure::Removed => StatusFailure::Removed,
            SyncFailure::DeviceIdInUse(evidence) => StatusFailure::DeviceIdInUse(evidence),
            SyncFailure::Storage(storage) => StatusFailure::Storage(storage.failure()),
            SyncFailure::Operation(record) => StatusFailure::Operation(*record),
            SyncFailure::Other(_) => StatusFailure::Other,
        };
        Self::Failed { failure, error }
    }

    fn encoded(&self) -> Result<Vec<u8>, DbError> {
        serde_json::to_vec(self).map_err(DbError::StateEncoding)
    }

    /// Write this as how the last pass ended when sync failed outside a
    /// pass's end: starting, stopping, or a local failure or panic of the
    /// loop's, in a pass or outside one; and that nothing moves, so the
    /// state shows the failure (IO `failed_worker_shows`). A
    /// failure fails the store's handle with its cause
    /// (`DbError::StatePersistence`), so the caller stops the work that
    /// failed.
    pub async fn record(&self, database: &coven_database::Database) -> Result<(), DbError> {
        database.record_last_pass(self.encoded()?).await
    }

    /// Write this, a store-wide worker's panic, as how the last pass ended,
    /// halting sync until the store reopens: nothing moves, and no pass
    /// writes over it (IO `halt`). A failure fails the store's handle, as
    /// `record`'s does.
    pub async fn record_halt(&self, database: &coven_database::Database) -> Result<(), DbError> {
        database.record_halt(self.encoded()?).await
    }

    /// Write this as how the running pass ended, `now` by the store's clock,
    /// and that it moves no more. A synced pass's end becomes the last
    /// synced time, unless that is later. A failure fails the store's
    /// handle, as `record`'s does.
    pub async fn record_end(
        &self,
        database: &coven_database::Database,
        now: SystemTime,
    ) -> Result<(), DbError> {
        let finished = matches!(self, Self::Synced).then_some(now);
        database.record_pass_end(self.encoded()?, finished).await
    }

    /// Follow `coven_sync_status.last_pass` with a live query, as an app's
    /// SQL does: `None` until a pass ends on this device.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn follow(
        database: &coven_database::Database,
    ) -> coven_database::test_utils::Followed<Option<Self>, coven_database::DbError> {
        coven_database::test_utils::Followed::new(database.subscribe(|sql| {
            Ok(sql.query_row("SELECT last_pass FROM coven_sync_status", [], |row| row.get(0))?)
        }))
    }
}

/// Reads `coven_sync_status.last_pass`.
impl coven_database::types::FromSql for LastPass {
    fn column_result(
        value: coven_database::types::ValueRef<'_>,
    ) -> coven_database::types::FromSqlResult<Self> {
        serde_json::from_slice(value.as_blob()?)
            .map_err(|error| coven_database::types::FromSqlError::Other(Box::new(error)))
    }
}

/// Why a pass, or the connection attempt that begins one, failed: how it
/// ended (`LastPass`, E5).
#[derive(Clone, Debug)]
pub enum PassFailure {
    /// The request preventing the pass received no usable provider response.
    Offline {
        /// The network failure and its original cause.
        error: std::sync::Arc<StorageError>,
    },
    /// This slot invokes §5.4's stuck-device rule.
    Blocked {
        /// The slot that cannot apply.
        slot: coven_merge::Slot,
        /// Why it cannot apply.
        reason: crate::PendingReason,
        /// The text of the error that blocked the slot: the cause the slot's
        /// pending record keeps.
        error: Option<String>,
    },
    /// Storage refused the whole pass, or coven's own operation failed.
    Failed {
        /// The failure and its original cause.
        error: SyncFailure,
    },
}

impl From<PassFailure> for LastPass {
    fn from(failure: PassFailure) -> Self {
        match failure {
            PassFailure::Offline { error } => {
                Self::Offline { failure: error.failure(), error: error.to_string() }
            }
            PassFailure::Blocked { slot, reason, error } => Self::Blocked { slot, reason, error },
            PassFailure::Failed { error } => Self::failed(error),
        }
    }
}
