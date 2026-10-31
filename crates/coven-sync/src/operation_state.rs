//! What an operation's journal row holds between its steps.

use crate::SyncError;
use coven_database::{OperationRecord, OperationUpdate};

/// The state an operation records after each step it completes. Every
/// operation advances the same way: one step past the row it read, guarded by
/// that row's step against a stale caller, carrying the state it reached.
pub(crate) trait OperationState: serde::Serialize {
    /// The transition from `record` to the next step, recording `self`.
    fn update(&self, record: &OperationRecord) -> Result<OperationUpdate, SyncError> {
        Ok(OperationUpdate {
            id: record.id,
            previous: record.last_step,
            last_step: record.last_step.checked_add(1).ok_or(SyncError::SequenceExhausted)?,
            data: serde_json::to_vec(self)?,
        })
    }
}
