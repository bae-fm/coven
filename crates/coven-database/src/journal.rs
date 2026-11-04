//! The journal as one of coven's steps in a write sees it (§4, §11): sync's
//! steps read and change the journal's rows, pending records and retry
//! waits in the write's own transaction, through this, and commit or roll
//! back with the app's SQL. Sync owns what the rows mean.

use super::FileWrite;
use crate::{
    DbError, NewOperation, OperationId, OperationRecord, OperationUpdate, RetrySubject, StoreState,
};

/// The journal inside one write, for sync's steps.
pub struct Journal<'write> {
    files: &'write FileWrite<'write>,
}

impl<'write> Journal<'write> {
    pub(crate) fn new(files: &'write FileWrite<'write>) -> Self {
        Self { files }
    }

    /// The operation's journal row, as this write sees it; `None` once it
    /// has finished, or if it never existed.
    pub fn operation(&self, id: OperationId) -> Result<Option<OperationRecord>, DbError> {
        crate::operation::find(self.files.database, id)
    }

    /// The journal's rows of `kind`, oldest first, as this write sees them.
    pub fn operations(&self, kind: &str) -> Result<Vec<OperationRecord>, DbError> {
        crate::operation::of_kind(self.files.database, kind)
    }

    /// Advance an operation one step past the row it read; its pending
    /// record and retry wait end with the step it left. A row that has
    /// moved on since fails with `OperationChanged`.
    pub fn advance_operation(&self, update: &OperationUpdate) -> Result<(), DbError> {
        crate::operation::advance(self.files.database, update)
    }

    /// Membership and audience state after the applied slots, as this
    /// write sees it.
    pub fn store_state(&self) -> Result<StoreState, DbError> {
        crate::store_state_tables::read(self.files.database)
    }

    /// The last slot whose effects this device has applied.
    pub fn applied_slot(&self) -> Result<u64, DbError> {
        crate::sequence::applied(self.files.database)
    }

    /// Journal an operation before its first step, which sync runs and
    /// ends on its own; `coven_operations` doesn't list it.
    pub fn start_operation(&self, operation: &NewOperation) -> Result<OperationId, DbError> {
        crate::operation::insert(self.files.database, operation)
    }

    /// Journal an operation the app's write asks for, of `kind` as sync
    /// names it, before its first step. It is listed in `coven_operations`
    /// from this write's commit until the app acknowledges its outcome.
    pub fn start_app_operation(
        &self,
        operation: &NewOperation,
        kind: &str,
    ) -> Result<OperationId, DbError> {
        crate::operation::insert_for_app(self.files.database, operation, kind)
    }

    /// End the app's record of a finished operation: it leaves
    /// `coven_operations`. One still running fails with `OperationRunning`;
    /// one this device doesn't list, never journaled by the app or
    /// acknowledged already, with `OperationUnknown`.
    pub(crate) fn acknowledge(&self, id: OperationId) -> Result<(), DbError> {
        crate::operation::acknowledge(self.files.database, id)
    }

    /// The pending record that names the operation, if any.
    pub fn operation_pending<P: crate::PendingValue>(
        &self,
        id: OperationId,
    ) -> Result<Option<P>, DbError> {
        crate::pending::operation(self.files.database, id)
    }

    /// Whether automatic work can retry the subject with the coven and
    /// schema versions installed now.
    pub fn pending_can_retry<P: crate::PendingValue>(
        &self,
        subject: &P::Subject,
    ) -> Result<bool, DbError> {
        crate::pending::retry::<P>(self.files.database, subject)
    }

    /// Clear the subject's pending record.
    pub fn clear_pending<S: serde::Serialize>(&self, subject: &S) -> Result<(), DbError> {
        crate::pending::clear(self.files.database, subject)
    }

    /// Clear the subject's retry wait.
    pub fn clear_retry(&self, subject: RetrySubject) -> Result<(), DbError> {
        crate::retry::clear(self.files.database, subject)
    }
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
