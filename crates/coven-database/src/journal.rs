//! The journal as one of coven's steps in a write sees it (§4, §11): sync's
//! steps read and change the journal's rows, pending records and retry
//! waits in the write's own transaction, through this, and commit or roll
//! back with the app's SQL. Sync owns what the rows mean.

use super::FileWrite;
use crate::{DbError, OperationId, OperationRecord, RetrySubject};

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

    /// Run `effect` once the write has committed, as the in-memory side of
    /// a step: nothing runs for a write that rolls back.
    pub fn after_commit(&self, effect: impl FnOnce() + 'static) {
        self.files.after_commit(Box::new(effect));
    }
}
