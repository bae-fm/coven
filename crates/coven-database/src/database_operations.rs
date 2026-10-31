//! Operation transactions, borrowing the database owner's connections.

use super::Database;
use crate::{DbError, NewOperation, OperationId, OperationRecord, OperationUpdate};

impl Database {
    /// Read every unfinished operation, including permanently failed ones.
    pub async fn operations(&self) -> Result<Vec<OperationRecord>, DbError> {
        self.call(move |inner| inner.access.with_committed(crate::operation::read)).await
    }

    /// Read one committed journal row without loading unrelated operation data.
    /// Absence means the operation has finished or never existed.
    pub async fn operation(&self, id: OperationId) -> Result<Option<OperationRecord>, DbError> {
        self.call(move |inner| {
            inner.access.with_committed(|writer| crate::operation::find(writer, id))
        })
        .await
    }

    /// Read the earliest unfinished row of a sync-owned kind, including failures.
    pub async fn first_operation(&self, kind: &str) -> Result<Option<OperationRecord>, DbError> {
        let kind = kind.to_owned();
        self.call(move |inner| {
            inner.access.with_committed(|writer| crate::operation::first(writer, &kind))
        })
        .await
    }

    /// Commit the operation before its first step.
    pub async fn start_operation(&self, operation: NewOperation) -> Result<OperationId, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::operation::insert(db, &operation))
            })
        })
        .await
    }

    /// Reserve the path number of a snapshot attempt about to be prepared:
    /// the next of this device's snapshot sequence, never reused, which §19.3
    /// compares with its stored snapshots.
    pub async fn reserve_snapshot_number(&self) -> Result<std::num::NonZeroU64, DbError> {
        self.call(move |inner| {
            inner
                .access
                .with_writer(|writer| writer.transaction(crate::operation::reserve_snapshot))
        })
        .await
    }

    /// Commit a local step's state after an idempotent storage effect.
    pub async fn advance_operation(&self, update: OperationUpdate) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::operation::advance(db, &update))
            })
        })
        .await
    }

    /// Read the pending condition associated with a journal row.
    pub async fn operation_pending<P: crate::PendingValue>(
        &self,
        id: OperationId,
    ) -> Result<Option<P>, DbError> {
        self.call(move |inner| inner.access.with_writer(|db| crate::pending::operation(db, id)))
            .await
    }

    /// Clear a condition for an explicit retry; completed steps stay recorded.
    #[cfg(test)]
    pub async fn retry_operation(&self, id: OperationId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::pending::clear_operation(db, id))
            })
        })
        .await
    }

    /// Commit completed work and its first remaining condition in one transaction.
    #[cfg(test)]
    pub async fn advance_operation_pending<P: crate::PendingValue>(
        &self,
        update: OperationUpdate,
        pending: P,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                if pending.operation() != Some(update.id) {
                    return Err(DbError::Invariant {
                        what: "an operation's condition names its journal row",
                    });
                }
                writer.transaction(|db| {
                    crate::operation::advance(db, &update)?;
                    crate::pending::save(db, &pending)
                })
            })
        })
        .await
    }

    /// Retire a finished operation. The caller must publish every reserved entry
    /// before discarding; removing this row cannot retire the publication queue.
    pub async fn finish_operation(&self, id: OperationId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| writer.transaction(|db| finish(db, id).map(drop)))
        })
        .await
    }
}

/// Delete the row and its condition, returning how many rows were deleted.
fn finish(db: &crate::sqlite::DatabaseConnection, id: OperationId) -> Result<usize, DbError> {
    crate::pending::clear_operation(db, id)?;
    db.internal_execute("DELETE FROM _coven_operations WHERE id=?1", [id.0])
}
