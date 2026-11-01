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

    /// Commit a new operation and a step of another that names it, in one
    /// transaction: `update` builds the step from the new operation's id. A
    /// parent that journals a child this way never loses track of it, nor
    /// starts a second one, whenever it stops.
    pub async fn start_operation_advancing(
        &self,
        operation: NewOperation,
        update: impl FnOnce(OperationId) -> Result<OperationUpdate, DbError> + Send + 'static,
    ) -> Result<OperationId, DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    let id = crate::operation::insert(db, &operation)?;
                    crate::operation::advance(db, &update(id)?)?;
                    Ok(id)
                })
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

    /// Read the journal's rows of `kind` and commit the transitions `step`
    /// makes from them, in one transaction on the writer: no other write
    /// comes between the read and the transitions. `step` also returns what
    /// it concluded from the rows. A failure fails this handle with its
    /// cause, as losing a pending record does: the rows would go on showing
    /// the app a state their owner has left.
    pub async fn advance_operations<T: Send + 'static>(
        &self,
        kind: &str,
        step: impl FnOnce(&[OperationRecord]) -> Result<(Vec<OperationUpdate>, T), DbError>
            + Send
            + 'static,
    ) -> Result<T, DbError> {
        let kind = kind.to_owned();
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.state_transaction(|db| {
                    let (updates, concluded) = step(&crate::operation::of_kind(db, &kind)?)?;
                    for update in &updates {
                        crate::operation::advance(db, update)?;
                    }
                    Ok(concluded)
                })
            })
        })
        .await
    }

    /// Record member pairs this installation holds, or is about to keep in
    /// custody, before custody keeps them: applying a binding of its member
    /// to a pair not recorded stops it for good (§12.3).
    pub async fn hold_member_keys(
        &self,
        keys: Vec<coven_crypto::MemberPublicKeys>,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::operation::hold_keys(db, &keys))
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

    /// Retire a finished operation with its outcome, as sync encodes it,
    /// which `coven_operations` lists for an operation the app's write
    /// journaled, in the same transaction: an outcome nobody awaits is
    /// never lost (§11.4).
    pub async fn finish_operation_with_outcome(
        &self,
        id: OperationId,
        outcome: crate::OperationOutcome,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute(
                        "UPDATE _coven_app_operations SET outcome=?2 WHERE operation=?1",
                        (id.0, outcome.encode()),
                    )?;
                    crate::operation::finish(db, id).map(drop)
                })
            })
        })
        .await
    }

    /// End an operation another started, with its outcome, in the
    /// transaction that advances its parent past it: the parent's step and
    /// the child's end commit together, so no restart finds one without the
    /// other. An outcome `coven_operations` lists is written as
    /// `finish_operation_with_outcome` writes it. A parent that has moved on
    /// since, or a child already ended, fails with `OperationChanged`.
    pub async fn finish_operation_advancing(
        &self,
        child: OperationId,
        outcome: crate::OperationOutcome,
        parent: OperationUpdate,
    ) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    crate::operation::advance(db, &parent)?;
                    db.internal_execute(
                        "UPDATE _coven_app_operations SET outcome=?2 WHERE operation=?1",
                        (child.0, outcome.encode()),
                    )?;
                    if crate::operation::finish(db, child)? != 1 {
                        return Err(DbError::OperationChanged(child));
                    }
                    Ok(())
                })
            })
        })
        .await
    }

    /// End every operation the app's writes journaled that still runs, and
    /// every invite this device made, as the transaction applying a stop for
    /// good does, for a stop no slot applies: history shows another copy of
    /// this installation using its device id (§19.3). A failure fails this
    /// handle with its cause: they would otherwise run, listed, forever.
    pub async fn end_for_good(&self, stop: crate::DeviceStop) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.state_transaction(|db| crate::operation::end_for_good(db, stop))
            })
        })
        .await
    }

    /// The paths of objects whose time to go has passed by `now`, in seconds
    /// since the Unix epoch.
    pub async fn due_object_removals(&self, now: u64) -> Result<Vec<String>, DbError> {
        let now = i64::try_from(now).unwrap_or(i64::MAX);
        self.call(move |inner| {
            inner.access.with_committed(|db| {
                db.query(
                    "SELECT path FROM _coven_object_removals WHERE after<=?1 ORDER BY after",
                    [now],
                    |r| r.get(0),
                )
            })
        })
        .await
    }

    /// The object at `path` is gone from storage.
    pub async fn object_removed(&self, path: String) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| {
                    db.internal_execute("DELETE FROM _coven_object_removals WHERE path=?1", [&path])
                        .map(drop)
                })
            })
        })
        .await
    }

    /// Retire a finished operation. The caller must publish every reserved entry
    /// before discarding; removing this row cannot retire the publication queue.
    pub async fn finish_operation(&self, id: OperationId) -> Result<(), DbError> {
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db| crate::operation::finish(db, id).map(drop))
            })
        })
        .await
    }
}
