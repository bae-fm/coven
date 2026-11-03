//! Operation waits use their own prerequisites and persisted retry deadlines.
use crate::{
    sequence_sync::SequenceSync, PendingReason, PendingRecord, PendingSubject, Retry, RetryWaits,
    SequenceState, SyncError,
};
use coven_crypto::StoreKeyring;
use coven_database::{Journal, OperationId, OperationRecord, RetrySubject};
use coven_merge::Audience;
use std::sync::Arc;

/// An operation's failed attempt, and who receives its error.
pub(crate) enum OperationFailure<'a> {
    /// Coven drives the operation and nothing awaits this attempt: the
    /// record keeps the error's text as its cause.
    Held(SyncError),
    /// The attempt's caller receives this error.
    Returned(&'a SyncError),
}

/// Make an operation's retry or undo due now, from its recorded progress,
/// as a step of a write (E6): its pending record and retry wait end in the
/// write's transaction. It cannot resurrect an invalidated action, or one
/// that waits for an update not installed yet, and it can't bypass a
/// provider cooldown or the stuck-device rule. Once the write commits, the
/// operation's wait ends in this session too, and a pass is asked for to
/// run it.
pub fn retry_pending_operation(
    sql: &coven_database::SqlContext<'_, '_>,
    waits: Arc<RetryWaits>,
    state: Arc<SequenceState>,
    id: OperationId,
) -> Result<(), SyncError> {
    sql.journal_step(|journal| clear_for_retry(journal, id))?;
    sql.after_commit(move || {
        waits.retried(id);
        state.request_pass();
    });
    Ok(())
}

/// End the operation's pending record and retry wait for a retry, unless
/// its record never retries, or only after an update not installed yet.
fn clear_for_retry(journal: &Journal<'_>, id: OperationId) -> Result<(), SyncError> {
    if journal.operation(id)?.is_none() {
        return Err(SyncError::NotPending(id));
    }
    if let Some(pending) = journal.operation_pending::<PendingRecord>(id)? {
        if pending.retry() == Retry::Never
            || (pending.retry() == Retry::AfterUpdate
                && !journal.pending_can_retry::<PendingRecord>(&pending.subject)?)
        {
            return Err(SyncError::Pending(Box::new(pending)));
        }
        journal.clear_pending(&pending.subject)?;
    }
    journal.clear_retry(RetrySubject::Operation(id))?;
    Ok(())
}

impl SequenceSync {
    /// Retry an operation and run a pass, as the app's retry does through a
    /// write.
    #[cfg(test)]
    pub(crate) async fn retry_pending_operation(
        &self,
        id: OperationId,
    ) -> Result<crate::SequenceStatus, SyncError> {
        self.retry_operation(id).await?;
        self.pass().await
    }

    /// Make an operation due now, in a write of its own, for a call that
    /// goes on to run it.
    pub(crate) async fn retry_operation(&self, id: OperationId) -> Result<(), SyncError> {
        self.database
            .write(move |sql| {
                sql.journal_step(|journal| clear_for_retry(journal, id))
                    .map_err(|error| coven_database::DbError::SyncStream(Box::new(error)))
            })
            .await?;
        self.waits.retried(id);
        Ok(())
    }

    pub(crate) async fn operation_ready(
        &self,
        record: &OperationRecord,
        ring: &StoreKeyring,
    ) -> Result<bool, SyncError> {
        let subject = RetrySubject::Operation(record.id);
        let woken =
            self.waits.evidence_woken(record.id, |audience, key| Ok(holds(ring, audience, key)))?;
        if let Some(pending) = self.database.operation_pending::<PendingRecord>(record.id).await? {
            if !self.database.pending_can_retry::<PendingRecord>(pending.subject).await? {
                self.waits.seen(subject);
                return Ok(false);
            }
        }
        let wait = self.database.retry_wait(subject).await?;
        Ok(self.waits.due(subject, wait.as_ref())
            || (woken && wait.is_some() && self.waits.cooldown().is_none()))
    }

    pub(crate) fn error_reason(&self, error: &SyncError) -> PendingReason {
        error.pending_reason(self.storage.config().provider())
    }

    /// Record an operation's failed attempt as its pending record and wait.
    /// The failure is logged when the operation's reason appears or changes,
    /// and the record keeps its error's text as its cause when no caller
    /// receives the error.
    pub(crate) async fn record_operation_error(
        &self,
        id: OperationId,
        kind: crate::OperationKind,
        failure: OperationFailure<'_>,
        ring: &StoreKeyring,
    ) -> Result<(), SyncError> {
        self.record_failure(id, kind, failure, ring, None).await
    }

    /// Record an invite's failed attempt, which read the invite at `step`,
    /// as `record_operation_error` does, while the invite is still at that
    /// step: the app's write deciding it, or its expiry, may have moved it on
    /// meanwhile, and the next pass then takes the step they wrote (§13.2).
    pub(crate) async fn record_invite_error(
        &self,
        id: OperationId,
        step: u32,
        failure: OperationFailure<'_>,
        ring: &StoreKeyring,
    ) -> Result<(), SyncError> {
        self.record_failure(id, crate::OperationKind::Invite, failure, ring, Some(step)).await
    }

    async fn record_failure(
        &self,
        id: OperationId,
        kind: crate::OperationKind,
        failure: OperationFailure<'_>,
        ring: &StoreKeyring,
        step: Option<u32>,
    ) -> Result<(), SyncError> {
        let subject = PendingSubject::Operation { id, kind };
        let (reason, error_source, cause): (_, &(dyn std::error::Error + 'static), _) =
            match &failure {
                OperationFailure::Held(error) => {
                    (self.error_reason(error), error.cause(), Some(error.cause().to_string()))
                }
                OperationFailure::Returned(error) => (self.error_reason(error), *error, None),
            };
        // Logged before any wait: the error is the attempt's, borrowed.
        self.waits.failures().failed(&subject, &reason, error_source);
        let save = |pending, wait| async move {
            match step {
                Some(step) => self.database.record_operation_retry_at(pending, wait, step).await,
                None => self.database.record_operation_retry(pending, wait).await.map(|()| true),
            }
        };
        self.waits
            .attempt_scope(id)
            .run(async {
                let pending = PendingRecord { subject, reason, cause };
                let subject = RetrySubject::Operation(id);
                if pending.retry() == Retry::Automatic {
                    let held = crate::RetryWaits::lacks_key(&pending.reason)
                        .map(|(audience, key)| holds(ring, &audience, key));
                    let reason = pending.reason.clone();
                    let prior = self.database.retry_wait(subject).await?;
                    let wait = self.waits.next(prior.as_ref())?;
                    match save(pending, Some(wait)).await {
                        Err(error) => {
                            self.waits.arm_evidence(id, &reason, held, None);
                            self.waits.hold(subject);
                            return Err(error.into());
                        }
                        // The operation has left the step this attempt read.
                        Ok(false) => return Ok(()),
                        Ok(true) => (),
                    }
                    self.waits.arm_evidence(id, &reason, held, None);
                    self.waits.arm(subject, &wait);
                } else {
                    if !save(pending, None).await? {
                        return Ok(());
                    }
                    self.waits.forget(subject);
                    self.waits.clear_evidence(id);
                }
                Ok(())
            })
            .await
    }
}
fn holds(
    ring: &StoreKeyring,
    audience: &Audience,
    key: coven_foundation::id_source::KeyId,
) -> bool {
    match audience {
        Audience::Store => ring.store_key_ids().any(|id| id == key),
        Audience::Circle(circle) => ring.circle_key_ids(*circle).any(|id| id == key),
    }
}

#[cfg(test)]
#[path = "sequence_retry_tests.rs"]
mod tests;
