//! Operation waits use their own prerequisites and persisted retry deadlines.
use crate::{
    sequence_sync::SequenceSync, PendingReason, PendingRecord, PendingSubject, Retry,
    SequenceStatus, SyncError,
};
use coven_crypto::StoreKeyring;
use coven_database::{OperationId, OperationRecord, RetrySubject};
use coven_merge::Audience;

impl SequenceSync {
    /// Make an operation's retry or undo due now, from its recorded progress.
    /// It cannot resurrect an invalidated action or bypass provider cooldown
    /// or the stuck-device rule (E6).
    pub async fn retry_pending_operation(
        &self,
        id: OperationId,
    ) -> Result<SequenceStatus, SyncError> {
        self.retry_operation(id).await?;
        self.pass().await
    }

    pub(crate) async fn retry_operation(&self, id: OperationId) -> Result<(), SyncError> {
        if self.database.operation(id).await?.is_none() {
            return Err(SyncError::NotPending(id));
        }
        if let Some(pending) = self.database.operation_pending::<PendingRecord>(id).await? {
            if pending.retry() == Retry::Never
                || (pending.retry() == Retry::AfterUpdate
                    && !self
                        .database
                        .pending_can_retry::<PendingRecord>(pending.subject.clone())
                        .await?)
            {
                return Err(SyncError::Pending(Box::new(pending)));
            }
            self.database.clear_pending(pending.subject).await?;
        }
        if self
            .database
            .retry_wait(RetrySubject::Operation(id))
            .await?
            .is_some()
        {
            self.database
                .clear_retry(RetrySubject::Operation(id))
                .await?;
        }
        self.waits.forget(RetrySubject::Operation(id));
        Ok(())
    }

    pub(crate) async fn operation_ready(
        &self,
        record: &OperationRecord,
        ring: &StoreKeyring,
    ) -> Result<bool, SyncError> {
        let subject = RetrySubject::Operation(record.id);
        let woken = self
            .waits
            .evidence_woken(record.id, |audience, key| Ok(holds(ring, audience, key)))?;
        if let Some(pending) = self
            .database
            .operation_pending::<PendingRecord>(record.id)
            .await?
        {
            if !self
                .database
                .pending_can_retry::<PendingRecord>(pending.subject)
                .await?
            {
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

    pub(crate) async fn record_operation_error(
        &self,
        id: OperationId,
        kind: crate::OperationKind,
        error: &SyncError,
        ring: &StoreKeyring,
    ) -> Result<(), SyncError> {
        self.waits
            .attempt_scope(id)
            .run(async {
                let pending = PendingRecord {
                    subject: PendingSubject::Operation { id, kind },
                    reason: self.error_reason(error),
                };
                let subject = RetrySubject::Operation(id);
                if pending.retry() == Retry::Automatic {
                    let held = crate::RetryWaits::lacks_key(&pending.reason)
                        .map(|(audience, key)| holds(ring, &audience, key));
                    self.waits.arm_evidence(id, &pending.reason, held, None);
                    let prior = self.database.retry_wait(subject).await?;
                    let wait = self.waits.next(prior.as_ref())?;
                    if let Err(error) = self
                        .database
                        .record_operation_retry(pending, Some(wait))
                        .await
                    {
                        self.waits.hold(subject);
                        return Err(error.into());
                    }
                    self.waits.arm(subject, &wait);
                } else {
                    self.database.record_operation_retry(pending, None).await?;
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
