//! Translate step outcomes to their durable first unmet condition.
use super::StoreLogSync;
use crate::{
    operation_data::Data,
    snapshot_data::{ReloadScope, SnapshotJob},
    *,
};
use coven_database::OperationRecord;
use coven_merge::Audience;

impl StoreLogSync {
    pub(crate) async fn operation_can_retry(
        &self,
        record: &OperationRecord,
    ) -> Result<bool, SyncError> {
        if let Data::Invite(crate::operation_data::InviteWork {
            access: crate::operation_data::InviteGrant::Account { email },
            state: crate::operation_data::InviteState::Grant,
            ..
        }) = Data::read(record)?
        {
            if let Some(access) = self.database.provider_access(email).await? {
                if access.request.is_none() && access.invitation.is_some() {
                    return Ok(true);
                }
            }
        }
        if self.storage.is_some()
            && self
                .database
                .operation_pending::<PendingRecord>(record.id)
                .await?
                .is_some_and(|p| p.reason == PendingReason::NoStorage)
        {
            return Ok(true);
        }
        Ok(self
            .database
            .pending_can_retry::<PendingRecord>(PendingSubject::Operation {
                id: record.id,
                kind: Data::read(record)?.kind(),
            })
            .await?)
    }

    /// Whether automatic work may run this operation now: its condition
    /// permits a retry, and its retry wait has elapsed or its prerequisites
    /// changed since, outside any provider cooldown (§16.5, SP step 5).
    pub(crate) async fn operation_ready(
        &self,
        record: &OperationRecord,
    ) -> Result<bool, SyncError> {
        let subject = coven_database::RetrySubject::Operation(record.id);
        if !self.operation_can_retry(record).await? {
            self.waits.seen(subject);
            return Ok(false);
        }
        self.observe_prerequisites()?;
        let persisted = self.database.retry_wait(subject).await?;
        Ok(self.waits.due(subject, persisted.as_ref()))
    }

    /// A commit that can satisfy a prerequisite, such as an own upload or an
    /// applied entry, makes every operation then waiting due once.
    fn observe_prerequisites(&self) -> Result<(), SyncError> {
        if self
            .prerequisites
            .lock()
            .expect("prerequisite observation poisoned")
            .take_changed()?
        {
            self.waits.prerequisites_changed();
        }
        Ok(())
    }

    /// Committed work may satisfy waiting operations' prerequisites: each
    /// becomes due once, keeping its backoff (SP waiting).
    pub(crate) fn prerequisites_changed(&self) {
        self.waits.prerequisites_changed();
    }

    /// Forget the waits of operations that no longer exist.
    pub(crate) fn retain_operation_waits(&self, live: &std::collections::BTreeSet<OperationId>) {
        self.waits.retain_operations(live);
    }

    /// When the earliest waiting operation's retry is due.
    pub(crate) fn next_operation_wake(&self) -> Option<crate::retry_waits::Wake> {
        self.waits
            .next_wake(|subject| matches!(subject, coven_database::RetrySubject::Operation(_)))
    }

    /// After an unsuccessful attempt, persist the next retry wait before the
    /// operation waits; a condition that waits for anything but time keeps no
    /// wait. A completed catch-up is the next pass's, so it is no backoff.
    pub(crate) async fn schedule_retry(
        &self,
        record: &OperationRecord,
        pending: &PendingRecord,
    ) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Operation(record.id);
        if pending.retry() == Retry::Automatic
            && pending.reason != PendingReason::Waits(Prerequisite::CatchUp)
        {
            // Commits before this wait began are not evidence against it.
            self.observe_prerequisites()?;
            let previous = self.database.retry_wait(subject).await?;
            self.waits.fail(subject, previous.as_ref()).await
        } else {
            self.waits.clear(subject).await
        }
    }

    pub(super) async fn reload_condition(
        &self,
        id: OperationId,
    ) -> Result<PendingReason, SyncError> {
        let reload = self
            .database
            .operation(id)
            .await?
            .ok_or(coven_database::DbError::OperationChanged(id))?;
        Ok(PendingReason::Waits(Prerequisite::Reload(reload_audience(
            &Data::read(&reload)?,
        )?)))
    }

    pub(crate) async fn operation_condition(
        &self,
        record: &OperationRecord,
        reason: PendingReason,
    ) -> Result<PendingRecord, SyncError> {
        Ok(PendingRecord {
            subject: PendingSubject::Operation {
                id: record.id,
                kind: Data::read(record)?.kind(),
            },
            reason,
            reported_by: self.database.sync_state(Vec::new()).await?.device,
        })
    }

    pub(crate) async fn record_operation_condition(
        &self,
        record: &OperationRecord,
        reason: PendingReason,
    ) -> Result<PendingRecord, SyncError> {
        let pending = self.operation_condition(record, reason).await?;
        self.database.record_pending(pending.clone()).await?;
        self.schedule_retry(record, &pending).await?;
        Ok(pending)
    }

    pub(crate) async fn record_operation_error(
        &self,
        record: &OperationRecord,
        error: &SyncError,
    ) -> Result<(), SyncError> {
        let reason = self.pending_reason(error).await?;
        let data = Data::read(record)?;
        if let Data::ProviderAccess { account } = &data {
            if record.last_step == 1 {
                let pending = self.operation_condition(record, reason).await?;
                self.database
                    .fail_provider_request(
                        account.clone(),
                        data.update(record, 2)?,
                        pending.clone(),
                    )
                    .await?;
                return self.schedule_retry(record, &pending).await;
            }
        }
        self.record_operation_condition(record, reason).await?;
        Ok(())
    }

    /// The typed first unmet condition a failure leaves, for an operation's
    /// record or a retention path's (§18, E5).
    pub(crate) async fn pending_reason(
        &self,
        error: &SyncError,
    ) -> Result<PendingReason, SyncError> {
        Ok(match error {
            SyncError::Pending(record) => record.reason.clone(),
            SyncError::NoStorage => PendingReason::NoStorage,
            SyncError::UpdateRequired(update) => PendingReason::UpdateRequired(*update),
            SyncError::Removed => PendingReason::Removed,
            SyncError::MemberKeysReplaced { member } => {
                PendingReason::MemberKeysReplaced { member: *member }
            }
            SyncError::Storage(error)
                if error.failure() == coven_storage::StorageFailure::CatchUpRequired =>
            {
                PendingReason::Waits(Prerequisite::CatchUp)
            }
            SyncError::Storage(error) => match &self.storage {
                Some(storage) => PendingReason::Storage {
                    provider: storage.config().provider(),
                    failure: error.failure(),
                },
                None => PendingReason::NoStorage,
            },
            SyncError::KeyUnavailable(key) => {
                let local = self.database.local_store_log().await?;
                // A key without a received introduction is a store key: an
                // entry or object header sealed with a rotation this device
                // has not opened yet. Circle keys are named only by writes whose
                // recorded past, with the circle key's introduction, arrived.
                let audience = crate::key_introductions::introductions(&local.log)
                    .into_iter()
                    .find_map(|i| (i.key == *key).then_some(i.audience))
                    .unwrap_or(Audience::Store);
                PendingReason::KeyUnavailable {
                    audience,
                    key: *key,
                }
            }
            SyncError::Damaged(object) => PendingReason::Refused(object.failure.clone()),
            SyncError::Rejected(reason) => PendingReason::Dropped(reason.clone()),
            SyncError::ReloadPending(id) => self.reload_condition(*id).await?,
            SyncError::Database(coven_database::DbError::Snapshot(error)) => match error {
                coven_database::SnapshotError::MissingWrites { missing } => {
                    crate::pending_logs::write_wait(
                        self.directory.id(),
                        &coven_database::WriteWait::Writes(missing.clone()),
                    )?
                }
                coven_database::SnapshotError::StoreLog { missing } => {
                    crate::pending_logs::write_wait(
                        self.directory.id(),
                        &coven_database::WriteWait::StoreLog(missing.clone()),
                    )?
                }
                coven_database::SnapshotError::WriteWaiting(wait) => {
                    crate::pending_logs::write_wait(self.directory.id(), wait)?
                }
                coven_database::SnapshotError::Schema { snapshot, database }
                    if snapshot > database =>
                {
                    PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: *snapshot })
                }
                _ => PendingReason::Failed(LocalFailure::Database),
            },
            SyncError::Cleanup { operation, .. } => {
                return Box::pin(self.pending_reason(operation)).await
            }
            error => PendingReason::Failed(error.local_failure()),
        })
    }
}

fn reload_audience(data: &Data) -> Result<Audience, SyncError> {
    match data {
        Data::Snapshots(crate::snapshot_data::SnapshotTask {
            job: SnapshotJob::Reload { scope, .. },
            ..
        }) => match scope {
            ReloadScope::All => Ok(Audience::Store),
            ReloadScope::Changed(audiences) => audiences.first().cloned().ok_or_else(|| {
                coven_database::DbError::Invariant {
                    what: "a changed-audience reload names an audience",
                }
                .into()
            }),
        },
        _ => Err(coven_database::DbError::Invariant {
            what: "a reload's condition names a reload",
        }
        .into()),
    }
}
