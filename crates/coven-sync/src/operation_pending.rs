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
                kind: Data::read(record)?.kind(&record.started_by),
            })
            .await?)
    }

    /// Whether automatic work may run this operation now: its condition
    /// permits a retry, and its retry wait has elapsed or what it waits on
    /// changed since, outside any provider cooldown (§16.5, SP step 5; IO
    /// `WaitingEvidence`).
    pub(crate) async fn operation_ready(
        &self,
        record: &OperationRecord,
    ) -> Result<bool, SyncError> {
        let subject = coven_database::RetrySubject::Operation(record.id);
        // Evaluating the operation consumes the change that woke the worker.
        let woken = self.waits.evidence_woken(record.id, |audience, key| {
            Ok(crate::store_log_keys::holds(
                self.store_keys.read()?.as_ref(),
                audience,
                key,
            ))
        })?;
        if !self.operation_can_retry(record).await? {
            self.waits.seen(subject);
            return Ok(false);
        }
        let persisted = self.database.retry_wait(subject).await?;
        let due = self.waits.due(subject, persisted.as_ref());
        // Evidence ends only a persisted wait, never a cooldown or a wait
        // that could not be saved.
        Ok(due || woken && persisted.is_some() && self.waits.cooldown().is_none())
    }

    /// Whether the operation's recorded condition waits on a pending reload:
    /// on the reload itself, on a device-log write, or on a write-log
    /// catch-up. Only step 6 sends or receives writes and catches up on them,
    /// and a pending reload holds step 6 back (SP), so a raise waiting on its
    /// own captured prefix, such as its migration marker, or preparing after
    /// the pass's write-log catch-up (§15), waits on the reload too. A
    /// store-log catch-up runs before step 6, so a wait on one does not.
    pub(crate) async fn waits_on_reload(&self, id: OperationId) -> Result<bool, SyncError> {
        Ok(self
            .database
            .operation_pending::<PendingRecord>(id)
            .await?
            .is_some_and(|pending| match &pending.reason {
                PendingReason::Waits(Prerequisite::Reload(_) | Prerequisite::WriteLogCatchUp) => {
                    true
                }
                PendingReason::Waits(Prerequisite::Object(path)) => path.write_position().is_some(),
                _ => false,
            }))
    }

    /// Coven's own operation whose last attempt failed or replay dropped, if
    /// any, first one that never clears: the store is not synced until it
    /// succeeds (E5).
    pub(crate) async fn failing_work(&self) -> Result<Option<PendingRecord>, SyncError> {
        let mut failing = None;
        for record in self.database.operations().await? {
            let Some(pending) = self
                .database
                .operation_pending::<PendingRecord>(record.id)
                .await?
            else {
                continue;
            };
            if matches!(&pending.subject, PendingSubject::Operation { kind, .. } if kind.coven_owned())
                && matches!(
                    pending.reason,
                    PendingReason::Failed(_)
                        | PendingReason::Storage { .. }
                        | PendingReason::Dropped(_)
                )
            {
                if pending.retry() == Retry::Never {
                    return Ok(Some(pending));
                }
                failing.get_or_insert(pending);
            }
        }
        Ok(failing)
    }

    /// The storage connection changed: waits for storage are woken.
    pub(crate) fn connection_changed(&self) {
        self.waits.connection_changed();
    }

    /// Forget the waits of operations that no longer exist.
    pub(crate) fn retain_operation_waits(&self, live: &std::collections::BTreeSet<OperationId>) {
        self.waits.retain_operations(live);
    }

    /// When the earliest waiting operation's retry is due, or what one waits
    /// on changes.
    pub(crate) fn next_operation_wake(&self) -> Option<crate::retry_waits::Wake> {
        let timer = self
            .waits
            .next_wake(|subject| matches!(subject, coven_database::RetrySubject::Operation(_)));
        let evidence = self.waits.evidence_wakes();
        if evidence.is_empty() {
            return timer;
        }
        let wakes = evidence.into_iter().chain(timer);
        Some(Box::pin(futures_util::FutureExt::map(
            futures_util::future::select_all(wakes),
            |_| (),
        )))
    }

    /// After an unsuccessful attempt, persist the next retry wait before the
    /// operation waits, and watch what it waits on; a condition that waits
    /// for anything but time keeps no wait. A completed catch-up is the next
    /// pass's, so it is no backoff.
    pub(crate) async fn schedule_retry(
        &self,
        record: &OperationRecord,
        pending: &PendingRecord,
    ) -> Result<(), SyncError> {
        let subject = coven_database::RetrySubject::Operation(record.id);
        if pending.retry() == Retry::Automatic
            && !matches!(
                pending.reason,
                PendingReason::Waits(Prerequisite::StoreLogCatchUp | Prerequisite::WriteLogCatchUp)
            )
        {
            let held = match crate::RetryWaits::lacks_key(&pending.reason) {
                Some((audience, key)) => Some(crate::store_log_keys::holds(
                    self.store_keys.read()?.as_ref(),
                    &audience,
                    key,
                )),
                None => None,
            };
            // A reload wait names the reload operation: it advances or finishes.
            let reload = match pending.reason {
                PendingReason::Waits(Prerequisite::Reload(_)) => self.pending_reload().await?,
                _ => None,
            };
            self.waits
                .arm_evidence(record.id, &pending.reason, held, reload);
            let previous = self.database.retry_wait(subject).await?;
            self.waits.fail(subject, previous.as_ref()).await
        } else {
            self.waits.clear_evidence(record.id);
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
                kind: Data::read(record)?.kind(&record.started_by),
            },
            reason,
            reported_by: self.database.sync_state(Vec::new()).await?.device,
        })
    }

    /// Record why an attempt stopped, and its next wait. Its commits are the
    /// attempt's own.
    pub(crate) async fn record_operation_condition(
        &self,
        record: &OperationRecord,
        reason: PendingReason,
    ) -> Result<PendingRecord, SyncError> {
        self.waits
            .attempt_scope(record.id)
            .run(async {
                let pending = self.operation_condition(record, reason).await?;
                self.database.record_pending(pending.clone()).await?;
                self.schedule_retry(record, &pending).await?;
                Ok(pending)
            })
            .await
    }

    /// Record an attempt's failure and its next wait. Its commits are the
    /// attempt's own.
    pub(crate) async fn record_operation_error(
        &self,
        record: &OperationRecord,
        error: &SyncError,
    ) -> Result<(), SyncError> {
        self.waits
            .attempt_scope(record.id)
            .run(self.record_attempt_error(record, error))
            .await
    }

    async fn record_attempt_error(
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
                PendingReason::Waits(Prerequisite::StoreLogCatchUp)
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
                coven_database::SnapshotError::KeyUnavailable { audience, key } => {
                    PendingReason::KeyUnavailable {
                        audience: audience.clone(),
                        key: *key,
                    }
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
            ReloadScope::Changed(audiences) | ReloadScope::Returned(audiences) => {
                audiences.first().cloned().ok_or_else(|| {
                    coven_database::DbError::Invariant {
                        what: "a changed-audience reload names an audience",
                    }
                    .into()
                })
            }
        },
        _ => Err(coven_database::DbError::Invariant {
            what: "a reload's condition names a reload",
        }
        .into()),
    }
}
