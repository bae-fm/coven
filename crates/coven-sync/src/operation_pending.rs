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
        if self.storage.is_some()
            && self
                .database
                .operation_pending::<PendingRecord>(record.id)
                .await?
                .is_some_and(|p| p.reason == PendingReason::NoStorage)
        {
            return Ok(true);
        }
        // Approval pauses the action, while its replaceable request is still observed.
        if matches!(
            Data::read(record)?,
            Data::Invite(crate::operation_data::InviteWork {
                state: crate::operation_data::InviteState::Waiting {
                    request: Some(_),
                    ..
                },
                ..
            })
        ) && self
            .database
            .operation_pending::<PendingRecord>(record.id)
            .await?
            .is_some_and(|p| p.reason == PendingReason::Paused)
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
        Ok(pending)
    }

    pub(crate) async fn record_operation_error(
        &self,
        record: &OperationRecord,
        error: &SyncError,
    ) -> Result<(), SyncError> {
        let reason = match error {
            SyncError::Pending(record) => record.reason.clone(),
            SyncError::NoStorage => PendingReason::NoStorage,
            SyncError::Stopped(SyncFailure::UpdateRequired(update)) => {
                PendingReason::UpdateRequired(*update)
            }
            SyncError::Stopped(SyncFailure::Removed) => PendingReason::Removed,
            SyncError::Storage(error) => match &self.storage {
                Some(storage) => PendingReason::Storage {
                    provider: storage.config().provider(),
                    failure: error.failure(),
                },
                None => PendingReason::NoStorage,
            },
            SyncError::KeyUnavailable(key) => {
                let local = self.database.local_store_log().await?;
                let audience = crate::store_log_keys::needed(&local.log)
                    .into_iter()
                    .find_map(|(audience, id)| (id == *key).then_some(audience))
                    .ok_or(coven_database::DbError::DamagedDatabase)?;
                PendingReason::KeyUnavailable {
                    audience,
                    key: *key,
                }
            }
            SyncError::Damaged(object) => PendingReason::Refused(object.failure.clone()),
            SyncError::Rejected(reason) => PendingReason::Dropped(reason.clone()),
            SyncError::ReloadPending(id) => self.reload_condition(*id).await?,
            SyncError::AccessRemains(shares) => {
                let data = Data::read(record)?;
                let Data::Invite(work) = data else {
                    return Err(coven_database::DbError::DamagedDatabase.into());
                };
                let access = work.access.member_access();
                let coven_format::MemberAccess::ProviderAccount(account) = access else {
                    return Err(coven_database::DbError::DamagedDatabase.into());
                };
                PendingReason::AccessRemains {
                    account,
                    shares: shares.clone(),
                }
            }
            SyncError::Failed { failure, .. } => PendingReason::Failed(*failure),
            SyncError::Database(coven_database::DbError::Snapshot(error)) => match error {
                coven_database::SnapshotError::MissingWrites { missing } => {
                    write_wait(&coven_database::WriteWait::Writes(missing.clone()))?
                }
                coven_database::SnapshotError::StoreLog { missing } => {
                    write_wait(&coven_database::WriteWait::StoreLog(missing.clone()))?
                }
                coven_database::SnapshotError::WriteWaiting(wait) => write_wait(wait)?,
                coven_database::SnapshotError::Schema { snapshot, database }
                    if snapshot > database =>
                {
                    PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: *snapshot })
                }
                _ => PendingReason::Failed(LocalFailure::Database),
            },
            SyncError::Database(_) => PendingReason::Failed(LocalFailure::Database),
            SyncError::Disk(_)
            | SyncError::SnapshotIo(_)
            | SyncError::SnapshotFile(_)
            | SyncError::Lock(_) => PendingReason::Failed(LocalFailure::Disk),
            SyncError::SecureStorage(_) => PendingReason::Failed(LocalFailure::KeyCustody),
            SyncError::Crypto(_) | SyncError::Key(_) => PendingReason::Failed(LocalFailure::Crypto),
            SyncError::Cleanup { operation, .. } => {
                return Box::pin(self.record_operation_error(record, operation)).await
            }
            _ => PendingReason::Failed(LocalFailure::Operation),
        };
        self.record_operation_condition(record, reason).await?;
        Ok(())
    }
}

fn reload_audience(data: &Data) -> Result<Audience, SyncError> {
    match data {
        Data::Snapshots(crate::snapshot_data::SnapshotTask {
            job: SnapshotJob::Reload { scope, .. },
            ..
        }) => match scope {
            ReloadScope::All => Ok(Audience::Store),
            ReloadScope::Changed(audiences) => audiences
                .first()
                .cloned()
                .ok_or_else(|| coven_database::DbError::DamagedDatabase.into()),
        },
        _ => Err(coven_database::DbError::DamagedDatabase.into()),
    }
}

fn write_wait(wait: &coven_database::WriteWait) -> Result<PendingReason, SyncError> {
    Ok(match wait {
        coven_database::WriteWait::Writes(missing) => {
            PendingReason::Waits(Prerequisite::Object(crate::write_seal::path(
                *missing
                    .first()
                    .ok_or(coven_database::DbError::DamagedDatabase)?,
            )))
        }
        coven_database::WriteWait::StoreLog(missing) => {
            PendingReason::Waits(Prerequisite::Object(crate::store_log_object::path(
                *missing
                    .first()
                    .ok_or(coven_database::DbError::DamagedDatabase)?,
            )))
        }
        coven_database::WriteWait::SchemaVersion(version) => {
            PendingReason::UpdateRequired(RequiredUpdate::AppSchema { version: *version })
        }
        coven_database::WriteWait::SchemaPublication(version) => {
            PendingReason::Waits(Prerequisite::SchemaPublication {
                audience: Audience::Store,
                version: *version,
            })
        }
    })
}
