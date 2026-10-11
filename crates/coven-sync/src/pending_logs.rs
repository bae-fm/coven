//! Log scheduling and D8 projection use the same persisted pending observations.
use crate::{PendingReason, PendingRecord, PendingSubject, SyncError};
use coven_database::{Database, LogObject, LogRefusal};
use coven_foundation::id_source::DeviceId;

impl From<LogObject> for PendingSubject {
    fn from(object: LogObject) -> Self {
        match object {
            LogObject::Write(id) => Self::Write(id),
            LogObject::Entry(id) => Self::Entry(id),
        }
    }
}
impl PendingRecord {
    pub(crate) fn log_refusal(record: LogRefusal, reported_by: DeviceId) -> Self {
        Self {
            subject: record.object.into(),
            reason: PendingReason::Refused(record.failure.into()),
            reported_by,
        }
    }
    pub(crate) fn as_log_refusal(&self) -> Option<LogRefusal> {
        let PendingReason::Refused(reason) = &self.reason else {
            return None;
        };
        let object = match self.subject {
            PendingSubject::Write(id) => LogObject::Write(id),
            PendingSubject::Entry(id) => LogObject::Entry(id),
            _ => return None,
        };
        Some(LogRefusal {
            object,
            failure: reason.into(),
        })
    }
}
pub(crate) async fn record_refusal(
    database: &Database,
    record: LogRefusal,
) -> Result<(), SyncError> {
    let own = database.sync_state(Vec::new()).await?.device;
    database
        .record_pending(PendingRecord::log_refusal(record, own))
        .await?;
    Ok(())
}
pub(crate) async fn local_refusals(database: &Database) -> Result<Vec<LogRefusal>, SyncError> {
    let own = database.sync_state(Vec::new()).await?.device;
    let mut refusals = Vec::new();
    for record in database.pending::<PendingRecord>().await? {
        if record.reported_by == own {
            if let Some(refusal) = record.as_log_refusal() {
                if !database
                    .pending_can_retry::<PendingRecord>(record.subject)
                    .await?
                {
                    refusals.push(refusal);
                }
            }
        }
    }
    Ok(refusals)
}
pub(crate) async fn required(
    database: &Database,
    refusal: LogRefusal,
) -> Result<SyncError, SyncError> {
    let own = database.sync_state(Vec::new()).await?.device;
    Ok(SyncError::Pending(Box::new(PendingRecord::log_refusal(
        refusal, own,
    ))))
}

pub(crate) async fn reports(
    database: &Database,
) -> Result<Vec<coven_format::pending::PendingReport>, SyncError> {
    let own = database.sync_state(Vec::new()).await?.device;
    let mut reports: Vec<_> = database
        .pending::<PendingRecord>()
        .await?
        .into_iter()
        .filter(|r| r.reported_by == own)
        .filter_map(|r| r.as_log_refusal().map(Into::into))
        .collect();
    reports.sort_by(|a: &coven_format::pending::PendingReport, b| a.subject.cmp(&b.subject));
    Ok(reports)
}
