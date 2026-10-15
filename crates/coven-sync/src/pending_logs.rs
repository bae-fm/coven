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
        .filter_map(|r| reports::publish(r, own))
        .collect();
    reports.sort_by(|a: &coven_format::pending::PendingReport, b| a.subject.cmp(&b.subject));
    Ok(reports)
}

/// A write's first unmet history or schema condition, named by the object or
/// update it waits for (§19.1).
/// A wait always names what it waits for.
const EMPTY_WAIT: coven_database::DbError = coven_database::DbError::Invariant {
    what: "a write wait names what it waits for",
};

pub(crate) fn write_wait(
    store: coven_foundation::id_source::StoreId,
    wait: &coven_database::WriteWait,
) -> Result<PendingReason, SyncError> {
    Ok(match wait {
        coven_database::WriteWait::Reload(audience) => {
            PendingReason::Waits(crate::Prerequisite::Reload(audience.clone()))
        }
        coven_database::WriteWait::Writes(missing) => {
            PendingReason::Waits(crate::Prerequisite::Object(crate::write_seal::path(
                store,
                *missing.first().ok_or(EMPTY_WAIT)?,
            )))
        }
        coven_database::WriteWait::Sources(missing) => {
            PendingReason::Waits(crate::Prerequisite::Object(crate::write_seal::path(
                store,
                missing.first().ok_or(EMPTY_WAIT)?.write,
            )))
        }
        coven_database::WriteWait::StoreLog(missing) => {
            PendingReason::Waits(crate::Prerequisite::Object(crate::store_log_object::path(
                store,
                *missing.first().ok_or(EMPTY_WAIT)?,
            )))
        }
        coven_database::WriteWait::SchemaVersion(version) => {
            PendingReason::UpdateRequired(crate::RequiredUpdate::AppSchema { version: *version })
        }
        coven_database::WriteWait::SchemaPublication(version) => {
            PendingReason::Waits(crate::Prerequisite::SchemaPublication {
                audience: coven_merge::Audience::Store,
                version: *version,
            })
        }
    })
}

#[path = "pending_reports.rs"]
pub(crate) mod reports;
