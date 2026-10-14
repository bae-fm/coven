//! Durable retry deadlines (§16.5, SP waiting).
//!
//! Before an automatic retry waits, its owner persists the not-before
//! wall-clock deadline `T` and the full wait `W` with the work: one row per
//! operation, queued upload, the joiner's approval poll, or the provider
//! cooldown. A running session enforces the wait monotonically; reopening
//! arms `max(0, min(T - now, W))` from these rows. An operation's or upload's
//! row is deleted with that work, an operation's also when its pending
//! condition clears (it advanced, or the app retried it).

use crate::{sqlite::DatabaseConnection, DbError, OperationId};
use std::time::{Duration, SystemTime};

/// The work a persisted wait delays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RetrySubject {
    /// A journal operation.
    Operation(OperationId),
    /// A queued file upload, by its queue id.
    Upload(i64),
    /// The joining device's polling for its request's outcome (§12.2).
    Join,
    /// The provider/account cooldown a `Retry-After` requested; every
    /// consumer of the store's connection waits for it.
    Provider,
}

/// A persisted wait: `T` and `W`, and the consecutive unsuccessful attempts
/// that chose `W`, so reopening does not reset the backoff exponent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryWait {
    /// Consecutive unsuccessful attempts; zero for a provider cooldown.
    pub failures: u32,
    /// `T`: the wall-clock time before which no automatic attempt starts.
    pub not_before: SystemTime,
    /// `W`: the full wait chosen when `T` was persisted.
    pub wait: Duration,
}

fn columns(subject: RetrySubject) -> (Option<&'static str>, Option<i64>, Option<i64>) {
    match subject {
        RetrySubject::Operation(id) => (None, Some(id.0), None),
        RetrySubject::Upload(id) => (None, None, Some(id)),
        RetrySubject::Join => (Some("join"), None, None),
        RetrySubject::Provider => (Some("provider"), None, None),
    }
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok(RawRow {
        scope: row.get(0)?,
        operation: row.get(1)?,
        upload: row.get(2)?,
        failures: row.get(3)?,
        not_before: row.get(4)?,
        wait: row.get(5)?,
    })
}

struct RawRow {
    scope: Option<String>,
    operation: Option<i64>,
    upload: Option<i64>,
    failures: i64,
    not_before: Vec<u8>,
    wait: i64,
}

impl RawRow {
    fn decode(self) -> Result<(RetrySubject, RetryWait), DbError> {
        let subject = match (self.scope.as_deref(), self.operation, self.upload) {
            (None, Some(id), None) => RetrySubject::Operation(OperationId(id)),
            (None, None, Some(id)) => RetrySubject::Upload(id),
            (Some("join"), None, None) => RetrySubject::Join,
            (Some("provider"), None, None) => RetrySubject::Provider,
            _ => return Err(DbError::DamagedDatabase),
        };
        Ok((
            subject,
            RetryWait {
                failures: u32::try_from(self.failures).map_err(|_| DbError::DamagedDatabase)?,
                not_before: crate::user_file::decode_time(&self.not_before)?,
                wait: Duration::from_millis(
                    u64::try_from(self.wait).map_err(|_| DbError::DamagedDatabase)?,
                ),
            },
        ))
    }
}

pub(crate) fn read_all(db: &DatabaseConnection) -> Result<Vec<(RetrySubject, RetryWait)>, DbError> {
    db.query(
        "SELECT scope,operation,upload,failures,not_before,wait FROM _coven_retries",
        [],
        read_row,
    )?
    .into_iter()
    .map(RawRow::decode)
    .collect()
}

pub(crate) fn find(
    db: &DatabaseConnection,
    subject: RetrySubject,
) -> Result<Option<RetryWait>, DbError> {
    let row = match subject {
        RetrySubject::Operation(id) => db.query(
            "SELECT scope,operation,upload,failures,not_before,wait FROM _coven_retries WHERE operation=?1",
            [id.0],
            read_row,
        )?,
        RetrySubject::Upload(id) => db.query(
            "SELECT scope,operation,upload,failures,not_before,wait FROM _coven_retries WHERE upload=?1",
            [id],
            read_row,
        )?,
        RetrySubject::Join | RetrySubject::Provider => db.query(
            "SELECT scope,operation,upload,failures,not_before,wait FROM _coven_retries WHERE scope=?1",
            [columns(subject).0],
            read_row,
        )?,
    }
    .pop();
    row.map(|row| row.decode().map(|(_, wait)| wait))
        .transpose()
}

/// Replace the subject's wait. `W` is stored in whole milliseconds, rounded
/// up so a persisted wait never shortens the one the session armed.
pub(crate) fn save(
    db: &DatabaseConnection,
    subject: RetrySubject,
    wait: &RetryWait,
) -> Result<(), DbError> {
    let millis = i64::try_from(wait.wait.as_nanos().div_ceil(1_000_000))
        .map_err(|_| DbError::DamagedDatabase)?;
    clear(db, subject)?;
    let (scope, operation, upload) = columns(subject);
    db.internal_execute(
        "INSERT INTO _coven_retries(scope,operation,upload,failures,not_before,wait)
         VALUES(?1,?2,?3,?4,?5,?6)",
        rusqlite::params![
            scope,
            operation,
            upload,
            i64::from(wait.failures),
            crate::user_file::encode_time(wait.not_before),
            millis,
        ],
    )?;
    Ok(())
}

pub(crate) fn clear(db: &DatabaseConnection, subject: RetrySubject) -> Result<(), DbError> {
    match subject {
        RetrySubject::Operation(id) => {
            db.internal_execute("DELETE FROM _coven_retries WHERE operation=?1", [id.0])?
        }
        RetrySubject::Upload(id) => {
            db.internal_execute("DELETE FROM _coven_retries WHERE upload=?1", [id])?
        }
        RetrySubject::Join | RetrySubject::Provider => db.internal_execute(
            "DELETE FROM _coven_retries WHERE scope=?1",
            [columns(subject).0],
        )?,
    };
    Ok(())
}

#[cfg(test)]
#[path = "retry_tests.rs"]
mod tests;
