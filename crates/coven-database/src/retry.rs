//! Durable retry deadlines (§6.7).
//!
//! Before an automatic retry waits, its owner persists the not-before
//! wall-clock deadline `T` and the full wait `W` with the work: one row per
//! operation, queued upload, pin coven keeps, the joiner's approval poll, or
//! the provider cooldown. A running session enforces the wait monotonically;
//! reopening arms `max(0, min(T - now, W))` from these rows. An operation's,
//! upload's or pin's row is deleted with that work, an operation's also when
//! its pending condition clears (it advanced, or the app retried it). The
//! app's own reads and pins keep none: only its next call tries again
//! (§17.7).

use crate::{sqlite::DatabaseConnection, DbError, OperationId};
use std::time::{Duration, SystemTime};

/// The work a persisted wait delays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RetrySubject {
    /// A journal operation.
    Operation(OperationId),
    /// A queued file upload, by its queue id, which is never another
    /// upload's.
    Upload(i64),
    /// A file a switch to the cloud asked this device to keep, which coven
    /// downloads whole itself (§17.8), by its request's id: a later request
    /// for the same file is another subject.
    Pin(i64),
    /// The joining device's polling for its request's outcome (§13.2).
    Join,
    /// A failed sequence pass.
    Sync,
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
impl RetryWait {
    /// A wait as a retry column set stores it: failures, `T` and `W` in
    /// milliseconds.
    pub(crate) fn stored(failures: i64, not_before: &[u8], wait: i64) -> Result<Self, DbError> {
        Ok(Self {
            failures: u32::try_from(failures).map_err(|_overflow| DbError::DamagedDatabase)?,
            not_before: crate::user_file::decode_time(not_before)?,
            wait: crate::stored_millis::stored_wait(wait)?,
        })
    }
}

/// The row's one subject column and its value.
enum Key {
    Scope(&'static str),
    Operation(i64),
    Upload(i64),
    Pin(i64),
}

fn key(subject: RetrySubject) -> Key {
    match subject {
        RetrySubject::Operation(id) => Key::Operation(id.0),
        RetrySubject::Upload(id) => Key::Upload(id),
        RetrySubject::Pin(id) => Key::Pin(id),
        RetrySubject::Join => Key::Scope("join"),
        RetrySubject::Sync => Key::Scope("sync"),
        RetrySubject::Provider => Key::Scope("provider"),
    }
}

/// The statements on `_coven_retries` for one subject column: read its
/// row, insert one, end its wait and delete it, each with the subject as
/// `?1`.
struct KeyStatements {
    find: &'static str,
    insert: &'static str,
    end: &'static str,
    delete: &'static str,
}

const SCOPE: KeyStatements = KeyStatements {
    find: "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries WHERE scope=?1",
    insert: "INSERT INTO _coven_retries(scope,failures,not_before,wait) VALUES(?1,?2,?3,?4)",
    end: "UPDATE _coven_retries SET not_before=?2,wait=0 WHERE scope=?1",
    delete: "DELETE FROM _coven_retries WHERE scope=?1",
};

const OPERATION: KeyStatements = KeyStatements {
    find: "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries WHERE operation=?1",
    insert: "INSERT INTO _coven_retries(operation,failures,not_before,wait) VALUES(?1,?2,?3,?4)",
    end: "UPDATE _coven_retries SET not_before=?2,wait=0 WHERE operation=?1",
    delete: "DELETE FROM _coven_retries WHERE operation=?1",
};

const UPLOAD: KeyStatements = KeyStatements {
    find: "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries WHERE upload=?1",
    insert: "INSERT INTO _coven_retries(upload,failures,not_before,wait) VALUES(?1,?2,?3,?4)",
    end: "UPDATE _coven_retries SET not_before=?2,wait=0 WHERE upload=?1",
    delete: "DELETE FROM _coven_retries WHERE upload=?1",
};

const PIN: KeyStatements = KeyStatements {
    find: "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries WHERE pin=?1",
    insert: "INSERT INTO _coven_retries(pin,failures,not_before,wait) VALUES(?1,?2,?3,?4)",
    end: "UPDATE _coven_retries SET not_before=?2,wait=0 WHERE pin=?1",
    delete: "DELETE FROM _coven_retries WHERE pin=?1",
};

impl Key {
    fn value(&self) -> rusqlite::types::Value {
        match self {
            Self::Scope(scope) => rusqlite::types::Value::Text((*scope).into()),
            Self::Operation(id) | Self::Upload(id) | Self::Pin(id) => {
                rusqlite::types::Value::Integer(*id)
            }
        }
    }

    fn statements(&self) -> KeyStatements {
        match self {
            Self::Scope(_) => SCOPE,
            Self::Operation(_) => OPERATION,
            Self::Upload(_) => UPLOAD,
            Self::Pin(_) => PIN,
        }
    }
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok(RawRow {
        scope: row.get(0)?,
        operation: row.get(1)?,
        upload: row.get(2)?,
        pin: row.get(3)?,
        failures: row.get(4)?,
        not_before: row.get(5)?,
        wait: row.get(6)?,
    })
}

struct RawRow {
    scope: Option<String>,
    operation: Option<i64>,
    upload: Option<i64>,
    pin: Option<i64>,
    failures: i64,
    not_before: Vec<u8>,
    wait: i64,
}

impl RawRow {
    fn decode(self) -> Result<(RetrySubject, RetryWait), DbError> {
        let subject = match (self.scope.as_deref(), self.operation, self.upload, self.pin) {
            (None, Some(id), None, None) => RetrySubject::Operation(OperationId(id)),
            (None, None, Some(id), None) => RetrySubject::Upload(id),
            (None, None, None, Some(id)) => RetrySubject::Pin(id),
            (Some("join"), None, None, None) => RetrySubject::Join,
            (Some("sync"), None, None, None) => RetrySubject::Sync,
            (Some("provider"), None, None, None) => RetrySubject::Provider,
            _ => return Err(DbError::DamagedDatabase),
        };
        Ok((subject, RetryWait::stored(self.failures, &self.not_before, self.wait)?))
    }
}

pub(crate) fn read_all(db: &DatabaseConnection) -> Result<Vec<(RetrySubject, RetryWait)>, DbError> {
    db.query(
        "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries",
        [],
        read_row,
    )?
    .into_iter()
    .map(RawRow::decode)
    .collect()
}

/// The waits of the journal operations, with the operation each delays.
pub(crate) fn operations(
    db: &DatabaseConnection,
) -> Result<Vec<(OperationId, RetryWait)>, DbError> {
    db.query(
        "SELECT scope,operation,upload,pin,failures,not_before,wait FROM _coven_retries
         WHERE operation>0",
        [],
        read_row,
    )?
    .into_iter()
    .map(|row| match row.decode()? {
        (RetrySubject::Operation(id), wait) => Ok((id, wait)),
        _ => Err(DbError::DamagedDatabase),
    })
    .collect()
}

pub(crate) fn find(
    db: &DatabaseConnection,
    subject: RetrySubject,
) -> Result<Option<RetryWait>, DbError> {
    let key = key(subject);
    db.query(key.statements().find, [key.value()], read_row)?
        .pop()
        .map(|row| row.decode().map(|(_, wait)| wait))
        .transpose()
}

/// `W` is stored in whole milliseconds, rounded up so a persisted wait never
/// shortens the one the session armed.
fn millis(wait: &RetryWait) -> Result<i64, DbError> {
    crate::stored_millis::millis(wait.wait, crate::stored_millis::Round::Up)
        .ok_or(DbError::Invariant { what: "a retry wait fits SQLite's integer" })
}

/// Replace the subject's wait.
pub(crate) fn save(
    db: &DatabaseConnection,
    subject: RetrySubject,
    wait: &RetryWait,
) -> Result<(), DbError> {
    clear(db, subject)?;
    let key = key(subject);
    db.internal_execute(
        key.statements().insert,
        rusqlite::params![
            key.value(),
            i64::from(wait.failures),
            crate::user_file::encode_time(wait.not_before),
            millis(wait)?,
        ],
    )?;
    Ok(())
}

/// Record a failed attempt's pending `record` with the subject's next wait,
/// or with none, in the caller's transaction.
pub(crate) fn save_failure<P: crate::PendingValue>(
    db: &DatabaseConnection,
    record: &P,
    subject: RetrySubject,
    wait: Option<&RetryWait>,
) -> Result<(), DbError> {
    crate::pending::save(db, record)?;
    set(db, subject, wait)
}

/// Replace the subject's wait, or end it when there is none.
pub(crate) fn set(
    db: &DatabaseConnection,
    subject: RetrySubject,
    wait: Option<&RetryWait>,
) -> Result<(), DbError> {
    match wait {
        Some(wait) => save(db, subject, wait),
        None => clear(db, subject),
    }
}

/// End the subject's wait at `now` in place: `T = now`, `W = 0`, keeping its
/// failure count.
pub(crate) fn end(
    db: &DatabaseConnection,
    subject: RetrySubject,
    now: SystemTime,
) -> Result<(), DbError> {
    let key = key(subject);
    db.internal_execute(
        key.statements().end,
        rusqlite::params![key.value(), crate::user_file::encode_time(now)],
    )?;
    Ok(())
}

pub(crate) fn clear(db: &DatabaseConnection, subject: RetrySubject) -> Result<(), DbError> {
    let key = key(subject);
    db.internal_execute(key.statements().delete, [key.value()])?;
    Ok(())
}

#[cfg(test)]
#[path = "retry_tests.rs"]
pub(crate) mod tests;
