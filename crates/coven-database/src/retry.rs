//! Durable retry deadlines (§16.5, SP waiting).
//!
//! Before an automatic retry waits, its owner persists the not-before
//! wall-clock deadline `T` and the full wait `W` with the work: one row per
//! operation, queued upload, eager file download, the joiner's approval poll,
//! or the provider cooldown. A running session enforces the wait
//! monotonically; reopening arms `max(0, min(T - now, W))` from these rows.
//! An operation's or upload's row is deleted with that work, an operation's
//! also when its pending condition clears (it advanced, or the app retried
//! it). A download's row belongs to the file's pending record: it is written
//! with that record and deleted with it.

use crate::{sqlite::DatabaseConnection, DbError, OperationId};
use coven_foundation::id_source::{DeviceId, FileId};
use std::time::{Duration, SystemTime};

/// The work a persisted wait delays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RetrySubject {
    /// A journal operation.
    Operation(OperationId),
    /// A queued file upload, by its queue id.
    Upload(i64),
    /// An eager download of an uploaded file, by its fixed identity (§16.3).
    Download {
        /// The device that uploads the file.
        uploader: DeviceId,
        /// The file's identity.
        file: FileId,
    },
    /// The joining device's polling for its request's outcome (§12.2).
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

/// The row's one subject column and its value.
enum Key {
    Scope(&'static str),
    Operation(i64),
    Upload(i64),
    Download(Vec<u8>),
}

fn key(subject: RetrySubject) -> Key {
    match subject {
        RetrySubject::Operation(id) => Key::Operation(id.0),
        RetrySubject::Upload(id) => Key::Upload(id),
        RetrySubject::Download { uploader, file } => {
            let mut bytes = uploader.0.to_be_bytes().to_vec();
            bytes.extend_from_slice(file.0.as_bytes());
            Key::Download(bytes)
        }
        RetrySubject::Join => Key::Scope("join"),
        RetrySubject::Sync => Key::Scope("sync"),
        RetrySubject::Provider => Key::Scope("provider"),
    }
}

impl Key {
    fn value(&self) -> rusqlite::types::Value {
        match self {
            Self::Scope(scope) => rusqlite::types::Value::Text((*scope).into()),
            Self::Operation(id) | Self::Upload(id) => rusqlite::types::Value::Integer(*id),
            Self::Download(bytes) => rusqlite::types::Value::Blob(bytes.clone()),
        }
    }

    fn find(&self) -> &'static str {
        match self {
            Self::Scope(_) => "SELECT scope,operation,upload,download,failures,not_before,wait FROM _coven_retries WHERE scope=?1",
            Self::Operation(_) => "SELECT scope,operation,upload,download,failures,not_before,wait FROM _coven_retries WHERE operation=?1",
            Self::Upload(_) => "SELECT scope,operation,upload,download,failures,not_before,wait FROM _coven_retries WHERE upload=?1",
            Self::Download(_) => "SELECT scope,operation,upload,download,failures,not_before,wait FROM _coven_retries WHERE download=?1",
        }
    }

    fn insert(&self) -> &'static str {
        match self {
            Self::Scope(_) => {
                "INSERT INTO _coven_retries(scope,failures,not_before,wait) VALUES(?1,?2,?3,?4)"
            }
            Self::Operation(_) => {
                "INSERT INTO _coven_retries(operation,failures,not_before,wait) VALUES(?1,?2,?3,?4)"
            }
            Self::Upload(_) => {
                "INSERT INTO _coven_retries(upload,failures,not_before,wait) VALUES(?1,?2,?3,?4)"
            }
            Self::Download(_) => {
                "INSERT INTO _coven_retries(download,failures,not_before,wait) VALUES(?1,?2,?3,?4)"
            }
        }
    }

    fn delete(&self) -> &'static str {
        match self {
            Self::Scope(_) => "DELETE FROM _coven_retries WHERE scope=?1",
            Self::Operation(_) => "DELETE FROM _coven_retries WHERE operation=?1",
            Self::Upload(_) => "DELETE FROM _coven_retries WHERE upload=?1",
            Self::Download(_) => "DELETE FROM _coven_retries WHERE download=?1",
        }
    }
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok(RawRow {
        scope: row.get(0)?,
        operation: row.get(1)?,
        upload: row.get(2)?,
        download: row.get(3)?,
        failures: row.get(4)?,
        not_before: row.get(5)?,
        wait: row.get(6)?,
    })
}

struct RawRow {
    scope: Option<String>,
    operation: Option<i64>,
    upload: Option<i64>,
    download: Option<Vec<u8>>,
    failures: i64,
    not_before: Vec<u8>,
    wait: i64,
}

impl RawRow {
    fn decode(self) -> Result<(RetrySubject, RetryWait), DbError> {
        let subject = match (
            self.scope.as_deref(),
            self.operation,
            self.upload,
            self.download.as_deref(),
        ) {
            (None, Some(id), None, None) => RetrySubject::Operation(OperationId(id)),
            (None, None, Some(id), None) => RetrySubject::Upload(id),
            (None, None, None, Some(bytes)) => {
                let (uploader, file) = bytes.split_at_checked(8).ok_or(DbError::DamagedDatabase)?;
                RetrySubject::Download {
                    uploader: DeviceId(u64::from_be_bytes(
                        uploader.try_into().map_err(|_| DbError::DamagedDatabase)?,
                    )),
                    file: FileId(
                        uuid::Uuid::from_slice(file).map_err(|_| DbError::DamagedDatabase)?,
                    ),
                }
            }
            (Some("join"), None, None, None) => RetrySubject::Join,
            (Some("sync"), None, None, None) => RetrySubject::Sync,
            (Some("provider"), None, None, None) => RetrySubject::Provider,
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
        "SELECT scope,operation,upload,download,failures,not_before,wait FROM _coven_retries",
        [],
        read_row,
    )?
    .into_iter()
    .map(RawRow::decode)
    .collect()
}

/// The eager downloads' waits, by the download index: every download key is
/// a blob, and no blob sorts before the empty one.
pub(crate) fn read_downloads(
    db: &DatabaseConnection,
) -> Result<Vec<(RetrySubject, RetryWait)>, DbError> {
    db.query(
        "SELECT scope,operation,upload,download,failures,not_before,wait FROM _coven_retries WHERE download>=x''",
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
    let key = key(subject);
    db.query(key.find(), [key.value()], read_row)?
        .pop()
        .map(|row| row.decode().map(|(_, wait)| wait))
        .transpose()
}

/// `W` is stored in whole milliseconds, rounded up so a persisted wait never
/// shortens the one the session armed.
fn millis(wait: &RetryWait) -> Result<i64, DbError> {
    i64::try_from(wait.wait.as_nanos().div_ceil(1_000_000)).map_err(|_| DbError::Invariant {
        what: "a retry wait fits SQLite's integer",
    })
}

/// Replace the subject's wait. A download's wait is saved only with its
/// pending record (`save_download`); the table refuses one without it.
pub(crate) fn save(
    db: &DatabaseConnection,
    subject: RetrySubject,
    wait: &RetryWait,
) -> Result<(), DbError> {
    clear(db, subject)?;
    let key = key(subject);
    db.internal_execute(
        key.insert(),
        rusqlite::params![
            key.value(),
            i64::from(wait.failures),
            crate::user_file::encode_time(wait.not_before),
            millis(wait)?,
        ],
    )?;
    Ok(())
}

/// Replace a download's wait, bound to the file's pending record: deleting
/// that record deletes the wait (§16.5).
pub(crate) fn save_download(
    db: &DatabaseConnection,
    subject: RetrySubject,
    pending: &[u8],
    wait: &RetryWait,
) -> Result<(), DbError> {
    let key @ Key::Download(_) = key(subject) else {
        return Err(DbError::Invariant {
            what: "only a download's wait records its pending record",
        });
    };
    clear(db, subject)?;
    db.internal_execute(
        "INSERT INTO _coven_retries(download,pending_subject,failures,not_before,wait)
         VALUES(?1,?2,?3,?4,?5)",
        rusqlite::params![
            key.value(),
            pending,
            i64::from(wait.failures),
            crate::user_file::encode_time(wait.not_before),
            millis(wait)?,
        ],
    )?;
    Ok(())
}

pub(crate) fn clear(db: &DatabaseConnection, subject: RetrySubject) -> Result<(), DbError> {
    let key = key(subject);
    db.internal_execute(key.delete(), [key.value()])?;
    Ok(())
}

#[cfg(test)]
#[path = "retry_tests.rs"]
mod tests;
