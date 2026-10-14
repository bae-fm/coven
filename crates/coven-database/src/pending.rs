//! Typed persistence for sync-owned pending values, without depending on providers.

use crate::{sqlite::DatabaseConnection, DbError, OperationId};
use coven_foundation::id_source::DeviceId;
use serde::{de::DeserializeOwned, Serialize};

/// Sync supplies its complete typed record; the database owns its atomic lifetime.
/// The associated subject is the stable key, independent of the reason.
pub trait PendingValue: Serialize + DeserializeOwned + Send + 'static {
    /// Report a local entry rejected by committed replay.
    fn dropped_entry(entry: crate::EntryId, reason: crate::DropReason) -> Self;
    /// Report an audience whose selected boundary has no committed data input.
    fn reload(audience: coven_merge::Audience, reporter: DeviceId) -> Self;
    /// Report an S3 key awaiting deletion on its journal operation (§13).
    fn access_key(
        operation: OperationId,
        reporter: DeviceId,
        key: crate::AccessKeyToDelete,
    ) -> Self;
    /// Stable identity, ordered as the app presents it.
    type Subject: Serialize + Ord;
    /// The work that cannot advance.
    fn subject(&self) -> &Self::Subject;
    /// The authenticated observer, including the local device.
    fn reported_by(&self) -> DeviceId;
    /// The journal row, if this record describes an operation.
    fn operation(&self) -> Option<OperationId>;
    /// Numbered log object whose progress clears this observation.
    fn log_object(&self) -> Option<crate::LogObject>;
    /// Whether automatic work can retry with these installed versions.
    fn can_retry(&self, recorded_coven_version: &str, schema_version: u32) -> bool;
}

const SELECT: &str =
    "SELECT subject,reporter,value,operation,object,coven_version FROM _coven_pending";

struct StoredRecord {
    subject: Vec<u8>,
    reporter: [u8; 8],
    value: Vec<u8>,
    operation: Option<i64>,
    object: Option<Vec<u8>>,
    coven_version: String,
}
impl StoredRecord {
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            subject: row.get(0)?,
            reporter: row.get(1)?,
            value: row.get(2)?,
            operation: row.get(3)?,
            object: row.get(4)?,
            coven_version: row.get(5)?,
        })
    }
    fn decode<P: PendingValue>(&self) -> Result<P, DbError> {
        let record: P = serde_json::from_slice(&self.value).map_err(DbError::PendingEncoding)?;
        if serde_json::to_vec(record.subject()).map_err(DbError::PendingEncoding)? != self.subject
            || record.reported_by().0.to_be_bytes() != self.reporter
            || record.operation().map(|id| id.0) != self.operation
            || record.log_object().map(object_key) != self.object
        {
            return Err(DbError::DamagedDatabase);
        }
        Ok(record)
    }
}

pub(crate) fn read<P: PendingValue>(db: &DatabaseConnection) -> Result<Vec<P>, DbError> {
    read_query(db, &format!("{SELECT} WHERE suppressed=0"))
}

pub(crate) fn retained<P: PendingValue>(db: &DatabaseConnection) -> Result<Vec<P>, DbError> {
    read_query(db, SELECT)
}

fn read_query<P: PendingValue>(db: &DatabaseConnection, sql: &str) -> Result<Vec<P>, DbError> {
    let mut records = db
        .query(sql, [], StoredRecord::read)?
        .iter()
        .map(StoredRecord::decode::<P>)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by(|a, b| (a.subject(), a.reported_by()).cmp(&(b.subject(), b.reported_by())));
    Ok(records)
}

pub(crate) fn save<P: PendingValue>(db: &DatabaseConnection, record: &P) -> Result<(), DbError> {
    db.pending_change(|| {
        let subject = serde_json::to_vec(record.subject()).map_err(DbError::PendingEncoding)?;
        let value = serde_json::to_vec(record).map_err(DbError::PendingEncoding)?;
        db.internal_execute("DELETE FROM _coven_pending_resets WHERE subject=?1 AND reporter=?2", (&subject, record.reported_by().0.to_be_bytes().as_slice()))?;
        db.internal_execute(
            "INSERT INTO _coven_pending(subject,reporter,value,operation,object,coven_version) VALUES(?1,?2,?3,?4,?5,?6)
             ON CONFLICT(subject,reporter) DO UPDATE SET value=excluded.value,operation=excluded.operation,object=excluded.object,coven_version=excluded.coven_version,suppressed=0
             WHERE value!=excluded.value OR coven_version!=excluded.coven_version OR suppressed!=0",
            (subject, record.reported_by().0.to_be_bytes().as_slice(), value, record.operation().map(|id| id.0), record.log_object().map(object_key), env!("CARGO_PKG_VERSION")),
        )?;
        Ok(())
    })
}

pub(crate) fn clear<S: Serialize>(
    db: &DatabaseConnection,
    subject: &S,
    reporter: DeviceId,
) -> Result<(), DbError> {
    db.pending_change(|| {
        db.internal_execute(
            "DELETE FROM _coven_pending WHERE subject=?1 AND reporter=?2",
            (
                serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?,
                reporter.0.to_be_bytes().as_slice(),
            ),
        )?;
        Ok(())
    })
}

pub(crate) fn clear_operation(db: &DatabaseConnection, id: OperationId) -> Result<(), DbError> {
    db.pending_change(|| {
        db.internal_execute("DELETE FROM _coven_pending WHERE operation=?1", [id.0])?;
        Ok(())
    })
}

pub(crate) fn operation<P: PendingValue>(
    db: &DatabaseConnection,
    id: OperationId,
) -> Result<Option<P>, DbError> {
    db.query(
        &format!("{SELECT} WHERE suppressed=0 AND operation=?1"),
        [id.0],
        StoredRecord::read,
    )?
    .pop()
    .map(|r| r.decode())
    .transpose()
}

pub(crate) fn retry<P: PendingValue>(
    db: &DatabaseConnection,
    subject: &P::Subject,
    reporter: DeviceId,
) -> Result<bool, DbError> {
    let record = db
        .query(
            &format!("{SELECT} WHERE suppressed=0 AND subject=?1 AND reporter=?2"),
            (
                serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?,
                reporter.0.to_be_bytes().as_slice(),
            ),
            StoredRecord::read,
        )?
        .pop();
    match record {
        None => Ok(true),
        Some(stored) => Ok(stored
            .decode::<P>()?
            .can_retry(&stored.coven_version, db.schema_version()?)),
    }
}

fn object_key(object: crate::LogObject) -> Vec<u8> {
    let mut key = vec![match object {
        crate::LogObject::Write(_) => 0,
        crate::LogObject::Entry(_) => 1,
    }];
    key.extend(object.device().0.to_be_bytes());
    key.extend(object.number().to_be_bytes());
    key
}

pub(crate) fn suppress(db: &DatabaseConnection, resets: &[crate::EntryId]) -> Result<(), DbError> {
    if resets.is_empty() {
        return Ok(());
    }
    db.pending_change(|| {
        for entry in resets {
            db.internal_execute(
                // A reset never hides an S3 key awaiting deletion (§13).
                "INSERT INTO _coven_pending_resets(subject,reporter,entry_device,entry_number)
                 SELECT p.subject,p.reporter,?1,?2 FROM _coven_pending AS p
                 WHERE NOT EXISTS(SELECT 1 FROM _coven_access_keys AS k WHERE k.operation=p.operation)
                 ON CONFLICT DO NOTHING",
                (entry.device.0.to_be_bytes().as_slice(), entry.number.to_be_bytes().as_slice()),
            )?;
        }
        refresh(db)
    })
}

pub(crate) fn refresh(db: &DatabaseConnection) -> Result<(), DbError> {
    db.pending_change(|| {
        db.internal_execute(
            "UPDATE _coven_pending AS p SET suppressed=EXISTS(
                SELECT 1 FROM _coven_pending_resets s
                JOIN _coven_boundaries b ON b.entry_device=s.entry_device AND b.entry_number=s.entry_number
                JOIN _coven_store_log l ON l.device=b.entry_device AND l.number=b.entry_number
                WHERE s.subject=p.subject AND s.reporter=p.reporter AND b.applied=1 AND l.outcome='kept')
             WHERE suppressed!=EXISTS(
                SELECT 1 FROM _coven_pending_resets s
                JOIN _coven_boundaries b ON b.entry_device=s.entry_device AND b.entry_number=s.entry_number
                JOIN _coven_store_log l ON l.device=b.entry_device AND l.number=b.entry_number
                WHERE s.subject=p.subject AND s.reporter=p.reporter AND b.applied=1 AND l.outcome='kept')",
            [],
        )?;
        Ok(())
    })
}

/// Kept progress removes only this installation's condition, in its transaction.
pub(crate) fn clear_log(db: &DatabaseConnection, object: crate::LogObject) -> Result<(), DbError> {
    let Some(device) = db.pending_device() else {
        return Ok(());
    };
    db.pending_change(|| {
        db.internal_execute(
            "DELETE FROM _coven_pending WHERE object=?1 AND reporter=?2",
            (object_key(object), device.0.to_be_bytes().as_slice()),
        )?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "pending_tests.rs"]
pub(crate) mod tests;
