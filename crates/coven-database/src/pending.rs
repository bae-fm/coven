//! Typed persistence for sync-owned pending values, without depending on providers.

use crate::{sqlite::DatabaseConnection, DbError, OperationId};
use coven_foundation::id_source::{DeviceId, FileId};
use serde::{de::DeserializeOwned, Serialize};

/// Sync supplies its complete typed record; the database owns its atomic lifetime.
/// The associated subject is the stable key, independent of the reason.
pub trait PendingValue: Serialize + DeserializeOwned + Send + 'static {
    /// Report a batch consumed after a preceding removal or key replacement.
    fn writes_refused(
        slot: coven_merge::Slot,
        cutoff: coven_merge::Slot,
        cause: crate::WriteCutoff,
    ) -> Self;
    /// Report an audience whose selected boundary has no committed data input.
    fn reload(audience: coven_merge::Audience) -> Self;
    /// Report one account's unresolved intended sharing change.
    fn provider_access(operation: OperationId, account: String, grant: bool, owner: bool) -> Self;
    /// Stable identity, ordered as the app presents it.
    type Subject: Serialize + Ord;
    /// The work that cannot advance.
    fn subject(&self) -> &Self::Subject;
    /// The journal row, if this record describes an operation.
    fn operation(&self) -> Option<OperationId>;
    /// The file this record describes, by its uploader and id, if any: how
    /// coven's views join a record to the row whose file it is.
    fn file(&self) -> Option<(DeviceId, FileId)>;
    /// Sequence slot whose progress clears this observation.
    fn slot(&self) -> Option<coven_merge::Slot>;
    /// Whether a slot coverage including this object without applying it, as a
    /// loaded snapshot's do, also end this observation: true for a write that
    /// waits, false for a verdict on its bytes, which loading never clears (§19.1).
    fn ends_when_covered(&self) -> bool;
    /// Whether automatic work can retry with these installed versions.
    fn can_retry(&self, recorded_coven_version: &str, schema_version: u32) -> bool;
}

const SELECT: &str =
    "SELECT subject,value,operation,object,device,file,coven_version FROM _coven_pending";

struct StoredRecord {
    subject: Vec<u8>,
    value: Vec<u8>,
    operation: Option<i64>,
    object: Option<Vec<u8>>,
    file: Option<(Vec<u8>, Vec<u8>)>,
    coven_version: String,
}
impl StoredRecord {
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            subject: row.get(0)?,
            value: row.get(1)?,
            operation: row.get(2)?,
            object: row.get(3)?,
            file: match (row.get::<_, Option<Vec<u8>>>(4)?, row.get(5)?) {
                (Some(device), Some(file)) => Some((device, file)),
                (None, None) => None,
                _ => return Err(rusqlite::Error::InvalidQuery),
            },
            coven_version: row.get(6)?,
        })
    }
    fn decode<P: PendingValue>(&self) -> Result<P, DbError> {
        let record: P = serde_json::from_slice(&self.value).map_err(DbError::PendingEncoding)?;
        if serde_json::to_vec(record.subject()).map_err(DbError::PendingEncoding)? != self.subject
            || record.operation().map(|id| id.0) != self.operation
            || stored_object(&record) != self.object
            || stored_file(&record) != self.file
        {
            return Err(DbError::DamagedDatabase);
        }
        Ok(record)
    }
}

pub(crate) fn read<P: PendingValue>(db: &DatabaseConnection) -> Result<Vec<P>, DbError> {
    let mut records = db
        .query(SELECT, [], StoredRecord::read)?
        .iter()
        .map(StoredRecord::decode::<P>)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by(|a, b| a.subject().cmp(b.subject()));
    Ok(records)
}

pub(crate) fn save<P: PendingValue>(db: &DatabaseConnection, record: &P) -> Result<(), DbError> {
    db.pending_change(|| {
        let subject = serde_json::to_vec(record.subject()).map_err(DbError::PendingEncoding)?;
        let value = serde_json::to_vec(record).map_err(DbError::PendingEncoding)?;
        let (device, file) = stored_file(record).unzip();
        db.internal_execute(
            "INSERT INTO _coven_pending(subject,value,operation,object,device,file,coven_version) VALUES(?1,?2,?3,?4,?5,?6,?7)
             ON CONFLICT(subject) DO UPDATE SET value=excluded.value,operation=excluded.operation,object=excluded.object,device=excluded.device,file=excluded.file,coven_version=excluded.coven_version
             WHERE value!=excluded.value OR coven_version!=excluded.coven_version",
            (subject, value, record.operation().map(|id| id.0), stored_object(record), device, file, env!("CARGO_PKG_VERSION")),
        )?;
        Ok(())
    })
}

pub(crate) fn clear<S: Serialize>(db: &DatabaseConnection, subject: &S) -> Result<(), DbError> {
    clear_encoded(
        db,
        serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?,
    )
}

/// Clear the record of a subject already encoded for its key.
pub(crate) fn clear_encoded(db: &DatabaseConnection, subject: Vec<u8>) -> Result<(), DbError> {
    db.pending_change(|| {
        db.internal_execute("DELETE FROM _coven_pending WHERE subject=?1", [&subject])?;
        Ok(())
    })
}

/// An operation's retry wait lives only while its condition does: advancing,
/// finishing or an explicit retry ends both (§6.7).
pub(crate) fn clear_operation(db: &DatabaseConnection, id: OperationId) -> Result<(), DbError> {
    crate::retry::clear(db, crate::RetrySubject::Operation(id))?;
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
        &format!("{SELECT} WHERE operation=?1"),
        [id.0],
        StoredRecord::read,
    )?
    .pop()
    .map(|r| r.decode())
    .transpose()
}

/// Whether a subject has a local record.
pub(crate) fn recorded<S: Serialize>(
    db: &DatabaseConnection,
    subject: &S,
) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_pending WHERE subject=?1)",
        [serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?],
        |r| r.get(0),
    )
}

/// The current local record for a subject, if any.
pub(crate) fn find<P: PendingValue>(
    db: &DatabaseConnection,
    subject: &P::Subject,
) -> Result<Option<P>, DbError> {
    db.query(
        &format!("{SELECT} WHERE subject=?1"),
        [serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?],
        StoredRecord::read,
    )?
    .pop()
    .map(|stored| stored.decode())
    .transpose()
}

pub(crate) fn retry<P: PendingValue>(
    db: &DatabaseConnection,
    subject: &P::Subject,
) -> Result<bool, DbError> {
    let record = db
        .query(
            &format!("{SELECT} WHERE subject=?1"),
            [serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?],
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

fn object_key(slot: coven_merge::Slot, covered: bool) -> Vec<u8> {
    let mut key = vec![u8::from(covered)];
    key.extend(slot.0.to_be_bytes());
    key
}
const COVERED_SLOT: u8 = 1;

fn stored_file<P: PendingValue>(record: &P) -> Option<(Vec<u8>, Vec<u8>)> {
    record
        .file()
        .map(|(device, file)| (device.0.to_be_bytes().to_vec(), file.0.as_bytes().to_vec()))
}

fn stored_object<P: PendingValue>(record: &P) -> Option<Vec<u8>> {
    Some(object_key(record.slot()?, record.ends_when_covered()))
}

/// An applied reset clears data-related observations permanently (§19.4).
pub(crate) fn reset(db: &DatabaseConnection) -> Result<(), DbError> {
    db.pending_change(|| {
        db.internal_execute("DELETE FROM _coven_pending AS p WHERE NOT EXISTS(SELECT 1 FROM _coven_provider_access a WHERE a.operation=p.operation)",[])?;
        Ok(())
    })
}

/// Kept progress removes only this installation's condition, in its transaction.
pub(crate) fn clear_slot(
    db: &DatabaseConnection,
    object: coven_merge::Slot,
) -> Result<(), DbError> {
    db.pending_change(|| {
        for covered in [false, true] {
            db.internal_execute(
                "DELETE FROM _coven_pending WHERE object=?1",
                [object_key(object, covered)],
            )?;
        }
        Ok(())
    })
}

/// Snapshot coverage that passes a waiting write ends that
/// wait in the same transaction. Verdicts on a write's bytes remain (§19.1).
pub(crate) fn clear_covered_slots(db: &DatabaseConnection) -> Result<(), DbError> {
    let waiting = db.query(
        "SELECT object FROM _coven_pending WHERE object>=?1 AND object<?2",
        ([COVERED_SLOT].as_slice(), [COVERED_SLOT + 1].as_slice()),
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    if waiting.is_empty() {
        return Ok(());
    }
    let applied = crate::sequence::applied(db)?;
    db.pending_change(|| {
        for object in waiting {
            let slot = u64::from_be_bytes(object[1..].try_into().expect("eight-byte slot"));
            if slot <= applied {
                db.internal_execute("DELETE FROM _coven_pending WHERE object=?1", [object])?;
            }
        }
        Ok(())
    })
}

#[cfg(test)]
#[path = "pending_tests.rs"]
pub(crate) mod tests;
