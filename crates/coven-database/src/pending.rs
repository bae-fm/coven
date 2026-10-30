//! Typed persistence for sync-owned pending values, without depending on providers.

use crate::{sqlite::DatabaseConnection, DbError, OperationId};
use coven_foundation::id_source::{DeviceId, FileId};
use serde::{de::DeserializeOwned, Serialize};

/// Which of this device's work on a file's stored bytes a record is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileWork {
    /// Its upload of them, and its reads of their staged original.
    Upload,
    /// Its downloads of them, for the app's pins, whose failures alone are
    /// recorded. A download's record lasts only while a file over the bytes
    /// does (§19.1).
    Download,
}

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
    /// The stored bytes this record describes, by their uploader and id,
    /// and which work on them, if any: how coven's views join a record to
    /// the rows whose file it is.
    fn file(&self) -> Option<(FileWork, DeviceId, FileId)>;
    /// The move this record describes, if any: cancelling the move ends it.
    fn move_of(&self) -> Option<crate::MoveId>;
    /// The file whose pin coven keeps this record describes, if any: the
    /// record ends with the pin request.
    fn pin_of(&self) -> Option<FileId>;
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
    "SELECT subject,value,operation,object,work,device,file,coven_version,move,pin FROM _coven_pending";

struct StoredRecord {
    subject: Vec<u8>,
    value: Vec<u8>,
    operation: Option<i64>,
    object: Option<Vec<u8>>,
    file: Option<(String, Vec<u8>, Vec<u8>)>,
    coven_version: String,
    moved: Option<i64>,
    pin: Option<String>,
}
impl StoredRecord {
    fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            subject: row.get(0)?,
            value: row.get(1)?,
            operation: row.get(2)?,
            object: row.get(3)?,
            file: match (
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<Vec<u8>>>(5)?,
                row.get(6)?,
            ) {
                (Some(work), Some(device), Some(file)) => Some((work, device, file)),
                (None, None, None) => None,
                _ => return Err(rusqlite::Error::InvalidQuery),
            },
            coven_version: row.get(7)?,
            moved: row.get(8)?,
            pin: row.get(9)?,
        })
    }
    fn decode<P: PendingValue>(&self) -> Result<P, DbError> {
        let record: P = serde_json::from_slice(&self.value).map_err(DbError::PendingEncoding)?;
        if serde_json::to_vec(record.subject()).map_err(DbError::PendingEncoding)? != self.subject
            || record.operation().map(|id| id.0) != self.operation
            || record.move_of().map(|id| id.0) != self.moved
            || record.pin_of().map(|file| file.to_string()) != self.pin
            || stored_object(&record) != self.object
            || stored_file(&record)
                != self
                    .file
                    .as_ref()
                    .map(|(work, device, file)| (work.as_str(), device.clone(), file.clone()))
        {
            return Err(DbError::DamagedDatabase);
        }
        Ok(record)
    }
}

/// Every record, in the order the records arose, as `coven_pending` lists
/// them by position.
pub(crate) fn read<P: PendingValue>(db: &DatabaseConnection) -> Result<Vec<P>, DbError> {
    db.query(&format!("{SELECT} ORDER BY id"), [], StoredRecord::read)?
        .iter()
        .map(StoredRecord::decode::<P>)
        .collect()
}

pub(crate) fn save<P: PendingValue>(db: &DatabaseConnection, record: &P) -> Result<(), DbError> {
    db.pending_change(|| {
        let subject = serde_json::to_vec(record.subject()).map_err(DbError::PendingEncoding)?;
        let value = serde_json::to_vec(record).map_err(DbError::PendingEncoding)?;
        let (work, device, file) = match stored_file(record) {
            Some((work, device, file)) => (Some(work), Some(device), Some(file)),
            None => (None, None, None),
        };
        // A download of bytes no file over them names any more records
        // nothing: no pin or read can try them again.
        db.internal_execute(
            "INSERT INTO _coven_pending(subject,value,operation,object,work,device,file,coven_version,move,pin)
             SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10
             WHERE ?5 IS NOT 'download' OR EXISTS(SELECT 1 FROM _coven_stored_files AS s WHERE s.device=?6 AND s.object=?7)
             ON CONFLICT(subject) DO UPDATE SET value=excluded.value,operation=excluded.operation,object=excluded.object,work=excluded.work,device=excluded.device,file=excluded.file,coven_version=excluded.coven_version,move=excluded.move,pin=excluded.pin
             WHERE value!=excluded.value OR coven_version!=excluded.coven_version",
            rusqlite::params![subject, value, record.operation().map(|id| id.0), stored_object(record), work, device, file, env!("CARGO_PKG_VERSION"), record.move_of(), record.pin_of()],
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

/// The record's stored bytes and work on them, as `_coven_pending` holds them.
fn stored_file<P: PendingValue>(record: &P) -> Option<(&'static str, Vec<u8>, Vec<u8>)> {
    record.file().map(|(work, device, file)| {
        let work = match work {
            FileWork::Upload => "upload",
            FileWork::Download => "download",
        };
        (
            work,
            device.0.to_be_bytes().to_vec(),
            file.0.as_bytes().to_vec(),
        )
    })
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
