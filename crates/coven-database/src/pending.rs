//! Typed persistence for sync-owned pending values, without depending on providers.

use crate::{sqlite::DatabaseConnection, DbError, OperationId};
use coven_foundation::id_source::DeviceId;
use serde::{de::DeserializeOwned, Serialize};

/// Sync supplies its complete typed record; the database owns its atomic lifetime.
/// The associated subject is the stable key, independent of the reason.
pub trait PendingValue: Serialize + DeserializeOwned + Send + 'static {
    /// Report a local entry rejected by committed replay.
    fn dropped_entry(entry: crate::EntryId, reason: crate::DropReason) -> Self;
    /// Report a received write consumed without effects, as `reporter` judged it.
    fn dropped_write(
        write: coven_merge::WriteId,
        reason: crate::DropReason,
        reporter: DeviceId,
    ) -> Self;
    /// Report an audience whose selected boundary has no committed data input.
    fn reload(audience: coven_merge::Audience, reporter: DeviceId) -> Self;
    /// Report an S3 key awaiting deletion on its journal operation (§13).
    fn access_key(
        operation: OperationId,
        reporter: DeviceId,
        key: crate::AccessKeyToDelete,
    ) -> Self;
    /// Report one account's unresolved intended sharing change.
    fn provider_access(
        operation: OperationId,
        reporter: DeviceId,
        account: String,
        grant: bool,
        owner: bool,
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
    /// Whether positions covering `log_object` without applying it, as a
    /// loaded snapshot's do, also end this observation: true for a write that
    /// waits, false for a verdict on its bytes, which loading never clears (§19.1).
    fn ends_when_covered(&self) -> bool;
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
            || stored_object(&record) != self.object
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
            (subject, record.reported_by().0.to_be_bytes().as_slice(), value, record.operation().map(|id| id.0), stored_object(record), env!("CARGO_PKG_VERSION")),
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

/// An operation's retry wait lives only while its condition does: advancing,
/// finishing or an explicit retry ends both (§16.5).
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
        &format!("{SELECT} WHERE suppressed=0 AND operation=?1"),
        [id.0],
        StoredRecord::read,
    )?
    .pop()
    .map(|r| r.decode())
    .transpose()
}

/// Whether one reporter keeps any record, current or suppressed, for a subject.
pub(crate) fn recorded<S: Serialize>(
    db: &DatabaseConnection,
    subject: &S,
    reporter: DeviceId,
) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_pending WHERE subject=?1 AND reporter=?2)",
        (
            serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?,
            reporter.0.to_be_bytes().as_slice(),
        ),
        |r| r.get(0),
    )
}

/// The current record one reporter keeps for one subject, if any.
pub(crate) fn find<P: PendingValue>(
    db: &DatabaseConnection,
    subject: &P::Subject,
    reporter: DeviceId,
) -> Result<Option<P>, DbError> {
    db.query(
        &format!("{SELECT} WHERE suppressed=0 AND subject=?1 AND reporter=?2"),
        (
            serde_json::to_vec(subject).map_err(DbError::PendingEncoding)?,
            reporter.0.to_be_bytes().as_slice(),
        ),
        StoredRecord::read,
    )?
    .pop()
    .map(|stored| stored.decode())
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

/// The indexed key of a record's log object. Its first byte also says which
/// progress ends it: applying the object, or any position covering a write.
fn object_key(object: crate::LogObject, covered: bool) -> Vec<u8> {
    let mut key = vec![match object {
        crate::LogObject::Write(_) if covered => COVERED_WRITE,
        crate::LogObject::Write(_) => 0,
        crate::LogObject::Entry(_) => 1,
    }];
    key.extend(object.device().0.to_be_bytes());
    key.extend(object.number().to_be_bytes());
    key
}
const COVERED_WRITE: u8 = 2;

fn stored_object<P: PendingValue>(record: &P) -> Option<Vec<u8>> {
    let object = record.log_object()?;
    Some(object_key(
        object,
        matches!(object, crate::LogObject::Write(_)) && record.ends_when_covered(),
    ))
}

pub(crate) fn suppress(db: &DatabaseConnection, resets: &[crate::EntryId]) -> Result<(), DbError> {
    if resets.is_empty() {
        return Ok(());
    }
    db.pending_change(|| {
        for entry in resets {
            db.internal_execute(
                // A data reset never hides unresolved provider access (§13).
                "INSERT INTO _coven_pending_resets(subject,reporter,entry_device,entry_number)
                 SELECT p.subject,p.reporter,?1,?2 FROM _coven_pending AS p
                 WHERE NOT EXISTS(SELECT 1 FROM _coven_access_keys AS k WHERE k.operation=p.operation)
                   AND NOT EXISTS(SELECT 1 FROM _coven_provider_access AS a WHERE a.operation=p.operation)
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
        for covered in [false, true] {
            db.internal_execute(
                "DELETE FROM _coven_pending WHERE object=?1 AND reporter=?2",
                (
                    object_key(object, covered),
                    device.0.to_be_bytes().as_slice(),
                ),
            )?;
        }
        Ok(())
    })
}

/// Positions that jump past a waiting write, as a snapshot load's do, end that
/// wait in the same transaction. Verdicts on a write's bytes remain (§19.1).
pub(crate) fn clear_covered_writes(db: &DatabaseConnection) -> Result<(), DbError> {
    let Some(device) = db.pending_device() else {
        return Ok(());
    };
    let reporter = device.0.to_be_bytes();
    let waiting = db.query(
        "SELECT object FROM _coven_pending WHERE object>=?1 AND object<?2 AND reporter=?3",
        (
            [COVERED_WRITE].as_slice(),
            [COVERED_WRITE + 1].as_slice(),
            reporter.as_slice(),
        ),
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    if waiting.is_empty() {
        return Ok(());
    }
    let positions = crate::download::positions(db)?;
    db.pending_change(|| {
        for object in waiting {
            let (device, number) = object[1..].split_at(8);
            let write = coven_merge::WriteId {
                device: DeviceId(u64::from_be_bytes(device.try_into().expect("8 bytes"))),
                number: u64::from_be_bytes(number.try_into().expect("8 bytes")),
            };
            if positions.covers(write) {
                db.internal_execute(
                    "DELETE FROM _coven_pending WHERE object=?1 AND reporter=?2",
                    (object, reporter.as_slice()),
                )?;
            }
        }
        Ok(())
    })
}

#[cfg(test)]
#[path = "pending_tests.rs"]
pub(crate) mod tests;

/// Prepare a set replacement before opening a durable transaction. Equal sets
/// have no writes; unchanged records keep their suppression and retry evidence.
pub(crate) struct Replacement<P: PendingValue> {
    removed: Vec<P>,
    changed: Vec<P>,
}
impl<P: PendingValue> Replacement<P> {
    pub(crate) fn new(previous: Vec<P>, next: Vec<P>) -> Result<Self, DbError> {
        let mut identities = std::collections::BTreeSet::new();
        for record in &next {
            if !identities.insert((record.subject(), record.reported_by())) {
                return Err(DbError::DamagedDatabase);
            }
        }
        let values = previous
            .iter()
            .map(|r| serde_json::to_vec(r).map_err(DbError::PendingEncoding))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        let removed = previous
            .into_iter()
            .filter(|r| !identities.contains(&(r.subject(), r.reported_by())))
            .collect();
        let mut changed = Vec::new();
        for record in next {
            if !values.contains(&serde_json::to_vec(&record).map_err(DbError::PendingEncoding)?) {
                changed.push(record);
            }
        }
        Ok(Self { removed, changed })
    }
    pub(crate) fn reports(
        db: &DatabaseConnection,
        own: DeviceId,
        next: Vec<P>,
    ) -> Result<Self, DbError> {
        if next
            .iter()
            .any(|r| r.reported_by() == own || r.operation().is_some())
        {
            return Err(DbError::DamagedDatabase);
        }
        Self::new(
            retained::<P>(db)?
                .into_iter()
                .filter(|r| r.reported_by() != own)
                .collect(),
            next,
        )
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.changed.is_empty()
    }
    pub(crate) fn apply(self, db: &DatabaseConnection) -> Result<(), DbError> {
        for record in self.removed {
            clear(db, record.subject(), record.reported_by())?;
        }
        for record in self.changed {
            save(db, &record)?;
        }
        Ok(())
    }
}
