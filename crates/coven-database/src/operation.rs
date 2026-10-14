//! Durable operation records. Sync owns kinds and interprets their data (§18).

use crate::{sqlite::DatabaseConnection, DbError};

/// The local, never reused identity of an operation.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct OperationId(pub i64);

/// An S3 key pending deletion: superseded, recorded for a removed member, or
/// from an ended invite (§13, E9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessKeyToDelete {
    /// Public identifier; never the key's secret.
    pub access_key_id: String,
    /// The provider identity holding the key, which deleting it needs.
    pub principal: coven_format::S3Principal,
    /// The member it was recorded for, absent when an invitation ended before approval.
    pub member: Option<coven_crypto::MemberId>,
}

/// A deletion obligation and the operation that carries its pending notice.
/// Recording it is a no-op when the key already has one or was confirmed deleted.
pub struct AccessKeyDeletion {
    /// The key to delete.
    pub key: AccessKeyToDelete,
    /// The operation journaling its deletion, started by `coven`.
    pub operation: NewOperation,
}

/// A journal row read as one committed value.
#[derive(Clone)]
pub struct OperationRecord {
    /// Its local identity.
    pub id: OperationId,
    /// The kind's stable name, defined by its sync implementation.
    pub kind: String,
    /// Last completed step; zero precedes the first step.
    pub last_step: u32,
    /// The kind's encoded state, including fixed identities and causal views.
    pub data: Vec<u8>,
    /// The initiating app method or `coven`.
    pub started_by: String,
}

impl std::fmt::Debug for OperationRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationRecord")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("last_step", &self.last_step)
            .field("data", &"[REDACTED]")
            .field("started_by", &self.started_by)
            .finish()
    }
}

/// The initial state committed before any external effect.
pub struct NewOperation {
    /// Stable kind name.
    pub kind: String,
    /// Encoded initial state.
    pub data: Vec<u8>,
    /// App method name or `coven`.
    pub started_by: String,
    /// Its id is the number of a snapshot this device may write: inserting it
    /// reserves that snapshot number, which §10's identity check compares
    /// with this device's stored snapshots. Other operations' ids are never
    /// snapshot numbers and reserve none.
    pub reserves_snapshot: bool,
}

/// A step transition, guarded against stale callers.
pub struct OperationUpdate {
    /// The operation being advanced.
    pub id: OperationId,
    /// Step from which this transition was made.
    pub previous: u32,
    /// The completed step.
    pub last_step: u32,
    /// Encoded state after the step.
    pub data: Vec<u8>,
}

pub(crate) fn insert(db: &DatabaseConnection, new: &NewOperation) -> Result<OperationId, DbError> {
    let id = db.query_row("INSERT INTO _coven_operations(kind,last_step,data,started_by) VALUES(?1,0,?2,?3) RETURNING id",
        (&new.kind, &new.data, &new.started_by), |r| Ok(OperationId(r.get(0)?)))?;
    if new.reserves_snapshot {
        db.internal_execute(
            "UPDATE _coven_snapshot_reservation SET number=max(number,?1) WHERE singleton=1",
            [id.0],
        )?;
    }
    Ok(id)
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<OperationRecord>, DbError> {
    db.query(
        "SELECT id,kind,last_step,data,started_by FROM _coven_operations ORDER BY id",
        [],
        record,
    )
}

pub(crate) fn find(
    db: &DatabaseConnection,
    id: OperationId,
) -> Result<Option<OperationRecord>, DbError> {
    Ok(db
        .query(
            "SELECT id,kind,last_step,data,started_by FROM _coven_operations WHERE id=?1",
            [id.0],
            record,
        )?
        .pop())
}

pub(crate) fn first(
    db: &DatabaseConnection,
    kind: &str,
) -> Result<Option<OperationRecord>, DbError> {
    Ok(db.query(
        "SELECT id,kind,last_step,data,started_by FROM _coven_operations WHERE kind=?1 ORDER BY id LIMIT 1",
        [kind],
        record,
    )?.pop())
}

fn record(row: &rusqlite::Row<'_>) -> rusqlite::Result<OperationRecord> {
    Ok(OperationRecord {
        id: OperationId(row.get(0)?),
        kind: row.get(1)?,
        last_step: row.get(2)?,
        data: row.get(3)?,
        started_by: row.get(4)?,
    })
}

/// Record each new obligation with its operation and pending notice, in the
/// caller's transaction. Open and confirmed keys keep their state (§13).
pub(crate) fn record_access_keys<P: crate::PendingValue>(
    db: &DatabaseConnection,
    reporter: coven_foundation::id_source::DeviceId,
    deletions: &[AccessKeyDeletion],
) -> Result<(), DbError> {
    for deletion in deletions {
        let known = db.query(
            "SELECT 1 FROM _coven_access_keys WHERE access_key_id=?1",
            [&deletion.key.access_key_id],
            |_| Ok(()),
        )?;
        if !known.is_empty() {
            continue;
        }
        let id = insert(db, &deletion.operation)?;
        db.internal_execute(
            "INSERT INTO _coven_access_keys(access_key_id,operation) VALUES(?1,?2)",
            (&deletion.key.access_key_id, id.0),
        )?;
        crate::pending::save(db, &P::access_key(id, reporter, deletion.key.clone()))?;
    }
    Ok(())
}

/// Retire a key's deletion obligation and its notice, keeping the confirmation
/// by id so no later entry, invite or retry records it again (§13).
pub(crate) fn clear_access_key(db: &DatabaseConnection, key: &str) -> Result<(), DbError> {
    let open = db.query(
        "SELECT operation FROM _coven_access_keys WHERE access_key_id=?1 AND operation IS NOT NULL",
        [key],
        |r| r.get::<_, i64>(0),
    )?;
    for id in open {
        crate::pending::clear_operation(db, OperationId(id))?;
        db.internal_execute("DELETE FROM _coven_operations WHERE id=?1", [id])?;
    }
    db.internal_execute(
        "INSERT INTO _coven_access_keys(access_key_id,operation) VALUES(?1,NULL)
         ON CONFLICT(access_key_id) DO UPDATE SET operation=NULL",
        [key],
    )?;
    Ok(())
}

pub(crate) fn advance(db: &DatabaseConnection, update: &OperationUpdate) -> Result<(), DbError> {
    let changed = db.internal_execute(
        "UPDATE _coven_operations SET last_step=?1,data=?2 WHERE id=?3 AND last_step=?4",
        (
            &update.last_step,
            &update.data,
            update.id.0,
            update.previous,
        ),
    )?;
    if changed != 1 {
        return Err(DbError::OperationChanged(update.id));
    }
    crate::pending::clear_operation(db, update.id)
}
