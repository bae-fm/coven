//! Durable operation records. Sync owns kinds and interprets their data (§11).

use crate::{sqlite::DatabaseConnection, DbError};

/// The local, never reused identity of an operation.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct OperationId(pub i64);

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
    /// App writes are refused with `DbError::MemberKeysChanging` from its
    /// insertion until it finishes: a self-replacement of member keys
    /// confirms its queue and installs its binding meanwhile (§12.3, §11).
    pub pauses_writes: bool,
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
    if new.pauses_writes {
        // Finishing or discarding the operation deletes its row and with it
        // the pause, in that same transaction.
        db.internal_execute(
            "INSERT INTO _coven_write_pause(paused,operation) VALUES(1,?1)",
            [id.0],
        )?;
    }
    Ok(id)
}

/// Refuse an app write while a self-replacement pauses writes (§12.3).
pub(crate) fn check_write_pause(db: &DatabaseConnection) -> Result<(), DbError> {
    let paused: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_write_pause WHERE paused=1)",
        [],
        |r| r.get(0),
    )?;
    if paused {
        return Err(DbError::MemberKeysChanging);
    }
    Ok(())
}

/// Reserve the next number of this device's snapshot sequence. Every
/// preparation takes a fresh one, so an abandoned attempt's path is never
/// reused, and §19.3's identity check compares stored snapshots with the
/// greatest number reserved, published or not.
pub(crate) fn reserve_snapshot(db: &DatabaseConnection) -> Result<std::num::NonZeroU64, DbError> {
    let number = db.query_row(
        "UPDATE _coven_snapshot_reservation SET number=number+1 WHERE singleton=1 RETURNING number",
        [],
        |r| r.get::<_, i64>(0),
    )?;
    u64::try_from(number)
        .ok()
        .and_then(std::num::NonZeroU64::new)
        .ok_or(DbError::DamagedDatabase)
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
