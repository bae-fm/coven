//! Durable operation records. Sync owns kinds and interprets their data (§18).

use crate::{sqlite::DatabaseConnection, DbError};

/// The local, never reused identity of an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId(pub i64);

/// An S3 key awaiting deletion in the provider console (E5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessKeyToDelete {
    /// Public identifier; never the key's secret.
    pub access_key_id: String,
    /// The removed member, absent when an invitation ended before approval.
    pub member: Option<coven_crypto::MemberId>,
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
    /// A permanent failure, cleared only by an explicit retry.
    pub failure: Option<String>,
}

impl std::fmt::Debug for OperationRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationRecord")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("last_step", &self.last_step)
            .field("data", &"[REDACTED]")
            .field("started_by", &self.started_by)
            .field("failure", &self.failure)
            .finish()
    }
}

/// The journal change committed together with a circle row-deletion write.
pub enum OperationCommit {
    /// Begin a circle deletion with its first row write.
    Start(NewOperation),
    /// Repeat row deletion after replay dropped the preceding attempt.
    Advance(OperationUpdate),
}

/// The initial state committed before any external effect.
pub struct NewOperation {
    /// Stable kind name.
    pub kind: String,
    /// Encoded initial state.
    pub data: Vec<u8>,
    /// App method name or `coven`.
    pub started_by: String,
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
    db.query_row("INSERT INTO _coven_operations(kind,last_step,data,started_by) VALUES(?1,0,?2,?3) RETURNING id",
        (&new.kind, &new.data, &new.started_by), |r| Ok(OperationId(r.get(0)?)))
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<OperationRecord>, DbError> {
    db.query(
        "SELECT id,kind,last_step,data,started_by,failure FROM _coven_operations ORDER BY id",
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
            "SELECT id,kind,last_step,data,started_by,failure FROM _coven_operations WHERE id=?1",
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
        "SELECT id,kind,last_step,data,started_by,failure FROM _coven_operations WHERE kind=?1 ORDER BY id LIMIT 1",
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
        failure: row.get(5)?,
    })
}

pub(crate) fn advance(db: &DatabaseConnection, update: &OperationUpdate) -> Result<(), DbError> {
    let changed = db.internal_execute("UPDATE _coven_operations SET last_step=?1,data=?2,failure=NULL WHERE id=?3 AND last_step=?4",
        (&update.last_step, &update.data, update.id.0, update.previous))?;
    if changed != 1 {
        return Err(DbError::OperationChanged(update.id));
    }
    Ok(())
}
