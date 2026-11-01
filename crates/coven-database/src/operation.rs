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
    let paused: bool =
        db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_write_pause WHERE paused=1)", [], |r| {
            r.get(0)
        })?;
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
    u64::try_from(number).ok().and_then(std::num::NonZeroU64::new).ok_or(DbError::DamagedDatabase)
}

pub(crate) fn read(db: &DatabaseConnection) -> Result<Vec<OperationRecord>, DbError> {
    db.query(
        "SELECT id,kind,last_step,data,started_by FROM _coven_operations ORDER BY id",
        [],
        record,
    )
}

pub(crate) fn of_kind(
    db: &DatabaseConnection,
    kind: &str,
) -> Result<Vec<OperationRecord>, DbError> {
    db.query(
        "SELECT id,kind,last_step,data,started_by FROM _coven_operations WHERE kind=?1 ORDER BY id",
        [kind],
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
        (&update.last_step, &update.data, update.id.0, update.previous),
    )?;
    if changed != 1 {
        return Err(DbError::OperationChanged(update.id));
    }
    crate::pending::clear_operation(db, update.id)
}

/// Delete the row and its condition, returning how many rows were deleted.
pub(crate) fn finish(db: &DatabaseConnection, id: OperationId) -> Result<usize, DbError> {
    crate::pending::clear_operation(db, id)?;
    db.internal_execute("DELETE FROM _coven_operations WHERE id=?1", [id.0])
}

/// End every operation an app's write journaled that still runs, and every
/// invite this device made, once the applied state this transaction commits
/// stops this installation for good: it removes or replaces this device,
/// removes its member, or binds its member to a pair it doesn't hold (§12.3,
/// §14.1). None of them would ever run (`end_for_good`).
pub(crate) fn end_on_stop(db: &DatabaseConnection) -> Result<(), DbError> {
    let (running, invites) = journaled_for_good(db)?;
    if running.is_empty() && invites.is_empty() {
        return Ok(());
    }
    match stopped(db)? {
        Some(stop) => end(db, stop, running, invites),
        None => Ok(()),
    }
}

/// End every operation an app's write journaled that still runs, and every
/// invite this device made, for an installation stopped for good by `stop`.
/// The operation whose claim the applied slots record as landed, the removal
/// itself when this device claimed it, ends landed at its slot; every other
/// ends stopped, naming the stop: nothing refused it. An invite ends with the operations it started, its
/// approval's binding and its access record: their journal rows, open claim
/// and pending records go, and it leaves `coven_invites` (§13.2).
pub(crate) fn end_for_good(
    db: &DatabaseConnection,
    stop: crate::DeviceStop,
) -> Result<(), DbError> {
    let (running, invites) = journaled_for_good(db)?;
    end(db, stop, running, invites)
}

/// The app's operations still running, and the journal rows of invites and
/// of the operations invites started.
fn journaled_for_good(
    db: &DatabaseConnection,
) -> Result<(Vec<OperationId>, Vec<OperationId>), DbError> {
    let running = db.query(
        "SELECT operation FROM _coven_app_operations WHERE outcome IS NULL ORDER BY operation",
        [],
        |r| Ok(OperationId(r.get(0)?)),
    )?;
    let invites = db.query(
        "SELECT id FROM _coven_operations WHERE kind=?1 OR started_by=?1 ORDER BY id",
        [INVITE],
        |r| Ok(OperationId(r.get(0)?)),
    )?;
    Ok((running, invites))
}

fn end(
    db: &DatabaseConnection,
    stop: crate::DeviceStop,
    running: Vec<OperationId>,
    invites: Vec<OperationId>,
) -> Result<(), DbError> {
    let landed = crate::claim::landed_operation(db)?;
    for id in running {
        let outcome = match landed {
            Some((entry, slot)) if entry == id => crate::OperationOutcome::Landed(slot),
            _ => crate::OperationOutcome::Stopped(stop),
        };
        db.internal_execute(
            "UPDATE _coven_app_operations SET outcome=?2 WHERE operation=?1",
            (id.0, outcome.encode()),
        )?;
        crate::claim::drop_operation(db, id)?;
        finish(db, id)?;
    }
    for id in invites {
        crate::claim::drop_operation(db, id)?;
        finish(db, id)?;
    }
    Ok(())
}

/// The kind of an invite's journal row, and the `started_by` of the
/// operations it starts, as coven-sync names them.
const INVITE: &str = "invite";

/// How the applied state stops this installation for good, if it does. One
/// whose device isn't registered yet hasn't started.
fn stopped(db: &DatabaseConnection) -> Result<Option<crate::DeviceStop>, DbError> {
    let held: bool =
        db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_held_keys)", [], |r| r.get(0))?;
    if !held {
        return Err(DbError::Invariant {
            what: "an installation whose writes journal operations records the pairs it holds",
        });
    }
    let found: Option<(String, bool, bool)> = db
        .query(
            "SELECT d.state,m.removed,h.signing IS NOT NULL FROM _coven_devices AS d
             JOIN _coven_members AS m ON m.member=d.member
             LEFT JOIN _coven_held_keys AS h ON h.signing=m.signing AND h.sealing=m.sealing
             WHERE d.device=_coven_device()",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?
        .pop();
    Ok(found.and_then(|(state, removed, held)| match (state.as_str(), removed, held) {
        ("replaced", ..) => Some(crate::DeviceStop::Replaced),
        ("active", false, true) => None,
        ("active", false, false) => Some(crate::DeviceStop::MemberKeysReplaced),
        _ => Some(crate::DeviceStop::Removed),
    }))
}

/// Record member pairs this installation holds, or is about to keep.
pub(crate) fn hold_keys(
    db: &DatabaseConnection,
    keys: &[coven_crypto::MemberPublicKeys],
) -> Result<(), DbError> {
    for pair in keys {
        db.internal_execute(
            "INSERT INTO _coven_held_keys(signing,sealing) VALUES(?1,?2) ON CONFLICT DO NOTHING",
            (pair.signing.as_bytes().as_slice(), pair.sealing.as_bytes().as_slice()),
        )?;
    }
    Ok(())
}
