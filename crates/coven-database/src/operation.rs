//! Durable operation records. Sync owns kinds and interprets their data (§11).

use crate::operation_kind::{INVITE, PAIRING};
use crate::sqlite::StoreSql;
use crate::DbError;

/// The local, never reused identity of an operation.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct OperationId(pub i64);

/// A journal row read as one committed value.
#[derive(Clone, PartialEq)]
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

impl OperationRecord {
    /// The step past this row, recording `data`, guarded by this row's step
    /// against a stale caller. An operation's steps fitting u32 is an
    /// invariant.
    pub fn next_step(&self, data: Vec<u8>) -> Result<OperationUpdate, crate::DbError> {
        Ok(OperationUpdate {
            id: self.id,
            previous: self.last_step,
            last_step: self
                .last_step
                .checked_add(1)
                .ok_or(crate::DbError::Invariant { what: "an operation's steps fit u32" })?,
            data,
        })
    }
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

/// Journal `$new` on `$db`, a store's open write or a test's connection
/// outside any store, returning its id. Both expand this one body, so a
/// test journals an operation as a store does.
macro_rules! journal {
    ($db:expr, $new:expr) => {{
        let (db, new): (_, &NewOperation) = ($db, $new);
        let id = db.query_row(
            "INSERT INTO _coven_operations(kind,last_step,data,started_by) VALUES(?1,0,?2,?3) RETURNING id",
            (&new.kind, &new.data, &new.started_by),
            |r| Ok(OperationId(r.get(0)?)),
        )?;
        if new.pauses_writes {
            // Finishing or discarding the operation deletes its row and with
            // it the pause, in that same transaction.
            db.execute("INSERT INTO _coven_write_pause(paused,operation) VALUES(1,?1)", [id.0])?;
        }
        Ok::<_, DbError>(id)
    }};
}

/// Lists an operation the app's write journals in `coven_operations`, by
/// its id and kind.
const LIST: &str = "INSERT INTO _coven_app_operations(operation,kind) VALUES(?1,?2)";

/// Lists an operation the app's write didn't journal, by its id, kind and
/// outcome.
const LIST_OUTCOME: &str =
    "INSERT INTO _coven_app_operations(operation,kind,outcome) VALUES(?1,?2,?3)";

/// Sets the outcome of the listed operation with this id.
const SET_OUTCOME: &str = "UPDATE _coven_app_operations SET outcome=?2 WHERE operation=?1";

/// Deletes the journal row of the operation with this id.
const DELETE: &str = "DELETE FROM _coven_operations WHERE id=?1";

/// Delete the journal row of the operation `$id` on `$db` and its pending
/// record, returning how many journal rows the statement deleted. The
/// caller passes whether the operation's claim is still open as `$claimed`,
/// which ends the call with an invariant failure, and its pending record's
/// clearing as `$clear_pending`. `finish` and `finish_outside_store` both
/// expand this one body.
macro_rules! finish {
    ($db:expr, $id:expr, $claimed:expr, $clear_pending:expr) => {{
        let (db, id): (_, OperationId) = ($db, $id);
        if $claimed {
            return Err(DbError::Invariant { what: "an operation ends after its claim" });
        }
        $clear_pending?;
        db.execute(DELETE, [id.0])?
    }};
}

/// Set the outcome of the listed operation `$id` on `$db` to `$outcome`,
/// encoded as `_coven_app_operations.outcome` keeps it, then end it with
/// `$finish`, which returns how many journal rows it deleted. An operation
/// that had ended already fails with `OperationChanged`. `finish_with_encoded`
/// and `finish_outside_store` both expand this one body.
macro_rules! finish_with_outcome {
    ($db:expr, $id:expr, $outcome:expr, $finish:expr) => {{
        let (db, id, outcome): (_, OperationId, Vec<u8>) = ($db, $id, $outcome);
        db.execute(SET_OUTCOME, (id.0, outcome))?;
        if $finish != 1 {
            return Err(DbError::OperationChanged(id));
        }
        Ok(())
    }};
}

pub(crate) fn insert(db: &StoreSql, new: &NewOperation) -> Result<OperationId, DbError> {
    journal!(db, new)
}

/// Journal an operation the app's write asks for, of `kind` as sync names
/// it: listed in `coven_operations` from this write's commit until the app
/// acknowledges its outcome.
pub(crate) fn insert_for_app(
    db: &StoreSql,
    new: &NewOperation,
    kind: &str,
) -> Result<OperationId, DbError> {
    let id = insert(db, new)?;
    db.execute(LIST, (id.0, kind))?;
    Ok(id)
}

/// List the outcome of an operation the app's write didn't journal, as one
/// it did: in `coven_operations` until the app acknowledges it. An invite
/// ends this way when a refusal only a person can fix ends it (§13.2).
pub(crate) fn list_outcome(
    db: &StoreSql,
    id: OperationId,
    kind: &str,
    outcome: &crate::InviteOutcome,
) -> Result<(), DbError> {
    db.execute(LIST_OUTCOME, (id.0, kind, crate::operation_outcome::encode(outcome)))?;
    Ok(())
}

/// Journal `new` on `connection`, a test's connection outside any store, as
/// `insert` does, and list it in `coven_operations` as `insert_for_app`
/// does when `listed_as` names its kind.
#[cfg(any(test, feature = "test-utils"))]
pub fn journal_outside_store(
    connection: &sqlive::rusqlite::Connection,
    new: &NewOperation,
    listed_as: Option<&str>,
) -> Result<OperationId, DbError> {
    let id = journal!(connection, new)?;
    if let Some(kind) = listed_as {
        connection.execute(LIST, (id.0, kind))?;
    }
    Ok(id)
}

/// End the operation `id` on `connection`, a test's connection outside any
/// store, with the outcome `listing` holds. For an operation that
/// `coven_operations` lists already, the function sets its outcome, as
/// `finish_with_outcome` does. For one it doesn't list, the function lists
/// the outcome under `kind` first, as `list_outcome` does when a refusal
/// ends an invite. The function then deletes the operation's pending record
/// and its journal row, as `finish` does. Outside a store no operation
/// holds a claim, so the function checks none.
#[cfg(any(test, feature = "test-utils"))]
pub fn finish_outside_store(
    connection: &sqlive::rusqlite::Connection,
    id: OperationId,
    listing: OutcomeListing<'_>,
) -> Result<(), DbError> {
    let finished = || -> Result<usize, DbError> {
        Ok(finish!(
            connection,
            id,
            false,
            crate::pending::clear_operation_outside_store(connection, id)
        ))
    };
    match listing {
        OutcomeListing::Listed(outcome) => {
            finish_with_outcome!(connection, id, outcome.encode(), finished()?)
        }
        OutcomeListing::Unlisted { kind, outcome } => {
            connection.execute(LIST_OUTCOME, (id.0, kind, outcome.encode()))?;
            if finished()? != 1 {
                return Err(DbError::OperationChanged(id));
            }
            Ok(())
        }
    }
}

/// How an operation a test ends outside a store reaches `coven_operations`:
/// listed from its start, which the outcome then completes, or listed only
/// as it ends, under `kind`.
#[cfg(any(test, feature = "test-utils"))]
pub enum OutcomeListing<'a> {
    /// The app's write journaled it, so `coven_operations` lists it already.
    Listed(&'a crate::ListedOutcome),
    /// Sync journaled it, and its outcome lists it under `kind`.
    Unlisted {
        /// The kind `coven_operations` lists it under.
        kind: &'a str,
        /// How it ended.
        outcome: &'a crate::ListedOutcome,
    },
}

/// End the app's record of a finished operation: it leaves
/// `coven_operations`. One still running fails with `OperationRunning`;
/// one this device doesn't list, never journaled by the app or
/// acknowledged already, with `OperationUnknown`.
pub(crate) fn acknowledge(db: &StoreSql, id: OperationId) -> Result<(), DbError> {
    let finished: Option<bool> = db
        .query(
            "SELECT outcome IS NOT NULL FROM _coven_app_operations WHERE operation=?1",
            [id.0],
            |row| row.get(0),
        )?
        .pop();
    match finished {
        None => Err(DbError::OperationUnknown(id)),
        Some(false) => Err(DbError::OperationRunning(id)),
        Some(true) => {
            db.execute("DELETE FROM _coven_app_operations WHERE operation=?1", [id.0])?;
            Ok(())
        }
    }
}

/// Whether a self-replacement pauses this device's writes (§12.3).
pub(crate) fn writes_paused(db: &StoreSql) -> Result<bool, DbError> {
    Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_write_pause WHERE paused=1)", [], |r| {
        r.get(0)
    })?)
}

/// Refuse an app write while a self-replacement pauses writes (§12.3).
pub(crate) fn check_write_pause(db: &StoreSql) -> Result<(), DbError> {
    if writes_paused(db)? {
        return Err(DbError::MemberKeysChanging);
    }
    Ok(())
}

/// Reserve the next number of this device's snapshot sequence. Every
/// preparation takes a fresh one, so an abandoned attempt's path is never
/// reused, and §19.3's identity check compares stored snapshots with the
/// greatest number reserved, published or not.
pub(crate) fn reserve_snapshot(db: &StoreSql) -> Result<std::num::NonZeroU64, DbError> {
    let number = db.query_row(
        "UPDATE _coven_snapshot_reservation SET number=number+1 WHERE singleton=1 RETURNING number",
        [],
        |r| r.get::<_, i64>(0),
    )?;
    u64::try_from(number).ok().and_then(std::num::NonZeroU64::new).ok_or(DbError::DamagedDatabase)
}

pub(crate) fn read(db: &StoreSql) -> Result<Vec<OperationRecord>, DbError> {
    Ok(db.query(
        "SELECT id,kind,last_step,data,started_by FROM _coven_operations ORDER BY id",
        [],
        record,
    )?)
}

pub(crate) fn of_kind(db: &StoreSql, kind: &str) -> Result<Vec<OperationRecord>, DbError> {
    crate::live_reads::once(&of_kind_read(kind.to_owned()), db)
}

/// The unfinished rows of `kind`, by id.
pub(crate) fn of_kind_read(
    kind: String,
) -> crate::live_reads::CovenRead<OperationRecord, Vec<OperationRecord>> {
    crate::live_reads::CovenRead::new(
        "SELECT id,kind,last_step,data,started_by FROM _coven_operations WHERE kind=?1 ORDER BY id",
        move || crate::live_reads::bound(&[&kind]),
        record,
    )
}

pub(crate) fn find(db: &StoreSql, id: OperationId) -> Result<Option<OperationRecord>, DbError> {
    Ok(db
        .query(
            "SELECT id,kind,last_step,data,started_by FROM _coven_operations WHERE id=?1",
            [id.0],
            record,
        )?
        .pop())
}

pub(crate) fn first(db: &StoreSql, kind: &str) -> Result<Option<OperationRecord>, DbError> {
    Ok(db.query(
        "SELECT id,kind,last_step,data,started_by FROM _coven_operations WHERE kind=?1 ORDER BY id LIMIT 1",
        [kind],
        record,
    )?.pop())
}

fn record(row: &sqlive::rusqlite::Row<'_>) -> sqlive::rusqlite::Result<OperationRecord> {
    Ok(OperationRecord {
        id: OperationId(row.get(0)?),
        kind: row.get(1)?,
        last_step: row.get(2)?,
        data: row.get(3)?,
        started_by: row.get(4)?,
    })
}

pub(crate) fn advance(db: &StoreSql, update: &OperationUpdate) -> Result<(), DbError> {
    let changed = db.execute(
        "UPDATE _coven_operations SET last_step=?1,data=?2 WHERE id=?3 AND last_step=?4",
        (&update.last_step, &update.data, update.id.0, update.previous),
    )?;
    if changed != 1 {
        return Err(DbError::OperationChanged(update.id));
    }
    crate::pending::clear_operation(db, update.id)
}

/// Put the operation `id` at `step`, keeping its data, unguarded by the step
/// it was at: a provider-access request moves between its steps as the
/// account's row says (§14.2).
pub(crate) fn set_step(db: &StoreSql, id: OperationId, step: u32) -> Result<(), DbError> {
    db.execute(
        "UPDATE _coven_operations SET last_step=?2 WHERE id=?1 AND last_step!=?2",
        (id.0, step),
    )?;
    Ok(())
}

/// End the running operation `id` with `outcome`, in the outcome type of its
/// kind, which `coven_operations` lists for an operation the app's write
/// journaled; `None` for a kind it never lists. An outcome is final: an
/// operation that no longer runs fails with `OperationChanged` and keeps the
/// outcome it ended with (§11.4). The outcome is written first, as the
/// journal row's end requires; the caller's transaction undoes it when the
/// operation had already ended.
pub(crate) fn finish_with_outcome(
    db: &StoreSql,
    id: OperationId,
    outcome: Option<&crate::ListedOutcome>,
) -> Result<(), DbError> {
    match outcome {
        Some(outcome) => finish_with_encoded(db, id, outcome.encode()),
        None if finish(db, id)? == 1 => Ok(()),
        None => Err(DbError::OperationChanged(id)),
    }
}

/// `finish_with_outcome` with the outcome encoded already, as `end` writes
/// an outcome of any listed kind.
fn finish_with_encoded(db: &StoreSql, id: OperationId, outcome: Vec<u8>) -> Result<(), DbError> {
    finish_with_outcome!(db, id, outcome, finish(db, id)?)
}

/// Delete the row and its condition, returning how many rows were deleted.
/// An operation whose claim is still open never ends: nothing would finish
/// that claim, and the device, one claim at a time, would claim no slot
/// again (§5.1).
pub(crate) fn finish(db: &StoreSql, id: OperationId) -> Result<usize, DbError> {
    Ok(finish!(db, id, crate::claim::held_by(db, id)?, crate::pending::clear_operation(db, id)))
}

/// End every operation an app's write journaled that still runs, and every
/// invite this device made, once the applied state this transaction commits
/// stops this installation for good: it removes or replaces this device,
/// removes its member, or binds its member to a pair it doesn't hold (§12.3,
/// §14.1). None of them would ever run (`end_for_good`).
pub(crate) fn end_on_stop(db: &StoreSql) -> Result<(), DbError> {
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
/// and pending records go, and it leaves `coven_invites` (§13.2). A pairing's
/// row goes the same way; it started nothing, and its request or handover is
/// left to the gone device's marker sweep (§13.2, §14.1).
pub(crate) fn end_for_good(db: &StoreSql, stop: crate::DeviceStop) -> Result<(), DbError> {
    if stop == crate::DeviceStop::Copied {
        return Err(DbError::Invariant { what: "a copy's stop records its slot (end_for_copy)" });
    }
    let (running, invites) = journaled_for_good(db)?;
    end(db, stop, running, invites)
}

/// `end_for_good` for a copy that found another installation using its
/// device id at `slot`: the same transaction records the stop, which stays
/// until a reset replaces the installation or the store is deleted (§19.3).
pub(crate) fn end_for_copy(db: &StoreSql, slot: coven_merge::Slot) -> Result<(), DbError> {
    let (running, invites) = journaled_for_good(db)?;
    end(db, crate::DeviceStop::Copied, running, invites)?;
    crate::device_reset::record_in_use(db, slot)
}

/// The app's operations still running, and the journal rows of invites and
/// of the operations invites started, and of pairings.
fn journaled_for_good(db: &StoreSql) -> Result<(Vec<OperationId>, Vec<OperationId>), DbError> {
    let running = db.query(
        "SELECT operation FROM _coven_app_operations WHERE outcome IS NULL ORDER BY operation",
        [],
        |r| Ok(OperationId(r.get(0)?)),
    )?;
    let invites = db.query(
        "SELECT id FROM _coven_operations WHERE kind=?1 OR started_by=?1 OR kind=?2 ORDER BY id",
        [INVITE, PAIRING],
        |r| Ok(OperationId(r.get(0)?)),
    )?;
    Ok((running, invites))
}

fn end(
    db: &StoreSql,
    stop: crate::DeviceStop,
    running: Vec<OperationId>,
    invites: Vec<OperationId>,
) -> Result<(), DbError> {
    let landed = crate::claim::landed_operation(db)?;
    for id in running {
        let outcome = match landed {
            Some((entry, slot)) if entry == id => crate::StopOutcome::Landed(slot),
            _ => crate::StopOutcome::Stopped(stop),
        };
        crate::claim::drop_operation(db, id)?;
        finish_with_encoded(db, id, outcome.encode())?;
    }
    for id in invites {
        crate::claim::drop_operation(db, id)?;
        finish(db, id)?;
    }
    Ok(())
}

/// How the applied state stops this installation for good, if it does. One
/// whose device isn't registered yet hasn't started.
pub(crate) fn stopped(db: &StoreSql) -> Result<Option<crate::DeviceStop>, DbError> {
    use crate::DeviceState::{Active, Replaced};
    let held: bool =
        db.query_row("SELECT EXISTS(SELECT 1 FROM _coven_held_keys)", [], |r| r.get(0))?;
    if !held {
        return Err(DbError::Invariant {
            what: "an installation whose writes journal operations records the pairs it holds",
        });
    }
    let found: Option<(crate::DeviceState, bool, bool)> = db
        .query(
            "SELECT d.state,m.removed,h.signing IS NOT NULL FROM _coven_devices AS d
             JOIN _coven_members AS m ON m.member=d.member
             LEFT JOIN _coven_held_keys AS h ON h.signing=m.signing AND h.sealing=m.sealing
             WHERE d.device=_coven_device()",
            [],
            |r| {
                Ok((
                    crate::write_encoding::stored(r, 0, sqlive::rusqlite::types::Type::Text)?,
                    r.get(1)?,
                    r.get(2)?,
                ))
            },
        )?
        .pop();
    Ok(found.and_then(|(state, removed, held)| match (state, removed, held) {
        (Replaced, ..) => Some(crate::DeviceStop::Replaced),
        (Active, false, true) => None,
        (Active, false, false) => Some(crate::DeviceStop::MemberKeysReplaced),
        // Removing a member removes each of its devices, so a removed member
        // is the stop. A device removed by itself while its member stays is
        // the device's. A removal that came second finds the operations the
        // first one ended already.
        (_, true, _) => Some(crate::DeviceStop::Removed(crate::Removal::Member)),
        _ => Some(crate::DeviceStop::Removed(crate::Removal::Device)),
    }))
}

/// Record member pairs this installation holds, or is about to keep.
pub(crate) fn hold_keys(
    db: &StoreSql,
    keys: &[coven_crypto::MemberPublicKeys],
) -> Result<(), DbError> {
    for pair in keys {
        db.execute(
            "INSERT INTO _coven_held_keys(signing,sealing) VALUES(?1,?2) ON CONFLICT DO NOTHING",
            (pair.signing.as_bytes().as_slice(), pair.sealing.as_bytes().as_slice()),
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "operation_tests.rs"]
pub(crate) mod tests;
