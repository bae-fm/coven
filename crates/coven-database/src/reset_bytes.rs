//! The bytes this device keeps for a file after a change takes the file's
//! record off them, while a reset may still undo the change (§17.11, §19.4):
//! the original's record, coven's copy and their hashes. A reset ignores
//! every write outside its snapshot that didn't read its entry, so a record
//! another write deleted or switched away can come back as it was. Its bytes
//! wait until the audience's reset bound reaches the slot of the change,
//! since every later reset names coverage at or above that bound.

use crate::file_location::Place;
use crate::file_table::{self, Record};
use crate::sqlite::StoreSql;
use crate::store_call::StoreCall;
use crate::DbError;
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::Audience;
use sqlive::rusqlite::params;

/// The change a transaction makes to the records it takes off this
/// device's bytes: this device's own write, by number, or the slot the
/// transaction applies through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Change {
    Write(u64),
    Slot(u64),
}

/// Whether this device keeps bytes for `file` that its record, `shown` as
/// the app sees it and `kept` while any audience still has it, doesn't
/// need: all of them unless the record puts the file on this device only,
/// and the chunk hashes and coven's copy once the file is stored (§17.3).
fn unneeded(
    db: &StoreSql,
    file: FileId,
    shown: Option<&Record>,
    kept: bool,
) -> Result<bool, DbError> {
    let place = shown.filter(|_| kept).map(|record| &record.place);
    let sql = match place {
        Some(Place::DeviceOnly(device)) if *device == this_device(db)? => return Ok(false),
        Some(Place::Cloud(_)) => {
            "SELECT EXISTS(SELECT 1 FROM _coven_user_chunks WHERE file=?1)
                 OR EXISTS(SELECT 1 FROM _coven_device_files WHERE file=?1)"
        }
        _ => {
            "SELECT EXISTS(SELECT 1 FROM _coven_user_files WHERE file=?1)
                 OR EXISTS(SELECT 1 FROM _coven_user_chunks WHERE file=?1)
                 OR EXISTS(SELECT 1 FROM _coven_device_files WHERE file=?1)"
        }
    };
    Ok(db.query_row(sql, [file], |r| r.get(0))?)
}

fn this_device(db: &StoreSql) -> Result<DeviceId, DbError> {
    Ok(db.query_row("SELECT _coven_device()", [], |r| r.get(0))?)
}

/// The audience of `file`'s record as this device's merge records hold it,
/// deleted or not; none once a reload left no trace of it.
pub(crate) fn audience(db: &StoreSql, file: FileId) -> Result<Option<Audience>, DbError> {
    Ok(db
        .query(
            "SELECT audience FROM _coven_rows WHERE table_name=?1 AND key=?2 LIMIT 1",
            params![crate::internal_schema::FILES, file_table::key(file)?],
            |r| r.get::<_, Audience>(0),
        )?
        .pop())
}

/// `file`'s record changed to `shown`, `kept` while any audience still has
/// it, in `audience`. Bytes it no longer needs wait for the change to be
/// past every reset that could undo it. A record gone for good needs no
/// wait: one in a deleted circle, which never comes back (§15.9), or one a
/// reload left no trace of, whose staging a reset ignored and every later
/// reset ignores too.
pub(crate) fn changed(
    db: &StoreSql,
    call: &StoreCall<'_>,
    file: FileId,
    shown: Option<&Record>,
    kept: bool,
    audience: Option<&Audience>,
) -> Result<(), DbError> {
    if !unneeded(db, file, shown, kept)? {
        return Ok(());
    }
    let gone = match audience {
        None => true,
        Some(Audience::Circle(circle)) => {
            crate::store_state_tables::deleted_circles(db)?.contains_key(circle)
        }
        Some(Audience::Store) => false,
    };
    match audience {
        Some(audience) if !gone => {
            let change = call.reset_change().ok_or(DbError::Invariant {
                what: "a record leaves this device's bytes in a write or a slot",
            })?;
            undoable(db, file, audience, change)
        }
        _ => forget(db, file),
    }
}

/// Record that `change` took `file`'s record off this device's bytes. The
/// later change stands: a write of this device's still queued over a slot,
/// and a later write or slot over an earlier one. A slot the audience's
/// reset bound has reached is past every reset already.
fn undoable(
    db: &StoreSql,
    file: FileId,
    audience: &Audience,
    change: Change,
) -> Result<(), DbError> {
    let (number, slot) = match change {
        Change::Write(number) => (Some(number.to_be_bytes()), None),
        Change::Slot(slot) => (None, Some(slot.to_be_bytes())),
    };
    db.execute(
        "INSERT INTO _coven_undoable_files(file,audience,number,slot,past)
         VALUES(?1,?2,?3,?4,coalesce(?4<=(SELECT slot FROM _coven_reset_bounds WHERE audience=?2),0))
         ON CONFLICT(file) DO UPDATE
         SET audience=excluded.audience,number=excluded.number,slot=excluded.slot,past=excluded.past
         WHERE (excluded.number IS NOT NULL AND (number IS NULL OR excluded.number>number))
            OR (excluded.slot IS NOT NULL AND number IS NULL AND excluded.slot>slot)",
        params![
            file,
            audience,
            number.as_ref().map(|n| n.as_slice()),
            slot.as_ref().map(|s| s.as_slice())
        ],
    )?;
    Ok(())
}

/// The lowest coverage a later reset of `audience` may name is at least
/// `slot`: an announcement covering it, or a raise or reset at it, applied
/// (§16.3). Every change of the audience at or below it is past every reset.
pub(crate) fn bound(db: &StoreSql, audience: &Audience, slot: u64) -> Result<(), DbError> {
    let slot = slot.to_be_bytes();
    db.execute(
        "INSERT INTO _coven_reset_bounds(audience,slot) VALUES(?1,?2)
         ON CONFLICT(audience) DO UPDATE SET slot=excluded.slot WHERE excluded.slot>slot",
        params![audience, slot.as_slice()],
    )?;
    db.execute(
        "UPDATE _coven_undoable_files SET past=1 WHERE audience=?1 AND slot<=?2 AND past=0",
        params![audience, slot.as_slice()],
    )?;
    Ok(())
}

/// This device's writes that `claim` carried left the queue, each at or
/// before the slot this device has applied (§17.11).
pub(crate) fn landed(db: &StoreSql, device: DeviceId, claim: u64) -> Result<(), DbError> {
    if device != this_device(db)? {
        return Ok(());
    }
    db.execute(
        "UPDATE _coven_undoable_files SET number=NULL,slot=?3
         WHERE number IN (SELECT number FROM _coven_upload_claims WHERE device=?1 AND claim=?2)",
        params![
            device,
            claim.to_be_bytes().as_slice(),
            crate::sequence::applied(db)?.to_be_bytes().as_slice()
        ],
    )?;
    Ok(())
}

/// Before a transaction that changes records commits: each change the reset
/// bound of its audience reached, or in a deleted circle, is past every
/// reset, so its file's bytes go where its record no longer needs them.
pub(crate) fn settle(db: &StoreSql, call: &StoreCall<'_>) -> Result<(), DbError> {
    call.set_reset_change(None);
    let mut past =
        db.query("DELETE FROM _coven_undoable_files WHERE past=1 RETURNING file", [], |r| {
            r.get::<_, FileId>(0)
        })?;
    for circle in crate::store_state_tables::deleted_circles(db)?.keys() {
        past.extend(db.query(
            "DELETE FROM _coven_undoable_files WHERE audience=?1 RETURNING file",
            [Audience::Circle(*circle)],
            |r| r.get::<_, FileId>(0),
        )?);
    }
    for file in past {
        forget(db, file)?;
    }
    Ok(())
}

/// Forget the bytes of `file` that no record needs, unless a change a reset
/// may undo still keeps them: none while the record puts the file here; the
/// chunk hashes and coven's copy once it is stored; and on another device
/// only, or with no record, every one, coven's copies in the person's
/// folders lost to that device's switch (§17.3).
fn forget(db: &StoreSql, file: FileId) -> Result<(), DbError> {
    if db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_undoable_files WHERE file=?1)",
        [file],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(());
    }
    match Record::read(db, file)?.map(|record| record.place) {
        Some(Place::DeviceOnly(device)) if device == this_device(db)? => return Ok(()),
        Some(Place::Cloud(_)) => crate::user_file::stored(db, file)?,
        Some(Place::DeviceOnly(_)) => crate::file_protection::elsewhere(db, file)?,
        None => crate::user_file::forget(db, file)?,
    }
    crate::file_queue::forget_copy(db, file)
}

/// Take every change that took a record off this device's bytes as past
/// every reset that could undo it, as an announcement covering each one
/// would once its write landed, and forget the bytes no record needs.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn past_every_reset(db: &StoreSql) -> Result<(), DbError> {
    for file in
        db.query("DELETE FROM _coven_undoable_files RETURNING file", [], |r| r.get::<_, FileId>(0))?
    {
        forget(db, file)?;
    }
    Ok(())
}
