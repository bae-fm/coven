//! The original queue prefix captured for an unpublished breaking raise.
//!
//! A capture fixes eligibility through its marker. Snapshot bytes are made from
//! the counted base only after that prefix has acquired slots; later local
//! originals remain outside it.
use crate::write_encoding::{audience, counter};
use crate::{sqlite::DatabaseConnection, write_schema::WriteSchema, DbError};
use coven_format::{admin::SnapshotId, snapshot::SnapshotHeader};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use rusqlite::params;

/// A breaking raise's fixed original prefix and current counted coverage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaiseCapture {
    /// Audience raised by this capture.
    pub audience: Audience,
    /// Schema version to announce.
    pub version: u32,
    /// Last captured original, including the migration marker.
    pub through: WriteId,
    /// Applied slot when this capture was prepared.
    pub covers: u64,
}

pub(crate) fn capture(
    db: &DatabaseConnection,
    _schema: &WriteSchema,
    marker: WriteId,
    version: u32,
) -> Result<bool, DbError> {
    let state = crate::store_state_tables::read(db)?;
    db.internal_execute("DELETE FROM _coven_raise_captures", [])?;
    let Some(member) = state
        .store
        .as_ref()
        .and_then(|_| state.devices.get(&marker.device))
        .map(|device| device.member)
    else {
        return Ok(false);
    };
    for selected in state.readable_audiences(&member) {
        db.internal_execute("INSERT INTO _coven_raise_captures(audience,version,through,covers) VALUES(?1,?2,?3,?4)",params![&selected,version,marker.number.to_be_bytes(),crate::sequence::applied(db)?.to_be_bytes()])?;
    }
    crate::pending::end_captured_raise_waits(db, marker.number)?;
    Ok(true)
}

/// The creator's raise, as the store it created comes into being. A breaking
/// migration that ran before the store existed captured nothing, as there
/// was no shared version to raise; the store's history records the version
/// its creating app runs only once this device raises it, through the marker
/// that migration queued (§18.3).
pub(crate) fn capture_created(
    db: &DatabaseConnection,
    schema: &WriteSchema,
) -> Result<(), DbError> {
    if breaking_version(db)? == 0 || !provisional(db)?.is_empty() {
        return Ok(());
    }
    let device = db
        .local_device()
        .ok_or(DbError::Invariant { what: "a writer connection observes this device" })?;
    let queued = db.query(
        "SELECT number FROM _coven_uploads WHERE device=?1 ORDER BY number DESC",
        [device],
        |r| counter(r.get::<_, Vec<u8>>(0)?),
    )?;
    for number in queued {
        let marker = WriteId { device, number };
        let header = crate::upload::queued(db, marker)?
            .ok_or(DbError::Invariant { what: "a queued write has its header" })?
            .header;
        if header.disposition == coven_format::write::WriteDisposition::Migration {
            let version = db.schema_version()?;
            if capture(db, schema, marker, version)? {
                publish_at(db, version)?;
            }
            return Ok(());
        }
    }
    Ok(())
}

/// Raise a circle this device's member just made, when the base its creation
/// recorded is below this installation's newest breaking migration: it was
/// prepared before that migration and applied after it, so the migration's
/// capture didn't include it. The circle has no rows yet, so this device holds
/// its state at its own version, and its later writes wait for the raise as
/// a migration's do. A device that starts reading a circle with history holds
/// none of it at the circle's version, and waits for a device that held it
/// (§15.5, §18.3).
pub(crate) fn capture_made(
    db: &DatabaseConnection,
    circle: coven_foundation::id_source::CircleId,
    state: &crate::StoreState,
    operation: Option<&crate::migration::MigrationOperation>,
) -> Result<(), DbError> {
    let audience = Audience::Circle(circle);
    let device = db
        .local_device()
        .ok_or(DbError::Invariant { what: "a writer connection observes this device" })?;
    let made = state
        .devices
        .get(&device)
        .is_some_and(|local| local.active() && state.reads_circle(circle, &local.member));
    if !made || state.reached(&audience) >= breaking_version(db)? {
        return Ok(());
    }
    // Every capture holds the same prefix; with none, the prefix is every
    // write this device has made so far.
    let through = match captured_through(db)? {
        Some(through) => through,
        None => crate::write_metadata::newest_number(db, device)?.unwrap_or(0),
    };
    db.internal_execute(
        "INSERT INTO _coven_raise_captures(audience,version,through,covers) VALUES(?1,?2,?3,?4)",
        params![
            &audience,
            db.schema_version()?,
            through.to_be_bytes(),
            crate::sequence::applied(db)?.to_be_bytes()
        ],
    )?;
    crate::pending::end_captured_raise_waits(db, through)?;
    // The migration's journal may have finished with its own raises; passes
    // publish captures while one is open.
    if let Some(operation) = operation {
        if crate::operation::of_kind(db, crate::operation_kind::SCHEMA_RAISE)?.is_empty() {
            journal(db, operation, db.schema_version()?)?;
        }
    }
    Ok(())
}

/// Journal the operation through which passes publish this installation's
/// raise captures at `version`, at its first step.
pub(crate) fn journal(
    db: &DatabaseConnection,
    operation: &crate::migration::MigrationOperation,
    version: u32,
) -> Result<(), DbError> {
    let new = operation(version)?;
    let id = crate::operation::insert(db, &new)?;
    crate::operation::advance(
        db,
        &crate::OperationUpdate { id, previous: 0, last_step: 1, data: new.data },
    )
}

pub(crate) fn raise_to(
    db: &DatabaseConnection,
    _tables: &[crate::SyncedTable],
    device: DeviceId,
    version: u32,
) -> Result<bool, DbError> {
    if captures(db, device)?.is_empty() {
        return Ok(false);
    }
    // A capture is made through one of this device's writes, which stays.
    let newest = crate::write_metadata::newest_number(db, device)?
        .ok_or(DbError::Invariant { what: "a raise capture's device has a write" })?;
    db.internal_execute(
        "UPDATE _coven_raise_captures SET version=?1,through=?2,covers=?3",
        params![version, newest.to_be_bytes(), crate::sequence::applied(db)?.to_be_bytes()],
    )?;
    crate::pending::end_captured_raise_waits(db, newest)?;
    Ok(true)
}

pub(crate) fn prepare(
    db: &DatabaseConnection,
    _schema: &WriteSchema,
    device: DeviceId,
    selected: &Audience,
    version: u32,
) -> Result<Option<RaiseCapture>, DbError> {
    db.internal_execute(
        "UPDATE _coven_raise_captures SET covers=?1 WHERE audience=?2 AND version=?3",
        params![crate::sequence::applied(db)?.to_be_bytes(), selected, version],
    )?;
    Ok(captures(db, device)?
        .into_iter()
        .find(|capture| capture.audience == *selected && capture.version == version))
}

/// This device's first queued write that waits for a raise: one made at or
/// after its newest breaking migration, with a part in an audience that
/// hasn't reached that migration's version and can still be raised
/// ([`crate::StoreState::awaits_raise`]). Every device would refuse that
/// part until the audience has, so it waits, and the writes behind it with it
/// (§18.5). A raise's captured prefix goes out before its raise; while one
/// waits, it holds everything after it already.
pub(crate) fn first_ahead(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Option<(u64, Audience)>, DbError> {
    let breaking = breaking_version(db)?;
    if breaking == 0 || captured_through(db)?.is_some() {
        return Ok(None);
    }
    let state = crate::store_state_tables::read(db)?;
    let below = |audience: &Audience| state.awaits_raise(audience, breaking);
    if !below(&Audience::Store) && !state.circles.keys().any(|c| below(&Audience::Circle(*c))) {
        return Ok(None);
    }
    let queued = db.query(
        "SELECT number FROM _coven_uploads WHERE device=?1 ORDER BY number",
        [device],
        |r| counter(r.get::<_, Vec<u8>>(0)?),
    )?;
    for number in queued {
        let write = crate::upload::queued(db, WriteId { device, number })?
            .ok_or(DbError::Invariant { what: "a queued write has its header" })?;
        if write.header.schema_version >= breaking {
            if let Some(part) = write.parts.iter().find(|part| below(&part.audience)) {
                return Ok(Some((number, part.audience.clone())));
            }
        }
    }
    Ok(None)
}

/// Record why the queue stops at a write ahead of an audience's version, as
/// [`first_ahead`] finds it (§18.5).
pub(crate) fn record_ahead<P: crate::PendingValue>(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<(), DbError> {
    if let Some((number, audience)) = first_ahead(db, device)? {
        let write = WriteId { device, number };
        crate::pending::save(db, &P::raise_wait(write, audience, breaking_version(db)?))?;
    }
    Ok(())
}

/// End every raise wait the installed store state has reached or whose
/// circle it deleted, in the commit that installs it; a write still below
/// another audience's version records that wait instead.
pub(crate) fn end_raise_waits<P: crate::PendingValue>(
    db: &DatabaseConnection,
) -> Result<(), DbError> {
    let state = crate::store_state_tables::read(db)?;
    for record in crate::pending::read::<P>(db)? {
        if let Some((write, audience, version)) = record.raise_awaited() {
            if !state.awaits_raise(audience, version) {
                crate::pending::clear(db, record.subject())?;
                record_ahead::<P>(db, write.device)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn captured_through(db: &DatabaseConnection) -> Result<Option<u64>, DbError> {
    db.query_row("SELECT max(through) FROM _coven_raise_captures", [], |r| {
        r.get::<_, Option<Vec<u8>>>(0)?.map(counter).transpose()
    })
}

pub(crate) fn provisional(db: &DatabaseConnection) -> Result<Vec<(Audience, u32, u64)>, DbError> {
    db.query(
        "SELECT audience,version,covers FROM _coven_raise_captures ORDER BY audience",
        [],
        |r| Ok((audience(r, 0)?, r.get(1)?, counter(r.get(2)?)?)),
    )
}

pub(crate) fn captures(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Vec<RaiseCapture>, DbError> {
    db.query(
        "SELECT audience,version,through,covers FROM _coven_raise_captures ORDER BY audience",
        [],
        |r| {
            Ok(RaiseCapture {
                audience: audience(r, 0)?,
                version: r.get(1)?,
                through: WriteId { device, number: counter(r.get(2)?)? },
                covers: counter(r.get(3)?)?,
            })
        },
    )
}

pub(crate) fn write<E>(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    id: SnapshotId,
    version: u32,
    begin: impl FnOnce(&SnapshotHeader) -> Result<(), E>,
    emit: impl FnMut(Vec<u8>) -> Result<(), E>,
) -> Result<u32, crate::SnapshotWriteError<E>> {
    let capture = captures(db, id.device)?
        .into_iter()
        .find(|capture| capture.audience == id.audience && capture.version == version)
        .ok_or(DbError::DamagedDatabase)?;
    if crate::upload::unconfirmed(db, capture.through)?.is_some() {
        return Err(DbError::ClaimUnsettled.into());
    }
    if capture.covers != crate::sequence::applied(db)? {
        return Err(DbError::CaptureChanged.into());
    }
    crate::snapshot_write::write(db, schema, id, begin, emit)
}

/// The version of the newest breaking migration this installation ran, zero
/// if none: received writes are read from it on (§18.5), and a snapshot this
/// installation writes announces it as its breaking version (§16.3).
pub(crate) fn breaking_version(db: &DatabaseConnection) -> Result<u32, DbError> {
    db.query_row("SELECT minimum FROM _coven_snapshot_schema WHERE singleton=1", [], |r| r.get(0))
}

/// Read received writes from schema version `minimum` on, the version a
/// breaking migration requires (§18.5).
pub(crate) fn require(db: &DatabaseConnection, minimum: u32) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_snapshot_schema SET minimum=?1 WHERE singleton=1",
        [minimum],
    )?;
    Ok(())
}

/// Wait for this installation's raise to publish at schema `version` before
/// applying received writes past it (§18.5).
pub(crate) fn publish_at(db: &DatabaseConnection, version: u32) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_snapshot_schema SET publication=?1 WHERE singleton=1",
        [version],
    )?;
    Ok(())
}

/// A snapshot of `audience` loaded at schema `version` covers every capture
/// of it at that version or before.
pub(crate) fn loaded(
    db: &DatabaseConnection,
    audience: &Audience,
    version: u32,
) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_raise_captures WHERE audience=?1 AND version<=?2",
        params![audience, version],
    )?;
    Ok(())
}

pub(crate) fn discard(
    db: &DatabaseConnection,
    audience: &Audience,
    version: u32,
) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_raise_captures WHERE audience=?1 AND version=?2",
        params![audience, version],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "raise_capture_tests.rs"]
pub(crate) mod tests;
