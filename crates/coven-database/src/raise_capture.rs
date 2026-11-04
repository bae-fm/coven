//! The original queue prefix captured for an unpublished breaking raise.
//!
//! A capture fixes eligibility through its marker. Snapshot bytes are made from
//! the counted base only after that prefix has acquired slots; later local
//! originals remain outside it.
use crate::write_encoding::{audience, audience_text, counter};
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
        db.internal_execute("INSERT INTO _coven_raise_captures(audience,version,through,covers) VALUES(?1,?2,?3,?4)",params![audience_text(&selected),version,marker.number.to_be_bytes(),crate::sequence::applied(db)?.to_be_bytes()])?;
    }
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
    let minimum: u32 =
        db.query_row("SELECT minimum FROM _coven_snapshot_schema WHERE singleton=1", [], |r| {
            r.get(0)
        })?;
    if minimum == 0 || !provisional(db)?.is_empty() {
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
        params![crate::sequence::applied(db)?.to_be_bytes(), audience_text(selected), version],
    )?;
    Ok(captures(db, device)?
        .into_iter()
        .find(|capture| capture.audience == *selected && capture.version == version))
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
        |r| Ok((audience(&r.get::<_, String>(0)?)?, r.get(1)?, counter(r.get(2)?)?)),
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
                audience: audience(&r.get::<_, String>(0)?)?,
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
) -> Result<(), crate::SnapshotWriteError<E>> {
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
        params![audience_text(audience), version],
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
        params![audience_text(audience), version],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "raise_capture_tests.rs"]
pub(crate) mod tests;
