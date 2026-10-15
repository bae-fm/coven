//! A breaking migration's raise snapshots, captured in the migration's own
//! transaction through its empty marker, so no later app write can extend the
//! cut (§17.1, §18.1). A capture is sent only after a completed catch-up that
//! applied nothing past it; otherwise it is captured again from the current
//! state, and the device's later writes join its prefix (§15). Sync seals
//! these frames, the header gaining its snapshot identity only then, and
//! sends them once the captured own prefix is confirmed.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text, counter, decoded, encoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::merge_fields;
use coven_format::snapshot::{SnapshotEncoder, SnapshotHeader, SECTIONS};
use coven_format::store_log::SnapshotId;
use coven_format::value::{EntryPositions, WritePositions};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use rusqlite::params;

/// One audience's captured raise: what its snapshot covers and the last own
/// write, the migration marker, that must be confirmed before it is sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaiseCapture {
    /// The audience the snapshot raises.
    pub audience: Audience,
    /// The schema version the raise publishes.
    pub version: u32,
    /// The migration marker: every own write through it precedes the snapshot.
    pub through: WriteId,
    /// The captured write positions, the snapshot's coverage.
    pub writes: WritePositions,
}

/// Capture every audience this device reads, replacing a capture an earlier,
/// still unpublished migration left. Without a store there is nothing to
/// raise; returns whether anything was captured.
pub(crate) fn capture(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    marker: WriteId,
    version: u32,
) -> Result<bool, DbError> {
    let log = crate::store_log_tables::read(db)?;
    let state = &log.replay.state;
    db.internal_execute("DELETE FROM _coven_raise_captures", [])?;
    let Some(member) = state
        .store
        .as_ref()
        .and_then(|_| state.devices.get(&marker.device))
        .map(|device| device.member)
    else {
        return Ok(false);
    };
    let audiences = std::iter::once(Audience::Store).chain(
        state
            .readable_circles(&member)
            .into_iter()
            .map(Audience::Circle),
    );
    for selected in audiences {
        capture_audience(db, schema, selected, marker, version)?;
    }
    Ok(true)
}

/// Capture one audience's current state through `through`, this device's
/// newest write, replacing any earlier capture of it.
fn capture_audience(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    selected: Audience,
    through: WriteId,
    version: u32,
) -> Result<(), DbError> {
    let text = audience_text(&selected);
    db.internal_execute(
        "DELETE FROM _coven_raise_captures WHERE audience=?1",
        [&text],
    )?;
    let id = SnapshotId {
        audience: selected,
        device: through.device,
        number: 1,
    };
    let mut sequence = 0i64;
    crate::snapshot_write::write(
        db,
        schema,
        id,
        |header| {
            db.internal_execute(
                "INSERT INTO _coven_raise_captures(audience,version,through,schema_version,writes,store_log,counts) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    text,
                    version,
                    through.number.to_be_bytes().as_slice(),
                    header.schema_version,
                    encoded(merge_fields::encode_write_positions(&header.writes))?,
                    encoded(merge_fields::encode_entry_positions(&header.store_log))?,
                    header.counts.iter().flat_map(|n| n.to_be_bytes()).collect::<Vec<_>>(),
                ],
            )?;
            Ok(())
        },
        |frame| {
            // The header frame is encoded again with the published identity.
            if sequence > 0 {
                db.internal_execute(
                    "INSERT INTO _coven_raise_capture_frames(audience,sequence,frame) VALUES(?1,?2,?3)",
                    params![text, sequence, frame],
                )?;
            }
            sequence += 1;
            Ok::<_, DbError>(())
        },
    )
    .map_err(|error| match error {
        crate::SnapshotWriteError::Database(error) | crate::SnapshotWriteError::Output(error) => {
            error
        }
        crate::SnapshotWriteError::Format(error) => error.into(),
    })?;
    Ok(())
}

/// Before an audience's raise snapshot is prepared, after a completed
/// catch-up: keep its capture if that catch-up applied nothing past it,
/// otherwise capture the audience again through this device's newest write
/// (§15's preparation rule; io `RaiseHistory`). A capture also goes stale
/// once another audience's capture let own writes past its `through` upload.
/// Returns the capture to send, or none when a later migration replaced it.
pub(crate) fn prepare(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    device: DeviceId,
    selected: &Audience,
    version: u32,
) -> Result<Option<RaiseCapture>, DbError> {
    let Some(capture) = captures(db, device)?
        .into_iter()
        .find(|c| c.audience == *selected && c.version == version)
    else {
        return Ok(None);
    };
    let positions = crate::download::positions(db)?;
    let remote = |positions: &WritePositions| -> Vec<WriteId> {
        positions
            .0
            .iter()
            .copied()
            .filter(|id| id.device != device)
            .collect()
    };
    let entries: EntryPositions = decoded(merge_fields::decode_entry_positions(&db.query_row(
        "SELECT store_log FROM _coven_raise_captures WHERE audience=?1",
        [audience_text(selected)],
        |r| r.get::<_, Vec<u8>>(0),
    )?))?;
    let sendable = captured_through(db)?.unwrap_or(0);
    if remote(&capture.writes) == remote(&positions)
        && entries == crate::store_log::positions(db)?
        && capture.through.number >= sendable
    {
        return Ok(Some(capture));
    }
    let own = positions
        .0
        .iter()
        .find(|id| id.device == device)
        .copied()
        .ok_or(DbError::Invariant {
            what: "a captured device has its own migration marker",
        })?;
    capture_audience(db, schema, selected.clone(), own, version)?;
    Ok(captures(db, device)?
        .into_iter()
        .find(|c| c.audience == *selected))
}

/// The newest own write any capture's prefix includes: the upload gate sends
/// own writes through it before a raise is kept (§17.1).
pub(crate) fn captured_through(db: &DatabaseConnection) -> Result<Option<u64>, DbError> {
    Ok(rows(db)?
        .into_iter()
        .map(|(_, _, through, _)| through)
        .max())
}

/// Received writes are interpreted under the captures as if their raises
/// were kept: an uncovered older-version part is converted or excluded at
/// the capture's version, as every device does once the raise is kept.
pub(crate) fn provisional(
    db: &DatabaseConnection,
) -> Result<Vec<(Audience, u32, WritePositions)>, DbError> {
    Ok(rows(db)?
        .into_iter()
        .map(|(audience, version, _, writes)| (audience, version, writes))
        .collect())
}

pub(crate) fn captures(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Vec<RaiseCapture>, DbError> {
    Ok(rows(db)?
        .into_iter()
        .map(|(audience, version, through, writes)| RaiseCapture {
            audience,
            version,
            through: WriteId {
                device,
                number: through,
            },
            writes,
        })
        .collect())
}

fn rows(db: &DatabaseConnection) -> Result<Vec<(Audience, u32, u64, WritePositions)>, DbError> {
    db.query(
        "SELECT audience,version,through,writes FROM _coven_raise_captures ORDER BY audience",
        [],
        |r| {
            Ok((
                audience(&r.get::<_, String>(0)?)?,
                r.get(1)?,
                counter(r.get(2)?),
                decoded(merge_fields::decode_write_positions(
                    &r.get::<_, Vec<u8>>(3)?,
                ))?,
            ))
        },
    )
}

/// Stream the captured plaintext under its published identity: a header
/// frame for `id`, then the captured records and end marker unchanged.
pub(crate) fn write<E>(
    db: &DatabaseConnection,
    id: SnapshotId,
    version: u32,
    begin: impl FnOnce(&SnapshotHeader) -> Result<(), E>,
    mut emit: impl FnMut(Vec<u8>) -> Result<(), E>,
) -> Result<(), crate::SnapshotWriteError<E>> {
    let text = audience_text(&id.audience);
    let captured = db
        .query(
            "SELECT version,schema_version,writes,store_log,counts FROM _coven_raise_captures WHERE audience=?1",
            [&text],
            |r| {
                Ok((
                    r.get::<_, u32>(0)?,
                    r.get::<_, u32>(1)?,
                    decoded(merge_fields::decode_write_positions(&r.get::<_, Vec<u8>>(2)?))?,
                    decoded(merge_fields::decode_entry_positions(&r.get::<_, Vec<u8>>(3)?))?,
                    r.get::<_, Vec<u8>>(4)?,
                ))
            },
        )?
        .pop();
    let Some((captured_version, schema_version, writes, store_log, counts)) = captured else {
        return Err(DbError::DamagedDatabase.into());
    };
    if captured_version != version || counts.len() != 8 * SECTIONS {
        return Err(DbError::DamagedDatabase.into());
    }
    let mut sections = [0u64; SECTIONS];
    for (section, bytes) in sections.iter_mut().zip(counts.chunks_exact(8)) {
        *section = u64::from_be_bytes(bytes.try_into().expect("eight bytes"));
    }
    let (encoder, header_frame) = SnapshotEncoder::start(SnapshotHeader {
        id,
        schema_version,
        writes,
        store_log,
        counts: sections,
    })?;
    begin(encoder.header()).map_err(crate::SnapshotWriteError::Output)?;
    emit(header_frame).map_err(crate::SnapshotWriteError::Output)?;
    db.for_each(
        "SELECT frame FROM _coven_raise_capture_frames WHERE audience=?1 ORDER BY sequence",
        [&text],
        |r| {
            emit(r.get::<_, Vec<u8>>(0).map_err(DbError::from)?)
                .map_err(crate::SnapshotWriteError::Output)
        },
    )
}

/// Forget an audience's capture once its raise is published or superseded.
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
