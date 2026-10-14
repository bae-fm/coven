//! A breaking migration's raise snapshots, captured in the migration's own
//! transaction through its empty marker, so no later app write can extend the
//! cut (§17.1, §18.1). Sync confirms the captured own prefix, then seals and
//! publishes these frames; the header gains its snapshot identity only then.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text, counter, decoded, encoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::merge_fields;
use coven_format::snapshot::{SnapshotEncoder, SnapshotHeader, SECTIONS};
use coven_format::store_log::SnapshotId;
use coven_format::value::WritePositions;
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
        let text = audience_text(&selected);
        let id = SnapshotId {
            audience: selected,
            device: marker.device,
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
                        marker.number.to_be_bytes().as_slice(),
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
            crate::SnapshotWriteError::Database(error)
            | crate::SnapshotWriteError::Output(error) => error,
            crate::SnapshotWriteError::Format(error) => error.into(),
        })?;
    }
    Ok(true)
}

pub(crate) fn captures(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Vec<RaiseCapture>, DbError> {
    db.query(
        "SELECT audience,version,through,writes FROM _coven_raise_captures ORDER BY audience",
        [],
        |r| {
            Ok(RaiseCapture {
                audience: audience(&r.get::<_, String>(0)?)?,
                version: r.get(1)?,
                through: WriteId {
                    device,
                    number: counter(r.get(2)?),
                },
                writes: decoded(merge_fields::decode_write_positions(
                    &r.get::<_, Vec<u8>>(3)?,
                ))?,
            })
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

/// The first own queued write the captured prefix still needs confirmed.
pub(crate) fn unconfirmed(
    db: &DatabaseConnection,
    through: WriteId,
) -> Result<Option<WriteId>, DbError> {
    Ok(db
        .query(
            "SELECT number FROM _coven_uploads WHERE device=?1 AND number<=?2 ORDER BY number LIMIT 1",
            params![
                through.device.0.to_be_bytes().as_slice(),
                through.number.to_be_bytes().as_slice()
            ],
            |r| {
                Ok(WriteId {
                    device: through.device,
                    number: counter(r.get(0)?),
                })
            },
        )?
        .pop())
}

#[cfg(test)]
#[path = "raise_capture_tests.rs"]
pub(crate) mod tests;
