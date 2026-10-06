//! Loaded audiences stay known even when empty. Their coverage is discarded
//! once the common positions catch up; later reloads retain the audience itself.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text, counter, decoded, encoded};
use crate::DbError;
use coven_format::{merge_fields, value::WritePositions};
use coven_merge::{Audience, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn frontier(
    database: &DatabaseConnection,
    audience: &Audience,
) -> Result<WritePositions, DbError> {
    let mut result = crate::download::positions(database)?;
    let saved = database.query(
        "SELECT writes FROM coven_snapshot_coverage WHERE audience=?1 AND writes IS NOT NULL",
        [audience_text(audience)],
        |r| {
            decoded(merge_fields::decode_write_positions(
                &r.get::<_, Vec<u8>>(0)?,
            ))
        },
    )?;
    for saved in saved {
        extend(&mut result, &saved);
    }
    Ok(result)
}

pub(crate) fn loaded(
    database: &DatabaseConnection,
    selected: &Audience,
    positions: &WritePositions,
) -> Result<(), DbError> {
    let audiences: BTreeSet<_>=database.query("SELECT audience FROM coven_snapshot_coverage UNION SELECT audience FROM coven_rows UNION SELECT audience FROM coven_lost UNION SELECT 'store'",[],|r|audience(&r.get::<_,String>(0)?))?.into_iter().chain(std::iter::once(selected.clone())).collect();
    for audience in audiences {
        let head = if audience == *selected {
            positions.clone()
        } else {
            frontier(database, &audience)?
        };
        database.internal_execute("INSERT INTO coven_snapshot_coverage(audience,writes) VALUES(?1,?2) ON CONFLICT(audience) DO UPDATE SET writes=excluded.writes",params![audience_text(&audience),encoded(merge_fields::encode_write_positions(&head))?])?;
    }
    let frontiers = database.query(
        "SELECT writes FROM coven_snapshot_coverage WHERE writes IS NOT NULL",
        [],
        |r| {
            decoded(merge_fields::decode_write_positions(
                &r.get::<_, Vec<u8>>(0)?,
            ))
        },
    )?;
    let devices: BTreeSet<_> = frontiers
        .iter()
        .flat_map(|p| p.0.iter().map(|p| p.device))
        .collect();
    database.internal_execute("DELETE FROM coven_positions", [])?;
    for device in devices {
        let minimum = frontiers
            .iter()
            .map(|frontier| {
                frontier
                    .0
                    .iter()
                    .find(|p| p.device == device)
                    .map_or(0, |p| p.number)
            })
            .min()
            .expect("loaded audience");
        if minimum > 0 {
            advance(
                database,
                WriteId {
                    device,
                    number: minimum,
                },
            )?;
        }
    }
    database.internal_execute("DELETE FROM coven_snapshot_pending", [])?;
    database.internal_execute(
        "INSERT INTO coven_snapshot_pending(device,number)
         SELECT substr(timestamp,9,8),max(number) FROM coven_writes GROUP BY substr(timestamp,9,8)
         HAVING max(number)>COALESCE((SELECT p.number FROM coven_positions p WHERE p.device=substr(timestamp,9,8)),x'0000000000000000')", [],
    )?;
    database.internal_execute(
        "DELETE FROM coven_snapshot_parts WHERE audience=?1",
        [audience_text(selected)],
    )?;
    discard_passed(database)
}

pub(crate) fn covers(
    database: &DatabaseConnection,
    audience: &Audience,
    write: WriteId,
) -> Result<bool, DbError> {
    if frontier(database, audience)?.covers(write) {
        return Ok(true);
    }
    database.query_row("SELECT EXISTS(SELECT 1 FROM coven_snapshot_parts WHERE audience=?1 AND device=?2 AND number=?3)",params![audience_text(audience),write.device.0.to_be_bytes().as_slice(),write.number.to_be_bytes().as_slice()],|r|r.get(0))
}

pub(crate) fn missing_past(
    database: &DatabaseConnection,
    audience: &Audience,
    header: &coven_format::write::WriteHeader,
) -> Result<Vec<WriteId>, DbError> {
    let mut missing = Vec::new();
    for write in &header.had_read.0 {
        if !covers(database, audience, *write)? {
            missing.push(*write);
        }
    }
    if header.position.number > 1 {
        let prior = WriteId {
            number: header.position.number - 1,
            ..header.position
        };
        if !covers(database, audience, prior)? {
            missing.push(prior);
        }
    }
    missing.sort();
    Ok(missing)
}

pub(crate) fn replayed(
    database: &DatabaseConnection,
    audience: &Audience,
    write: WriteId,
) -> Result<(), DbError> {
    database.internal_execute("INSERT INTO coven_snapshot_parts(audience,device,number) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",params![audience_text(audience),write.device.0.to_be_bytes().as_slice(),write.number.to_be_bytes().as_slice()])?;
    Ok(())
}

pub(crate) fn committed(
    database: &DatabaseConnection,
    write: &coven_format::write::WriteRecord,
) -> Result<(), DbError> {
    let position = write.header.position;
    let positions = crate::download::positions(database)?;
    let current = positions
        .0
        .iter()
        .find(|p| p.device == position.device)
        .map_or(0, |p| p.number);
    if position.number <= current {
        return Ok(());
    }
    if position.number == current + 1
        && write
            .header
            .had_read
            .0
            .iter()
            .all(|past| positions.covers(*past))
    {
        advance(database, position)?;
        discard_passed(database)?;
    } else {
        for part in &write.parts {
            replayed(database, &part.audience, position)?;
        }
    }
    Ok(())
}

pub(crate) fn require_caught_up(database: &DatabaseConnection) -> Result<(), DbError> {
    // A loaded audience or an earlier local state can have identities beyond
    // the common frontier. None can be omitted from a new write's causal read.
    let missing = database.query(
        "SELECT device,number FROM coven_snapshot_pending ORDER BY device",
        [],
        |r| {
            Ok(WriteId {
                device: coven_foundation::id_source::DeviceId(counter(r.get(0)?)),
                number: counter(r.get(1)?),
            })
        },
    )?;
    if missing.is_empty() {
        Ok(())
    } else {
        Err(DbError::ReloadPending { writes: missing })
    }
}

pub(crate) fn advance(database: &DatabaseConnection, write: WriteId) -> Result<(), DbError> {
    database.internal_execute("INSERT INTO coven_positions(device,number) VALUES(?1,?2) ON CONFLICT(device) DO UPDATE SET number=excluded.number",params![write.device.0.to_be_bytes().as_slice(),write.number.to_be_bytes().as_slice()])?;
    Ok(())
}

fn discard_passed(database: &DatabaseConnection) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM coven_snapshot_pending WHERE number<=(SELECT number FROM coven_positions p WHERE p.device=coven_snapshot_pending.device)",[])?;
    let positions = crate::download::positions(database)?;
    let covered = database.query(
        "SELECT audience,writes FROM coven_snapshot_coverage WHERE writes IS NOT NULL",
        [],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                decoded(merge_fields::decode_write_positions(
                    &r.get::<_, Vec<u8>>(1)?,
                ))?,
            ))
        },
    )?;
    for (audience, frontier) in covered {
        if frontier.0.iter().all(|p| positions.covers(*p)) {
            database.internal_execute(
                "UPDATE coven_snapshot_coverage SET writes=NULL WHERE audience=?1",
                [audience],
            )?;
        }
    }
    database.internal_execute("DELETE FROM coven_snapshot_parts WHERE number<=(SELECT number FROM coven_positions p WHERE p.device=coven_snapshot_parts.device)",[])?;
    Ok(())
}

fn extend(frontier: &mut WritePositions, other: &WritePositions) {
    let mut positions: BTreeMap<_, _> = frontier.0.iter().map(|p| (p.device, p.number)).collect();
    for p in &other.0 {
        positions
            .entry(p.device)
            .and_modify(|n| *n = (*n).max(p.number))
            .or_insert(p.number);
    }
    frontier.0 = positions
        .into_iter()
        .map(|(device, number)| WriteId { device, number })
        .collect();
}
