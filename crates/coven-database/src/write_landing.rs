//! A received write's own retirement window (§10, D5): its first complete
//! publication against every kept entry retiring its device. The verdict is
//! derived from storage times and kept entries, so replay recomputes it; the
//! write's inputs stay retained while a non-final retirement could change it.

use crate::{
    sqlite::DatabaseConnection,
    store_log_tables::entry_id,
    write_encoding::{audience_text, counter},
    DbError, EntryId, PendingValue,
};
use coven_format::value::StorageTime;
use coven_format::{merge_fields, store_log::StoreChange, write::WriteHeader};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use rusqlite::params;
use std::collections::BTreeSet;
use std::time::Duration;

/// A retired write counts when published no more than thirty storage days after
/// each kept entry retiring its device; exactly thirty days counts (§10).
pub(crate) const WRITE_WINDOW: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// A kept entry retiring a write's device, with its first publication time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Retirement {
    /// The removal of the device or its member, or the replacement of its id.
    pub(crate) entry: EntryId,
    /// The entry's first complete publication time.
    pub(crate) stored_at: StorageTime,
}

/// Lean `Receive.withinWindow`: the write landed within the window of every
/// kept retirement of its device, read or not. A write whose own recorded view
/// retires its device was already refused by its authority check (§7.1, §10).
pub(crate) fn within_window(stored_at: StorageTime, kept: &[Retirement]) -> bool {
    kept.iter().all(|r| within(stored_at, r.stored_at))
}

/// Whether `stored_at` is no more than thirty storage days after `start`,
/// compared exactly at the provider's precision.
fn within(stored_at: StorageTime, start: StorageTime) -> bool {
    stored_at
        .duration_since(start)
        .is_none_or(|after| after <= WRITE_WINDOW)
}

/// The device and member columns `_coven_store_log` keeps for an entry that
/// retires a device: removal of the device or its member, or replacement (§10).
pub(crate) fn retires(
    entry: &coven_format::store_log::StoreLogEntry,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    match &entry.change {
        StoreChange::RemoveDevice { device }
        | StoreChange::AddDevice {
            replaces: Some(device),
            ..
        } => (Some(device.0.to_be_bytes().to_vec()), None),
        StoreChange::RemoveMember { member } => (None, Some(member.0.as_bytes().to_vec())),
        _ => (None, None),
    }
}

/// Every entry retiring `device`, except permanently late ones, with whether replay keeps it and
/// whether it is final (stored strictly before the horizon, §9).
fn candidates(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Vec<(Retirement, bool, bool)>, DbError> {
    let horizon = crate::store_log_tables::horizon(db)?;
    let rows = db.query(
        "SELECT device,number,stored_at,outcome='kept' FROM _coven_store_log
         WHERE retires_device=?1 AND outcome<>'late'
         UNION ALL
         SELECT device,number,stored_at,outcome='kept' FROM _coven_store_log
         WHERE retires_member=(SELECT member FROM _coven_devices WHERE device=?1) AND outcome<>'late'",
        [device.0.to_be_bytes().as_slice()],
        |r| {
            Ok((
                entry_id(r, 0)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, bool>(3)?,
            ))
        },
    )?;
    rows.into_iter()
        .map(|(entry, stored, kept)| {
            let stored_at = crate::storage_time::decode(&stored)?;
            let fixed = horizon.is_some_and(|h| stored_at < h);
            Ok((Retirement { entry, stored_at }, kept, fixed))
        })
        .collect()
}

/// The window verdict and whether it is final (§10): a late verdict once a
/// final kept retirement excludes the write (`retirement_verdict_final`); an
/// admitted one once every retirement that could exclude it, stored more than
/// thirty days before the write, lies in the final prefix.
pub(crate) struct Landing {
    pub(crate) late: bool,
    pub(crate) fixed: bool,
}

pub(crate) fn judge(
    db: &DatabaseConnection,
    header: &WriteHeader,
    stored_at: StorageTime,
) -> Result<Landing, DbError> {
    let candidates = candidates(db, header.position.device)?;
    let kept: Vec<_> = candidates
        .iter()
        .filter(|(_, kept, _)| *kept)
        .map(|(r, _, _)| *r)
        .collect();
    let late = !within_window(stored_at, &kept);
    let fixed = if late {
        candidates
            .iter()
            .any(|(r, kept, fixed)| *kept && *fixed && !within_window(stored_at, &[*r]))
    } else {
        settled(crate::store_log_tables::horizon(db)?, stored_at)
    };
    Ok(Landing { late, fixed })
}

/// Judge a write on its first application (§10). An unsettled verdict is
/// recorded so replay can recompute it; with `retain`, a write whose verdict a
/// non-final or unseen retirement could still change first retains its
/// audiences' current data, so its parts and every later one are kept: replay
/// then rebuilds rows, source verdicts and losses from that input in its own
/// transaction, whichever way the verdict turns. A reload's own data is not
/// such an origin: reversing its verdict reloads again. Returns whether it is
/// late.
pub(crate) fn land<P: PendingValue>(
    db: &DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    header: &WriteHeader,
    stored_at: StorageTime,
    audiences: &[Audience],
    retain: bool,
) -> Result<bool, DbError> {
    let landing = judge(db, header, stored_at)?;
    if retain && !landing.fixed {
        for audience in audiences {
            crate::entry_inputs::original(db, schema, audience)?;
        }
    }
    if landing.late || !landing.fixed {
        record(
            db,
            header,
            stored_at,
            landing.late,
            audiences.iter().cloned(),
        )?;
    }
    if landing.late {
        let own = db.pending_device().ok_or(DbError::DamagedDatabase)?;
        report::<P>(db, header.position, own, true)?;
    }
    Ok(landing.late)
}

/// Record a received write's publication time, verdict and opened audiences.
fn record(
    db: &DatabaseConnection,
    header: &WriteHeader,
    stored_at: StorageTime,
    late: bool,
    audiences: impl IntoIterator<Item = Audience>,
) -> Result<(), DbError> {
    let [device, number] = id(header.position);
    db.internal_execute(
        "INSERT INTO _coven_write_landings(device,number,stored_at,header,late) VALUES(?1,?2,?3,?4,?5)
         ON CONFLICT(device,number) DO UPDATE SET late=excluded.late",
        params![
            device,
            number,
            crate::storage_time::encode(stored_at),
            merge_fields::encode_write_header(header)?,
            late
        ],
    )?;
    for audience in audiences {
        db.internal_execute(
            "INSERT INTO _coven_write_landing_parts(device,number,audience) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
            params![device, number, audience_text(&audience)],
        )?;
    }
    Ok(())
}

fn id(write: WriteId) -> [Vec<u8>; 2] {
    [
        write.device.0.to_be_bytes().to_vec(),
        write.number.to_be_bytes().to_vec(),
    ]
}

/// Whether the current verdict consumes this write without effects or losses.
pub(crate) fn late(db: &DatabaseConnection, write: WriteId) -> Result<bool, DbError> {
    let [device, number] = id(write);
    Ok(db
        .query_row(
            "SELECT (SELECT late FROM _coven_write_landings WHERE device=?1 AND number=?2)",
            params![device, number],
            |r| r.get::<_, Option<bool>>(0),
        )?
        .unwrap_or(false))
}

/// Writes whose verdict replay changed, by each audience they opened, and the
/// retiring entries deciding them.
#[derive(Default)]
pub(crate) struct Rejudged {
    pub(crate) audiences: std::collections::BTreeMap<Audience, Vec<WriteId>>,
    pub(crate) causes: BTreeSet<EntryId>,
}

/// Recompute every retained verdict against the replay just committed. Each
/// changed write's report changes with it; the audiences it opened must be
/// rebuilt in this transaction (§9, §10).
pub(crate) fn rejudge<P: PendingValue>(db: &DatabaseConnection) -> Result<Rejudged, DbError> {
    let landings = db.query(
        "SELECT device,number,stored_at,header,late FROM _coven_write_landings",
        [],
        |r| {
            Ok((
                WriteId {
                    device: DeviceId(counter(r.get(0)?)),
                    number: counter(r.get(1)?),
                },
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, bool>(4)?,
            ))
        },
    )?;
    let own = db.pending_device().ok_or(DbError::DamagedDatabase)?;
    let mut rejudged = Rejudged::default();
    for (write, stored, header, was_late) in landings {
        let stored_at = crate::storage_time::decode(&stored)?;
        let header = merge_fields::decode_write_header(&header)?;
        let landing = judge(db, &header, stored_at)?;
        if landing.late == was_late {
            continue;
        }
        let [device, number] = id(write);
        db.internal_execute(
            "UPDATE _coven_write_landings SET late=?3 WHERE device=?1 AND number=?2",
            params![device, number, landing.late],
        )?;
        report::<P>(db, write, own, landing.late)?;
        rejudged.causes.extend(
            candidates(db, write.device)?
                .into_iter()
                .map(|(r, _, _)| r.entry),
        );
        for audience in audiences(db, write)? {
            rejudged.audiences.entry(audience).or_default().push(write);
        }
    }
    Ok(rejudged)
}

/// A loaded snapshot's verdict for a covered change must agree with this
/// device's own window verdict for its write, when it holds one: only then do
/// the snapshot's contributions count what this device's replay counts (§10).
/// A dropped verdict and only it means the snapshot's author judged the write
/// outside its window; every other verdict, unjudged included, kept its effects.
pub(crate) fn agrees(
    db: &DatabaseConnection,
    authored: &coven_format::snapshot_rows::AuthoredChange,
) -> Result<(), DbError> {
    let write = authored.header.position;
    let [device, number] = id(write);
    let judged: Option<bool> = db.query_row(
        "SELECT (SELECT late FROM _coven_write_landings WHERE device=?1 AND number=?2)",
        params![device, number],
        |r| r.get(0),
    )?;
    let dropped = matches!(
        authored.verdict,
        coven_format::snapshot_rows::ChangeVerdict::Dropped
    );
    match judged {
        Some(late) if late != dropped => {
            Err(crate::SnapshotError::RetirementVerdict { write }.into())
        }
        _ => Ok(()),
    }
}

/// A late write is reported as dropped for its own subject; admission clears it.
pub(crate) fn report<P: PendingValue>(
    db: &DatabaseConnection,
    write: WriteId,
    reporter: DeviceId,
    late: bool,
) -> Result<(), DbError> {
    if late {
        crate::pending::save(
            db,
            &P::dropped_write(write, crate::DropReason::LandedTooLate, reporter),
        )
    } else {
        crate::pending::clear_log(db, crate::LogObject::Write(write))
    }
}

/// Every recorded verdict, with whether it is final now.
fn recorded(db: &DatabaseConnection) -> Result<Vec<(WriteId, Landing)>, DbError> {
    let rows = db.query(
        "SELECT device,number,stored_at,header FROM _coven_write_landings",
        [],
        |r| {
            Ok((
                WriteId {
                    device: DeviceId(counter(r.get(0)?)),
                    number: counter(r.get(1)?),
                },
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, Vec<u8>>(3)?,
            ))
        },
    )?;
    rows.into_iter()
        .map(|(write, stored, header)| {
            let stored_at = crate::storage_time::decode(&stored)?;
            let header = merge_fields::decode_write_header(&header)?;
            Ok((write, judge(db, &header, stored_at)?))
        })
        .collect()
}

fn audiences(db: &DatabaseConnection, write: WriteId) -> Result<Vec<Audience>, DbError> {
    let [device, number] = id(write);
    db.query(
        "SELECT audience FROM _coven_write_landing_parts WHERE device=?1 AND number=?2",
        params![device, number],
        |r| crate::write_encoding::audience(&r.get::<_, String>(0)?),
    )
}

/// Audiences whose retained inputs a verdict that can still change needs.
pub(crate) fn open_audiences(db: &DatabaseConnection) -> Result<BTreeSet<Audience>, DbError> {
    let mut open = BTreeSet::new();
    for (write, landing) in recorded(db)? {
        if !landing.fixed {
            open.extend(audiences(db, write)?);
        }
    }
    Ok(open)
}

/// Every retirement that could exclude a write published at `stored_at` was
/// stored more than thirty days earlier; once the horizon reaches that point,
/// all of them are final and known (§9, §10).
fn settled(horizon: Option<StorageTime>, stored_at: StorageTime) -> bool {
    horizon.is_some_and(|h| within(stored_at, h))
}

/// Forget final verdicts. A late one stays while an audience it opened
/// retains inputs, whose rebuild must exclude it again.
pub(crate) fn release(db: &DatabaseConnection) -> Result<(), DbError> {
    for (write, landing) in recorded(db)? {
        if !landing.fixed {
            continue;
        }
        let [device, number] = id(write);
        let retained: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_write_landing_parts p JOIN _coven_data_inputs i
             ON i.audience=p.audience WHERE p.device=?1 AND p.number=?2)",
            params![device, number],
            |r| r.get(0),
        )?;
        if !(landing.late && retained) {
            db.internal_execute(
                "DELETE FROM _coven_write_landings WHERE device=?1 AND number=?2",
                params![device, number],
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "write_landing_tests.rs"]
pub(crate) mod tests;
