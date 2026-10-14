//! Retired devices are drained once (§6): after a kept entry retiring the
//! device has been final for more than one day and five minutes, its three
//! logs are read forward to a miss once and it is never probed again.
//!
//! `_coven_retirement_finality` keeps the qualifying storage time `F` of the
//! finality observation that first found a kept retiring entry final, saved in
//! that observation's transaction. `_coven_drained_devices` marks a completed
//! drain with the entry that justified it. Replay removes both in its own
//! transaction when that entry is no longer kept. Neither is posted or
//! included in snapshots.

use crate::{
    sqlite::DatabaseConnection, store_log_tables::entry_id, write_encoding::counter, DbError,
    EntryId,
};
use coven_foundation::id_source::DeviceId;
use rusqlite::params;
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

/// One day for any request in flight to land, plus the five-minute catch-up
/// freshness every sender requires (§6, §10).
pub const DRAIN_WAIT: Duration = Duration::from_secs(24 * 60 * 60 + 5 * 60);

/// A kept, final entry retiring `device` and the storage time `final_at` (`F`)
/// sampled before the finality observation that established it (§6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetirementFinality {
    /// The retired device.
    pub device: DeviceId,
    /// The kept, final entry retiring it.
    pub entry: EntryId,
    /// `F`: the qualifying sample `T` of that finality observation, never the
    /// entry's own publication time.
    pub final_at: SystemTime,
}

impl RetirementFinality {
    /// Reads that follow storage time `sample` can drain this device only when
    /// `sample > F + one day + five minutes`; equality is insufficient.
    pub fn drain_permitted(&self, sample: SystemTime) -> bool {
        sample > self.final_at + DRAIN_WAIT
    }

    /// The first storage time strictly past the wait, for scheduling a time
    /// probe; a device clock reaching it only schedules that check.
    pub fn drain_due(&self) -> SystemTime {
        self.final_at + DRAIN_WAIT
    }
}

fn device_bytes(device: DeviceId) -> [u8; 8] {
    device.0.to_be_bytes()
}

/// Record `F = sample` for every device retired by a kept entry stored strictly
/// before `horizon` that has no finality evidence yet, choosing its earliest
/// such entry. Called in the transaction that advances the horizon, so the
/// evidence and the observation commit together. Existing evidence is never
/// refreshed: a later `F` would only delay the drain.
pub(crate) fn record_finality(
    db: &DatabaseConnection,
    horizon: Duration,
    sample: SystemTime,
) -> Result<(), DbError> {
    let retirements = db.query(
        "SELECT l.retires_device,l.device,l.number,l.stored_at FROM _coven_store_log l
         WHERE l.retires_device IS NOT NULL AND l.outcome='kept'
         UNION ALL
         SELECT d.device,l.device,l.number,l.stored_at FROM _coven_store_log l
         JOIN _coven_devices d ON d.member=l.retires_member
         WHERE l.retires_member IS NOT NULL AND l.outcome='kept'
         ORDER BY 1,4,2,3",
        [],
        |row| {
            Ok((
                DeviceId(counter(row.get(0)?)),
                entry_id(row, 1)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        },
    )?;
    let mut recorded = finalities_and_drained(db)?;
    for (device, entry, stored) in retirements {
        if recorded.contains(&device) {
            continue;
        }
        let stored_at =
            crate::user_file::decode_time(&stored).map_err(|_| DbError::DamagedDatabase)?;
        if stored_at
            .duration_since(std::time::UNIX_EPOCH)
            .is_ok_and(|time| time >= horizon)
        {
            continue;
        }
        db.internal_execute(
            "INSERT INTO _coven_retirement_finality(device,entry_device,entry_number,final_at)
             VALUES(?1,?2,?3,?4)",
            params![
                device_bytes(device).as_slice(),
                device_bytes(entry.device).as_slice(),
                entry.number.to_be_bytes().as_slice(),
                crate::user_file::encode_time(sample),
            ],
        )?;
        recorded.insert(device);
    }
    Ok(())
}

/// In the replay transaction: forget finality evidence, and with it any drain
/// mark, whose retiring entry is no longer kept. The device is polled again.
pub(crate) fn forget_dropped(db: &DatabaseConnection) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_retirement_finality WHERE NOT EXISTS(
            SELECT 1 FROM _coven_store_log l WHERE l.device=entry_device
            AND l.number=entry_number AND l.outcome='kept')",
        [],
    )?;
    Ok(())
}

pub(crate) fn finalities(db: &DatabaseConnection) -> Result<Vec<RetirementFinality>, DbError> {
    db.query(
        "SELECT f.device,f.entry_device,f.entry_number,f.final_at FROM _coven_retirement_finality f
         WHERE NOT EXISTS(SELECT 1 FROM _coven_drained_devices d WHERE d.device=f.device)
         ORDER BY f.device",
        [],
        |row| {
            Ok((
                DeviceId(counter(row.get(0)?)),
                entry_id(row, 1)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        },
    )?
    .into_iter()
    .map(|(device, entry, time)| {
        Ok(RetirementFinality {
            device,
            entry,
            final_at: crate::user_file::decode_time(&time).map_err(|_| DbError::DamagedDatabase)?,
        })
    })
    .collect()
}

/// Every device with finality evidence, drained or not.
fn finalities_and_drained(db: &DatabaseConnection) -> Result<BTreeSet<DeviceId>, DbError> {
    Ok(db
        .query("SELECT device FROM _coven_retirement_finality", [], |row| {
            Ok(DeviceId(counter(row.get(0)?)))
        })?
        .into_iter()
        .collect())
}

pub(crate) fn drained(db: &DatabaseConnection) -> Result<BTreeSet<DeviceId>, DbError> {
    Ok(db
        .query("SELECT device FROM _coven_drained_devices", [], |row| {
            Ok(DeviceId(counter(row.get(0)?)))
        })?
        .into_iter()
        .collect())
}

/// Mark a completed drain, checking in the same transaction that its entry is
/// still kept and the reads followed `sample > F + one day + five minutes`.
pub(crate) fn mark(
    db: &DatabaseConnection,
    drain: RetirementFinality,
    sample: SystemTime,
) -> Result<bool, DbError> {
    let current = finalities(db)?
        .into_iter()
        .find(|saved| saved.device == drain.device);
    let Some(current) = current else {
        return Ok(false);
    };
    let kept: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_store_log WHERE device=?1 AND number=?2 AND outcome='kept')",
        params![
            device_bytes(current.entry.device).as_slice(),
            current.entry.number.to_be_bytes().as_slice()
        ],
        |row| row.get(0),
    )?;
    if current != drain || !kept || !current.drain_permitted(sample) {
        return Ok(false);
    }
    db.internal_execute(
        "INSERT INTO _coven_drained_devices(device,entry_device,entry_number) VALUES(?1,?2,?3)",
        params![
            device_bytes(current.device).as_slice(),
            device_bytes(current.entry.device).as_slice(),
            current.entry.number.to_be_bytes().as_slice(),
        ],
    )?;
    Ok(true)
}

#[cfg(test)]
#[path = "retirement_drain_tests.rs"]
mod tests;
