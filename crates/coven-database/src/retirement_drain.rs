//! Retired devices are drained once (§6): after an entry retiring the device
//! has been final for more than one day and five minutes, its three logs are
//! read forward to a miss once and it is never probed again. A kept removal or
//! replacement retires a device, and so does a dropped registration of a
//! device that no kept entry registers (`Writers.retirements`).
//!
//! `_coven_retirement_finality` keeps the qualifying storage time `F` of the
//! finality observation that first found a kept retiring entry final, saved in
//! that observation's transaction. `_coven_drained_devices` marks a completed
//! drain with the entry that justified it. Replay removes both in its own
//! transaction when that entry no longer retires the device. Neither is posted or
//! included in snapshots.

use crate::{
    sqlite::DatabaseConnection, store_log_tables::entry_id, write_encoding::counter, DbError,
    EntryId,
};
use coven_format::value::StorageTime;
use coven_foundation::id_source::DeviceId;
use rusqlite::params;
use std::collections::BTreeSet;
use std::time::Duration;

/// One day for any request in flight to land, plus the five-minute catch-up
/// freshness every sender requires (§6, §10).
pub const DRAIN_WAIT: Duration = Duration::from_secs(24 * 60 * 60 + 5 * 60);

/// A final entry retiring `device` and the storage time `final_at` (`F`)
/// sampled before the finality observation that established it (§6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetirementFinality {
    /// The retired device.
    pub device: DeviceId,
    /// The final entry retiring it: a kept removal or replacement, or a
    /// dropped registration of a device no kept entry registers.
    pub entry: EntryId,
    /// `F`: the qualifying sample `T` of that finality observation, never the
    /// entry's own publication time.
    pub final_at: StorageTime,
}

impl RetirementFinality {
    /// Reads that follow storage time `sample` can drain this device only when
    /// `sample > F + one day + five minutes`; equality is insufficient.
    pub fn drain_permitted(&self, sample: StorageTime) -> bool {
        sample
            .duration_since(self.final_at)
            .is_some_and(|waited| waited > DRAIN_WAIT)
    }

    /// `F + one day + five minutes`: the last storage time that does not yet
    /// permit the drain, for scheduling a time probe once a device clock
    /// passes it; a device clock passing it only schedules that check. `None`
    /// past the representable range, where no sample can permit the drain.
    pub fn drain_due(&self) -> Option<StorageTime> {
        self.final_at.checked_add(DRAIN_WAIT)
    }
}

fn device_bytes(device: DeviceId) -> [u8; 8] {
    device.0.to_be_bytes()
}

/// Record `F = sample` for every device retired by an entry stored strictly
/// before `horizon` that has no finality evidence yet, choosing its earliest
/// such entry: a kept removal or replacement, or a dropped registration of a
/// device no kept entry registers (`Writers.registered_or_retired`). Called in the transaction that advances the horizon, so the
/// evidence and the observation commit together. Existing evidence is never
/// refreshed: a later `F` would only delay the drain.
pub(crate) fn record_finality(
    db: &DatabaseConnection,
    horizon: StorageTime,
    sample: StorageTime,
) -> Result<(), DbError> {
    let retirements = db.query(
        "SELECT l.retires_device,l.device,l.number,l.stored_at FROM _coven_store_log l
         WHERE l.retires_device IS NOT NULL AND l.outcome='kept'
         UNION ALL
         SELECT d.device,l.device,l.number,l.stored_at FROM _coven_store_log l
         JOIN _coven_devices d ON d.member=l.retires_member
         WHERE l.retires_member IS NOT NULL AND l.outcome='kept'
         UNION ALL
         SELECT l.registers_device,l.device,l.number,l.stored_at FROM _coven_store_log l
         WHERE l.registers_device IS NOT NULL AND l.outcome NOT IN ('kept','late')
           AND NOT EXISTS(SELECT 1 FROM _coven_devices d WHERE d.device=l.registers_device)
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
        if crate::storage_time::decode(&stored)? >= horizon {
            continue;
        }
        db.internal_execute(
            "INSERT INTO _coven_retirement_finality(device,entry_device,entry_number,final_at)
             VALUES(?1,?2,?3,?4)",
            params![
                device_bytes(device).as_slice(),
                device_bytes(entry.device).as_slice(),
                entry.number.to_be_bytes().as_slice(),
                crate::storage_time::encode(sample),
            ],
        )?;
        recorded.insert(device);
    }
    Ok(())
}

/// In the replay transaction: forget finality evidence, and with it any drain
/// mark, whose entry no longer retires its device: a removal no longer kept,
/// or a registration kept again or whose device a kept entry now registers.
/// The device is polled again.
pub(crate) fn forget_dropped(db: &DatabaseConnection) -> Result<(), DbError> {
    // The entry still retires the device: kept and not a registration of it,
    // or a dropped (not late) registration of it while no kept entry
    // registers it (`Writers.retirements`). `mark` checks the same condition.
    db.internal_execute(
        "DELETE FROM _coven_retirement_finality WHERE NOT EXISTS(
            SELECT 1 FROM _coven_store_log l WHERE l.device=entry_device
            AND l.number=entry_number
            AND ((l.outcome='kept' AND (l.registers_device IS NULL
                    OR l.registers_device<>_coven_retirement_finality.device))
                OR (l.registers_device=_coven_retirement_finality.device
                    AND l.outcome NOT IN ('kept','late')
                    AND NOT EXISTS(SELECT 1 FROM _coven_devices r
                        WHERE r.device=_coven_retirement_finality.device))))",
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
            final_at: crate::storage_time::decode(&time)?,
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

/// Mark a completed drain, checking in the same transaction that its entry
/// still retires the device and the reads followed
/// `sample > F + one day + five minutes`.
pub(crate) fn mark(
    db: &DatabaseConnection,
    drain: RetirementFinality,
    sample: StorageTime,
) -> Result<bool, DbError> {
    let current = finalities(db)?
        .into_iter()
        .find(|saved| saved.device == drain.device);
    let Some(current) = current else {
        return Ok(false);
    };
    // The condition `forget_dropped` keeps evidence under.
    let retires: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_store_log l WHERE l.device=?1 AND l.number=?2
            AND ((l.outcome='kept' AND (l.registers_device IS NULL OR l.registers_device<>?3))
                OR (l.registers_device=?3 AND l.outcome NOT IN ('kept','late')
                    AND NOT EXISTS(SELECT 1 FROM _coven_devices r WHERE r.device=?3))))",
        params![
            device_bytes(current.entry.device).as_slice(),
            current.entry.number.to_be_bytes().as_slice(),
            device_bytes(current.device).as_slice(),
        ],
        |row| row.get(0),
    )?;
    if current != drain || !retires || !current.drain_permitted(sample) {
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
pub(crate) mod tests;
