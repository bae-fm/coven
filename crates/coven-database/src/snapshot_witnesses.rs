//! §15's historical coverage evidence. A log object goes only once, for each
//! of its part audiences, a counting snapshot covering it landed strictly more
//! than one storage day ago. That snapshot may since have been superseded or
//! deleted, and its body may be gone, so the evidence is kept apart from the
//! catalog and across reopening (SP "What survives a pass"; io
//! `MatureCoverage`, `maturity_survives_new_snapshots`).
//!
//! For each audience and writer the evidence is a staircase: positions with the
//! earliest landing of a checked counting snapshot reaching them. A row reached
//! by a later row no later is redundant, so the landings rise with the
//! positions, and a write's earliest witness is the first row at or past it.

use crate::{sqlite::DatabaseConnection, write_encoding, DbError};
use coven_format::value::{StorageTime, WritePositions};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use std::collections::BTreeMap;

/// Every audience's retained witnesses, as checked prefixes left them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotWitnesses(BTreeMap<Audience, BTreeMap<DeviceId, Vec<(u64, StorageTime)>>>);

impl SnapshotWitnesses {
    /// The earliest landing of a checked counting snapshot of `audience` that
    /// covers `write`, if one was ever observed.
    pub fn landed(&self, audience: &Audience, write: WriteId) -> Option<StorageTime> {
        self.0
            .get(audience)?
            .get(&write.device)?
            .iter()
            .find(|(number, _)| *number >= write.number)
            .map(|(_, landed)| *landed)
    }

    /// Whether some witness of `audience` covering `write` landed strictly
    /// more than one storage day before `now` (§15; io `MatureCoverage`).
    pub fn mature(&self, audience: &Audience, write: WriteId, now: Option<StorageTime>) -> bool {
        match (self.landed(audience, write), now) {
            (Some(landed), Some(now)) => !landed.within_day(now),
            _ => false,
        }
    }

    /// Add a checked counting snapshot of `audience` that landed at `landed`.
    /// Returns whether the evidence changed.
    pub fn record(
        &mut self,
        audience: &Audience,
        positions: &WritePositions,
        landed: StorageTime,
    ) -> bool {
        let mut changed = false;
        for reached in &positions.0 {
            let steps = self
                .0
                .entry(audience.clone())
                .or_default()
                .entry(reached.device)
                .or_default();
            if steps
                .iter()
                .any(|(number, time)| *number >= reached.number && *time <= landed)
            {
                continue;
            }
            steps.retain(|(number, time)| !(*number <= reached.number && *time >= landed));
            let at = steps.partition_point(|(number, _)| *number < reached.number);
            steps.insert(at, (reached.number, landed));
            changed = true;
        }
        changed
    }

    /// Drop rows below a row already mature at `now`: a write they cover is
    /// covered by that row too, and both stay mature as storage time advances.
    /// Keeps the evidence bounded by the snapshots of the last day. Returns
    /// whether anything was dropped.
    pub fn prune(&mut self, now: StorageTime) -> bool {
        let mut changed = false;
        for writers in self.0.values_mut() {
            for steps in writers.values_mut() {
                let mature = steps
                    .iter()
                    .rposition(|(_, landed)| !landed.within_day(now));
                if let Some(index) = mature.filter(|index| *index > 0) {
                    steps.drain(..index);
                    changed = true;
                }
            }
        }
        changed
    }

    /// The staircase of one audience, for its database rows.
    fn rows(&self, audience: &Audience) -> impl Iterator<Item = (DeviceId, u64, StorageTime)> + '_ {
        self.0.get(audience).into_iter().flat_map(|writers| {
            writers.iter().flat_map(|(device, steps)| {
                steps
                    .iter()
                    .map(move |(number, landed)| (*device, *number, *landed))
            })
        })
    }
}

/// Every retained witness row.
pub(crate) fn read(db: &DatabaseConnection) -> Result<SnapshotWitnesses, DbError> {
    let mut witnesses = SnapshotWitnesses::default();
    db.for_each(
        "SELECT audience,device,number,landed FROM _coven_snapshot_witnesses ORDER BY audience,device,number",
        [],
        |r| {
            let audience = write_encoding::audience(&r.get::<_, String>(0)?)
                .map_err(|_| DbError::DamagedDatabase)?;
            let device = DeviceId(write_encoding::counter(r.get(1)?));
            let number = write_encoding::counter(r.get(2)?);
            let landed = crate::storage_time::decode(&r.get::<_, Vec<u8>>(3)?)?;
            witnesses
                .0
                .entry(audience)
                .or_default()
                .entry(device)
                .or_default()
                .push((number, landed));
            Ok::<_, DbError>(())
        },
    )?;
    Ok(witnesses)
}

/// Record checked counting snapshots of `audience` and prune with `now`.
/// Returns the evidence when it changed, for the caller to write.
pub(crate) fn update(
    mut witnesses: SnapshotWitnesses,
    audience: &Audience,
    observed: &[(WritePositions, StorageTime)],
    now: Option<StorageTime>,
) -> Option<SnapshotWitnesses> {
    let mut changed = false;
    for (positions, landed) in observed {
        changed |= witnesses.record(audience, positions, *landed);
    }
    if let Some(now) = now {
        changed |= witnesses.prune(now);
    }
    changed.then_some(witnesses)
}

/// Replace every row of `audience` with this evidence.
pub(crate) fn write(
    db: &DatabaseConnection,
    witnesses: &SnapshotWitnesses,
    audience: &Audience,
) -> Result<(), DbError> {
    let text = write_encoding::audience_text(audience);
    db.internal_execute(
        "DELETE FROM _coven_snapshot_witnesses WHERE audience=?1",
        [&text],
    )?;
    for (device, number, landed) in witnesses.rows(audience) {
        db.internal_execute(
            "INSERT INTO _coven_snapshot_witnesses(audience,device,number,landed) VALUES(?1,?2,?3,?4)",
            rusqlite::params![
                text,
                device.0.to_be_bytes().as_slice(),
                number.to_be_bytes().as_slice(),
                crate::storage_time::encode(landed)
            ],
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_witnesses_tests.rs"]
mod tests;
