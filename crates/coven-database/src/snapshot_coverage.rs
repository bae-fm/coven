//! Audience frontiers exist only inside the atomic loading transaction.

use crate::snapshot_error::invalid;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text};
use crate::{DbError, DownloadedPart, DownloadedWrite, SnapshotError};
use coven_format::value::WritePositions;
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct SnapshotCoverage {
    audiences: BTreeMap<Audience, WritePositions>,
    target: BTreeMap<DeviceId, u64>,
}

impl SnapshotCoverage {
    pub(crate) fn new(database: &DatabaseConnection) -> Result<Self, DbError> {
        let positions = crate::download::positions(database)?;
        let audiences = database
            .query(
                "SELECT audience FROM _coven_loaded_audiences UNION SELECT audience FROM _coven_rows
             UNION SELECT audience FROM _coven_lost UNION SELECT 'store'",
                [],
                |r| audience(&r.get::<_, String>(0)?),
            )?
            .into_iter()
            .map(|a| (a, positions.clone()))
            .collect();
        // A reload never rewinds this device's applied positions: they hold
        // its own writes, and a write made during staging read them (§15).
        let target = positions.0.iter().map(|w| (w.device, w.number)).collect();
        Ok(Self { audiences, target })
    }

    pub(crate) fn loaded(&mut self, audience: Audience, positions: WritePositions) {
        self.audiences.insert(audience, positions);
    }

    pub(crate) fn opened(&mut self, audience: &Audience) {
        // A newly readable audience without a snapshot must consume its log
        // from the start, even if the store snapshot already covers the writes.
        self.audiences
            .entry(audience.clone())
            .or_insert_with(|| WritePositions(Vec::new()));
    }

    pub(crate) fn include(&mut self, write: WriteId) {
        self.target
            .entry(write.device)
            .and_modify(|n| *n = (*n).max(write.number))
            .or_insert(write.number);
    }

    pub(crate) fn start(&mut self, database: &DatabaseConnection) -> Result<(), DbError> {
        let devices: BTreeSet<_> = self
            .audiences
            .values()
            .flat_map(|p| p.0.iter().map(|p| p.device))
            .collect();
        database.internal_execute("DELETE FROM _coven_positions", [])?;
        for device in devices {
            let minimum = self
                .audiences
                .values()
                .map(|p| {
                    p.0.iter()
                        .find(|p| p.device == device)
                        .map_or(0, |p| p.number)
                })
                .min()
                .expect("store audience");
            let maximum = self
                .audiences
                .values()
                .flat_map(|p| &p.0)
                .filter(|p| p.device == device)
                .map(|p| p.number)
                .max()
                .expect("known device");
            self.include(WriteId {
                device,
                number: maximum,
            });
            if minimum > 0 {
                database.internal_execute(
                    "INSERT INTO _coven_positions(device,number) VALUES(?1,?2)",
                    (
                        device.0.to_be_bytes().as_slice(),
                        minimum.to_be_bytes().as_slice(),
                    ),
                )?;
            }
        }
        for audience in self.audiences.keys() {
            database.internal_execute(
                "INSERT INTO _coven_loaded_audiences(audience) VALUES(?1) ON CONFLICT DO NOTHING",
                [audience_text(audience)],
            )?;
        }
        Ok(())
    }

    pub(crate) fn uncovered(&self, mut write: DownloadedWrite) -> Result<DownloadedWrite, DbError> {
        let mut parts = Vec::new();
        for part in write.parts {
            let audience = match &part {
                DownloadedPart::Opened(part) => &part.audience,
                DownloadedPart::Skipped(audience) => audience,
            };
            if let Some(positions) = self.audiences.get(audience) {
                if positions.covers(write.header.position) {
                    continue;
                }
                if matches!(part, DownloadedPart::Skipped(_)) {
                    return Err(invalid(
                        "reload is missing an opened part for a loaded audience",
                    ));
                }
            }
            parts.push(part);
        }
        write.parts = parts;
        Ok(write)
    }

    pub(crate) fn finish(&self, database: &DatabaseConnection) -> Result<(), DbError> {
        let positions = crate::download::positions(database)?;
        let missing: Vec<_> = self
            .target
            .iter()
            .map(|(device, number)| WriteId {
                device: *device,
                number: *number,
            })
            .filter(|p| !positions.covers(*p))
            .collect();
        if !missing.is_empty() {
            return Err(SnapshotError::MissingWrites { missing }.into());
        }
        Ok(())
    }
}
