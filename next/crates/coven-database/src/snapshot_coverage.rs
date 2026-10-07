//! Audience frontiers exist only inside the atomic loading transaction.

use crate::snapshot_error::invalid;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text, decoded};
use crate::{DbError, DownloadedPart, DownloadedWrite, SnapshotError};
use coven_format::{merge_fields, value::WritePositions};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct SnapshotCoverage {
    audiences: BTreeMap<Audience, WritePositions>,
    loaded: BTreeSet<Audience>,
    target: BTreeMap<DeviceId, u64>,
}

impl SnapshotCoverage {
    pub(crate) fn new(database: &DatabaseConnection, device: DeviceId) -> Result<Self, DbError> {
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
        let mut coverage = Self {
            audiences,
            loaded: BTreeSet::new(),
            target: BTreeMap::new(),
        };
        // A device's next write implicitly reads every earlier own write, even
        // when the upload queue is empty. Reload cannot rewind that causal past.
        let own = database.query(
            "SELECT number,had_read FROM _coven_writes WHERE substr(timestamp,9,8)=?1
             ORDER BY number DESC LIMIT 1",
            [device.0.to_be_bytes().as_slice()],
            |r| {
                Ok((
                    crate::write_encoding::counter(r.get(0)?),
                    decoded(merge_fields::decode_write_positions(
                        &r.get::<_, Vec<u8>>(1)?,
                    ))?,
                ))
            },
        )?;
        for (number, past) in own {
            coverage.include(WriteId { device, number });
            for write in past.0 {
                coverage.include(write);
            }
        }
        Ok(coverage)
    }

    pub(crate) fn loaded(&mut self, audience: Audience, positions: WritePositions) {
        self.loaded.insert(audience.clone());
        self.audiences.insert(audience, positions);
    }

    pub(crate) fn covered_by_snapshot(&self, write: WriteId) -> bool {
        self.loaded
            .iter()
            .any(|audience| self.audiences[audience].covers(write))
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
