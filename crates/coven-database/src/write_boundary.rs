//! Checked entry boundaries, their immutable coverage, and slot selection.
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text, counter};
use crate::{DbError, StoreState, StoreVersion};
use coven_format::{
    admin::SnapshotId, snapshot_rows::ChangeVerdict, value::Slot, write::WriteHeader,
};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use rusqlite::params;
use std::collections::BTreeSet;

/// A checked raise/reset and the snapshot named by that entry. Its selection is
/// derived from kept entries, independently of whether coverage has been read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Boundary {
    /// A breaking schema raise.
    SchemaChange(StoreVersion),
    /// A reset of the named snapshot's audience.
    Reset {
        /// The reset's counted slot.
        entry: Slot,
        /// Its snapshot, including the audience.
        snapshot: SnapshotId,
    },
}

impl Boundary {
    /// The checked entry that owns this boundary.
    pub fn entry(&self) -> Slot {
        match self {
            Self::SchemaChange(version) => version.entry,
            Self::Reset { entry, .. } => *entry,
        }
    }

    /// The immutable snapshot named by this boundary.
    pub fn snapshot(&self) -> &SnapshotId {
        match self {
            Self::SchemaChange(version) => &version.snapshot,
            Self::Reset { snapshot, .. } => snapshot,
        }
    }

    /// The no-op rule for a raise; a lower version never replaces a higher one.
    pub fn schema_in_place(state: &StoreState, version: u32, snapshot: &SnapshotId) -> bool {
        state
            .schema
            .get(&snapshot.audience)
            .is_some_and(|current| version <= current.number)
    }

    /// Repeating the current reset does not introduce a new boundary.
    pub fn reset_in_place(state: &StoreState, snapshot: &SnapshotId) -> bool {
        state.resets.get(&snapshot.audience) == Some(snapshot)
    }

    /// Whether applying this checked boundary leaves the applied state unchanged.
    pub fn already_in_place(&self, state: &StoreState) -> bool {
        match self {
            Self::SchemaChange(version) => {
                Self::schema_in_place(state, version.number, &version.snapshot)
            }
            Self::Reset { snapshot, .. } => Self::reset_in_place(state, snapshot),
        }
    }

    /// Realize a non-no-op boundary after the preceding prefix authorizes it.
    pub fn apply(&self, state: &mut StoreState) {
        match self {
            Self::SchemaChange(version) => {
                state
                    .schema
                    .insert(version.snapshot.audience.clone(), version.clone());
            }
            Self::Reset { snapshot, .. } => {
                state
                    .resets
                    .insert(snapshot.audience.clone(), snapshot.clone());
            }
        }
    }

    pub(crate) fn record(&self, database: &DatabaseConnection) -> Result<(), DbError> {
        let (kind, version) = match self {
            Self::SchemaChange(version) => ("schema", Some(version.number)),
            Self::Reset { .. } => ("reset", None),
        };
        let entry = self.entry();
        let snapshot = self.snapshot();
        database.internal_execute(
            "INSERT INTO _coven_boundaries(slot,kind,version,audience,snapshot_device,snapshot_number)
             VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(slot) DO NOTHING",
            params![entry.0.to_be_bytes().as_slice(),
                kind, version, audience_text(&snapshot.audience),
                snapshot.device.0.to_be_bytes().as_slice(), snapshot.number.to_be_bytes().as_slice()],
        )?;
        Ok(())
    }

    pub(crate) fn load_selected(database: &DatabaseConnection) -> Result<Vec<Self>, DbError> {
        Ok(retained(database)?
            .into_iter()
            .map(|row| row.boundary)
            .collect())
    }
}

/// A checked raise or reset and its authenticated snapshot coverage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryCoverage {
    /// The checked change, including its original slot and snapshot identity.
    pub boundary: Boundary,
    /// The slot coverage authenticated by its snapshot prefix.
    pub included: u64,
}

impl BoundaryCoverage {
    pub(crate) fn record_inside(&self, database: &DatabaseConnection) -> Result<bool, DbError> {
        self.boundary.record(database)?;
        let previous: (Option<Vec<u8>>, bool) = database.query_row(
            "SELECT included,applied FROM _coven_boundaries WHERE slot=?1",
            [self.boundary.entry().0.to_be_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let included = self.included.to_be_bytes().to_vec();
        if let Some(checked) = previous.0 {
            if checked != included {
                return Err(crate::snapshot_error::invalid(
                    "checked boundary coverage changed",
                ));
            }
        }
        if previous.1 {
            return Ok(false);
        }
        database.internal_execute(
            "UPDATE _coven_boundaries SET included=?2,applied=1 WHERE slot=?1",
            params![self.boundary.entry().0.to_be_bytes().as_slice(), included],
        )?;
        Ok(true)
    }

    /// Install the reload's effective readable boundaries, retaining every
    /// checked coverage fact. The applied bit records the committed data view,
    /// not slot selection. It also distinguishes a repeated reset reload.
    pub(crate) fn replace(
        database: &DatabaseConnection,
        boundaries: Vec<Self>,
    ) -> Result<Vec<Slot>, DbError> {
        let mut ids = BTreeSet::new();
        for boundary in &boundaries {
            if !ids.insert(boundary.boundary.entry()) {
                return Err(crate::snapshot_error::invalid(
                    "reload repeats a boundary slot",
                ));
            }
        }
        let previous: BTreeSet<_> = database
            .query(
                "SELECT slot FROM _coven_boundaries WHERE applied=1 AND kind='reset'",
                [],
                |row| crate::store_state_tables::slot(row, 0),
            )?
            .into_iter()
            .collect();
        let changed_resets = boundaries
            .iter()
            .filter_map(|coverage| match &coverage.boundary {
                Boundary::Reset { entry, .. }
                    if ids.contains(entry) && !previous.contains(entry) =>
                {
                    Some(*entry)
                }
                _ => None,
            })
            .collect();
        database.internal_execute("DELETE FROM _coven_boundaries", [])?;
        for boundary in boundaries {
            boundary.record_inside(database)?;
        }
        Ok(changed_resets)
    }
}

struct RetainedBoundary {
    boundary: Boundary,
    included: Option<u64>,
    applied: bool,
}

fn retained(database: &DatabaseConnection) -> Result<Vec<RetainedBoundary>, DbError> {
    database.query(
        "SELECT b.kind,b.version,b.audience,b.snapshot_device,b.snapshot_number,b.slot,b.included,b.applied
         FROM _coven_boundaries b ORDER BY b.slot",
        [],
        |row| {
            let snapshot = SnapshotId {
                audience: audience(&row.get::<_, String>(2)?)?,
                device: DeviceId(counter(row.get(3)?)),
                number: counter(row.get(4)?),
            };
            let entry = crate::store_state_tables::slot(row, 5)?;
            let boundary = match (row.get::<_, String>(0)?.as_str(), row.get::<_, Option<u32>>(1)?) {
                ("schema", Some(number)) => Boundary::SchemaChange(StoreVersion { number, snapshot, entry }),
                ("reset", None) => Boundary::Reset { entry, snapshot },
                _ => return Err(rusqlite::Error::InvalidQuery),
            };
            let included = row.get::<_, Option<Vec<u8>>>(6)?.map(counter);
            Ok(RetainedBoundary { boundary, included, applied: row.get(7)? })
        },
    )
}

/// What a realized boundary decides about an uncovered part of its audience.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BoundaryRule {
    /// An older-schema part needs admission through the raise's retained
    /// breaking steps (§17.1).
    Raise(u32),
    /// A reset ignores a part whose write had not read this entry (§19.3).
    Reset(Slot),
}

/// The boundary facts realized by the current data projection.
pub(crate) struct WriteBoundary {
    pub(crate) audience: Audience,
    pub(crate) rule: BoundaryRule,
    pub(crate) included: u64,
}

impl WriteBoundary {
    pub(crate) fn load(database: &DatabaseConnection) -> Result<Vec<Self>, DbError> {
        let mut state = StoreState::default();
        let mut result = Vec::new();
        for row in retained(database)? {
            if row.boundary.already_in_place(&state) {
                continue;
            }
            row.boundary.apply(&mut state);
            if row.applied {
                result.push(Self {
                    audience: row.boundary.snapshot().audience.clone(),
                    rule: match &row.boundary {
                        Boundary::SchemaChange(version) => BoundaryRule::Raise(version.number),
                        Boundary::Reset { entry, .. } => BoundaryRule::Reset(*entry),
                    },
                    included: row.included.ok_or(DbError::DamagedDatabase)?,
                });
            }
        }
        Ok(result)
    }

    /// The realized boundaries, with each unpublished raise capture judging
    /// its audience as its kept raise will: a migrated device applies
    /// received writes before its raise is kept, so a capture taken again
    /// after its catch-up covers them (§15, §17.1). A capture whose
    /// audience already realized a raise to its version adds nothing.
    pub(crate) fn judging(database: &DatabaseConnection) -> Result<Vec<Self>, DbError> {
        let mut boundaries = Self::load(database)?;
        for (audience, version, included) in crate::raise_capture::provisional(database)? {
            let realized = boundaries.iter().any(|b| {
                b.audience == audience
                    && matches!(b.rule, BoundaryRule::Raise(number) if number >= version)
            });
            if !realized {
                boundaries.push(Self {
                    audience,
                    rule: BoundaryRule::Raise(version),
                    included,
                });
            }
        }
        Ok(boundaries)
    }

    /// This boundary's verdict on an uncovered part of its audience: a kept
    /// reset ignores a part whose write had not read the reset entry (§19.3);
    /// a raise returns a candidate schema exclusion, resolved by the interpreter
    /// using the retained breaking steps (§17.1).
    pub(crate) fn excludes(
        &self,
        header: &WriteHeader,
        audience: &Audience,
        slot: Option<Slot>,
    ) -> Option<ChangeVerdict> {
        if *audience != self.audience || slot.is_some_and(|slot| slot.0 <= self.included) {
            return None;
        }
        match &self.rule {
            BoundaryRule::Raise(version) => {
                (header.schema_version < *version).then_some(ChangeVerdict::Excluded(*version))
            }
            BoundaryRule::Reset(entry) => {
                (header.read_through < entry.0).then_some(ChangeVerdict::Ignored(*entry))
            }
        }
    }

    /// The selected boundaries' combined verdict on one part. Reset
    /// eligibility comes first, so an ignored part never gains a schema loss;
    /// otherwise the highest excluding raise names the loss.
    pub(crate) fn part_verdict(
        boundaries: &[Self],
        header: &WriteHeader,
        audience: &Audience,
        slot: Option<Slot>,
    ) -> Option<ChangeVerdict> {
        let verdicts: Vec<_> = boundaries
            .iter()
            .filter_map(|boundary| boundary.excludes(header, audience, slot))
            .collect();
        verdicts
            .iter()
            .find(|verdict| matches!(verdict, ChangeVerdict::Ignored(_)))
            .cloned()
            .or_else(|| {
                verdicts
                    .iter()
                    .filter_map(|verdict| match verdict {
                        ChangeVerdict::Excluded(version) => Some(*version),
                        _ => None,
                    })
                    .max()
                    .map(ChangeVerdict::Excluded)
            })
    }
}
#[cfg(test)]
#[path = "write_boundary_tests.rs"]
pub(crate) mod tests;
