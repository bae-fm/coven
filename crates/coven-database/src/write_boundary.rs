//! Checked entry boundaries, their immutable coverage, and replay selection.
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience, audience_text, counter, decoded, encoded};
use crate::{DbError, StoreLogState, StoreVersion};
use coven_format::{
    merge_fields,
    snapshot_rows::ChangeVerdict,
    store_log::{SnapshotId, StoreChange, StoreLogEntry},
    value::{EntryId, WritePositions},
    write::{WriteHeader, WritePart},
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
        /// The reset's checked store-log identity.
        entry: EntryId,
        /// Its snapshot, including the audience.
        snapshot: SnapshotId,
    },
}

impl Boundary {
    /// Extract the boundary carried by an entry, including a currently dropped one.
    pub fn from_entry(entry: &StoreLogEntry) -> Option<Self> {
        match &entry.change {
            StoreChange::RaiseSchema { version, snapshot } => {
                Some(Self::SchemaChange(StoreVersion {
                    number: *version,
                    snapshot: snapshot.clone(),
                    entry: entry.position,
                }))
            }
            StoreChange::Reset { snapshot } => Some(Self::Reset {
                entry: entry.position,
                snapshot: snapshot.clone(),
            }),
            _ => None,
        }
    }

    /// The checked entry that owns this boundary.
    pub fn entry(&self) -> EntryId {
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

    /// Replay's no-op rule for a raise; a lower version never replaces a higher one.
    pub fn schema_in_place(state: &StoreLogState, version: u32, snapshot: &SnapshotId) -> bool {
        state.schema.get(&snapshot.audience).is_some_and(|current| {
            version < current.number || (version == current.number && *snapshot == current.snapshot)
        })
    }

    /// Repeating the current reset does not introduce a new boundary.
    pub fn reset_in_place(state: &StoreLogState, snapshot: &SnapshotId) -> bool {
        state.resets.get(&snapshot.audience) == Some(snapshot)
    }

    /// Whether applying this checked boundary leaves the replayed state unchanged.
    pub fn already_in_place(&self, state: &StoreLogState) -> bool {
        match self {
            Self::SchemaChange(version) => {
                Self::schema_in_place(state, version.number, &version.snapshot)
            }
            Self::Reset { snapshot, .. } => Self::reset_in_place(state, snapshot),
        }
    }

    /// Realize a non-no-op boundary after replay's authority and conflict checks.
    pub fn apply(&self, state: &mut StoreLogState) {
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

    /// Effective boundaries in entry timestamp order. The input contains only
    /// kept entries; the same no-op and effect functions are used during replay.
    pub(crate) fn select(boundaries: impl IntoIterator<Item = Self>) -> impl Iterator<Item = Self> {
        let mut state = StoreLogState::default();
        boundaries.into_iter().filter(move |boundary| {
            if boundary.already_in_place(&state) {
                return false;
            }
            boundary.apply(&mut state);
            true
        })
    }

    pub(crate) fn record(&self, database: &DatabaseConnection) -> Result<(), DbError> {
        let (kind, version) = match self {
            Self::SchemaChange(version) => ("schema", Some(version.number)),
            Self::Reset { .. } => ("reset", None),
        };
        let entry = self.entry();
        let snapshot = self.snapshot();
        database.internal_execute(
            "INSERT INTO _coven_boundaries(entry_device,entry_number,kind,version,audience,snapshot_device,snapshot_number)
             VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(entry_device,entry_number) DO NOTHING",
            params![entry.device.0.to_be_bytes().as_slice(), entry.number.to_be_bytes().as_slice(),
                kind, version, audience_text(&snapshot.audience),
                snapshot.device.0.to_be_bytes().as_slice(), snapshot.number.to_be_bytes().as_slice()],
        )?;
        Ok(())
    }

    pub(crate) fn load_selected(database: &DatabaseConnection) -> Result<Vec<Self>, DbError> {
        Ok(Self::select(retained(database)?.into_iter().map(|row| row.boundary)).collect())
    }
}

/// Authenticated coverage of the snapshot named by one checked entry. The entry
/// supplies the audience and kind; callers cannot install an unrelated boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryCoverage {
    /// The already checked raise/reset entry.
    pub entry: EntryId,
    /// Device-write positions authenticated by its snapshot prefix.
    pub included: WritePositions,
}

impl BoundaryCoverage {
    pub(crate) fn record_inside(&self, database: &DatabaseConnection) -> Result<bool, DbError> {
        let previous: (Option<Vec<u8>>, bool) = database.query_row(
            "SELECT included,applied FROM _coven_boundaries WHERE entry_device=?1 AND entry_number=?2",
            (self.entry.device.0.to_be_bytes().as_slice(), self.entry.number.to_be_bytes().as_slice()),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let included = encoded(merge_fields::encode_write_positions(&self.included))?;
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
            "UPDATE _coven_boundaries SET included=?3,applied=1 WHERE entry_device=?1 AND entry_number=?2",
            params![self.entry.device.0.to_be_bytes().as_slice(), self.entry.number.to_be_bytes().as_slice(), included],
        )?;
        Ok(true)
    }

    /// Install the reload's effective readable boundaries, retaining every
    /// checked coverage fact. The applied bit records the committed data view,
    /// not replay selection. It also distinguishes a repeated reset reload.
    pub(crate) fn replace(
        database: &DatabaseConnection,
        boundaries: Vec<Self>,
    ) -> Result<Vec<EntryId>, DbError> {
        let selected = Boundary::load_selected(database)?;
        let selected_ids: BTreeSet<_> = selected.iter().map(Boundary::entry).collect();
        let mut ids = BTreeSet::new();
        for boundary in &boundaries {
            if !selected_ids.contains(&boundary.entry) || !ids.insert(boundary.entry) {
                return Err(crate::snapshot_error::invalid(
                    "reload boundary is not a distinct effective checked entry",
                ));
            }
        }
        let previous: BTreeSet<_> = database.query(
            "SELECT entry_device,entry_number FROM _coven_boundaries WHERE applied=1 AND kind='reset'",
            [], |row| crate::store_log_tables::entry_id(row, 0),
        )?.into_iter().collect();
        let changed_resets = selected
            .iter()
            .filter_map(|boundary| match boundary {
                Boundary::Reset { entry, .. }
                    if ids.contains(entry) && !previous.contains(entry) =>
                {
                    Some(*entry)
                }
                _ => None,
            })
            .collect();
        database.internal_execute("UPDATE _coven_boundaries SET applied=0 WHERE applied=1", [])?;
        for boundary in boundaries {
            boundary.record_inside(database)?;
        }
        Ok(changed_resets)
    }
}

struct RetainedBoundary {
    boundary: Boundary,
    included: Option<WritePositions>,
    applied: bool,
}

fn retained(database: &DatabaseConnection) -> Result<Vec<RetainedBoundary>, DbError> {
    database.query(
        "SELECT b.kind,b.version,b.audience,b.snapshot_device,b.snapshot_number,b.entry_device,b.entry_number,b.included,b.applied
         FROM _coven_store_log l JOIN _coven_boundaries b ON b.entry_device=l.device AND b.entry_number=l.number
         WHERE l.outcome='kept' ORDER BY l.timestamp",
        [],
        |row| {
            let snapshot = SnapshotId {
                audience: audience(&row.get::<_, String>(2)?)?,
                device: DeviceId(counter(row.get(3)?)),
                number: counter(row.get(4)?),
            };
            let entry = crate::store_log_tables::entry_id(row, 5)?;
            let boundary = match (row.get::<_, String>(0)?.as_str(), row.get::<_, Option<u32>>(1)?) {
                ("schema", Some(number)) => Boundary::SchemaChange(StoreVersion { number, snapshot, entry }),
                ("reset", None) => Boundary::Reset { entry, snapshot },
                _ => return Err(rusqlite::Error::InvalidQuery),
            };
            let included = row.get::<_, Option<Vec<u8>>>(7)?
                .map(|bytes| decoded(merge_fields::decode_write_positions(&bytes))).transpose()?;
            Ok(RetainedBoundary { boundary, included, applied: row.get(8)? })
        },
    )
}

/// What a realized boundary decides about an uncovered part of its audience.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BoundaryRule {
    /// A raise excludes a part authored at an older schema version (§17.1).
    Raise(u32),
    /// A reset ignores a part whose write had not read this entry (§19.3).
    Reset(EntryId),
}

/// The boundary facts realized by the current data projection.
pub(crate) struct WriteBoundary {
    pub(crate) audience: Audience,
    pub(crate) rule: BoundaryRule,
    pub(crate) included: WritePositions,
}

impl WriteBoundary {
    pub(crate) fn load(database: &DatabaseConnection) -> Result<Vec<Self>, DbError> {
        let mut state = StoreLogState::default();
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
    /// a raise excludes a part authored at an older schema version (§17.1).
    pub(crate) fn excludes(&self, header: &WriteHeader, part: &WritePart) -> Option<ChangeVerdict> {
        if part.audience != self.audience || self.included.covers(header.position) {
            return None;
        }
        match &self.rule {
            BoundaryRule::Raise(version) => {
                (header.schema_version < *version).then_some(ChangeVerdict::Excluded(*version))
            }
            BoundaryRule::Reset(entry) => {
                (!header.store_log_read.covers(*entry)).then_some(ChangeVerdict::Ignored(*entry))
            }
        }
    }

    /// The selected boundaries' combined verdict on one part. Reset
    /// eligibility comes first, so an ignored part never gains a schema loss;
    /// otherwise the highest excluding raise names the loss.
    pub(crate) fn part_verdict(
        boundaries: &[Self],
        header: &WriteHeader,
        part: &WritePart,
    ) -> Option<ChangeVerdict> {
        let verdicts: Vec<_> = boundaries
            .iter()
            .filter_map(|boundary| boundary.excludes(header, part))
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
