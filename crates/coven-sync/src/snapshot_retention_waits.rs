//! What one physical deletion waits for (§9, §15). A covered object goes only
//! once every store-log entry its decision depends on is final, and then once
//! every active reader posted past it or storage has held it for thirty days.
//! Its retention record names the first of these still unmet.

use coven_database::StoreLog;
use coven_format::{
    store_log::StoreChange,
    value::{EntryId, StorageTime, WritePositions},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::{Audience, WriteId};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// A covered write object whose readers have not all posted past it may go
/// once its storage age reaches thirty days (§15).
const RETENTION_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Whether storage time `now` is at least thirty days after `stored_at`;
/// equality counts (§15, IO `Deletable`).
pub(crate) fn reached_age(now: Option<StorageTime>, stored_at: StorageTime) -> bool {
    now.and_then(|now| now.duration_since(stored_at))
        .is_some_and(|age| age >= RETENTION_AGE)
}

/// The entries a deletion's decision reads from the replay, so a later
/// arrival reversing one of them could make the deleted object needed. Each
/// list includes dropped entries, which can return; an entry that landed too
/// late never can, so it is no dependency (§9).
pub(crate) struct Dependencies {
    /// Every reset and raise of an audience: a superseded own snapshot goes
    /// only once its replacement follows the final boundary (§15).
    boundaries: BTreeMap<Audience, BTreeSet<EntryId>>,
    /// Every registration, replacement and removal of a device, and every
    /// removal of a member that registered it: whether the device is an
    /// active reader, and who may delete its objects (§10, §13, §15).
    devices: BTreeMap<DeviceId, BTreeSet<EntryId>>,
}

impl Dependencies {
    pub(crate) fn new(log: &StoreLog) -> Self {
        let mut boundaries: BTreeMap<Audience, BTreeSet<EntryId>> = BTreeMap::new();
        let mut devices: BTreeMap<DeviceId, BTreeSet<EntryId>> = BTreeMap::new();
        let mut registered: BTreeMap<DeviceId, BTreeSet<_>> = BTreeMap::new();
        let mut removals = Vec::new();
        for applied in &log.entries {
            if applied.check.landed_too_late() {
                continue;
            }
            let entry = &applied.entry;
            match &entry.change {
                StoreChange::Reset { snapshot } | StoreChange::RaiseSchema { snapshot, .. } => {
                    boundaries
                        .entry(snapshot.audience.clone())
                        .or_default()
                        .insert(entry.position);
                }
                StoreChange::AddDevice {
                    device, replaces, ..
                } => {
                    registered.entry(*device).or_default().insert(entry.author);
                    devices.entry(*device).or_default().insert(entry.position);
                    if let Some(replaced) = replaces {
                        devices.entry(*replaced).or_default().insert(entry.position);
                    }
                }
                StoreChange::RemoveDevice { device } => {
                    devices.entry(*device).or_default().insert(entry.position);
                }
                StoreChange::RemoveMember { member, .. } => {
                    removals.push((*member, entry.position))
                }
                _ => (),
            }
        }
        for (device, members) in registered {
            let entries = devices.entry(device).or_default();
            for (member, removal) in &removals {
                if members.contains(member) {
                    entries.insert(*removal);
                }
            }
        }
        Self {
            boundaries,
            devices,
        }
    }

    pub(super) fn audience(&self, audience: &Audience) -> impl Iterator<Item = EntryId> + '_ {
        self.boundaries.get(audience).into_iter().flatten().copied()
    }

    pub(crate) fn device(&self, device: DeviceId) -> impl Iterator<Item = EntryId> + '_ {
        self.devices.get(&device).into_iter().flatten().copied()
    }

    /// Every device some entry registered, whether or not it is active now.
    pub(super) fn known_devices(&self) -> impl Iterator<Item = DeviceId> + '_ {
        self.devices.keys().copied()
    }
}

/// One covered, owned object's remaining conditions, in the order its
/// record names them (storelog-data `RetentionSafety.conditions`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Facts {
    /// The entries the decision depends on, ascending, with whether each is
    /// final (§9).
    pub(crate) dependencies: Vec<(EntryId, bool)>,
    /// Active readers ascending by device id, with whether each posted past
    /// the object. A missing or invalid post is no position (§15).
    pub(crate) readers: Vec<(DeviceId, bool)>,
    /// Whether storage has held the object for thirty days.
    pub(crate) aged: bool,
}

impl Facts {
    /// The facts for deleting `write`, stored at `stored_at`, at storage time
    /// `now`. A device that is not an active reader and has not posted past
    /// the write is one only by entries a later arrival may reverse, so those
    /// join `dependencies` unless storage age already suffices (§15).
    pub(super) fn for_write(
        log: &StoreLog,
        dependencies: &Dependencies,
        mut entries: BTreeSet<EntryId>,
        write: WriteId,
        stored_at: StorageTime,
        now: Option<StorageTime>,
        posted: &BTreeMap<DeviceId, WritePositions>,
    ) -> Self {
        let aged = reached_age(now, stored_at);
        let passed = |device: &DeviceId| posted.get(device).is_some_and(|p| p.covers(write));
        let active: BTreeSet<_> = log
            .replay
            .state
            .devices
            .iter()
            .filter(|(_, d)| d.active())
            .map(|(device, _)| *device)
            .collect();
        if !aged {
            for device in dependencies.known_devices() {
                if !active.contains(&device) && !passed(&device) {
                    entries.extend(dependencies.device(device));
                }
            }
        }
        Self {
            dependencies: entries
                .into_iter()
                .map(|entry| (entry, log.is_final(entry)))
                .collect(),
            readers: active
                .into_iter()
                .map(|device| (device, passed(&device)))
                .collect(),
            aged,
        }
    }

    /// The facts for deleting a superseded own snapshot: only the entries
    /// that make its replacement usable (§15).
    pub(super) fn for_snapshot(log: &StoreLog, entries: BTreeSet<EntryId>) -> Self {
        Self {
            dependencies: entries
                .into_iter()
                .map(|entry| (entry, log.is_final(entry)))
                .collect(),
            readers: Vec::new(),
            aged: true,
        }
    }

    /// The first unmet condition, or none when the object may go: the first
    /// dependency not yet final, then, unless storage age suffices, the
    /// first active reader by device id that has not posted past it.
    pub(crate) fn first_unmet(&self) -> Option<crate::Prerequisite> {
        if let Some((entry, _)) = self.dependencies.iter().find(|(_, last)| !last) {
            return Some(crate::Prerequisite::EntryFinality(*entry));
        }
        if self.aged {
            return None;
        }
        self.readers
            .iter()
            .find(|(_, passed)| !passed)
            .map(|(device, _)| crate::Prerequisite::Positions(*device))
    }
}

/// io `RetentionDeadline.refreshDue`: a write whose dependencies are final
/// and whose readers posted past it, or which storage held thirty days, needs
/// a fresh snapshot of each readable part audience whose current newest
/// counting snapshot (`current`, none without one) does not cover it (§15).
pub(crate) fn refresh_due(
    facts: &Facts,
    readable: bool,
    current: Option<&WritePositions>,
    write: WriteId,
) -> bool {
    readable && facts.first_unmet().is_none() && !current.is_some_and(|p| p.covers(write))
}
