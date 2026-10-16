//! Which store-log entries one physical deletion depends on (§9, §15,
//! §16.5): those deciding the state of its uploader and of each device that
//! could still read it, as storelog-data `RetentionSafety.Deletion` derives
//! them.

use coven_database::{StoreDevice, StoreLog};
use coven_format::{store_log::StoreChange, value::EntryId};
use coven_foundation::id_source::DeviceId;
use coven_merge::Audience;
use std::collections::{BTreeMap, BTreeSet};

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
    /// Store creation registers the device that wrote it (storelog-data
    /// `RetentionSafety.concerns`, `creation_device_counterexample`).
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
                // A registration registers the device that wrote it (§10).
                StoreChange::CreateStore { .. } | StoreChange::AddDevice { .. } => {
                    let device = entry.position.device;
                    registered.entry(device).or_default().insert(entry.author);
                    devices.entry(device).or_default().insert(entry.position);
                    if let StoreChange::AddDevice {
                        replaces: Some(replaced),
                        ..
                    } = &entry.change
                    {
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

    /// The entries that let `own` delete `uploader`'s objects: none for its
    /// own, otherwise those deciding the uploader's state (§9, §15).
    pub(crate) fn deleting(&self, own: DeviceId, uploader: DeviceId) -> BTreeSet<EntryId> {
        if uploader == own {
            return BTreeSet::new();
        }
        self.device(uploader).collect()
    }

    /// Whether another uploader's files may go: the entries deciding its
    /// state are final, so no later replay makes it name them again (§9,
    /// §16.5; storelog-data `files_survive_later_replay`).
    pub(crate) fn releases_files(&self, log: &StoreLog, own: DeviceId, uploader: DeviceId) -> bool {
        uploader == own || self.device(uploader).all(|entry| log.is_final(entry))
    }
}

/// Whether `own` deletes `uploader`'s objects: its own, or a removed device's
/// it is `entitled` to (§13, §15).
pub(crate) fn deletes_for<E>(
    log: &StoreLog,
    own: DeviceId,
    uploader: DeviceId,
    entitled: impl FnOnce(&StoreDevice) -> Result<bool, E>,
) -> Result<bool, E> {
    if uploader == own {
        return Ok(true);
    }
    match log.replay.state.devices.get(&uploader) {
        Some(device) if device.removed() => entitled(device),
        _ => Ok(false),
    }
}
