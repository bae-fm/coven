//! Membership and audience state after one committed sequence prefix.
use crate::DbError;
use coven_crypto::MemberPublicKeys;
use coven_format::admin::{MemberRole, SnapshotId};
use coven_foundation::id_source::{CircleId, DeviceId, MemberId, StoreId};
use coven_merge::{Audience, Slot};
use std::collections::{BTreeMap, BTreeSet};

/// Store state, including removed identities needed to check older writes (§10).
///
/// Schema versions belong to each audience: the store's version is
/// under [`Audience::Store`], and each circle has its own. A version is absent
/// until a kept raise selects it, as in Appendix C. These are independent of the
/// local app migration version returned by [`crate::Database::schema_version`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreState {
    /// Absent before the first entry creates the store.
    pub store: Option<StoreIdentity>,
    /// Every member added by a kept entry, including removed members.
    pub members: BTreeMap<MemberId, StoreMember>,
    /// Every public pair a kept creation, addition or member-key replacement
    /// bound, including pairs of removed members and pairs a later replacement
    /// retired. An addition of an id not active in its view, and every
    /// replacement, must share no key with these (§9).
    pub bound_keys: BTreeSet<MemberPublicKeys>,
    /// Every device added by a kept entry, including removed devices.
    pub devices: BTreeMap<DeviceId, StoreDevice>,
    /// Every circle made by a kept entry, including deleted circles.
    pub circles: BTreeMap<CircleId, StoreCircle>,
    /// Deleted circles and their deciding slots; absence means no deletion.
    pub circle_deletions: BTreeMap<CircleId, Slot>,
    /// The selected schema raise per audience; creation itself carries no raise.
    pub schema: BTreeMap<Audience, StoreVersion>,
    /// The last kept reset of each audience (§19.3).
    pub resets: BTreeMap<Audience, SnapshotId>,
}

impl StoreState {
    /// Whether `member` remains in the store and in `circle`, which exists and
    /// is not deleted (§14.3).
    pub fn reads_circle(&self, circle: CircleId, member: &MemberId) -> bool {
        self.members.get(member).is_some_and(|m| !m.removed)
            && !self.circle_deletions.contains_key(&circle)
            && self
                .circles
                .get(&circle)
                .is_some_and(|c| c.members.contains(member))
    }

    /// Every circle `member` reads under [`Self::reads_circle`]: the circles
    /// whose parts its devices open, snapshot and reload (§14.4, §15).
    pub fn readable_circles(&self, member: &MemberId) -> BTreeSet<CircleId> {
        self.circles
            .keys()
            .copied()
            .filter(|circle| self.reads_circle(*circle, member))
            .collect()
    }
}

/// The created store, as its kept creation entry names it (§9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    /// The store's id.
    pub id: StoreId,
    /// Its name.
    pub name: String,
}

impl StoreIdentity {
    /// Opening requires directory settings to match checked creation (§9).
    pub fn check_settings(
        &self,
        settings: &coven_foundation::files::StoreSettings,
    ) -> Result<(), DbError> {
        check_creation(self.id, &self.name, settings.id, &settings.name)
    }
}

/// A creation's id and name must be the ones the directory settings name (§9).
pub(crate) fn check_creation(
    store: StoreId,
    name: &str,
    settings_store: StoreId,
    settings_name: &str,
) -> Result<(), DbError> {
    if store != settings_store || name != settings_name {
        return Err(DbError::StoreSettingsMismatch {
            store,
            name: name.into(),
        });
    }
    Ok(())
}

/// A member in the applied prefix, keyed by stable id in [`StoreState::members`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreMember {
    /// The current public pair: the signing key that verifies this member's
    /// objects in this view and the sealing key copies go to (§9, §11).
    pub keys: MemberPublicKeys,
    /// Current access after the applied slot. Revocation also reads superseded
    /// access from the retained administrative objects (§13).
    pub access: coven_format::MemberAccess,
    /// The member's role, retained when removed.
    pub role: MemberRole,
    /// Whether a kept removal removed this member.
    pub removed: bool,
}

/// One install, retained after removal or replacement so its older objects
/// remain attributable (§10).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreDevice {
    /// The member who added the device.
    pub member: MemberId,
    /// The device's name from its addition.
    pub name: String,
    /// Whether kept entries removed or replaced it.
    pub state: DeviceState,
}

impl StoreDevice {
    /// Neither removed nor replaced: it may still author objects (§10).
    pub fn active(&self) -> bool {
        self.state == DeviceState::Active
    }

    /// Removed, by its own removal or its member's: it stops for good.
    pub fn removed(&self) -> bool {
        self.state == DeviceState::Removed
    }
}

/// What counted administrative slots did to a device id (E9). Later slots
/// from a removed or replaced device are refused (§10, D6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceState {
    /// Registered and neither removed nor replaced.
    Active,
    /// A kept removal of the device or of its member.
    Removed,
    /// A kept add-device entry names this id as the one it replaces.
    Replaced,
}

/// A circle and its current membership; deleted circles retain their name.
/// Its keys come from received introductions, not this membership projection (§11).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreCircle {
    /// The last kept name.
    pub name: String,
    /// Current members; empty after deletion.
    pub members: BTreeSet<MemberId>,
}

/// A selected version raise and the snapshot it chose (§17).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreVersion {
    /// The raised schema version.
    pub number: u32,
    /// The snapshot in this version.
    pub snapshot: SnapshotId,
    /// The kept raise that selected it.
    pub entry: Slot,
}
