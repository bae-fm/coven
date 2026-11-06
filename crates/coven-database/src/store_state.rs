//! Membership and audience state after one committed sequence prefix.
use crate::DbError;
use coven_crypto::MemberPublicKeys;
use coven_format::admin::{MemberRole, SnapshotId};
use coven_foundation::id_source::{CircleId, DeviceId, MemberId, StoreId};
use coven_merge::{Audience, Slot};
use std::collections::{BTreeMap, BTreeSet};

/// Store state, including removed identities needed to check older writes (§10.4).
///
/// Schema versions belong to each audience: the store's raise is under
/// [`Audience::Store`], and each circle has its own. A raise is absent until a
/// kept raise selects it, as in §18.3; before one, the audience is at its base
/// ([`StoreState::reached`]). These are independent of the local app
/// migration version returned by [`crate::Database::schema_version`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreState {
    /// Absent before the first entry creates the store.
    pub store: Option<StoreIdentity>,
    /// Every member added by a kept entry, including removed members.
    pub members: BTreeMap<MemberId, StoreMember>,
    /// Every public pair a kept creation, addition or member-key replacement
    /// bound, including pairs of removed members and pairs a later replacement
    /// retired. An addition of an id not active in its view, and every
    /// replacement, must share no key with these (§10).
    pub bound_keys: BTreeSet<MemberPublicKeys>,
    /// Every device added by a kept entry, including removed devices.
    pub devices: BTreeMap<DeviceId, StoreDevice>,
    /// Every circle made by a kept entry, including deleted circles.
    pub circles: BTreeMap<CircleId, StoreCircle>,
    /// Deleted circles and their deciding slots; absence means no deletion.
    pub circle_deletions: BTreeMap<CircleId, Slot>,
    /// The selected schema raise per audience; creation itself carries no raise.
    /// [`Self::reached`] gives an audience's version, a circle's base included.
    pub schema: BTreeMap<Audience, StoreVersion>,
    /// The last kept reset of each audience (§19.4).
    pub resets: BTreeMap<Audience, SnapshotId>,
}

impl StoreState {
    /// The member, unless a kept removal removed it.
    pub fn active_member(&self, id: &MemberId) -> Option<&StoreMember> {
        self.members.get(id).filter(|member| !member.removed)
    }

    /// Whether `device` is installed and active, and belongs to `member`.
    pub fn device_acts_for(&self, device: &DeviceId, member: &MemberId) -> bool {
        self.devices.get(device).is_some_and(|device| device.active() && device.member == *member)
    }

    /// Whether `id` is a member who has not been removed and holds the admin role.
    pub fn active_admin(&self, id: &MemberId) -> bool {
        self.active_member(id).is_some_and(|member| member.role == MemberRole::Admin)
    }

    /// Whether `member` remains in the store and in `circle`, which exists and
    /// is not deleted (§15.5).
    pub fn reads_circle(&self, circle: CircleId, member: &MemberId) -> bool {
        self.active_member(member).is_some()
            && !self.circle_deletions.contains_key(&circle)
            && self.circles.get(&circle).is_some_and(|c| c.members.contains(member))
    }

    /// Every circle `member` reads under [`Self::reads_circle`]: the circles
    /// whose parts its devices open, snapshot and reload (§15.6, §16).
    pub fn readable_circles(&self, member: &MemberId) -> BTreeSet<CircleId> {
        self.circles.keys().copied().filter(|circle| self.reads_circle(*circle, member)).collect()
    }

    /// The schema version `audience` has reached: the one its latest kept
    /// raise selected, or before any raise its base, zero for the store and
    /// for a circle the breaking version its creation recorded. A raise goes
    /// past the base, so this is the greater of the two (§16.3, §18.3).
    pub fn reached(&self, audience: &Audience) -> u32 {
        self.schema.get(audience).map(|raised| raised.number).unwrap_or_else(|| match audience {
            Audience::Store => 0,
            Audience::Circle(circle) => self.circles.get(circle).map_or(0, |circle| circle.base),
        })
    }

    /// Whether a part at `version` in `audience` waits for a raise: the
    /// audience is below that version and can still be raised. Nobody can
    /// raise a deleted circle, and its parts are taken out wherever they land
    /// (§15.9, §18.5).
    pub fn awaits_raise(&self, audience: &Audience, version: u32) -> bool {
        !self.deleted(audience) && self.reached(audience) < version
    }

    /// Whether `audience` is a deleted circle (§15.9).
    pub fn deleted(&self, audience: &Audience) -> bool {
        match audience {
            Audience::Store => false,
            Audience::Circle(circle) => self.circle_deletions.contains_key(circle),
        }
    }

    /// Every audience `member` reads: the store's, then each of
    /// [`Self::readable_circles`] in id order.
    pub fn readable_audiences(&self, member: &MemberId) -> Vec<Audience> {
        std::iter::once(Audience::Store)
            .chain(self.readable_circles(member).into_iter().map(Audience::Circle))
            .collect()
    }
}

/// A store's identity and current name (§10).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    /// The store's id.
    pub id: StoreId,
    /// Its name.
    pub name: String,
    /// The slot that set `name`: creation's slot 1, or the latest applied
    /// rename that changed it. A saved restore code's name is replaced only
    /// by one from a later slot (§13.1).
    pub named_at: Slot,
}

impl StoreIdentity {
    /// Opening requires directory settings to match checked creation (§10).
    pub fn check_settings(
        &self,
        settings: &coven_foundation::files::StoreSettings,
    ) -> Result<(), DbError> {
        check_creation(self.id, settings.id)
    }
}

/// The sequence's store id must match the directory settings (§10).
pub(crate) fn check_creation(store: StoreId, settings_store: StoreId) -> Result<(), DbError> {
    if store != settings_store {
        return Err(DbError::StoreSettingsMismatch { store });
    }
    Ok(())
}

/// A member in the applied prefix, keyed by stable id in [`StoreState::members`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreMember {
    /// The current public pair: the signing key that verifies this member's
    /// objects in this view and the sealing key copies go to (§10, §12.1).
    pub keys: MemberPublicKeys,
    /// Current access after the applied slot. Revocation also reads superseded
    /// access from the retained administrative objects (§14.2).
    pub access: coven_format::MemberAccess,
    /// The member's role, retained when removed.
    pub role: MemberRole,
    /// Whether a kept removal removed this member.
    pub removed: bool,
}

/// One install, retained after removal or replacement so its older objects
/// remain attributable (§10.4).
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
    /// Neither removed nor replaced: it may still author objects (§14.1).
    pub fn active(&self) -> bool {
        self.state == DeviceState::Active
    }

    /// Removed, by its own removal or its member's: it stops for good.
    pub fn removed(&self) -> bool {
        self.state == DeviceState::Removed
    }
}

/// What counted administrative slots did to a device id (E9). Later slots
/// from a removed or replaced device are refused (§14.1, D6).
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
/// Its keys come from received introductions, not this membership projection (§12.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreCircle {
    /// The last kept name.
    pub name: String,
    /// Current members; empty after deletion.
    pub members: BTreeSet<MemberId>,
    /// The breaking version its creation entry recorded: its schema version
    /// until a raise of its own goes past it (§15.5, §16.3).
    pub base: u32,
}

/// A selected version raise and the snapshot it chose (§18.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreVersion {
    /// The raised schema version.
    pub number: u32,
    /// The snapshot in this version.
    pub snapshot: SnapshotId,
    /// The kept raise that selected it.
    pub entry: Slot,
}

#[cfg(test)]
#[path = "store_state_tests.rs"]
mod tests;
