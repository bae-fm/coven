//! Admin changes in sequence slots (D6). The enclosing slot supplies identity and author.

use crate::error::{require, Error, Rule};
use crate::value::{name, Slot};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
pub use coven_crypto::MemberPublicKeys;
use coven_foundation::id_source::{CircleId, DeviceId, InviteId, KeyId, MemberId, StoreId};
use coven_merge::Audience;

/// A member's role in the store (§10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MemberRole {
    /// Can add and remove members and change roles.
    Admin,
    /// Can write data and manage their own devices.
    Member,
}
impl Wire for MemberRole {
    fn put(&self, out: &mut Encoder) -> Result<(), Error> {
        match self {
            Self::Admin => 0u8,
            Self::Member => 1,
        }
        .put(out)
    }
    fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
        match u8::get(input)? {
            0 => Ok(Self::Admin),
            1 => Ok(Self::Member),
            tag => Err(Error::UnknownTag { field: "role", tag }),
        }
    }
}

/// A snapshot's identity without a storage path (§16.2).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapshotId {
    /// The rows the snapshot covers.
    pub audience: Audience,
    /// The device that wrote the snapshot.
    pub device: DeviceId,
    /// The positive number in this device's snapshot sequence.
    pub number: u64,
}
wire_struct!(SnapshotId, audience, device, number);

/// A circle store creation makes, with its first key (§10.3, D6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreationCircle {
    /// The circle's id.
    pub circle: CircleId,
    /// The circle's name.
    pub name: String,
    /// The newest breaking migration the creator's app had run, zero if
    /// none, as `CreateCircle` records it.
    pub breaking_version: u32,
    /// The circle's first key, introduced by the creation.
    pub key: KeyId,
    /// SHA-256 of the exact 32 circle-key bytes.
    pub key_hash: [u8; 32],
}
wire_struct!(CreationCircle, circle, name => crate::wire::get_name, breaking_version, key, key_hash);

/// Every admin operation listed in §10, including store creation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreChange {
    /// Create a store, name its first admin and register the writing device.
    /// The entry's author is the first admin's stable id; `admin` supplies
    /// that member's initial pair, which verifies this entry (D6). It records
    /// no storage access: `SetUpStorage` does, once storage exists.
    CreateStore {
        /// The store's id.
        store: StoreId,
        /// The store's name.
        name: String,
        /// The first admin's initial public pair.
        admin: MemberPublicKeys,
        /// The first store key, introduced by this entry.
        key: KeyId,
        /// SHA-256 of the exact 32 store-key bytes.
        key_hash: [u8; 32],
        /// The name of the device that wrote this entry, belonging to its author.
        device_name: String,
        /// The circles the creator's first write asks for, each with the
        /// creator as its first member, in the order they were asked for.
        circles: Vec<CreationCircle>,
    },
    /// Record how the store's first admin reaches storage, which fixes the
    /// kind of access every member has and, for a provider account, the
    /// account that holds the store (§10.3). Applies once, as an admin in the
    /// preceding state; access entries, admissions and invite shares wait
    /// for it.
    SetUpStorage {
        /// The author's provider account, or the public S3 key id they use.
        access: crate::MemberAccess,
    },
    /// Set the store name, as an admin in the preceding state (§10).
    RenameStore {
        /// The new name.
        name: String,
    },
    /// Admit a joining person: add their stable id with both public keys,
    /// their initial role and access, and register the device their request
    /// names. Only an admission slot holds it, beside the member's key copies
    /// (§13.2, D9).
    AdmitMember {
        /// The added member's stable id, fixed by the admin's invite.
        member: MemberId,
        /// The member's initial public pair.
        keys: MemberPublicKeys,
        /// The initial role.
        role: MemberRole,
        /// Storage access granted by the inviting owner.
        access: crate::MemberAccess,
        /// The joining installation, registered for the member. Never the
        /// writing device.
        device: DeviceId,
        /// The joining installation's name, from its request.
        device_name: String,
    },
    /// Recover an active member on a new device: rebind their pair, as
    /// replace-member-keys does, set their access when the invite carried a
    /// new S3 key, and register the device their request names. Only an
    /// admission slot holds it (§13.2, D9).
    RecoverMember {
        /// The recovered member.
        member: MemberId,
        /// The binding which must still be current before this slot.
        expected: MemberPublicKeys,
        /// The member's new public pair.
        keys: MemberPublicKeys,
        /// The member's new storage access, as set-access records it: the
        /// S3 key the recovery invite carries. `None` keeps their access.
        access: Option<crate::MemberAccess>,
        /// The new installation, registered for the member. Never the writing
        /// device.
        device: DeviceId,
        /// The new installation's name, from its request.
        device_name: String,
    },
    /// Record a member's current storage access, used by a later removal (§10):
    /// the author's own, or an admin's replacement S3 key for another member.
    SetAccess {
        /// The member whose access this records.
        member: MemberId,
        /// The provider account, or the S3 key id and its principal; no credentials.
        access: crate::MemberAccess,
    },
    /// Remove a member and delete circles left empty (§14.1).
    RemoveMember {
        /// The removed member.
        member: MemberId,
    },
    /// Set a member's role.
    ChangeRole {
        /// The affected member.
        member: MemberId,
        /// The target role.
        role: MemberRole,
    },
    /// Register the writing install for the entry's author: an install
    /// registers only itself (§10.2).
    AddDevice {
        /// The device's name.
        name: String,
        /// The older install of the same member being replaced, if any (§19.3).
        /// Must differ from the writing device.
        replaces: Option<DeviceId>,
    },
    /// Remove a device's access.
    RemoveDevice {
        /// The removed install.
        device: DeviceId,
    },
    /// Make a circle with its creator as its first member.
    CreateCircle {
        /// The new circle's id.
        circle: CircleId,
        /// The circle's name.
        name: String,
        /// The newest breaking migration its maker's app had run, zero if
        /// none: the circle's schema version until a raise of its own goes
        /// past it (§15.5, §16.3).
        breaking_version: u32,
        /// The first circle key, introduced by this entry.
        key: KeyId,
        /// SHA-256 of the exact 32 circle-key bytes.
        key_hash: [u8; 32],
    },
    /// Rename a circle without changing its members, keys or rows.
    RenameCircle {
        /// The circle being renamed.
        circle: CircleId,
        /// Its new name.
        name: String,
    },
    /// Delete a circle (§15.9).
    DeleteCircle {
        /// The deleted circle.
        circle: CircleId,
    },
    /// Add a store member to a circle (§15.5).
    AddCircleMember {
        /// The circle being joined.
        circle: CircleId,
        /// The added member.
        member: MemberId,
    },
    /// Remove a circle member, deleting the circle if left empty, and
    /// introduce the circle's replacement key (§15.8).
    RemoveCircleMember {
        /// The circle being left.
        circle: CircleId,
        /// The removed member.
        member: MemberId,
        /// The replacement circle key, introduced by this entry.
        key: KeyId,
        /// SHA-256 of the exact 32 circle-key bytes.
        key_hash: [u8; 32],
    },
    /// Introduce a key without changing audience membership (§12.1).
    RotateKey {
        /// The audience whose member authored the rotation.
        audience: Audience,
        /// The introduced key's random id.
        key: KeyId,
        /// SHA-256 of the exact 32 key bytes.
        key_hash: [u8; 32],
    },
    /// Raise an audience's app schema version with its replacement snapshot (§18.3).
    RaiseSchema {
        /// The new schema version.
        version: u32,
        /// The earlier announcement of the prepared snapshot.
        announcement: Slot,
    },
    /// Reset the store's or a circle's rows to a snapshot (§19.4).
    Reset {
        /// The earlier announcement of the prepared reset snapshot.
        announcement: Slot,
    },
    /// Replace an active member's public pair, keeping their id, role, access,
    /// circles and devices (§10). It carries no audience keys.
    ReplaceMemberKeys {
        /// The member whose keys change.
        member: MemberId,
        /// The binding which must still be current before this slot.
        expected: MemberPublicKeys,
        /// The member's new public pair.
        keys: MemberPublicKeys,
    },
    /// Record that the writing device's invite shares the store with a
    /// provider account (§13.2, §14.2). Claimed before the grant, so the
    /// owner's other devices know of the share and take it back once the
    /// invite ends or its device is gone for good.
    AddInviteShare {
        /// The invite, fixed when it was made.
        invite: InviteId,
        /// The provider account the invite shares the store with.
        account: String,
    },
    /// End the share the writing device recorded for its invite (§13.2).
    EndInviteShare {
        /// The invite whose share ends.
        invite: InviteId,
    },
}

impl StoreChange {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::CreateStore { name: n, device_name, circles, .. } => {
                name(n)?;
                name(device_name)?;
                circles.iter().try_for_each(|circle| name(&circle.name))
            }
            Self::RenameStore { name: n }
            | Self::AdmitMember { device_name: n, .. }
            | Self::RecoverMember { device_name: n, .. }
            | Self::AddDevice { name: n, .. }
            | Self::CreateCircle { name: n, .. }
            | Self::RenameCircle { name: n, .. } => name(n),
            Self::RaiseSchema { version, .. } => {
                require(*version > 0, "schema version", Rule::Required)
            }
            Self::Reset { .. } => Ok(()),
            Self::AddInviteShare { account, .. } => {
                require(!account.trim().is_empty(), "invite share account", Rule::Required)?;
                crate::error::bound(
                    account.len(),
                    crate::member_access::MAX_TEXT,
                    "invite share account",
                )
            }
            Self::RemoveMember { .. }
            | Self::RotateKey { .. }
            | Self::SetUpStorage { .. }
            | Self::SetAccess { .. }
            | Self::ChangeRole { .. }
            | Self::RemoveDevice { .. }
            | Self::DeleteCircle { .. }
            | Self::AddCircleMember { .. }
            | Self::RemoveCircleMember { .. }
            | Self::ReplaceMemberKeys { .. }
            | Self::EndInviteShare { .. } => Ok(()),
        }
    }
}

// The tag and named fields are the whole wire layout of each store change.
macro_rules! store_changes {
    ($($tag:literal => $variant:ident { $($field:ident $(=> $get:path)?),+ }),+ $(,)?) => {
        impl Wire for StoreChange {
            fn put(&self, out: &mut Encoder) -> Result<(), Error> {
                match self { $(Self::$variant { $($field),+ } => {
                    ($tag as u8).put(out)?; $($field.put(out)?;)+ Ok(())
                }),+ }
            }
            fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
                match u8::get(input)? {
                    $($tag => Ok(Self::$variant { $($field: crate::wire::wire_struct!(@get input $(, $get)?)?),+ }),)+
                    tag => Err(Error::UnknownTag { field: "store change", tag }),
                }
            }
        }
    };
}
store_changes!(
    0 => CreateStore { store, name => crate::wire::get_name, admin, device_name => crate::wire::get_name, key, key_hash, circles },
    1 => AdmitMember { member, keys, role, access, device, device_name => crate::wire::get_name },
    2 => RemoveMember { member },
    3 => ChangeRole { member, role },
    4 => AddDevice { name => crate::wire::get_name, replaces },
    5 => RemoveDevice { device },
    6 => CreateCircle { circle, name => crate::wire::get_name, breaking_version, key, key_hash },
    7 => RenameCircle { circle, name => crate::wire::get_name },
    8 => DeleteCircle { circle },
    9 => AddCircleMember { circle, member },
    10 => RemoveCircleMember { circle, member, key, key_hash },
    11 => RaiseSchema { version, announcement },
    13 => Reset { announcement },
    14 => SetAccess { member, access },
    15 => RotateKey { audience, key, key_hash },
    16 => ReplaceMemberKeys { member, expected, keys },
    17 => RenameStore { name => crate::wire::get_name },
    18 => AddInviteShare { invite, account => get_account },
    19 => EndInviteShare { invite },
    20 => RecoverMember { member, expected, keys, access, device, device_name => crate::wire::get_name },
    21 => SetUpStorage { access },
);

/// An invite share's provider account, bounded as member access bounds an
/// account (D6).
fn get_account(input: &mut Decoder<'_>) -> Result<String, Error> {
    crate::wire::get_text(input, crate::member_access::MAX_TEXT, "invite share account")
}

impl StoreChange {
    /// Encode the one bounded kind-4 admin frame (D6).
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        crate::encode_frame(crate::kinds::ADMIN, self)
    }

    /// Decode exactly one admin frame. Slot-dependent authorization is separate.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let change: Self =
            crate::wire::decode_frame_value(bytes, crate::kinds::ADMIN, "admin frame")?;
        change.validate()?;
        Ok(change)
    }
}

#[cfg(test)]
#[path = "admin_tests.rs"]
mod tests;
