//! Admin changes in sequence slots (D6). The enclosing slot supplies identity and author.

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, Slot};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
pub use coven_crypto::MemberPublicKeys;
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, MemberId, StoreId};
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

/// Every admin operation listed in §10, including store creation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreChange {
    /// Create a store, name its first admin and register the writing device.
    /// The entry's author is the first admin's stable id; `admin` supplies
    /// that member's initial pair, which verifies this entry (D6).
    CreateStore {
        /// The store's id.
        store: StoreId,
        /// The store's name.
        name: String,
        /// The first admin's initial public pair.
        admin: MemberPublicKeys,
        /// The owner’s provider account or public S3 key id.
        access: crate::MemberAccess,
        /// The first store key, introduced by this entry.
        key: KeyId,
        /// SHA-256 of the exact 32 store-key bytes.
        key_hash: [u8; 32],
        /// The name of the device that wrote this entry, belonging to its author.
        device_name: String,
    },
    /// Add a member's stable id with both public keys and their initial role.
    AddMember {
        /// The added member's stable id, fixed by the admin's invite.
        member: MemberId,
        /// The member's initial public pair.
        keys: MemberPublicKeys,
        /// Storage access granted by the inviting owner.
        access: crate::MemberAccess,
        /// The initial role.
        role: MemberRole,
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
    /// Remove a circle member, deleting the circle if left empty (§15.8).
    RemoveCircleMember {
        /// The circle being left.
        circle: CircleId,
        /// The removed member.
        member: MemberId,
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
}

impl StoreChange {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::CreateStore {
                name: n,
                device_name,
                ..
            } => {
                name(n)?;
                name(device_name)
            }
            Self::AddDevice { name: n, .. }
            | Self::CreateCircle { name: n, .. }
            | Self::RenameCircle { name: n, .. } => name(n),
            Self::RaiseSchema {
                version,
                announcement,
                ..
            } => {
                require(*version > 0, "schema version", Rule::Required)?;
                positive(announcement.0)
            }
            Self::Reset { announcement } => positive(announcement.0),
            Self::RemoveMember { .. }
            | Self::RotateKey { .. }
            | Self::SetAccess { .. }
            | Self::AddMember { .. }
            | Self::ChangeRole { .. }
            | Self::RemoveDevice { .. }
            | Self::DeleteCircle { .. }
            | Self::AddCircleMember { .. }
            | Self::RemoveCircleMember { .. }
            | Self::ReplaceMemberKeys { .. } => Ok(()),
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
    0 => CreateStore { store, name => crate::wire::get_name, admin, access, device_name => crate::wire::get_name, key, key_hash },
    1 => AddMember { member, keys, role, access },
    2 => RemoveMember { member },
    3 => ChangeRole { member, role },
    4 => AddDevice { name => crate::wire::get_name, replaces },
    5 => RemoveDevice { device },
    6 => CreateCircle { circle, name => crate::wire::get_name, key, key_hash },
    7 => RenameCircle { circle, name => crate::wire::get_name },
    8 => DeleteCircle { circle },
    9 => AddCircleMember { circle, member },
    10 => RemoveCircleMember { circle, member },
    11 => RaiseSchema { version, announcement },
    13 => Reset { announcement },
    14 => SetAccess { member, access },
    15 => RotateKey { audience, key, key_hash },
    16 => ReplaceMemberKeys { member, expected, keys },
);

impl StoreChange {
    /// Encode the one bounded kind-4 admin frame (D6).
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        crate::encode_frame(4, self)
    }

    /// Decode exactly one admin frame. Slot-dependent authorization is separate.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (kind, mut input) = crate::wire::decode_frame(bytes)?;
        require(kind == 4, "admin frame", Rule::Kind)?;
        let change = Self::get(&mut input)?;
        input.finish()?;
        change.validate()?;
        Ok(change)
    }
}

#[cfg(test)]
#[path = "admin_tests.rs"]
mod tests;
