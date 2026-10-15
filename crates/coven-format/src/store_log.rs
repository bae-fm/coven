//! Changes to the store itself, carrying causal positions and replay timestamps (§9).
//!
//! Decoding checks one frame's fields. Sync checks causal closure before replay;
//! replay determines authority, conflicts, version effects and deleted circles
//! from the entry set. Key commitments bind introductions to their key bytes.

use crate::error::{require, Error, Rule};
use crate::value::{name, positive, EntryId, EntryPositions};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
pub use coven_crypto::MemberPublicKeys;
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, MemberId, StoreId};
use coven_merge::{Audience, Timestamp};

/// Complete encrypted store-log object limit, including routing and signature.
/// Every provider must publish these single-chunk objects in one request (§9).
pub const MAX_STORE_LOG_OBJECT: usize = 4 * 1024 * 1024;

pub(crate) fn check_frame_length(length: usize) -> Result<(), Error> {
    crate::error::bound(
        length + 19 + 4 + coven_crypto::SEALED_OBJECT_CHUNK_OVERHEAD + 64,
        MAX_STORE_LOG_OBJECT,
        "store-log object",
    )
}

/// A member's role in the store (§9).
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

/// A snapshot's identity without a storage path (§15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotId {
    /// The device that wrote the snapshot.
    pub device: DeviceId,
    /// The positive number in this device's snapshot sequence.
    pub number: u64,
    /// The rows the snapshot covers.
    pub audience: Audience,
}
wire_struct!(SnapshotId, audience, device, number);

/// Every kind of store-log change listed in §9, including the first entry.
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
    /// Record a member's current storage access, used by a later removal (§9):
    /// the author's own, or an admin's replacement S3 key for another member.
    SetAccess {
        /// The member whose access this records.
        member: MemberId,
        /// The provider account, or the S3 key id and its principal; no credentials.
        access: crate::MemberAccess,
    },
    /// Remove a member and delete circles left empty (§13).
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
    /// Add an install belonging to the entry's author.
    AddDevice {
        /// The install's id.
        device: DeviceId,
        /// The device's name.
        name: String,
        /// The older install of the same member being replaced, if any (§10).
        /// Must differ from `device`; replacement carries no log ends.
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
    /// Remove a circle from the store log (§14.7).
    DeleteCircle {
        /// The deleted circle.
        circle: CircleId,
    },
    /// Add a store member to a circle (§14.3).
    AddCircleMember {
        /// The circle being joined.
        circle: CircleId,
        /// The added member.
        member: MemberId,
    },
    /// Remove a circle member, deleting the circle if left empty (§14.6).
    RemoveCircleMember {
        /// The circle being left.
        circle: CircleId,
        /// The removed member.
        member: MemberId,
    },
    /// Introduce a key without changing audience membership (§11).
    RotateKey {
        /// The audience whose member authored the rotation.
        audience: Audience,
        /// The introduced key's random id.
        key: KeyId,
        /// SHA-256 of the exact 32 key bytes.
        key_hash: [u8; 32],
    },
    /// Raise an audience's app schema version with its replacement snapshot (§17.1).
    RaiseSchema {
        /// The new schema version.
        version: u32,
        /// The snapshot whose audience is raised to that schema.
        snapshot: SnapshotId,
    },
    /// Reset the store's or a circle's rows to a snapshot (§19.3).
    Reset {
        /// The snapshot and the audience being reset.
        snapshot: SnapshotId,
    },
    /// Replace an active member's public pair, keeping their id, role, access,
    /// circles and devices (§9). It carries no audience keys.
    ReplaceMemberKeys {
        /// The member whose keys change.
        member: MemberId,
        /// The member's new public pair.
        keys: MemberPublicKeys,
    },
    /// Record that the provider damaged one of the writing device's own key
    /// copies and that a later copy of its log reseals what it held (§11).
    /// Both numbers belong to the device whose log holds this entry.
    ReplaceKeyCopy {
        /// The copy number whose bytes no longer decode.
        damaged: u64,
        /// The copy number holding the same key for the same recipient.
        replacement: u64,
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
            Self::AddDevice {
                device,
                name: n,
                replaces,
            } => {
                name(n)?;
                require(
                    replaces.as_ref() != Some(device),
                    "replacement device",
                    Rule::DeviceReplacement,
                )
            }
            Self::CreateCircle { name: n, .. } | Self::RenameCircle { name: n, .. } => name(n),
            Self::RaiseSchema { version, snapshot } => {
                require(*version > 0, "schema version", Rule::Required)?;
                positive(snapshot.number)
            }
            Self::Reset { snapshot } => positive(snapshot.number),
            Self::ReplaceKeyCopy {
                damaged,
                replacement,
            } => {
                positive(*damaged)?;
                require(
                    damaged < replacement,
                    "key-copy replacement",
                    Rule::CopyReplacement,
                )
            }
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
    4 => AddDevice { device, name => crate::wire::get_name, replaces },
    5 => RemoveDevice { device },
    6 => CreateCircle { circle, name => crate::wire::get_name, key, key_hash },
    7 => RenameCircle { circle, name => crate::wire::get_name },
    8 => DeleteCircle { circle },
    9 => AddCircleMember { circle, member },
    10 => RemoveCircleMember { circle, member },
    11 => RaiseSchema { version, snapshot },
    13 => Reset { snapshot },
    14 => SetAccess { member, access },
    15 => RotateKey { audience, key, key_hash },
    16 => ReplaceMemberKeys { member, keys },
    17 => ReplaceKeyCopy { damaged, replacement },
);

/// One store-log entry. Its eventual signature establishes who authored it (§9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreLogEntry {
    /// The writing device and its store-log number.
    pub position: EntryId,
    /// The timestamp used to replay entries and break concurrent ties.
    pub timestamp: Timestamp,
    /// The stable id of the member whose current signing key in the recorded
    /// view, or creation's supplied pair, must verify the entry (§9).
    pub author: MemberId,
    /// Other devices’ store-log positions; the author’s own earlier entries are implicit.
    pub had_read: EntryPositions,
    /// The change to the store.
    pub change: StoreChange,
}
wire_struct!(StoreLogEntry, position, timestamp, author, had_read, change);
impl StoreLogEntry {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        positive(self.position.number)?;
        require(
            self.timestamp.device() == self.position.device,
            "store-log timestamp",
            Rule::TimestampDevice,
        )?;
        self.had_read.without_own_device(self.position.device)?;
        if let StoreChange::CreateStore { .. } = &self.change {
            require(
                self.position.number == 1 && self.had_read.0.is_empty(),
                "first store entry",
                Rule::OwnPosition,
            )?;
        }
        self.change.validate()
    }
}

#[cfg(test)]
#[path = "store_log_tests.rs"]
mod tests;
