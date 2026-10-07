//! Changes to the store itself, carrying causal positions and replay timestamps (§9).
//!
//! Decoding checks one frame's fields. Sync checks causal closure before replay;
//! replay determines authority, conflicts, version effects and current keys from
//! the entry set. A removal's exact replacement-circle list and deleted circles
//! also depend on its author's view, so the frame decoder checks only list order.

use crate::error::{require, Error, Rule};
use crate::value::{name, ordered, positive, EntryId, EntryPositions};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_crypto::{MemberId, SealingPublicKey};
use coven_foundation::id_source::{CircleId, DeviceId, KeyId, StoreId};
use coven_merge::{Audience, Timestamp};

/// The signing and sealed-box public keys of a member (§11.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberPublicKeys {
    /// The Ed25519 public key, which identifies the member.
    pub signing: MemberId,
    /// The X25519 public key that opens keys sealed to this member.
    pub sealing: SealingPublicKey,
}
wire_struct!(MemberPublicKeys, signing, sealing);

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

/// A circle and the replacement key made for it by a member removal (§13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CircleKeyId {
    /// The circle whose key was replaced.
    pub circle: CircleId,
    /// The replacement circle key's random id.
    pub key: KeyId,
}
wire_struct!(CircleKeyId, circle, key);

/// Every kind of store-log change listed in §9, including the first entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreChange {
    /// Create a store, name its first admin and register the writing device.
    CreateStore {
        /// The store's id.
        store: StoreId,
        /// The store's name.
        name: String,
        /// The first admin's public keys.
        admin: MemberPublicKeys,
        /// The owner’s provider account or public S3 key id.
        access: crate::MemberAccess,
        /// The first store key, introduced by this entry.
        key: KeyId,
        /// The name of the device that wrote this entry, belonging to its author.
        device_name: String,
    },
    /// Add a member with both public keys and their initial role.
    AddMember {
        /// The member's public keys.
        keys: MemberPublicKeys,
        /// Storage access granted by the inviting owner.
        access: crate::MemberAccess,
        /// The initial role.
        role: MemberRole,
    },
    /// Remove a member, replace their audience keys and delete circles left empty (§13).
    RemoveMember {
        /// The removed member.
        member: MemberId,
        /// The replacement store key's random id (§13).
        key: KeyId,
        /// Each remaining circle the member was in and its replacement key,
        /// strictly increasing by circle.
        circle_keys: Vec<CircleKeyId>,
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
    /// Remove a circle member and replace its key (§14.6).
    RemoveCircleMember {
        /// The circle being left.
        circle: CircleId,
        /// The removed member.
        member: MemberId,
        /// The replacement circle key's random id.
        key: KeyId,
    },
    /// Raise an audience's app schema version with its replacement snapshot (§17.1).
    RaiseSchema {
        /// The new schema version.
        version: u32,
        /// The snapshot whose audience is raised to that schema.
        snapshot: SnapshotId,
    },
    /// Raise an audience's format version with its replacement snapshot (§17.2).
    RaiseFormat {
        /// The new format version.
        version: u16,
        /// The snapshot whose audience is raised to that format.
        snapshot: SnapshotId,
    },
    /// Reset the store's or a circle's rows to a snapshot (§19.3).
    Reset {
        /// The snapshot and the audience being reset.
        snapshot: SnapshotId,
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
            Self::RemoveMember { circle_keys, .. } => {
                ordered(circle_keys, |key| key.circle, "replacement circle keys")
            }
            Self::RaiseSchema { version, snapshot } => {
                require(*version > 0, "schema version", Rule::Required)?;
                positive(snapshot.number)
            }
            Self::RaiseFormat { version, snapshot } => {
                require(*version > 0, "format version", Rule::Required)?;
                positive(snapshot.number)
            }
            Self::Reset { snapshot } => positive(snapshot.number),
            Self::AddMember { .. }
            | Self::ChangeRole { .. }
            | Self::RemoveDevice { .. }
            | Self::DeleteCircle { .. }
            | Self::AddCircleMember { .. }
            | Self::RemoveCircleMember { .. } => Ok(()),
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
    0 => CreateStore { store, name => crate::wire::get_name, admin, access, device_name => crate::wire::get_name, key },
    1 => AddMember { keys, role, access },
    2 => RemoveMember { member, key, circle_keys },
    3 => ChangeRole { member, role },
    4 => AddDevice { device, name => crate::wire::get_name },
    5 => RemoveDevice { device },
    6 => CreateCircle { circle, name => crate::wire::get_name, key },
    7 => RenameCircle { circle, name => crate::wire::get_name },
    8 => DeleteCircle { circle },
    9 => AddCircleMember { circle, member },
    10 => RemoveCircleMember { circle, member, key },
    11 => RaiseSchema { version, snapshot },
    12 => RaiseFormat { version, snapshot },
    13 => Reset { snapshot },
);

/// One store-log entry. Its eventual signature establishes who authored it (§9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreLogEntry {
    /// The writing device and its store-log number.
    pub position: EntryId,
    /// The timestamp used to replay entries and break concurrent ties.
    pub timestamp: Timestamp,
    /// The member whose signature must cover the entry.
    pub author: MemberId,
    /// Other devices’ store-log positions; the author’s own earlier entries are implicit.
    pub had_read: EntryPositions,
    /// The change to the store.
    pub change: StoreChange,
}
wire_struct!(StoreLogEntry, position, timestamp, author, had_read, change);
impl StoreLogEntry {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.position.validate()?;
        require(
            self.timestamp.device() == self.position.device,
            "store-log timestamp",
            Rule::TimestampDevice,
        )?;
        self.had_read.without_own_device(self.position.device)?;
        if let StoreChange::CreateStore { admin, .. } = &self.change {
            require(
                self.position.number == 1 && self.had_read.0.is_empty(),
                "first store entry",
                Rule::OwnPosition,
            )?;
            require(admin.signing == self.author, "first admin", Rule::Required)?;
        }
        self.change.validate()
    }
}

#[cfg(test)]
#[path = "store_log_tests.rs"]
mod tests;
