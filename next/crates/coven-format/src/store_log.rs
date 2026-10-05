//! Changes to the store itself, carrying causal positions and replay timestamps (§9).

use crate::error::{require, Error, Rule};
use crate::value::{name, ordered, positive, EntryId, EntryPositions};
use crate::wire::{wire_struct, Decoder, Encoder, Wire};
use coven_crypto::{MemberId, SealingPublicKey};
use coven_foundation::id_source::{CircleId, DeviceId, StoreId};
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
wire_struct!(SnapshotId, device, number, audience);

/// A circle and the replacement key made for it by a member removal (§13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CircleKeyNumber {
    /// The circle whose key was replaced.
    pub circle: CircleId,
    /// The replacement circle key's positive number.
    pub key_number: u64,
}
wire_struct!(CircleKeyNumber, circle, key_number);

/// Every kind of store-log change listed in §9, including the first entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreChange {
    /// Create a store and name its first admin.
    CreateStore {
        /// The store's id.
        store: StoreId,
        /// The store's name.
        name: String,
        /// The first admin's public keys.
        admin: MemberPublicKeys,
    },
    /// Add a member with both public keys and their initial role.
    AddMember {
        /// The member's public keys.
        keys: MemberPublicKeys,
        /// The initial role.
        role: MemberRole,
    },
    /// Remove a member, replace their audience keys and delete circles left empty (§13).
    RemoveMember {
        /// The removed member.
        member: MemberId,
        /// The replacement store key's positive number (§13).
        key_number: u64,
        /// Each remaining circle the member was in and its replacement key,
        /// strictly increasing by circle.
        circle_keys: Vec<CircleKeyNumber>,
        /// Circles the member was alone in, deleted by this entry, strictly increasing.
        deleted_circles: Vec<CircleId>,
    },
    /// Set a member's role.
    ChangeRole {
        /// The affected member.
        member: MemberId,
        /// The target role.
        role: MemberRole,
    },
    /// Add an install belonging to a member.
    AddDevice {
        /// The member whose key authorizes the device.
        member: MemberId,
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
        /// Its first member.
        creator: MemberId,
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
        /// The replacement circle key's positive number.
        key_number: u64,
    },
    /// Raise the store's app schema version with its replacement snapshot (§17.1).
    RaiseSchema {
        /// The new schema version.
        version: u32,
        /// The snapshot in that schema.
        snapshot: SnapshotId,
    },
    /// Raise the store's format version with its replacement snapshot (§17.2).
    RaiseFormat {
        /// The new format version.
        version: u16,
        /// The snapshot in that format.
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
            Self::CreateStore { name: n, .. }
            | Self::AddDevice { name: n, .. }
            | Self::CreateCircle { name: n, .. }
            | Self::RenameCircle { name: n, .. } => name(n),
            Self::RemoveMember {
                key_number,
                circle_keys,
                deleted_circles,
                ..
            } => {
                require(*key_number > 0, "replacement key number", Rule::Required)?;
                ordered(circle_keys, |key| key.circle, "replacement circle keys")?;
                ordered(deleted_circles, |circle| *circle, "deleted circles")?;
                for key in circle_keys {
                    require(
                        key.key_number > 0,
                        "replacement circle key number",
                        Rule::Required,
                    )?;
                    require(
                        deleted_circles.binary_search(&key.circle).is_err(),
                        "removed member circles",
                        Rule::CircleRemoval,
                    )?;
                }
                Ok(())
            }
            Self::RemoveCircleMember { key_number, .. } => {
                require(*key_number > 0, "replacement key number", Rule::Required)
            }
            Self::RaiseSchema { version, snapshot } => {
                require(*version > 0, "schema version", Rule::Required)?;
                require(
                    snapshot.audience == Audience::Store,
                    "schema snapshot",
                    Rule::Audience,
                )?;
                positive(snapshot.number)
            }
            Self::RaiseFormat { version, snapshot } => {
                require(*version > 0, "format version", Rule::Required)?;
                require(
                    snapshot.audience == Audience::Store,
                    "format snapshot",
                    Rule::Audience,
                )?;
                positive(snapshot.number)
            }
            Self::Reset { snapshot } => positive(snapshot.number),
            Self::AddMember { .. }
            | Self::ChangeRole { .. }
            | Self::RemoveDevice { .. }
            | Self::DeleteCircle { .. }
            | Self::AddCircleMember { .. } => Ok(()),
        }
    }
}

// The tag and named fields are the whole wire layout of each store change.
macro_rules! store_changes {
    ($($tag:literal => $variant:ident { $($field:ident),+ }),+ $(,)?) => {
        impl Wire for StoreChange {
            fn put(&self, out: &mut Encoder) -> Result<(), Error> {
                match self { $(Self::$variant { $($field),+ } => {
                    ($tag as u8).put(out)?; $($field.put(out)?;)+ Ok(())
                }),+ }
            }
            fn get(input: &mut Decoder<'_>) -> Result<Self, Error> {
                match u8::get(input)? {
                    $($tag => Ok(Self::$variant { $($field: Wire::get(input)?),+ }),)+
                    tag => Err(Error::UnknownTag { field: "store change", tag }),
                }
            }
        }
    };
}
store_changes!(
    0 => CreateStore { store, name, admin },
    1 => AddMember { keys, role },
    2 => RemoveMember { member, key_number, circle_keys, deleted_circles },
    3 => ChangeRole { member, role },
    4 => AddDevice { member, device, name },
    5 => RemoveDevice { device },
    6 => CreateCircle { circle, name, creator },
    7 => RenameCircle { circle, name },
    8 => DeleteCircle { circle },
    9 => AddCircleMember { circle, member },
    10 => RemoveCircleMember { circle, member, key_number },
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
    /// Store-log positions the author had read, including their own if present.
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
        self.had_read.own_before(self.position, true)?;
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
