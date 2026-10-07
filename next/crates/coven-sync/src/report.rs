//! Store-log results presented to the application (§20.5).

use coven_crypto::{CryptoError, MemberId};
use coven_database::DropReason;
use coven_format::{
    store_log::{MemberRole, SnapshotId, StoreChange},
    value::EntryId,
};
use coven_foundation::id_source::{CircleId, DeviceId, StoreId};
use std::sync::Arc;

/// Results of the store-log step. Waiting entries remain only in storage.
#[derive(Debug, Default)]
pub struct SyncReport {
    /// Permanently failed operations awaiting app retry or discard.
    pub blocked_operations: Vec<crate::BlockedOperation>,
    /// S3 key ids awaiting confirmation of deletion in the provider console.
    pub access_keys_to_delete: Vec<crate::AccessKeyToDelete>,
    /// This member's dropped entries in the resulting replay.
    pub dropped_entries: Vec<DroppedEntry>,
    /// Entries or sealed keys that failed validation, with their storage paths.
    pub damaged_objects: Vec<DamagedObject>,
}

/// An object whose checks failed (§19.1).
#[derive(Debug, thiserror::Error)]
#[error("damaged object at {path}: {failure}")]
pub struct DamagedObject {
    /// The exact path read from storage.
    pub path: String,
    /// The failed check and its cause.
    #[source]
    pub failure: ObjectCheckFailure,
}

/// Authentication and parsing failures retain their original causes.
#[derive(Debug, thiserror::Error)]
pub enum ObjectCheckFailure {
    /// Opening the object or its authenticated path failed.
    #[error("decryption failed: {0}")]
    Decryption(#[source] CryptoError),
    /// The author's signature did not verify.
    #[error("signature failed: {0}")]
    Signature(#[source] CryptoError),
    /// The bytes or their causal metadata violate the format.
    #[error("parse failed: {0}")]
    Parse(#[source] Arc<dyn std::error::Error + Send + Sync>),
}

/// One of this member's entries that the current replay drops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DroppedEntry {
    /// Its immutable position.
    pub entry: EntryId,
    /// Its intended effect, without cryptographic material.
    pub change: StoreLogChange,
    /// Why it was dropped.
    pub reason: DropReason,
}

/// The intended effect of a dropped store-log entry (§20.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreLogChange {
    /// Creates the store and registers its first device.
    CreateStore {
        /// The store’s identity.
        store: StoreId,
        /// The chosen name.
        name: String,
        /// The first admin.
        admin: MemberId,
        /// The writing device’s name.
        device_name: String,
    },
    /// Adds a member with the stated role.
    AddMember {
        /// The affected member.
        member: MemberId,
        /// The chosen role.
        role: MemberRole,
    },
    /// Removes a member and their devices.
    RemoveMember {
        /// The affected member.
        member: MemberId,
    },
    /// Sets the member’s role.
    SetMemberRole {
        /// The affected member.
        member: MemberId,
        /// The chosen role.
        role: MemberRole,
    },
    /// Adds a device belonging to the author.
    AddDevice {
        /// The affected device.
        device: DeviceId,
    },
    /// Removes the named device.
    RemoveDevice {
        /// The affected device.
        device: DeviceId,
    },
    /// Creates a circle with the author as its first member.
    CreateCircle {
        /// The affected circle.
        circle: CircleId,
        /// The chosen name.
        name: String,
    },
    /// Changes a circle’s name.
    RenameCircle {
        /// The affected circle.
        circle: CircleId,
        /// The chosen name.
        name: String,
    },
    /// Deletes a circle.
    DeleteCircle {
        /// The affected circle.
        circle: CircleId,
    },
    /// Adds a store member to a circle.
    AddCircleMember {
        /// The affected circle.
        circle: CircleId,
        /// The affected member.
        member: MemberId,
    },
    /// Removes a circle member and replaces its key.
    RemoveCircleMember {
        /// The affected circle.
        circle: CircleId,
        /// The affected member.
        member: MemberId,
    },
    /// Raises the snapshot audience’s schema version.
    SchemaChange {
        /// The raised version number.
        version: u32,
        /// The snapshot and its audience.
        snapshot: SnapshotId,
    },
    /// Raises the snapshot audience’s format version.
    FormatChange {
        /// The raised version number.
        version: u16,
        /// The snapshot and its audience.
        snapshot: SnapshotId,
    },
    /// Resets an audience to the named snapshot.
    Reset {
        /// The snapshot and its audience.
        snapshot: SnapshotId,
    },
}

impl From<&StoreChange> for StoreLogChange {
    fn from(change: &StoreChange) -> Self {
        match change.clone() {
            StoreChange::CreateStore {
                store,
                name,
                admin,
                device_name,
                ..
            } => Self::CreateStore {
                store,
                name,
                admin: admin.signing,
                device_name,
            },
            StoreChange::AddMember { keys, role, .. } => Self::AddMember {
                member: keys.signing,
                role,
            },
            StoreChange::RemoveMember { member, .. } => Self::RemoveMember { member },
            StoreChange::ChangeRole { member, role } => Self::SetMemberRole { member, role },
            StoreChange::AddDevice { device, .. } => Self::AddDevice { device },
            StoreChange::RemoveDevice { device } => Self::RemoveDevice { device },
            StoreChange::CreateCircle { circle, name, .. } => Self::CreateCircle { circle, name },
            StoreChange::RenameCircle { circle, name } => Self::RenameCircle { circle, name },
            StoreChange::DeleteCircle { circle } => Self::DeleteCircle { circle },
            StoreChange::AddCircleMember { circle, member } => {
                Self::AddCircleMember { circle, member }
            }
            StoreChange::RemoveCircleMember { circle, member, .. } => {
                Self::RemoveCircleMember { circle, member }
            }
            StoreChange::RaiseSchema { version, snapshot } => {
                Self::SchemaChange { version, snapshot }
            }
            StoreChange::RaiseFormat { version, snapshot } => {
                Self::FormatChange { version, snapshot }
            }
            StoreChange::Reset { snapshot } => Self::Reset { snapshot },
        }
    }
}
