//! Administrative validation against the preceding sequence prefix (§10).

use coven_foundation::id_source::{CircleId, DeviceId, MemberId};

/// The target a proposed change can no longer affect.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EntryTarget {
    /// A store member.
    Member(MemberId),
    /// An installation.
    Device(DeviceId),
    /// A circle.
    Circle(CircleId),
}

/// Why the current applied state refuses an administrative operation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationRejection {
    /// The provider account owns the store and cannot be removed (§10.1):
    /// OneDrive and CloudKit can't hand a store to another account, and coven
    /// doesn't use Dropbox's ownership transfer (bae-fm/coven#237).
    StoreOwner,
    /// The target is absent, removed, or cannot be reused.
    TargetGone(EntryTarget),
    /// The resulting store would have no active admin.
    NoAdminLeft,
    /// Either proposed public key was already bound.
    KeysAlreadyUsed,
    /// Another replacement changed the expected binding.
    MemberKeysChanged(MemberId),
    /// The author's device, membership or role does not permit this change.
    NotAllowed,
}
