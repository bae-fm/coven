//! Why the applied state refuses an administrative operation (§10), and
//! how an operation an app's write journaled ended, as `coven_operations`
//! lists it until a write acknowledges it (§11.4).

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

/// How an operation an app's write journaled ended (§11.4):
/// `coven_operations.outcome` holds it from the transaction that ends the
/// operation's journal row until a write acknowledges it, so an outcome
/// nobody awaited is still there to read.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationOutcome {
    /// Its entry landed at this slot, which this device has applied.
    Landed(#[serde(with = "slot")] coven_merge::Slot),
    /// The applied state already had what it asked for, so it claimed no
    /// slot (§11.2).
    Satisfied,
    /// The applied state refused it once this device had caught up; its
    /// preparation was undone (§11.2, §11.3).
    Rejected(OperationRejection),
}

impl OperationOutcome {
    /// The outcome as `_coven_app_operations.outcome` keeps it.
    pub(crate) fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("an outcome is plain data, which always encodes")
    }
}

/// Reads `coven_operations.outcome`, once it is not NULL.
impl rusqlite::types::FromSql for OperationOutcome {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        serde_json::from_slice(value.as_blob()?)
            .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

mod slot {
    use serde::{Deserialize, Serialize};
    pub(super) fn serialize<S: serde::Serializer>(
        value: &coven_merge::Slot,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        value.0.serialize(output)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<coven_merge::Slot, D::Error> {
        Ok(coven_merge::Slot(u64::deserialize(input)?))
    }
}
