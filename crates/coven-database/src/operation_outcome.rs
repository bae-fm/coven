//! Why the applied state refuses an administrative operation (§10), and
//! how an operation an app's write journaled ended, as `coven_operations`
//! lists it until a write acknowledges it (§11.4): each kind it lists has
//! an outcome type that holds only the outcomes coven gives that kind.

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
    /// The provider account owns the store: its member can't be removed, and
    /// their access can't move to another account (§10.1): OneDrive and
    /// CloudKit can't hand a store to another account, and coven doesn't use
    /// Dropbox's ownership transfer (bae-fm/coven#237).
    StoreOwner,
    /// The target is absent, removed, or cannot be reused.
    TargetGone(EntryTarget),
    /// The resulting store would have no active admin.
    NoAdminLeft,
    /// Either proposed public key was already bound.
    KeysAlreadyUsed,
    /// Another replacement changed the expected binding.
    MemberKeysChanged(MemberId),
    /// The author's device, membership or role does not permit this change,
    /// or, for a snapshot, this device's raise of its audience has not landed
    /// yet (§16.2).
    NotAllowed,
    /// The admission approving a join would carry a copy of every store key
    /// in history, more than one slot holds (format.md D2): about 22,000
    /// keys, one per removal or exposed key (§13.2).
    AdmissionTooLarge,
}

/// How an admin change an app's write journaled ended, for the kinds
/// `RenameStore`, `SetRole`, `RemoveMember`, `RemoveDevice`, `RenameCircle`,
/// `DeleteCircle`, `AddCircleMember` and `RemoveCircleMember` (§11.4):
/// `coven_operations.outcome` holds it from the transaction that ends the
/// operation's journal row until a write acknowledges it, so an outcome
/// nobody awaited is still there to read.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChangeOutcome {
    /// Its entry landed at this slot, which this device has applied.
    Landed(#[serde(with = "slot_json")] coven_merge::Slot),
    /// The applied state already had what it asked for, so it claimed no
    /// slot (§11.2).
    Satisfied,
    /// The applied state refused it once this device had caught up; its
    /// preparation was undone (§11.2, §11.3).
    Rejected(OperationRejection),
    /// This device stopped for good before the operation ended, so it never
    /// will here. Nothing refused it: another device may still make the
    /// same change, and for a copy of this installation, the other copy may
    /// still land it (§14.1, §19.3).
    Stopped(DeviceStop),
}

/// How an admin change an app's write journaled ended, for the kinds
/// `CreateCircle` and `ReplaceMemberKeys`, as `coven_operations.outcome`
/// holds it (§11.4). Each names something no applied state holds yet, a
/// new circle or a member's new pair, so it is never satisfied: a state
/// that already holds it refuses it (§12.3, §15.5).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FreshChangeOutcome {
    /// Its entry landed at this slot, which this device has applied.
    Landed(#[serde(with = "slot_json")] coven_merge::Slot),
    /// The applied state refused it once this device had caught up; its
    /// preparation was undone (§11.2, §11.3).
    Rejected(OperationRejection),
    /// This device stopped for good before the operation ended, so it never
    /// will here (§14.1, §19.3).
    Stopped(DeviceStop),
}

/// How an operation of the kind `Invite` ended, as `coven_operations.outcome`
/// holds it (§11.4, §13.2): the binding an approval journals, which ends as
/// an admin change does, or the invite itself, which `coven_operations`
/// lists only when a sharing refusal ends it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum InviteOutcome {
    /// The binding's entry landed at this slot, which this device has
    /// applied.
    Landed(#[serde(with = "slot_json")] coven_merge::Slot),
    /// The applied state already admitted the request, so the binding
    /// claimed no slot (§11.2).
    Satisfied,
    /// The applied state refused the binding once this device had caught
    /// up (§11.1).
    Rejected(OperationRejection),
    /// This device stopped for good before the binding ended, so it never
    /// will here (§14.1).
    Stopped(DeviceStop),
    /// The invite ended because the provider refused to share the store for
    /// a reason only a person can fix (§13.2). The invite is gone from
    /// `coven_invites`; the outcome stays until a write acknowledges it,
    /// for the app to say what went wrong and what to fix.
    SharingRefused {
        /// The address the invite was to share with.
        account: String,
        /// What was refused.
        refusal: coven_foundation::sharing::SharingRefusal,
    },
}

/// How a storage setup ended, the kind `StorageSetup`, as
/// `coven_operations.outcome` holds it (§11.4, §11.6).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StorageSetupOutcome {
    /// Its entry landed at this slot: slot 1 for a creation, the
    /// `Set access` slot for a reconnect.
    Landed(#[serde(with = "slot_json")] coven_merge::Slot),
    /// A reconnect had nothing to claim: the applied state already held
    /// what it asked for (§11.2).
    Satisfied,
    /// The caught-up state refused the entry it claimed, and setup was
    /// undone (§11.2, §11.6).
    Rejected(OperationRejection),
    /// This device stopped for good before setup ended, so it never will
    /// here (§14.1).
    Stopped(DeviceStop),
    /// The app cancelled it before its point of no return, and what it had
    /// written was undone (§11.6).
    Cancelled,
    /// It was undone before its point of no return, or refused after it
    /// before anything it claimed landed (§11.6).
    Undone(UndoCause),
}

/// An outcome as a write keeps it in `coven_operations.outcome`, in the
/// outcome type of the kind the operation is listed under. The column
/// holds the inner outcome, so each type reads its own kinds' rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListedOutcome {
    /// An outcome of a kind `ChangeOutcome` lists.
    Change(ChangeOutcome),
    /// An outcome of a kind `FreshChangeOutcome` lists.
    FreshChange(FreshChangeOutcome),
    /// An outcome of the kind `Invite`.
    Invite(InviteOutcome),
    /// An outcome of the kind `StorageSetup`.
    StorageSetup(StorageSetupOutcome),
}

impl ListedOutcome {
    /// The outcome as `_coven_app_operations.outcome` keeps it.
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Change(outcome) => encode(outcome),
            Self::FreshChange(outcome) => encode(outcome),
            Self::Invite(outcome) => encode(outcome),
            Self::StorageSetup(outcome) => encode(outcome),
        }
    }
}

impl From<ChangeOutcome> for ListedOutcome {
    fn from(outcome: ChangeOutcome) -> Self {
        Self::Change(outcome)
    }
}

impl From<FreshChangeOutcome> for ListedOutcome {
    fn from(outcome: FreshChangeOutcome) -> Self {
        Self::FreshChange(outcome)
    }
}

impl From<InviteOutcome> for ListedOutcome {
    fn from(outcome: InviteOutcome) -> Self {
        Self::Invite(outcome)
    }
}

impl From<StorageSetupOutcome> for ListedOutcome {
    fn from(outcome: StorageSetupOutcome) -> Self {
        Self::StorageSetup(outcome)
    }
}

/// How a listed operation of any kind ends once this installation stops for
/// good (`operation::end`): landed at the slot its claim took, or stopped.
/// Every kind's outcome type has both, encoded as here.
#[derive(serde::Serialize)]
pub(crate) enum StopOutcome {
    /// Its claim landed at this slot.
    Landed(#[serde(with = "slot_json")] coven_merge::Slot),
    /// Nothing of it landed.
    Stopped(DeviceStop),
}

impl StopOutcome {
    /// The outcome as `_coven_app_operations.outcome` keeps it.
    pub(crate) fn encode(&self) -> Vec<u8> {
        encode(self)
    }
}

/// `outcome` as `_coven_app_operations.outcome` keeps it.
pub(crate) fn encode(outcome: &impl serde::Serialize) -> Vec<u8> {
    serde_json::to_vec(outcome).expect("an outcome is plain data, which always encodes")
}

/// Reads `coven_operations.outcome`, once it is not NULL, as `$outcome`: an
/// outcome `$outcome` doesn't hold, which coven never gives its kinds, fails
/// to read.
macro_rules! read_outcome {
    ($($outcome:ty),*) => {$(
        impl sqlive::rusqlite::types::FromSql for $outcome {
            fn column_result(
                value: sqlive::rusqlite::types::ValueRef<'_>,
            ) -> sqlive::rusqlite::types::FromSqlResult<Self> {
                serde_json::from_slice(value.as_blob()?).map_err(|error| {
                    sqlive::rusqlite::types::FromSqlError::Other(Box::new(error))
                })
            }
        }
    )*};
}
read_outcome!(ChangeOutcome, FreshChangeOutcome, InviteOutcome, StorageSetupOutcome);

/// Why an operation was undone (§11.6).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UndoCause {
    /// The app or the device stopped before its point of no return, and
    /// opening the store undid it.
    Interrupted,
    /// A step failed, with this error's text; the call that ran it returned
    /// the error itself.
    Failed {
        /// The failure's text, secrets and URLs already cut.
        error: String,
    },
}

/// Why a device is not in the store, and won't be again (§13.2, §14.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Removal {
    /// An admin removed this device.
    Device,
    /// This device's member was removed, or left.
    Member,
    /// The admission that published this joined store was refused: its
    /// device never became a member (§13.2).
    NeverAdmitted,
}

/// Why a device stopped for good (§14.1, §19.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeviceStop {
    /// The device is not in the store.
    Removed(Removal),
    /// A reset registered another installation in this device's place.
    Replaced,
    /// History shows another copy of this installation using its device id.
    Copied,
    /// Its member's keys were replaced with a pair it doesn't hold.
    MemberKeysReplaced,
}

/// A slot as the JSON coven keeps it in: its number. For
/// `#[serde(with = "coven_database::slot_json")]`.
pub mod slot_json {
    use serde::{Deserialize, Serialize};
    /// Write the slot's number.
    pub fn serialize<S: serde::Serializer>(
        value: &coven_merge::Slot,
        output: S,
    ) -> Result<S::Ok, S::Error> {
        value.get().serialize(output)
    }
    /// Read a slot from its number.
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        input: D,
    ) -> Result<coven_merge::Slot, D::Error> {
        let number = u64::deserialize(input)?;
        coven_merge::Slot::new(number).ok_or_else(|| {
            serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(number),
                &"a positive slot number",
            )
        })
    }

    /// A list of slots in the same form, for
    /// `#[serde(with = "coven_database::slot_json::list")]`.
    pub mod list {
        use serde::{Deserialize, Serialize};
        /// Write each slot's number.
        pub fn serialize<S: serde::Serializer>(
            value: &[coven_merge::Slot],
            output: S,
        ) -> Result<S::Ok, S::Error> {
            value.iter().map(|slot| slot.get()).collect::<Vec<_>>().serialize(output)
        }
        /// Read slots from their numbers, refusing 0.
        pub fn deserialize<'de, D: serde::Deserializer<'de>>(
            input: D,
        ) -> Result<Vec<coven_merge::Slot>, D::Error> {
            Vec::<u64>::deserialize(input)?
                .into_iter()
                .map(|number| {
                    coven_merge::Slot::new(number).ok_or_else(|| {
                        serde::de::Error::invalid_value(
                            serde::de::Unexpected::Unsigned(number),
                            &"a positive slot number",
                        )
                    })
                })
                .collect()
        }
    }
}

#[cfg(test)]
#[path = "operation_outcome_tests.rs"]
mod tests;
