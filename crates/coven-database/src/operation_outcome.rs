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

/// How an operation an app's write journaled ended (§11.4):
/// `coven_operations.outcome` holds it from the transaction that ends the
/// operation's journal row until a write acknowledges it, so an outcome
/// nobody awaited is still there to read.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationOutcome {
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
    /// The app cancelled it before its point of no return, and what it had
    /// written was undone (§11.6).
    Cancelled,
    /// It was undone before its point of no return, or refused after it
    /// before anything it claimed landed (§11.6).
    Undone(UndoCause),
}

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

/// Why a device stopped for good (§14.1, §19.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeviceStop {
    /// The device, or its member, was removed.
    Removed,
    /// The admission that published this joined store was refused: its
    /// device never became a member (§13.2).
    NotAdmitted,
    /// A reset registered another installation in this device's place.
    Replaced,
    /// History shows another copy of this installation using its device id.
    Copied,
    /// Its member's keys were replaced with a pair it doesn't hold.
    MemberKeysReplaced,
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
