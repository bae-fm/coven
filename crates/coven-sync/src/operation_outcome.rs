//! How an operation ended (§11.4, api.md E6): an entry's outcome as the
//! call that waits for it reads it, and as `coven_operations` lists it in
//! the outcome type of the operation's kind.

use crate::{OperationKind, OperationRejection, SyncError};
use coven_database::types::{FromSql, FromSqlError, FromSqlResult, ValueRef};
use coven_database::{ChangeOutcome, FreshChangeOutcome, InviteOutcome, ListedOutcome};
use coven_merge::Slot;

/// How an operation that claims an admin entry ended (§11.2): every kind
/// the operation journal runs ends one of these ways.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EntryOutcome {
    /// Its entry landed at this slot, which this device has applied.
    Landed(Slot),
    /// The applied state already had what it asked for, so it claimed no
    /// slot.
    Satisfied,
    /// The caught-up state refused it; its preparation was undone.
    Rejected(OperationRejection),
}

impl EntryOutcome {
    /// The landed slot, or the rejection as the error of a call that waits
    /// for the operation itself.
    pub(crate) fn into_slot(self) -> Result<Option<Slot>, SyncError> {
        match self {
            Self::Landed(slot) => Ok(Some(slot)),
            Self::Satisfied => Ok(None),
            Self::Rejected(reason) => Err(SyncError::Rejected(reason)),
        }
    }
}

/// An entry's outcome in each listed outcome type that holds every way an
/// entry ends.
macro_rules! from_entry {
    ($($listed:ty),*) => {$(
        impl From<EntryOutcome> for $listed {
            fn from(outcome: EntryOutcome) -> Self {
                match outcome {
                    EntryOutcome::Landed(slot) => Self::Landed(slot),
                    EntryOutcome::Satisfied => Self::Satisfied,
                    EntryOutcome::Rejected(reason) => Self::Rejected(reason),
                }
            }
        }
    )*};
}
from_entry!(ChangeOutcome, InviteOutcome);

impl TryFrom<EntryOutcome> for FreshChangeOutcome {
    type Error = SyncError;

    /// A fresh change's entry names what no applied state holds yet, so
    /// one that ends satisfied is a bug.
    fn try_from(outcome: EntryOutcome) -> Result<Self, SyncError> {
        match outcome {
            EntryOutcome::Landed(slot) => Ok(Self::Landed(slot)),
            EntryOutcome::Rejected(reason) => Ok(Self::Rejected(reason)),
            EntryOutcome::Satisfied => Err(coven_database::DbError::Invariant {
                what: "a change naming a new circle or pair is never satisfied",
            }
            .into()),
        }
    }
}

/// How a call that journals an operation and runs the pass that settles it
/// reads the operation's listed outcome: it ended in the pass, so it landed
/// or was rejected.
#[cfg(test)]
pub(crate) trait SettledByPass: std::fmt::Debug + Sized {
    /// Whether the operation took effect, or the rejection as the error.
    /// An outcome the pass can't give, a device that stopped, panics.
    fn settled(self) -> Result<(), SyncError>;
}

#[cfg(test)]
impl SettledByPass for FreshChangeOutcome {
    fn settled(self) -> Result<(), SyncError> {
        match self {
            Self::Landed(_) => Ok(()),
            Self::Rejected(reason) => Err(SyncError::Rejected(reason)),
            other => panic!("the pass ended the operation {other:?}"),
        }
    }
}

#[cfg(test)]
impl SettledByPass for InviteOutcome {
    fn settled(self) -> Result<(), SyncError> {
        match self {
            Self::Landed(_) | Self::Satisfied => Ok(()),
            Self::Rejected(reason) => Err(SyncError::Rejected(reason)),
            other => panic!("the pass ended the operation {other:?}"),
        }
    }
}

/// The outcome types `coven_operations` lists outcomes in, one for each
/// group of kinds that end the same ways.
#[derive(PartialEq)]
enum OutcomeType {
    Change,
    FreshChange,
    Invite,
    StorageSetup,
}

impl OperationKind {
    /// The type `coven_operations` lists this kind's outcomes in; `None` for
    /// a kind no app write or call journals there.
    fn outcome_type(self) -> Option<OutcomeType> {
        use OperationKind as K;
        match self {
            K::RenameStore
            | K::SetRole
            | K::RemoveMember
            | K::RemoveDevice
            | K::RenameCircle
            | K::DeleteCircle
            | K::AddCircleMember
            | K::RemoveCircleMember => Some(OutcomeType::Change),
            K::CreateCircle | K::ReplaceMemberKeys => Some(OutcomeType::FreshChange),
            K::Invite => Some(OutcomeType::Invite),
            K::StorageSetup => Some(OutcomeType::StorageSetup),
            K::CreateStore
            | K::SetUpStorage
            | K::AddDevice
            | K::SetAccess
            | K::RetainSequence
            | K::RetainFiles
            | K::WriteSnapshot
            | K::SchemaChange
            | K::Reset
            | K::ReloadFromSnapshot
            | K::RequiredReload
            | K::RotateKey
            | K::SendKeyCopy
            | K::Pairing
            | K::RevokeAccess => None,
        }
    }

    /// Whether `coven_operations` lists this kind's outcomes in the type
    /// `outcome` has, which a test's outcome outside a store must be.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn lists(self, outcome: &ListedOutcome) -> bool {
        let of_outcome = match outcome {
            ListedOutcome::Change(_) => OutcomeType::Change,
            ListedOutcome::FreshChange(_) => OutcomeType::FreshChange,
            ListedOutcome::Invite(_) => OutcomeType::Invite,
            ListedOutcome::StorageSetup(_) => OutcomeType::StorageSetup,
        };
        self.outcome_type() == Some(of_outcome)
    }

    /// How `coven_operations` lists an operation of this kind that ended
    /// `outcome`, in the outcome type of its kind; `None` for a kind it
    /// never lists, whose outcome is nobody's to read.
    pub(crate) fn listed_outcome(
        self,
        outcome: EntryOutcome,
    ) -> Result<Option<ListedOutcome>, SyncError> {
        Ok(match self.outcome_type() {
            Some(OutcomeType::Change) => Some(ChangeOutcome::from(outcome).into()),
            Some(OutcomeType::FreshChange) => Some(FreshChangeOutcome::try_from(outcome)?.into()),
            Some(OutcomeType::Invite) => Some(InviteOutcome::from(outcome).into()),
            Some(OutcomeType::StorageSetup) => {
                return Err(coven_database::DbError::Invariant {
                    what: "storage setup ends with its own outcome, never an entry's",
                }
                .into())
            }
            None => None,
        })
    }

    /// The kind's name, as `coven_operations.kind` lists it.
    pub fn listed(self) -> String {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::String(name)) => name,
            other => unreachable!("an operation kind is named by its variant: {other:?}"),
        }
    }
}

/// Reads `coven_operations.kind`.
impl FromSql for OperationKind {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        serde_json::from_value(serde_json::Value::String(value.as_str()?.to_owned()))
            .map_err(|error| FromSqlError::Other(Box::new(error)))
    }
}
