//! The immutable facts a replay needs from an entry's recorded past (§9).

use std::collections::BTreeSet;

use coven_crypto::MemberId;
use coven_format::store_log::StoreLogEntry;
use coven_foundation::id_source::CircleId;

use crate::DbError;

/// An entry and the checks computed by sync against exactly its recorded past.
/// Both are immutable once the database applies the entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayEntry {
    /// The checked entry, stored using format's encoding.
    pub entry: StoreLogEntry,
    /// Authority and the author-view facts needed by effects and conflicts.
    pub check: StoreLogCheck,
}

/// The result of checking an entry against its author's immutable view.
/// This retains only facts needed during later replays, not the whole past state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreLogCheck {
    /// Authorized; this change needs no further facts from its author's view.
    Allowed,
    /// The author's view did not allow the change.
    NotAllowed,
    /// The removal's replacement keys differ from its author's shared circles.
    WrongCircleKeys,
    /// An authorized device removal, with the owner its author observed.
    DeviceOwner(MemberId),
    /// An authorized store or circle member removal, with the circles it deletes
    /// in its author's view. The set may be empty.
    DeletedCircles(BTreeSet<CircleId>),
}

impl StoreLogCheck {
    // This is a local table encoding, not a downloaded format. The tag determines
    // the payload's shape; invalid or noncanonical bytes fail the database read.
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Allowed => vec![0],
            Self::NotAllowed => vec![1],
            Self::WrongCircleKeys => vec![2],
            Self::DeviceOwner(owner) => {
                let mut bytes = vec![3];
                bytes.extend(owner.to_bytes());
                bytes
            }
            Self::DeletedCircles(circles) => {
                let mut bytes = vec![4];
                for circle in circles {
                    bytes.extend(circle.0.as_bytes());
                }
                bytes
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        match bytes {
            [0] => Ok(Self::Allowed),
            [1] => Ok(Self::NotAllowed),
            [2] => Ok(Self::WrongCircleKeys),
            [3, owner @ ..] if owner.len() == 32 => Ok(Self::DeviceOwner(
                MemberId::from_bytes(owner.try_into().expect("checked owner length"))
                    .map_err(|_| DbError::DamagedDatabase)?,
            )),
            [4, circles @ ..] if circles.len().is_multiple_of(16) => {
                let mut result = BTreeSet::new();
                for bytes in circles.chunks_exact(16) {
                    let circle = CircleId(uuid::Uuid::from_bytes(
                        bytes.try_into().expect("exact circle chunk"),
                    ));
                    if result.last().is_some_and(|previous| *previous >= circle) {
                        return Err(DbError::DamagedDatabase);
                    }
                    result.insert(circle);
                }
                Ok(Self::DeletedCircles(result))
            }
            _ => Err(DbError::DamagedDatabase),
        }
    }
}

#[cfg(test)]
#[path = "store_log_check_tests.rs"]
mod tests;
