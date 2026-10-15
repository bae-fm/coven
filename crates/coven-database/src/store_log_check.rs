//! Immutable landing and author-view checks retained for replay (§9).

use std::collections::BTreeMap;

use coven_format::store_log::StoreLogEntry;
use coven_foundation::id_source::{CircleId, MemberId};

use crate::DbError;

/// An entry, its first publication time and checks against stored history and
/// its recorded past. These are immutable once the database applies the entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayEntry {
    /// The checked entry, stored using format's encoding.
    pub entry: StoreLogEntry,
    /// Provider time of the first complete publication; retries cannot replace it.
    pub stored_at: coven_format::value::StorageTime,
    /// Permanent landing exclusion, or the authority and author-view facts used by replay.
    pub check: StoreLogCheck,
}

/// What removing a member does to a circle in the author's recorded past.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircleRemoval {
    /// Other members remain in that view.
    Member,
    /// The removed member was its only member, so the removal deletes it.
    Circle,
}

/// The landing verdict or checks against an admitted entry's immutable author view.
/// This retains only facts needed during later replays, not the whole past state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreLogCheck {
    /// Permanently excluded by the landing check against complete stored history.
    /// Replay never applies it, but whether its recorded view authorized it still
    /// decides if its key introduction counts (§9, §11).
    LandedTooLate {
        /// Whether its author's recorded view allowed the change.
        authorized: bool,
    },
    /// Authorized; this change needs no further facts from its author's view.
    Allowed,
    /// The author's view did not allow the change.
    NotAllowed,
    /// An addition of an id not active in the author's view, or a member-key
    /// replacement, whose pair shares a signing or sealing key with a pair
    /// already bound in that view (§9).
    KeysAlreadyUsed,
    /// An authorized device removal, with the owner its author observed.
    DeviceOwner(MemberId),
    /// An authorized store or circle member removal. Each circle contained the
    /// removed member in the author's view; the value records whether it deletes
    /// that circle. The map may be empty.
    MemberRemoval(BTreeMap<CircleId, CircleRemoval>),
}

impl StoreLogCheck {
    /// Whether the author's recorded view allowed the change, landed in time or not.
    pub fn authorized(&self) -> bool {
        match self {
            Self::LandedTooLate { authorized } => *authorized,
            Self::NotAllowed | Self::KeysAlreadyUsed => false,
            Self::Allowed | Self::DeviceOwner(_) | Self::MemberRemoval(_) => true,
        }
    }

    /// Whether the landing check permanently excluded the entry from replay.
    pub fn landed_too_late(&self) -> bool {
        matches!(self, Self::LandedTooLate { .. })
    }

    /// Whether this removal deletes the circle in its immutable author view.
    pub fn deletes_circle(&self, circle: CircleId) -> bool {
        let Self::MemberRemoval(circles) = self else {
            unreachable!("authorized member removal has its circle classifications")
        };
        circles.get(&circle) == Some(&CircleRemoval::Circle)
    }

    // This is a local table encoding, not a downloaded format. The tag determines
    // the payload's shape; invalid or noncanonical bytes fail the database read.
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::LandedTooLate { authorized } => vec![4, u8::from(*authorized)],
            Self::Allowed => vec![0],
            Self::NotAllowed => vec![1],
            Self::KeysAlreadyUsed => vec![5],
            Self::DeviceOwner(owner) => {
                let mut bytes = vec![2];
                bytes.extend(owner.0.as_bytes());
                bytes
            }
            Self::MemberRemoval(circles) => {
                let mut bytes = vec![3];
                for (circle, removal) in circles {
                    bytes.extend(circle.0.as_bytes());
                    bytes.push(match removal {
                        CircleRemoval::Member => 0,
                        CircleRemoval::Circle => 1,
                    });
                }
                bytes
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        match bytes {
            [4, authorized @ (0 | 1)] => Ok(Self::LandedTooLate {
                authorized: *authorized == 1,
            }),
            [0] => Ok(Self::Allowed),
            [1] => Ok(Self::NotAllowed),
            [5] => Ok(Self::KeysAlreadyUsed),
            [2, owner @ ..] if owner.len() == 16 => Ok(Self::DeviceOwner(MemberId(
                uuid::Uuid::from_bytes(owner.try_into().expect("checked owner length")),
            ))),
            [3, circles @ ..] if circles.len().is_multiple_of(17) => {
                let mut result = BTreeMap::new();
                for bytes in circles.chunks_exact(17) {
                    let circle = CircleId(uuid::Uuid::from_bytes(
                        bytes[..16].try_into().expect("exact circle chunk"),
                    ));
                    let removal = match bytes[16] {
                        0 => CircleRemoval::Member,
                        1 => CircleRemoval::Circle,
                        _ => return Err(DbError::DamagedDatabase),
                    };
                    if result
                        .last_key_value()
                        .is_some_and(|(previous, _)| *previous >= circle)
                    {
                        return Err(DbError::DamagedDatabase);
                    }
                    result.insert(circle, removal);
                }
                Ok(Self::MemberRemoval(result))
            }
            _ => Err(DbError::DamagedDatabase),
        }
    }
}

#[cfg(test)]
#[path = "store_log_check_tests.rs"]
mod tests;
