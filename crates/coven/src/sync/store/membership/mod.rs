//! Membership operations: get members, invite, and revoke.
//!
//! These are the high-level orchestration functions that download the membership
//! chain from the storage, perform the operation, and upload the results.

use crate::protocol::membership::MembershipConflict;
use crate::protocol::objects::StorageError;
use crate::protocol::objects::StoreObjectError;
use coven_keys::keys::KeyError;

/// Why a high-level membership operation (list members, invite, remove, rotate)
/// failed. The security-critical orchestration layer that downloads the chain,
/// performs the operation, and uploads the result: it preserves the typed error
/// each step already produces — [`StorageError`], the owner-anchored
/// [`AnchoredChainError`], the [`InviteError`] the invite/revoke path raises,
/// [`KeyError`] — rather than flattening them into a string,
/// and names the domain rules it enforces in place as their own variants.
#[derive(Debug, thiserror::Error)]
pub enum MembershipOpsError {
    #[error("membership storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Store protocol object error: {0}")]
    StoreObject(#[from] crate::protocol::objects::StoreObjectError),
    #[error("membership database state error: {0}")]
    Database(String),
    #[error("{0}")]
    Chain(#[from] AnchoredChainError),
    #[error("{0}")]
    Invite(#[from] InviteError),
    /// The removal and cloud rotation committed, but this device could not adopt
    /// the rotated key into custody and its live cipher. The exact removal journal
    /// and rotation gate remain durable, and retrying the same removal resumes it.
    #[error(
        "member removal committed the cloud key rotation, but this device could not \
         adopt the rotated key locally: {source}; retry the same removal"
    )]
    RotationCommittedAdoptionFailed {
        #[source]
        source: KeyError,
    },
    #[error("cannot invite yourself")]
    SelfInvite,
    /// Inviting into a store whose founder entry is missing (a fresh store
    /// that never founded, or a wiped `membership/*`). Bootstrapping a founder on
    /// the spot is the takeover primitive, so the invite is refused (issue #104).
    #[error(
        "no membership chain to invite into: the store's founder entry is \
         missing (it is established at store creation, not on invite)"
    )]
    NoFounderChain,
    #[error("membership chain has no founder")]
    ChainHasNoFounder,
    #[error("membership has an unresolved semantic conflict: {0:?}")]
    SemanticConflict(Box<MembershipConflict>),
    #[error("sharing requires an encrypted cloud home")]
    NotEncryptedHome,
}

mod mutation;

/// Why loading an owner-anchored membership chain failed.
#[derive(Debug, thiserror::Error)]
pub enum AnchoredChainError {
    #[error("membership storage unavailable while {operation}: {source}")]
    StorageUnavailable {
        operation: String,
        #[source]
        source: StorageError,
    },
    #[error("membership chain failed to load/validate: {0}")]
    LoadFailed(String),
    #[error("chain founder {founder:?} is not the pinned owner {owner}")]
    FounderMismatch {
        founder: Option<String>,
        owner: String,
    },
}

impl AnchoredChainError {
    pub(crate) fn from_store_object(error: StoreObjectError) -> Self {
        match error {
            StoreObjectError::Storage(source @ StorageError::Storage(_))
            | StoreObjectError::Storage(source @ StorageError::RotationPending(_)) => {
                Self::StorageUnavailable {
                    operation: "discovering immutable membership objects".to_string(),
                    source,
                }
            }
            error => Self::LoadFailed(error.to_string()),
        }
    }
}

pub(crate) use mutation::InviteError;

#[cfg(test)]
mod tests;
