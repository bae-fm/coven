//! Membership operations: list members, admit, and revoke.
//!
//! These are the high-level orchestration functions that download the membership
//! chain from the storage, perform the operation, and upload the results.

use coven_keys::keys::KeyError;
use coven_protocol::membership::MembershipConflict;
use coven_protocol::objects::StorageError;
use coven_protocol::objects::StoreObjectError;
use coven_storage::CloudHomeJoinInfo;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct MemberAdmission {
    pub store_id: String,
    pub store_name: String,
    pub join_info: CloudHomeJoinInfo,
    pub owner_pubkey: String,
    pub wrapped_key: coven_protocol::wrapped_store_key::WrappedStoreKeyRef,
    pub store_root: coven_protocol::store_commit::StoreRootRef,
    pub membership_floor: coven_protocol::membership::MembershipFloor,
}

/// Why a high-level membership operation (list members, admit, remove, rotate)
/// failed. The security-critical orchestration layer that downloads the chain,
/// performs the operation, and uploads the result: it preserves the typed error
/// each step already produces — [`StorageError`], the owner-anchored
/// [`AnchoredChainError`], the [`MembershipMutationError`] the admit/revoke path raises,
/// [`KeyError`] — rather than flattening them into a string,
/// and names the domain rules it enforces in place as their own variants.
#[derive(Debug, thiserror::Error)]
pub enum MembershipOpsError {
    #[error("membership storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Store protocol object error: {0}")]
    StoreObject(#[from] coven_protocol::objects::StoreObjectError),
    #[error("membership database state error: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("Store access failed: {0}")]
    Store(#[from] crate::sync::store::StoreError),
    #[error("{0}")]
    Chain(#[from] AnchoredChainError),
    #[error("{0}")]
    Mutation(#[from] MembershipMutationError),
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
    #[error("cannot admit this device as a new member")]
    SelfAdmission,
    #[error("the identity is already a member with different role or provider account")]
    ExistingMemberMismatch,
    #[error("the existing member does not have exactly one current wrapped Store key")]
    ExistingMemberKeyAuthority,
    /// Admitting into a store whose founder entry is missing (a fresh store
    /// that never founded, or a wiped `membership/*`). Bootstrapping a founder on
    /// the spot is the takeover primitive, so admission is refused (issue #104).
    #[error(
        "no membership chain to admit into: the store's founder entry is \
         missing (it is established at store creation)"
    )]
    NoFounderChainForAdmission,
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
    #[error("membership object: {0}")]
    Object(#[from] StoreObjectError),
    #[error("membership database: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("membership protocol: {0}")]
    Membership(#[from] coven_protocol::membership::MembershipError),
    #[error("membership provider probe: {0}")]
    ProviderProbe(#[from] coven_protocol::provider::ProviderProbeError),
    #[error("membership floor failed validation: {0}")]
    InvalidFloor(#[from] coven_protocol::membership::MembershipFloorError),
    #[error("membership Store pull: {0}")]
    StorePull(#[source] Box<crate::sync::store::StorePullError>),
    #[error("chain founder {founder:?} is not the pinned owner {owner}")]
    FounderMismatch {
        founder: Option<String>,
        owner: String,
    },
}

impl From<crate::sync::store::StorePullError> for AnchoredChainError {
    fn from(error: crate::sync::store::StorePullError) -> Self {
        Self::StorePull(Box::new(error))
    }
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
            error => Self::Object(error),
        }
    }
}

pub use mutation::MembershipMutationError;

#[cfg(test)]
mod tests;
