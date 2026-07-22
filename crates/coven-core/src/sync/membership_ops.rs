//! Membership operations: get members, invite, and revoke.
//!
//! These are the high-level orchestration functions that download the membership
//! chain from the storage, perform the operation, and upload the results.

use rusqlite::OptionalExtension;
use tracing::{debug, info};

use crate::encryption::EncryptionService;
#[cfg(test)]
use crate::encryption::MasterKeyring;
use crate::keys::{KeyError, MasterKeyCustody, UserKeypair};

use super::cloud_storage::{CloudCipherAccess, PendingRotation};
use super::hlc::Hlc;
use super::invite::InviteError;
use super::membership::{
    AuthorHead, MemberInfo, MemberRole, MembershipChain, MembershipChange, MembershipConflict,
    MembershipCoord, MembershipEntry, MembershipGrantId, MembershipHeadRef,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use super::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage};
use super::store_commit::{GrantStreamAnchor, ResolvedStoreDeviceState, StoreRootRef};
use super::store_objects::StoreObjectError;
use crate::database::Database;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

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
    StoreObject(#[from] super::store_objects::StoreObjectError),
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
    #[error("no membership chain exists")]
    NoMembershipChain,
    #[error("membership chain has no founder")]
    ChainHasNoFounder,
    #[error("membership has an unresolved semantic conflict: {0:?}")]
    SemanticConflict(Box<MembershipConflict>),
    #[error("sharing requires an encrypted cloud home")]
    NotEncryptedHome,
}

pub(crate) fn require_resolved_membership(
    chain: &MembershipChain,
) -> Result<(), MembershipOpsError> {
    match chain.conflict() {
        Some(conflict) => Err(MembershipOpsError::SemanticConflict(Box::new(
            conflict.clone(),
        ))),
        None => Ok(()),
    }
}

async fn required_store_root_ref(db: &Database) -> Result<StoreRootRef, MembershipOpsError> {
    db.local_store_root_ref()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)
}

pub const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";
pub(crate) const MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX: &str = "membership_head_cursor/";

fn validate_invitation(
    user_keypair: &UserKeypair,
    public_key_hex: &str,
    role: &MemberRole,
) -> Result<(), MembershipOpsError> {
    if *role == MemberRole::Owner {
        return Err(MembershipOpsError::Invite(InviteError::Membership(
            super::membership::MembershipError::OwnerPromotionRequired,
        )));
    }
    if public_key_hex == hex::encode(user_keypair.public_key()) {
        return Err(MembershipOpsError::SelfInvite);
    }
    Ok(())
}

mod cursors;
mod exact_chain;
mod key_rotation;
mod listing;
mod merge;

pub use cursors::seed_head_watermark;
pub use exact_chain::AnchoredChainError;
pub use key_rotation::apply_key_rotation;
pub use listing::{current_membership_floor, get_members};
pub use merge::{invite_member, remove_member};

pub(crate) use cursors::{
    load_and_persist_owner_anchor, upsert_head_cursor_on, validate_membership_floor,
};
use cursors::{persist_head_cursors, read_head_cursors};
use exact_chain::map_membership_object_error;
pub(crate) use exact_chain::{
    authorize_loaded_membership_author, load_anchored_chain_at_exact_heads,
    load_anchored_chain_at_exact_heads_with_root,
    load_anchored_chain_at_exact_heads_with_root_and_verified_activations,
    load_current_exact_chain, load_exact_anchored_chain, load_exact_membership_head,
    project_anchored_chain_to_verified_store_prefix, MembershipAuthorRequirement,
};

#[cfg(test)]
use cursors::head_cursor_key;
#[cfg(test)]
use exact_chain::{
    load_exact_membership_graph_objects, membership_projection_statuses,
    LoadedExactMembershipGraph, MembershipProjectionStatus,
};

#[cfg(test)]
mod tests;
