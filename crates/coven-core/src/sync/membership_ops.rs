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
use crate::storage::cloud::{CloudAccessOutcome, CloudAccessState};

use super::cloud_storage::{CloudCipherAccess, PendingRotation};
use super::hlc::Hlc;
use super::invite::InviteError;
use super::membership::{
    AuthorHead, MemberInfo, MemberRole, MembershipChain, MembershipChange, MembershipConflict,
    MembershipCoord, MembershipEntry, MembershipGrantId, MembershipHeadRef,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use super::storage::{
    CoordinationStorage, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use super::store_commit::{
    GrantStreamAnchor, ResolvedStoreDeviceState, StoreBatchCommitRef, StoreControl, StoreRootRef,
};
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
    #[error(transparent)]
    Serial(#[from] super::store_outbound::StoreOutboundError),
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

pub(crate) async fn load_current_exact_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: Option<&str>,
    db: Option<&Database>,
) -> Result<MembershipChain, MembershipOpsError> {
    let _membership_load = match db {
        Some(db) => Some(db.lock_membership_load().await),
        None => None,
    };
    let cursors = match db {
        Some(db) => read_head_cursors(db)
            .await
            .map_err(MembershipOpsError::Database)?,
        None => Vec::new(),
    };
    let chain = Box::pin(load_exact_anchored_chain(
        storage,
        root,
        &cursors,
        owner_pubkey,
    ))
    .await?;
    if let Some(db) = db {
        persist_head_cursors(db, chain.head_refs())
            .await
            .map_err(MembershipOpsError::Database)?;
    }
    Ok(chain)
}

/// `protocol_state` key holding the hex Ed25519 pubkey of the store's established
/// owner — pinned at create (the creator), join (the invite's owner), or restore.
/// The membership chain is anchored to it: a chain whose founder differs is a
/// takeover attempt and is rejected (issue #95).
pub const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";
pub(crate) const MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX: &str = "membership_head_cursor/";

pub struct SerialMembershipContext<'a> {
    pub coordination: &'a dyn CoordinationStorage,
    pub device_id: String,
}

/// The per-author-stream membership-head floor at the chain's current committed
/// state: every stream's highest signed seq ([`MembershipChain::author_heads`]). A
/// pre-initialization bootstrap read can return an empty floor before a founder is
/// established. Minted into an invite or restore code so the joiner or restorer
/// can seed its watermark ([`seed_head_watermark`]) before its first sync cycle.
///
/// `watermark_db`, when present, makes this read monotonic the same way every
/// other chain load is: the minting device's own view of the chain never
/// regresses either. Shares [`load_anchored_chain`]'s fail-closed stance — for a
/// `pinned_owner` a chain that won't validate or anchor is a takeover attempt,
/// not silently treated as absent.
pub async fn current_membership_floor(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    pinned_owner: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<Vec<MembershipHeadRef>, MembershipOpsError> {
    let chain = load_current_exact_chain(storage, root, pinned_owner, watermark_db).await?;
    Ok(chain.head_refs().to_vec())
}

/// Read the membership chain from the sync storage and return the current members.
pub async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    db: &Database,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    if db.write_policy() == crate::WritePolicy::Serial {
        let state = match db
            .serial_authorization_state()
            .await
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        {
            Some(state) => state.membership,
            None => {
                if db
                    .latest_local_store_position()
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?
                    .is_some()
                {
                    return Err(MembershipOpsError::Database(
                        "Serial membership state is absent after a Serial commit was materialized"
                            .to_string(),
                    ));
                }
                let root = super::store_objects::load_store_protocol_root(storage, &root_ref)
                    .await?
                    .value;
                let founder =
                    super::store_objects::load_founder_registration(storage, &root_ref).await?;
                let founder_ref =
                    super::store_commit::StoreDeviceRegistrationRef::from_registration(
                        &founder.value,
                        founder.object.clone(),
                    );
                super::membership::SerialAuthorizationState::from_founder(
                    &root_ref,
                    &root,
                    &founder_ref,
                    &founder.value,
                )
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?
                .membership
            }
        };
        let user_pubkey_hex = user_pubkey.map(hex::encode);
        return Ok(state
            .current_members()
            .into_iter()
            .map(|(pubkey, role)| MemberInfo {
                is_self: user_pubkey_hex.as_deref() == Some(&pubkey),
                pubkey,
                role,
            })
            .collect());
    }
    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)?;
    let chain = load_current_exact_chain(storage, &root_ref, Some(&pinned_owner), Some(db)).await?;
    require_resolved_membership(&chain)?;
    let user_pubkey_hex = user_pubkey.map(hex::encode);

    let current = chain.current_members();
    let members = current
        .into_iter()
        .map(|(pubkey, role)| {
            let is_self = user_pubkey_hex.as_deref() == Some(&pubkey);
            MemberInfo {
                pubkey,
                role,
                is_self,
            }
        })
        .collect();

    Ok(members)
}

/// Invite a member to the shared store.
///
/// Downloads the membership chain (bootstrapping a founder entry if needed),
/// creates a signed Add entry, wraps the encryption key to the invitee's
/// public key, and uploads everything to the sync storage.
///
/// Returns the JoinInfo for building an invite code.
pub fn invite_member<'a>(
    storage: &'a dyn SyncStorage,
    cloud_home: &'a dyn crate::storage::cloud::CloudHome,
    user_keypair: &'a UserKeypair,
    hlc: &'a Hlc,
    public_key_hex: &'a str,
    invitee_email: Option<&'a str>,
    role: MemberRole,
    encryption: &'a EncryptionService,
    store_id: &'a str,
    store_name: &'a str,
    db: &'a Database,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<crate::join_code::InviteCode, MembershipOpsError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(invite_member_with_coordination(
        storage,
        cloud_home,
        user_keypair,
        hlc,
        public_key_hex,
        invitee_email,
        role,
        encryption,
        store_id,
        store_name,
        db,
        None,
    ))
}

#[allow(clippy::too_many_arguments)]
pub async fn invite_member_with_coordination(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    store_name: &str,
    db: &Database,
    serial: Option<SerialMembershipContext<'_>>,
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    if role == MemberRole::Owner {
        return Err(MembershipOpsError::Invite(InviteError::Membership(
            super::membership::MembershipError::OwnerPromotionRequired,
        )));
    }
    let user_pubkey_hex = hex::encode(user_keypair.public_key());

    if public_key_hex == user_pubkey_hex {
        return Err(MembershipOpsError::SelfInvite);
    }

    if db.write_policy() == crate::WritePolicy::Serial {
        let serial =
            serial.ok_or(super::store_outbound::StoreOutboundError::MissingSerialCoordination)?;
        return invite_serial_member(
            storage,
            cloud_home,
            serial.coordination,
            &serial.device_id,
            user_keypair,
            hlc,
            public_key_hex,
            invitee_email,
            role,
            encryption,
            store_id,
            store_name,
            db,
        )
        .await;
    }

    // Download existing membership entries
    let root_ref = required_store_root_ref(db).await?;
    let store_root_hash = root_ref.store_root_hash;

    // The founder is written once, when a store is created and first connects
    // its cloud (issue #102) — never lazily here. An empty listing at invite time
    // means the chain is missing (a fresh store that never founded, or a wiped
    // `membership/*`); bootstrapping a new founder on the spot is the takeover
    // primitive #104 describes, so refuse instead.
    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)?;
    let mut chain = Box::pin(load_current_exact_chain(
        storage,
        &root_ref,
        Some(&pinned_owner),
        Some(db),
    ))
    .await?;
    require_resolved_membership(&chain)?;
    let protocol_store_id = root_ref.store_root_id.to_string();

    // Create the invitation
    let invite_ts = hlc.now().to_string();
    let (join_info, wrapped_key) =
        Box::pin(super::invite::create_invitation_with_encryption_durable(
            storage,
            cloud_home,
            store_root_hash,
            &mut chain,
            user_keypair,
            public_key_hex,
            invitee_email,
            role,
            encryption,
            &protocol_store_id,
            &invite_ts,
            db,
        ))
        .await?;

    info!(
        "Invited member {}...",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // The invite anchors the joiner to the store's OWNER — the chain founder,
    // not whichever Owner happens to send the invite. A second Owner inviting must
    // still hand the joiner the founder's pubkey, or the joiner would pin the wrong
    // owner and reject the (correctly founder-anchored) chain forever (issue #102).
    let owner_pubkey = chain
        .founder_pubkey()
        .ok_or(MembershipOpsError::ChainHasNoFounder)?
        .to_string();

    // The chain as it stands after this invite is committed (including the
    // invitee's own Add and the head just published for it): its per-author-stream
    // heads are the floor the joiner seeds its watermark from, so a provider
    // can never roll the joiner back to a state before this invite.
    let membership_floor = chain.head_refs().to_vec();
    let store_protocol_root =
        super::store_objects::load_store_protocol_root(storage, &root_ref).await?;
    if store_protocol_root.value.descriptor.store_root_id() != root_ref.store_root_id
        || store_protocol_root.value.descriptor.founder_pubkey != owner_pubkey
    {
        return Err(MembershipOpsError::Chain(AnchoredChainError::LoadFailed(
            "Store protocol root differs from the invite authority".to_string(),
        )));
    }

    // Build the invite code
    Ok(crate::join_code::InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: store_name.to_string(),
        join_info,
        owner_pubkey,
        wrapped_key,
        store_root: root_ref,
        membership_floor: crate::join_code::MembershipFloor::MergeConcurrent(membership_floor),
    })
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SerialInvitePlan {
    prepared: super::store_outbound::PreparedStoreOperationCommit,
    invitee_pubkey: String,
    invitee_email: Option<String>,
    role: MemberRole,
    desired_access: CloudAccessState,
    invitee_was_member: bool,
    wrapped_key: super::wrapped_store_key::PreparedWrappedStoreKey,
    store_id: String,
    store_name: String,
    store_root: StoreRootRef,
    owner_pubkey: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum SerialInviteProgress {
    Pending,
    AccessGranted {
        join_info: crate::storage::cloud::CloudHomeJoinInfo,
    },
    CandidateNonactivating {
        join_info: crate::storage::cloud::CloudHomeJoinInfo,
    },
    Activated {
        join_info: crate::storage::cloud::CloudHomeJoinInfo,
        candidate: StoreBatchCommitRef,
    },
}

fn serial_invite_result(
    plan: &SerialInvitePlan,
    join_info: crate::storage::cloud::CloudHomeJoinInfo,
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    if plan.store_root.store_root_hash != plan.prepared.commit.store_root_hash {
        return Err(MembershipOpsError::Database(
            "Serial invite commit names a different Store root".to_string(),
        ));
    }
    Ok(crate::join_code::InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: plan.store_id.clone(),
        store_name: plan.store_name.clone(),
        join_info,
        owner_pubkey: plan.owner_pubkey.clone(),
        wrapped_key: plan.wrapped_key.reference.clone(),
        store_root: plan.store_root.clone(),
        membership_floor: crate::join_code::MembershipFloor::Serial(Some(
            plan.prepared.reference.clone(),
        )),
    })
}

fn serial_invite_receipt_is_current(
    plan: &SerialInvitePlan,
    authorization: &super::membership::SerialAuthorizationState,
) -> Result<bool, MembershipOpsError> {
    plan.prepared.validate_closed_shape().map_err(|error| {
        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
            "terminal Serial invitation candidate is invalid: {error}"
        )))
    })?;
    let Some(StoreControl::SerialMembership { entry }) = plan.prepared.commit.control() else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial invitation does not carry a Serial membership grant".to_string(),
            ),
        ));
    };
    let super::membership::SerialMembershipChange::SetMember {
        user_pubkey,
        provider_account_email,
        role,
        grant_id,
        wrapped_key,
        ..
    } = &entry.change
    else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial invitation carries a removal".to_string(),
            ),
        ));
    };
    let role_matches = matches!(
        (&plan.role, role),
        (
            MemberRole::Member,
            super::membership::StoreMembershipRoleGrant::Member
        ) | (
            MemberRole::Follower,
            super::membership::StoreMembershipRoleGrant::Follower
        )
    );
    if user_pubkey != &plan.invitee_pubkey
        || provider_account_email != &plan.invitee_email
        || !role_matches
        || wrapped_key != &plan.wrapped_key.reference
    {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial invitation plan differs from its membership grant".to_string(),
            ),
        ));
    }
    let expected_grants = BTreeSet::from([grant_id.clone()]);
    let expected_wraps = vec![wrapped_key.clone()];
    Ok(authorization.key_generation == wrapped_key.generation
        && authorization
            .membership
            .active_grant_ids(&plan.invitee_pubkey)
            == expected_grants
        && authorization.active_wrapped_keys_for(&plan.invitee_pubkey) == expected_wraps
        && authorization
            .membership
            .current_members()
            .iter()
            .any(|(pubkey, role)| pubkey == &plan.invitee_pubkey && role == &plan.role)
        && authorization
            .membership
            .current_member_provider_email(&plan.invitee_pubkey)
            == plan.invitee_email.as_deref())
}

async fn restore_serial_invite_provider_access(
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialInvitePlan,
) -> Result<(), MembershipOpsError> {
    if !plan.invitee_was_member {
        let outcome = cloud_home
            .set_access(CloudAccessState::Absent {
                member_pubkey: plan.invitee_pubkey.clone(),
                provider_account_email: plan.invitee_email.clone(),
            })
            .await
            .map_err(InviteError::from)?;
        if !matches!(outcome, CloudAccessOutcome::Absent(_)) {
            return Err(MembershipOpsError::Invite(
                InviteError::InvalidDurableMutation(
                    "provider returned present while rolling back a Serial invitation".to_string(),
                ),
            ));
        }
    }
    Ok(())
}

async fn finish_nonactivating_serial_invite(
    db: &Database,
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialInvitePlan,
    intent_hash: super::store_commit::ObjectHash,
) -> Result<(), MembershipOpsError> {
    restore_serial_invite_provider_access(cloud_home, plan).await?;
    super::store_objects::delete_exact_object(storage, &plan.prepared.reference.object).await?;
    db.mark_candidate_cleanup_absent(plan.prepared.reference.object.clone())
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    db.complete_nonactivating_membership_candidate_mutation(
        intent_hash,
        plan.prepared.reference.clone(),
        vec![plan.prepared.reference.object.clone()],
        vec![plan.wrapped_key.reference.object.clone()],
        None,
    )
    .await
    .map_err(|error| MembershipOpsError::Database(error.to_string()))
}

pub(crate) async fn publish_serial_membership_wraps(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    candidate: &super::store_outbound::PreparedStoreOperationCommit,
    wraps: &[super::wrapped_store_key::PreparedWrappedStoreKey],
) -> Result<(), MembershipOpsError> {
    let remote_objects = candidate.membership_control_remote_objects(wraps)?;
    for prepared in wraps {
        prepared.validate()?;
        storage.create_protocol_object(&prepared.object).await?;
        super::wrapped_store_key::load_wrapped_store_key(
            storage,
            root.store_root_hash,
            &prepared.reference,
        )
        .await?;
        let expected = remote_objects
            .iter()
            .find(|remote| remote.object() == &prepared.reference.object)
            .cloned()
            .ok_or_else(|| {
                MembershipOpsError::Database(
                    "Serial membership wrap is absent from its durable ownership graph".to_string(),
                )
            })?;
        db.mark_reusable_retained_authority_uploaded(expected)
            .await
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    }
    super::store_pull::validate_serial_control_wrapped_keys(
        storage,
        root,
        candidate.commit.control(),
    )
    .await
    .map_err(|error| MembershipOpsError::Database(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
async fn invite_serial_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    store_name: &str,
    db: &Database,
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    let protocol_store_id = root_ref.store_root_id.to_string();
    let _mutation = db.lock_membership_mutation().await;
    if let Some(receipt) = db
        .terminal_serial_invite_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        let plan: SerialInvitePlan =
            serde_json::from_slice(&receipt.plan_bytes).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "parse terminal Serial invitation plan: {error}"
                )))
            })?;
        if plan.invitee_pubkey == public_key_hex
            && plan.invitee_email.as_deref() == invitee_email
            && plan.role == role
            && plan.store_id == store_id
            && plan.store_name == store_name
        {
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            if serial_invite_receipt_is_current(&plan, &authorization)? {
                return serde_json::from_slice(&receipt.result_bytes).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse terminal Serial invitation result: {error}"
                    )))
                });
            }
        }
    }
    let (plan, mut progress, intent_hash) = match db
        .outbound_membership_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        Some(row) => {
            let plan: SerialInvitePlan =
                serde_json::from_slice(&row.plan_bytes).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse Serial invitation plan: {error}"
                    )))
                })?;
            let progress = serde_json::from_slice(&row.progress_bytes).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "parse Serial invitation progress: {error}"
                )))
            })?;
            if plan.invitee_pubkey != public_key_hex
                || plan.invitee_email.as_deref() != invitee_email
                || plan.role != role
            {
                return Err(MembershipOpsError::Invite(InviteError::PendingMutation(
                    "the pending Serial invitation has different immutable inputs".to_string(),
                )));
            }
            (plan, progress, row.intent_hash)
        }
        None => {
            let root = super::store_objects::load_store_protocol_root(storage, &root_ref)
                .await?
                .value;
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            let invitee_was_member = authorization
                .membership
                .current_members()
                .iter()
                .any(|(pubkey, _)| pubkey == public_key_hex);
            let authority_refs =
                authorization.active_wrapped_keys_for(&crate::keys::public_key_hex(user_keypair));
            let authorized_keyring = super::invite::load_authorized_owner_keyring(
                storage,
                root_ref.store_root_hash,
                user_keypair,
                &protocol_store_id,
                &authority_refs,
                encryption,
            )
            .await?;
            if authorized_keyring.current_generation() != authorization.key_generation {
                return Err(MembershipOpsError::Invite(
                    InviteError::InvalidDurableMutation(format!(
                        "authorized key generation {} differs from committed Serial generation {}",
                        authorized_keyring.current_generation(),
                        authorization.key_generation
                    )),
                ));
            }
            let wrapped_key = super::wrapped_store_key::prepare_wrapped_store_key(
                storage,
                root_ref.store_root_hash,
                public_key_hex,
                super::invite::signed_serial_wrapped_key(
                    &protocol_store_id,
                    public_key_hex,
                    &authorized_keyring,
                    user_keypair,
                )?,
            )
            .await?;
            let entry = authorization
                .membership
                .signed_set_member_with_wrapped_key(
                    user_keypair,
                    public_key_hex.to_string(),
                    invitee_email.map(str::to_string),
                    role.clone(),
                    wrapped_key.reference.clone(),
                    hlc.now().to_string(),
                )
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                        error.to_string(),
                    ))
                })?;
            let operation = super::store_outbound::prepare_store_operation_commit(
                db,
                storage,
                Some(coordination),
                device_id,
                user_keypair,
                None,
            )
            .await?;
            let prepared = super::store_outbound::prepare_store_operation_candidate(
                db,
                storage,
                operation,
                super::store_outbound::StoreOperationBatch::Control(
                    StoreControl::SerialMembership { entry },
                ),
            )
            .await?;
            let plan = SerialInvitePlan {
                prepared,
                invitee_pubkey: public_key_hex.to_string(),
                invitee_email: invitee_email.map(str::to_string),
                role,
                desired_access: CloudAccessState::Present {
                    member_pubkey: public_key_hex.to_string(),
                    provider_account_email: invitee_email.map(str::to_string),
                },
                invitee_was_member,
                wrapped_key,
                store_id: store_id.to_string(),
                store_name: store_name.to_string(),
                store_root: root_ref.clone(),
                owner_pubkey: root.descriptor.founder_pubkey,
            };
            let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial invitation plan: {error}"
                )))
            })?;
            let progress = SerialInviteProgress::Pending;
            let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial invitation progress: {error}"
                )))
            })?;
            let remote_objects = plan
                .prepared
                .membership_control_remote_objects(std::slice::from_ref(&plan.wrapped_key))?;
            let intent_hash = db
                .stage_serial_invite_candidate_mutation(plan_bytes, progress_bytes, remote_objects)
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            (plan, progress, intent_hash)
        }
    };
    if matches!(
        &progress,
        SerialInviteProgress::CandidateNonactivating { .. }
    ) {
        finish_nonactivating_serial_invite(db, storage, cloud_home, &plan, intent_hash).await?;
        return Err(super::store_outbound::StoreOutboundError::InvalidOutbound(
            "Serial invitation candidate did not activate".to_string(),
        )
        .into());
    }
    let outcome = cloud_home
        .set_access(plan.desired_access.clone())
        .await
        .map_err(InviteError::from)?;
    let CloudAccessOutcome::Present(observed_join_info) = outcome else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "provider returned absent for a Serial invitation".to_string(),
            ),
        ));
    };
    let join_info =
        match &progress {
            SerialInviteProgress::Pending => {
                progress = SerialInviteProgress::AccessGranted {
                    join_info: observed_join_info.clone(),
                };
                let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize Serial invitation progress: {error}"
                    )))
                })?;
                db.update_membership_mutation_progress(intent_hash, progress_bytes)
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                observed_join_info
            }
            SerialInviteProgress::AccessGranted { join_info } => {
                if *join_info != observed_join_info {
                    return Err(MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                    "provider returned different join information for persisted Serial access"
                        .to_string(),
                )));
                }
                join_info.clone()
            }
            SerialInviteProgress::CandidateNonactivating { .. } => {
                unreachable!("nonactivating invitation returned before access grant")
            }
            SerialInviteProgress::Activated { join_info, .. } => join_info.clone(),
        };
    if let SerialInviteProgress::Activated {
        join_info,
        candidate,
    } = &progress
    {
        if candidate != &plan.prepared.reference {
            return Err(MembershipOpsError::Database(
                "activated Serial invitation names another candidate".to_string(),
            ));
        }
        let result = serial_invite_result(&plan, join_info.clone())?;
        let result_bytes = serde_json::to_vec(&result).map_err(|error| {
            MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                "serialize terminal Serial invitation result: {error}"
            )))
        })?;
        db.complete_serial_invite_mutation(intent_hash, result_bytes)
            .await
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
        #[cfg(any(test, feature = "test-utils"))]
        db.reach_test_point(crate::database::DatabaseTestPoint::SerialMembershipTerminalized)
            .await;
        return Ok(result);
    }
    publish_serial_membership_wraps(
        db,
        storage,
        &root_ref,
        &plan.prepared,
        std::slice::from_ref(&plan.wrapped_key),
    )
    .await?;
    let mut plan = plan;
    let mut intent_hash = intent_hash;
    loop {
        let activated_progress = SerialInviteProgress::Activated {
            join_info: join_info.clone(),
            candidate: plan.prepared.reference.clone(),
        };
        let remote_objects = plan
            .prepared
            .membership_control_remote_objects(std::slice::from_ref(&plan.wrapped_key))?;
        match super::store_outbound::publish_prepared_serial_membership_operation(
            db,
            storage,
            coordination,
            Box::new(plan.prepared.clone()),
            super::store_outbound::StoreMembershipJournalCompletion::Mutation {
                intent_hash,
                progress_bytes: serde_json::to_vec(&activated_progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize activated Serial invitation: {error}"
                    )))
                })?,
                remote_objects,
            },
        )
        .await?
        {
            super::store_outbound::StoreOperationPublicationOutcome::Activated(_) => break,
            super::store_outbound::StoreOperationPublicationOutcome::RepreparedCandidate(
                candidate,
            ) => {
                plan.prepared = *candidate;
                let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize reprepared Serial invitation: {error}"
                    )))
                })?;
                intent_hash = db
                    .replace_membership_candidate(intent_hash, plan_bytes)
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            }
            super::store_outbound::StoreOperationPublicationOutcome::NonactivatedCandidate {
                candidate,
                nonactivation,
            } => {
                if *candidate != plan.prepared {
                    return Err(MembershipOpsError::Database(
                        "nonactivation returned a different Serial invitation candidate"
                            .to_string(),
                    ));
                }
                progress = SerialInviteProgress::CandidateNonactivating {
                    join_info: join_info.clone(),
                };
                let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize nonactivating Serial invitation: {error}"
                    )))
                })?;
                db.begin_membership_candidate_nonactivation(
                    intent_hash,
                    plan.prepared.reference.clone(),
                    vec![plan.prepared.reference.object.clone()],
                    vec![plan.wrapped_key.reference.object.clone()],
                    progress_bytes,
                    *nonactivation,
                )
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                finish_nonactivating_serial_invite(db, storage, cloud_home, &plan, intent_hash)
                    .await?;
                return Err(super::store_outbound::StoreOutboundError::InvalidOutbound(
                    "Serial invitation candidate did not activate".to_string(),
                )
                .into());
            }
            super::store_outbound::StoreOperationPublicationOutcome::Nonactivated(reference) => {
                return Err(
                    super::store_outbound::StoreOutboundError::InvalidOutbound(format!(
                        "Serial invitation candidate {} did not activate",
                        reference.commit_hash
                    ))
                    .into(),
                );
            }
            super::store_outbound::StoreOperationPublicationOutcome::Reprepared => {
                return Err(MembershipOpsError::Database(
                    "Serial invitation returned acknowledgement-only reprepare state".to_string(),
                ));
            }
        }
    }
    let result = serial_invite_result(&plan, join_info)?;
    let result_bytes = serde_json::to_vec(&result).map_err(|error| {
        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
            "serialize terminal Serial invitation result: {error}"
        )))
    })?;
    db.complete_serial_invite_mutation(intent_hash, result_bytes)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    #[cfg(any(test, feature = "test-utils"))]
    db.reach_test_point(crate::database::DatabaseTestPoint::SerialMembershipTerminalized)
        .await;
    Ok(result)
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SerialRemovalPlan {
    prepared: super::store_outbound::PreparedStoreOperationCommit,
    revokee_pubkey: String,
    revokee_email: Option<String>,
    wraps: Vec<super::wrapped_store_key::PreparedWrappedStoreKey>,
    keyring_payload: Vec<u8>,
    generation: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum SerialRemovalProgress {
    Pending,
    AccessRevoked,
    CandidateNonactivating,
    Activated { candidate: StoreBatchCommitRef },
}

struct ActivatedSerialRemoval {
    encryption: EncryptionService,
    intent_hash: super::store_commit::ObjectHash,
    generation: u64,
}

enum SerialRemovalResult {
    Activated(ActivatedSerialRemoval),
    Terminal(String),
}

fn serial_removal_receipt_is_current(
    plan: &SerialRemovalPlan,
    authorization: &super::membership::SerialAuthorizationState,
) -> Result<bool, MembershipOpsError> {
    plan.prepared.validate_closed_shape().map_err(|error| {
        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
            "terminal Serial removal candidate is invalid: {error}"
        )))
    })?;
    let Some(StoreControl::SerialMembershipAndKeyRotation {
        entry, generation, ..
    }) = plan.prepared.commit.control()
    else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial removal does not carry a membership key rotation".to_string(),
            ),
        ));
    };
    let super::membership::SerialMembershipChange::RemoveMember { user_pubkey, .. } = &entry.change
    else {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial removal carries a membership grant".to_string(),
            ),
        ));
    };
    if user_pubkey != &plan.revokee_pubkey || generation != &plan.generation {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "terminal Serial removal plan differs from its membership rotation".to_string(),
            ),
        ));
    }
    Ok(authorization.key_generation == plan.generation
        && authorization
            .membership
            .active_grant_ids(&plan.revokee_pubkey)
            .is_empty()
        && authorization
            .active_wrapped_keys_for(&plan.revokee_pubkey)
            .is_empty())
}

async fn restore_serial_removal_provider_access(
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialRemovalPlan,
) -> Result<(), MembershipOpsError> {
    let outcome = cloud_home
        .set_access(CloudAccessState::Present {
            member_pubkey: plan.revokee_pubkey.clone(),
            provider_account_email: plan.revokee_email.clone(),
        })
        .await
        .map_err(InviteError::from)?;
    if !matches!(outcome, CloudAccessOutcome::Present(_)) {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "provider returned absent while rolling back a Serial removal".to_string(),
            ),
        ));
    }
    Ok(())
}

async fn finish_nonactivating_serial_removal(
    db: &Database,
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    plan: &SerialRemovalPlan,
    intent_hash: super::store_commit::ObjectHash,
    pending_rotation: &PendingRotation,
) -> Result<(), MembershipOpsError> {
    restore_serial_removal_provider_access(cloud_home, plan).await?;
    super::store_objects::delete_exact_object(storage, &plan.prepared.reference.object).await?;
    db.mark_candidate_cleanup_absent(plan.prepared.reference.object.clone())
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    db.complete_nonactivating_membership_candidate_mutation(
        intent_hash,
        plan.prepared.reference.clone(),
        vec![plan.prepared.reference.object.clone()],
        plan.wraps
            .iter()
            .map(|wrap| wrap.reference.object.clone())
            .collect(),
        Some(plan.generation),
    )
    .await
    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    pending_rotation
        .remove_candidate(plan.generation, intent_hash)
        .map_err(|error| MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error)))
}

#[allow(clippy::too_many_arguments)]
async fn remove_serial_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    store_id: &str,
    current_encryption: &EncryptionService,
    new_key: [u8; 32],
    pending_rotation: &PendingRotation,
    db: &Database,
) -> Result<SerialRemovalResult, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    let _mutation = db.lock_membership_mutation().await;
    if let Some(receipt) = db
        .terminal_serial_removal_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        let plan: SerialRemovalPlan =
            serde_json::from_slice(&receipt.plan_bytes).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "parse terminal Serial removal plan: {error}"
                )))
            })?;
        if plan.revokee_pubkey == public_key_hex {
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            if serial_removal_receipt_is_current(&plan, &authorization)? {
                let fingerprint =
                    serde_json::from_slice(&receipt.result_bytes).map_err(|error| {
                        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                            "parse terminal Serial removal result: {error}"
                        )))
                    })?;
                return Ok(SerialRemovalResult::Terminal(fingerprint));
            }
        }
    }
    let (plan, mut progress, intent_hash) = match db
        .outbound_membership_mutation()
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
    {
        Some(row) => {
            let plan: SerialRemovalPlan =
                serde_json::from_slice(&row.plan_bytes).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse Serial removal plan: {error}"
                    )))
                })?;
            let progress: SerialRemovalProgress = serde_json::from_slice(&row.progress_bytes)
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "parse Serial removal progress: {error}"
                    )))
                })?;
            if plan.revokee_pubkey != public_key_hex {
                return Err(MembershipOpsError::Invite(InviteError::PendingMutation(
                    "the pending Serial removal names another member".to_string(),
                )));
            }
            (plan, progress, row.intent_hash)
        }
        None => {
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            let authority_refs =
                authorization.active_wrapped_keys_for(&crate::keys::public_key_hex(user_keypair));
            let current_keyring = super::invite::load_authorized_owner_keyring(
                storage,
                root_ref.store_root_hash,
                user_keypair,
                store_id,
                &authority_refs,
                current_encryption,
            )
            .await?;
            if current_keyring.current_generation() != authorization.key_generation {
                return Err(MembershipOpsError::Invite(
                    InviteError::InvalidDurableMutation(format!(
                        "authorized key generation {} differs from committed Serial generation {}",
                        current_keyring.current_generation(),
                        authorization.key_generation
                    )),
                ));
            }
            let revokee_email = authorization
                .membership
                .current_member_provider_email(public_key_hex)
                .map(str::to_string);
            let entry = authorization
                .membership
                .signed_remove_member(
                    user_keypair,
                    public_key_hex.to_string(),
                    hlc.now().to_string(),
                )
                .map_err(|error| match error {
                    super::membership::SerialMembershipError::NotAMember(pubkey) => {
                        MembershipOpsError::Invite(InviteError::NotAMember(pubkey))
                    }
                    super::membership::SerialMembershipError::LastOwner => {
                        MembershipOpsError::Invite(InviteError::LastOwner)
                    }
                    error => MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                        error.to_string(),
                    )),
                })?;
            let generation = authorization.key_generation.checked_add(1).ok_or_else(|| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                    "Serial key generation overflow".to_string(),
                ))
            })?;
            let new_keyring = current_keyring
                .with_appended_generation(generation, new_key)
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::Crypto(format!(
                        "append Serial key generation: {error}"
                    )))
                })?;
            let membership_after = authorization.membership.apply(&entry).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error.to_string()))
            })?;
            let mut wraps = Vec::new();
            for (recipient, _) in membership_after.current_members() {
                wraps.push(
                    super::wrapped_store_key::prepare_wrapped_store_key(
                        storage,
                        root_ref.store_root_hash,
                        &recipient,
                        super::invite::signed_serial_wrapped_key(
                            store_id,
                            &recipient,
                            &new_keyring,
                            user_keypair,
                        )?,
                    )
                    .await?,
                );
            }
            wraps.sort_by(|left, right| left.reference.cmp(&right.reference));
            let wrapped_keys = wraps.iter().map(|wrap| wrap.reference.clone()).collect();
            let operation = super::store_outbound::prepare_store_operation_commit(
                db,
                storage,
                Some(coordination),
                device_id,
                user_keypair,
                None,
            )
            .await?;
            let prepared = super::store_outbound::prepare_store_operation_candidate(
                db,
                storage,
                operation,
                super::store_outbound::StoreOperationBatch::Control(
                    StoreControl::SerialMembershipAndKeyRotation {
                        entry,
                        generation,
                        wrapped_keys,
                    },
                ),
            )
            .await?;
            let keyring_payload = new_keyring.to_keyring_payload().map_err(|error| {
                MembershipOpsError::Invite(InviteError::Crypto(format!(
                    "serialize Serial rotated keyring: {error}"
                )))
            })?;
            let plan = SerialRemovalPlan {
                prepared,
                revokee_pubkey: public_key_hex.to_string(),
                revokee_email,
                wraps,
                keyring_payload,
                generation,
            };
            let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial removal plan: {error}"
                )))
            })?;
            let progress = SerialRemovalProgress::Pending;
            let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial removal progress: {error}"
                )))
            })?;
            let remote_objects = plan
                .prepared
                .membership_control_remote_objects(&plan.wraps)?;
            let intent_hash = db
                .stage_serial_removal_candidate_mutation(
                    plan_bytes,
                    progress_bytes,
                    remote_objects,
                    generation,
                )
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            (plan, progress, intent_hash)
        }
    };
    match &progress {
        SerialRemovalProgress::Activated { .. } => {
            pending_rotation.mark_committed_mutation(plan.generation, intent_hash)
        }
        _ => pending_rotation.mark_candidate(plan.generation, intent_hash),
    }
    .map_err(|error| MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error)))?;
    if matches!(&progress, SerialRemovalProgress::CandidateNonactivating) {
        finish_nonactivating_serial_removal(
            db,
            storage,
            cloud_home,
            &plan,
            intent_hash,
            pending_rotation,
        )
        .await?;
        return Err(super::store_outbound::StoreOutboundError::InvalidOutbound(
            "Serial removal candidate did not activate".to_string(),
        )
        .into());
    }
    if let SerialRemovalProgress::Activated { candidate } = &progress {
        if candidate != &plan.prepared.reference {
            return Err(MembershipOpsError::Database(
                "activated Serial removal names another candidate".to_string(),
            ));
        }
        let encryption =
            EncryptionService::from_keyring_payload(plan.keyring_payload).map_err(|error| {
                MembershipOpsError::Invite(InviteError::Crypto(format!(
                    "parse Serial rotated keyring: {error}"
                )))
            })?;
        return Ok(SerialRemovalResult::Activated(ActivatedSerialRemoval {
            encryption,
            intent_hash,
            generation: plan.generation,
        }));
    }
    let outcome = cloud_home
        .set_access(CloudAccessState::Absent {
            member_pubkey: plan.revokee_pubkey.clone(),
            provider_account_email: plan.revokee_email.clone(),
        })
        .await
        .map_err(InviteError::from)?;
    if !matches!(outcome, CloudAccessOutcome::Absent(_)) {
        return Err(MembershipOpsError::Invite(
            InviteError::InvalidDurableMutation(
                "provider returned present for a Serial removal".to_string(),
            ),
        ));
    }
    if matches!(&progress, SerialRemovalProgress::Pending) {
        progress = SerialRemovalProgress::AccessRevoked;
        db.update_membership_mutation_progress(
            intent_hash,
            serde_json::to_vec(&progress).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize revoked Serial removal access: {error}"
                )))
            })?,
        )
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    }
    publish_serial_membership_wraps(db, storage, &root_ref, &plan.prepared, &plan.wraps).await?;
    let mut plan = plan;
    let mut intent_hash = intent_hash;
    loop {
        let activated_progress = SerialRemovalProgress::Activated {
            candidate: plan.prepared.reference.clone(),
        };
        let remote_objects = plan
            .prepared
            .membership_control_remote_objects(&plan.wraps)?;
        match super::store_outbound::publish_prepared_serial_membership_operation(
            db,
            storage,
            coordination,
            Box::new(plan.prepared.clone()),
            super::store_outbound::StoreMembershipJournalCompletion::RotationMutation {
                intent_hash,
                progress_bytes: serde_json::to_vec(&activated_progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize activated Serial removal: {error}"
                    )))
                })?,
                generation: plan.generation,
                remote_objects,
            },
        )
        .await?
        {
            super::store_outbound::StoreOperationPublicationOutcome::Activated(_) => break,
            super::store_outbound::StoreOperationPublicationOutcome::RepreparedCandidate(
                candidate,
            ) => {
                plan.prepared = *candidate;
                let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize reprepared Serial removal: {error}"
                    )))
                })?;
                let previous_intent_hash = intent_hash;
                let replacement_hash = db
                    .replace_serial_removal_candidate(
                        previous_intent_hash,
                        plan.generation,
                        plan_bytes,
                    )
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                pending_rotation
                    .replace_candidate_mutation(
                        plan.generation,
                        previous_intent_hash,
                        replacement_hash,
                    )
                    .map_err(|error| {
                        MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error))
                    })?;
                intent_hash = replacement_hash;
            }
            super::store_outbound::StoreOperationPublicationOutcome::NonactivatedCandidate {
                candidate,
                nonactivation,
            } => {
                if *candidate != plan.prepared {
                    return Err(MembershipOpsError::Database(
                        "nonactivation returned a different Serial removal candidate".to_string(),
                    ));
                }
                progress = SerialRemovalProgress::CandidateNonactivating;
                let progress_bytes = serde_json::to_vec(&progress).map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                        "serialize nonactivating Serial removal: {error}"
                    )))
                })?;
                db.begin_membership_candidate_nonactivation(
                    intent_hash,
                    plan.prepared.reference.clone(),
                    vec![plan.prepared.reference.object.clone()],
                    plan.wraps
                        .iter()
                        .map(|wrap| wrap.reference.object.clone())
                        .collect(),
                    progress_bytes,
                    *nonactivation,
                )
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
                finish_nonactivating_serial_removal(
                    db,
                    storage,
                    cloud_home,
                    &plan,
                    intent_hash,
                    pending_rotation,
                )
                .await?;
                return Err(super::store_outbound::StoreOutboundError::InvalidOutbound(
                    "Serial removal candidate did not activate".to_string(),
                )
                .into());
            }
            super::store_outbound::StoreOperationPublicationOutcome::Nonactivated(reference) => {
                return Err(
                    super::store_outbound::StoreOutboundError::InvalidOutbound(format!(
                        "Serial removal candidate {} did not activate",
                        reference.commit_hash
                    ))
                    .into(),
                );
            }
            super::store_outbound::StoreOperationPublicationOutcome::Reprepared => {
                return Err(MembershipOpsError::Database(
                    "Serial removal returned acknowledgement-only reprepare state".to_string(),
                ));
            }
        }
    }
    pending_rotation
        .mark_committed_mutation(plan.generation, intent_hash)
        .map_err(|error| MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error)))?;
    let encryption =
        EncryptionService::from_keyring_payload(plan.keyring_payload).map_err(|error| {
            MembershipOpsError::Invite(InviteError::Crypto(format!(
                "parse Serial rotated keyring: {error}"
            )))
        })?;
    Ok(SerialRemovalResult::Activated(ActivatedSerialRemoval {
        encryption,
        intent_hash,
        generation: plan.generation,
    }))
}

/// Remove a member from the shared store and adopt the rotated key locally.
///
/// Downloads the membership chain, creates a signed Remove entry, rotates the
/// encryption key on the cloud (re-wrapping it for every remaining member), then
/// swaps this device's live cipher and persists the rotated key to its keyring.
/// Returns the rotated key's fingerprint for the host to record in its own config.
///
/// The cloud rotation commits before the local adoption. When adoption fails, the
/// removal is already durable, so the failure surfaces as
/// [`MembershipOpsError::RotationCommittedAdoptionFailed`] — distinct from a
/// rotation that never committed. The durable operation remains resumable by the
/// same call, while its rotation gate prevents every cloud seal until adoption
/// and exact journal completion both succeed.
pub async fn remove_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    store_id: &str,
    current_encryption: &EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    db: &Database,
) -> Result<String, MembershipOpsError> {
    remove_member_with_coordination(
        storage,
        cloud_home,
        user_keypair,
        hlc,
        public_key_hex,
        store_id,
        current_encryption,
        custody,
        cipher,
        pending_rotation,
        db,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn remove_member_with_coordination(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    _store_id: &str,
    current_encryption: &EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    db: &Database,
    serial: Option<SerialMembershipContext<'_>>,
) -> Result<String, MembershipOpsError> {
    let root_ref = required_store_root_ref(db).await?;
    let protocol_store_id = root_ref.store_root_id.to_string();
    if db.write_policy() == crate::WritePolicy::Serial {
        let serial =
            serial.ok_or(super::store_outbound::StoreOutboundError::MissingSerialCoordination)?;
        let removal = remove_serial_member(
            storage,
            cloud_home,
            serial.coordination,
            &serial.device_id,
            user_keypair,
            hlc,
            public_key_hex,
            &protocol_store_id,
            current_encryption,
            crate::encryption::generate_random_key(),
            pending_rotation,
            db,
        )
        .await?;
        let removal = match removal {
            SerialRemovalResult::Activated(removal) => removal,
            SerialRemovalResult::Terminal(fingerprint) => return Ok(fingerprint),
        };
        #[cfg(any(test, feature = "test-utils"))]
        db.reach_test_point(crate::database::DatabaseTestPoint::SerialRemovalBeforeAdoption)
            .await;
        let fingerprint = apply_key_rotation(removal.encryption, custody, cipher)
            .map_err(|source| MembershipOpsError::RotationCommittedAdoptionFailed { source })?;
        let result_bytes = serde_json::to_vec(&fingerprint).map_err(|error| {
            MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                "serialize terminal Serial removal result: {error}"
            )))
        })?;
        let gate = db
            .complete_serial_removal_mutation(removal.intent_hash, removal.generation, result_bytes)
            .await
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
        pending_rotation
            .install_durable_gate(gate)
            .map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(error))
            })?;
        #[cfg(any(test, feature = "test-utils"))]
        db.reach_test_point(crate::database::DatabaseTestPoint::SerialMembershipTerminalized)
            .await;
        return Ok(fingerprint);
    }
    // Download existing membership entries and build the chain.
    let store_root_hash = root_ref.store_root_hash;

    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoMembershipChain)?;
    let mut chain =
        load_current_exact_chain(storage, &root_ref, Some(&pinned_owner), Some(db)).await?;
    require_resolved_membership(&chain)?;

    // Revoke the member and rotate the cloud key. On return the rotation is
    // committed for every remaining member.
    let revoke_ts = hlc.now().to_string();
    let new_key = super::invite::revoke_member_durable(
        storage,
        cloud_home,
        store_root_hash,
        &mut chain,
        user_keypair,
        public_key_hex,
        &protocol_store_id,
        &revoke_ts,
        current_encryption,
        pending_rotation,
        db,
    )
    .await?;

    info!(
        "Revoked member {}... and rotated encryption key",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // Adopt the rotated key into this device's live cipher and custody. The cloud
    // rotation is already committed, so a failure here is not a generic membership
    // error but the specific half-applied state its own variant names.
    let generation = new_key.current_generation();
    let fingerprint = apply_key_rotation(new_key, custody, cipher)
        .map_err(|source| MembershipOpsError::RotationCommittedAdoptionFailed { source })?;
    super::invite::complete_revoke_rotation_adoption(db, pending_rotation, generation).await?;
    Ok(fingerprint)
}

/// Rotate the in-use encryption key after a member removal: persist it via
/// custody and swap the live cipher in place. Returns the new key's fingerprint
/// for the host to record in its own config — coven never writes the host's
/// config.
///
/// The key is persisted before the live cipher is swapped, so a persistence
/// failure leaves the cipher untouched on the superseded generation rather than
/// live on a key that never reached custody. Reapplying an already-held keyring
/// returns its fingerprint, allowing the durable caller to finish its exact
/// journal transition after a lost response or restart.
///
/// A plaintext home has no store key to rotate, so this is an error there:
/// sharing (and hence member removal) requires an encrypted home.
pub fn apply_key_rotation(
    new_encryption: EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
) -> Result<String, KeyError> {
    // Merge, never replace: the fixed-mode cipher state can extend an encrypted
    // keyring but has no transition to plaintext. Re-check under its keyring lock
    // because another member operation may have adopted a newer rotation while
    // this caller was reading the cloud.
    if let Some(fingerprint) = cipher.merge_key_rotation(&new_encryption, custody)? {
        return Ok(fingerprint);
    }
    let super::cloud_storage::CloudCipher::Encrypted(live) = cipher.snapshot() else {
        return Err(KeyError::Crypto(
            "cannot rotate the key of a plaintext cloud home".to_string(),
        ));
    };
    if live.merged_with(&new_encryption).key_count() != live.key_count() {
        return Err(KeyError::Crypto(
            "live keyring changed without retaining an adopted rotation".to_string(),
        ));
    }
    Ok(live.fingerprint())
}

/// Why [`load_anchored_chain`] refused a chain. Split so each caller renders the
/// right typed error: a chain that won't load/validate is a corrupt or malformed
/// membership set; a chain that loads but is founded by a different key is a
/// wiped-and-refounded takeover. The pull cycle maps both to its
/// `MembershipTampered`; the snapshot authorize maps both to `UnauthorizedAuthor`.
#[derive(Debug, thiserror::Error)]
pub enum AnchoredChainError {
    /// A cloud read required to decide the committed membership prefix was
    /// unavailable. Pull callers retain their position and retry this case.
    #[error("membership storage unavailable while {operation}: {source}")]
    StorageUnavailable {
        operation: String,
        #[source]
        source: StorageError,
    },
    /// The entries didn't download, parse, or validate into a well-formed chain.
    #[error("membership chain failed to load/validate: {0}")]
    LoadFailed(String),
    /// The chain is well-formed but its founder is not the store's pinned owner.
    #[error("chain founder {founder:?} is not the pinned owner {owner}")]
    FounderMismatch {
        founder: Option<String>,
        owner: String,
    },
}

/// The membership role a signed control author must currently hold.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MembershipAuthorRequirement {
    /// The author must be a current Owner.
    Owner,
    /// The author must be a current Owner or Member.
    WriteCapable,
}

impl MembershipAuthorRequirement {
    fn permits(self, chain: &MembershipChain, author_pubkey: &str) -> bool {
        match self {
            MembershipAuthorRequirement::Owner => chain.is_owner_now(author_pubkey),
            MembershipAuthorRequirement::WriteCapable => chain.can_write_now(author_pubkey),
        }
    }

    fn denial_message(self, author_pubkey: &str) -> String {
        match self {
            MembershipAuthorRequirement::Owner => {
                format!("author {author_pubkey} is not a current owner")
            }
            MembershipAuthorRequirement::WriteCapable => {
                format!("author {author_pubkey} is not a current write-capable member")
            }
        }
    }
}

/// Authorize an already-loaded membership chain. `None` exists for
/// pre-initialization bootstrap callers; an initialized sync session always has
/// an owner-anchored chain.
pub(crate) fn authorize_loaded_membership_author(
    chain: Option<&MembershipChain>,
    author_pubkey: &str,
    requirement: MembershipAuthorRequirement,
) -> Result<(), String> {
    let Some(chain) = chain else {
        debug!(
            author = %author_pubkey,
            required_role = ?requirement,
            "membership author authorization skipped before membership initialization"
        );
        return Ok(());
    };

    if !requirement.permits(chain, author_pubkey) {
        return Err(requirement.denial_message(author_pubkey));
    }

    Ok(())
}

struct ExactMembershipStream {
    entries: Vec<(MembershipCoord, MembershipEntry)>,
    heads: Vec<(MembershipHeadRef, AuthorHead)>,
    resolutions: BTreeMap<StoreMembershipConflictResolutionRef, StoreMembershipConflictResolution>,
}

fn membership_entry_requires_store_activation(entry: &MembershipEntry) -> bool {
    match &entry.change {
        MembershipChange::Founder { .. } | MembershipChange::ProviderAdmin => false,
        MembershipChange::SetMember {
            role,
            retirement_barriers,
            ..
        } => {
            matches!(
                role,
                super::membership::StoreMembershipRoleGrant::Owner { .. }
            ) || retirement_barriers.values().any(|barrier| {
                matches!(
                    barrier,
                    super::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                )
            })
        }
        MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } => retirement_barriers.values().any(|barrier| {
            matches!(
                barrier,
                super::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
            )
        }),
        MembershipChange::ResolutionActivation { .. } => true,
    }
}

async fn validate_membership_head_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &MembershipHeadRef,
    head: &AuthorHead,
    entry: &MembershipEntry,
    verified_activations: Option<&super::store_pull::VerifiedMergeMembershipPrefix>,
) -> Result<bool, AnchoredChainError> {
    match (
        membership_entry_requires_store_activation(entry),
        &head.activation,
    ) {
        (false, super::membership::MembershipHeadActivation::Direct) => Ok(true),
        (true, super::membership::MembershipHeadActivation::StoreCommit { commit }) => {
            if let Some(verified_activations) = verified_activations {
                let activation = verified_activations
                    .head_activation(commit)
                    .ok_or_else(|| {
                        AnchoredChainError::LoadFailed(
                            "membership head activation is absent from its verified Store prefix"
                                .to_string(),
                        )
                    })?;
                if !activation.verifies(reference, head, commit) {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership head differs from its verified Store activation".to_string(),
                    ));
                }
                return Ok(true);
            }
            Box::pin(super::store_pull::verify_merge_membership_head_activation(
                storage, root, reference, head, commit,
            ))
            .await
            .map_err(AnchoredChainError::LoadFailed)
        }
        (true, super::membership::MembershipHeadActivation::Direct) => {
            Err(AnchoredChainError::LoadFailed(
                "membership authority change has no exact Store activation".to_string(),
            ))
        }
        (false, super::membership::MembershipHeadActivation::StoreCommit { .. }) => {
            Err(AnchoredChainError::LoadFailed(
                "direct membership change carries an unrelated Store activation".to_string(),
            ))
        }
    }
}

async fn traverse_exact_membership_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    author: &str,
    grant: &MembershipGrantId,
    stream_id: super::membership::AuthorStreamId,
    anchor: &GrantStreamAnchor,
    cursor: Option<&MembershipHeadRef>,
) -> Result<ExactMembershipStream, AnchoredChainError> {
    let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
        return Err(AnchoredChainError::LoadFailed(
            "membership stream uses a recovery anchor".to_string(),
        ));
    };
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let mut slot = first_slot.clone();
    let mut expected_sequence = 1_u64;
    let mut predecessor: Option<MembershipHeadRef> = None;
    let mut entries = Vec::new();
    let mut heads = Vec::new();
    let mut resolutions = BTreeMap::new();
    let mut reached_cursor = cursor.is_none();

    loop {
        let semantic_prefix = slot.logical_key().strip_suffix(".json").ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "membership head exact slot has no .json suffix".to_string(),
            )
        })?;
        let (bytes, object) = match storage
            .read_protocol_slot(&context, &slot, semantic_prefix)
            .await
        {
            Ok(value) => value,
            Err(StorageError::NotFound(_)) => break,
            Err(source) if source.is_transport() => {
                return Err(AnchoredChainError::StorageUnavailable {
                    operation: format!(
                        "read membership head {author}/{grant}/{stream_id}/{expected_sequence}"
                    ),
                    source,
                })
            }
            Err(error) => return Err(AnchoredChainError::LoadFailed(error.to_string())),
        };
        let head: AuthorHead = serde_json::from_slice(&bytes)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        let coord = head.entry_coord();
        if coord.author_pubkey != author
            || coord.author_owner_grant != *grant
            || coord.stream_id != stream_id
            || coord.seq != expected_sequence
        {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head at sequence {expected_sequence} selects coordinate {coord:?}"
            )));
        }
        let reference = MembershipHeadRef {
            coord: coord.clone(),
            head_hash: head.head_hash(),
            object,
        };
        if head.body.predecessor != predecessor
            || head.body.successor.predecessor
                != predecessor
                    .as_ref()
                    .map(|reference| reference.object.clone())
        {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head {coord:?} does not extend its exact predecessor"
            )));
        }
        let registration = super::store_objects::load_registration_ref(
            storage,
            root,
            &head.body.author_registration,
        )
        .await
        .map_err(map_membership_object_error)?
        .value;
        if registration.author_pubkey != author
            || !head.verify(&registration)
            || head.body.successor.activation
                != super::store_commit::StreamActivation::grant_authorized(
                    root.store_root_hash,
                    head.body.author_registration.clone(),
                    grant.clone(),
                    anchor.clone(),
                )
                .activation_id()
        {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head {coord:?} is not signed by its activated certified device"
            )));
        }
        let loaded_entry = super::store_objects::load_membership_entry_ref(
            storage,
            root.store_root_hash,
            &head.body.entry,
        )
        .await
        .map_err(map_membership_object_error)?;
        if loaded_entry.value.resolution_dependencies != head.body.resolutions {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head {coord:?} carries a resolution cut different from its entry"
            )));
        }
        if !validate_membership_head_activation(
            storage,
            root,
            &reference,
            &head,
            &loaded_entry.value,
            None,
        )
        .await?
        {
            if cursor == Some(&reference) {
                return Err(AnchoredChainError::LoadFailed(
                    "membership cursor names an unactivated Store-bound head".to_string(),
                ));
            }
            break;
        }
        for resolution_ref in &head.body.resolutions {
            if !resolutions.contains_key(resolution_ref) {
                let resolution = super::store_objects::load_membership_resolution_ref(
                    storage,
                    root.store_root_hash,
                    resolution_ref,
                )
                .await
                .map_err(map_membership_object_error)?
                .value;
                resolutions.insert(resolution_ref.clone(), resolution);
            }
        }
        if cursor == Some(&reference) {
            reached_cursor = true;
        }
        entries.push((coord, loaded_entry.value));
        heads.push((reference.clone(), head.clone()));
        predecessor = Some(reference);
        slot = head.body.successor.next_slot.clone();
        expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
            AnchoredChainError::LoadFailed("membership head sequence overflow".to_string())
        })?;
    }
    if !reached_cursor {
        return Err(AnchoredChainError::LoadFailed(
            "membership head successor chain regressed below its durable cursor".to_string(),
        ));
    }
    Ok(ExactMembershipStream {
        entries,
        heads,
        resolutions,
    })
}

pub(crate) async fn load_exact_anchored_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    cursors: &[MembershipHeadRef],
    owner_pubkey: Option<&str>,
) -> Result<MembershipChain, AnchoredChainError> {
    let root_value = super::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    if let Some(owner) = owner_pubkey {
        if root_value.descriptor.founder_pubkey != owner {
            return Err(AnchoredChainError::FounderMismatch {
                founder: Some(root_value.descriptor.founder_pubkey),
                owner: owner.to_string(),
            });
        }
    }
    let super::store_commit::StoreMembershipGenesis::MergeConcurrent {
        founder_membership: anchor,
    } = &root_value.descriptor.membership
    else {
        return Err(AnchoredChainError::LoadFailed(
            "Serial Store has no Merge membership stream".to_string(),
        ));
    };
    let founder_stream = super::membership::derive_founder_stream_id(
        &root.store_root_id.to_string(),
        &root_value.descriptor.founder_pubkey,
    );
    let cursor = cursors.iter().find(|cursor| {
        cursor.coord.author_pubkey == root_value.descriptor.founder_pubkey
            && cursor.coord.author_owner_grant == root_value.descriptor.founder_grant
            && cursor.coord.stream_id == founder_stream
    });
    let founder_loaded = Box::pin(traverse_exact_membership_stream(
        storage,
        root,
        &root_value.descriptor.founder_pubkey,
        &root_value.descriptor.founder_grant,
        founder_stream,
        anchor,
        cursor,
    ))
    .await?;
    let founder_latest = founder_loaded.heads.last().cloned().ok_or_else(|| {
        AnchoredChainError::LoadFailed("founder membership head is absent".to_string())
    })?;
    let founder = founder_loaded
        .entries
        .first()
        .map(|(_, entry)| entry)
        .ok_or_else(|| {
            AnchoredChainError::LoadFailed("founder membership entry is absent".to_string())
        })?;
    if root_value
        .descriptor
        .validate_merge_founder_entry(founder)
        .is_err()
    {
        return Err(AnchoredChainError::LoadFailed(
            "first exact membership entry differs from the signed Store founder".to_string(),
        ));
    }
    let mut discovered = std::collections::BTreeSet::from([founder_latest.0.coord.stream_key()]);
    let mut consumed_cursors = std::collections::BTreeSet::new();
    if let Some(cursor) = cursor {
        consumed_cursors.insert(cursor.clone());
    }
    let mut latest_heads = vec![founder_latest];
    let mut resolutions = founder_loaded.resolutions;

    loop {
        let exact_heads = latest_heads
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        let resolution_refs = resolutions.keys().cloned().collect::<Vec<_>>();
        let chain = Box::pin(load_anchored_chain_at_exact_heads(
            storage,
            root,
            &root_value.descriptor.founder_pubkey,
            &exact_heads,
            &resolution_refs,
        ))
        .await?;
        let pending = chain
            .activated_membership_streams()
            .into_iter()
            .filter(|(stream, _)| !discovered.contains(stream))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            if consumed_cursors.len() != cursors.len() {
                return Err(AnchoredChainError::LoadFailed(
                    "membership cursor names a stream that is not activated by the anchored chain"
                        .to_string(),
                ));
            }
            return Ok(chain);
        }

        for (stream, anchor) in pending {
            let cursor = cursors
                .iter()
                .find(|cursor| cursor.coord.stream_key() == stream);
            let loaded = Box::pin(traverse_exact_membership_stream(
                storage,
                root,
                &stream.author_pubkey,
                &stream.author_owner_grant,
                stream.stream_id,
                &anchor,
                cursor,
            ))
            .await?;
            if let Some(cursor) = cursor {
                consumed_cursors.insert(cursor.clone());
            }
            resolutions.extend(loaded.resolutions);
            if let Some(latest) = loaded.heads.last().cloned() {
                latest_heads.push(latest);
                latest_heads.sort_by_key(|(reference, _)| reference.coord.stream_key());
            }
            discovered.insert(stream);
        }
    }
}

pub(crate) async fn load_exact_membership_head(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &MembershipHeadRef,
) -> Result<AuthorHead, AnchoredChainError> {
    let root_value = super::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    Box::pin(load_exact_membership_head_with_root(
        storage,
        root,
        &root_value,
        reference,
    ))
    .await
}

async fn load_exact_membership_head_with_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    reference: &MembershipHeadRef,
) -> Result<AuthorHead, AnchoredChainError> {
    let coord = &reference.coord;
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let semantic_prefix = reference
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "membership head exact slot has no .json suffix".to_string(),
            )
        })?;
    let bytes = storage
        .read_protocol_object(&context, &reference.object, semantic_prefix)
        .await
        .map_err(|source| AnchoredChainError::StorageUnavailable {
            operation: format!("read exact membership head {coord:?}"),
            source,
        })?;
    let head: AuthorHead = serde_json::from_slice(&bytes)
        .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    if head.entry_coord() != *coord || head.head_hash() != reference.head_hash {
        return Err(AnchoredChainError::LoadFailed(
            "membership head differs from its exact reference".to_string(),
        ));
    }
    let registration = Box::pin(super::store_objects::load_registration_ref_with_root(
        storage,
        root,
        root_value,
        &head.body.author_registration,
    ))
    .await
    .map_err(map_membership_object_error)?
    .value;
    if registration.author_pubkey != coord.author_pubkey || !head.verify(&registration) {
        return Err(AnchoredChainError::LoadFailed(
            "membership head is not signed by its exact certified device".to_string(),
        ));
    }
    Ok(head)
}

pub(crate) async fn load_anchored_chain_at_exact_heads(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
) -> Result<MembershipChain, AnchoredChainError> {
    let root_value = super::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    Box::pin(load_anchored_chain_at_exact_heads_with_root(
        storage,
        root,
        &root_value,
        owner_pubkey,
        exact_heads,
        exact_resolutions,
    ))
    .await
}

pub(crate) async fn load_anchored_chain_at_exact_heads_with_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
) -> Result<MembershipChain, AnchoredChainError> {
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        storage,
        root,
        root_value,
        owner_pubkey,
        exact_heads,
        exact_resolutions,
        None,
        None,
    ))
    .await
}

pub(crate) async fn load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
    verified_activations: &super::store_pull::VerifiedMergeMembershipPrefix,
    pending_resolution: Option<&super::store_pull::VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, AnchoredChainError> {
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        storage,
        root,
        root_value,
        owner_pubkey,
        exact_heads,
        exact_resolutions,
        Some(verified_activations),
        pending_resolution,
    ))
    .await
}

#[derive(Clone)]
struct LoadedExactMembershipHead {
    reference: MembershipHeadRef,
    head: AuthorHead,
    entry: MembershipEntry,
}

#[derive(Clone)]
struct LoadedExactMembershipGraph {
    entries: BTreeMap<MembershipCoord, MembershipEntry>,
    heads: Vec<(MembershipHeadRef, AuthorHead)>,
    path_heads: BTreeMap<MembershipCoord, LoadedExactMembershipHead>,
}

impl LoadedExactMembershipGraph {
    fn head_refs(&self) -> Vec<MembershipHeadRef> {
        self.heads
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect()
    }

    fn resolution_cut(&self) -> Vec<StoreMembershipConflictResolutionRef> {
        self.heads
            .iter()
            .flat_map(|(_, head)| head.body.resolutions.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

async fn load_exact_membership_head_node(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    reference: &MembershipHeadRef,
) -> Result<LoadedExactMembershipHead, AnchoredChainError> {
    let head = Box::pin(load_exact_membership_head_with_root(
        storage, root, root_value, reference,
    ))
    .await?;
    let loaded_entry = Box::pin(super::store_objects::load_membership_entry_ref(
        storage,
        root.store_root_hash,
        &head.body.entry,
    ))
    .await
    .map_err(map_membership_object_error)?;
    if loaded_entry.value.resolution_dependencies != head.body.resolutions {
        return Err(AnchoredChainError::LoadFailed(
            "membership head and selected entry carry different resolution cuts".to_string(),
        ));
    }
    Ok(LoadedExactMembershipHead {
        reference: reference.clone(),
        head,
        entry: loaded_entry.value,
    })
}

fn validate_exact_membership_head_paths(
    graph: &LoadedExactMembershipGraph,
) -> Result<(), AnchoredChainError> {
    for requested in &graph.heads {
        let mut current = Some(&requested.0);
        let mut visited = BTreeSet::new();
        while let Some(reference) = current {
            if !visited.insert(reference.clone()) {
                return Err(AnchoredChainError::LoadFailed(
                    "membership head predecessor chain contains a cycle".to_string(),
                ));
            }
            let node = graph.path_heads.get(&reference.coord).ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "membership head predecessor is absent from its exact path".to_string(),
                )
            })?;
            if node.reference != *reference {
                return Err(AnchoredChainError::LoadFailed(
                    "membership coordinate selects different exact heads".to_string(),
                ));
            }
            match &node.head.body.predecessor {
                Some(predecessor) => {
                    if predecessor.coord.stream_key() != reference.coord.stream_key()
                        || predecessor.coord.seq.checked_add(1) != Some(reference.coord.seq)
                        || predecessor.coord.entry_hash
                            != node.entry.previous_hash.ok_or_else(|| {
                                AnchoredChainError::LoadFailed(
                                    "membership head successor entry omits its predecessor hash"
                                        .to_string(),
                                )
                            })?
                    {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership head does not extend its exact author stream".to_string(),
                        ));
                    }
                    let predecessor_node =
                        graph.path_heads.get(&predecessor.coord).ok_or_else(|| {
                            AnchoredChainError::LoadFailed(
                                "membership head predecessor is absent from its exact path"
                                    .to_string(),
                            )
                        })?;
                    if predecessor_node.reference != *predecessor
                        || predecessor_node.head.body.successor.next_slot
                            != *reference.object.slot()
                    {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership head does not occupy its predecessor-reserved slot"
                                .to_string(),
                        ));
                    }
                }
                None => {
                    if reference.coord.seq != 1 || node.entry.previous_hash.is_some() {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership stream begins without its exact first entry".to_string(),
                        ));
                    }
                }
            }
            current = node.head.body.predecessor.as_ref();
        }
    }
    Ok(())
}

fn validate_exact_membership_stream_anchors(
    root: &StoreRootRef,
    graph: &LoadedExactMembershipGraph,
    chain: &MembershipChain,
) -> Result<(), AnchoredChainError> {
    for node in graph.path_heads.values() {
        let grant = &node.reference.coord.author_owner_grant;
        let anchor = chain.membership_anchor(grant).ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "membership head author has no exact membership stream anchor".to_string(),
            )
        })?;
        let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
            return Err(AnchoredChainError::LoadFailed(
                "membership head author uses a recovery stream anchor".to_string(),
            ));
        };
        if node.reference.coord.seq == 1 && node.reference.object.slot() != first_slot {
            return Err(AnchoredChainError::LoadFailed(
                "membership stream does not begin at its grant-authorized slot".to_string(),
            ));
        }
        let expected = super::store_commit::StreamActivation::grant_authorized(
            root.store_root_hash,
            node.head.body.author_registration.clone(),
            grant.clone(),
            anchor.clone(),
        )
        .activation_id();
        if node.head.body.successor.activation != expected {
            return Err(AnchoredChainError::LoadFailed(
                "membership head carries another grant stream activation".to_string(),
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MembershipProjectionStatus {
    Included,
    OutsidePrefix,
}

fn membership_resolution_activations(
    graph: &LoadedExactMembershipGraph,
) -> Result<BTreeMap<StoreMembershipConflictResolutionRef, MembershipCoord>, AnchoredChainError> {
    let mut activations = BTreeMap::new();
    for node in graph.path_heads.values() {
        if let MembershipChange::ResolutionActivation { resolution } = &node.entry.change {
            if activations
                .insert(resolution.clone(), node.reference.coord.clone())
                .is_some()
            {
                return Err(AnchoredChainError::LoadFailed(
                    "membership resolution has multiple candidate activation heads".to_string(),
                ));
            }
        }
    }
    Ok(activations)
}

fn membership_projection_activation_status(
    graph: &LoadedExactMembershipGraph,
    prefix: &super::store_pull::VerifiedMergeMembershipPrefix,
    coord: &MembershipCoord,
) -> Result<MembershipProjectionStatus, AnchoredChainError> {
    let node = graph.path_heads.get(coord).ok_or_else(|| {
        AnchoredChainError::LoadFailed(
            "membership projection dependency is absent from its candidate graph".to_string(),
        )
    })?;
    match (
        membership_entry_requires_store_activation(&node.entry),
        &node.head.activation,
    ) {
        (false, super::membership::MembershipHeadActivation::Direct) => {
            Ok(MembershipProjectionStatus::Included)
        }
        (true, super::membership::MembershipHeadActivation::StoreCommit { commit }) => prefix
            .classify_head(&node.reference, &node.head, commit)
            .map(|status| match status {
                super::store_pull::VerifiedMergePrefixHeadStatus::Included => {
                    MembershipProjectionStatus::Included
                }
                super::store_pull::VerifiedMergePrefixHeadStatus::OutsidePrefix => {
                    MembershipProjectionStatus::OutsidePrefix
                }
            })
            .map_err(AnchoredChainError::LoadFailed),
        (true, super::membership::MembershipHeadActivation::Direct) => {
            Err(AnchoredChainError::LoadFailed(
                "membership authority change has no exact Store activation".to_string(),
            ))
        }
        (false, super::membership::MembershipHeadActivation::StoreCommit { .. }) => {
            Err(AnchoredChainError::LoadFailed(
                "direct membership change carries an unrelated Store activation".to_string(),
            ))
        }
    }
}

fn membership_projection_dependencies(
    graph: &LoadedExactMembershipGraph,
    resolution_activations: &BTreeMap<StoreMembershipConflictResolutionRef, MembershipCoord>,
    coord: &MembershipCoord,
) -> Result<Vec<MembershipCoord>, AnchoredChainError> {
    let node = graph.path_heads.get(coord).ok_or_else(|| {
        AnchoredChainError::LoadFailed(
            "membership projection dependency is absent from its candidate graph".to_string(),
        )
    })?;
    let mut dependencies = node
        .head
        .body
        .predecessor
        .iter()
        .map(|reference| reference.coord.clone())
        .chain(node.entry.dependencies.iter().cloned())
        .collect::<Vec<_>>();
    for resolution in &node.entry.resolution_dependencies {
        let introduced_here = matches!(
            &node.entry.change,
            MembershipChange::ResolutionActivation { resolution: introduced }
                if introduced == resolution
        );
        if !introduced_here {
            dependencies.push(
                resolution_activations
                    .get(resolution)
                    .ok_or_else(|| {
                        AnchoredChainError::LoadFailed(
                            "membership resolution lacks its candidate activation head".to_string(),
                        )
                    })?
                    .clone(),
            );
        }
    }
    Ok(dependencies)
}

fn membership_projection_statuses(
    graph: &LoadedExactMembershipGraph,
    prefix: &super::store_pull::VerifiedMergeMembershipPrefix,
    resolution_activations: &BTreeMap<StoreMembershipConflictResolutionRef, MembershipCoord>,
) -> Result<BTreeMap<MembershipCoord, MembershipProjectionStatus>, AnchoredChainError> {
    let mut statuses = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    for root in graph.path_heads.keys() {
        if statuses.contains_key(root) {
            continue;
        }
        let mut stack = vec![(root.clone(), false)];
        while let Some((coord, expanded)) = stack.pop() {
            if statuses.contains_key(&coord) {
                continue;
            }
            if expanded {
                let node = graph.path_heads.get(&coord).ok_or_else(|| {
                    AnchoredChainError::LoadFailed(
                        "membership projection dependency is absent from its candidate graph"
                            .to_string(),
                    )
                })?;
                let dependencies =
                    membership_projection_dependencies(graph, resolution_activations, &coord)?;
                let status = if dependencies.iter().any(|dependency| {
                    statuses.get(dependency) == Some(&MembershipProjectionStatus::OutsidePrefix)
                }) {
                    MembershipProjectionStatus::OutsidePrefix
                } else {
                    for resolution in &node.entry.resolution_dependencies {
                        if !prefix.verifies_conflict_resolution(resolution) {
                            return Err(AnchoredChainError::LoadFailed(
                                "in-prefix membership resolution lacks its verified Store authority"
                                    .to_string(),
                            ));
                        }
                    }
                    MembershipProjectionStatus::Included
                };
                visiting.remove(&coord);
                statuses.insert(coord, status);
                continue;
            }
            if !visiting.insert(coord.clone()) {
                return Err(AnchoredChainError::LoadFailed(
                    "membership projection dependency graph contains a cycle".to_string(),
                ));
            }
            let activation_status = membership_projection_activation_status(graph, prefix, &coord)?;
            if activation_status == MembershipProjectionStatus::OutsidePrefix {
                visiting.remove(&coord);
                statuses.insert(coord, MembershipProjectionStatus::OutsidePrefix);
                continue;
            }
            let dependencies =
                membership_projection_dependencies(graph, resolution_activations, &coord)?;
            stack.push((coord, true));
            for dependency in dependencies.into_iter().rev() {
                if !statuses.contains_key(&dependency) {
                    stack.push((dependency, false));
                }
            }
        }
    }
    Ok(statuses)
}

fn project_membership_cut_to_store_prefix(
    graph: &LoadedExactMembershipGraph,
    prefix: &super::store_pull::VerifiedMergeMembershipPrefix,
) -> Result<
    (
        Vec<MembershipHeadRef>,
        Vec<StoreMembershipConflictResolutionRef>,
    ),
    AnchoredChainError,
> {
    let resolution_activations = membership_resolution_activations(graph)?;
    let statuses = membership_projection_statuses(graph, prefix, &resolution_activations)?;
    let mut projected = Vec::new();
    for (candidate, _) in &graph.heads {
        let mut current = Some(candidate);
        let selected = loop {
            let Some(reference) = current else {
                break None;
            };
            match statuses.get(&reference.coord).copied().ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "membership projection status is absent from its candidate graph".to_string(),
                )
            })? {
                MembershipProjectionStatus::Included => break Some(reference.clone()),
                MembershipProjectionStatus::OutsidePrefix => {
                    current = graph
                        .path_heads
                        .get(&reference.coord)
                        .ok_or_else(|| {
                            AnchoredChainError::LoadFailed(
                                "membership projection cursor is absent from its exact path"
                                    .to_string(),
                            )
                        })?
                        .head
                        .body
                        .predecessor
                        .as_ref();
                }
            }
        };
        if let Some(selected) = selected {
            projected.push(selected);
        }
    }
    projected.sort_by_key(|reference| reference.coord.stream_key());
    validate_membership_floor(&projected).map_err(AnchoredChainError::LoadFailed)?;
    let resolutions = projected
        .iter()
        .map(|reference| {
            graph.path_heads.get(&reference.coord).ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "projected membership head is absent from its exact candidate path".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flat_map(|node| node.head.body.resolutions.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok((projected, resolutions))
}

pub(crate) async fn project_anchored_chain_to_verified_store_prefix(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: &str,
    candidate_heads: &[MembershipHeadRef],
    prefix: &super::store_pull::VerifiedMergeMembershipPrefix,
) -> Result<MembershipChain, AnchoredChainError> {
    let root_value = super::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    let candidate =
        load_exact_membership_graph_objects(storage, root, &root_value, candidate_heads).await?;
    let (heads, resolutions) = project_membership_cut_to_store_prefix(&candidate, prefix)?;
    let projected = load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
        storage,
        root,
        &root_value,
        owner_pubkey,
        &heads,
        &resolutions,
        prefix,
        None,
    )
    .await?;
    prefix
        .validate_complete_membership(&projected)
        .map_err(AnchoredChainError::LoadFailed)?;
    Ok(projected)
}

async fn load_exact_membership_graph(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    exact_heads: &[MembershipHeadRef],
    verified_activations: Option<&super::store_pull::VerifiedMergeMembershipPrefix>,
) -> Result<LoadedExactMembershipGraph, AnchoredChainError> {
    let graph = load_exact_membership_graph_objects(storage, root, root_value, exact_heads).await?;
    for node in graph.path_heads.values() {
        if !Box::pin(validate_membership_head_activation(
            storage,
            root,
            &node.reference,
            &node.head,
            &node.entry,
            verified_activations,
        ))
        .await?
        {
            return Err(AnchoredChainError::LoadFailed(
                "exact membership state names an unactivated Store-bound head".to_string(),
            ));
        }
    }
    let entry_values = graph.entries.values().cloned().collect::<Vec<_>>();
    Box::pin(validate_provider_admin_records(
        storage,
        root,
        root_value,
        &entry_values,
    ))
    .await?;
    validate_owner_grant_records(root_value, &entry_values)?;
    Ok(graph)
}

async fn load_exact_membership_graph_objects(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    exact_heads: &[MembershipHeadRef],
) -> Result<LoadedExactMembershipGraph, AnchoredChainError> {
    let mut entries = BTreeMap::new();
    let mut heads = Vec::with_capacity(exact_heads.len());
    let mut path_heads = BTreeMap::<MembershipCoord, LoadedExactMembershipHead>::new();
    for requested in exact_heads {
        let mut current = Some(requested.clone());
        let mut requested_head = None;
        while let Some(reference) = current {
            let node = Box::pin(load_exact_membership_head_node(
                storage, root, root_value, &reference,
            ))
            .await?;
            if reference == *requested {
                requested_head = Some((reference.clone(), node.head.clone()));
            }
            match entries.entry(reference.coord.clone()) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(node.entry.clone());
                }
                std::collections::btree_map::Entry::Occupied(slot) => {
                    if slot.get() != &node.entry {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership coordinate selects different exact entries".to_string(),
                        ));
                    }
                }
            }
            match path_heads.entry(reference.coord.clone()) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(node.clone());
                }
                std::collections::btree_map::Entry::Occupied(slot) => {
                    if slot.get().reference != node.reference
                        || slot.get().head != node.head
                        || slot.get().entry != node.entry
                    {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership coordinate selects different exact head paths".to_string(),
                        ));
                    }
                }
            }
            current = node.head.body.predecessor.clone();
        }
        heads.push(requested_head.ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "requested exact membership head was not loaded".to_string(),
            )
        })?);
    }
    let graph = LoadedExactMembershipGraph {
        entries,
        heads,
        path_heads,
    };
    validate_exact_membership_head_paths(&graph)?;
    Ok(graph)
}

fn exact_membership_chain_from_graph(
    root: &StoreRootRef,
    graph: LoadedExactMembershipGraph,
    provider_admin: super::provider::ProviderAdminState,
) -> Result<MembershipChain, AnchoredChainError> {
    let chain = MembershipChain::from_entries_with_coords_and_heads_and_provider_admin(
        graph
            .entries
            .iter()
            .map(|(coord, entry)| (coord.clone(), entry.clone()))
            .collect(),
        graph.heads.clone(),
        provider_admin,
    )
    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    validate_exact_membership_stream_anchors(root, &graph, &chain)?;
    Ok(chain)
}

fn add_exact_membership_suffix(
    chain: &mut MembershipChain,
    graph: &LoadedExactMembershipGraph,
) -> Result<(), AnchoredChainError> {
    let mut pending = graph
        .entries
        .iter()
        .filter(|(coord, _)| !chain.contains_coord(coord))
        .map(|(coord, entry)| (coord.clone(), entry.clone()))
        .collect::<BTreeMap<_, _>>();
    while !pending.is_empty() {
        let next = pending.iter().find_map(|(coord, entry)| {
            let dependencies_loaded = entry
                .dependencies
                .iter()
                .all(|dependency| chain.contains_coord(dependency));
            let predecessor_loaded = entry.previous_hash.is_none()
                || graph.entries.keys().any(|candidate| {
                    candidate.author_pubkey == coord.author_pubkey
                        && candidate.author_owner_grant == coord.author_owner_grant
                        && candidate.stream_id == coord.stream_id
                        && candidate.seq.checked_add(1) == Some(coord.seq)
                        && Some(candidate.entry_hash) == entry.previous_hash
                        && chain.contains_coord(candidate)
                });
            (dependencies_loaded && predecessor_loaded).then(|| coord.clone())
        });
        let Some(coord) = next else {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution suffix has an unresolved causal predecessor".to_string(),
            ));
        };
        let entry = pending
            .remove(&coord)
            .expect("selected membership resolution suffix entry remains pending");
        chain
            .add_entry_at(coord, entry)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    }
    for (reference, _) in &graph.heads {
        chain
            .activate_head_ref(reference.clone())
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    }
    Ok(())
}

type LayeredMembershipFuture<'a> =
    Pin<Box<dyn Future<Output = Result<MembershipChain, AnchoredChainError>> + Send + 'a>>;

#[allow(clippy::too_many_arguments)]
fn load_layered_membership_chain<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    root_value: &'a super::store_commit::StoreProtocolRoot,
    graph: LoadedExactMembershipGraph,
    exact_resolutions: &'a [StoreMembershipConflictResolutionRef],
    provider_admin: &'a super::provider::ProviderAdminState,
    verified_activations: Option<&'a super::store_pull::VerifiedMergeMembershipPrefix>,
    pending_resolution: Option<&'a super::store_pull::VerifiedMergeConflictResolutionActivation>,
) -> LayeredMembershipFuture<'a> {
    Box::pin(async move {
        let exact_heads = graph.head_refs();
        if exact_resolutions.is_empty() {
            if !graph.resolution_cut().is_empty() {
                return Err(AnchoredChainError::LoadFailed(
                    "membership signed heads name a nonempty resolution cut".to_string(),
                ));
            }
            return exact_membership_chain_from_graph(root, graph, provider_admin.clone());
        }

        let mut resolutions = BTreeMap::new();
        for reference in exact_resolutions {
            let value = Box::pin(super::store_objects::load_membership_resolution_ref(
                storage,
                root.store_root_hash,
                reference,
            ))
            .await
            .map_err(map_membership_object_error)?
            .value;
            let verified_by_prefix = verified_activations
                .is_some_and(|prefix| prefix.verifies_conflict_resolution(reference));
            let verified_by_pending =
                pending_resolution.is_some_and(|pending| pending.verifies(reference));
            if verified_activations.is_some() && !verified_by_prefix && !verified_by_pending {
                return Err(AnchoredChainError::LoadFailed(
                    "membership conflict resolution is absent from its verified Store authority"
                        .to_string(),
                ));
            }
            if verified_activations.is_none() {
                Box::pin(super::store_pull::verify_merge_owner_conflict_acceptance(
                    storage,
                    root,
                    &value.replacement_acceptance,
                    &value.resolver_pubkey,
                ))
                .await
                .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
            }
            resolutions.insert(reference.clone(), value);
        }

        let target_cut = exact_resolutions.iter().cloned().collect::<BTreeSet<_>>();
        let mut activation_counts = BTreeMap::<_, usize>::new();
        for entry in graph.entries.values() {
            if let MembershipChange::ResolutionActivation { resolution } = &entry.change {
                if entry.resolution_dependencies == exact_resolutions {
                    *activation_counts.entry(resolution.clone()).or_default() += 1;
                }
            }
        }
        if let Some(pending) = pending_resolution {
            *activation_counts
                .entry(pending.reference().clone())
                .or_default() += 1;
        }
        if activation_counts.values().any(|count| *count != 1) {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution has multiple exact activations".to_string(),
            ));
        }
        let activated_here = activation_counts.keys().cloned().collect::<BTreeSet<_>>();
        if !activated_here.is_subset(&target_cut) || activated_here.is_empty() {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution cut has no exact activation layer".to_string(),
            ));
        }

        let first_resolution = resolutions
            .get(
                activated_here
                    .first()
                    .expect("activation layer is nonempty"),
            )
            .expect("activated resolution belongs to the exact cut");
        let conflict_heads = &first_resolution.conflicting_heads;
        if activated_here.iter().any(|reference| {
            resolutions.get(reference).is_none_or(|resolution| {
                resolution.conflict_hash != first_resolution.conflict_hash
                    || resolution.conflicting_heads != *conflict_heads
            })
        }) {
            return Err(AnchoredChainError::LoadFailed(
                "membership activation layer combines different conflicts".to_string(),
            ));
        }
        let conflict_graph = load_exact_membership_graph(
            storage,
            root,
            root_value,
            conflict_heads,
            verified_activations,
        )
        .await?;
        let prior_cut = conflict_graph.resolution_cut();
        let prior_set = prior_cut.iter().cloned().collect::<BTreeSet<_>>();
        let introduced = target_cut
            .difference(&prior_set)
            .cloned()
            .collect::<BTreeSet<_>>();
        if activated_here != introduced {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution activations differ from the introduced cut".to_string(),
            ));
        }
        let mut chain = load_layered_membership_chain(
            storage,
            root,
            root_value,
            conflict_graph,
            &prior_cut,
            provider_admin,
            verified_activations,
            None,
        )
        .await?;
        let introduced_resolutions = introduced
            .iter()
            .map(|reference| {
                resolutions
                    .get(reference)
                    .cloned()
                    .map(|value| (reference.clone(), value))
                    .ok_or_else(|| {
                        AnchoredChainError::LoadFailed(
                            "introduced membership resolution is absent from its exact cut"
                                .to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        chain
            .apply_resolutions(root.store_root_hash, &introduced_resolutions)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        add_exact_membership_suffix(&mut chain, &graph)?;
        validate_exact_membership_stream_anchors(root, &graph, &chain)?;
        if chain.head_refs() != exact_heads || chain.resolution_refs() != exact_resolutions {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution reconstruction differs from its exact state".to_string(),
            ));
        }
        Ok(chain)
    })
}

async fn load_anchored_chain_at_exact_heads_with_root_impl(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
    verified_activations: Option<&super::store_pull::VerifiedMergeMembershipPrefix>,
    pending_resolution: Option<&super::store_pull::VerifiedMergeConflictResolutionActivation>,
) -> Result<MembershipChain, AnchoredChainError> {
    validate_membership_floor(exact_heads).map_err(AnchoredChainError::LoadFailed)?;
    if !exact_resolutions.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(AnchoredChainError::LoadFailed(
            "membership resolution cut is not canonical".to_string(),
        ));
    }
    let founder_registration = Box::pin(super::store_objects::load_founder_registration_with_root(
        storage, root, root_value,
    ))
    .await
    .map_err(map_membership_object_error)?;
    let founder_registration_ref =
        super::store_commit::StoreDeviceRegistrationRef::from_registration(
            &founder_registration.value,
            founder_registration.object,
        );
    let provider_admin = super::provider::ProviderAdminState::founder_from_root(
        root.clone(),
        founder_registration_ref,
        &root_value.descriptor.founder_provider_admin,
    );
    let graph =
        load_exact_membership_graph(storage, root, root_value, exact_heads, verified_activations)
            .await?;
    let chain = load_layered_membership_chain(
        storage,
        root,
        root_value,
        graph,
        exact_resolutions,
        &provider_admin,
        verified_activations,
        pending_resolution,
    )
    .await?;
    if !chain.is_founded_by(owner_pubkey) {
        return Err(AnchoredChainError::FounderMismatch {
            founder: chain.founder_pubkey().map(str::to_string),
            owner: owner_pubkey.to_string(),
        });
    }
    Ok(chain)
}

async fn validate_provider_admin_records(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &super::store_commit::StoreProtocolRoot,
    entries: &[MembershipEntry],
) -> Result<(), AnchoredChainError> {
    for entry in entries {
        let Some(super::provider::ProviderAdminMembershipChange::MergeConcurrent {
            change:
                super::provider::ProviderAdminChange::Set {
                    administrator,
                    provider,
                    capability,
                    ..
                },
            ..
        }) = &entry.provider_admin
        else {
            continue;
        };
        let registration = Box::pin(super::store_objects::load_registration_ref_with_root(
            storage,
            root,
            root_value,
            administrator,
        ))
        .await
        .map_err(map_membership_object_error)?;
        if registration.value.store_root != *root || registration.value.provider != *provider {
            return Err(AnchoredChainError::LoadFailed(
                "provider administrator grant does not match its exact device registration"
                    .to_string(),
            ));
        }
        capability
            .verify(&root_value.descriptor.provider, provider, false)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    }
    Ok(())
}

fn validate_owner_grant_records(
    root_value: &super::store_commit::StoreProtocolRoot,
    entries: &[MembershipEntry],
) -> Result<(), AnchoredChainError> {
    for entry in entries {
        match &entry.change {
            MembershipChange::Founder { creation_id, .. }
                if *creation_id == root_value.descriptor.creation_id => {}
            MembershipChange::Founder { .. } => {
                return Err(AnchoredChainError::LoadFailed(
                    "founder Owner grant carries another Store creation id".to_string(),
                ));
            }
            MembershipChange::SetMember {
                role:
                    super::membership::StoreMembershipRoleGrant::Owner {
                        recovery: super::membership::OwnerRecoveryAnchorRef::Promotion { .. },
                    },
                ..
            } => {
                // The exact head's Store activation verified this entry's promotion
                // acceptance before records are reduced into a membership chain.
            }
            MembershipChange::SetMember {
                role: super::membership::StoreMembershipRoleGrant::Owner { .. },
                ..
            } => {
                return Err(AnchoredChainError::LoadFailed(
                    "non-founder Owner grant does not carry promotion acceptance".to_string(),
                ));
            }
            MembershipChange::SetMember { .. }
            | MembershipChange::RemoveMember { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => {}
        }
    }
    Ok(())
}

fn map_membership_object_error(error: StoreObjectError) -> AnchoredChainError {
    match error {
        StoreObjectError::Storage(source @ StorageError::Storage(_))
        | StoreObjectError::Storage(source @ StorageError::RotationPending(_)) => {
            AnchoredChainError::StorageUnavailable {
                operation: "discovering immutable membership objects".to_string(),
                source,
            }
        }
        error => AnchoredChainError::LoadFailed(error.to_string()),
    }
}

fn head_cursor_key(reference: &MembershipHeadRef) -> String {
    format!(
        "{}{}/{}",
        MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX,
        reference.coord.author_owner_grant,
        reference.coord.stream_id
    )
}

async fn read_head_cursors(db: &Database) -> Result<Vec<MembershipHeadRef>, String> {
    let prefix = MEMBERSHIP_HEAD_CURSOR_STATE_KEY_PREFIX.to_string();
    db.call(move |conn| {
        let mut statement = conn
            .prepare(
                "SELECT value FROM protocol_state \
                 WHERE substr(key, 1, length(?1)) = ?1 ORDER BY key",
            )
            .map_err(crate::database::DbError::from)?;
        let rows = statement
            .query_map([&prefix], |row| row.get::<_, String>(0))
            .map_err(crate::database::DbError::from)?;
        let mut cursors = Vec::new();
        for row in rows {
            let value = row.map_err(crate::database::DbError::from)?;
            let reference: MembershipHeadRef = serde_json::from_str(&value).map_err(|error| {
                crate::database::DbError::Message(format!(
                    "membership head cursor is malformed: {error}"
                ))
            })?;
            if reference.coord.seq == 0 {
                return Err(crate::database::DbError::Message(
                    "membership head cursor has sequence zero".to_string(),
                ));
            }
            cursors.push(reference);
        }
        Ok(cursors)
    })
    .await
    .map_err(|error| format!("read membership head cursors: {error}"))
}

pub(crate) fn upsert_head_cursor_on(
    conn: &rusqlite::Connection,
    reference: &MembershipHeadRef,
) -> Result<(), crate::database::DbError> {
    let key = head_cursor_key(reference);
    let existing: Option<String> = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [&key],
            |row| row.get(0),
        )
        .optional()
        .map_err(crate::database::DbError::from)?;
    if let Some(existing) = existing {
        let existing: MembershipHeadRef = serde_json::from_str(&existing).map_err(|error| {
            crate::database::DbError::Message(format!(
                "membership head cursor is malformed: {error}"
            ))
        })?;
        if existing.coord.stream_key() != reference.coord.stream_key() {
            return Err(crate::database::DbError::Message(
                "membership head cursor key names a different stream".to_string(),
            ));
        }
        if existing.coord.seq > reference.coord.seq {
            return Ok(());
        }
        if existing.coord.seq == reference.coord.seq {
            if existing == *reference {
                return Ok(());
            }
            return Err(crate::database::DbError::Message(
                "membership head cursor forks at the same sequence".to_string(),
            ));
        }
    }
    let value = serde_json::to_string(reference).map_err(|error| {
        crate::database::DbError::Message(format!("serialize membership head cursor: {error}"))
    })?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )
    .map(|_| ())
    .map_err(crate::database::DbError::from)
}

async fn persist_head_cursors(db: &Database, cursors: &[MembershipHeadRef]) -> Result<(), String> {
    let cursors = cursors.to_vec();
    db.call(move |conn| {
        let transaction = conn
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        for reference in &cursors {
            upsert_head_cursor_on(&transaction, reference)?;
        }
        transaction.commit().map_err(crate::database::DbError::from)
    })
    .await
    .map_err(|error| format!("persist membership head cursors: {error}"))
}

pub async fn seed_head_watermark(db: &Database, floor: &[MembershipHeadRef]) -> Result<(), String> {
    persist_head_cursors(db, floor).await
}

pub(crate) fn validate_membership_floor(floor: &[MembershipHeadRef]) -> Result<(), String> {
    if floor.is_empty() {
        return Err("membership floor is empty".to_string());
    }
    for (index, reference) in floor.iter().enumerate() {
        if reference.coord.seq == 0 {
            return Err("membership floor contains sequence zero".to_string());
        }
        if index > 0 && floor[index - 1].coord.stream_key() >= reference.coord.stream_key() {
            return Err("membership floor is not strictly ordered by author stream".to_string());
        }
    }
    Ok(())
}

pub(crate) async fn load_and_persist_owner_anchor(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: &str,
    db: &Database,
) -> Result<MembershipChain, AnchoredChainError> {
    let _membership_load = db.lock_membership_load().await;
    let cursors = read_head_cursors(db)
        .await
        .map_err(AnchoredChainError::LoadFailed)?;
    let chain = load_exact_anchored_chain(storage, root, &cursors, Some(owner_pubkey)).await?;
    let founder = chain.founder_coord().ok_or_else(|| {
        AnchoredChainError::LoadFailed("owner-anchored membership chain is empty".to_string())
    })?;
    let founder_head_ref = chain
        .head_ref_for_stream(
            &founder.author_pubkey,
            &founder.author_owner_grant,
            founder.stream_id,
        )
        .cloned()
        .ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "owner-anchored membership chain has no exact founder head".to_string(),
            )
        })?;
    let founder_head = load_exact_membership_head(storage, root, &founder_head_ref).await?;
    let founder_registration_ref = founder_head.body.author_registration.clone();
    let founder_registration =
        super::store_objects::load_registration_ref(storage, root, &founder_registration_ref)
            .await
            .map_err(map_membership_object_error)?;
    let founder_registration_bytes = founder_registration.bytes;
    let founder_registration = founder_registration.value;
    if founder_registration.author_pubkey != owner_pubkey
        || !matches!(
            founder_registration.origin,
            super::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
        )
    {
        return Err(AnchoredChainError::LoadFailed(
            "founder head registration is not activated by the Store root".to_string(),
        ));
    }
    let protocol_root = super::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    if protocol_root.descriptor.founder_pubkey != owner_pubkey {
        return Err(AnchoredChainError::LoadFailed(
            "owner anchor differs from the Store root founder".to_string(),
        ));
    }
    let founder_genesis = ResolvedStoreDeviceState::founder(
        root,
        founder_registration_ref.clone(),
        &protocol_root.descriptor.founder_pubkey,
        protocol_root.descriptor.founder_grant.clone(),
        &protocol_root.descriptor.founder_recovery,
    )
    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    db.install_store_owner_anchor(
        root.clone(),
        founder_registration_ref,
        founder_registration,
        founder_registration_bytes,
        founder_genesis,
        owner_pubkey.to_string(),
        crate::database::InitialStoreMembershipAuthority::MergeConcurrent {
            head_refs: chain.head_refs().to_vec(),
        },
    )
    .await
    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::ObjectSlot;
    use crate::sync::cloud_storage::{CloudCipher, PendingRotation};
    use crate::sync::membership::AuthorStreamId;
    use crate::sync::storage::ExactObjectRef;
    use crate::sync::test_helpers::{
        install_active_device_fixture, open_test_db, promote_active_member_fixture, pubkey_hex,
        TestCustody, TestStore,
    };
    use std::sync::RwLock;

    struct MergeFixture {
        store: TestStore,
        db: Database,
        owner: UserKeypair,
        owner_pubkey: String,
    }

    async fn merge_fixture(store_id: &str) -> MergeFixture {
        let db = open_test_db();
        let owner = UserKeypair::generate();
        let owner_pubkey = pubkey_hex(&owner);
        let store = TestStore::create(&db, store_id, owner.clone())
            .await
            .expect("create exact MergeConcurrent Store");
        MergeFixture {
            store,
            db,
            owner,
            owner_pubkey,
        }
    }

    async fn load_fixture(fixture: &MergeFixture) -> MembershipChain {
        load_current_exact_chain(
            &fixture.store.storage,
            &fixture.store.root,
            Some(&fixture.owner_pubkey),
            Some(&fixture.db),
        )
        .await
        .expect("load exact membership chain")
    }

    async fn invite_fixture_member(
        fixture: &MergeFixture,
        member: &UserKeypair,
        role: MemberRole,
    ) -> crate::join_code::InviteCode {
        invite_member(
            &fixture.store.storage,
            fixture.store.home.as_ref(),
            &fixture.owner,
            &Hlc::new("owner-device".to_string()),
            &pubkey_hex(member),
            None,
            role,
            &EncryptionService::from_key([42; 32]),
            fixture.store.storage.store_id(),
            "Test Store",
            &fixture.db,
        )
        .await
        .expect("invite exact member")
    }

    async fn remove_fixture_member(fixture: &MergeFixture, member: &UserKeypair) {
        let custody = TestCustody::default();
        let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
            [42; 32],
        )));
        remove_member(
            &fixture.store.storage,
            fixture.store.home.as_ref(),
            &fixture.owner,
            &Hlc::new("owner-device".to_string()),
            &pubkey_hex(member),
            fixture.store.storage.store_id(),
            &EncryptionService::from_key([42; 32]),
            &custody,
            &cipher,
            &PendingRotation::none(),
            &fixture.db,
        )
        .await
        .expect("remove exact member");
    }

    fn altered_exact(reference: &ExactObjectRef, label: &[u8]) -> ExactObjectRef {
        ExactObjectRef::new(
            reference.slot().clone(),
            label.len() as u64,
            super::super::store_commit::ObjectHash::digest(label),
        )
    }

    async fn overwrite_head(
        fixture: &MergeFixture,
        reference: &MembershipHeadRef,
        head: &AuthorHead,
    ) {
        fixture
            .store
            .storage
            .delete_protocol_object(&reference.object)
            .await
            .expect("delete exact head before replacement");
        let context = ProtocolObjectContext::signed_plaintext(
            fixture.store.root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let prefix = crate::sync::store_commit::membership_head_slot_prefix(
            &reference.coord.author_pubkey,
            &reference.coord.author_owner_grant,
            reference.coord.stream_id,
            reference.coord.seq,
        );
        let prepared = fixture
            .store
            .storage
            .prepare_protocol_object(
                &context,
                reference.object.slot().clone(),
                &prefix,
                serde_json::to_vec(head).expect("serialize replacement head"),
            )
            .expect("prepare replacement head");
        super::super::store_objects::create_exact_object(&fixture.store.storage, &prepared)
            .await
            .expect("write replacement head");
    }

    #[tokio::test]
    async fn anchored_chain_loads_the_root_named_by_its_authoritative_hash() {
        let fixture = merge_fixture("pinned-root").await;
        let unrelated = merge_fixture("unrelated-root").await;
        assert_ne!(
            fixture.store.root.store_root_hash,
            unrelated.store.root.store_root_hash
        );

        let loaded = load_fixture(&fixture).await;
        let expected_store_id = fixture.store.root.store_root_id.to_string();
        assert_eq!(loaded.store_id(), Some(expected_store_id.as_str()));
        assert_eq!(loaded.founder_pubkey(), Some(fixture.owner_pubkey.as_str()));
    }

    #[tokio::test]
    async fn current_floor_is_the_exact_signed_head_cut() {
        let fixture = merge_fixture("exact-floor").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;

        let chain = load_fixture(&fixture).await;
        let floor = current_membership_floor(
            &fixture.store.storage,
            &fixture.store.root,
            Some(&fixture.owner_pubkey),
            Some(&fixture.db),
        )
        .await
        .expect("read exact membership floor");

        assert_eq!(floor, chain.head_refs());
        assert!(floor.iter().all(|reference| reference.coord.seq > 0));
    }

    #[tokio::test]
    async fn current_floor_requires_every_exact_entry() {
        let fixture = merge_fixture("missing-entry").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        let chain = load_fixture(&fixture).await;
        let head = chain.head_refs().last().expect("current head").clone();
        let loaded_head =
            load_exact_membership_head(&fixture.store.storage, &fixture.store.root, &head)
                .await
                .expect("load exact head");
        fixture
            .store
            .storage
            .delete_protocol_object(&loaded_head.body.entry.object)
            .await
            .expect("remove exact selected entry");

        current_membership_floor(
            &fixture.store.storage,
            &fixture.store.root,
            Some(&fixture.owner_pubkey),
            Some(&fixture.db),
        )
        .await
        .expect_err("a signed head whose exact entry is absent must fail");
    }

    #[tokio::test]
    async fn persisted_author_floor_requires_readable_head() {
        let fixture = merge_fixture("missing-head").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        let chain = load_fixture(&fixture).await;
        let head = chain.head_refs().last().expect("current head").clone();
        fixture
            .store
            .storage
            .delete_protocol_object(&head.object)
            .await
            .expect("remove exact head");

        load_fixture_result(&fixture)
            .await
            .expect_err("a durable exact cursor requires its head");
    }

    async fn load_fixture_result(
        fixture: &MergeFixture,
    ) -> Result<MembershipChain, MembershipOpsError> {
        load_current_exact_chain(
            &fixture.store.storage,
            &fixture.store.root,
            Some(&fixture.owner_pubkey),
            Some(&fixture.db),
        )
        .await
    }

    #[tokio::test]
    async fn membership_head_must_match_its_exact_author_coordinate() {
        let fixture = merge_fixture("head-author").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        let chain = load_fixture(&fixture).await;
        let reference = chain.head_refs().last().expect("current head").clone();
        let mut head =
            load_exact_membership_head(&fixture.store.storage, &fixture.store.root, &reference)
                .await
                .expect("load exact head");
        head.body.entry.coord.author_pubkey = hex::encode([9; 32]);
        overwrite_head(&fixture, &reference, &head).await;

        load_fixture_result(&fixture)
            .await
            .expect_err("a head selecting another author coordinate must fail");
    }

    #[tokio::test]
    async fn invalid_membership_head_signature_preserves_owner_and_cursor() {
        let fixture = merge_fixture("bad-head-signature").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        let chain = load_fixture(&fixture).await;
        let before_owner = fixture
            .db
            .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
            .await
            .unwrap();
        let before_cursors = read_head_cursors(&fixture.db).await.unwrap();
        let reference = chain.head_refs().last().expect("current head").clone();
        let mut head =
            load_exact_membership_head(&fixture.store.storage, &fixture.store.root, &reference)
                .await
                .expect("load exact head");
        head.signature = hex::encode([0; 64]);
        overwrite_head(&fixture, &reference, &head).await;

        load_fixture_result(&fixture)
            .await
            .expect_err("an invalid exact head signature must fail");
        assert_eq!(
            fixture
                .db
                .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
                .await
                .unwrap(),
            before_owner
        );
        assert_eq!(
            read_head_cursors(&fixture.db).await.unwrap(),
            before_cursors
        );
    }

    #[tokio::test]
    async fn forked_membership_cursor_preserves_the_accepted_reference() {
        let fixture = merge_fixture("forked-cursor").await;
        let current = load_fixture(&fixture)
            .await
            .head_refs()
            .first()
            .expect("founder head")
            .clone();
        persist_head_cursors(&fixture.db, std::slice::from_ref(&current))
            .await
            .unwrap();
        let mut fork = current.clone();
        fork.head_hash = super::super::store_commit::ObjectHash::digest(b"forked head");
        fork.object = altered_exact(&current.object, b"forked object");

        assert!(persist_head_cursors(&fixture.db, &[fork]).await.is_err());
        assert_eq!(read_head_cursors(&fixture.db).await.unwrap(), vec![current]);
    }

    #[tokio::test]
    async fn missing_membership_head_is_rejected() {
        let fixture = merge_fixture("missing-founder-head").await;
        let chain = load_fixture(&fixture).await;
        let head = chain.head_refs().first().expect("founder head");
        fixture
            .store
            .storage
            .delete_protocol_object(&head.object)
            .await
            .expect("remove founder head");

        load_exact_anchored_chain(
            &fixture.store.storage,
            &fixture.store.root,
            &[],
            Some(&fixture.owner_pubkey),
        )
        .await
        .expect_err("a founder entry without its exact signed head is uncommitted");
    }

    #[tokio::test]
    async fn entry_beyond_membership_head_is_not_committed() {
        let fixture = merge_fixture("unheaded-entry").await;
        let member = UserKeypair::generate();
        let chain = load_fixture(&fixture).await;
        let founder = chain.entries().first().expect("founder");
        let entry = chain
            .signed_set_member_in_stream(
                &fixture.owner,
                founder.stream_id,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "unheaded member".to_string(),
            )
            .expect("sign entry after exact head");
        let (prepared, _) = super::super::store_objects::prepare_membership_entry(
            &fixture.store.storage,
            fixture.store.root.store_root_hash,
            &entry,
        )
        .await
        .expect("prepare unheaded entry");
        super::super::store_objects::create_exact_object(&fixture.store.storage, &prepared)
            .await
            .expect("publish unheaded entry");

        let loaded = load_fixture(&fixture).await;
        assert!(!loaded.can_write_now(&pubkey_hex(&member)));
    }

    #[tokio::test]
    async fn complete_chain_still_validates() {
        let fixture = merge_fixture("complete-chain").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;

        assert!(load_fixture(&fixture)
            .await
            .can_write_now(&pubkey_hex(&member)));
    }

    #[tokio::test]
    async fn store_prefix_projection_retains_direct_membership_heads() {
        let fixture = merge_fixture("project-direct-membership").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        let current = load_fixture(&fixture).await;

        let projected = project_anchored_chain_to_verified_store_prefix(
            &fixture.store.storage,
            &fixture.store.root,
            &fixture.owner_pubkey,
            current.head_refs(),
            &super::super::store_pull::VerifiedMergeMembershipPrefix::default(),
        )
        .await
        .expect("project direct membership to the empty Store prefix");

        assert_eq!(projected.head_refs(), current.head_refs());
        assert!(projected.can_write_now(&pubkey_hex(&member)));
    }

    #[tokio::test]
    async fn store_prefix_projection_excludes_store_bound_membership_and_its_direct_suffix() {
        let fixture = merge_fixture("project-store-bound-membership").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        let member_db = open_test_db();
        install_active_device_fixture(
            &fixture.store,
            &fixture.db,
            &member_db,
            &member,
            "2026-07-21T00:00:00Z",
        )
        .await
        .expect("activate member device");
        let before_promotion = load_fixture(&fixture).await;
        promote_active_member_fixture(
            &fixture.store,
            &fixture.db,
            &member_db,
            &fixture.owner,
            &member,
            &EncryptionService::from_key([42; 32]),
        )
        .await
        .expect("promote member to Owner");
        let after_promotion = load_fixture(&fixture).await;
        assert_ne!(after_promotion.head_refs(), before_promotion.head_refs());
        let later_member = UserKeypair::generate();
        invite_fixture_member(&fixture, &later_member, MemberRole::Member).await;
        let candidate = load_fixture(&fixture).await;
        assert!(candidate.can_write_now(&pubkey_hex(&later_member)));

        let projected = project_anchored_chain_to_verified_store_prefix(
            &fixture.store.storage,
            &fixture.store.root,
            &fixture.owner_pubkey,
            candidate.head_refs(),
            &super::super::store_pull::VerifiedMergeMembershipPrefix::default(),
        )
        .await
        .expect("project membership before the Owner promotion Store control");

        assert_eq!(projected.head_refs(), before_promotion.head_refs());
        assert!(projected.can_write_now(&pubkey_hex(&member)));
        assert!(!projected.is_owner_now(&pubkey_hex(&member)));
        assert!(!projected.can_write_now(&pubkey_hex(&later_member)));
    }

    #[tokio::test]
    async fn exact_membership_heads_must_begin_at_their_grant_anchor() {
        let fixture = merge_fixture("relocated-exact-membership-head").await;
        let current = load_fixture(&fixture).await;
        let founder_ref = current
            .head_refs()
            .first()
            .expect("founder membership head")
            .clone();
        let founder_head =
            load_exact_membership_head(&fixture.store.storage, &fixture.store.root, &founder_ref)
                .await
                .expect("load founder membership head");
        let registration = fixture
            .db
            .activated_store_device_registration(founder_head.body.author_registration.clone())
            .await
            .expect("load founder device registration");
        let signer = registration
            .device_signer(&fixture.owner)
            .expect("derive founder device signer");
        let relocated = AuthorHead::signed(
            founder_head.store_id.clone(),
            founder_head.body.clone(),
            founder_head.activation.clone(),
            &signer,
        );
        let context = ProtocolObjectContext::signed_plaintext(
            fixture.store.root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let prefix = super::super::store_commit::membership_head_slot_prefix(
            &founder_ref.coord.author_pubkey,
            &founder_ref.coord.author_owner_grant,
            AuthorStreamId::from_bytes([99; 32]),
            founder_ref.coord.seq,
        );
        let slot = fixture
            .store
            .storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await
            .expect("allocate relocated membership head slot");
        let prepared = fixture
            .store
            .storage
            .prepare_protocol_object(
                &context,
                slot,
                &prefix,
                serde_json::to_vec(&relocated).expect("serialize relocated membership head"),
            )
            .expect("prepare relocated membership head");
        super::super::store_objects::create_exact_object(&fixture.store.storage, &prepared)
            .await
            .expect("publish relocated membership head");
        let relocated_ref = MembershipHeadRef {
            coord: founder_ref.coord,
            head_hash: relocated.head_hash(),
            object: prepared.reference().clone(),
        };

        load_anchored_chain_at_exact_heads(
            &fixture.store.storage,
            &fixture.store.root,
            &fixture.owner_pubkey,
            &[relocated_ref],
            &[],
        )
        .await
        .expect_err("a membership head relocated outside its grant anchor must fail");
    }

    #[tokio::test]
    async fn invite_carries_the_founder_and_exact_root() {
        let fixture = merge_fixture("invite-authority").await;
        let invitee = UserKeypair::generate();
        let invite = invite_fixture_member(&fixture, &invitee, MemberRole::Member).await;

        assert_eq!(invite.owner_pubkey, fixture.owner_pubkey);
        assert_eq!(invite.store_root, fixture.store.root);
        assert!(matches!(
            invite.membership_floor,
            crate::join_code::MembershipFloor::MergeConcurrent(ref floor) if !floor.is_empty()
        ));
    }

    #[tokio::test]
    async fn inviting_yourself_is_a_typed_self_invite_error() {
        let fixture = merge_fixture("self-invite").await;
        let result = invite_member(
            &fixture.store.storage,
            fixture.store.home.as_ref(),
            &fixture.owner,
            &Hlc::new("owner-device".to_string()),
            &fixture.owner_pubkey,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            fixture.store.storage.store_id(),
            "Test Store",
            &fixture.db,
        )
        .await;

        assert!(matches!(result, Err(MembershipOpsError::SelfInvite)));
    }

    #[tokio::test]
    async fn inviting_without_an_exact_root_is_refused_with_a_typed_variant() {
        let store = TestStore::new().await;
        let db = open_test_db();
        let invitee = UserKeypair::generate();

        let result = invite_member(
            &store.storage,
            store.home.as_ref(),
            &store.signer,
            &Hlc::new("owner-device".to_string()),
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &EncryptionService::from_key([42; 32]),
            store.storage.store_id(),
            "Test Store",
            &db,
        )
        .await;

        assert!(matches!(result, Err(MembershipOpsError::NoFounderChain)));
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_membership_error_variants() {
        let store = TestStore::new().await;
        let db = open_test_db();

        assert!(matches!(
            get_members(&store.storage, None, &db).await,
            Err(MembershipOpsError::NoFounderChain)
        ));
    }

    #[tokio::test]
    async fn remove_member_completes_when_the_home_reports_no_per_member_revocation() {
        let fixture = merge_fixture("unsupported-revocation").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        remove_fixture_member(&fixture, &member).await;

        assert!(!load_fixture(&fixture)
            .await
            .can_write_now(&pubkey_hex(&member)));
    }

    #[tokio::test]
    async fn suppressed_remove_is_detected_by_the_exact_cursor() {
        let fixture = merge_fixture("suppressed-remove").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        remove_fixture_member(&fixture, &member).await;
        let chain = load_fixture(&fixture).await;
        let remove_head = chain.head_refs().last().expect("remove head").clone();
        fixture
            .store
            .storage
            .delete_protocol_object(&remove_head.object)
            .await
            .expect("suppress exact remove head");

        load_fixture_result(&fixture)
            .await
            .expect_err("the accepted exact remove cursor cannot be suppressed");
    }

    #[test]
    fn apply_key_rotation_replays_an_already_adopted_keyring() {
        let live = EncryptionService::from_key([1; 32])
            .with_appended_generation(2, [2; 32])
            .unwrap();
        let custody = TestCustody::default();
        custody
            .persist(&MasterKeyring::from(live.clone()))
            .expect("seed custody");
        let cipher = RwLock::new(CloudCipher::Encrypted(live.clone()));
        let fingerprint =
            apply_key_rotation(EncryptionService::from_key([1; 32]), &custody, &cipher)
                .expect("an already-covered keyring is an idempotent adoption");
        assert_eq!(fingerprint, live.fingerprint());
        assert_eq!(
            custody.unlock().unwrap().unwrap().fingerprint(),
            live.fingerprint()
        );
    }

    #[tokio::test]
    async fn head_cursor_persist_never_regresses() {
        let fixture = merge_fixture("cursor-monotonic").await;
        let current = load_fixture(&fixture)
            .await
            .head_refs()
            .first()
            .expect("founder head")
            .clone();
        let mut higher = current.clone();
        higher.coord.seq = 10;
        higher.coord.entry_hash = super::super::store_commit::ObjectHash::digest(b"entry 10");
        higher.head_hash = super::super::store_commit::ObjectHash::digest(b"head 10");
        higher.object = altered_exact(&current.object, b"object 10");
        let mut lower = higher.clone();
        lower.coord.seq = 9;
        lower.coord.entry_hash = super::super::store_commit::ObjectHash::digest(b"entry 9");
        lower.head_hash = super::super::store_commit::ObjectHash::digest(b"head 9");
        lower.object = altered_exact(&current.object, b"object 9");

        persist_head_cursors(&fixture.db, std::slice::from_ref(&higher))
            .await
            .unwrap();
        persist_head_cursors(&fixture.db, &[lower]).await.unwrap();

        assert_eq!(read_head_cursors(&fixture.db).await.unwrap(), vec![higher]);
    }

    #[tokio::test]
    async fn head_cursor_rejects_a_reference_from_another_author_stream() {
        let fixture = merge_fixture("cursor-stream").await;
        let current = load_fixture(&fixture)
            .await
            .head_refs()
            .first()
            .expect("founder head")
            .clone();
        persist_head_cursors(&fixture.db, std::slice::from_ref(&current))
            .await
            .unwrap();
        let mut mismatched = current.clone();
        mismatched.coord.author_pubkey = hex::encode([3; 32]);
        mismatched.coord.seq = current.coord.seq + 1;

        assert!(persist_head_cursors(&fixture.db, &[mismatched])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn pruned_membership_author_stream_is_replaced_and_persisted() {
        let db = open_test_db();
        let author = hex::encode([3; crate::keys::SIGN_PUBLICKEYBYTES]);
        let grant = MembershipGrantId(super::super::store_commit::ObjectHash::digest(
            b"local author stream grant",
        ));
        let first = db
            .select_membership_author_stream(&author, &grant, Default::default())
            .await
            .unwrap();
        let reused = db
            .select_membership_author_stream(
                &author,
                &grant,
                std::collections::BTreeSet::from([first]),
            )
            .await
            .unwrap();
        assert_eq!(reused, first);
        let replacement = db
            .select_membership_author_stream(&author, &grant, Default::default())
            .await
            .unwrap();
        assert_ne!(replacement, first);
    }

    #[test]
    fn membership_floor_rejects_unsorted_author_streams() {
        let grant = MembershipGrantId(super::super::store_commit::ObjectHash::digest(
            b"floor ordering grant",
        ));
        let object = ExactObjectRef::new(
            ObjectSlot::logical("test/floor/head.json".to_string()).unwrap(),
            1,
            super::super::store_commit::ObjectHash::digest(b"x"),
        );
        let make = |author: &str, stream: u8| MembershipHeadRef {
            coord: MembershipCoord {
                author_pubkey: author.to_string(),
                author_owner_grant: grant.clone(),
                stream_id: AuthorStreamId::from_bytes([stream; 32]),
                seq: 1,
                entry_hash: super::super::store_commit::ObjectHash::digest(author.as_bytes()),
            },
            head_hash: super::super::store_commit::ObjectHash::digest(&[stream]),
            object: object.clone(),
        };
        let later = make("bbbb", 2);
        let earlier = make("aaaa", 1);

        assert!(validate_membership_floor(&[later, earlier]).is_err());
    }

    #[tokio::test]
    async fn seeding_a_complete_head_floor_is_atomic() {
        let fixture = merge_fixture("atomic-floor").await;
        let db = open_test_db();
        let first = load_fixture(&fixture)
            .await
            .head_refs()
            .first()
            .expect("founder head")
            .clone();
        let mut second = first.clone();
        second.coord.author_pubkey = hex::encode([8; 32]);
        second.coord.author_owner_grant = MembershipGrantId(
            super::super::store_commit::ObjectHash::digest(b"second grant"),
        );
        second.coord.stream_id = AuthorStreamId::from_bytes([8; 32]);
        second.object = altered_exact(&first.object, b"second exact head");
        let mut floor = vec![first, second.clone()];
        floor.sort_by_key(|reference| reference.coord.stream_key());
        let rejected_key = head_cursor_key(&second);
        db.call(move |conn| {
            conn.execute_batch(&format!(
                "CREATE TRIGGER reject_second_membership_floor \
                     BEFORE INSERT ON protocol_state \
                     WHEN NEW.key = '{rejected_key}' \
                     BEGIN SELECT RAISE(ABORT, 'forced cursor failure'); END;"
            ))
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();

        assert!(seed_head_watermark(&db, &floor).await.is_err());
        assert!(read_head_cursors(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn owner_pin_and_complete_head_floor_commit_atomically() {
        let fixture = merge_fixture("atomic-owner-pin").await;
        let db = open_test_db();
        let head = load_fixture(&fixture)
            .await
            .head_refs()
            .first()
            .expect("founder head")
            .clone();
        let rejected_key = head_cursor_key(&head);
        db.call(move |conn| {
            conn.execute_batch(&format!(
                "CREATE TRIGGER reject_anchor_cursor \
                 BEFORE INSERT ON protocol_state \
                 WHEN NEW.key = '{rejected_key}' \
                 BEGIN SELECT RAISE(ABORT, 'forced cursor failure'); END;"
            ))
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();

        assert!(load_and_persist_owner_anchor(
            &fixture.store.storage,
            &fixture.store.root,
            &fixture.owner_pubkey,
            &db,
        )
        .await
        .is_err());
        assert_eq!(
            db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
            None
        );
        assert!(read_head_cursors(&db).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn reader_refuses_a_head_that_regresses_below_its_cursor() {
        let fixture = merge_fixture("cursor-regression").await;
        let member = UserKeypair::generate();
        invite_fixture_member(&fixture, &member, MemberRole::Member).await;
        remove_fixture_member(&fixture, &member).await;
        let chain = load_fixture(&fixture).await;
        let latest = chain.head_refs().last().expect("latest head").clone();
        let latest_head =
            load_exact_membership_head(&fixture.store.storage, &fixture.store.root, &latest)
                .await
                .expect("load latest head");
        let predecessor = latest_head.body.predecessor.expect("remove predecessor");
        fixture
            .store
            .storage
            .delete_protocol_object(&latest.object)
            .await
            .expect("remove latest exact head");

        let error = load_fixture_result(&fixture)
            .await
            .expect_err("the accepted cursor cannot regress to its predecessor");
        assert!(error.to_string().contains("regressed"));
        assert!(predecessor.coord.seq < latest.coord.seq);
    }

    #[tokio::test]
    async fn membership_projection_handles_a_deep_valid_predecessor_path_iteratively() {
        let fixture = merge_fixture("deep-membership-projection").await;
        let chain = load_fixture(&fixture).await;
        let root_value = super::super::store_objects::load_store_protocol_root(
            &fixture.store.storage,
            &fixture.store.root,
        )
        .await
        .expect("load Store root")
        .value;
        let seed = load_exact_membership_graph_objects(
            &fixture.store.storage,
            &fixture.store.root,
            &root_value,
            chain.head_refs(),
        )
        .await
        .expect("load seed membership graph")
        .path_heads
        .into_values()
        .next()
        .expect("founder membership head");

        let mut path_heads = BTreeMap::new();
        let mut predecessor = None;
        for sequence in 1..=20_000_u64 {
            let mut node = seed.clone();
            node.entry.seq = sequence;
            node.entry.previous_hash = predecessor
                .as_ref()
                .map(|reference: &MembershipHeadRef| reference.coord.entry_hash);
            node.entry.dependencies.clear();
            node.entry.resolution_dependencies.clear();
            node.reference.coord = node.entry.coord();
            node.head.body.entry.coord = node.reference.coord.clone();
            node.head.body.predecessor = predecessor.clone();
            predecessor = Some(node.reference.clone());
            path_heads.insert(node.reference.coord.clone(), node);
        }
        let graph = LoadedExactMembershipGraph {
            entries: path_heads
                .iter()
                .map(|(coord, node)| (coord.clone(), node.entry.clone()))
                .collect(),
            heads: Vec::new(),
            path_heads,
        };
        let statuses = membership_projection_statuses(
            &graph,
            &super::super::store_pull::VerifiedMergeMembershipPrefix::default(),
            &BTreeMap::new(),
        )
        .expect("project deep predecessor path");

        assert_eq!(statuses.len(), 20_000);
        assert!(statuses
            .values()
            .all(|status| *status == MembershipProjectionStatus::Included));
    }
}
