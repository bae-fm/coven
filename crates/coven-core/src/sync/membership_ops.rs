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
use crate::storage::cloud::ListingCoverage;
use crate::storage::cloud::{CloudAccessOutcome, CloudAccessState};

use super::cloud_storage::{CloudCipherAccess, PendingRotation};
use super::hlc::Hlc;
use super::invite::InviteError;
#[cfg(test)]
use super::membership::founder_entry;
use super::membership::{
    AuthorHead, MemberInfo, MemberRole, MembershipChain, MembershipCoord, MembershipEntry,
    OwnerGrantId,
};
use super::storage::{CoordinationStorage, StorageError, SyncStorage};
use super::store_commit::{ObjectHash, StoreControl};
#[cfg(test)]
use super::store_objects::append_membership_entry_object;
use super::store_objects::{
    append_membership_head_object, list_membership_entry_objects, list_membership_head_objects,
    load_membership_entry_slot, load_membership_head_slot, StoreObjectError,
};
use crate::database::Database;
use std::collections::BTreeMap;

pub async fn list_membership_entries(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<Vec<MembershipCoord>, StoreObjectError> {
    Ok(list_membership_entry_objects(storage, store_root_hash)
        .await?
        .entries
        .into_iter()
        .map(|(coord, _)| coord)
        .collect())
}

async fn read_membership_entry(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    coord: &MembershipCoord,
) -> Result<Vec<u8>, StoreObjectError> {
    load_membership_entry_slot(
        storage,
        store_root_hash,
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.seq,
    )
    .await?
    .map(|entry| entry.bytes)
    .ok_or_else(|| {
        StorageError::NotFound(format!(
            "membership entry {}/{}/{}",
            coord.author_pubkey, coord.author_owner_grant, coord.seq
        ))
        .into()
    })
}

struct VisibleMembershipHeads {
    by_grant: BTreeMap<OwnerGrantId, AuthorHead>,
    coverage: ListingCoverage,
}

async fn visible_membership_heads(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
) -> Result<VisibleMembershipHeads, StoreObjectError> {
    let listing = list_membership_head_objects(storage, store_root_hash).await?;
    let mut latest = BTreeMap::<OwnerGrantId, AuthorHead>::new();
    for verified in listing.heads {
        let head = verified.value;
        if latest
            .get(&head.author_owner_grant)
            .is_none_or(|current| current.seq < head.seq)
        {
            latest.insert(head.author_owner_grant.clone(), head);
        }
    }
    Ok(VisibleMembershipHeads {
        by_grant: latest,
        coverage: listing.coverage,
    })
}

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
    /// The cloud key rotation a member removal performs is committed — the member
    /// is out and the store is rotated for every remaining member — but this
    /// device could not adopt the rotated key into its own keyring and live cipher.
    /// The removal is durable; this device keeps sealing under the superseded
    /// generation until adoption succeeds. Its two remedies are non-destructive:
    /// retrying the removal re-derives and re-adopts the same generation (the write
    /// is idempotent), and the next sync cycle adopts it from this device's own
    /// `keys/{self}` wrapped key.
    #[error(
        "member removal committed the cloud key rotation, but this device could not \
         adopt the rotated key locally: {source}; retry the removal to re-adopt it, \
         or let the next sync cycle adopt it from this device's wrapped key"
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
    #[error("sharing requires an encrypted cloud home")]
    NotEncryptedHome,
    #[error(transparent)]
    Serial(#[from] super::store_outbound::StoreOutboundError),
}

async fn required_store_root_hash(db: &Database) -> Result<ObjectHash, MembershipOpsError> {
    db.required_store_root_hash_mapped(
        || MembershipOpsError::NoFounderChain,
        |reason| MembershipOpsError::Database(format!("Store protocol root hash: {reason}")),
        |error| MembershipOpsError::Database(error.to_string()),
    )
    .await
}

/// `protocol_state` key holding the hex Ed25519 pubkey of the store's established
/// owner — pinned at create (the creator), join (the invite's owner), or restore.
/// The membership chain is anchored to it: a chain whose founder differs is a
/// takeover attempt and is rejected (issue #95).
pub const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";

pub struct SerialMembershipContext<'a> {
    pub coordination: &'a dyn CoordinationStorage,
    pub device_id: String,
}

/// The per-author membership-head floor at the chain's current committed state:
/// every author's highest committed seq ([`MembershipChain::author_heads`]). A
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
    store_root_hash: ObjectHash,
    pinned_owner: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<Vec<super::membership::MembershipCoord>, MembershipOpsError> {
    let entries = list_membership_entries(storage, store_root_hash).await?;
    let chain = load_anchored_chain_if_known(
        storage,
        store_root_hash,
        &entries,
        pinned_owner,
        watermark_db,
    )
    .await?;
    Ok(chain.map_or_else(Vec::new, |chain| chain.author_heads()))
}

/// Read the membership chain from the sync storage and return the current members.
pub async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
    db: &Database,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let store_root_hash = required_store_root_hash(db).await?;
    if db.write_policy() == crate::WritePolicy::Serial {
        let state = match db
            .serial_membership_state()
            .await
            .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        {
            Some(state) => state,
            None => {
                if db
                    .latest_outbound_store_position()
                    .await
                    .map_err(|error| MembershipOpsError::Database(error.to_string()))?
                    .is_some()
                {
                    return Err(MembershipOpsError::Database(
                        "Serial membership state is absent after a Serial commit was materialized"
                            .to_string(),
                    ));
                }
                let root = super::store_objects::load_store_protocol_root_at_hash(
                    storage,
                    store_root_hash,
                )
                .await?
                .ok_or(MembershipOpsError::NoFounderChain)?
                .value;
                super::membership::SerialMembershipState::from_founder(
                    store_root_hash,
                    &root.founder,
                )
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?
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
    let entry_keys = list_membership_entries(storage, store_root_hash).await?;
    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoFounderChain)?;
    let chain = load_anchored_chain(
        storage,
        store_root_hash,
        &entry_keys,
        Some(&pinned_owner),
        Some(db),
    )
    .await?;
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
pub async fn invite_member(
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
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    invite_member_with_coordination(
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
    )
    .await
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
    let store_root_hash = required_store_root_hash(db).await?;
    let entry_keys = list_membership_entries(storage, store_root_hash).await?;

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
    let mut chain = load_anchored_chain(
        storage,
        store_root_hash,
        &entry_keys,
        Some(&pinned_owner),
        Some(db),
    )
    .await?;

    // Create the invitation
    let invite_ts = hlc.now().to_string();
    let join_info = super::invite::create_invitation_with_encryption_durable(
        storage,
        cloud_home,
        store_root_hash,
        &mut chain,
        user_keypair,
        public_key_hex,
        invitee_email,
        role,
        encryption,
        store_id,
        &invite_ts,
        db,
    )
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
    // invitee's own Add and the head just published for it): its per-author
    // heads are the floor the joiner seeds its watermark from, so a provider
    // can never roll the joiner back to a state before this invite.
    let membership_floor = chain.author_heads();
    let store_protocol_root = super::store_objects::load_pinned_store_protocol_root(
        storage,
        store_root_hash,
        store_id,
        &owner_pubkey,
    )
    .await?
    .ok_or_else(|| {
        MembershipOpsError::Storage(StorageError::NotFound(format!(
            "Store protocol root {store_root_hash}"
        )))
    })?;

    // Build the invite code
    Ok(crate::join_code::InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: store_name.to_string(),
        join_info,
        owner_pubkey,
        key_author_pubkey: user_pubkey_hex,
        store_root_hash: store_protocol_root.semantic_hash,
        membership_floor: crate::join_code::MembershipFloor::MergeConcurrent(membership_floor),
    })
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SerialInvitePlan {
    prepared: super::store_outbound::PreparedSerialControl,
    invitee_pubkey: String,
    invitee_email: Option<String>,
    role: MemberRole,
    desired_access: CloudAccessState,
    prior_wrapped_key: Option<Vec<u8>>,
    invitee_was_member: bool,
    wrapped_key: Vec<u8>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum SerialInviteProgress {
    Pending,
    AccessGranted {
        join_info: crate::storage::cloud::CloudHomeJoinInfo,
    },
}

async fn rollback_serial_invite(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    author: &str,
    plan: &SerialInvitePlan,
) -> Result<(), MembershipOpsError> {
    match plan.prior_wrapped_key.as_ref() {
        Some(bytes) => {
            storage
                .put_wrapped_key(author, &plan.invitee_pubkey, bytes.clone())
                .await?;
        }
        None => {
            storage
                .delete_wrapped_key(author, &plan.invitee_pubkey)
                .await?
        }
    }
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
    let _mutation = db.lock_membership_mutation().await;
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
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            let invitee_was_member = authorization
                .membership
                .current_members()
                .iter()
                .any(|(pubkey, _)| pubkey == public_key_hex);
            let entry = authorization
                .membership
                .signed_set_member(
                    user_keypair,
                    public_key_hex.to_string(),
                    invitee_email.map(str::to_string),
                    role.clone(),
                    hlc.now().to_string(),
                )
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::InvalidDurableMutation(
                        error.to_string(),
                    ))
                })?;
            let prepared = super::store_outbound::prepare_serial_control(
                db,
                storage,
                coordination,
                device_id,
                StoreControl::SerialMembership { entry },
                user_keypair,
            )
            .await?;
            let wrapped_key = super::invite::signed_serial_wrapped_key(
                store_id,
                public_key_hex,
                encryption,
                user_keypair,
                prepared.commit.position(),
            )?;
            let author = crate::keys::public_key_hex(user_keypair);
            let prior_wrapped_key = match storage.get_wrapped_key(&author, public_key_hex).await {
                Ok(bytes) => Some(bytes),
                Err(StorageError::NotFound(_)) => None,
                Err(error) => return Err(error.into()),
            };
            let plan = SerialInvitePlan {
                prepared,
                invitee_pubkey: public_key_hex.to_string(),
                invitee_email: invitee_email.map(str::to_string),
                role,
                desired_access: CloudAccessState::Present {
                    member_pubkey: public_key_hex.to_string(),
                    provider_account_email: invitee_email.map(str::to_string),
                },
                prior_wrapped_key,
                invitee_was_member,
                wrapped_key,
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
            let intent_hash = db
                .stage_membership_mutation(plan_bytes, progress_bytes)
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            (plan, progress, intent_hash)
        }
    };
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
        };
    let author = crate::keys::public_key_hex(user_keypair);
    if let Err(error) = storage
        .put_wrapped_key(&author, &plan.invitee_pubkey, plan.wrapped_key.clone())
        .await
    {
        if error.definitely_uncommitted() {
            rollback_serial_invite(storage, cloud_home, &author, &plan).await?;
            db.complete_membership_mutation(intent_hash)
                .await
                .map_err(|db_error| MembershipOpsError::Database(db_error.to_string()))?;
        }
        return Err(error.into());
    }
    match super::store_outbound::activate_serial_control(db, storage, coordination, &plan.prepared)
        .await
    {
        Ok(()) => {}
        Err(error) if error.definitely_uncommitted() => {
            rollback_serial_invite(storage, cloud_home, &author, &plan).await?;
            db.complete_membership_mutation(intent_hash)
                .await
                .map_err(|db_error| MembershipOpsError::Database(db_error.to_string()))?;
            return Err(error.into());
        }
        Err(error) => return Err(error.into()),
    }
    db.complete_membership_mutation(intent_hash)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    let root_hash = plan.prepared.commit.store_root_hash;
    let root = super::store_objects::load_store_protocol_root_at_hash(storage, root_hash)
        .await?
        .ok_or_else(|| MembershipOpsError::Database("Store protocol root is absent".to_string()))?
        .value;
    Ok(crate::join_code::InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: store_name.to_string(),
        join_info,
        owner_pubkey: root.author_pubkey,
        key_author_pubkey: crate::keys::public_key_hex(user_keypair),
        store_root_hash: root_hash,
        membership_floor: crate::join_code::MembershipFloor::Serial(Some(
            plan.prepared.commit.position(),
        )),
    })
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SerialReplacementWrap {
    recipient: String,
    prior: Option<Vec<u8>>,
    replacement: Option<Vec<u8>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SerialRemovalPlan {
    prepared: super::store_outbound::PreparedSerialControl,
    revokee_pubkey: String,
    revokee_email: Option<String>,
    wraps: Vec<SerialReplacementWrap>,
    keyring_payload: Vec<u8>,
}

async fn rollback_serial_removal(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    author: &str,
    plan: &SerialRemovalPlan,
) -> Result<(), MembershipOpsError> {
    for wrap in &plan.wraps {
        match wrap.prior.as_ref() {
            Some(bytes) => {
                storage
                    .put_wrapped_key(author, &wrap.recipient, bytes.clone())
                    .await?
            }
            None => storage.delete_wrapped_key(author, &wrap.recipient).await?,
        }
    }
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
    db: &Database,
) -> Result<EncryptionService, MembershipOpsError> {
    let _mutation = db.lock_membership_mutation().await;
    let (plan, intent_hash) = match db
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
            if plan.revokee_pubkey != public_key_hex {
                return Err(MembershipOpsError::Invite(InviteError::PendingMutation(
                    "the pending Serial removal names another member".to_string(),
                )));
            }
            (plan, row.intent_hash)
        }
        None => {
            let authorization =
                super::store_outbound::current_serial_authorization(db, storage, coordination)
                    .await?;
            if current_encryption.current_generation() != authorization.key_generation {
                return Err(MembershipOpsError::Invite(
                    InviteError::InvalidDurableMutation(format!(
                        "live key generation {} differs from committed Serial generation {}",
                        current_encryption.current_generation(),
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
            let prepared = super::store_outbound::prepare_serial_control(
                db,
                storage,
                coordination,
                device_id,
                StoreControl::SerialMembershipAndKeyRotation { entry, generation },
                user_keypair,
            )
            .await?;
            let new_keyring = current_encryption
                .with_appended_generation(generation, new_key)
                .map_err(|error| {
                    MembershipOpsError::Invite(InviteError::Crypto(format!(
                        "append Serial key generation: {error}"
                    )))
                })?;
            let author = crate::keys::public_key_hex(user_keypair);
            let mut wraps = Vec::new();
            for (recipient, _) in prepared.authorization_after.membership.current_members() {
                let prior = match storage.get_wrapped_key(&author, &recipient).await {
                    Ok(bytes) => Some(bytes),
                    Err(StorageError::NotFound(_)) => None,
                    Err(error) => return Err(error.into()),
                };
                let replacement = super::invite::signed_serial_wrapped_key(
                    store_id,
                    &recipient,
                    &new_keyring,
                    user_keypair,
                    prepared.commit.position(),
                )?;
                wraps.push(SerialReplacementWrap {
                    recipient,
                    prior,
                    replacement: Some(replacement),
                });
            }
            let revokee_prior = match storage.get_wrapped_key(&author, public_key_hex).await {
                Ok(bytes) => Some(bytes),
                Err(StorageError::NotFound(_)) => None,
                Err(error) => return Err(error.into()),
            };
            wraps.push(SerialReplacementWrap {
                recipient: public_key_hex.to_string(),
                prior: revokee_prior,
                replacement: None,
            });
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
            };
            let plan_bytes = serde_json::to_vec(&plan).map_err(|error| {
                MembershipOpsError::Invite(InviteError::InvalidDurableMutation(format!(
                    "serialize Serial removal plan: {error}"
                )))
            })?;
            let progress = serde_json::to_vec(&SerialInviteProgress::Pending).unwrap();
            let intent_hash = db
                .stage_membership_mutation(plan_bytes, progress)
                .await
                .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
            (plan, intent_hash)
        }
    };
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
    let author = crate::keys::public_key_hex(user_keypair);
    for wrap in &plan.wraps {
        let result = match wrap.replacement.as_ref() {
            Some(bytes) => {
                storage
                    .put_wrapped_key(&author, &wrap.recipient, bytes.clone())
                    .await
            }
            None => storage.delete_wrapped_key(&author, &wrap.recipient).await,
        };
        if let Err(error) = result {
            if error.definitely_uncommitted() {
                rollback_serial_removal(storage, cloud_home, &author, &plan).await?;
                db.complete_membership_mutation(intent_hash)
                    .await
                    .map_err(|db_error| MembershipOpsError::Database(db_error.to_string()))?;
            }
            return Err(error.into());
        }
    }
    match super::store_outbound::activate_serial_control(db, storage, coordination, &plan.prepared)
        .await
    {
        Ok(()) => {}
        Err(error) if error.definitely_uncommitted() => {
            rollback_serial_removal(storage, cloud_home, &author, &plan).await?;
            db.complete_membership_mutation(intent_hash)
                .await
                .map_err(|db_error| MembershipOpsError::Database(db_error.to_string()))?;
            return Err(error.into());
        }
        Err(error) => return Err(error.into()),
    }
    db.complete_membership_mutation(intent_hash)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?;
    EncryptionService::from_keyring_payload(plan.keyring_payload).map_err(|error| {
        MembershipOpsError::Invite(InviteError::Crypto(format!(
            "parse Serial rotated keyring: {error}"
        )))
    })
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
/// rotation that never committed — and both of its remedies converge without data
/// loss: a retry of this call (idempotent for an already-removed member) or the
/// device's next sync cycle. Either way, `pending_rotation` marks the committed
/// generation the moment the cloud rotation lands (see
/// [`apply_key_rotation`]), so this device seals nothing new for the cloud until
/// one of those remedies adopts it — the failed return here does not leave a
/// window where the store keeps producing content the removed member can still
/// decrypt.
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
    store_id: &str,
    current_encryption: &EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    db: &Database,
    serial: Option<SerialMembershipContext<'_>>,
) -> Result<String, MembershipOpsError> {
    if db.write_policy() == crate::WritePolicy::Serial {
        let serial =
            serial.ok_or(super::store_outbound::StoreOutboundError::MissingSerialCoordination)?;
        let rotated = remove_serial_member(
            storage,
            cloud_home,
            serial.coordination,
            &serial.device_id,
            user_keypair,
            hlc,
            public_key_hex,
            store_id,
            current_encryption,
            crate::encryption::generate_random_key(),
            db,
        )
        .await?;
        return apply_key_rotation(rotated, custody, cipher, pending_rotation)
            .map_err(|source| MembershipOpsError::RotationCommittedAdoptionFailed { source });
    }
    // Download existing membership entries and build the chain.
    let store_root_hash = required_store_root_hash(db).await?;
    let entry_keys = list_membership_entries(storage, store_root_hash).await?;

    let pinned_owner = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| MembershipOpsError::Database(error.to_string()))?
        .ok_or(MembershipOpsError::NoMembershipChain)?;
    let mut chain = load_anchored_chain(
        storage,
        store_root_hash,
        &entry_keys,
        Some(&pinned_owner),
        Some(db),
    )
    .await?;

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
        store_id,
        &revoke_ts,
        current_encryption,
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
    apply_key_rotation(new_key, custody, cipher, pending_rotation)
        .map_err(|source| MembershipOpsError::RotationCommittedAdoptionFailed { source })
}

/// Rotate the in-use encryption key after a member removal: persist it via
/// custody and swap the live cipher in place. Returns the new key's fingerprint
/// for the host to record in its own config — coven never writes the host's
/// config.
///
/// `pending_rotation` is marked with the committed generation before adoption is
/// even attempted, and cleared only once the swap below actually lands — so a
/// concurrent seal can never observe "adoption hasn't finished" as "nothing is
/// pending" in the gap between this function starting and either outcome. On
/// success the mark is redundant (the swap already makes the live cipher current)
/// but harmless; on failure it is what keeps every seal path refusing until one
/// of the two remedies (a retried removal, or the next sync cycle's own adoption)
/// clears it.
///
/// The key is persisted before the live cipher is swapped, so a persistence
/// failure leaves the cipher untouched on the superseded generation rather than
/// live on a key that never reached custody. The write is idempotent —
/// persisting the same keyring and swapping to it again converges — so a caller
/// that failed here can retry adoption alone.
///
/// A plaintext home has no store key to rotate, so this is an error there:
/// sharing (and hence member removal) requires an encrypted home.
pub fn apply_key_rotation(
    new_encryption: EncryptionService,
    custody: &dyn MasterKeyCustody,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
) -> Result<String, KeyError> {
    // Mark first, so a seal that clones the live cipher during the persist+swap
    // below refuses under the superseded generation until the swap lands.
    pending_rotation.mark_committed(new_encryption.current_generation());
    // Merge, never replace: the fixed-mode cipher state can extend an encrypted
    // keyring but has no transition to plaintext. Re-check under its keyring lock
    // because another member operation may have adopted a newer rotation while
    // this caller was reading the cloud.
    let new_fingerprint = cipher.merge_key_rotation(&new_encryption, custody)?;
    // Re-derive the pause from the live cipher: a merge (or an already-covered
    // stale apply) that now covers everything committed clears it; a strictly
    // newer generation still pending stays paused.
    pending_rotation.resolve(&cipher.snapshot());
    match new_fingerprint {
        Some(fingerprint) => Ok(fingerprint),
        None => Err(KeyError::StaleKeyRotation),
    }
}

/// Write a store's founder entry: chain entry #1, a self-signed Owner `Add` of
/// `owner`, uploaded to `membership/{owner_pubkey}/1`. Called once, when a created
/// store first connects its cloud, so every opaque store has an owner-anchored
/// chain from the start (issue #102). The caller is responsible for only invoking
/// this when no chain exists yet; this unconditionally writes seq 1.
#[cfg(test)]
pub(crate) async fn write_founder_entry(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    store_id: &str,
    owner: &UserKeypair,
    timestamp: &str,
) -> Result<(), String> {
    let entry = founder_entry(store_id, owner, timestamp);
    let coord = entry.coord();
    let mut chain = MembershipChain::new();
    chain
        .add_entry_at(coord.clone(), entry.clone())
        .map_err(|e| format!("Failed to validate founder entry: {e}"))?;
    append_membership_entry_object(storage, store_root_hash, &coord, &entry)
        .await
        .map_err(|e| format!("Failed to upload founder entry: {e}"))?;
    publish_membership_head(storage, store_root_hash, &chain, owner)
        .await
        .map_err(|e| format!("Failed to upload founder membership head: {e}"))?;
    info!("Wrote founder Owner entry for the store's membership chain");
    Ok(())
}

/// Download each membership entry, paired with the storage coordinate it was
/// loaded from. The pairing lets a caller name the exact entry that authorizes a
/// given pubkey (see [`super::membership::write_grant_coord`]);
/// [`download_chain`] drops the coordinates and builds the validated chain.
pub async fn download_entries(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
) -> Result<Vec<(MembershipCoord, MembershipEntry)>, String> {
    let mut entries = Vec::with_capacity(entry_keys.len());
    for coord in entry_keys {
        let data = read_membership_entry(storage, store_root_hash, coord)
            .await
            .map_err(|e| format!("Failed to get membership entry {coord:?}: {e}"))?;
        let entry: MembershipEntry = serde_json::from_slice(&data)
            .map_err(|error| format!("Failed to parse membership entry {coord:?}: {error}"))?;
        validate_membership_entry_at(coord, &entry)?;
        entries.push((coord.clone(), entry));
    }
    Ok(entries)
}

/// Parse a membership entry read from `coord` and bind its signed author to the
/// author namespace in that coordinate.
pub(crate) fn parse_membership_entry_at(
    coord: &MembershipCoord,
    data: &[u8],
) -> Result<MembershipEntry, String> {
    let entry: MembershipEntry = serde_json::from_slice(data).map_err(|error| {
        format!(
            "Failed to parse membership entry {}/{}: {error}",
            coord.author_pubkey, coord.seq
        )
    })?;
    validate_membership_entry_at(coord, &entry)?;
    Ok(entry)
}

fn validate_membership_entry_at(
    coord: &MembershipCoord,
    entry: &MembershipEntry,
) -> Result<(), String> {
    if entry.author_pubkey != coord.author_pubkey
        || entry.author_owner_grant != coord.author_owner_grant
        || entry.seq != coord.seq
    {
        return Err(format!(
            "membership entry {coord:?} declares stream {}/{}/{}",
            entry.author_pubkey, entry.author_owner_grant, entry.seq
        ));
    }
    let actual_hash = super::membership::entry_hash(entry);
    if actual_hash != coord.entry_hash {
        return Err(format!(
            "membership entry {coord:?} hashes to {actual_hash}"
        ));
    }
    Ok(())
}

/// Download and build a membership chain from the storage.
pub async fn download_chain(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
) -> Result<MembershipChain, String> {
    let raw_entries = download_entries(storage, store_root_hash, entry_keys).await?;

    MembershipChain::from_entries_with_coords(raw_entries)
        .map_err(|e| format!("Invalid membership chain: {e}"))
}

/// The seq of `author`'s committed head in storage, or `None` when the author has
/// never published one (a legitimate absence — membership seqs start at 1). A head
/// present but whose signature does not verify is tamper, not absence: it fails
/// loud rather than reading as `None` and being silently overwritten.
pub(crate) async fn committed_head_seq(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    author: &str,
    grant: &OwnerGrantId,
) -> Result<Option<u64>, String> {
    visible_membership_heads(storage, store_root_hash)
        .await
        .map(|heads| {
            heads
                .by_grant
                .get(grant)
                .and_then(|head| (head.author_pubkey == author).then_some(head.seq))
        })
        .map_err(|error| format!("Failed to read membership heads: {error}"))
}

/// Publish `signer`'s membership head, certifying its own committed prefix in
/// `chain`. The write is monotonic: it refuses to publish a head whose seq does
/// not advance the one already stored, so a device working from a stale view fails
/// loud (and retries on top of the observed head) instead of rolling the head back
/// over a peer's newer commit.
///
/// Two different valid heads at the same sequence occupy the same semantic slot
/// and are therefore an immutable fork that every reader rejects.
pub async fn publish_membership_head(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    chain: &MembershipChain,
    signer: &UserKeypair,
) -> Result<AuthorHead, String> {
    let head = chain
        .signed_head(signer)
        .ok_or_else(|| "cannot publish a head for an author with no entries".to_string())?;
    if let Some(stored_seq) = committed_head_seq(
        storage,
        store_root_hash,
        &head.author_pubkey,
        &head.author_owner_grant,
    )
    .await?
    {
        if stored_seq >= head.seq {
            return Err(format!(
                "stale membership head: {} already committed through seq {stored_seq}, \
                 refusing to publish seq {}",
                head.author_pubkey, head.seq
            ));
        }
    }
    append_membership_head_object(storage, store_root_hash, &head)
        .await
        .map_err(|e| format!("Failed to upload membership head: {e}"))?;
    Ok(head)
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

/// Download and validate the membership chain from `entry_keys`, then confirm it
/// is anchored to `owner_pubkey` when one is pinned. Returns the validated,
/// owner-anchored chain.
///
/// The shared load+anchor step the pull cycle and the snapshot authorization both
/// run: validation proves the chain is well-formed (signatures, owner-only
/// authorship), and the anchor proves it descends from the store's established
/// owner rather than a wiped-and-refounded chain under an attacker's key. A
/// caller with no pinned owner is performing a pre-initialization bootstrap read;
/// ordinary sync sessions establish the owner before they can run.
///
/// The committed set is the union of every known author's committed prefix. Known
/// authors come from both the current entry listing and this reader's persisted
/// head floors, so hiding an entire listed prefix cannot erase a head this reader
/// already accepted. For each author that has a signed head, entries
/// `1..=head.seq` are admitted (fetched by keyed GET, so a listing that lags a
/// fresh write can't hide a committed entry). `validate` then re-derives owner
/// authorship across the merged, timestamp-ordered set, so an author who was never
/// an owner has its entries rejected — the same fold authorization already
/// performs. A listed author with no persisted floor may have entries but no head;
/// those entries stay uncommitted until it publishes a head covering them. An
/// author with a persisted floor must retain a readable head at or above it.
///
/// `watermark_db`, when present, makes the read monotonic per author: a head whose
/// seq regresses the last one this reader accepted (persisted in `protocol_state`) is
/// refused, and each accepted head advances the watermark. This closes the window
/// where a stale head replica (or a same-author two-device overwrite) would rewind
/// a reader's committed view.
pub(crate) async fn load_anchored_chain(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
    owner_pubkey: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<MembershipChain, AnchoredChainError> {
    load_anchored_chain_if_known(
        storage,
        store_root_hash,
        entry_keys,
        owner_pubkey,
        watermark_db,
    )
    .await?
    .ok_or_else(|| AnchoredChainError::LoadFailed("no membership authors are known".to_string()))
}

/// The central membership loader for callers that distinguish an absent
/// pre-initialization chain from a required chain. `entry_keys` is the caller's
/// one already-fetched LIST result; persisted floors can supply authors it omits.
pub(crate) async fn load_anchored_chain_if_known(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
    owner_pubkey: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<Option<MembershipChain>, AnchoredChainError> {
    Ok(load_anchored_chain_if_known_with_proof(
        storage,
        store_root_hash,
        entry_keys,
        owner_pubkey,
        watermark_db,
    )
    .await?
    .chain)
}

/// Load an owner-anchored membership chain against the exact signed-head floor
/// carried by a join or restore code, without requiring a local database first.
/// This is the bootstrap trust path: the floor constrains membership before a
/// wrapped key or snapshot is accepted, and the opened database persists the
/// same coordinates afterward for every later load.
pub(crate) async fn load_anchored_chain_at_floor(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
    owner_pubkey: &str,
    floor: &[MembershipCoord],
) -> Result<MembershipChain, AnchoredChainError> {
    let floors = membership_floor_by_grant(floor).map_err(AnchoredChainError::LoadFailed)?;
    let loaded = validate_anchored_chain(
        storage,
        store_root_hash,
        entry_keys,
        Some(owner_pubkey),
        &floors,
    )
    .await?;
    loaded
        .validated
        .map(|validated| validated.chain)
        .ok_or_else(|| {
            AnchoredChainError::LoadFailed(format!(
                "membership chain has no committed heads but owner {owner_pubkey} is pinned"
            ))
        })
}

pub(crate) fn membership_floor_by_grant(
    floor: &[MembershipCoord],
) -> Result<BTreeMap<OwnerGrantId, MembershipCoord>, String> {
    if floor.is_empty() {
        return Err("membership floor is empty".to_string());
    }
    let mut floors = BTreeMap::new();
    for coord in floor {
        if coord.seq == 0 {
            return Err(format!(
                "membership floor contains seq zero for {}",
                coord.author_pubkey
            ));
        }
        let author = hex::decode(&coord.author_pubkey).map_err(|error| {
            format!(
                "membership floor author {} is not hex: {error}",
                coord.author_pubkey
            )
        })?;
        if author.len() != crate::keys::SIGN_PUBLICKEYBYTES {
            return Err(format!(
                "membership floor author {} must be {} bytes, got {}",
                coord.author_pubkey,
                crate::keys::SIGN_PUBLICKEYBYTES,
                author.len()
            ));
        }
        if let Some(existing) = floors.insert(coord.author_owner_grant.clone(), coord.clone()) {
            return Err(format!(
                "membership floor repeats grant {} at {:?} and {:?}",
                coord.author_owner_grant, existing, coord
            ));
        }
    }
    Ok(floors)
}

pub(crate) struct AnchoredChainLoad {
    pub chain: Option<MembershipChain>,
    pub head_coverage: ListingCoverage,
}

pub(crate) async fn load_anchored_chain_if_known_with_proof(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
    owner_pubkey: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<AnchoredChainLoad, AnchoredChainError> {
    let _membership_load = match watermark_db {
        Some(db) => Some(db.lock_membership_load().await),
        None => None,
    };
    let persisted_floors = match watermark_db {
        Some(db) => read_head_watermarks(db)
            .await
            .map_err(AnchoredChainError::LoadFailed)?,
        None => BTreeMap::new(),
    };
    let mut loaded = validate_anchored_chain(
        storage,
        store_root_hash,
        entry_keys,
        owner_pubkey,
        &persisted_floors,
    )
    .await?;
    if let (Some(owner), None) = (owner_pubkey, loaded.validated.as_ref()) {
        return Err(AnchoredChainError::LoadFailed(format!(
            "membership chain has no committed heads but owner {owner} is pinned"
        )));
    }
    if let (Some(db), Some(validated)) = (watermark_db, loaded.validated.as_ref()) {
        persist_head_watermarks(db, &validated.head_floor)
            .await
            .map_err(AnchoredChainError::LoadFailed)?;
    }
    Ok(AnchoredChainLoad {
        chain: loaded.validated.take().map(|validated| validated.chain),
        head_coverage: loaded.head_coverage,
    })
}

/// Load the same signed, watermarked, owner-anchored committed chain as
/// [`load_anchored_chain_if_known`], while requiring the signed head for every
/// author named by `candidate_coords` to participate in discovery. A changeset's
/// membership grant uses this path when LIST has not exposed that author's
/// prefix yet. The coordinate itself is never appended: only the author's
/// verified head determines the committed prefix, and the caller checks that the
/// resulting chain's effective grant equals the named coordinate.
pub(crate) async fn load_anchored_chain_with_candidates(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
    candidate_coords: &[MembershipCoord],
    owner_pubkey: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<Option<MembershipChain>, AnchoredChainError> {
    let mut augmented = entry_keys.to_vec();
    augmented.extend_from_slice(candidate_coords);
    load_anchored_chain_if_known(
        storage,
        store_root_hash,
        &augmented,
        owner_pubkey,
        watermark_db,
    )
    .await
}

struct ValidatedAnchoredChain {
    chain: MembershipChain,
    head_floor: Vec<MembershipCoord>,
}

struct ValidatedAnchoredLoad {
    validated: Option<ValidatedAnchoredChain>,
    head_coverage: ListingCoverage,
}

async fn validate_anchored_chain(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
    owner_pubkey: Option<&str>,
    persisted_floors: &BTreeMap<OwnerGrantId, MembershipCoord>,
) -> Result<ValidatedAnchoredLoad, AnchoredChainError> {
    let mut streams = BTreeMap::<OwnerGrantId, String>::new();
    for coord in entry_keys.iter().chain(persisted_floors.values()) {
        if let Some(existing) = streams.insert(
            coord.author_owner_grant.clone(),
            coord.author_pubkey.clone(),
        ) {
            if existing != coord.author_pubkey {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership grant {} is claimed by both {existing} and {}",
                    coord.author_owner_grant, coord.author_pubkey
                )));
            }
        }
    }
    let visible = visible_membership_heads(storage, store_root_hash)
        .await
        .map_err(map_membership_object_error)?;
    let head_coverage = visible.coverage;
    let mut visible_heads = visible.by_grant;
    for head in visible_heads.values() {
        streams
            .entry(head.author_owner_grant.clone())
            .or_insert_with(|| head.author_pubkey.clone());
    }
    if streams.is_empty() {
        return Ok(ValidatedAnchoredLoad {
            validated: None,
            head_coverage,
        });
    }
    let mut requested = BTreeMap::<OwnerGrantId, (String, u64)>::new();
    for coord in entry_keys.iter().chain(persisted_floors.values()) {
        requested
            .entry(coord.author_owner_grant.clone())
            .and_modify(|(_, current)| *current = (*current).max(coord.seq))
            .or_insert_with(|| (coord.author_pubkey.clone(), coord.seq));
    }
    for (grant, (author, seq)) in requested {
        if let Some(exact) =
            load_membership_head_slot(storage, store_root_hash, &author, &grant, seq)
                .await
                .map_err(map_membership_object_error)?
        {
            let head = exact.value;
            if visible_heads
                .get(&grant)
                .is_none_or(|current| current.seq < head.seq)
            {
                visible_heads.insert(grant, head);
            }
        }
    }

    let mut heads: Vec<AuthorHead> = Vec::new();
    for (grant, author) in &streams {
        let Some(head) = visible_heads.remove(grant) else {
            if let Some(accepted) = persisted_floors.get(grant) {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {author}/{grant} is missing below the accepted floor {}",
                    accepted.seq,
                )));
            }
            debug!(%author, %grant, "membership head absent; stream entries are uncommitted");
            continue;
        };
        if head.author_pubkey != *author || head.author_owner_grant != *grant {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head for {author}/{grant} declares {}/{}",
                head.author_pubkey, head.author_owner_grant
            )));
        }
        if let Some(accepted) = persisted_floors.get(grant) {
            if head.seq < accepted.seq {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {author}/{grant} regressed to seq {} below the accepted {}",
                    head.seq, accepted.seq,
                )));
            }
        }
        heads.push(head);
    }

    if heads.is_empty() {
        return Ok(ValidatedAnchoredLoad {
            validated: None,
            head_coverage,
        });
    }

    let chain = download_committed_chain(storage, store_root_hash, &heads).await?;

    // Each head must match the prefix it certifies: same tip seq, same tip hash.
    for head in &heads {
        match chain.raw_stream_tip(&head.author_pubkey, &head.author_owner_grant) {
            Some(coord) if coord.seq == head.seq && coord.entry_hash == head.tip_hash => {}
            other => {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {}/{} claims seq {}/{} but the chain tip is {other:?}",
                    head.author_pubkey, head.author_owner_grant, head.seq, head.tip_hash
                )))
            }
        }
    }

    for accepted in persisted_floors.values() {
        if !chain.contains_coord(accepted) {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership chain does not descend from accepted coordinate {:?}",
                accepted,
            )));
        }
    }

    if let Some(owner) = owner_pubkey {
        if !chain.is_founded_by(owner) {
            return Err(AnchoredChainError::FounderMismatch {
                founder: chain.founder_pubkey().map(str::to_string),
                owner: owner.to_string(),
            });
        }
    }

    let store_id = chain.store_id().ok_or_else(|| {
        AnchoredChainError::LoadFailed("membership chain has no store id".to_string())
    })?;
    let founder_coord = chain.founder_coord().ok_or_else(|| {
        AnchoredChainError::LoadFailed("membership chain has no founder root".to_string())
    })?;
    let store_protocol_root = super::store_objects::load_pinned_store_protocol_root(
        storage,
        store_root_hash,
        store_id,
        &founder_coord.author_pubkey,
    )
    .await
    .map_err(map_membership_object_error)?
    .ok_or_else(|| {
        AnchoredChainError::LoadFailed(format!("Store protocol root {store_root_hash} is absent"))
    })?;
    if store_protocol_root.value.founder.coord() != *founder_coord {
        return Err(AnchoredChainError::LoadFailed(format!(
            "membership founder {founder_coord:?} differs from Store protocol root founder {:?}",
            store_protocol_root.value.founder.coord()
        )));
    }

    let head_floor = chain.author_heads();
    Ok(ValidatedAnchoredLoad {
        validated: Some(ValidatedAnchoredChain { chain, head_floor }),
        head_coverage,
    })
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

/// Download the complete prefixes certified by the accepted signed heads. Cloud
/// availability failures remain typed for pull retry; absent, undecryptable, or
/// malformed committed objects are invalid membership content and cannot hold a
/// position indefinitely.
async fn download_committed_chain(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    heads: &[AuthorHead],
) -> Result<MembershipChain, AnchoredChainError> {
    let capacity = heads.iter().map(|head| head.seq as usize).sum();
    let mut entries = Vec::with_capacity(capacity);
    for head in heads {
        for seq in 1..=head.seq {
            let loaded = load_membership_entry_slot(
                storage,
                store_root_hash,
                &head.author_pubkey,
                &head.author_owner_grant,
                seq,
            )
            .await
            .map_err(map_membership_object_error)?
            .ok_or_else(|| {
                AnchoredChainError::LoadFailed(format!(
                    "membership entry {}/{}/{} is missing",
                    head.author_pubkey, head.author_owner_grant, seq
                ))
            })?;
            let coord = MembershipCoord {
                author_pubkey: head.author_pubkey.clone(),
                author_owner_grant: head.author_owner_grant.clone(),
                seq,
                entry_hash: loaded.semantic_hash,
            };
            validate_membership_entry_at(&coord, &loaded.value)
                .map_err(AnchoredChainError::LoadFailed)?;
            entries.push((coord, loaded.value));
        }
    }

    MembershipChain::from_entries_with_coords(entries).map_err(|error| {
        AnchoredChainError::LoadFailed(format!("Invalid membership chain: {error}"))
    })
}

/// Validate the signed committed membership state against `owner_pubkey`, then
/// record the owner and every accepted head floor in one local transaction.
/// `None` means no signed head commits any listed prefix; no trust state is
/// persisted in that case.
pub(crate) async fn load_and_persist_owner_anchor(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry_keys: &[MembershipCoord],
    owner_pubkey: &str,
    db: &Database,
) -> Result<Option<MembershipChain>, AnchoredChainError> {
    let _membership_load = db.lock_membership_load().await;
    let persisted_floors = read_head_watermarks(db)
        .await
        .map_err(AnchoredChainError::LoadFailed)?;
    let loaded = validate_anchored_chain(
        storage,
        store_root_hash,
        entry_keys,
        Some(owner_pubkey),
        &persisted_floors,
    )
    .await?;
    if let Some(loaded) = loaded.validated {
        persist_owner_and_head_watermarks(db, owner_pubkey, &loaded.head_floor)
            .await
            .map_err(AnchoredChainError::LoadFailed)?;
        Ok(Some(loaded.chain))
    } else {
        Ok(None)
    }
}

/// `protocol_state` key holding the greatest membership-head seq this reader has
/// accepted from `author`. The read path refuses any later head that regresses it.
fn head_watermark_key(grant: &str) -> String {
    format!("membership_head_seq/{grant}")
}

async fn read_head_watermarks(
    db: &Database,
) -> Result<BTreeMap<OwnerGrantId, MembershipCoord>, String> {
    let prefix = head_watermark_key("");
    db.call(move |conn| {
        let mut statement = conn
            .prepare(
                "SELECT key, value FROM protocol_state \
                 WHERE substr(key, 1, length(?1)) = ?1 \
                 ORDER BY key",
            )
            .map_err(crate::database::DbError::from)?;
        let rows = statement
            .query_map([&prefix], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(crate::database::DbError::from)?;
        let mut watermarks = BTreeMap::new();
        for row in rows {
            let (key, value) = row.map_err(crate::database::DbError::from)?;
            let grant = key.strip_prefix(&prefix).ok_or_else(|| {
                crate::database::DbError::Message(format!(
                    "membership head watermark key {key:?} is outside its queried prefix"
                ))
            })?;
            if grant.is_empty() {
                return Err(crate::database::DbError::Message(
                    "membership head watermark has an empty grant".to_string(),
                ));
            }
            let grant = OwnerGrantId(grant.parse().map_err(|error| {
                crate::database::DbError::Message(format!(
                    "membership head watermark grant is malformed: {error}"
                ))
            })?);
            let coord: MembershipCoord = serde_json::from_str(&value).map_err(|error| {
                crate::database::DbError::Message(format!(
                    "membership head watermark for {grant} is malformed: {error}"
                ))
            })?;
            if coord.author_owner_grant != grant || coord.seq == 0 {
                return Err(crate::database::DbError::Message(format!(
                    "membership head watermark for {grant} has an invalid exact coordinate"
                )));
            }
            watermarks.insert(grant, coord);
        }
        Ok(watermarks)
    })
    .await
    .map_err(|error| format!("read membership head watermarks: {error}"))
}

/// Persist `seq` as `author`'s head watermark, monotonically: a write at or
/// below the stored value leaves it untouched. The regression *check* runs
/// against a read taken earlier, so two concurrent watermark-writing loads (the
/// cycle's membership load and a user-triggered restore-code floor computation)
/// can both pass it and then persist out of order — a plain overwrite would let
/// the lower value land last, and a provider replaying the pre-removal head
/// below it would then be accepted, the exact rollback the watermark exists to
/// refuse. The values are unpadded decimal strings, so the guard compares
/// numerically, never lexically.
fn upsert_head_watermark_on(
    conn: &rusqlite::Connection,
    coord: &MembershipCoord,
) -> Result<(), crate::database::DbError> {
    let key = head_watermark_key(&coord.author_owner_grant.to_string());
    let existing: Option<String> = conn
        .query_row(
            "SELECT value FROM protocol_state WHERE key = ?1",
            [&key],
            |row| row.get(0),
        )
        .optional()
        .map_err(crate::database::DbError::from)?;
    if let Some(existing) = existing {
        let existing: MembershipCoord = serde_json::from_str(&existing).map_err(|error| {
            crate::database::DbError::Message(format!(
                "membership head watermark for {} is malformed: {error}",
                coord.author_pubkey
            ))
        })?;
        if existing.seq > coord.seq {
            return Ok(());
        }
        if existing.seq == coord.seq {
            if existing == *coord {
                return Ok(());
            }
            return Err(crate::database::DbError::Message(format!(
                "membership head watermark for {} forks at seq {}: {} versus {}",
                coord.author_pubkey, coord.seq, existing.entry_hash, coord.entry_hash
            )));
        }
    }
    let value = serde_json::to_string(coord).map_err(|error| {
        crate::database::DbError::Message(format!("serialize membership head watermark: {error}"))
    })?;
    conn.execute(
        "INSERT INTO protocol_state (key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )
    .map(|_| ())
    .map_err(crate::database::DbError::from)
}

async fn persist_head_watermarks(db: &Database, floor: &[MembershipCoord]) -> Result<(), String> {
    let floor = floor.to_vec();
    db.call(move |conn| {
        let tx = conn
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        for coord in &floor {
            upsert_head_watermark_on(&tx, coord)?;
        }
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await
    .map_err(|error| format!("persist membership head watermarks: {error}"))
}

async fn persist_owner_and_head_watermarks(
    db: &Database,
    owner_pubkey: &str,
    floor: &[MembershipCoord],
) -> Result<(), String> {
    let owner_pubkey = owner_pubkey.to_string();
    let floor = floor.to_vec();
    db.call(move |conn| {
        let tx = conn
            .unchecked_transaction()
            .map_err(crate::database::DbError::from)?;
        let pinned: Option<String> = tx
            .query_row(
                "SELECT value FROM protocol_state WHERE key = ?1",
                [OWNER_PUBKEY_STATE_KEY],
                |row| row.get(0),
            )
            .optional()
            .map_err(crate::database::DbError::from)?;
        if let Some(pinned) = pinned {
            if pinned != owner_pubkey {
                return Err(crate::database::DbError::Message(format!(
                    "owner {pinned} is already pinned; refusing to replace it with {owner_pubkey}"
                )));
            }
        } else {
            tx.execute(
                "INSERT INTO protocol_state (key, value) VALUES (?1, ?2)",
                rusqlite::params![OWNER_PUBKEY_STATE_KEY, owner_pubkey],
            )
            .map_err(crate::database::DbError::from)?;
        }
        for coord in &floor {
            upsert_head_watermark_on(&tx, coord)?;
        }
        tx.commit().map_err(crate::database::DbError::from)
    })
    .await
    .map_err(|error| format!("persist owner and membership head watermarks: {error}"))
}

/// Seed this reader's per-author membership-head watermark from `floor` — the
/// `(author_pubkey, seq)` pairs an invite or restore code carries from mint time
/// ([`current_membership_floor`]). Persists through the exact `protocol_state`
/// entries [`load_anchored_chain`]'s monotonic guard reads, so from this
/// device's first sync cycle on, any head at or below the seeded floor is
/// refused as a regression — exactly as if this reader had already accepted it.
///
/// Called once, before a join or restore's first sync cycle, on a `db` whose
/// `protocol_state` has no membership watermark yet; the persist is monotonic
/// regardless, so a seed can never lower a watermark either.
pub async fn seed_head_watermark(
    db: &Database,
    floor: &[super::membership::MembershipCoord],
) -> Result<(), String> {
    persist_head_watermarks(db, floor).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{
        BlobBody, BoxPartSink, CloudAccessOutcome, CloudAccessState, CloudHeadCreateError,
        CloudHeadReplaceError, CloudHeadStorage, CloudHeadVersion, CloudHome, CloudHomeError,
        CloudHomeJoinInfo, CloudVersionedHead, RevokeOutcome, SequentialCopyIdGenerator,
        UploadProgress,
    };
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::hlc::Hlc;
    use crate::sync::membership::founder_entry;
    use crate::sync::test_helpers::{
        append_membership_entry, pubkey_hex, MockSyncStorage, TestCustody,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, RwLock};

    async fn download_entries(
        storage: &MockSyncStorage,
        entry_keys: &[MembershipCoord],
    ) -> Result<Vec<(MembershipCoord, MembershipEntry)>, String> {
        super::download_entries(storage, storage.store_root_hash(), entry_keys).await
    }

    async fn publish_membership_head(
        storage: &MockSyncStorage,
        chain: &MembershipChain,
        signer: &UserKeypair,
    ) -> Result<AuthorHead, String> {
        super::publish_membership_head(storage, storage.store_root_hash(), chain, signer).await
    }

    async fn load_anchored_chain(
        storage: &MockSyncStorage,
        entry_keys: &[MembershipCoord],
        owner_pubkey: Option<&str>,
        watermark_db: Option<&Database>,
    ) -> Result<MembershipChain, AnchoredChainError> {
        super::load_anchored_chain(
            storage,
            storage.store_root_hash(),
            entry_keys,
            owner_pubkey,
            watermark_db,
        )
        .await
    }

    async fn load_and_persist_owner_anchor(
        storage: &MockSyncStorage,
        entry_keys: &[MembershipCoord],
        owner_pubkey: &str,
        db: &Database,
    ) -> Result<Option<MembershipChain>, AnchoredChainError> {
        super::load_and_persist_owner_anchor(
            storage,
            storage.store_root_hash(),
            entry_keys,
            owner_pubkey,
            db,
        )
        .await
    }

    async fn current_membership_floor(
        storage: &MockSyncStorage,
        pinned_owner: Option<&str>,
        watermark_db: Option<&Database>,
    ) -> Result<Vec<MembershipCoord>, MembershipOpsError> {
        super::current_membership_floor(
            storage,
            storage.store_root_hash(),
            pinned_owner,
            watermark_db,
        )
        .await
    }

    #[derive(Clone)]
    struct SerialMutationHome {
        inner: InMemoryCloudHome,
        present: Arc<Mutex<std::collections::BTreeSet<String>>>,
        fail_next_wrapped_write: Arc<AtomicBool>,
    }

    impl SerialMutationHome {
        fn new() -> Self {
            Self {
                inner: InMemoryCloudHome::new(),
                present: Arc::new(Mutex::new(std::collections::BTreeSet::new())),
                fail_next_wrapped_write: Arc::new(AtomicBool::new(false)),
            }
        }

        fn fail_next_wrapped_write(&self) {
            self.fail_next_wrapped_write.store(true, Ordering::SeqCst);
        }

        fn has_access(&self, member: &str) -> bool {
            self.present.lock().unwrap().contains(member)
        }
    }

    #[async_trait]
    impl CloudHeadStorage for SerialMutationHome {
        async fn read_head(&self, key: &str) -> Result<CloudVersionedHead, CloudHomeError> {
            self.inner.read_head(key).await
        }

        async fn create_head(
            &self,
            key: &str,
            bytes: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadCreateError> {
            self.inner.create_head(key, bytes).await
        }

        async fn replace_head(
            &self,
            key: &str,
            expected: &CloudHeadVersion,
            bytes: Vec<u8>,
        ) -> Result<CloudVersionedHead, CloudHeadReplaceError> {
            self.inner.replace_head(key, expected, bytes).await
        }

        async fn delete_probe_head(&self, key: &str) -> Result<(), CloudHomeError> {
            self.inner.delete_probe_head(key).await
        }
    }

    #[async_trait]
    impl CloudHome for SerialMutationHome {
        async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), CloudHomeError> {
            if key.starts_with("keys/")
                && self.fail_next_wrapped_write.swap(false, Ordering::SeqCst)
            {
                return Err(CloudHomeError::Configuration(
                    "injected definite wrapped-key write failure".to_string(),
                ));
            }
            self.inner.put_object(key, data).await
        }

        async fn open_multipart<'a>(
            &'a self,
            key: &str,
            total_len: u64,
        ) -> Result<BoxPartSink<'a>, CloudHomeError> {
            self.inner.open_multipart(key, total_len).await
        }

        fn multipart_threshold(&self) -> u64 {
            self.inner.multipart_threshold()
        }

        async fn append_object(
            &self,
            key: &str,
            body: BlobBody,
            progress: &UploadProgress<'_>,
        ) -> Result<crate::storage::cloud::AppendedObject, CloudHomeError> {
            self.inner.append_object(key, body, progress).await
        }

        async fn list_appended(
            &self,
            prefix: &str,
        ) -> Result<crate::storage::cloud::AppendedListing, CloudHomeError> {
            self.inner.list_appended(prefix).await
        }

        async fn read_appended(
            &self,
            object: &crate::storage::cloud::AppendedObject,
        ) -> Result<Vec<u8>, CloudHomeError> {
            self.inner.read_appended(object).await
        }

        async fn delete_appended(
            &self,
            object: &crate::storage::cloud::AppendedObject,
        ) -> Result<(), CloudHomeError> {
            self.inner.delete_appended(object).await
        }

        async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError> {
            self.inner.read(key).await
        }

        async fn read_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            self.inner.read_range(key, start, end).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), CloudHomeError> {
            self.inner.delete(key).await
        }

        async fn exists(&self, key: &str) -> Result<bool, CloudHomeError> {
            self.inner.exists(key).await
        }

        async fn set_access(
            &self,
            desired: CloudAccessState,
        ) -> Result<CloudAccessOutcome, CloudHomeError> {
            match desired {
                CloudAccessState::Present { member_pubkey, .. } => {
                    self.present.lock().unwrap().insert(member_pubkey);
                    Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                        bucket: "test-bucket".to_string(),
                        region: "us-east-1".to_string(),
                        endpoint: None,
                        access_key: "test-access".to_string(),
                        secret_key: "test-secret".to_string(),
                        key_prefix: None,
                    }))
                }
                CloudAccessState::Absent { member_pubkey, .. } => {
                    self.present.lock().unwrap().remove(&member_pubkey);
                    Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
                }
            }
        }
    }

    async fn anchored_db(storage: &MockSyncStorage, founder_pubkey: &str) -> Database {
        let db = crate::sync::test_helpers::open_test_db();
        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &storage.store_root_hash().to_string(),
        )
        .await
        .expect("bind membership fixture to its Store protocol root");
        let entries = storage.discover_membership_entries().await;
        load_and_persist_owner_anchor(storage, &entries, founder_pubkey, &db)
            .await
            .expect("anchor test membership")
            .expect("test membership exists");
        db
    }

    async fn append_alternative_store_protocol_root(
        storage: &MockSyncStorage,
        founder: &UserKeypair,
    ) -> ObjectHash {
        let pinned = storage.store_protocol_root();
        let alternative = super::super::store_commit::StoreProtocolRoot::signed(
            pinned.store_id,
            pinned.founder,
            pinned.schema_version + 1,
            pinned.sync_routing_hash,
            pinned.write_policy,
            founder,
        )
        .expect("sign alternative Store protocol root");
        let hash = alternative.object_hash();
        storage
            .append_protocol_object(
                &super::super::storage::ProtocolObjectContext::store(
                    hash,
                    super::super::storage::ProtocolObjectDomain::StoreProtocolRoot,
                ),
                &super::super::store_commit::store_protocol_root_semantic_prefix(hash),
                ".json",
                alternative.to_bytes(),
            )
            .await
            .expect("append alternative Store protocol root");
        hash
    }

    #[tokio::test]
    async fn anchored_chain_loads_the_root_named_by_its_authoritative_hash() {
        let founder = UserKeypair::generate();
        let storage = MockSyncStorage::with_store_and_keypair("pinned-root", founder.clone());
        let founder_pubkey = pubkey_hex(&founder);
        let chain = storage.publish_protocol_founder_membership().await;
        let pinned_hash = storage.store_root_hash();
        let alternative_hash = append_alternative_store_protocol_root(&storage, &founder).await;
        assert_ne!(alternative_hash, pinned_hash);
        let entries = storage.discover_membership_entries().await;

        let loaded = load_anchored_chain(&storage, &entries, Some(&founder_pubkey), None)
            .await
            .expect("the authoritative root hash selects the pinned root");

        assert_eq!(loaded.founder_coord(), chain.founder_coord());
    }

    async fn serial_mutation_fixture(
        name: &str,
    ) -> (
        SerialMutationHome,
        CloudSyncStorage,
        Database,
        UserKeypair,
        EncryptionService,
    ) {
        let home = SerialMutationHome::new();
        let owner = UserKeypair::generate();
        let storage = CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Encrypted(EncryptionService::from_key([7_u8; 32])),
            BlobPathScheme::Hashed,
            name,
            owner.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(name)))
        .with_test_serial_coordination(Arc::new(home.clone()));
        let db = crate::sync::test_helpers::open_serial_test_db();
        crate::sync::test_helpers::publish_test_serial_store_protocol_root(
            &db,
            &storage,
            name,
            "owner-device",
            &owner,
        )
        .await;
        (
            home,
            storage,
            db,
            owner,
            EncryptionService::from_key([7_u8; 32]),
        )
    }

    #[tokio::test]
    async fn definite_serial_invite_wrap_failure_rolls_access_back_and_clears_the_intent() {
        let (home, storage, db, owner, encryption) =
            serial_mutation_fixture("serial-invite-definite-rollback").await;
        let invitee = UserKeypair::generate();
        let invitee_pubkey = pubkey_hex(&invitee);
        home.fail_next_wrapped_write();

        let result = invite_serial_member(
            &storage,
            &home,
            storage.serial_coordination().unwrap(),
            "owner-device",
            &owner,
            &Hlc::new("owner-device".to_string()),
            &invitee_pubkey,
            None,
            MemberRole::Member,
            &encryption,
            "serial-invite-definite-rollback",
            "Serial invite rollback",
            &db,
        )
        .await;

        assert!(result.is_err());
        assert!(!home.has_access(&invitee_pubkey));
        assert!(db.outbound_membership_mutation().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn definite_serial_removal_wrap_failure_restores_access_and_clears_the_intent() {
        let (home, storage, db, owner, encryption) =
            serial_mutation_fixture("serial-removal-definite-rollback").await;
        let member = UserKeypair::generate();
        let member_pubkey = pubkey_hex(&member);
        invite_serial_member(
            &storage,
            &home,
            storage.serial_coordination().unwrap(),
            "owner-device",
            &owner,
            &Hlc::new("owner-device".to_string()),
            &member_pubkey,
            None,
            MemberRole::Member,
            &encryption,
            "serial-removal-definite-rollback",
            "Serial removal rollback",
            &db,
        )
        .await
        .expect("establish member before removal");
        assert!(home.has_access(&member_pubkey));
        home.fail_next_wrapped_write();

        let result = remove_serial_member(
            &storage,
            &home,
            storage.serial_coordination().unwrap(),
            "owner-device",
            &owner,
            &Hlc::new("owner-device".to_string()),
            &member_pubkey,
            "serial-removal-definite-rollback",
            &encryption,
            [9_u8; 32],
            &db,
        )
        .await;

        assert!(result.is_err());
        assert!(home.has_access(&member_pubkey));
        assert!(db.outbound_membership_mutation().await.unwrap().is_none());
    }

    struct CommittedRemoval {
        storage: MockSyncStorage,
        db: Database,
        founder: UserKeypair,
        founder_pubkey: String,
        second_owner_pubkey: String,
        removed_member_pubkey: String,
    }

    impl CommittedRemoval {
        fn hide_all_entries_from_listing(&self) {
            for seq in 1..=3 {
                self.storage
                    .hide_membership_from_listing(&self.founder_pubkey, seq);
            }
            self.storage
                .hide_membership_from_listing(&self.second_owner_pubkey, 1);
        }
    }

    async fn committed_removal_by_second_owner() -> CommittedRemoval {
        use crate::sync::test_helpers::open_test_db;

        let founder = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(founder.clone());
        let db = open_test_db();
        let founder_pubkey = pubkey_hex(&founder);
        let second_owner_pubkey = pubkey_hex(&second_owner);
        let removed_member_pubkey = pubkey_hex(&member);
        let mut chain = MembershipChain::new();

        let founder_entry = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &founder_pubkey, 1, founder_entry).await;
        let add_owner = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&second_owner),
                None,
                MemberRole::Owner,
                "0000000002000-0000-founder".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &founder_pubkey, 2, add_owner).await;
        let add_member = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000003000-0000-founder".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &founder_pubkey, 3, add_member).await;
        let remove_member = chain
            .signed_remove_member(
                &second_owner,
                pubkey_hex(&member),
                "0000000004000-0000-second".to_string(),
            )
            .expect("active Owner removes membership grant");
        append_membership_entry(&storage, &mut chain, &second_owner_pubkey, 1, remove_member).await;
        publish_membership_head(&storage, &chain, &founder)
            .await
            .unwrap();
        publish_membership_head(&storage, &chain, &second_owner)
            .await
            .unwrap();

        let visible = storage.discover_membership_entries().await;
        let accepted = load_anchored_chain(&storage, &visible, Some(&founder_pubkey), Some(&db))
            .await
            .expect("accept committed multi-owner chain");
        assert!(!accepted.can_write_now(&removed_member_pubkey));

        CommittedRemoval {
            storage,
            db,
            founder,
            founder_pubkey,
            second_owner_pubkey,
            removed_member_pubkey,
        }
    }

    async fn pin_fixture_anchor(
        fixture: &CommittedRemoval,
    ) -> (Option<String>, BTreeMap<OwnerGrantId, MembershipCoord>) {
        let visible = fixture.storage.discover_membership_entries().await;
        load_and_persist_owner_anchor(
            &fixture.storage,
            &visible,
            &fixture.founder_pubkey,
            &fixture.db,
        )
        .await
        .expect("pin valid fixture anchor")
        .expect("fixture has committed membership");
        (
            fixture
                .db
                .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
                .await
                .unwrap(),
            read_head_watermarks(&fixture.db).await.unwrap(),
        )
    }

    async fn assert_fixture_trust_state(
        fixture: &CommittedRemoval,
        expected_owner: &Option<String>,
        expected_floors: &BTreeMap<OwnerGrantId, MembershipCoord>,
    ) {
        assert_eq!(
            &fixture
                .db
                .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
                .await
                .unwrap(),
            expected_owner,
        );
        assert_eq!(
            &read_head_watermarks(&fixture.db).await.unwrap(),
            expected_floors,
        );
    }

    #[tokio::test]
    async fn persisted_author_floor_recovers_head_when_listing_omits_author() {
        let fixture = committed_removal_by_second_owner().await;
        fixture
            .storage
            .hide_membership_from_listing(&fixture.second_owner_pubkey, 1);
        let visible = fixture.storage.discover_membership_entries().await;

        let loaded = load_anchored_chain(
            &fixture.storage,
            &visible,
            Some(&fixture.founder_pubkey),
            Some(&fixture.db),
        )
        .await
        .expect("persisted author floor requires its committed prefix");

        assert!(
            !loaded.can_write_now(&fixture.removed_member_pubkey),
            "hiding the removing Owner's listing prefix must not reactivate the member",
        );
    }

    #[tokio::test]
    async fn current_floor_recovers_every_author_when_listing_is_empty() {
        let fixture = committed_removal_by_second_owner().await;
        fixture.hide_all_entries_from_listing();

        let floor = current_membership_floor(
            &fixture.storage,
            Some(&fixture.founder_pubkey),
            Some(&fixture.db),
        )
        .await
        .expect("persisted floors recover an entirely omitted membership listing");

        assert_eq!(floor.len(), 2);
        assert!(floor
            .iter()
            .any(|coord| { coord.author_pubkey == fixture.founder_pubkey && coord.seq == 3 }));
        assert!(floor
            .iter()
            .any(|coord| { coord.author_pubkey == fixture.second_owner_pubkey && coord.seq == 1 }));
    }

    #[tokio::test]
    async fn current_floor_requires_every_keyed_entry_when_listing_is_empty() {
        let fixture = committed_removal_by_second_owner().await;
        fixture.hide_all_entries_from_listing();
        fixture
            .storage
            .remove_membership_entry(&fixture.second_owner_pubkey, 1);

        let error = current_membership_floor(
            &fixture.storage,
            Some(&fixture.founder_pubkey),
            Some(&fixture.db),
        )
        .await
        .expect_err("a persisted head requires every entry in its committed prefix");

        let message = error.to_string();
        assert!(message.contains(&fixture.second_owner_pubkey), "{message}");
        assert!(message.contains("/1 is missing"), "{message}");
    }

    #[tokio::test]
    async fn concurrent_membership_loads_complete_in_floor_order() {
        use crate::sync::test_helpers::open_test_db;

        let founder = UserKeypair::generate();
        let member = UserKeypair::generate();
        let founder_pubkey = pubkey_hex(&founder);
        let member_pubkey = pubkey_hex(&member);
        let storage = Arc::new(MockSyncStorage::with_keypair(founder.clone()));
        let db = open_test_db();
        let mut chain = MembershipChain::new();

        let first = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &founder_pubkey, 1, first).await;
        let add_member = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000002000-0000-founder".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &founder_pubkey, 2, add_member).await;
        publish_membership_head(storage.as_ref(), &chain, &founder)
            .await
            .unwrap();

        let old_listing = storage.discover_membership_entries().await;
        let accepted = load_anchored_chain(
            storage.as_ref(),
            &old_listing,
            Some(&founder_pubkey),
            Some(&db),
        )
        .await
        .expect("accept member at the initial floor");
        assert!(accepted.can_write_now(&member_pubkey));

        let (old_head_snapshotted, release_old_load) =
            storage.pause_next_membership_head_read(&founder_pubkey);
        let old_storage = storage.clone();
        let old_db = db.clone();
        let old_owner = founder_pubkey.clone();
        let old_load = tokio::spawn(async move {
            load_anchored_chain(
                old_storage.as_ref(),
                &old_listing,
                Some(&old_owner),
                Some(&old_db),
            )
            .await
        });
        old_head_snapshotted.notified().await;

        let remove_member = chain
            .signed_remove_member(
                &founder,
                pubkey_hex(&member),
                "0000000003000-0000-founder".to_string(),
            )
            .expect("active Owner removes membership grant");
        append_membership_entry(
            storage.as_ref(),
            &mut chain,
            &founder_pubkey,
            3,
            remove_member,
        )
        .await;
        publish_membership_head(storage.as_ref(), &chain, &founder)
            .await
            .unwrap();
        let new_listing = storage.discover_membership_entries().await;

        let new_storage = storage.clone();
        let new_db = db.clone();
        let new_owner = founder_pubkey.clone();
        let mut new_load = tokio::spawn(async move {
            load_anchored_chain(
                new_storage.as_ref(),
                &new_listing,
                Some(&new_owner),
                Some(&new_db),
            )
            .await
        });

        let new_completed_while_old_was_paused =
            tokio::time::timeout(std::time::Duration::from_millis(500), &mut new_load).await;
        release_old_load.notify_one();
        let old_chain = old_load
            .await
            .expect("old load task")
            .expect("old load returns its accepted chain");

        match new_completed_while_old_was_paused {
            Ok(result) => {
                let new_chain = result.expect("new load task").expect("new load result");
                assert!(!new_chain.can_write_now(&member_pubkey));
                panic!("a newer floor committed while an older membership load could still return");
            }
            Err(_) => {
                assert!(old_chain.can_write_now(&member_pubkey));
                let new_chain = new_load
                    .await
                    .expect("new load task")
                    .expect("new load result");
                assert!(!new_chain.can_write_now(&member_pubkey));
            }
        }
    }

    #[tokio::test]
    async fn persisted_author_floor_requires_readable_head() {
        let fixture = committed_removal_by_second_owner().await;
        let (owner_before, floors_before) = pin_fixture_anchor(&fixture).await;
        fixture
            .storage
            .hide_membership_from_listing(&fixture.second_owner_pubkey, 1);
        fixture
            .storage
            .remove_membership_head(&fixture.second_owner_pubkey);
        let visible = fixture.storage.discover_membership_entries().await;

        let error = load_and_persist_owner_anchor(
            &fixture.storage,
            &visible,
            &fixture.founder_pubkey,
            &fixture.db,
        )
        .await
        .expect_err("a persisted author floor requires a readable head");

        assert!(
            error.to_string().contains(&fixture.second_owner_pubkey),
            "missing-head error must name the persisted author: {error}",
        );
        assert_fixture_trust_state(&fixture, &owner_before, &floors_before).await;
    }

    #[tokio::test]
    async fn membership_head_must_match_storage_author() {
        let fixture = committed_removal_by_second_owner().await;
        let (owner_before, floors_before) = pin_fixture_anchor(&fixture).await;
        fixture
            .storage
            .hide_membership_from_listing(&fixture.second_owner_pubkey, 1);
        let other_author = hex::encode([9u8; 32]);
        let second_owner_head = fixture
            .storage
            .read_latest_membership_head_bytes(&fixture.second_owner_pubkey)
            .await
            .unwrap();
        fixture
            .storage
            .append_membership_head_bytes(&other_author, second_owner_head)
            .await
            .unwrap();
        let visible = fixture.storage.discover_membership_entries().await;

        let error = load_and_persist_owner_anchor(
            &fixture.storage,
            &visible,
            &fixture.founder_pubkey,
            &fixture.db,
        )
        .await
        .expect_err("a signed head must match the author namespace it was read from");

        let message = error.to_string();
        assert!(message.contains(&other_author), "{message}");
        assert!(message.contains(&fixture.second_owner_pubkey), "{message}");
        assert_fixture_trust_state(&fixture, &owner_before, &floors_before).await;
    }

    #[tokio::test]
    async fn invalid_membership_head_signature_preserves_owner_and_floors() {
        let fixture = committed_removal_by_second_owner().await;
        let (owner_before, floors_before) = pin_fixture_anchor(&fixture).await;
        let bytes = fixture
            .storage
            .read_latest_membership_head_bytes(&fixture.founder_pubkey)
            .await
            .unwrap();
        let mut head: AuthorHead = serde_json::from_slice(&bytes).unwrap();
        head.signature = hex::encode([0u8; 64]);
        fixture
            .storage
            .append_membership_head_bytes(
                &fixture.founder_pubkey,
                serde_json::to_vec(&head).unwrap(),
            )
            .await
            .unwrap();
        let visible = fixture.storage.discover_membership_entries().await;

        let error = load_and_persist_owner_anchor(
            &fixture.storage,
            &visible,
            &fixture.founder_pubkey,
            &fixture.db,
        )
        .await
        .expect_err("an invalid signed head must reject anchor refresh");

        assert!(
            error
                .to_string()
                .contains("Store protocol signature is invalid"),
            "{error}"
        );
        assert_fixture_trust_state(&fixture, &owner_before, &floors_before).await;
    }

    #[tokio::test]
    async fn forked_membership_head_preserves_owner_and_floors() {
        let fixture = committed_removal_by_second_owner().await;
        let (owner_before, floors_before) = pin_fixture_anchor(&fixture).await;
        let current: AuthorHead = serde_json::from_slice(
            &fixture
                .storage
                .read_latest_membership_head_bytes(&fixture.founder_pubkey)
                .await
                .unwrap(),
        )
        .unwrap();
        let mismatched = AuthorHead::signed(
            current.store_id,
            current.author_owner_grant,
            3,
            super::super::store_commit::ObjectHash::digest(b"mismatched membership tip"),
            &fixture.founder,
        );
        fixture
            .storage
            .append_membership_head_bytes(
                &fixture.founder_pubkey,
                serde_json::to_vec(&mismatched).unwrap(),
            )
            .await
            .unwrap();
        let visible = fixture.storage.discover_membership_entries().await;

        let error = load_and_persist_owner_anchor(
            &fixture.storage,
            &visible,
            &fixture.founder_pubkey,
            &fixture.db,
        )
        .await
        .expect_err("two signed heads for one immutable slot must be rejected");

        assert!(error.to_string().contains("valid forks"), "{error}");
        assert_fixture_trust_state(&fixture, &owner_before, &floors_before).await;
    }

    #[tokio::test]
    async fn membership_entry_must_match_storage_author() {
        let author = UserKeypair::generate();
        let other_author = hex::encode([8u8; 32]);
        let entry = founder_entry("test-store", &author, "0000000001000-0000-author");
        let storage = MockSyncStorage::with_keypair(author.clone());
        let bytes = serde_json::to_vec(&entry).unwrap();
        let prefix = super::super::store_commit::membership_entry_semantic_prefix(
            &other_author,
            &entry.author_owner_grant,
            1,
            super::super::store_commit::ObjectHash::digest(&bytes),
        );
        storage
            .append_protocol_object(
                &super::super::storage::ProtocolObjectContext::store(
                    storage.store_root_hash(),
                    super::super::storage::ProtocolObjectDomain::StoreMembershipEntry,
                ),
                &prefix,
                ".json",
                bytes,
            )
            .await
            .unwrap();

        let error = download_entries(
            &storage,
            &[MembershipCoord {
                author_pubkey: other_author.clone(),
                author_owner_grant: entry.author_owner_grant.clone(),
                seq: 1,
                entry_hash: entry.coord().entry_hash,
            }],
        )
        .await
        .expect_err("an entry must match the author namespace it was read from");

        let message = error.to_string();
        assert!(message.contains(&other_author), "{message}");
        assert!(message.contains(&pubkey_hex(&author)), "{message}");
    }

    /// Adopting a rotated key re-checks under the write lock that the incoming
    /// keyring genuinely extends the live one. A concurrent member op may have
    /// committed and adopted a newer rotation between the caller's is-it-newer
    /// check and here; adopting a keyring that adds nothing would regress the seal
    /// key and custody. A stale (non-extending) apply fails loud and touches
    /// neither the live cipher nor custody.
    #[test]
    fn apply_key_rotation_refuses_a_nonextending_keyring_and_leaves_custody_untouched() {
        let live = EncryptionService::from_key([1u8; 32])
            .with_appended_generation(2, [2u8; 32])
            .unwrap();
        let custody = TestCustody::default();
        custody
            .persist(&MasterKeyring::from(live.clone()))
            .expect("seed custody with the live keyring");
        let cipher = RwLock::new(CloudCipher::Encrypted(live.clone()));
        let pending = PendingRotation::none();

        // A strict subset of the live keyring — only its generation-1 key. Adopting
        // it would drop generation 2 and regress the seal key.
        let stale = EncryptionService::from_key([1u8; 32]);
        let error = apply_key_rotation(stale, &custody, &cipher, &pending)
            .expect_err("a non-extending keyring must fail loud");
        assert!(matches!(error, KeyError::StaleKeyRotation), "{error:?}");

        assert_eq!(
            custody.unlock().unwrap().unwrap().fingerprint(),
            live.fingerprint(),
            "a stale apply must not rewrite custody",
        );
        let guard = cipher.read().unwrap();
        match &*guard {
            CloudCipher::Encrypted(enc) => assert_eq!(
                enc.fingerprint(),
                live.fingerprint(),
                "a stale apply must not swap the live cipher",
            ),
            CloudCipher::Plaintext => panic!("cipher must stay encrypted"),
        }
    }

    /// The invite anchors the joiner to the store FOUNDER, regardless of which
    /// Owner sends it. A second Owner inviting must still hand over the founder's
    /// pubkey, or the joiner pins the wrong owner and rejects the founder-anchored
    /// chain forever (issue #102).
    #[tokio::test]
    async fn invite_carries_the_founder_not_the_inviting_owner() {
        let founder = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let invitee = UserKeypair::generate();
        let founder_pk = pubkey_hex(&founder);

        // Chain: founder, then the founder adds `second_owner` as an Owner.
        let storage = MockSyncStorage::with_store_and_keypair("lib-1", founder.clone());
        let mut chain = MembershipChain::new();
        let founder_entry = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &founder_pk, 1, founder_entry).await;
        let add_owner = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&second_owner),
                None,
                MemberRole::Owner,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &founder_pk, 2, add_owner).await;
        publish_membership_head(&storage, &chain, &founder)
            .await
            .unwrap();
        let db = anchored_db(&storage, &founder_pk).await;

        // The SECOND owner invites a new member. (MockSyncStorage is both the
        // SyncStorage and the CloudHome.)
        let hlc = Hlc::new("f".to_string());
        let invite = invite_member(
            &storage,
            &storage,
            &second_owner,
            &hlc,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &EncryptionService::from_key([7u8; 32]),
            "lib-1",
            "Lib One",
            &db,
        )
        .await
        .expect("invite");

        assert_eq!(
            invite.owner_pubkey, founder_pk,
            "the invite must carry the founder's pubkey",
        );
        assert_ne!(
            invite.owner_pubkey,
            pubkey_hex(&second_owner),
            "not the inviting owner's pubkey",
        );
    }

    #[tokio::test]
    async fn invite_carries_the_root_named_by_the_authoritative_hash() {
        let founder = UserKeypair::generate();
        let invitee = UserKeypair::generate();
        let storage = MockSyncStorage::with_store_and_keypair("pinned-invite", founder.clone());
        let founder_pubkey = pubkey_hex(&founder);
        storage.publish_protocol_founder_membership().await;
        let db = anchored_db(&storage, &founder_pubkey).await;
        let pinned_hash = storage.store_root_hash();
        let alternative_hash = append_alternative_store_protocol_root(&storage, &founder).await;
        assert_ne!(alternative_hash, pinned_hash);

        let invite = invite_member(
            &storage,
            &storage,
            &founder,
            &Hlc::new("founder".to_string()),
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &EncryptionService::from_key([7u8; 32]),
            "pinned-invite",
            "Pinned invite",
            &db,
        )
        .await
        .expect("invite uses the root already pinned in database state");

        assert_eq!(invite.store_root_hash, pinned_hash);
    }

    #[tokio::test]
    async fn inviting_yourself_is_a_typed_self_invite_error() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let hlc = Hlc::new("f".to_string());
        let db = crate::sync::test_helpers::open_test_db();

        let result = invite_member(
            &storage,
            &storage,
            &owner,
            &hlc,
            &pubkey_hex(&owner),
            None,
            MemberRole::Member,
            &EncryptionService::from_key([7u8; 32]),
            "lib-1",
            "Lib One",
            &db,
        )
        .await;
        assert!(matches!(result, Err(MembershipOpsError::SelfInvite)));
    }

    #[tokio::test]
    async fn inviting_into_a_founderless_chain_is_refused_with_a_typed_variant() {
        // A wiped or never-founded `membership/*` must not let an invite bootstrap
        // a founder on the spot — that is the takeover primitive. The refusal is a
        // matchable variant, not a scraped string (issue #104).
        let owner = UserKeypair::generate();
        let invitee = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let hlc = Hlc::new("f".to_string());
        let db = crate::sync::test_helpers::open_test_db();

        let result = invite_member(
            &storage,
            &storage,
            &owner,
            &hlc,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &EncryptionService::from_key([7u8; 32]),
            "lib-1",
            "Lib One",
            &db,
        )
        .await;
        assert!(matches!(result, Err(MembershipOpsError::NoFounderChain)));
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_membership_error_variants() {
        let storage = MockSyncStorage::new();
        let db = crate::sync::test_helpers::open_test_db();

        assert!(matches!(
            get_members(&storage, None, &db).await,
            Err(MembershipOpsError::NoFounderChain)
        ));

        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            "not-an-object-hash",
        )
        .await
        .expect("write malformed Store root");
        assert!(matches!(
            get_members(&storage, None, &db).await,
            Err(MembershipOpsError::Database(reason))
                if reason.contains("Store protocol root hash")
        ));
    }

    #[tokio::test]
    async fn invite_and_remove_reuse_their_loaded_membership_listing() {
        let owner = UserKeypair::generate();
        let invitee = UserKeypair::generate();
        let storage = MockSyncStorage::with_store_and_keypair("lib-1", owner.clone());
        let owner_pk = pubkey_hex(&owner);
        let invitee_pk = pubkey_hex(&invitee);
        let mut chain = MembershipChain::new();
        let founder = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();
        let db = anchored_db(&storage, &owner_pk).await;
        let lists_before_invite = storage.membership_list_count();

        let hlc = Hlc::new("f".to_string());
        invite_member(
            &storage,
            &storage,
            &owner,
            &hlc,
            &invitee_pk,
            None,
            MemberRole::Member,
            &EncryptionService::from_key([7u8; 32]),
            "lib-1",
            "Lib One",
            &db,
        )
        .await
        .expect("invite");

        assert_eq!(storage.membership_list_count(), lists_before_invite + 1);

        let custody = TestCustody::default();
        let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
            [7u8; 32],
        )));
        remove_member(
            &storage,
            &storage,
            &owner,
            &hlc,
            &invitee_pk,
            "lib-1",
            &EncryptionService::from_key([7u8; 32]),
            &custody,
            &cipher,
            &crate::sync::cloud_storage::PendingRotation::none(),
            &db,
        )
        .await
        .expect("remove");

        assert_eq!(storage.membership_list_count(), lists_before_invite + 2);
    }

    /// Member removal completes on a home that offers no per-member credential
    /// revocation — the S3 position, where the bucket key is one static
    /// credential that cannot be withdrawn from a single member. The home
    /// reports [`RevokeOutcome::Unsupported`](crate::storage::cloud::RevokeOutcome)
    /// (`MockSyncStorage` returns it, as [`S3CloudHome`](crate::storage::cloud)
    /// does), and removal proceeds: the chain Remove and the store-key rotation,
    /// not the credential withdrawal, are what protect post-removal content, so
    /// an `Unsupported` outcome must not abort the removal.
    #[tokio::test]
    async fn remove_member_completes_when_the_home_reports_no_per_member_revocation() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::with_store_and_keypair("lib-1", owner.clone());
        let owner_pk = pubkey_hex(&owner);
        let member_pk = pubkey_hex(&member);
        let mut chain = MembershipChain::new();

        let founder = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_member = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();
        let db = anchored_db(&storage, &owner_pk).await;

        let custody = TestCustody::default();
        let cipher = RwLock::new(CloudCipher::Encrypted(EncryptionService::from_key(
            [7u8; 32],
        )));
        let hlc = Hlc::new("f".to_string());
        remove_member(
            &storage,
            &storage,
            &owner,
            &hlc,
            &member_pk,
            "lib-1",
            &EncryptionService::from_key([7u8; 32]),
            &custody,
            &cipher,
            &crate::sync::cloud_storage::PendingRotation::none(),
            &db,
        )
        .await
        .expect("remove completes even though the home cannot revoke the credential");

        // The Remove entry was published, so the removed member is no longer a
        // current member of the reloaded chain.
        let visible = storage.discover_membership_entries().await;
        let reloaded = load_anchored_chain(&storage, &visible, Some(&owner_pk), None)
            .await
            .expect("chain reloads after removal");
        assert!(
            !reloaded.can_write_now(&member_pk),
            "the removed member must not read as a current writer",
        );
    }

    #[tokio::test]
    async fn suppressed_remove_is_detected() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let owner_pk = pubkey_hex(&owner);
        let member_pk = pubkey_hex(&member);
        let mut chain = MembershipChain::new();

        let founder = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_member = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
        let remove_member = chain
            .signed_remove_member(
                &owner,
                pubkey_hex(&member),
                "0000000003000-0000-f".to_string(),
            )
            .expect("active Owner removes membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        storage.remove_membership_entry(&owner_pk, 3);
        let visible = storage.discover_membership_entries().await;

        let result = load_anchored_chain(&storage, &visible, Some(&owner_pk), None).await;
        assert!(
            result.is_err(),
            "a listed chain missing a signed Remove must be rejected, not read {member_pk} as current"
        );
    }

    /// A committed prefix is fetched by keyed GET up to the head's seq, so an entry
    /// a lagging (eventually-consistent) LIST hasn't surfaced yet is recovered
    /// rather than treated as a gap: the head, not the listing, decides how far the
    /// prefix reaches.
    #[tokio::test]
    async fn list_lagging_middle_entry_is_recovered_by_keyed_get() {
        let owner = UserKeypair::generate();
        let member_a = UserKeypair::generate();
        let member_b = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_a = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member_a),
                None,
                MemberRole::Member,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_a).await;
        let add_b = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member_b),
                None,
                MemberRole::Member,
                "0000000003000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, add_b).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        // The LIST omits seq 2, but the keyed GET still serves it.
        storage.hide_membership_from_listing(&owner_pk, 2);
        let visible = storage.discover_membership_entries().await;

        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk), None)
            .await
            .expect("a list-lagging entry within the head is fetched by keyed GET");
        assert!(loaded.can_write_now(&pubkey_hex(&member_a)));
        assert!(loaded.can_write_now(&pubkey_hex(&member_b)));
    }

    #[tokio::test]
    async fn complete_chain_still_validates() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_member = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        let visible = storage.discover_membership_entries().await;
        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk), None)
            .await
            .expect("complete chain validates");

        assert!(loaded.can_write_now(&pubkey_hex(&member)));
    }

    #[tokio::test]
    async fn missing_membership_head_is_rejected() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let visible = storage.discover_membership_entries().await;

        let result = load_anchored_chain(&storage, &visible, Some(&owner_pk), None).await;
        assert!(
            result.is_err(),
            "a non-empty membership chain without a signed head is not committed"
        );
    }

    #[tokio::test]
    async fn entry_beyond_membership_head_is_not_committed() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_member = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        let remove_member = chain
            .signed_remove_member(
                &owner,
                pubkey_hex(&member),
                "0000000003000-0000-f".to_string(),
            )
            .expect("active Owner removes membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
        let visible = storage.discover_membership_entries().await;

        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk), None)
            .await
            .expect("unheaded entry is ignored");
        assert!(
            loaded.can_write_now(&pubkey_hex(&member)),
            "membership state follows the signed head, not listed entries past it"
        );
    }

    /// Two owners commit concurrently, each under its own head; the reader's
    /// committed set is the union of both prefixes. A Remove one owner publishes is
    /// therefore never suppressed by the other owner's concurrent invite — no single
    /// object carries both, so neither publisher can discard the other's entry.
    #[tokio::test]
    async fn concurrent_owner_heads_union_and_never_suppress_a_remove() {
        let founder = UserKeypair::generate();
        let second_owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let invitee = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(founder.clone());
        let founder_pk = pubkey_hex(&founder);
        let second_pk = pubkey_hex(&second_owner);

        // Founder's prefix: found, promote second_owner to Owner, add member.
        let mut chain = MembershipChain::new();
        let f = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &founder_pk, 1, f).await;
        let add_owner = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&second_owner),
                None,
                MemberRole::Owner,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &founder_pk, 2, add_owner).await;
        let add_member = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000003000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &founder_pk, 3, add_member).await;

        // Concurrent, independent commits: the founder invites `invitee` at
        // founder/4 while second_owner removes `member` at second_owner/1, each
        // publishing only its own head.
        let add_invitee = chain
            .signed_set_member(
                &founder,
                pubkey_hex(&invitee),
                None,
                MemberRole::Member,
                "0000000004000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &founder_pk, 4, add_invitee).await;
        let remove_member = chain
            .signed_remove_member(
                &second_owner,
                pubkey_hex(&member),
                "0000000005000-0000-f".to_string(),
            )
            .expect("active Owner removes membership grant");
        append_membership_entry(&storage, &mut chain, &second_pk, 1, remove_member).await;

        publish_membership_head(&storage, &chain, &founder)
            .await
            .unwrap();
        publish_membership_head(&storage, &chain, &second_owner)
            .await
            .unwrap();

        // A third reader loads the union: invitee added AND member removed.
        let visible = storage.discover_membership_entries().await;
        let loaded = load_anchored_chain(&storage, &visible, Some(&founder_pk), None)
            .await
            .unwrap();
        let members = loaded.current_members();
        assert!(
            members.iter().any(|(pk, _)| pk == &pubkey_hex(&invitee)),
            "the concurrent invite is committed"
        );
        assert!(
            !members.iter().any(|(pk, _)| pk == &pubkey_hex(&member)),
            "the concurrent Remove is never suppressed"
        );
        assert!(members
            .iter()
            .any(|(pk, r)| pk == &second_pk && *r == MemberRole::Owner));
    }

    /// The durable watermark write is monotonic. Two concurrent watermark-writing
    /// loads can each pass the in-memory regression check against the old stored
    /// value and then persist out of order; a plain overwrite would let the lower
    /// seq land last, and a provider replaying the pre-removal head below it would
    /// then be accepted. The out-of-order persist must leave the higher value.
    /// The values are unpadded decimal strings, so this also pins the comparison
    /// as numeric — lexically, "9" beats "10".
    #[tokio::test]
    async fn head_watermark_persist_never_regresses() {
        use crate::sync::test_helpers::open_test_db;

        let db = open_test_db();
        let author = "aabbccdd";
        let grant = OwnerGrantId(super::super::store_commit::ObjectHash::digest(
            b"watermark grant",
        ));

        persist_head_watermarks(
            &db,
            &[MembershipCoord {
                author_pubkey: author.to_string(),
                author_owner_grant: grant.clone(),
                seq: 10,
                entry_hash: super::super::store_commit::ObjectHash::digest(b"entry 10"),
            }],
        )
        .await
        .unwrap();
        persist_head_watermarks(
            &db,
            &[MembershipCoord {
                author_pubkey: author.to_string(),
                author_owner_grant: grant.clone(),
                seq: 9,
                entry_hash: super::super::store_commit::ObjectHash::digest(b"entry 9"),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            read_head_watermarks(&db)
                .await
                .unwrap()
                .get(&grant)
                .map(|coord| coord.seq),
            Some(10),
            "an out-of-order lower persist must not regress the stored watermark",
        );

        persist_head_watermarks(
            &db,
            &[MembershipCoord {
                author_pubkey: author.to_string(),
                author_owner_grant: grant.clone(),
                seq: 11,
                entry_hash: super::super::store_commit::ObjectHash::digest(b"entry 11"),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            read_head_watermarks(&db)
                .await
                .unwrap()
                .get(&grant)
                .map(|coord| coord.seq),
            Some(11),
            "a higher persist still advances it",
        );
    }

    #[tokio::test]
    async fn seeding_a_complete_head_floor_is_atomic() {
        use crate::sync::test_helpers::open_test_db;

        let db = open_test_db();
        let first_grant = OwnerGrantId(super::super::store_commit::ObjectHash::digest(
            b"first floor grant",
        ));
        let second_grant = OwnerGrantId(super::super::store_commit::ObjectHash::digest(
            b"second floor grant",
        ));
        let rejected_key = head_watermark_key(&second_grant.to_string());
        db.call(move |conn| {
            conn.execute_batch(&format!(
                "CREATE TRIGGER reject_second_membership_floor \
                 BEFORE INSERT ON protocol_state \
                 WHEN NEW.key = '{rejected_key}' \
                 BEGIN SELECT RAISE(ABORT, 'forced second floor failure'); END;"
            ))
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
        let floor = vec![
            MembershipCoord {
                author_pubkey: "aaaa".to_string(),
                author_owner_grant: first_grant,
                seq: 3,
                entry_hash: super::super::store_commit::ObjectHash::digest(b"first floor"),
            },
            MembershipCoord {
                author_pubkey: "bbbb".to_string(),
                author_owner_grant: second_grant,
                seq: 7,
                entry_hash: super::super::store_commit::ObjectHash::digest(b"second floor"),
            },
        ];

        assert!(seed_head_watermark(&db, &floor).await.is_err());
        assert_eq!(
            read_head_watermarks(&db).await.unwrap(),
            BTreeMap::new(),
            "a failed complete-floor seed rolls back every author",
        );
    }

    #[tokio::test]
    async fn owner_pin_and_complete_head_floor_commit_atomically() {
        use crate::sync::test_helpers::open_test_db;

        let fixture = committed_removal_by_second_owner().await;
        let db = open_test_db();
        let visible = fixture.storage.discover_membership_entries().await;
        let chain = load_anchored_chain(
            &fixture.storage,
            &visible,
            Some(&fixture.founder_pubkey),
            None,
        )
        .await
        .unwrap();
        let rejected_key = head_watermark_key(
            &chain
                .active_owner_grant(&fixture.second_owner_pubkey)
                .expect("second owner grant")
                .to_string(),
        );
        db.call(move |conn| {
            conn.execute_batch(&format!(
                "CREATE TRIGGER reject_second_anchor_floor \
                 BEFORE INSERT ON protocol_state \
                 WHEN NEW.key = '{rejected_key}' \
                 BEGIN SELECT RAISE(ABORT, 'forced second floor failure'); END;",
            ))
            .map_err(crate::database::DbError::from)
        })
        .await
        .unwrap();
        assert!(load_and_persist_owner_anchor(
            &fixture.storage,
            &visible,
            &fixture.founder_pubkey,
            &db,
        )
        .await
        .is_err());
        assert_eq!(
            db.get_protocol_state(OWNER_PUBKEY_STATE_KEY).await.unwrap(),
            None,
            "a failed floor write rolls back the owner pin",
        );
        assert_eq!(
            read_head_watermarks(&db).await.unwrap(),
            BTreeMap::new(),
            "a failed floor write rolls back every author's floor",
        );
    }

    /// A reader that has accepted a head at some seq refuses a later read whose seq
    /// regresses below it (a stale replica on an eventually-consistent provider, or
    /// a rewound head object), so a committed removal can't be undone underneath a
    /// reader that already saw it. Without the persisted watermark the second load
    /// would re-admit the removed member.
    #[tokio::test]
    async fn reader_refuses_a_head_that_regresses_below_its_watermark() {
        use crate::sync::membership::{entry_hash, AuthorHead};
        use crate::sync::test_helpers::open_test_db;

        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let db = open_test_db();
        let owner_pk = pubkey_hex(&owner);

        let mut chain = MembershipChain::new();
        let f = storage.store_protocol_root().founder.clone();
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, f).await;
        let add = chain
            .signed_set_member(
                &owner,
                pubkey_hex(&member),
                None,
                MemberRole::Member,
                "0000000002000-0000-f".to_string(),
            )
            .expect("active Owner signs membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add.clone()).await;
        let remove = chain
            .signed_remove_member(
                &owner,
                pubkey_hex(&member),
                "0000000003000-0000-f".to_string(),
            )
            .expect("active Owner removes membership grant");
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        // The reader accepts the head at seq 3: member removed, watermark now 3.
        let visible = storage.discover_membership_entries().await;
        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk), Some(&db))
            .await
            .unwrap();
        assert!(!loaded
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&member)));

        // A stale read serves the head from before the Remove (seq 2). The reader
        // refuses to regress rather than re-admit the member.
        let stale = AuthorHead::signed(
            "test-store".to_string(),
            add.author_owner_grant.clone(),
            2,
            entry_hash(&add),
            &owner,
        );
        storage.remove_membership_head(&owner_pk);
        storage
            .append_membership_head_bytes(&owner_pk, serde_json::to_vec(&stale).unwrap())
            .await
            .unwrap();
        let result = load_anchored_chain(&storage, &visible, Some(&owner_pk), Some(&db)).await;
        assert!(
            result.is_err(),
            "a head regressing below the accepted watermark must be refused"
        );
    }
}
