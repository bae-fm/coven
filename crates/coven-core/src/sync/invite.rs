use crate::database::{Database, DurableMembershipMutation};
use crate::encryption::{self, EncryptionService};
use crate::keys::{self, KeyError, UserKeypair};
/// Invitation and revocation flow for shared store membership.
///
/// `create_invitation()` is called by the store owner to invite a new member.
/// `unwrap_store_keyring()` is called by the invitee to join and unwrap the store key.
/// `revoke_member()` is called by the store owner to remove a member and rotate the key.
use crate::storage::cloud::{
    CloudAccessOutcome, CloudAccessState, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    RevokeOutcome,
};

use super::membership::{
    AuthorHead, AuthorStreamId, MemberRole, MembershipChain, MembershipChange, MembershipEntry,
    MembershipEntryRef, MembershipError, MembershipHeadRef,
};
use super::storage::{
    PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use super::store_commit::{membership_head_slot_prefix, GrantStreamAnchor, SuccessorLink};
use super::wrapped_store_key::{
    load_wrapped_store_key, prepare_wrapped_store_key, PreparedWrappedStoreKey, WrappedStoreKey,
    WrappedStoreKeyRef,
};

#[derive(Debug, thiserror::Error)]
pub enum InviteError {
    #[error("Bucket error: {0}")]
    Bucket(#[from] StorageError),
    #[error("Key error: {0}")]
    Key(#[from] KeyError),
    #[error("Membership error: {0}")]
    Membership(#[from] MembershipError),
    #[error("Cloud home error: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("Crypto error: {0}")]
    Crypto(String),
    #[error(
        "stale membership head for {author}: committed through seq {committed}, \
         cannot write seq {attempted}"
    )]
    StaleMembershipHead {
        author: String,
        committed: u64,
        attempted: u64,
    },
    #[error("{operation} failed: {original}; rollback failed: {rollback}")]
    Rollback {
        operation: &'static str,
        original: String,
        rollback: String,
    },
    #[error("User {0} is not a current member")]
    NotAMember(String),
    #[error("Cannot revoke the last owner of a store")]
    LastOwner,
    #[error("membership mutation database state: {0}")]
    Database(String),
    #[error("pending membership mutation does not match this request: {0}")]
    PendingMutation(String),
    #[error("durable membership mutation is invalid: {0}")]
    InvalidDurableMutation(String),
}

impl From<crate::database::DbError> for InviteError {
    fn from(error: crate::database::DbError) -> Self {
        Self::Database(error.into_message())
    }
}

async fn select_mutation_author_stream(
    db: &Database,
    chain: &MembershipChain,
    signer: &UserKeypair,
) -> Result<AuthorStreamId, InviteError> {
    let author = keys::public_key_hex(signer);
    let grant = chain
        .active_owner_grant(&author)
        .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
    let mut reusable = chain.reusable_author_streams(&author, &grant);
    if let Some(anchored) = chain.membership_stream_id(&grant) {
        reusable.insert(anchored);
    }
    Ok(db
        .select_membership_author_stream(&author, &grant, reusable)
        .await?)
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "kind",
    content = "plan",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum MembershipMutationPlan {
    Invite(InviteMutationPlan),
    Revoke(RevokeMutationPlan),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InviteMutationPlan {
    publication: PreparedMembershipPublication,
    invitee_pubkey: String,
    invitee_email: Option<String>,
    role: MemberRole,
    desired_access: CloudAccessState,
    wrapped_key: PreparedWrappedStoreKey,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeMutationPlan {
    publication: PreparedMembershipPublication,
    revokee_pubkey: String,
    desired_access: CloudAccessState,
    wraps: Vec<ReplacementWrappedKey>,
    keyring_payload: Vec<u8>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedMembershipPublication {
    entry: MembershipEntry,
    entry_ref: MembershipEntryRef,
    entry_object: PreparedExactObject,
    head: AuthorHead,
    head_ref: MembershipHeadRef,
    head_object: PreparedExactObject,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplacementWrappedKey {
    prepared: PreparedWrappedStoreKey,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum MembershipMutationProgress {
    Pending,
    InviteGranted { join_info: CloudHomeJoinInfo },
}

struct MutationPersistence<'a> {
    db: &'a Database,
    intent_hash: super::store_commit::ObjectHash,
}

impl MutationPersistence<'_> {
    async fn record_progress(
        &self,
        progress: &MembershipMutationProgress,
    ) -> Result<(), InviteError> {
        let bytes = serde_json::to_vec(progress).map_err(|error| {
            InviteError::InvalidDurableMutation(format!("serialize progress: {error}"))
        })?;
        self.db
            .update_membership_mutation_progress(self.intent_hash, bytes)
            .await?;
        Ok(())
    }

    async fn complete(&self) -> Result<(), InviteError> {
        self.db
            .complete_membership_mutation(self.intent_hash)
            .await?;
        Ok(())
    }
}

/// Refuse to write over the exact author stream's committed prefix. The intended
/// `seq` must sit past the head already in storage. Author stream ids are generated
/// and persisted per database, so independently restored devices write different
/// streams. If local protocol state is copied and two devices do reuse one stream,
/// immutable same-sequence objects expose the fork and readers reject it.
fn ed25519_hex_to_x25519(
    ed25519_pubkey_hex: &str,
) -> Result<[u8; keys::CURVE25519_PUBLICKEYBYTES], InviteError> {
    let pk_bytes: [u8; keys::SIGN_PUBLICKEYBYTES] = hex::decode(ed25519_pubkey_hex)
        .map_err(|e| InviteError::Crypto(format!("invalid pubkey hex: {e}")))?
        .try_into()
        .map_err(|_| InviteError::Crypto("pubkey wrong length".to_string()))?;
    keys::ed25519_to_x25519_public_key(&pk_bytes)
        .map_err(|e| InviteError::Crypto(format!("invalid pubkey: {e}")))
}

/// Seal the store key to one member and wrap it in an owner-signed
/// [`WrappedStoreKey`], serialized to the bytes stored at
/// `keys/{owner_pubkey}/{recipient_pubkey}` (the owner writes into its own
/// prefix). The signature binds `(store_id, recipient_pubkey, author_pubkey,
/// sealed)` so the joiner can prove the key came from the owner and was meant for
/// them, not substituted by a bucket writer.
///
/// `owner_keypair` is whatever Owner is performing the invite/revoke — NOT
/// necessarily the chain founder. The two callers below pass the local device's
/// own keypair, and the membership chain authorizes any current Owner to add or
/// remove members, so a second Owner can reach here and sign with their own key.
///
/// A joining device pins exactly one clear-text authority: the founder the invite
/// carries (`InviteCode::owner_pubkey`, set from `chain.founder_pubkey()`),
/// because the joiner has no membership chain yet. Existing members are different:
/// they reload the anchored chain first and authorize rotated wrapped keys
/// against the current Owner set.
fn signed_wrapped_key(
    store_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
) -> Result<WrappedStoreKey, InviteError> {
    let payload = encryption
        .to_keyring_payload()
        .map_err(|e| InviteError::Crypto(format!("serialize keyring payload: {e}")))?;
    let sealed = keys::seal_box_encrypt(&payload, recipient_x25519_pk);
    let wrapped = WrappedStoreKey::signed(
        store_id,
        recipient_ed25519_pubkey,
        encryption.current_generation(),
        sealed,
        owner_keypair,
    );
    Ok(wrapped)
}

pub(crate) fn signed_serial_wrapped_key(
    store_id: &str,
    recipient_ed25519_pubkey: &str,
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
) -> Result<WrappedStoreKey, InviteError> {
    let recipient_x25519_pk = ed25519_hex_to_x25519(recipient_ed25519_pubkey)?;
    let payload = encryption
        .to_keyring_payload()
        .map_err(|error| InviteError::Crypto(format!("serialize keyring payload: {error}")))?;
    let sealed = keys::seal_box_encrypt(&payload, &recipient_x25519_pk);
    let wrapped = WrappedStoreKey::signed(
        store_id,
        recipient_ed25519_pubkey,
        encryption.current_generation(),
        sealed,
        owner_keypair,
    );
    Ok(wrapped)
}

#[cfg(test)]
pub(crate) fn signed_wrapped_keyring_for_test(
    store_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
) -> WrappedStoreKey {
    signed_wrapped_key(
        store_id,
        recipient_ed25519_pubkey,
        recipient_x25519_pk,
        encryption,
        owner_keypair,
    )
    .expect("signed wrapped key")
}

fn encode_membership_mutation(plan: &MembershipMutationPlan) -> Result<Vec<u8>, InviteError> {
    serde_json::to_vec(plan)
        .map_err(|error| InviteError::InvalidDurableMutation(format!("serialize plan: {error}")))
}

fn encode_membership_progress(
    progress: &MembershipMutationProgress,
) -> Result<Vec<u8>, InviteError> {
    serde_json::to_vec(progress).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("serialize progress: {error}"))
    })
}

fn decode_membership_mutation(
    row: DurableMembershipMutation,
) -> Result<(MembershipMutationPlan, MembershipMutationProgress), InviteError> {
    let plan = serde_json::from_slice(&row.plan_bytes)
        .map_err(|error| InviteError::InvalidDurableMutation(format!("parse plan: {error}")))?;
    let progress = serde_json::from_slice(&row.progress_bytes)
        .map_err(|error| InviteError::InvalidDurableMutation(format!("parse progress: {error}")))?;
    Ok((plan, progress))
}

fn chain_with_exact_entry(
    chain: &MembershipChain,
    entry: &MembershipEntry,
) -> Result<MembershipChain, InviteError> {
    let coord = entry.coord();
    if let Some((_, stored)) = chain
        .entries_with_coords()
        .find(|(stored_coord, _)| **stored_coord == coord)
    {
        if stored != entry {
            return Err(InviteError::InvalidDurableMutation(format!(
                "committed entry at {coord:?} differs from the durable plan"
            )));
        }
        return Ok(chain.clone());
    }
    let mut validated = chain.clone();
    validated.add_entry_at(coord, entry.clone())?;
    Ok(validated)
}

fn validate_prepared_publication(
    publication: &PreparedMembershipPublication,
) -> Result<(), InviteError> {
    let coord = publication.entry.coord();
    if publication.entry_ref.coord != coord
        || publication.entry_ref.object != *publication.entry_object.reference()
        || publication.head.entry != publication.entry_ref
        || publication.head.entry_coord() != coord
        || publication.head_ref.coord != coord
        || publication.head_ref.head_hash != publication.head.head_hash()
        || publication.head_ref.object != *publication.head_object.reference()
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership publication does not bind one exact entry and head".to_string(),
        ));
    }
    Ok(())
}

async fn prepare_membership_publication(
    storage: &dyn SyncStorage,
    db: &Database,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &MembershipChain,
    entry: MembershipEntry,
    owner_keypair: &UserKeypair,
) -> Result<PreparedMembershipPublication, InviteError> {
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(
                "local Store device registration is absent".to_string(),
            )
        })?;
    let (root, registration_ref, registration, device_signer) =
        super::store_outbound::load_local_store_authority(db, &device_id, owner_keypair)
            .await
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    if root.store_root_hash != store_root_hash
        || registration.author_pubkey != entry.author_pubkey
        || registration_ref.device_id != registration.device_id
    {
        return Err(InviteError::InvalidDurableMutation(
            "membership author differs from the active exact device registration".to_string(),
        ));
    }
    let (entry_object, entry_ref) =
        super::store_objects::prepare_membership_entry(storage, store_root_hash, &entry)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
    let coord = entry.coord();
    let predecessor = chain
        .head_ref_for_stream(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
        )
        .cloned();
    let current_slot = match predecessor.as_ref() {
        Some(reference) => {
            let loaded = super::store_objects::load_membership_head_ref(
                storage,
                store_root_hash,
                reference,
                &registration,
            )
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
            loaded.value.successor.next_slot
        }
        None => match chain.membership_anchor(&coord.author_owner_grant) {
            Some(super::store_commit::GrantStreamAnchor::StoreMembership { first_slot }) => {
                first_slot.clone()
            }
            Some(
                super::store_commit::GrantStreamAnchor::OwnerRecovery { .. }
                | super::store_commit::GrantStreamAnchor::CircleControl { .. }
                | super::store_commit::GrantStreamAnchor::CircleRoster { .. }
                | super::store_commit::GrantStreamAnchor::CircleMetadata { .. },
            ) => {
                return Err(InviteError::InvalidDurableMutation(format!(
                    "Owner grant {} uses another domain's anchor as its membership stream",
                    coord.author_owner_grant
                )))
            }
            None => {
                return Err(InviteError::InvalidDurableMutation(format!(
                    "Owner grant {} has no activated membership stream anchor",
                    coord.author_owner_grant
                )))
            }
        },
    };
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
        InviteError::InvalidDurableMutation("membership head sequence overflow".to_string())
    })?;
    let next_prefix = membership_head_slot_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        next_sequence,
    );
    let next_slot = storage
        .allocate_protocol_slot(&context, &next_prefix, ".json")
        .await?;
    let anchor = chain
        .membership_anchor(&coord.author_owner_grant)
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(format!(
                "Owner grant {} has no activated membership stream anchor",
                coord.author_owner_grant
            ))
        })?;
    let head = AuthorHead::signed(
        entry.store_id.clone(),
        registration_ref.clone(),
        entry_ref.clone(),
        predecessor.clone(),
        entry.resolution_dependencies.clone(),
        SuccessorLink {
            activation: super::store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                registration_ref.clone(),
                coord.author_owner_grant.clone(),
                anchor.clone(),
            )
            .activation_id(),
            predecessor: predecessor
                .as_ref()
                .map(|reference| reference.object.clone()),
            next_slot,
        },
        &device_signer,
    );
    let head_prefix = membership_head_slot_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
    );
    let head_object = storage.prepare_protocol_object(
        &context,
        current_slot,
        &head_prefix,
        serde_json::to_vec(&head).expect("membership head serialization cannot fail"),
    )?;
    let head_ref = MembershipHeadRef {
        coord,
        head_hash: head.head_hash(),
        object: head_object.reference().clone(),
    };
    let publication = PreparedMembershipPublication {
        entry,
        entry_ref,
        entry_object,
        head,
        head_ref,
        head_object,
    };
    validate_prepared_publication(&publication)?;
    Ok(publication)
}

async fn build_invite_mutation(
    storage: &dyn SyncStorage,
    db: &Database,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &MembershipChain,
    owner_keypair: &UserKeypair,
    stream_id: AuthorStreamId,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    timestamp: &str,
) -> Result<InviteMutationPlan, InviteError> {
    if chain.store_id() != Some(store_id) {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership chain store {:?} differs from requested store {store_id:?}",
            chain.store_id()
        )));
    }
    let invitee_x25519_pk = ed25519_hex_to_x25519(invitee_ed25519_pubkey)?;
    let grant_id =
        chain.next_member_grant_id_in_stream(owner_keypair, stream_id, invitee_ed25519_pubkey)?;
    let membership = if role == MemberRole::Owner {
        let owner_stream = db
            .select_membership_author_stream(invitee_ed25519_pubkey, &grant_id, Default::default())
            .await?;
        let context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let prefix =
            membership_head_slot_prefix(invitee_ed25519_pubkey, &grant_id, owner_stream, 1);
        Some(GrantStreamAnchor::StoreMembership {
            first_slot: storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await?,
        })
    } else {
        None
    };
    let owner_refs = chain.wrapped_key_authority_for(&keys::public_key_hex(owner_keypair))?;
    let authorized_keyring = load_authorized_owner_keyring(
        storage,
        store_root_hash,
        owner_keypair,
        store_id,
        &owner_refs,
        encryption,
    )
    .await?;
    let wrapped_key = prepare_wrapped_store_key(
        storage,
        store_root_hash,
        invitee_ed25519_pubkey,
        signed_wrapped_key(
            store_id,
            invitee_ed25519_pubkey,
            &invitee_x25519_pk,
            &authorized_keyring,
            owner_keypair,
        )?,
    )
    .await?;
    let entry = chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
        owner_keypair,
        stream_id,
        invitee_ed25519_pubkey.to_string(),
        invitee_email.map(str::to_string),
        role.clone(),
        membership,
        wrapped_key.reference.clone(),
        timestamp.to_string(),
    )?;
    let publication =
        prepare_membership_publication(storage, db, store_root_hash, chain, entry, owner_keypair)
            .await?;
    Ok(InviteMutationPlan {
        publication,
        invitee_pubkey: invitee_ed25519_pubkey.to_string(),
        invitee_email: invitee_email.map(str::to_string),
        role,
        desired_access: CloudAccessState::Present {
            member_pubkey: invitee_ed25519_pubkey.to_string(),
            provider_account_email: invitee_email.map(str::to_string),
        },
        wrapped_key,
    })
}

fn invite_plan_matches_request(
    plan: &InviteMutationPlan,
    owner_keypair: &UserKeypair,
    invitee_pubkey: &str,
    invitee_email: Option<&str>,
    role: &MemberRole,
    store_id: &str,
) -> bool {
    plan.publication.entry.author_pubkey == hex::encode(owner_keypair.public_key())
        && plan.publication.entry.store_id == store_id
        && plan.invitee_pubkey == invitee_pubkey
        && plan.invitee_email.as_deref() == invitee_email
        && &plan.role == role
        && plan.desired_access
            == (CloudAccessState::Present {
                member_pubkey: invitee_pubkey.to_string(),
                provider_account_email: invitee_email.map(str::to_string),
            })
        && matches!(
            &plan.publication.entry.change,
            MembershipChange::SetMember {
                user_pubkey,
                provider_account_email,
                role: entry_role,
                ..
            } if user_pubkey == invitee_pubkey
                && provider_account_email.as_deref() == invitee_email
                && entry_role == role
        )
}

async fn execute_invite_mutation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &mut MembershipChain,
    plan: InviteMutationPlan,
    mut progress: MembershipMutationProgress,
    persistence: MutationPersistence<'_>,
) -> Result<(CloudHomeJoinInfo, WrappedStoreKeyRef), InviteError> {
    validate_prepared_publication(&plan.publication)?;
    let mut validated_chain = chain_with_exact_entry(chain, &plan.publication.entry)?;
    let root = persistence
        .db
        .local_store_root_ref()
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation("local Store root reference is absent".to_string())
        })?;
    let author = super::store_objects::load_registration_ref(
        storage,
        &root,
        &plan.publication.head.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    if !plan.publication.head.verify(&author) {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership head has an invalid certified-device signature".to_string(),
        ));
    }
    let wrapped = plan.wrapped_key.validate()?;
    let authority_matches = matches!(
        &plan.publication.entry.change,
        MembershipChange::SetMember { wrapped_key, .. }
            if wrapped_key == &plan.wrapped_key.reference
    );
    if !authority_matches
        || wrapped.author_pubkey != plan.publication.entry.author_pubkey
        || wrapped
            .verify_and_unwrap(
                &plan.publication.entry.store_id,
                &plan.invitee_pubkey,
                std::iter::once(plan.publication.entry.author_pubkey.as_str()),
            )
            .is_err()
    {
        return Err(InviteError::InvalidDurableMutation(
            "planned invitation wrap is not bound to its exact entry, recipient, and author"
                .to_string(),
        ));
    }
    let outcome = cloud_home.set_access(plan.desired_access.clone()).await?;
    let CloudAccessOutcome::Present(observed_join_info) = outcome else {
        return Err(InviteError::InvalidDurableMutation(
            "provider returned absent outcome for present access request".to_string(),
        ));
    };
    let join_info = match progress {
        MembershipMutationProgress::Pending => {
            progress = MembershipMutationProgress::InviteGranted {
                join_info: observed_join_info.clone(),
            };
            persistence.record_progress(&progress).await?;
            observed_join_info
        }
        MembershipMutationProgress::InviteGranted { join_info } => {
            if join_info != observed_join_info {
                return Err(InviteError::InvalidDurableMutation(
                    "provider returned different join information while verifying persisted access"
                        .to_string(),
                ));
            }
            join_info
        }
    };
    super::store_objects::create_exact_object(storage, &plan.wrapped_key.object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::create_exact_object(storage, &plan.publication.entry_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::load_membership_entry_ref(
        storage,
        store_root_hash,
        &plan.publication.entry_ref,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::create_exact_object(storage, &plan.publication.head_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::load_membership_head_ref(
        storage,
        store_root_hash,
        &plan.publication.head_ref,
        &author,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    validated_chain.activate_head_ref(plan.publication.head_ref.clone())?;
    *chain = validated_chain;
    persistence.complete().await?;
    Ok((join_info, plan.wrapped_key.reference))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_invitation_with_encryption_durable(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    store_id: &str,
    timestamp: &str,
    db: &Database,
) -> Result<(CloudHomeJoinInfo, WrappedStoreKeyRef), InviteError> {
    let _mutation = db.lock_membership_mutation().await;
    let (plan, progress, intent_hash) = match db.outbound_membership_mutation().await? {
        Some(row) => {
            let intent_hash = row.intent_hash;
            let (pending, progress) = decode_membership_mutation(row)?;
            let MembershipMutationPlan::Invite(plan) = pending else {
                return Err(InviteError::PendingMutation(
                    "a member removal is pending".to_string(),
                ));
            };
            if !invite_plan_matches_request(
                &plan,
                owner_keypair,
                invitee_ed25519_pubkey,
                invitee_email,
                &role,
                store_id,
            ) {
                return Err(InviteError::PendingMutation(
                    "the pending invitation has different immutable inputs".to_string(),
                ));
            }
            (plan, progress, intent_hash)
        }
        None => {
            let stream_id = select_mutation_author_stream(db, chain, owner_keypair).await?;
            let plan = build_invite_mutation(
                storage,
                db,
                store_root_hash,
                chain,
                owner_keypair,
                stream_id,
                invitee_ed25519_pubkey,
                invitee_email,
                role,
                encryption,
                store_id,
                timestamp,
            )
            .await?;
            let encoded =
                encode_membership_mutation(&MembershipMutationPlan::Invite(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let intent_hash = db
                .stage_membership_mutation(encoded, encode_membership_progress(&progress)?)
                .await?;
            (plan, progress, intent_hash)
        }
    };
    execute_invite_mutation(
        storage,
        cloud_home,
        store_root_hash,
        chain,
        plan,
        progress,
        MutationPersistence { db, intent_hash },
    )
    .await
}

pub async fn unwrap_store_keyring(
    bootstrap_storage: &dyn SyncStorage,
    keypair: &UserKeypair,
    store_root: &super::store_commit::StoreRootRef,
    founder: &str,
    wrapped_key: &WrappedStoreKeyRef,
    membership_floor: &[MembershipHeadRef],
) -> Result<EncryptionService, InviteError> {
    let recipient = hex::encode(keypair.public_key());
    if wrapped_key.recipient_pubkey != recipient {
        return Err(InviteError::Crypto(
            "invite wrapped-key ref names another recipient".to_string(),
        ));
    }
    // Store membership is a signed plaintext control plane: a device must read
    // the authority that selects its current recipient-sealed keys before it has
    // those keys. The joiner reads only, so `watermark_db` is None.
    super::membership_ops::validate_membership_floor(membership_floor)
        .map_err(InviteError::Crypto)?;
    let chain = super::membership_ops::load_exact_anchored_chain(
        bootstrap_storage,
        store_root,
        membership_floor,
        Some(founder),
    )
    .await
    .map_err(|e| InviteError::Crypto(format!("membership chain: {e}")))?;

    let authorized = chain.wrapped_key_authority_for(&recipient)?;
    if !authorized.contains(wrapped_key) {
        return Err(InviteError::Crypto(
            "invite wrapped-key ref is not activated by the anchored membership floor".to_string(),
        ));
    }
    unwrap_store_keyring_for_refs(
        bootstrap_storage,
        store_root.store_root_hash,
        keypair,
        &store_root.store_root_id.to_string(),
        &authorized,
    )
    .await
}

pub async fn unwrap_serial_store_keyring(
    storage: &dyn SyncStorage,
    coordination: &dyn super::storage::CoordinationStorage,
    keypair: &UserKeypair,
    store_root: &super::store_commit::StoreRootRef,
    wrapped_key: &WrappedStoreKeyRef,
    activation: &super::store_commit::StoreBatchCommitRef,
) -> Result<EncryptionService, InviteError> {
    let recipient = hex::encode(keypair.public_key());
    if wrapped_key.recipient_pubkey != recipient {
        return Err(InviteError::Crypto(
            "invite wrapped-key ref names another recipient".to_string(),
        ));
    }
    let semantic_prefix =
        super::store_commit::semantic_prefix_from_exact_object(&activation.object, ".json")
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
    let context = ProtocolObjectContext::signed_plaintext(
        store_root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let bytes = storage
        .read_protocol_object(&context, &activation.object, &semantic_prefix)
        .await?;
    let unverified: super::store_commit::StoreBatchCommit = serde_json::from_slice(&bytes)
        .map_err(|error| InviteError::Crypto(format!("parse Serial invite commit: {error}")))?;
    let author = super::store_objects::load_registration_ref(
        storage,
        store_root,
        &unverified.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    let commit = super::store_commit::StoreBatchCommit::parse_at(
        &bytes,
        store_root.store_root_hash,
        &activation.coord,
        &author,
    )
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    activation
        .verify_commit(&commit)
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    let activated = commit.control().is_some_and(|control| {
        control.serial_membership_entry().is_some_and(|entry| {
            matches!(
                &entry.change,
                super::membership::SerialMembershipChange::SetMember {
                    wrapped_key: authority,
                    ..
                } if authority == wrapped_key
            )
        })
    });
    if !activated {
        return Err(InviteError::Crypto(
            "Serial invite commit does not activate the supplied wrapped-key ref".to_string(),
        ));
    }
    let current =
        super::store_pull::load_serial_cycle_authorization(storage, coordination, store_root)
            .await
            .map_err(|error| InviteError::Crypto(format!("current Serial authority: {error}")))?;
    if !current
        .authorization
        .membership
        .current_members()
        .iter()
        .any(|(pubkey, _)| pubkey == &recipient)
    {
        return Err(InviteError::Crypto(
            "invite recipient is not a current Serial member".to_string(),
        ));
    }
    let current_refs = current.authorization.active_wrapped_keys_for(&recipient);
    unwrap_store_keyring_for_refs(
        storage,
        store_root.store_root_hash,
        keypair,
        &store_root.store_root_id.to_string(),
        &current_refs,
    )
    .await
}

/// Open a sealed box carrying a store keyring to `keypair` and reconstruct the
/// [`EncryptionService`]. `sealed` is the raw sealed-box bytes — the unverified
/// candidate takes them straight off the wrapped key, the authenticated path
/// takes them from [`WrappedStoreKey::verify_and_unwrap`].
fn open_sealed_keyring(
    sealed: &[u8],
    keypair: &UserKeypair,
) -> Result<EncryptionService, InviteError> {
    let plaintext = keys::seal_box_decrypt(sealed, &keypair.to_x25519_secret_key())?;
    EncryptionService::from_keyring_payload(plaintext)
        .map_err(|e| InviteError::Crypto(format!("keyring payload: {e}")))
}

pub(crate) async fn unwrap_store_keyring_for_refs(
    storage: &dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    keypair: &UserKeypair,
    store_id: &str,
    references: &[WrappedStoreKeyRef],
) -> Result<EncryptionService, InviteError> {
    let recipient_hex = hex::encode(keypair.public_key());
    let mut merged: Option<EncryptionService> = None;
    for reference in references {
        if reference.recipient_pubkey != recipient_hex {
            return Err(InviteError::Crypto(
                "activated wrapped-key ref names another recipient".to_string(),
            ));
        }
        let wrapped = load_wrapped_store_key(storage, store_root_hash, reference).await?;
        let sealed = wrapped
            .verify_and_unwrap(
                store_id,
                &recipient_hex,
                std::iter::once(reference.owner_pubkey.as_str()),
            )
            .map_err(|error| InviteError::Crypto(format!("verify wrapped Store key: {error}")))?;
        let keyring = open_sealed_keyring(&sealed, keypair)?;
        if keyring.current_generation() != reference.generation {
            return Err(InviteError::Crypto(format!(
                "wrapped Store-key ref declares generation {}, but its keyring declares {}",
                reference.generation,
                keyring.current_generation(),
            )));
        }
        merged = Some(match merged {
            Some(existing) => existing.merged_with(&keyring),
            None => keyring,
        });
    }
    merged.ok_or_else(|| {
        InviteError::Bucket(StorageError::NotFound(format!(
            "no activated wrapped Store-key ref for {recipient_hex}"
        )))
    })
}

pub(crate) async fn load_authorized_owner_keyring(
    storage: &dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    keypair: &UserKeypair,
    store_id: &str,
    authority_refs: &[WrappedStoreKeyRef],
    initial_keyring: &EncryptionService,
) -> Result<EncryptionService, InviteError> {
    if authority_refs.is_empty() {
        Ok(initial_keyring.clone())
    } else {
        unwrap_store_keyring_for_refs(storage, store_root_hash, keypair, store_id, authority_refs)
            .await
    }
}

/// Revoke a member from the store. This:
/// 1. Revokes access on the cloud home
/// 2. Re-wraps a new store key to all remaining members
/// 3. Publishes the signed Remove membership entry as the visible commit point
///
/// Returns the new encryption key (caller must persist it and start using it).
async fn build_revoke_mutation(
    storage: &dyn SyncStorage,
    db: &Database,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &MembershipChain,
    owner_keypair: &UserKeypair,
    stream_id: AuthorStreamId,
    revokee_pubkey: &str,
    store_id: &str,
    timestamp: &str,
    current_encryption: &EncryptionService,
) -> Result<RevokeMutationPlan, InviteError> {
    if chain.store_id() != Some(store_id) {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership chain store {:?} differs from requested store {store_id:?}",
            chain.store_id()
        )));
    }
    let members = chain.current_members();
    if !members.iter().any(|(pubkey, _)| pubkey == revokee_pubkey) {
        return Err(InviteError::NotAMember(revokee_pubkey.to_string()));
    }
    let current_owners = members
        .iter()
        .filter(|(pubkey, role)| pubkey != revokee_pubkey && *role == MemberRole::Owner)
        .map(|(pubkey, _)| pubkey.clone())
        .collect::<Vec<_>>();
    if current_owners.is_empty() {
        return Err(InviteError::LastOwner);
    }
    let owner_pubkey = keys::public_key_hex(owner_keypair);
    let authority_refs = chain.wrapped_key_authority_for(&owner_pubkey)?;
    let current_keyring = load_authorized_owner_keyring(
        storage,
        store_root_hash,
        owner_keypair,
        store_id,
        &authority_refs,
        current_encryption,
    )
    .await?;
    let new_keyring = current_keyring
        .with_appended_generation(
            current_keyring
                .current_generation()
                .checked_add(1)
                .ok_or_else(|| InviteError::Crypto("store key generation overflow".to_string()))?,
            encryption::generate_random_key(),
        )
        .map_err(|error| InviteError::Crypto(format!("append key generation: {error}")))?;
    let remaining_members = members
        .iter()
        .filter(|(pubkey, _)| pubkey != revokee_pubkey)
        .cloned()
        .collect::<Vec<_>>();
    let mut wraps = Vec::with_capacity(remaining_members.len());
    for (recipient, _) in remaining_members {
        let recipient_key = ed25519_hex_to_x25519(&recipient)?;
        wraps.push(ReplacementWrappedKey {
            prepared: prepare_wrapped_store_key(
                storage,
                store_root_hash,
                &recipient,
                signed_wrapped_key(
                    store_id,
                    &recipient,
                    &recipient_key,
                    &new_keyring,
                    owner_keypair,
                )?,
            )
            .await?,
        });
    }
    wraps.sort_by(|left, right| left.prepared.reference.cmp(&right.prepared.reference));
    let entry = chain.signed_remove_member_with_wrapped_keys_in_stream(
        owner_keypair,
        stream_id,
        revokee_pubkey.to_string(),
        wraps
            .iter()
            .map(|wrap| wrap.prepared.reference.clone())
            .collect(),
        timestamp.to_string(),
    )?;
    let publication =
        prepare_membership_publication(storage, db, store_root_hash, chain, entry, owner_keypair)
            .await?;
    Ok(RevokeMutationPlan {
        publication,
        revokee_pubkey: revokee_pubkey.to_string(),
        desired_access: CloudAccessState::Absent {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email: chain
                .current_member_provider_email(revokee_pubkey)
                .map(str::to_string),
        },
        wraps,
        keyring_payload: new_keyring
            .to_keyring_payload()
            .map_err(|error| InviteError::Crypto(format!("serialize rotated keyring: {error}")))?,
    })
}

fn revoke_plan_matches_request(
    plan: &RevokeMutationPlan,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
) -> bool {
    plan.publication.entry.author_pubkey == hex::encode(owner_keypair.public_key())
        && plan.publication.entry.store_id == store_id
        && plan.revokee_pubkey == revokee_pubkey
        && matches!(
            &plan.publication.entry.change,
            MembershipChange::RemoveMember { user_pubkey, .. }
                if user_pubkey == revokee_pubkey
        )
        && matches!(
            &plan.desired_access,
            CloudAccessState::Absent { member_pubkey, .. }
                if member_pubkey == revokee_pubkey
        )
}

async fn execute_revoke_mutation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &mut MembershipChain,
    plan: RevokeMutationPlan,
    progress: MembershipMutationProgress,
    persistence: MutationPersistence<'_>,
) -> Result<EncryptionService, InviteError> {
    if !matches!(progress, MembershipMutationProgress::Pending) {
        return Err(InviteError::InvalidDurableMutation(
            "removal carries invitation progress".to_string(),
        ));
    }
    validate_prepared_publication(&plan.publication)?;
    let mut validated_chain = chain_with_exact_entry(chain, &plan.publication.entry)?;
    let root = persistence
        .db
        .local_store_root_ref()
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation("local Store root reference is absent".to_string())
        })?;
    let author = super::store_objects::load_registration_ref(
        storage,
        &root,
        &plan.publication.head.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    if !plan.publication.head.verify(&author) {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership head has an invalid certified-device signature".to_string(),
        ));
    }
    let keyring = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
        .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?;
    let remaining = validated_chain.current_members();
    if remaining.len() != plan.wraps.len() {
        return Err(InviteError::InvalidDurableMutation(
            "planned replacement wraps do not cover every remaining member exactly once"
                .to_string(),
        ));
    }
    let mut planned_recipients = std::collections::BTreeSet::new();
    for wrapped in &plan.wraps {
        let reference = &wrapped.prepared.reference;
        if !planned_recipients.insert(reference.recipient_pubkey.clone())
            || !remaining
                .iter()
                .any(|(member_pubkey, _)| member_pubkey == &reference.recipient_pubkey)
        {
            return Err(InviteError::InvalidDurableMutation(format!(
                "planned replacement wrap has duplicate or non-member recipient {}",
                reference.recipient_pubkey
            )));
        }
        let envelope = wrapped.prepared.validate()?;
        if envelope.generation != keyring.current_generation()
            || envelope.author_pubkey != plan.publication.entry.author_pubkey
            || envelope
                .verify_and_unwrap(
                    &plan.publication.entry.store_id,
                    &reference.recipient_pubkey,
                    std::iter::once(plan.publication.entry.author_pubkey.as_str()),
                )
                .is_err()
        {
            return Err(InviteError::InvalidDurableMutation(format!(
                "planned replacement wrap for {} is not bound to the exact removal, generation, recipient, and author",
                reference.recipient_pubkey
            )));
        }
        super::store_objects::create_exact_object(storage, &wrapped.prepared.object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
    }
    let authority_refs = match &plan.publication.entry.change {
        MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys,
        _ => {
            return Err(InviteError::InvalidDurableMutation(
                "planned removal publication is not a removal".to_string(),
            ))
        }
    };
    let planned_refs = plan
        .wraps
        .iter()
        .map(|wrap| wrap.prepared.reference.clone())
        .collect::<Vec<_>>();
    if authority_refs != &planned_refs {
        return Err(InviteError::InvalidDurableMutation(
            "planned removal authority differs from its exact wrapped keys".to_string(),
        ));
    }
    super::store_objects::create_exact_object(storage, &plan.publication.entry_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::load_membership_entry_ref(
        storage,
        store_root_hash,
        &plan.publication.entry_ref,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    match cloud_home.set_access(plan.desired_access.clone()).await? {
        CloudAccessOutcome::Absent(_) => {}
        CloudAccessOutcome::Present(_) => {
            return Err(InviteError::InvalidDurableMutation(
                "provider returned present outcome for absent access request".to_string(),
            ))
        }
    }
    super::store_objects::create_exact_object(storage, &plan.publication.head_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::load_membership_head_ref(
        storage,
        store_root_hash,
        &plan.publication.head_ref,
        &author,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    validated_chain.activate_head_ref(plan.publication.head_ref.clone())?;
    *chain = validated_chain;
    persistence.complete().await?;
    Ok(keyring)
}

async fn complete_already_removed_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &MembershipChain,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
) -> Result<EncryptionService, InviteError> {
    if !chain
        .current_members()
        .into_iter()
        .any(|(_, role)| role == MemberRole::Owner)
    {
        return Err(InviteError::LastOwner);
    }
    let references = chain.wrapped_key_authority_for(&keys::public_key_hex(owner_keypair))?;
    let keyring = unwrap_store_keyring_for_refs(
        storage,
        store_root_hash,
        owner_keypair,
        store_id,
        &references,
    )
    .await?;
    revoke_provider_access(
        cloud_home,
        CloudAccessState::Absent {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email: None,
        },
    )
    .await?;
    Ok(keyring)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn revoke_member_durable(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
    timestamp: &str,
    current_encryption: &EncryptionService,
    db: &Database,
) -> Result<EncryptionService, InviteError> {
    let _mutation = db.lock_membership_mutation().await;
    let (plan, progress, intent_hash) = match db.outbound_membership_mutation().await? {
        Some(row) => {
            let intent_hash = row.intent_hash;
            let (pending, progress) = decode_membership_mutation(row)?;
            let MembershipMutationPlan::Revoke(plan) = pending else {
                return Err(InviteError::PendingMutation(
                    "an invitation is pending".to_string(),
                ));
            };
            if !revoke_plan_matches_request(&plan, owner_keypair, revokee_pubkey, store_id) {
                return Err(InviteError::PendingMutation(
                    "the pending removal has different immutable inputs".to_string(),
                ));
            }
            (plan, progress, intent_hash)
        }
        None => {
            let is_current = chain
                .current_members()
                .iter()
                .any(|(pubkey, _)| pubkey == revokee_pubkey);
            let was_removed = chain.entries().iter().any(|entry| {
                matches!(
                    &entry.change,
                    MembershipChange::RemoveMember { user_pubkey, .. }
                        if user_pubkey == revokee_pubkey
                )
            });
            if !is_current && was_removed {
                return complete_already_removed_member(
                    storage,
                    cloud_home,
                    store_root_hash,
                    chain,
                    owner_keypair,
                    revokee_pubkey,
                    store_id,
                )
                .await;
            }
            let stream_id = select_mutation_author_stream(db, chain, owner_keypair).await?;
            let plan = Box::pin(build_revoke_mutation(
                storage,
                db,
                store_root_hash,
                chain,
                owner_keypair,
                stream_id,
                revokee_pubkey,
                store_id,
                timestamp,
                current_encryption,
            ))
            .await?;
            let encoded =
                encode_membership_mutation(&MembershipMutationPlan::Revoke(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let intent_hash = db
                .stage_membership_mutation(encoded, encode_membership_progress(&progress)?)
                .await?;
            (plan, progress, intent_hash)
        }
    };
    Box::pin(execute_revoke_mutation(
        storage,
        cloud_home,
        store_root_hash,
        chain,
        plan,
        progress,
        MutationPersistence { db, intent_hash },
    ))
    .await
}

async fn revoke_provider_access(
    cloud_home: &dyn CloudHome,
    absent_access: CloudAccessState,
) -> Result<bool, InviteError> {
    match cloud_home.set_access(absent_access).await? {
        CloudAccessOutcome::Absent(RevokeOutcome::Revoked) => Ok(true),
        CloudAccessOutcome::Absent(RevokeOutcome::Unsupported) => {
            tracing::info!(
                "cloud provider offers no per-member credential revocation; chain revocation and store key rotation protect later content",
            );
            Ok(false)
        }
        CloudAccessOutcome::Present(_) => Err(InviteError::Crypto(
            "provider returned present outcome for absent access request".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store_commit::ObjectHash;

    #[tokio::test]
    async fn wrapped_ref_generation_must_match_its_decrypted_keyring() {
        let owner = UserKeypair::generate();
        let recipient = UserKeypair::generate();
        let recipient_pubkey = keys::public_key_hex(&recipient);
        let storage = CloudSyncStorage::new(
            Arc::new(InMemoryCloudHome::new()),
            CloudCipher::Encrypted(EncryptionService::from_key([3; 32])),
            BlobPathScheme::Hashed,
            "wrapped-generation-test",
            owner.clone(),
        )
        .expect("build exact test storage");
        let keyring = EncryptionService::from_key([7; 32]);
        let sealed = keys::seal_box_encrypt(
            &keyring.to_keyring_payload().expect("serialize keyring"),
            &recipient.to_x25519_public_key(),
        );
        let prepared = prepare_wrapped_store_key(
            &storage,
            ObjectHash::digest(b"wrapped generation root"),
            &recipient_pubkey,
            WrappedStoreKey::signed(
                "wrapped-generation-store",
                &recipient_pubkey,
                2,
                sealed,
                &owner,
            ),
        )
        .await
        .expect("prepare mismatched generation wrap");
        storage
            .create_protocol_object(&prepared.object)
            .await
            .expect("create mismatched generation wrap");

        assert!(unwrap_store_keyring_for_refs(
            &storage,
            ObjectHash::digest(b"wrapped generation root"),
            &recipient,
            "wrapped-generation-store",
            &[prepared.reference],
        )
        .await
        .is_err());
    }
}
