use std::sync::Arc;

use crate::config::HomeStorage;
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

use super::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use super::membership::{
    AuthorHead, AuthorStreamId, MemberRole, MembershipChain, MembershipChange, MembershipCoord,
    MembershipEntry, MembershipEntryRef, MembershipError, MembershipHeadRef,
};
use super::storage::{
    PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use super::store_commit::{
    membership_head_slot_prefix, GrantStreamAnchor, StreamActivationId, SuccessorLink,
};
use super::wrapped_store_key::{WrappedKeyActivation, WrappedStoreKey};

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
        "wrapped store key for generation {generation} activation is not visible: {activation:?}"
    )]
    InactiveWrappedKey {
        activation: Box<WrappedKeyActivation>,
        /// The committed generation this inactive wrap names (from its signed
        /// envelope), so a refresh can pause sealing at exactly this generation
        /// without opening the sealed keyring the activation gate forbids adopting.
        generation: u64,
    },
    #[error("wrapped store key activation {actual:?} differs from invite activation {expected:?}")]
    WrappedKeyActivationMismatch {
        expected: Box<WrappedKeyActivation>,
        actual: Option<Box<WrappedKeyActivation>>,
    },
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
    wrapped_key: Vec<u8>,
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

fn visible_membership_activations(
    chain: &MembershipChain,
    additional: Option<super::membership::MembershipCoord>,
) -> Vec<WrappedKeyActivation> {
    chain
        .author_heads()
        .into_iter()
        .chain(additional)
        .map(WrappedKeyActivation::MergeConcurrent)
        .collect()
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplacementWrappedKey {
    recipient: String,
    bytes: Vec<u8>,
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
fn signed_wrapped_key_with_activation(
    store_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
    activation: Option<MembershipCoord>,
) -> Result<Vec<u8>, InviteError> {
    let payload = encryption
        .to_keyring_payload()
        .map_err(|e| InviteError::Crypto(format!("serialize keyring payload: {e}")))?;
    let sealed = keys::seal_box_encrypt(&payload, recipient_x25519_pk);
    let wrapped = WrappedStoreKey::signed(
        store_id,
        recipient_ed25519_pubkey,
        activation.map(WrappedKeyActivation::MergeConcurrent),
        encryption.current_generation(),
        sealed,
        owner_keypair,
    );
    serde_json::to_vec(&wrapped)
        .map_err(|e| InviteError::Crypto(format!("serialize wrapped key: {e}")))
}

pub(crate) fn signed_serial_wrapped_key(
    store_id: &str,
    recipient_ed25519_pubkey: &str,
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
    activation: super::store_commit::CommitPosition,
) -> Result<Vec<u8>, InviteError> {
    let recipient_x25519_pk = ed25519_hex_to_x25519(recipient_ed25519_pubkey)?;
    let payload = encryption
        .to_keyring_payload()
        .map_err(|error| InviteError::Crypto(format!("serialize keyring payload: {error}")))?;
    let sealed = keys::seal_box_encrypt(&payload, &recipient_x25519_pk);
    let wrapped = WrappedStoreKey::signed(
        store_id,
        recipient_ed25519_pubkey,
        Some(WrappedKeyActivation::Serial(activation)),
        encryption.current_generation(),
        sealed,
        owner_keypair,
    );
    serde_json::to_vec(&wrapped)
        .map_err(|error| InviteError::Crypto(format!("serialize wrapped key: {error}")))
}

#[cfg(test)]
pub(crate) fn signed_wrapped_key_for_test(
    store_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption_key: &[u8; 32],
    owner_keypair: &UserKeypair,
) -> Vec<u8> {
    signed_wrapped_keyring_for_test(
        store_id,
        recipient_ed25519_pubkey,
        recipient_x25519_pk,
        &EncryptionService::from_key(*encryption_key),
        owner_keypair,
        None,
    )
}

#[cfg(test)]
pub(crate) fn signed_wrapped_keyring_for_test(
    store_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
    activation: Option<MembershipCoord>,
) -> Vec<u8> {
    signed_wrapped_key_with_activation(
        store_id,
        recipient_ed25519_pubkey,
        recipient_x25519_pk,
        encryption,
        owner_keypair,
        activation,
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
            Some(super::store_commit::GrantStreamAnchor::OwnerRecovery { .. }) => {
                return Err(InviteError::InvalidDurableMutation(format!(
                    "Owner grant {} uses a recovery anchor as its membership stream",
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
    let context =
        ProtocolObjectContext::store(store_root_hash, ProtocolObjectDomain::StoreMembershipHead);
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
            activation: StreamActivationId::store_membership(
                &root,
                &registration_ref,
                &coord.author_owner_grant,
                anchor,
            ),
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
        let context = ProtocolObjectContext::store(
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
    let entry = chain.signed_set_member_with_anchor_in_stream(
        owner_keypair,
        stream_id,
        invitee_ed25519_pubkey.to_string(),
        invitee_email.map(str::to_string),
        role.clone(),
        membership,
        timestamp.to_string(),
    )?;
    let entry_coord = entry.coord();
    let publication =
        prepare_membership_publication(storage, db, store_root_hash, chain, entry, owner_keypair)
            .await?;
    let wrapped_key = signed_wrapped_key_with_activation(
        store_id,
        invitee_ed25519_pubkey,
        &invitee_x25519_pk,
        encryption,
        owner_keypair,
        Some(entry_coord),
    )?;
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
) -> Result<CloudHomeJoinInfo, InviteError> {
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
    let wrapped: WrappedStoreKey = serde_json::from_slice(&plan.wrapped_key).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("parse planned invitation wrap: {error}"))
    })?;
    if wrapped.activation
        != Some(WrappedKeyActivation::MergeConcurrent(
            plan.publication.entry.coord(),
        ))
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
    storage
        .put_wrapped_key(
            &plan.publication.entry.author_pubkey,
            &plan.invitee_pubkey,
            plan.wrapped_key.clone(),
        )
        .await?;
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
    Ok(join_info)
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
) -> Result<CloudHomeJoinInfo, InviteError> {
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
    cloud_home: Arc<dyn CloudHome>,
    keypair: &UserKeypair,
    store_root: &super::store_commit::StoreRootRef,
    store_id: &str,
    founder: &str,
    membership_floor: &[MembershipHeadRef],
) -> Result<EncryptionService, InviteError> {
    // The candidate keyring: enough to read the sealed membership chain, but not
    // yet trusted to be the real, owner-authorized store key.
    let candidate = decrypt_wrapped_store_key_unverified(cloud_home.as_ref(), keypair).await?;

    // Read + anchor the chain to the pinned founder using the candidate key. A
    // candidate that is not the real store key cannot decrypt the sealed chain,
    // and a chain not founded by `founder` is a takeover — both fail closed here,
    // before the key is trusted. The joiner reads only, so `watermark_db` is None.
    let storage = CloudSyncStorage::new(
        cloud_home.clone(),
        CloudCipher::Encrypted(candidate),
        BlobPathScheme::for_storage(HomeStorage::Opaque),
        store_id.to_string(),
        keypair.clone(),
    )?;
    super::membership_ops::validate_membership_floor(membership_floor)
        .map_err(InviteError::Crypto)?;
    let chain = super::membership_ops::load_exact_anchored_chain(
        &storage,
        store_root,
        membership_floor,
        Some(founder),
    )
    .await
    .map_err(|e| InviteError::Crypto(format!("membership chain: {e}")))?;

    // The current Owner set — the same non-temporal fold every other
    // authorization path uses (`current_members` filtered to Owner) — is the
    // authority a wrapped key must be signed by. Judging by current role, not by
    // the position of the joiner's Add, is deliberate: an entry's position is set
    // by its own author-chosen timestamp, so anchoring trust there would let a
    // removed Owner with residual bucket write back-date a fresh Add and resurrect
    // itself as a valid signer. The consequence is correct product behavior — an
    // outstanding invite dies if its inviting Owner is removed or demoted before
    // the join (a removed Owner's invites die with it, and its wrapped key wraps a
    // pre-rotation generation regardless, so the join could not yield the current
    // keyring anyway).
    let owners: Vec<String> = chain
        .current_members()
        .into_iter()
        .filter_map(|(pubkey, role)| (role == MemberRole::Owner).then_some(pubkey))
        .collect();

    // Authenticate the wrapped key against that Owner set — and, if a revoke
    // re-wrapped the slot between invite and join, against the activation's
    // now-visible Remove entry — then decrypt and adopt it.
    let visible: Vec<_> = chain
        .author_heads()
        .into_iter()
        .map(WrappedKeyActivation::MergeConcurrent)
        .collect();
    unwrap_store_keyring_for_owners_with_activation(
        cloud_home.as_ref(),
        keypair,
        store_id,
        owners.iter().map(String::as_str),
        Some(&visible),
    )
    .await
}

pub async fn unwrap_serial_store_keyring(
    cloud_home: Arc<dyn CloudHome>,
    keypair: &UserKeypair,
    store_id: &str,
    key_author_pubkey: &str,
    activation: &super::store_commit::CommitPosition,
) -> Result<EncryptionService, InviteError> {
    let recipient = hex::encode(keypair.public_key());
    let wrapped = fetch_wrapped_key(cloud_home.as_ref(), key_author_pubkey, &recipient).await?;
    if wrapped.activation != Some(WrappedKeyActivation::Serial(activation.clone())) {
        return Err(InviteError::WrappedKeyActivationMismatch {
            expected: Box::new(WrappedKeyActivation::Serial(activation.clone())),
            actual: wrapped.activation.map(Box::new),
        });
    }
    let sealed = wrapped
        .verify_and_unwrap(store_id, &recipient, std::iter::once(key_author_pubkey))
        .map_err(|error| InviteError::Crypto(format!("verify Serial wrapped key: {error}")))?;
    open_sealed_keyring(&sealed, keypair)
}

/// Fetch and parse the wrapped-key object `owner_hex` sealed for `recipient_hex`,
/// off the cloud home. The `.enc` suffix is hardcoded because reading a wrapped
/// store key is an encrypted-home-only path — wrapping a key is meaningful only
/// for a shared (encrypted) home — so `CloudSyncStorage::put_wrapped_key` always
/// wrote the slot at `keys/{owner}/{recipient}.enc`. Read straight off the home,
/// not through `CloudSyncStorage`, which the joiner has not built yet.
async fn fetch_wrapped_key(
    cloud_home: &dyn CloudHome,
    owner_hex: &str,
    recipient_hex: &str,
) -> Result<WrappedStoreKey, InviteError> {
    let wrapped_bytes = cloud_home
        .read(&format!("keys/{owner_hex}/{recipient_hex}.enc"))
        .await?;
    serde_json::from_slice(&wrapped_bytes)
        .map_err(|e| InviteError::Crypto(format!("malformed wrapped key: {e}")))
}

/// The owner prefixes that hold a wrapped-key object for `recipient_hex`, found by
/// scanning the `keys/` keyspace off the cloud home. The joiner's candidate
/// bootstrap uses this: it has no membership chain yet and so cannot name the
/// current owners, so it discovers which prefixes actually hold a wrap for it.
async fn wrap_owners_for_recipient(
    cloud_home: &dyn CloudHome,
    recipient_hex: &str,
) -> Result<Vec<String>, InviteError> {
    let keys = cloud_home.list("keys/").await?;
    let suffix = format!("/{recipient_hex}.enc");
    let mut owners = Vec::new();
    for key in keys {
        let Some(rest) = key.strip_prefix("keys/") else {
            continue;
        };
        let Some(owner) = rest.strip_suffix(&suffix) else {
            continue;
        };
        // The owner segment is a single slash-free hex pubkey token.
        if !owner.is_empty() && !owner.contains('/') {
            owners.push(owner.to_string());
        }
    }
    Ok(owners)
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

/// Open the joiner's own wrapped-key sealed box to a candidate keyring *without*
/// authenticating who signed it. A sealed box authenticates only its recipient,
/// so this proves nothing about authorship — the candidate is only enough to read
/// the membership chain, every entry of which is sealed under the store key.
/// Never adopt the returned keyring without the founder-anchored Owner-set check
/// [`unwrap_store_keyring`] runs; it exists solely to bootstrap that check.
async fn decrypt_wrapped_store_key_unverified(
    cloud_home: &dyn CloudHome,
    keypair: &UserKeypair,
) -> Result<EncryptionService, InviteError> {
    let recipient_hex = hex::encode(keypair.public_key());
    let owners = wrap_owners_for_recipient(cloud_home, &recipient_hex).await?;

    // The candidate is untrusted — it only has to decrypt the sealed membership
    // chain, whose authenticated Owner set the caller then re-derives the real key
    // against. A rotation before the join may have re-wrapped this slot under a
    // non-founder owner, and two owners may have rotated concurrently, so MERGE
    // every wrap that opens: the union holds every key generation any owner wrote,
    // reading the furthest into the sealed chain regardless of which owner sealed
    // a given entry.
    let mut merged: Option<EncryptionService> = None;
    for owner in &owners {
        let wrapped = match fetch_wrapped_key(cloud_home, owner, &recipient_hex).await {
            Ok(wrapped) => wrapped,
            // `owners` came from a listing that just reported this prefix holds a
            // wrap for the recipient, so a keyed miss now is a listed-then-deleted
            // race (or an eventually-consistent listing): note it and move on.
            Err(InviteError::CloudHome(CloudHomeError::NotFound(_))) => {
                tracing::debug!(
                    "candidate wrap {owner}/{recipient_hex} was listed but is now missing; skipping"
                );
                continue;
            }
            Err(e) => return Err(e),
        };
        let Ok(sealed) = hex::decode(&wrapped.sealed) else {
            tracing::debug!(
                "skipping candidate wrap in {owner}'s prefix with a non-hex sealed box"
            );
            continue;
        };
        let candidate = match open_sealed_keyring(&sealed, keypair) {
            Ok(candidate) => candidate,
            Err(e) => {
                tracing::debug!(
                    "skipping candidate wrap in {owner}'s prefix this device cannot open: {e}"
                );
                continue;
            }
        };
        merged = Some(match merged {
            Some(existing) => existing.merged_with(&candidate),
            None => candidate,
        });
    }
    merged.ok_or_else(|| {
        InviteError::CloudHome(CloudHomeError::NotFound(format!(
            "keys/*/{recipient_hex}.enc"
        )))
    })
}

pub(crate) async fn unwrap_store_keyring_for_owners_with_activation<'a>(
    cloud_home: &dyn CloudHome,
    keypair: &UserKeypair,
    store_id: &str,
    expected_owners: impl IntoIterator<Item = &'a str>,
    visible_activations: Option<&[WrappedKeyActivation]>,
) -> Result<EncryptionService, InviteError> {
    let recipient_hex = hex::encode(keypair.public_key());

    // Scan each current owner's prefix for this recipient's wrap and MERGE every
    // one an owner's signature authenticates. An owner writes only into its own
    // prefix, so `keys/{owner}/{recipient}` is authenticated against THAT owner.
    // Two owners rotating at once each wrap a distinct key at the same generation
    // number; merging holds both, so this device can decrypt content sealed by
    // either and, via deterministic seal selection, converges on one seal key
    // rather than partitioning on whichever it happened to read first.
    let mut merged: Option<EncryptionService> = None;
    // Remember why non-adoptable wraps were rejected, so "no wrap adopted" reports
    // the real reason rather than a bare not-found (which the caller treats as
    // "this device has no wrapped key"). For an inactive wrap, keep the highest
    // generation seen: that is the committed generation a refresh must pause at,
    // even if two owners each left an inactive rotation at different generations.
    let mut saw_inactive: Option<(WrappedKeyActivation, u64)> = None;
    let mut saw_unauthentic = false;
    let mut owners_tried = 0usize;

    for owner in expected_owners {
        owners_tried += 1;
        let wrapped = match fetch_wrapped_key(cloud_home, owner, &recipient_hex).await {
            Ok(wrapped) => wrapped,
            // This owner has never wrapped for this recipient — the common shape of
            // a scan across owners, not an anomaly, but noted so a "no wrap found"
            // outcome is traceable to which prefixes were empty.
            Err(InviteError::CloudHome(CloudHomeError::NotFound(_))) => {
                tracing::debug!("no wrapped key for {recipient_hex} under owner {owner}");
                continue;
            }
            Err(e) => return Err(e),
        };

        // A rotated wrap names the Remove entry that must be visible before it is
        // adopted; skip an owner's wrap whose activation the reader can't yet see.
        if let Some(activation) = wrapped.activation.as_ref() {
            let visible = visible_activations
                .is_some_and(|entries| entries.iter().any(|entry| entry == activation));
            if !visible {
                if saw_inactive
                    .as_ref()
                    .is_none_or(|(_, gen)| wrapped.generation > *gen)
                {
                    saw_inactive = Some((activation.clone(), wrapped.generation));
                }
                continue;
            }
        }

        // Authenticate against the owner whose prefix this wrap lives under. Any
        // bucket writer can drop a forged or relocated object into an owner's
        // prefix while no ACL enforces the layout; it fails to verify against that
        // owner and is skipped, so it can neither be adopted nor block a valid wrap
        // under another owner.
        let sealed =
            match wrapped.verify_and_unwrap(store_id, &recipient_hex, std::iter::once(owner)) {
                Ok(sealed) => sealed,
                Err(e) => {
                    tracing::warn!(
                        "skipping wrapped key in {owner}'s prefix that is not authentic: {e}"
                    );
                    saw_unauthentic = true;
                    continue;
                }
            };
        let keyring = match open_sealed_keyring(&sealed, keypair) {
            Ok(keyring) => keyring,
            Err(e) => {
                tracing::warn!("skipping corrupt wrapped key in {owner}'s prefix: {e}");
                saw_unauthentic = true;
                continue;
            }
        };
        merged = Some(match merged {
            Some(existing) => existing.merged_with(&keyring),
            None => keyring,
        });
    }

    if let Some(keyring) = merged {
        return Ok(keyring);
    }
    if let Some((activation, generation)) = saw_inactive {
        return Err(InviteError::InactiveWrappedKey {
            activation: Box::new(activation),
            generation,
        });
    }
    if saw_unauthentic {
        return Err(InviteError::Crypto(format!(
            "no authentic wrapped store key for {recipient_hex} under any current owner"
        )));
    }
    Err(InviteError::CloudHome(CloudHomeError::NotFound(format!(
        "keys/*/{recipient_hex}.enc (no wrap under any of {owners_tried} owner prefixes)"
    ))))
}

/// Revoke a member from the store. This:
/// 1. Revokes access on the cloud home
/// 2. Re-wraps a new store key to all remaining members
/// 3. Deletes the revoked member's wrapped key
/// 4. Publishes the signed Remove membership entry as the visible commit point
///
/// Returns the new encryption key (caller must persist it and start using it).
async fn build_revoke_mutation(
    storage: &dyn SyncStorage,
    db: &Database,
    store_root_hash: super::store_commit::ObjectHash,
    cloud_home: &dyn CloudHome,
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
    let entry = chain.signed_remove_member_in_stream(
        owner_keypair,
        stream_id,
        revokee_pubkey.to_string(),
        timestamp.to_string(),
    )?;
    let remove_coord = entry.coord();
    let mut validated = chain.clone();
    validated.add_entry_at(remove_coord.clone(), entry.clone())?;
    let publication =
        prepare_membership_publication(storage, db, store_root_hash, chain, entry, owner_keypair)
            .await?;
    let author = hex::encode(owner_keypair.public_key());
    let visible_coords = visible_membership_activations(chain, Some(remove_coord.clone()));
    let prior_attempt = match unwrap_store_keyring_for_owners_with_activation(
        cloud_home,
        owner_keypair,
        store_id,
        std::iter::once(author.as_str()),
        Some(&visible_coords),
    )
    .await
    {
        Ok(keyring) if keyring.current_generation() > current_encryption.current_generation() => {
            Some(keyring)
        }
        Ok(_) => None,
        Err(InviteError::CloudHome(CloudHomeError::NotFound(_)))
        | Err(InviteError::InactiveWrappedKey { .. }) => None,
        Err(error) => return Err(error),
    };
    let new_keyring = match prior_attempt {
        Some(prior) => current_encryption.merged_with(&prior),
        None => current_encryption
            .with_appended_generation(
                current_encryption
                    .current_generation()
                    .checked_add(1)
                    .ok_or_else(|| {
                        InviteError::Crypto("store key generation overflow".to_string())
                    })?,
                encryption::generate_random_key(),
            )
            .map_err(|error| InviteError::Crypto(format!("append key generation: {error}")))?,
    };
    let remaining_members = validated.current_members();
    let mut wraps = Vec::with_capacity(remaining_members.len());
    for (recipient, _) in remaining_members {
        let recipient_key = ed25519_hex_to_x25519(&recipient)?;
        wraps.push(ReplacementWrappedKey {
            recipient: recipient.clone(),
            bytes: signed_wrapped_key_with_activation(
                store_id,
                &recipient,
                &recipient_key,
                &new_keyring,
                owner_keypair,
                Some(remove_coord.clone()),
            )?,
        });
    }
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
        if !planned_recipients.insert(wrapped.recipient.clone())
            || !remaining
                .iter()
                .any(|(member_pubkey, _)| member_pubkey == &wrapped.recipient)
        {
            return Err(InviteError::InvalidDurableMutation(format!(
                "planned replacement wrap has duplicate or non-member recipient {}",
                wrapped.recipient
            )));
        }
        let envelope: WrappedStoreKey =
            serde_json::from_slice(&wrapped.bytes).map_err(|error| {
                InviteError::InvalidDurableMutation(format!(
                    "parse planned replacement wrap for {}: {error}",
                    wrapped.recipient
                ))
            })?;
        if envelope.activation
            != Some(WrappedKeyActivation::MergeConcurrent(
                plan.publication.entry.coord(),
            ))
            || envelope.generation != keyring.current_generation()
            || envelope.author_pubkey != plan.publication.entry.author_pubkey
            || envelope
                .verify_and_unwrap(
                    &plan.publication.entry.store_id,
                    &wrapped.recipient,
                    std::iter::once(plan.publication.entry.author_pubkey.as_str()),
                )
                .is_err()
        {
            return Err(InviteError::InvalidDurableMutation(format!(
                "planned replacement wrap for {} is not bound to the exact removal, generation, recipient, and author",
                wrapped.recipient
            )));
        }
        storage
            .put_wrapped_key(
                &plan.publication.entry.author_pubkey,
                &wrapped.recipient,
                wrapped.bytes.clone(),
            )
            .await?;
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
    storage
        .delete_wrapped_key(&plan.publication.entry.author_pubkey, &plan.revokee_pubkey)
        .await?;
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
    chain: &MembershipChain,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
) -> Result<EncryptionService, InviteError> {
    let owners = chain
        .current_members()
        .into_iter()
        .filter_map(|(pubkey, role)| (role == MemberRole::Owner).then_some(pubkey))
        .collect::<Vec<_>>();
    if owners.is_empty() {
        return Err(InviteError::LastOwner);
    }
    let visible = visible_membership_activations(chain, None);
    let keyring = unwrap_store_keyring_for_owners_with_activation(
        cloud_home,
        owner_keypair,
        store_id,
        owners.iter().map(String::as_str),
        Some(&visible),
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
    storage
        .delete_wrapped_key(&keys::public_key_hex(owner_keypair), revokee_pubkey)
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
                    chain,
                    owner_keypair,
                    revokee_pubkey,
                    store_id,
                )
                .await;
            }
            let stream_id = select_mutation_author_stream(db, chain, owner_keypair).await?;
            let plan = build_revoke_mutation(
                storage,
                db,
                store_root_hash,
                cloud_home,
                chain,
                owner_keypair,
                stream_id,
                revokee_pubkey,
                store_id,
                timestamp,
                current_encryption,
            )
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
    execute_revoke_mutation(
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
