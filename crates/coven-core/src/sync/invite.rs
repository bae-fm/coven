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
use super::store_commit::{
    membership_entry_semantic_prefix, membership_head_slot_prefix, SuccessorLink,
};
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
    Resolve(ResolveMutationPlan),
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
    publication: RevokeMembershipPublication,
    revokee_pubkey: String,
    desired_access: CloudAccessState,
    prior_access: CloudAccessState,
    wraps: Vec<ReplacementWrappedKey>,
    keyring_payload: Vec<u8>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveMutationPlan {
    resolution: super::membership::StoreMembershipConflictResolution,
    reference: super::membership::StoreMembershipConflictResolutionRef,
    resolution_object: PreparedExactObject,
    transition: Box<PreparedMembershipTransition>,
    candidate: Box<crate::sync::store_engine::engine::operations::PreparedStoreOperationCommit>,
    publication: Box<PreparedMembershipPublication>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "activation", rename_all = "snake_case", deny_unknown_fields)]
enum RevokeMembershipPublication {
    Direct {
        publication: Box<PreparedMembershipPublication>,
    },
    StoreActivated {
        transition: Box<PreparedMembershipTransition>,
        candidate: Box<crate::sync::store_engine::engine::operations::PreparedStoreOperationCommit>,
        publication: Box<PreparedMembershipPublication>,
    },
}

impl RevokeMembershipPublication {
    fn publication(&self) -> &PreparedMembershipPublication {
        match self {
            Self::Direct { publication } | Self::StoreActivated { publication, .. } => publication,
        }
    }

    fn entry(&self) -> &MembershipEntry {
        &self.publication().entry
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedMembershipPublication {
    pub(crate) entry: MembershipEntry,
    pub(crate) entry_ref: MembershipEntryRef,
    pub(crate) entry_object: PreparedExactObject,
    pub(crate) head: AuthorHead,
    pub(crate) head_ref: MembershipHeadRef,
    pub(crate) head_object: PreparedExactObject,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreparedMembershipTransition {
    pub(crate) entry: MembershipEntry,
    pub(crate) entry_ref: MembershipEntryRef,
    pub(crate) entry_object: PreparedExactObject,
    pub(crate) transition: super::membership::MergeMembershipHeadTransition,
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
    InviteGranted {
        join_info: CloudHomeJoinInfo,
    },
    RevokeAccessRemoved,
    RevokeCandidateNonactivating {
        nonactivation: super::remote_object::CandidateNonactivation,
    },
    ResolutionCandidateNonactivating {
        nonactivation: super::remote_object::CandidateNonactivation,
    },
    RevokeActivated {
        candidate: Option<super::store_commit::StoreBatchCommitRef>,
    },
    ResolutionActivated {
        candidate: super::store_commit::StoreBatchCommitRef,
    },
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
pub(crate) fn ed25519_hex_to_x25519(
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
pub(crate) fn signed_wrapped_key(
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

pub(crate) fn validate_prepared_publication(
    publication: &PreparedMembershipPublication,
) -> Result<(), InviteError> {
    validate_prepared_transition(&PreparedMembershipTransition {
        entry: publication.entry.clone(),
        entry_ref: publication.entry_ref.clone(),
        entry_object: publication.entry_object.clone(),
        transition: super::membership::MergeMembershipHeadTransition {
            body: publication.head.body.clone(),
            head_slot: publication.head_ref.object.slot().clone(),
        },
    })?;
    let coord = publication.entry.coord();
    if publication.entry_ref.coord != coord
        || publication.entry_ref.object != *publication.entry_object.reference()
        || publication.head.body.entry != publication.entry_ref
        || publication.head.entry_coord() != coord
        || publication.head_ref.coord != coord
        || publication.head_ref.head_hash != publication.head.head_hash()
        || publication.head_ref.object != *publication.head_object.reference()
        || publication.head_object.stored_bytes()
            != serde_json::to_vec(&publication.head)
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership publication does not bind one exact entry and head".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_prepared_transition(
    transition: &PreparedMembershipTransition,
) -> Result<(), InviteError> {
    let coord = transition.entry.coord();
    let entry_bytes = serde_json::to_vec(&transition.entry)
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let next_sequence = coord.seq.checked_add(1).ok_or_else(|| {
        InviteError::InvalidDurableMutation("membership sequence is exhausted".to_string())
    })?;
    let entry_key = format!(
        "{}.json",
        membership_entry_semantic_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
            coord.entry_hash,
        )
    );
    let head_key = format!(
        "{}.json",
        membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            coord.seq,
        )
    );
    let successor_key = format!(
        "{}.json",
        membership_head_slot_prefix(
            &coord.author_pubkey,
            &coord.author_owner_grant,
            coord.stream_id,
            next_sequence,
        )
    );
    if transition.entry_ref.coord != transition.entry.coord()
        || transition.entry_ref.object != *transition.entry_object.reference()
        || transition.entry_object.stored_bytes() != entry_bytes
        || transition.entry_ref.object.slot().logical_key() != entry_key
        || transition.transition.body.entry != transition.entry_ref
        || transition.transition.body.resolutions != transition.entry.resolution_dependencies
        || transition.transition.head_slot.logical_key() != head_key
        || transition.transition.body.successor.next_slot.logical_key() != successor_key
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared membership transition does not bind its exact entry".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn prepare_membership_publication(
    storage: &dyn SyncStorage,
    db: &Database,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &MembershipChain,
    entry: MembershipEntry,
    owner_keypair: &UserKeypair,
) -> Result<PreparedMembershipPublication, InviteError> {
    let prepared =
        prepare_membership_transition(storage, db, store_root_hash, chain, entry, owner_keypair)
            .await?;
    finish_membership_transition(
        storage,
        db,
        store_root_hash,
        prepared,
        super::membership::MembershipHeadActivation::Direct,
        owner_keypair,
    )
    .await
}

pub(crate) async fn prepare_membership_transition(
    storage: &dyn SyncStorage,
    db: &Database,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &MembershipChain,
    entry: MembershipEntry,
    owner_keypair: &UserKeypair,
) -> Result<PreparedMembershipTransition, InviteError> {
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await?
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(
                "local Store device registration is absent".to_string(),
            )
        })?;
    let (root, registration_ref, registration, _) =
        crate::sync::store_engine::engine::operations::load_local_store_authority(
            db,
            &device_id,
            owner_keypair,
        )
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
            loaded.value.body.successor.next_slot.clone()
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
    let transition = super::membership::MergeMembershipHeadTransition {
        body: super::membership::MembershipHeadBody {
            author_registration: registration_ref.clone(),
            entry: entry_ref.clone(),
            predecessor: predecessor.clone(),
            resolutions: entry.resolution_dependencies.clone(),
            successor: SuccessorLink {
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
        },
        head_slot: current_slot,
    };
    Ok(PreparedMembershipTransition {
        entry,
        entry_ref,
        entry_object,
        transition,
    })
}

pub(crate) async fn finish_membership_transition(
    storage: &dyn SyncStorage,
    db: &Database,
    store_root_hash: super::store_commit::ObjectHash,
    prepared: PreparedMembershipTransition,
    activation: super::membership::MembershipHeadActivation,
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
        crate::sync::store_engine::engine::operations::load_local_store_authority(
            db,
            &device_id,
            owner_keypair,
        )
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    if root.store_root_hash != store_root_hash
        || registration.author_pubkey != prepared.entry.author_pubkey
        || registration_ref != prepared.transition.body.author_registration
    {
        return Err(InviteError::InvalidDurableMutation(
            "membership transition author differs from the active exact device registration"
                .to_string(),
        ));
    }
    let head = AuthorHead::signed(
        prepared.entry.store_id.clone(),
        prepared.transition.body.clone(),
        activation,
        &device_signer,
    );
    let coord = prepared.entry.coord();
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let head_prefix = membership_head_slot_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
    );
    let head_bytes = serde_json::to_vec(&head).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("serialize membership head: {error}"))
    })?;
    let head_object = storage.prepare_protocol_object(
        &context,
        prepared.transition.head_slot.clone(),
        &head_prefix,
        head_bytes,
    )?;
    let head_ref = MembershipHeadRef {
        coord,
        head_hash: head.head_hash(),
        object: head_object.reference().clone(),
    };
    let publication = PreparedMembershipPublication {
        entry: prepared.entry,
        entry_ref: prepared.entry_ref,
        entry_object: prepared.entry_object,
        head,
        head_ref,
        head_object,
    };
    validate_prepared_publication(&publication)?;
    Ok(publication)
}

pub(crate) async fn publish_prepared_merge_membership_authority(
    storage: &dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    transition: &PreparedMembershipTransition,
    wraps: &[PreparedWrappedStoreKey],
) -> Result<(), InviteError> {
    validate_prepared_transition(transition)?;
    let expected_wraps: Vec<&WrappedStoreKeyRef> = match &transition.entry.change {
        MembershipChange::SetMember { wrapped_key, .. } => vec![wrapped_key],
        MembershipChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.iter().collect(),
        MembershipChange::Founder { .. }
        | MembershipChange::ProviderAdmin
        | MembershipChange::ResolutionActivation { .. } => Vec::new(),
    };
    if expected_wraps.len() != wraps.len()
        || expected_wraps
            .iter()
            .zip(wraps)
            .any(|(expected, prepared)| **expected != prepared.reference)
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared Merge membership wraps differ from their exact transition".to_string(),
        ));
    }
    for prepared in wraps {
        prepared.validate()?;
        super::store_objects::create_exact_object(storage, &prepared.object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        load_wrapped_store_key(storage, store_root_hash, &prepared.reference).await?;
    }
    super::store_objects::create_exact_object(storage, &transition.entry_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::load_membership_entry_ref(
        storage,
        store_root_hash,
        &transition.entry_ref,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    Ok(())
}

pub(crate) async fn publish_prepared_merge_membership_activation(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &super::store_commit::StoreRootRef,
    author: &super::store_commit::StoreDeviceRegistration,
    transition: &PreparedMembershipTransition,
    publication: &PreparedMembershipPublication,
    candidate: Box<crate::sync::store_engine::engine::operations::PreparedStoreOperationCommit>,
    completion: crate::sync::store_engine::engine::operations::StoreMembershipJournalCompletion,
) -> Result<
    crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome,
    InviteError,
> {
    validate_prepared_transition(transition)?;
    validate_prepared_publication(publication)?;
    candidate
        .validate_closed_shape()
        .map_err(InviteError::InvalidDurableMutation)?;
    if candidate.commit.control()
        != Some(&super::store_commit::StoreControl {
            transition: transition.transition.clone(),
        })
        || !transition
            .transition
            .matches_head(&publication.head, &publication.head_ref)
        || !matches!(
            &publication.head.activation,
            super::membership::MembershipHeadActivation::StoreCommit { commit }
                if commit == &candidate.reference
        )
        || !publication.head.verify(author)
    {
        return Err(InviteError::InvalidDurableMutation(
            "prepared Merge membership head differs from its exact Store activation".to_string(),
        ));
    }
    super::store_objects::create_exact_object(storage, &publication.head_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::load_membership_head_ref(
        storage,
        root.store_root_hash,
        &publication.head_ref,
        author,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    db.mark_remote_object_uploaded(
        completion
            .remote_object(&publication.head_ref.object)
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?,
    )
    .await
    .map_err(|error| {
        InviteError::InvalidDurableMutation(format!(
            "record uploaded Merge membership head: {error}"
        ))
    })?;
    let membership_objects = crate::database::VerifiedMergeMembershipObjects::verify(
        &candidate.commit,
        &candidate.reference,
        &transition.entry,
        &publication.head,
        publication.head_ref.clone(),
    )?;
    crate::sync::store_engine::engine::operations::publish_prepared_store_membership_operation(
        db,
        storage,
        candidate,
        membership_objects,
        completion,
    )
    .await
    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
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
    if role == MemberRole::Owner {
        return Err(MembershipError::OwnerPromotionRequired.into());
    }
    if chain.store_id() != Some(store_id) {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership chain store {:?} differs from requested store {store_id:?}",
            chain.store_id()
        )));
    }
    let invitee_x25519_pk = ed25519_hex_to_x25519(invitee_ed25519_pubkey)?;
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
    let signing_store_id = store_id.to_string();
    let signing_recipient = invitee_ed25519_pubkey.to_string();
    let signing_keyring = authorized_keyring.clone();
    let signing_owner = owner_keypair.clone();
    let signed = super::blocking::run(move || {
        signed_wrapped_key(
            &signing_store_id,
            &signing_recipient,
            &invitee_x25519_pk,
            &signing_keyring,
            &signing_owner,
        )
    })
    .await
    .map_err(|error| InviteError::Crypto(format!("seal invited member Store key: {error}")))??;
    let wrapped_key =
        prepare_wrapped_store_key(storage, store_root_hash, invitee_ed25519_pubkey, signed).await?;
    let entry = chain.signed_set_member_with_anchor_and_wrapped_key_in_stream(
        owner_keypair,
        stream_id,
        invitee_ed25519_pubkey.to_string(),
        invitee_email.map(str::to_string),
        role.clone(),
        None,
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
                && entry_role.role() == *role
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
        &plan.publication.head.body.author_registration,
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
        MembershipMutationProgress::RevokeAccessRemoved
        | MembershipMutationProgress::RevokeCandidateNonactivating { .. }
        | MembershipMutationProgress::ResolutionCandidateNonactivating { .. }
        | MembershipMutationProgress::RevokeActivated { .. }
        | MembershipMutationProgress::ResolutionActivated { .. } => {
            return Err(InviteError::InvalidDurableMutation(
                "invitation carries member-removal progress".to_string(),
            ))
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
            let plan = Box::pin(build_invite_mutation(
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
            ))
            .await?;
            let encoded =
                encode_membership_mutation(&MembershipMutationPlan::Invite(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let intent_hash = db
                .stage_membership_mutation(encoded, encode_membership_progress(&progress)?, None)
                .await?;
            (plan, progress, intent_hash)
        }
    };
    Box::pin(execute_invite_mutation(
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
    let wrapped_keys = wraps
        .iter()
        .map(|wrap| wrap.prepared.reference.clone())
        .collect();
    let publication = if chain.is_owner_now(revokee_pubkey) {
        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or_else(|| {
                InviteError::InvalidDurableMutation(
                    "local Store device registration is absent".to_string(),
                )
            })?;
        let operation = Box::pin(crate::sync::store_engine::engine::operations::prepare_plan(
            db,
            storage,
            chain,
            &device_id,
            owner_keypair,
        ))
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let entry = chain.signed_remove_member_with_owner_barrier_state(
            owner_keypair,
            stream_id,
            revokee_pubkey.to_string(),
            wrapped_keys,
            operation.device_state().clone(),
            timestamp.to_string(),
        )?;
        let transition = prepare_membership_transition(
            storage,
            db,
            store_root_hash,
            chain,
            entry,
            owner_keypair,
        )
        .await?;
        let mut candidate = Box::pin(crate::sync::store_engine::engine::operations::prepare_candidate(
            db,
            storage,
            operation,
            crate::sync::store_engine::engine::operations::StoreOperationBatch::MergeMembershipActivation {
                transition: transition.transition.clone(),
                stream_activations: Vec::new(),
            },
        ))
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        let head = finish_membership_transition(
            storage,
            db,
            store_root_hash,
            transition.clone(),
            super::membership::MembershipHeadActivation::StoreCommit {
                commit: candidate.reference.clone(),
            },
            owner_keypair,
        )
        .await?;
        candidate
            .attach_merge_membership_proof(storage, &head, None, owner_keypair)
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        RevokeMembershipPublication::StoreActivated {
            transition: Box::new(transition),
            candidate: Box::new(candidate),
            publication: Box::new(head),
        }
    } else {
        let entry = chain.signed_remove_member_with_wrapped_keys_in_stream(
            owner_keypair,
            stream_id,
            revokee_pubkey.to_string(),
            wrapped_keys,
            timestamp.to_string(),
        )?;
        RevokeMembershipPublication::Direct {
            publication: Box::new(
                prepare_membership_publication(
                    storage,
                    db,
                    store_root_hash,
                    chain,
                    entry,
                    owner_keypair,
                )
                .await?,
            ),
        }
    };
    let provider_account_email = chain
        .current_member_provider_email(revokee_pubkey)
        .map(str::to_string);
    Ok(RevokeMutationPlan {
        publication,
        revokee_pubkey: revokee_pubkey.to_string(),
        desired_access: CloudAccessState::Absent {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email: provider_account_email.clone(),
        },
        prior_access: CloudAccessState::Present {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email,
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
    plan.publication.entry().author_pubkey == hex::encode(owner_keypair.public_key())
        && plan.publication.entry().store_id == store_id
        && plan.revokee_pubkey == revokee_pubkey
        && matches!(
            &plan.publication.entry().change,
            MembershipChange::RemoveMember { user_pubkey, .. }
                if user_pubkey == revokee_pubkey
        )
        && matches!(
            &plan.desired_access,
            CloudAccessState::Absent { member_pubkey, .. }
                if member_pubkey == revokee_pubkey
        )
        && matches!(
            &plan.prior_access,
            CloudAccessState::Present { member_pubkey, .. }
                if member_pubkey == revokee_pubkey
        )
}

impl RevokeMutationPlan {
    fn validate_closed_shape(&self) -> Result<(), InviteError> {
        let publication = self.publication.publication();
        validate_prepared_publication(publication)?;
        let (desired_member, desired_email) = match &self.desired_access {
            CloudAccessState::Absent {
                member_pubkey,
                provider_account_email,
            } => (member_pubkey, provider_account_email),
            CloudAccessState::Present { .. } => {
                return Err(InviteError::InvalidDurableMutation(
                    "membership removal requests present provider access".to_string(),
                ))
            }
        };
        let (prior_member, prior_email) = match &self.prior_access {
            CloudAccessState::Present {
                member_pubkey,
                provider_account_email,
            } => (member_pubkey, provider_account_email),
            CloudAccessState::Absent { .. } => {
                return Err(InviteError::InvalidDurableMutation(
                    "membership removal compensation requests absent provider access".to_string(),
                ))
            }
        };
        if desired_member != &self.revokee_pubkey
            || prior_member != &self.revokee_pubkey
            || desired_email != prior_email
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal access and compensation intents disagree".to_string(),
            ));
        }
        let MembershipChange::RemoveMember {
            user_pubkey,
            wrapped_keys,
            retirement_device_state,
            retirement_barriers,
            ..
        } = &publication.entry.change
        else {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal plan contains another change".to_string(),
            ));
        };
        let planned_wraps = self
            .wraps
            .iter()
            .map(|wrap| wrap.prepared.reference.clone())
            .collect::<Vec<_>>();
        if user_pubkey != &self.revokee_pubkey || wrapped_keys != &planned_wraps {
            return Err(InviteError::InvalidDurableMutation(
                "membership removal plan differs from its exact entry".to_string(),
            ));
        }
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => {
                if retirement_device_state.is_some()
                    || retirement_barriers.values().any(|barrier| {
                        matches!(
                            barrier,
                            super::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                        )
                    })
                    || !matches!(
                        publication.head.activation,
                        super::membership::MembershipHeadActivation::Direct
                    )
                {
                    return Err(InviteError::InvalidDurableMutation(
                        "direct membership removal carries Owner retirement authority".to_string(),
                    ));
                }
            }
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                ..
            } => {
                validate_prepared_transition(transition)?;
                candidate
                    .validate_closed_shape()
                    .map_err(InviteError::InvalidDurableMutation)?;
                if transition.entry != publication.entry
                    || transition.entry_ref != publication.entry_ref
                    || transition.entry_object != publication.entry_object
                    || retirement_device_state.as_ref() != Some(&candidate.commit.device_state)
                    || !retirement_barriers.values().any(|barrier| {
                        matches!(
                            barrier,
                            super::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                        )
                    })
                    || candidate.commit.control()
                        != Some(&super::store_commit::StoreControl {
                            transition: transition.transition.clone(),
                        })
                    || !transition
                        .transition
                        .matches_head(&publication.head, &publication.head_ref)
                    || !matches!(
                        &publication.head.activation,
                        super::membership::MembershipHeadActivation::StoreCommit { commit }
                            if commit == &candidate.reference
                    )
                {
                    return Err(InviteError::InvalidDurableMutation(
                        "Owner retirement differs from its exact Store activation graph"
                            .to_string(),
                    ));
                }
                candidate
                    .merge_membership_activation_remote_objects(
                        transition,
                        publication,
                        &self
                            .wraps
                            .iter()
                            .map(|wrap| wrap.prepared.clone())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
            }
        }
        Ok(())
    }

    fn candidate_remote_objects(
        &self,
    ) -> Result<Option<Vec<super::remote_object::RemoteObjectRecord>>, InviteError> {
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => Ok(None),
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                publication,
            } => candidate
                .merge_membership_activation_remote_objects(
                    transition,
                    publication,
                    &self
                        .wraps
                        .iter()
                        .map(|wrap| wrap.prepared.clone())
                        .collect::<Vec<_>>(),
                )
                .map(Some)
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string())),
        }
    }

    fn candidate_cleanup_objects(
        &self,
    ) -> (
        Vec<super::storage::ExactObjectRef>,
        Vec<super::storage::ExactObjectRef>,
    ) {
        match &self.publication {
            RevokeMembershipPublication::Direct { .. } => (Vec::new(), Vec::new()),
            RevokeMembershipPublication::StoreActivated {
                transition,
                candidate,
                publication,
            } => {
                let candidate_objects = std::iter::once(candidate.reference.object.clone())
                    .chain(std::iter::once(transition.entry_ref.object.clone()))
                    .chain(std::iter::once(publication.head_ref.object.clone()))
                    .chain(
                        self.wraps
                            .iter()
                            .map(|wrap| wrap.prepared.reference.object.clone()),
                    )
                    .collect();
                let retained = vec![candidate.head_ref().object];
                (candidate_objects, retained)
            }
        }
    }
}

fn exact_owned_remote(
    remotes: &[super::remote_object::RemoteObjectRecord],
    object: &super::storage::ExactObjectRef,
) -> Result<super::remote_object::RemoteObjectRecord, InviteError> {
    let mut matching = remotes.iter().filter(|remote| remote.object() == object);
    let remote = matching.next().cloned().ok_or_else(|| {
        InviteError::InvalidDurableMutation(format!(
            "membership candidate does not own exact object {}",
            object.slot().logical_key()
        ))
    })?;
    if matching.next().is_some() {
        return Err(InviteError::InvalidDurableMutation(format!(
            "membership candidate repeats exact object {}",
            object.slot().logical_key()
        )));
    }
    Ok(remote)
}

impl ResolveMutationPlan {
    fn remote_objects(&self) -> Result<Vec<super::remote_object::RemoteObjectRecord>, InviteError> {
        self.candidate
            .merge_membership_resolution_remote_objects(
                &self.transition,
                &self.publication,
                &self.resolution,
                &self.reference,
                &self.resolution_object,
            )
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))
    }

    fn validate_closed_shape(&self) -> Result<(), InviteError> {
        validate_prepared_transition(&self.transition)?;
        validate_prepared_publication(&self.publication)?;
        if !self.resolution.verify_signature()
            || self.reference
                != self
                    .resolution
                    .resolution_ref(self.resolution_object.reference().clone())
            || self.resolution_object.stored_bytes()
                != serde_json::to_vec(&self.resolution).map_err(|error| {
                    InviteError::InvalidDurableMutation(format!(
                        "serialize membership resolution: {error}"
                    ))
                })?
            || self.transition.entry != self.publication.entry
            || self.transition.entry_ref != self.publication.entry_ref
            || self.transition.entry_object != self.publication.entry_object
            || self.candidate.commit.control()
                != Some(&super::store_commit::StoreControl {
                    transition: self.transition.transition.clone(),
                })
            || !matches!(
                &self.publication.entry.change,
                MembershipChange::ResolutionActivation { resolution }
                    if resolution == &self.reference
            )
            || !matches!(
                &self.publication.head.activation,
                super::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == &self.candidate.reference
            )
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership resolution plan violates its exact activation graph".to_string(),
            ));
        }
        self.remote_objects()?;
        Ok(())
    }
}

async fn build_resolution_mutation(
    storage: &dyn SyncStorage,
    db: &Database,
    chain: &MembershipChain,
    signer: &UserKeypair,
    device_id: &str,
    conflict_hash: super::store_commit::ObjectHash,
    resolver_branch_heads: Vec<MembershipHeadRef>,
    created_at: &str,
) -> Result<ResolveMutationPlan, InviteError> {
    let base =
        crate::sync::store_engine::engine::operations::prepare_merge_conflict_resolution_commit(
            db,
            storage,
            device_id,
            signer,
            chain.head_refs(),
        )
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let chain = base.membership();
    let super::membership::MembershipStatus::Conflict(
        super::membership::MembershipConflict::RevocationCycle {
            conflict_hash: current,
            maximal_valid_branches,
            ..
        },
    ) = chain.status()
    else {
        return Err(MembershipError::Conflict.into());
    };
    if *current != conflict_hash
        || !maximal_valid_branches
            .iter()
            .any(|branch| branch.heads == resolver_branch_heads)
    {
        return Err(MembershipError::InvalidConflictResolution.into());
    }
    let resolver_pubkey = keys::public_key_hex(signer);
    let replacement_grant =
        super::membership::derive_store_resolution_grant(&conflict_hash, &resolver_pubkey);
    let stream_id = super::store_commit::StreamActivation::grant_authorized_stream_id(
        base.root().store_root_hash,
        base.registration_ref(),
        &replacement_grant,
        super::store_commit::StreamAnchorDomain::StoreMembership,
    );
    let membership_context = ProtocolObjectContext::signed_plaintext(
        base.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let membership_slot = storage
        .allocate_protocol_slot(
            &membership_context,
            &membership_head_slot_prefix(&resolver_pubkey, &replacement_grant, stream_id, 1),
            ".json",
        )
        .await?;
    let recovery_context = ProtocolObjectContext::signed_plaintext(
        base.root().store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let recovery_slot = storage
        .allocate_protocol_slot(
            &recovery_context,
            &super::store_commit::owner_recovery_semantic_prefix(
                &resolver_pubkey,
                replacement_grant.clone(),
                1,
            ),
            ".json",
        )
        .await?;
    let membership = super::store_commit::GrantStreamAnchor::StoreMembership {
        first_slot: membership_slot,
    };
    let recovery = super::store_commit::GrantStreamAnchor::OwnerRecovery {
        first_slot: recovery_slot,
    };
    let acceptance = super::store_commit::OwnerConflictResolutionAcceptance::signed(
        base.root().store_root_hash,
        replacement_grant,
        base.registration_ref().clone(),
        membership.clone(),
        recovery,
        base.device_state().clone(),
        base.registration(),
        signer,
    )
    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let resolution = chain.signed_cycle_resolution(
        base.root().store_root_hash,
        resolver_branch_heads,
        membership,
        acceptance,
        signer,
    )?;
    let resolution_bytes = serde_json::to_vec(&resolution).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("serialize membership resolution: {error}"))
    })?;
    let resolution_context = ProtocolObjectContext::signed_plaintext(
        base.root().store_root_hash,
        ProtocolObjectDomain::StoreMembershipResolution,
    );
    let resolution_hash = resolution.resolution_hash();
    let resolution_prefix = super::store_commit::membership_resolution_semantic_prefix(
        conflict_hash,
        &resolver_pubkey,
        resolution_hash,
    );
    let resolution_slot = storage
        .allocate_protocol_slot(&resolution_context, &resolution_prefix, ".json")
        .await?;
    let resolution_object = storage.prepare_protocol_object(
        &resolution_context,
        resolution_slot,
        &resolution_prefix,
        resolution_bytes,
    )?;
    let reference = resolution.resolution_ref(resolution_object.reference().clone());
    let mut resolved_chain = chain.clone();
    resolved_chain.apply_resolutions(
        base.root().store_root_hash,
        &[(reference.clone(), resolution.clone())],
    )?;
    let entry = resolved_chain.signed_resolution_activation_in_stream(
        base.root().store_root_hash,
        signer,
        stream_id,
        reference.clone(),
        &resolution,
        created_at.to_string(),
    )?;
    let transition = prepare_membership_transition(
        storage,
        db,
        base.root().store_root_hash,
        &resolved_chain,
        entry,
        signer,
    )
    .await?;
    let operation = base
        .finish(&resolved_chain, &reference)
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let mut stream_activations = vec![
        super::store_commit::StreamActivation::grant_authorized(
            resolution.store_root_hash,
            resolution.replacement_acceptance.owner_registration.clone(),
            resolution.replacement_grant.clone(),
            resolution.replacement_acceptance.membership.clone(),
        ),
        super::store_commit::StreamActivation::grant_authorized(
            resolution.store_root_hash,
            resolution.replacement_acceptance.owner_registration.clone(),
            resolution.replacement_grant.clone(),
            resolution.replacement_acceptance.recovery.clone(),
        ),
    ];
    stream_activations.sort();
    let mut candidate = crate::sync::store_engine::engine::operations::prepare_candidate(
        db,
        storage,
        operation,
        crate::sync::store_engine::engine::operations::StoreOperationBatch::MergeMembershipActivation {
            transition: transition.transition.clone(),
            stream_activations,
        },
    )
    .await
    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let publication = finish_membership_transition(
        storage,
        db,
        resolution.store_root_hash,
        transition.clone(),
        super::membership::MembershipHeadActivation::StoreCommit {
            commit: candidate.reference.clone(),
        },
        signer,
    )
    .await?;
    candidate
        .attach_merge_membership_proof(storage, &publication, Some(&resolution), signer)
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    let plan = ResolveMutationPlan {
        resolution,
        reference,
        resolution_object,
        transition: Box::new(transition),
        candidate: Box::new(candidate),
        publication: Box::new(publication),
    };
    plan.validate_closed_shape()?;
    Ok(plan)
}

async fn finish_nonactivating_revoke(
    plan: &RevokeMutationPlan,
    persistence: &MutationPersistence<'_>,
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
) -> Result<(), InviteError> {
    let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication else {
        return Err(InviteError::InvalidDurableMutation(
            "direct membership removal has no candidate cleanup".to_string(),
        ));
    };
    let (candidate_objects, _) = plan.candidate_cleanup_objects();
    let cleanup = persistence
        .db
        .membership_candidate_cleanup_targets(
            persistence.intent_hash,
            candidate.reference.clone(),
            candidate_objects,
        )
        .await?;
    finish_nonactivating_revoke_with_targets(plan, persistence, storage, cloud_home, cleanup).await
}

async fn finish_nonactivating_revoke_with_targets(
    plan: &RevokeMutationPlan,
    persistence: &MutationPersistence<'_>,
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    cleanup: Vec<crate::database::CandidateCleanupObject>,
) -> Result<(), InviteError> {
    let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication else {
        return Err(InviteError::InvalidDurableMutation(
            "direct membership removal has no candidate terminalization".to_string(),
        ));
    };
    match cloud_home.set_access(plan.prior_access.clone()).await? {
        CloudAccessOutcome::Present(_) => {}
        CloudAccessOutcome::Absent(_) => {
            return Err(InviteError::InvalidDurableMutation(
                "provider returned absent while restoring a nonactivated removal".to_string(),
            ))
        }
    }
    for target in cleanup {
        super::store_objects::delete_exact_object(storage, &target.object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        persistence
            .db
            .mark_candidate_cleanup_absent(target.object)
            .await?;
    }
    let (candidate_objects, retained) = plan.candidate_cleanup_objects();
    persistence
        .db
        .complete_nonactivating_membership_candidate_mutation(
            persistence.intent_hash,
            candidate.reference.clone(),
            candidate_objects,
            retained,
            Some(
                EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                    .map_err(|error| {
                        InviteError::Crypto(format!("parse rotated keyring: {error}"))
                    })?
                    .current_generation(),
            ),
        )
        .await?;
    Ok(())
}

async fn execute_revoke_mutation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &mut MembershipChain,
    mut plan: RevokeMutationPlan,
    mut progress: MembershipMutationProgress,
    mut persistence: MutationPersistence<'_>,
    pending_rotation: &super::cloud_storage::PendingRotation,
) -> Result<EncryptionService, InviteError> {
    plan.validate_closed_shape()?;
    if matches!(progress, MembershipMutationProgress::InviteGranted { .. }) {
        return Err(InviteError::InvalidDurableMutation(
            "removal carries invitation progress".to_string(),
        ));
    }
    let mut validated_chain = chain_with_exact_entry(chain, plan.publication.entry())?;
    if let MembershipMutationProgress::RevokeActivated { candidate } = &progress {
        let (expected, publication) = match &plan.publication {
            RevokeMembershipPublication::Direct { publication } => (None, publication),
            RevokeMembershipPublication::StoreActivated {
                candidate,
                publication,
                ..
            } => (Some(&candidate.reference), publication),
        };
        if candidate.as_ref() != expected {
            return Err(InviteError::InvalidDurableMutation(
                "membership activation names another candidate".to_string(),
            ));
        }
        validated_chain.activate_head_ref(publication.head_ref.clone())?;
        *chain = validated_chain;
        return EncryptionService::from_keyring_payload(plan.keyring_payload)
            .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")));
    }
    if let MembershipMutationProgress::RevokeCandidateNonactivating { nonactivation } = &progress {
        let RevokeMembershipPublication::StoreActivated { candidate, .. } = &plan.publication
        else {
            return Err(InviteError::InvalidDurableMutation(
                "direct removal carries Store-candidate nonactivation".to_string(),
            ));
        };
        nonactivation
            .validate()
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
        if nonactivation
            .reference()
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
            != candidate.reference
        {
            return Err(InviteError::InvalidDurableMutation(
                "membership nonactivation names another candidate".to_string(),
            ));
        }
        finish_nonactivating_revoke(&plan, &persistence, storage, cloud_home).await?;
        let generation = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
            .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?
            .current_generation();
        pending_rotation
            .remove_candidate(generation, persistence.intent_hash)
            .map_err(InviteError::InvalidDurableMutation)?;
        return Err(InviteError::InvalidDurableMutation(
            "membership removal candidate did not activate".to_string(),
        ));
    }
    let publication = plan.publication.publication().clone();
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
        &publication.head.body.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    if !publication.head.verify(&author) {
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
            || envelope.author_pubkey != publication.entry.author_pubkey
            || envelope
                .verify_and_unwrap(
                    &publication.entry.store_id,
                    &reference.recipient_pubkey,
                    std::iter::once(publication.entry.author_pubkey.as_str()),
                )
                .is_err()
        {
            return Err(InviteError::InvalidDurableMutation(format!(
                "planned replacement wrap for {} is not bound to the exact removal, generation, recipient, and author",
                reference.recipient_pubkey
            )));
        }
    }
    let authority_refs = match &publication.entry.change {
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
    let remote_objects = plan.candidate_remote_objects()?;
    for wrapped in &plan.wraps {
        if let Some(remotes) = &remote_objects {
            let expected = exact_owned_remote(remotes, &wrapped.prepared.reference.object)?;
            persistence.db.mark_remote_object_uploaded(expected).await?;
        }
    }
    match &plan.publication {
        RevokeMembershipPublication::Direct { .. } => {
            for wrapped in &plan.wraps {
                super::store_objects::create_exact_object(storage, &wrapped.prepared.object)
                    .await
                    .map_err(|error| InviteError::Crypto(error.to_string()))?;
                load_wrapped_store_key(storage, store_root_hash, &wrapped.prepared.reference)
                    .await?;
            }
            super::store_objects::create_exact_object(storage, &publication.entry_object)
                .await
                .map_err(|error| InviteError::Crypto(error.to_string()))?;
            super::store_objects::load_membership_entry_ref(
                storage,
                store_root_hash,
                &publication.entry_ref,
            )
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        }
        RevokeMembershipPublication::StoreActivated { transition, .. } => {
            publish_prepared_merge_membership_authority(
                storage,
                store_root_hash,
                transition,
                &plan
                    .wraps
                    .iter()
                    .map(|wrap| wrap.prepared.clone())
                    .collect::<Vec<_>>(),
            )
            .await?;
        }
    }
    if let Some(remotes) = &remote_objects {
        let expected = exact_owned_remote(remotes, &publication.entry_ref.object)?;
        persistence.db.mark_remote_object_uploaded(expected).await?;
    }
    match cloud_home.set_access(plan.desired_access.clone()).await? {
        CloudAccessOutcome::Absent(_) => {}
        CloudAccessOutcome::Present(_) => {
            return Err(InviteError::InvalidDurableMutation(
                "provider returned present outcome for absent access request".to_string(),
            ))
        }
    }
    if matches!(progress, MembershipMutationProgress::Pending) {
        progress = MembershipMutationProgress::RevokeAccessRemoved;
        persistence.record_progress(&progress).await?;
    }
    match plan.publication.clone() {
        RevokeMembershipPublication::Direct { publication } => {
            super::store_objects::create_exact_object(storage, &publication.head_object)
                .await
                .map_err(|error| InviteError::Crypto(error.to_string()))?;
            super::store_objects::load_membership_head_ref(
                storage,
                store_root_hash,
                &publication.head_ref,
                &author,
            )
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
            validated_chain.activate_head_ref(publication.head_ref.clone())?;
            persistence
                .db
                .record_direct_revoke_activation(
                    persistence.intent_hash,
                    encode_membership_progress(&MembershipMutationProgress::RevokeActivated {
                        candidate: None,
                    })?,
                    keyring.current_generation(),
                )
                .await?;
            pending_rotation
                .mark_committed_mutation(keyring.current_generation(), persistence.intent_hash)
                .map_err(InviteError::InvalidDurableMutation)?;
            *chain = validated_chain;
            Ok(keyring)
        }
        RevokeMembershipPublication::StoreActivated {
            transition,
            mut candidate,
            publication,
        } => {
            let initial_remotes = candidate
                .merge_membership_activation_remote_objects(
                    &transition,
                    &publication,
                    &plan
                        .wraps
                        .iter()
                        .map(|wrap| wrap.prepared.clone())
                        .collect::<Vec<_>>(),
                )
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
            super::store_engine::engine::operations::upload_commit(storage, &candidate)
                .await
                .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
            persistence
                .db
                .mark_remote_object_uploaded(exact_owned_remote(
                    &initial_remotes,
                    &candidate.reference.object,
                )?)
                .await?;
            loop {
                let previous_candidate = candidate.as_ref().clone();
                let current_remotes = candidate
                    .merge_membership_activation_remote_objects(
                        &transition,
                        &publication,
                        &plan
                            .wraps
                            .iter()
                            .map(|wrap| wrap.prepared.clone())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
                let outcome = Box::pin(publish_prepared_merge_membership_activation(
                    persistence.db,
                    storage,
                    &root,
                    &author,
                    &transition,
                    &publication,
                    candidate.clone(),
                    crate::sync::store_engine::engine::operations::StoreMembershipJournalCompletion::RotationMutation {
                        intent_hash: persistence.intent_hash,
                        progress_bytes: encode_membership_progress(
                            &MembershipMutationProgress::RevokeActivated {
                                candidate: Some(candidate.reference.clone()),
                            },
                        )?,
                        generation: keyring.current_generation(),
                        remote_objects: current_remotes.clone(),
                    },
                ))
                .await?;
                match outcome {
                    crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::Activated(reference) => {
                        if reference != candidate.reference {
                            return Err(InviteError::InvalidDurableMutation(
                                "membership removal activated another Store candidate".to_string(),
                            ));
                        }
                        validated_chain.activate_head_ref(publication.head_ref.clone())?;
                        pending_rotation
                            .mark_committed_mutation(
                                keyring.current_generation(),
                                persistence.intent_hash,
                            )
                            .map_err(InviteError::InvalidDurableMutation)?;
                        *chain = validated_chain;
                        return Ok(keyring);
                    }
                    crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::RepreparedCandidate(
                        replacement,
                    ) => {
                        if replacement.reference != candidate.reference {
                            return Err(InviteError::InvalidDurableMutation(
                                "membership removal reprepare changed its signed candidate"
                                    .to_string(),
                            ));
                        }
                        let previous_remotes = previous_candidate
                            .merge_membership_activation_remote_objects(
                                &transition,
                                &publication,
                                &plan
                                    .wraps
                                    .iter()
                                    .map(|wrap| wrap.prepared.clone())
                                    .collect::<Vec<_>>(),
                            )
                            .map_err(|error| {
                                InviteError::InvalidDurableMutation(error.to_string())
                            })?;
                        candidate = replacement;
                        let replacement_remotes = candidate
                            .merge_membership_activation_remote_objects(
                                &transition,
                                &publication,
                                &plan
                                    .wraps
                                    .iter()
                                    .map(|wrap| wrap.prepared.clone())
                                    .collect::<Vec<_>>(),
                            )
                            .map_err(|error| {
                                InviteError::InvalidDurableMutation(error.to_string())
                            })?;
                        let previous_head = previous_candidate.head_ref();
                        let replacement_head = candidate.head_ref();
                        plan.publication = RevokeMembershipPublication::StoreActivated {
                            transition: transition.clone(),
                            candidate: candidate.clone(),
                            publication: publication.clone(),
                        };
                        let plan_bytes = encode_membership_mutation(
                            &MembershipMutationPlan::Revoke(plan.clone()),
                        )?;
                        let previous_intent_hash = persistence.intent_hash;
                        let replacement_intent_hash = persistence
                            .db
                            .adopt_merge_membership_candidate_head(
                                persistence.intent_hash,
                                plan_bytes,
                                exact_owned_remote(&previous_remotes, &previous_head.object)?,
                                exact_owned_remote(&replacement_remotes, &replacement_head.object)?,
                                Some(keyring.current_generation()),
                            )
                            .await?;
                        pending_rotation
                            .replace_candidate_mutation(
                                keyring.current_generation(),
                                previous_intent_hash,
                                replacement_intent_hash,
                            )
                            .map_err(InviteError::InvalidDurableMutation)?;
                        persistence.intent_hash = replacement_intent_hash;
                    }
                    crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                        candidate: returned,
                        nonactivation,
                    } => {
                        if *returned != *candidate {
                            return Err(InviteError::InvalidDurableMutation(
                                "membership removal nonactivation returned another candidate"
                                    .to_string(),
                            ));
                        }
                        let verified = *nonactivation;
                        let durable = verified.clone().into_durable();
                        progress = MembershipMutationProgress::RevokeCandidateNonactivating {
                            nonactivation: durable,
                        };
                        let progress_bytes = encode_membership_progress(&progress)?;
                        let (candidate_objects, retained) =
                            plan.candidate_cleanup_objects();
                        let cleanup = persistence
                            .db
                            .begin_membership_candidate_nonactivation(
                                persistence.intent_hash,
                                candidate.reference.clone(),
                                candidate_objects,
                                retained,
                                progress_bytes,
                                verified,
                            )
                            .await?;
                        finish_nonactivating_revoke_with_targets(
                            &plan,
                            &persistence,
                            storage,
                            cloud_home,
                            cleanup,
                        )
                        .await?;
                        pending_rotation
                            .remove_candidate(
                                keyring.current_generation(),
                                persistence.intent_hash,
                            )
                            .map_err(InviteError::InvalidDurableMutation)?;
                        return Err(InviteError::InvalidDurableMutation(
                            "membership removal candidate did not activate".to_string(),
                        ));
                    }
                    crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::Nonactivated(
                        reference,
                    ) => {
                        return Err(InviteError::InvalidDurableMutation(format!(
                            "membership removal candidate {} lost without exact evidence",
                            reference.commit_hash
                        )))
                    }
                    crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::Reprepared => {
                        return Err(InviteError::InvalidDurableMutation(
                            "membership removal returned acknowledgement-only reprepare state"
                                .to_string(),
                        ))
                    }
                }
            }
        }
    }
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
async fn finish_nonactivating_resolution(
    plan: &ResolveMutationPlan,
    persistence: &MutationPersistence<'_>,
    storage: &dyn SyncStorage,
) -> Result<(), InviteError> {
    let candidate_objects = vec![
        plan.candidate.reference.object.clone(),
        plan.transition.entry_ref.object.clone(),
        plan.publication.head_ref.object.clone(),
    ];
    let mut retained = vec![plan.reference.object.clone()];
    retained.push(plan.candidate.head_ref().object);
    let cleanup = persistence
        .db
        .membership_candidate_cleanup_targets(
            persistence.intent_hash,
            plan.candidate.reference.clone(),
            candidate_objects.iter().chain(&retained).cloned().collect(),
        )
        .await?;
    for target in cleanup {
        super::store_objects::delete_exact_object(storage, &target.object)
            .await
            .map_err(|error| InviteError::Crypto(error.to_string()))?;
        persistence
            .db
            .mark_candidate_cleanup_absent(target.object)
            .await?;
    }
    persistence
        .db
        .complete_nonactivating_membership_candidate_mutation(
            persistence.intent_hash,
            plan.candidate.reference.clone(),
            candidate_objects,
            retained,
            None,
        )
        .await?;
    Ok(())
}

async fn execute_resolution_mutation(
    storage: &dyn SyncStorage,
    chain: &mut MembershipChain,
    mut plan: ResolveMutationPlan,
    progress: MembershipMutationProgress,
    mut persistence: MutationPersistence<'_>,
) -> Result<super::membership::StoreMembershipConflictResolutionRef, InviteError> {
    plan.validate_closed_shape()?;
    if let MembershipMutationProgress::ResolutionCandidateNonactivating { nonactivation } =
        &progress
    {
        if nonactivation
            .reference()
            .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?
            != plan.candidate.reference
        {
            return Err(InviteError::InvalidDurableMutation(
                "resolution nonactivation names another candidate".to_string(),
            ));
        }
        finish_nonactivating_resolution(&plan, &persistence, storage).await?;
        return Err(InviteError::InvalidDurableMutation(
            "membership resolution candidate did not activate".to_string(),
        ));
    }
    if let MembershipMutationProgress::ResolutionActivated { candidate } = &progress {
        if candidate != &plan.candidate.reference {
            return Err(InviteError::InvalidDurableMutation(
                "resolution activation names another candidate".to_string(),
            ));
        }
        let mut successor = chain.clone();
        successor.apply_resolutions(
            plan.resolution.store_root_hash,
            &[(plan.reference.clone(), plan.resolution.clone())],
        )?;
        successor.add_entry(plan.publication.entry.clone())?;
        successor.activate_head_ref(plan.publication.head_ref.clone())?;
        *chain = successor;
        return Ok(plan.reference);
    }
    if !matches!(progress, MembershipMutationProgress::Pending) {
        return Err(InviteError::InvalidDurableMutation(
            "membership resolution carries another mutation's progress".to_string(),
        ));
    }
    let mut successor = chain.clone();
    successor.apply_resolutions(
        plan.resolution.store_root_hash,
        &[(plan.reference.clone(), plan.resolution.clone())],
    )?;
    successor.add_entry(plan.publication.entry.clone())?;
    let root = persistence
        .db
        .local_store_root_ref()
        .await?
        .ok_or_else(|| InviteError::InvalidDurableMutation("Store root is absent".to_string()))?;
    let author = super::store_objects::load_registration_ref(
        storage,
        &root,
        &plan.publication.head.body.author_registration,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?
    .value;
    let remotes = plan.remote_objects()?;
    super::store_objects::create_exact_object(storage, &plan.resolution_object)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::load_membership_resolution_ref(
        storage,
        root.store_root_hash,
        &plan.reference,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    persistence
        .db
        .mark_remote_object_uploaded(exact_owned_remote(&remotes, &plan.reference.object)?)
        .await?;
    publish_prepared_merge_membership_authority(
        storage,
        root.store_root_hash,
        &plan.transition,
        &[],
    )
    .await?;
    persistence
        .db
        .mark_remote_object_uploaded(exact_owned_remote(
            &remotes,
            &plan.transition.entry_ref.object,
        )?)
        .await?;
    super::store_engine::engine::operations::upload_commit(storage, &plan.candidate)
        .await
        .map_err(|error| InviteError::InvalidDurableMutation(error.to_string()))?;
    persistence
        .db
        .mark_remote_object_uploaded(exact_owned_remote(
            &remotes,
            &plan.candidate.reference.object,
        )?)
        .await?;
    loop {
        let previous = plan.candidate.as_ref().clone();
        let current_remotes = plan.remote_objects()?;
        let outcome = publish_prepared_merge_membership_activation(
            persistence.db,
            storage,
            &root,
            &author,
            &plan.transition,
            &plan.publication,
            plan.candidate.clone(),
            crate::sync::store_engine::engine::operations::StoreMembershipJournalCompletion::Mutation {
                intent_hash: persistence.intent_hash,
                progress_bytes: encode_membership_progress(
                    &MembershipMutationProgress::ResolutionActivated {
                        candidate: plan.candidate.reference.clone(),
                    },
                )?,
                remote_objects: current_remotes.clone(),
            },
        )
        .await?;
        match outcome {
            crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::Activated(reference)
                if reference == plan.candidate.reference =>
            {
                successor.activate_head_ref(plan.publication.head_ref.clone())?;
                *chain = successor;
                return Ok(plan.reference);
            }
            crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::RepreparedCandidate(
                replacement,
            ) if replacement.reference == plan.candidate.reference => {
                let previous_remotes = plan.remote_objects()?;
                let previous_head = previous.head_ref();
                plan.candidate = replacement;
                let replacement_remotes = plan.remote_objects()?;
                let replacement_head = plan.candidate.head_ref();
                let bytes =
                    encode_membership_mutation(&MembershipMutationPlan::Resolve(plan.clone()))?;
                persistence.intent_hash = persistence
                    .db
                    .adopt_merge_membership_candidate_head(
                        persistence.intent_hash,
                        bytes,
                        exact_owned_remote(&previous_remotes, &previous_head.object)?,
                        exact_owned_remote(&replacement_remotes, &replacement_head.object)?,
                        None,
                    )
                    .await?;
            }
            crate::sync::store_engine::engine::operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                candidate,
                nonactivation,
            } if candidate.as_ref() == plan.candidate.as_ref() => {
                let progress = MembershipMutationProgress::ResolutionCandidateNonactivating {
                    nonactivation: nonactivation.clone().into_durable(),
                };
                let cleanup = persistence
                    .db
                    .begin_membership_candidate_nonactivation(
                        persistence.intent_hash,
                        plan.candidate.reference.clone(),
                        vec![
                            plan.candidate.reference.object.clone(),
                            plan.transition.entry_ref.object.clone(),
                            plan.publication.head_ref.object.clone(),
                        ],
                        vec![
                            plan.reference.object.clone(),
                            plan.candidate.head_ref().object,
                        ],
                        encode_membership_progress(&progress)?,
                        *nonactivation,
                    )
                    .await?;
                for target in cleanup {
                    super::store_objects::delete_exact_object(storage, &target.object)
                        .await
                        .map_err(|error| InviteError::Crypto(error.to_string()))?;
                    persistence
                        .db
                        .mark_candidate_cleanup_absent(target.object)
                        .await?;
                }
                finish_nonactivating_resolution(&plan, &persistence, storage).await?;
                return Err(InviteError::InvalidDurableMutation(
                    "membership resolution candidate did not activate".to_string(),
                ));
            }
            _ => {
                return Err(InviteError::InvalidDurableMutation(
                    "membership resolution returned an inapplicable publication outcome"
                        .to_string(),
                ))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_membership_conflict(
    storage: &dyn SyncStorage,
    chain: &mut MembershipChain,
    signer: &UserKeypair,
    device_id: &str,
    conflict_hash: super::store_commit::ObjectHash,
    resolver_branch_heads: Vec<MembershipHeadRef>,
    created_at: &str,
    db: &Database,
) -> Result<super::membership::StoreMembershipConflictResolutionRef, InviteError> {
    let _mutation = db.lock_membership_mutation().await;
    let (plan, progress, intent_hash) = match db.outbound_membership_mutation().await? {
        Some(row) => {
            let intent_hash = row.intent_hash;
            let (pending, progress) = decode_membership_mutation(row)?;
            let MembershipMutationPlan::Resolve(plan) = pending else {
                return Err(InviteError::PendingMutation(
                    "another membership mutation is pending".to_string(),
                ));
            };
            if plan.resolution.conflict_hash != conflict_hash
                || plan.resolution.resolver_pubkey != keys::public_key_hex(signer)
                || plan.resolution.resolver_branch_heads != resolver_branch_heads
            {
                return Err(InviteError::PendingMutation(
                    "the pending resolution has different immutable inputs".to_string(),
                ));
            }
            (plan, progress, intent_hash)
        }
        None => {
            let plan = build_resolution_mutation(
                storage,
                db,
                chain,
                signer,
                device_id,
                conflict_hash,
                resolver_branch_heads,
                created_at,
            )
            .await?;
            let bytes = encode_membership_mutation(&MembershipMutationPlan::Resolve(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let intent_hash = db
                .stage_membership_candidate_mutation(
                    bytes,
                    encode_membership_progress(&progress)?,
                    plan.remote_objects()?,
                    None,
                )
                .await?;
            (plan, progress, intent_hash)
        }
    };
    execute_resolution_mutation(
        storage,
        chain,
        plan,
        progress,
        MutationPersistence { db, intent_hash },
    )
    .await
}

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
    pending_rotation: &super::cloud_storage::PendingRotation,
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
            plan.validate_closed_shape()?;
            let encoded =
                encode_membership_mutation(&MembershipMutationPlan::Revoke(plan.clone()))?;
            let progress = MembershipMutationProgress::Pending;
            let progress_bytes = encode_membership_progress(&progress)?;
            let pending_generation =
                EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
                    .map_err(|error| {
                        InviteError::Crypto(format!("parse rotated keyring: {error}"))
                    })?
                    .current_generation();
            let intent_hash = match plan.candidate_remote_objects()? {
                Some(remotes) => {
                    db.stage_membership_candidate_mutation(
                        encoded,
                        progress_bytes,
                        remotes,
                        Some(pending_generation),
                    )
                    .await?
                }
                None => {
                    db.stage_membership_mutation(encoded, progress_bytes, Some(pending_generation))
                        .await?
                }
            };
            (plan, progress, intent_hash)
        }
    };
    let pending_generation = EncryptionService::from_keyring_payload(plan.keyring_payload.clone())
        .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?
        .current_generation();
    match &progress {
        MembershipMutationProgress::RevokeActivated { .. } => {
            pending_rotation.mark_committed_mutation(pending_generation, intent_hash)
        }
        _ => pending_rotation.mark_candidate(pending_generation, intent_hash),
    }
    .map_err(InviteError::InvalidDurableMutation)?;
    Box::pin(execute_revoke_mutation(
        storage,
        cloud_home,
        store_root_hash,
        chain,
        plan,
        progress,
        MutationPersistence { db, intent_hash },
        pending_rotation,
    ))
    .await
}

pub(crate) async fn complete_revoke_rotation_adoption(
    db: &Database,
    pending_rotation: &super::cloud_storage::PendingRotation,
    adopted_generation: u64,
) -> Result<(), InviteError> {
    let _mutation = db.lock_membership_mutation().await;
    let row = db.outbound_membership_mutation().await?.ok_or_else(|| {
        InviteError::InvalidDurableMutation(
            "activated removal journal is absent during key adoption".to_string(),
        )
    })?;
    let intent_hash = row.intent_hash;
    let (plan, progress) = decode_membership_mutation(row)?;
    let MembershipMutationPlan::Revoke(plan) = plan else {
        return Err(InviteError::InvalidDurableMutation(
            "key adoption found another membership mutation".to_string(),
        ));
    };
    if !matches!(progress, MembershipMutationProgress::RevokeActivated { .. }) {
        return Err(InviteError::InvalidDurableMutation(
            "key adoption found a removal that is not activated".to_string(),
        ));
    }
    let planned_generation = EncryptionService::from_keyring_payload(plan.keyring_payload)
        .map_err(|error| InviteError::Crypto(format!("parse rotated keyring: {error}")))?
        .current_generation();
    if planned_generation != adopted_generation {
        return Err(InviteError::InvalidDurableMutation(format!(
            "adopted key generation {adopted_generation} differs from the activated removal generation {planned_generation}"
        )));
    }
    let gate = db
        .complete_local_rotation_adoption(intent_hash, adopted_generation)
        .await?;
    pending_rotation
        .install_durable_gate(gate)
        .map_err(InviteError::InvalidDurableMutation)?;
    Ok(())
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

    #[tokio::test]
    async fn prepared_membership_transition_rejects_substituted_slots_and_bytes() {
        let db = crate::sync::test_helpers::open_test_db();
        let owner = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "prepared-membership-binding",
            owner.clone(),
        )
        .await
        .expect("create Merge Store");
        let chain = super::super::membership_ops::load_current_exact_chain(
            &store.storage,
            &store.root,
            Some(&keys::public_key_hex(&owner)),
            Some(&db),
        )
        .await
        .expect("load exact membership chain");
        let stream_id = select_mutation_author_stream(&db, &chain, &owner)
            .await
            .expect("select membership stream");
        let entry = chain
            .signed_set_member_in_stream(
                &owner,
                stream_id,
                keys::public_key_hex(&UserKeypair::generate()),
                None,
                MemberRole::Member,
                "2026-07-21T00:00:00Z".to_string(),
            )
            .expect("sign membership entry");
        let prepared = prepare_membership_transition(
            &store.storage,
            &db,
            store.root.store_root_hash,
            &chain,
            entry,
            &owner,
        )
        .await
        .expect("prepare membership transition");
        validate_prepared_transition(&prepared).expect("validate prepared transition");

        let mut redirected_head = prepared.clone();
        redirected_head.transition.head_slot = crate::storage::cloud::ObjectSlot::logical(
            "store-v1/tests/redirected-membership-head.json".to_string(),
        )
        .expect("valid redirected head slot");
        assert!(validate_prepared_transition(&redirected_head).is_err());

        let mut redirected_successor = prepared.clone();
        redirected_successor.transition.body.successor.next_slot =
            crate::storage::cloud::ObjectSlot::logical(
                "store-v1/tests/redirected-membership-successor.json".to_string(),
            )
            .expect("valid redirected successor slot");
        assert!(validate_prepared_transition(&redirected_successor).is_err());

        let mut substituted_entry = prepared.clone();
        let substituted_bytes = b"substituted exact membership entry".to_vec();
        let substituted_ref = super::super::storage::ExactObjectRef::new(
            substituted_entry.entry_object.reference().slot().clone(),
            substituted_bytes.len() as u64,
            ObjectHash::digest(&substituted_bytes),
        );
        substituted_entry.entry_object = super::super::storage::PreparedExactObject::new(
            substituted_ref.clone(),
            substituted_bytes,
        )
        .expect("prepare substituted membership entry object");
        substituted_entry.entry_ref.object = substituted_ref.clone();
        substituted_entry.transition.body.entry.object = substituted_ref;
        assert!(validate_prepared_transition(&substituted_entry).is_err());

        let publication = finish_membership_transition(
            &store.storage,
            &db,
            store.root.store_root_hash,
            prepared,
            super::super::membership::MembershipHeadActivation::Direct,
            &owner,
        )
        .await
        .expect("finish membership transition");
        let mut substituted_head = publication;
        let substituted_bytes = b"substituted exact membership head".to_vec();
        let substituted_ref = super::super::storage::ExactObjectRef::new(
            substituted_head.head_object.reference().slot().clone(),
            substituted_bytes.len() as u64,
            ObjectHash::digest(&substituted_bytes),
        );
        substituted_head.head_object = super::super::storage::PreparedExactObject::new(
            substituted_ref.clone(),
            substituted_bytes,
        )
        .expect("prepare substituted membership head object");
        substituted_head.head_ref.object = substituted_ref;
        assert!(validate_prepared_publication(&substituted_head).is_err());
    }
}
