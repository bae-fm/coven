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
    MembershipEntry, MembershipError, MembershipGrantId,
};
use super::membership_ops::{list_membership_entries, publish_membership_stream_head};
use super::storage::{StorageError, SyncStorage};
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
        activation: WrappedKeyActivation,
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
    Ok(db
        .select_membership_author_stream(
            &author,
            &grant,
            chain.reusable_author_streams(&author, &grant),
        )
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
    entry: MembershipEntry,
    head: AuthorHead,
    invitee_pubkey: String,
    invitee_email: Option<String>,
    role: MemberRole,
    desired_access: CloudAccessState,
    wrapped_key: Vec<u8>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeMutationPlan {
    entry: MembershipEntry,
    head: AuthorHead,
    revokee_pubkey: String,
    desired_access: CloudAccessState,
    wraps: Vec<ReplacementWrappedKey>,
    keyring_payload: Vec<u8>,
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
async fn guard_extends_committed_head(
    storage: &dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    author: &str,
    grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
) -> Result<(), InviteError> {
    if let Some(committed) = super::membership_ops::committed_head_seq(
        storage,
        store_root_hash,
        author,
        grant,
        stream_id,
    )
    .await
    .map_err(InviteError::Crypto)?
    {
        if committed >= seq {
            return Err(InviteError::StaleMembershipHead {
                author: author.to_string(),
                committed,
                attempted: seq,
            });
        }
    }
    Ok(())
}

/// Decode and convert an Ed25519 hex pubkey to X25519 for sealed box encryption.
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
) -> Result<Vec<u8>, InviteError> {
    signed_wrapped_key_with_activation(
        store_id,
        recipient_ed25519_pubkey,
        recipient_x25519_pk,
        encryption,
        owner_keypair,
        None,
    )
}

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

/// Upload a signed membership entry to the storage.
async fn upload_membership_entry(
    storage: &dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    coord: &MembershipCoord,
    entry: &MembershipEntry,
) -> Result<(), InviteError> {
    super::store_objects::append_membership_entry_object(storage, store_root_hash, coord, entry)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;

    Ok(())
}

/// Undo the durable writes a pre-commit invite failure leaves once the
/// wrapped-key slot has been written but the Add entry has not committed: put the
/// slot back — the prior object restored for a current member, the slot deleted
/// otherwise — and, for an invitee this invitation first granted access to,
/// revoke that access. Dispatched on committed chain membership, not slot
/// presence: a current member keeps the access they held before this invite and
/// gets their prior slot back; a non-member has both this invite's grant and slot
/// undone. Returns the rollback failures so the caller can surface them alongside
/// the original error.
///
/// Every failure between the slot write and the commit point (the head publish)
/// exits through here, so they all land on the same clean state a retry rebuilds
/// from: the guard that refuses a stale head, and the entry upload itself.
async fn rollback_written_invite(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    author_pubkey_hex: &str,
    invitee_ed25519_pubkey: &str,
    invitee_is_current_member: bool,
    prior_wrapped_key: Option<Vec<u8>>,
    absent_access: CloudAccessState,
) -> Vec<String> {
    let mut rollback_errors = Vec::new();
    if invitee_is_current_member {
        // Keep the member's cloud access untouched — they held it before this
        // invitation — and put their wrapped-key slot back the way it stood.
        match prior_wrapped_key {
            // Restore the exact prior signed object. If the restore write itself
            // fails the slot is left holding this invitation's key — surface it
            // loud, naming the slot; retrying the whole invitation overwrites the
            // slot again and is idempotent.
            Some(prior) => {
                if let Err(restore) = storage
                    .put_wrapped_key(author_pubkey_hex, invitee_ed25519_pubkey, prior)
                    .await
                {
                    rollback_errors.push(format!(
                        "restore wrapped key for {invitee_ed25519_pubkey}: {restore}"
                    ));
                }
            }
            // The member had no slot before (an interrupted earlier invite could
            // leave it absent); delete the wrap just written so the slot returns to
            // absent, which the member's next refresh re-wraps.
            None => {
                if let Err(rollback) = storage
                    .delete_wrapped_key(author_pubkey_hex, invitee_ed25519_pubkey)
                    .await
                {
                    rollback_errors.push(rollback.to_string());
                }
            }
        }
    } else {
        // The invitee is not a member: this invitation both granted access and
        // wrote the slot, so undo both. A slot already present belongs to no
        // authorized member, so delete rather than restore it — an unauthorized
        // slot must not be rewritten; note the anomaly.
        if prior_wrapped_key.is_some() {
            tracing::warn!(
                slot = invitee_ed25519_pubkey,
                "invite rollback found a wrapped-key slot for a non-member; \
                 deleting it rather than restoring an unauthorized slot"
            );
        }
        if let Err(rollback) = storage
            .delete_wrapped_key(author_pubkey_hex, invitee_ed25519_pubkey)
            .await
        {
            rollback_errors.push(rollback.to_string());
        }
        match cloud_home.set_access(absent_access).await {
            Ok(CloudAccessOutcome::Absent(_)) => {}
            Ok(CloudAccessOutcome::Present(_)) => rollback_errors
                .push("provider returned present outcome for absent access request".to_string()),
            Err(rollback) => rollback_errors.push(rollback.to_string()),
        }
    }
    rollback_errors
}

/// Create an invitation for a new member.
///
/// This grants access on the cloud home, wraps the store encryption key
/// to the invitee's X25519 public key, creates and signs a membership entry
/// (Add), validates it against the local chain, and uploads both to the storage.
/// Returns the JoinInfo so the caller can share connection details with the invitee.
pub async fn create_invitation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption_key: &[u8; 32],
    store_id: &str,
    timestamp: &str,
) -> Result<CloudHomeJoinInfo, InviteError> {
    let encryption = EncryptionService::from_key(*encryption_key);
    create_invitation_with_encryption(
        storage,
        cloud_home,
        store_root_hash,
        chain,
        owner_keypair,
        invitee_ed25519_pubkey,
        invitee_email,
        role,
        &encryption,
        store_id,
        timestamp,
    )
    .await
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

fn validate_planned_head(entry: &MembershipEntry, head: &AuthorHead) -> Result<(), InviteError> {
    let coord = entry.coord();
    if !head.verify()
        || head.store_id != entry.store_id
        || head.author_pubkey != coord.author_pubkey
        || head.author_owner_grant != coord.author_owner_grant
        || head.stream_id != coord.stream_id
        || head.seq != coord.seq
        || head.tip_hash != coord.entry_hash
    {
        return Err(InviteError::InvalidDurableMutation(
            "signed head does not commit the exact planned entry".to_string(),
        ));
    }
    Ok(())
}

fn build_invite_mutation(
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
    let entry = chain.signed_set_member_in_stream(
        owner_keypair,
        stream_id,
        invitee_ed25519_pubkey.to_string(),
        invitee_email.map(str::to_string),
        role.clone(),
        timestamp.to_string(),
    )?;
    let entry_coord = entry.coord();
    let mut validated = chain.clone();
    validated.add_entry_at(entry_coord.clone(), entry.clone())?;
    let head = validated
        .signed_head_for_stream(owner_keypair, stream_id)
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(
                "invitation leaves its author without a membership head".to_string(),
            )
        })?;
    validate_planned_head(&entry, &head)?;
    let wrapped_key = signed_wrapped_key_with_activation(
        store_id,
        invitee_ed25519_pubkey,
        &invitee_x25519_pk,
        encryption,
        owner_keypair,
        Some(entry_coord),
    )?;
    Ok(InviteMutationPlan {
        entry,
        head,
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
    plan.entry.author_pubkey == hex::encode(owner_keypair.public_key())
        && plan.entry.store_id == store_id
        && plan.invitee_pubkey == invitee_pubkey
        && plan.invitee_email.as_deref() == invitee_email
        && &plan.role == role
        && plan.desired_access
            == (CloudAccessState::Present {
                member_pubkey: invitee_pubkey.to_string(),
                provider_account_email: invitee_email.map(str::to_string),
            })
        && matches!(
            &plan.entry.change,
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
    validate_planned_head(&plan.entry, &plan.head)?;
    let validated_chain = chain_with_exact_entry(chain, &plan.entry)?;
    let wrapped: WrappedStoreKey = serde_json::from_slice(&plan.wrapped_key).map_err(|error| {
        InviteError::InvalidDurableMutation(format!("parse planned invitation wrap: {error}"))
    })?;
    if wrapped.activation != Some(WrappedKeyActivation::MergeConcurrent(plan.entry.coord()))
        || wrapped.author_pubkey != plan.entry.author_pubkey
        || wrapped
            .verify_and_unwrap(
                &plan.entry.store_id,
                &plan.invitee_pubkey,
                std::iter::once(plan.entry.author_pubkey.as_str()),
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
            &plan.entry.author_pubkey,
            &plan.invitee_pubkey,
            plan.wrapped_key.clone(),
        )
        .await?;
    super::store_objects::append_membership_entry_object(
        storage,
        store_root_hash,
        &plan.entry.coord(),
        &plan.entry,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    super::store_objects::append_membership_head_object(storage, store_root_hash, &plan.head)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
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
                chain,
                owner_keypair,
                stream_id,
                invitee_ed25519_pubkey,
                invitee_email,
                role,
                encryption,
                store_id,
                timestamp,
            )?;
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

pub async fn create_invitation_with_encryption(
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
) -> Result<CloudHomeJoinInfo, InviteError> {
    // Convert Ed25519 -> X25519 for sealed box encryption.
    let invitee_x25519_pk = ed25519_hex_to_x25519(invitee_ed25519_pubkey)?;

    // Seal the store key to the invitee and sign the binding so the joiner can
    // authenticate it on adoption. The joiner verifies this signature against the
    // founder the invite pins, so for the invitee to actually adopt the key
    // `owner_keypair` must be the founder's for a fresh joiner; existing members
    // authorize rotated keys against the current Owner set during refresh.
    let wrapped_key = signed_wrapped_key(
        store_id,
        invitee_ed25519_pubkey,
        &invitee_x25519_pk,
        encryption,
        owner_keypair,
    )?;

    // Create and sign a membership entry.
    let author_pubkey_hex = hex::encode(owner_keypair.public_key());
    let author_grant = chain
        .active_owner_grant(&author_pubkey_hex)
        .ok_or_else(|| MembershipError::SignerIsNotOwner(author_pubkey_hex.clone()))?;
    let stream_id = chain
        .preferred_author_stream(&author_pubkey_hex, &author_grant)
        .ok_or(MembershipError::PrunedAuthorStream)?;
    let entry = chain.signed_set_member_in_stream(
        owner_keypair,
        stream_id,
        invitee_ed25519_pubkey.to_string(),
        invitee_email.map(str::to_string),
        role,
        timestamp.to_string(),
    )?;
    let entry_coord = entry.coord();

    // Validate against the local chain before any provider or storage mutation.
    let mut validated_chain = chain.clone();
    validated_chain.add_entry_at(entry_coord.clone(), entry.clone())?;

    let present_access = CloudAccessState::Present {
        member_pubkey: invitee_ed25519_pubkey.to_string(),
        provider_account_email: invitee_email.map(str::to_string),
    };
    let absent_access = CloudAccessState::Absent {
        member_pubkey: invitee_ed25519_pubkey.to_string(),
        provider_account_email: invitee_email.map(str::to_string),
    };

    // Whether the invitee is already a current member, judged against the chain as
    // it stands before this invitation's entry is added. This — the authoritative
    // committed state, not whether a `keys/{owner}/{pk}` object happens to be present — is
    // what a rollback dispatches on: a current member keeps the access and the
    // wrapped-key slot they held before this invite, while an invitee who is not a
    // member has this invite's grant and slot both undone. A current member can
    // have an absent slot (an interrupted earlier invite could leave it so); their
    // access must still be preserved.
    let invitee_is_current_member = chain
        .current_members()
        .iter()
        .any(|(pubkey, _)| pubkey == invitee_ed25519_pubkey);

    // Retain the invitee's existing wrapped-key object, if any, before overwriting
    // it. These bytes are only the payload for a restore: if the entry upload below
    // fails while re-inviting a current member, the rollback writes this exact prior
    // object back so the member never loses their wrapped key. Read before any
    // mutation so a read failure aborts before granting access or writing the key.
    let prior_wrapped_key = match storage
        .get_wrapped_key(&author_pubkey_hex, invitee_ed25519_pubkey)
        .await
    {
        Ok(bytes) => Some(bytes),
        Err(StorageError::NotFound(_)) => None,
        Err(e) => return Err(e.into()),
    };

    let join_info = match cloud_home.set_access(present_access).await? {
        CloudAccessOutcome::Present(join_info) => join_info,
        CloudAccessOutcome::Absent(_) => {
            return Err(InviteError::Crypto(
                "provider returned absent outcome for present access request".to_string(),
            ))
        }
    };

    // Upload wrapped key and membership entry.
    if let Err(original) = storage
        .put_wrapped_key(&author_pubkey_hex, invitee_ed25519_pubkey, wrapped_key)
        .await
    {
        // The write failed, so the slot is unchanged and a current member keeps
        // their existing key. Revoke the grant only for an invitee who is not a
        // current member, whose access this invitation created; a current member
        // held their access before this invite, so revoking it would strip access
        // this invite never granted.
        if !invitee_is_current_member {
            match cloud_home.set_access(absent_access.clone()).await {
                Ok(CloudAccessOutcome::Absent(_)) => {}
                Ok(CloudAccessOutcome::Present(_)) => {
                    return Err(InviteError::Rollback {
                        operation: "upload wrapped key",
                        original: original.to_string(),
                        rollback: "provider returned present outcome for absent access request"
                            .to_string(),
                    });
                }
                Err(rollback) => {
                    return Err(InviteError::Rollback {
                        operation: "upload wrapped key",
                        original: original.to_string(),
                        rollback: rollback.to_string(),
                    });
                }
            }
        }
        return Err(original.into());
    }

    // The head governs commitment, so re-check right before writing the entry: if
    // another owner device advanced this author's head to our seq while we were
    // preparing, its committed entry must not be overwritten. Fail loud — and roll
    // back the same way the entry-upload failure below does, so a stale-head loss
    // never leaves a durable cloud grant plus an owner-signed wrapped key behind
    // for an Add that never committed (the wrap authenticates against the current
    // Owner set, so the invitee could otherwise read the store while peers reject
    // their writes). The retry recomputes the seq from the now-newer head.
    if let Err(original) = guard_extends_committed_head(
        storage,
        store_root_hash,
        &author_pubkey_hex,
        &entry_coord.author_owner_grant,
        entry_coord.stream_id,
        entry_coord.seq,
    )
    .await
    {
        let rollback_errors = rollback_written_invite(
            storage,
            cloud_home,
            &author_pubkey_hex,
            invitee_ed25519_pubkey,
            invitee_is_current_member,
            prior_wrapped_key,
            absent_access,
        )
        .await;
        if !rollback_errors.is_empty() {
            return Err(InviteError::Rollback {
                operation: "stale membership head guard",
                original: original.to_string(),
                rollback: rollback_errors.join("; "),
            });
        }
        return Err(original);
    }

    if let Err(original) =
        upload_membership_entry(storage, store_root_hash, &entry_coord, &entry).await
    {
        let rollback_errors = rollback_written_invite(
            storage,
            cloud_home,
            &author_pubkey_hex,
            invitee_ed25519_pubkey,
            invitee_is_current_member,
            prior_wrapped_key,
            absent_access,
        )
        .await;
        if !rollback_errors.is_empty() {
            return Err(InviteError::Rollback {
                operation: "upload membership entry",
                original: original.to_string(),
                rollback: rollback_errors.join("; "),
            });
        }
        return Err(original);
    }
    publish_membership_stream_head(
        storage,
        store_root_hash,
        &validated_chain,
        owner_keypair,
        stream_id,
    )
    .await
    .map_err(|e| InviteError::Crypto(format!("publish membership head: {e}")))?;

    *chain = validated_chain;

    Ok(join_info)
}

/// Join a shared store: authenticate and unwrap its encryption key from the
/// invitee's own wrapped-key slot.
///
/// `founder` is the owner the invite pins (the chain founder). The joiner holds
/// no membership chain yet — and every chain entry is sealed under the store
/// key — so this opens the sealed box to a *candidate* keyring first (a sealed
/// box authenticates only its recipient, so opening it grants no trust), reads
/// and anchors the membership chain to `founder` with that candidate, and only
/// then adopts the key if its signer is a *current* Owner in that anchored chain.
///
/// The founder pin is the trust root; the current Owner set is derived from it,
/// so authority is never delegated to unanchored state. The rule is non-temporal,
/// like every other authorization path (see `can_write_now`): a wrapped key is
/// judged against who is an Owner now, never against the chain position of the
/// joiner's Add — a position an author picks via its own timestamp, which a
/// removed Owner with residual bucket write could back-date to resurrect itself
/// as a valid signer. So a key signed by a *current* non-founder Owner is adopted,
/// while one signed by a non-Owner member, by an "Owner" whose add is not
/// committed in the anchored chain, or by an Owner since removed or demoted, is
/// refused. The bucket is writable by every member and anyone holding the
/// credential, and a sealed box authenticates only its recipient, so without this
/// check a bucket writer could substitute a key of its choosing.
pub async fn unwrap_store_keyring(
    cloud_home: Arc<dyn CloudHome>,
    keypair: &UserKeypair,
    store_root_hash: super::store_commit::ObjectHash,
    store_id: &str,
    founder: &str,
    membership_floor: &[MembershipCoord],
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
    )
    .with_copy_ids(Arc::new(crate::storage::cloud::RandomCopyIdGenerator));
    let entry_keys = list_membership_entries(&storage, store_root_hash)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    let chain = super::membership_ops::load_anchored_chain_at_floor(
        &storage,
        store_root_hash,
        &entry_keys,
        founder,
        membership_floor,
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
            activation,
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
    let head = validated
        .signed_head_for_stream(owner_keypair, stream_id)
        .ok_or_else(|| {
            InviteError::InvalidDurableMutation(
                "removal leaves its author without a membership head".to_string(),
            )
        })?;
    validate_planned_head(&entry, &head)?;
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
        entry,
        head,
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
    plan.entry.author_pubkey == hex::encode(owner_keypair.public_key())
        && plan.entry.store_id == store_id
        && plan.revokee_pubkey == revokee_pubkey
        && matches!(
            &plan.entry.change,
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
    validate_planned_head(&plan.entry, &plan.head)?;
    let validated_chain = chain_with_exact_entry(chain, &plan.entry)?;
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
        if envelope.activation != Some(WrappedKeyActivation::MergeConcurrent(plan.entry.coord()))
            || envelope.generation != keyring.current_generation()
            || envelope.author_pubkey != plan.entry.author_pubkey
            || envelope
                .verify_and_unwrap(
                    &plan.entry.store_id,
                    &wrapped.recipient,
                    std::iter::once(plan.entry.author_pubkey.as_str()),
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
                &plan.entry.author_pubkey,
                &wrapped.recipient,
                wrapped.bytes.clone(),
            )
            .await?;
    }
    super::store_objects::append_membership_entry_object(
        storage,
        store_root_hash,
        &plan.entry.coord(),
        &plan.entry,
    )
    .await
    .map_err(|error| InviteError::Crypto(error.to_string()))?;
    storage
        .delete_wrapped_key(&plan.entry.author_pubkey, &plan.revokee_pubkey)
        .await?;
    match cloud_home.set_access(plan.desired_access.clone()).await? {
        CloudAccessOutcome::Absent(_) => {}
        CloudAccessOutcome::Present(_) => {
            return Err(InviteError::InvalidDurableMutation(
                "provider returned present outcome for absent access request".to_string(),
            ))
        }
    }
    super::store_objects::append_membership_head_object(storage, store_root_hash, &plan.head)
        .await
        .map_err(|error| InviteError::Crypto(error.to_string()))?;
    *chain = validated_chain;
    persistence.complete().await?;
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
                return revoke_member(
                    storage,
                    cloud_home,
                    store_root_hash,
                    chain,
                    owner_keypair,
                    revokee_pubkey,
                    store_id,
                    timestamp,
                    current_encryption,
                )
                .await;
            }
            let stream_id = select_mutation_author_stream(db, chain, owner_keypair).await?;
            let plan = build_revoke_mutation(
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

pub async fn revoke_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    store_root_hash: super::store_commit::ObjectHash,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    store_id: &str,
    timestamp: &str,
    current_encryption: &EncryptionService,
) -> Result<EncryptionService, InviteError> {
    let members = chain.current_members();
    let revokee_is_current = members.iter().any(|(pk, _)| pk == revokee_pubkey);
    let revokee_was_removed = chain.entries().iter().any(|entry| {
        matches!(
            &entry.change,
            MembershipChange::RemoveMember { user_pubkey, .. } if user_pubkey == revokee_pubkey
        )
    });
    if !revokee_is_current && !revokee_was_removed {
        return Err(InviteError::NotAMember(revokee_pubkey.to_string()));
    }

    // Ensure at least one owner would remain after the removal.
    let current_owners = members
        .iter()
        .filter(|(pk, role)| pk != revokee_pubkey && *role == MemberRole::Owner)
        .map(|(pk, _)| pk.clone())
        .collect::<Vec<_>>();
    if current_owners.is_empty() {
        return Err(InviteError::LastOwner);
    }

    let provider_account_email = chain
        .current_member_provider_email(revokee_pubkey)
        .map(str::to_string);
    let absent_access = CloudAccessState::Absent {
        member_pubkey: revokee_pubkey.to_string(),
        provider_account_email: provider_account_email.clone(),
    };
    let present_access = CloudAccessState::Present {
        member_pubkey: revokee_pubkey.to_string(),
        provider_account_email,
    };

    // This owner deletes only its own wrap for the revokee, from its own prefix.
    // Any wrap another owner sealed for the revokee is a pre-rotation generation —
    // it wraps a key the revokee already held — so leaving it is harmless; that
    // owner reclaims it when it next rotates.
    let author_pubkey_hex = hex::encode(owner_keypair.public_key());

    if !revokee_is_current {
        let visible_coords = visible_membership_activations(chain, None);
        let keyring = unwrap_store_keyring_for_owners_with_activation(
            cloud_home,
            owner_keypair,
            store_id,
            current_owners.iter().map(String::as_str),
            Some(&visible_coords),
        )
        .await?;
        revoke_provider_access(cloud_home, absent_access).await?;
        storage
            .delete_wrapped_key(&author_pubkey_hex, revokee_pubkey)
            .await?;
        return Ok(keyring);
    }

    let author_grant = chain
        .active_owner_grant(&author_pubkey_hex)
        .ok_or_else(|| MembershipError::SignerIsNotOwner(author_pubkey_hex.clone()))?;
    let stream_id = chain
        .preferred_author_stream(&author_pubkey_hex, &author_grant)
        .ok_or(MembershipError::PrunedAuthorStream)?;
    let entry = chain.signed_remove_member_in_stream(
        owner_keypair,
        stream_id,
        revokee_pubkey.to_string(),
        timestamp.to_string(),
    )?;
    let remove_coord = entry.coord();

    // Validate against a clone before any storage writes. The caller's chain
    // advances only after the Remove entry is uploaded as the commit point.
    let mut validated_chain = chain.clone();
    validated_chain.add_entry_at(remove_coord.clone(), entry.clone())?;

    // A prior attempt of this same removal may have already minted a rotation and
    // durably wrapped it to every remaining member — including this owner's own
    // slot at keys/{self}/{self} — before crashing short of publishing the head
    // (which is why the revokee still reads as current here). Re-adopt that prior
    // key rather than minting a second one at the same generation: a member whose
    // cycle already ran adopted the prior key, so minting fresh would fork the
    // fleet across two keys for one generation. Read this owner's own slot back and
    // authenticate it against this owner; reuse it only if it carries a generation
    // above the one this device still holds (a genuine prior attempt, not just its
    // own pre-rotation wrap). Anything else — no prior wrap, an activation not yet
    // visible — falls through to a fresh mint.
    let visible_coords = visible_membership_activations(chain, Some(remove_coord.clone()));
    let prior_attempt = match unwrap_store_keyring_for_owners_with_activation(
        cloud_home,
        owner_keypair,
        store_id,
        std::iter::once(author_pubkey_hex.as_str()),
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
        Err(e) => return Err(e),
    };

    let new_keyring = match prior_attempt {
        Some(prior) => current_encryption.merged_with(&prior),
        None => {
            let new_key = encryption::generate_random_key();
            let new_generation = current_encryption.current_generation() + 1;
            current_encryption
                .with_appended_generation(new_generation, new_key)
                .map_err(|e| InviteError::Crypto(format!("append key generation: {e}")))?
        }
    };

    // Re-wrap the new key to all remaining members, each signed so a joiner that
    // later adopts it can authenticate the signer against the current Owner set.
    let remaining_members = validated_chain.current_members();
    let remaining_member_keys = remaining_members
        .iter()
        .map(|(member_pubkey, _)| {
            ed25519_hex_to_x25519(member_pubkey).map(|x25519_pk| (member_pubkey, x25519_pk))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut rewraps = Vec::with_capacity(remaining_member_keys.len());
    for (member_pubkey, x25519_pk) in remaining_member_keys {
        let previous = match storage
            .get_wrapped_key(&author_pubkey_hex, member_pubkey)
            .await
        {
            Ok(bytes) => Some(bytes),
            Err(StorageError::NotFound(_)) => None,
            Err(error) => return Err(InviteError::Bucket(error)),
        };
        let wrapped = signed_wrapped_key_with_activation(
            store_id,
            member_pubkey,
            &x25519_pk,
            &new_keyring,
            owner_keypair,
            Some(remove_coord.clone()),
        )?;
        rewraps.push(PlannedWrappedKey {
            recipient: member_pubkey.clone(),
            previous,
            replacement: wrapped,
        });
    }
    let mut touched = Vec::with_capacity(rewraps.len() + 1);
    for rewrap in rewraps {
        touched.push(PriorWrappedKey {
            recipient: rewrap.recipient.clone(),
            bytes: rewrap.previous,
        });
        if let Err(error) = storage
            .put_wrapped_key(&author_pubkey_hex, &rewrap.recipient, rewrap.replacement)
            .await
        {
            let original = InviteError::Bucket(error);
            rollback_revocation(
                storage,
                cloud_home,
                &author_pubkey_hex,
                &touched,
                None,
                "write remaining member wraps",
                original.to_string(),
            )
            .await?;
            return Err(original);
        }
    }

    // Don't overwrite a committed Remove another owner device already published at
    // this seq; fail loud and let the retry rebuild on top of the observed head.
    if let Err(original) = guard_extends_committed_head(
        storage,
        store_root_hash,
        &author_pubkey_hex,
        &remove_coord.author_owner_grant,
        remove_coord.stream_id,
        remove_coord.seq,
    )
    .await
    {
        rollback_revocation(
            storage,
            cloud_home,
            &author_pubkey_hex,
            &touched,
            None,
            "verify membership head predecessor",
            original.to_string(),
        )
        .await?;
        return Err(original);
    }
    if let Err(original) =
        upload_membership_entry(storage, store_root_hash, &remove_coord, &entry).await
    {
        rollback_revocation(
            storage,
            cloud_home,
            &author_pubkey_hex,
            &touched,
            None,
            "write membership removal entry",
            original.to_string(),
        )
        .await?;
        return Err(original);
    }
    let prior_wrap = match storage
        .get_wrapped_key(&author_pubkey_hex, revokee_pubkey)
        .await
    {
        Ok(bytes) => Some(bytes),
        Err(StorageError::NotFound(_)) => None,
        Err(error) => {
            let original = InviteError::Bucket(error);
            rollback_revocation(
                storage,
                cloud_home,
                &author_pubkey_hex,
                &touched,
                None,
                "read prior revokee wrap",
                original.to_string(),
            )
            .await?;
            return Err(original);
        }
    };
    touched.push(PriorWrappedKey {
        recipient: revokee_pubkey.to_string(),
        bytes: prior_wrap,
    });
    if let Err(error) = storage
        .delete_wrapped_key(&author_pubkey_hex, revokee_pubkey)
        .await
    {
        let original = InviteError::Bucket(error);
        rollback_revocation(
            storage,
            cloud_home,
            &author_pubkey_hex,
            &touched,
            None,
            "delete revokee wrap",
            original.to_string(),
        )
        .await?;
        return Err(original);
    }
    let revoked = match revoke_provider_access(cloud_home, absent_access).await {
        Ok(revoked) => revoked,
        Err(original) => {
            rollback_revocation(
                storage,
                cloud_home,
                &author_pubkey_hex,
                &touched,
                Some(present_access.clone()),
                "revoke provider access",
                original.to_string(),
            )
            .await?;
            return Err(original);
        }
    };
    if let Err(error) = publish_membership_stream_head(
        storage,
        store_root_hash,
        &validated_chain,
        owner_keypair,
        stream_id,
    )
    .await
    {
        let original = format!("publish membership head: {error}");
        rollback_revocation(
            storage,
            cloud_home,
            &author_pubkey_hex,
            &touched,
            revoked.then_some(present_access),
            "publish membership head",
            original.clone(),
        )
        .await?;
        return Err(InviteError::Crypto(original));
    }
    *chain = validated_chain;

    Ok(new_keyring)
}

struct PlannedWrappedKey {
    recipient: String,
    previous: Option<Vec<u8>>,
    replacement: Vec<u8>,
}

struct PriorWrappedKey {
    recipient: String,
    bytes: Option<Vec<u8>>,
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

#[allow(clippy::too_many_arguments)]
async fn rollback_revocation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    author: &str,
    wrapped_keys: &[PriorWrappedKey],
    regrant: Option<CloudAccessState>,
    operation: &'static str,
    original: String,
) -> Result<(), InviteError> {
    let mut failures = Vec::new();
    for wrapped_key in wrapped_keys.iter().rev() {
        let result = match wrapped_key.bytes.as_ref() {
            Some(bytes) => {
                storage
                    .put_wrapped_key(author, &wrapped_key.recipient, bytes.clone())
                    .await
            }
            None => {
                storage
                    .delete_wrapped_key(author, &wrapped_key.recipient)
                    .await
            }
        };
        if let Err(error) = result {
            failures.push(format!(
                "restore wrapped key for {}: {error}",
                wrapped_key.recipient
            ));
        }
    }
    if let Some(grant) = regrant {
        match cloud_home.set_access(grant).await {
            Ok(CloudAccessOutcome::Present(_)) => {}
            Ok(CloudAccessOutcome::Absent(_)) => failures
                .push("restore provider access: provider returned absent outcome".to_string()),
            Err(error) => failures.push(format!("restore provider access: {error}")),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(InviteError::Rollback {
            operation,
            original,
            rollback: failures.join("; "),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge_activations(entries: &[MembershipCoord]) -> Vec<WrappedKeyActivation> {
        entries
            .iter()
            .cloned()
            .map(WrappedKeyActivation::MergeConcurrent)
            .collect()
    }
    use crate::config::HomeStorage;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::MemberRole;
    use crate::sync::membership_ops::download_entries;
    use crate::sync::test_helpers::{
        bootstrap_chain, pubkey_hex, publish_test_protocol_roots, test_migrations,
        test_synced_tables, MockSyncStorage,
    };
    use async_trait::async_trait;
    use std::sync::Arc;

    #[allow(clippy::too_many_arguments)]
    async fn create_invitation(
        storage: &MockSyncStorage,
        cloud_home: &dyn CloudHome,
        chain: &mut MembershipChain,
        owner_keypair: &UserKeypair,
        invitee_ed25519_pubkey: &str,
        invitee_email: Option<&str>,
        role: MemberRole,
        encryption_key: &[u8; 32],
        store_id: &str,
        timestamp: &str,
    ) -> Result<CloudHomeJoinInfo, InviteError> {
        super::create_invitation(
            storage,
            cloud_home,
            storage.store_root_hash(),
            chain,
            owner_keypair,
            invitee_ed25519_pubkey,
            invitee_email,
            role,
            encryption_key,
            store_id,
            timestamp,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_invitation_with_encryption(
        storage: &MockSyncStorage,
        cloud_home: &dyn CloudHome,
        chain: &mut MembershipChain,
        owner_keypair: &UserKeypair,
        invitee_ed25519_pubkey: &str,
        invitee_email: Option<&str>,
        role: MemberRole,
        encryption: &EncryptionService,
        store_id: &str,
        timestamp: &str,
    ) -> Result<CloudHomeJoinInfo, InviteError> {
        super::create_invitation_with_encryption(
            storage,
            cloud_home,
            storage.store_root_hash(),
            chain,
            owner_keypair,
            invitee_ed25519_pubkey,
            invitee_email,
            role,
            encryption,
            store_id,
            timestamp,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_invitation_with_encryption_durable(
        storage: &MockSyncStorage,
        cloud_home: &dyn CloudHome,
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
        super::create_invitation_with_encryption_durable(
            storage,
            cloud_home,
            storage.store_root_hash(),
            chain,
            owner_keypair,
            invitee_ed25519_pubkey,
            invitee_email,
            role,
            encryption,
            store_id,
            timestamp,
            db,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn revoke_member(
        storage: &MockSyncStorage,
        cloud_home: &dyn CloudHome,
        chain: &mut MembershipChain,
        owner_keypair: &UserKeypair,
        revokee_pubkey: &str,
        store_id: &str,
        timestamp: &str,
        current_encryption: &EncryptionService,
    ) -> Result<EncryptionService, InviteError> {
        super::revoke_member(
            storage,
            cloud_home,
            storage.store_root_hash(),
            chain,
            owner_keypair,
            revokee_pubkey,
            store_id,
            timestamp,
            current_encryption,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn revoke_member_durable(
        storage: &MockSyncStorage,
        cloud_home: &dyn CloudHome,
        chain: &mut MembershipChain,
        owner_keypair: &UserKeypair,
        revokee_pubkey: &str,
        store_id: &str,
        timestamp: &str,
        current_encryption: &EncryptionService,
        db: &Database,
    ) -> Result<EncryptionService, InviteError> {
        super::revoke_member_durable(
            storage,
            cloud_home,
            storage.store_root_hash(),
            chain,
            owner_keypair,
            revokee_pubkey,
            store_id,
            timestamp,
            current_encryption,
            db,
        )
        .await
    }

    async fn membership_coords(
        _storage: &MockSyncStorage,
        entry_keys: &[MembershipCoord],
    ) -> Vec<MembershipCoord> {
        entry_keys.to_vec()
    }

    /// Minimal CloudHome mock that returns a dummy S3 JoinInfo.
    struct MockCloudHome;

    #[async_trait]
    impl CloudHome for MockCloudHome {
        async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
            Ok(())
        }
        async fn open_multipart<'a>(
            &'a self,
            _key: &str,
            _total_len: u64,
        ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
            Err(CloudHomeError::Transport(
                "mock has no multipart".to_string(),
            ))
        }
        fn multipart_threshold(&self) -> u64 {
            u64::MAX
        }
        async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
            Err(CloudHomeError::NotFound("mock".to_string()))
        }
        async fn read_range(
            &self,
            _key: &str,
            _start: u64,
            _end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            Err(CloudHomeError::NotFound("mock".to_string()))
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            Ok(vec![])
        }
        async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
            Ok(())
        }
        async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
            Ok(false)
        }
        async fn set_access(
            &self,
            desired: CloudAccessState,
        ) -> Result<CloudAccessOutcome, CloudHomeError> {
            Ok(match desired {
                CloudAccessState::Present { .. } => {
                    CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                        bucket: "test-bucket".to_string(),
                        region: "us-east-1".to_string(),
                        endpoint: None,
                        access_key: "test-access-key".to_string(),
                        secret_key: "test-secret-key".to_string(),
                        key_prefix: None,
                    })
                }
                CloudAccessState::Absent { .. } => {
                    CloudAccessOutcome::Absent(RevokeOutcome::Unsupported)
                }
            })
        }
    }

    /// CloudHome mock that records grant/revoke identities.
    struct RecordingCloudHome {
        accesses: std::sync::Mutex<Vec<CloudAccessState>>,
        fail_next_grant: std::sync::atomic::AtomicBool,
    }

    impl RecordingCloudHome {
        fn new() -> Self {
            Self {
                accesses: std::sync::Mutex::new(Vec::new()),
                fail_next_grant: std::sync::atomic::AtomicBool::new(false),
            }
        }
        fn last_grant(&self) -> Option<CloudAccessState> {
            self.accesses
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|access| matches!(access, CloudAccessState::Present { .. }))
                .cloned()
        }
        fn last_revoke(&self) -> Option<CloudAccessState> {
            self.accesses
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|access| matches!(access, CloudAccessState::Absent { .. }))
                .cloned()
        }
        fn fail_next_grant(&self) {
            self.fail_next_grant
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl CloudHome for RecordingCloudHome {
        async fn put_object(&self, _key: &str, _data: Vec<u8>) -> Result<(), CloudHomeError> {
            Ok(())
        }
        async fn open_multipart<'a>(
            &'a self,
            _key: &str,
            _total_len: u64,
        ) -> Result<crate::storage::cloud::BoxPartSink<'a>, CloudHomeError> {
            Err(CloudHomeError::Transport(
                "mock has no multipart".to_string(),
            ))
        }
        fn multipart_threshold(&self) -> u64 {
            u64::MAX
        }
        async fn read(&self, _key: &str) -> Result<Vec<u8>, CloudHomeError> {
            Err(CloudHomeError::NotFound("mock".to_string()))
        }
        async fn read_range(
            &self,
            _key: &str,
            _start: u64,
            _end: u64,
        ) -> Result<Vec<u8>, CloudHomeError> {
            Err(CloudHomeError::NotFound("mock".to_string()))
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>, CloudHomeError> {
            Ok(vec![])
        }
        async fn delete(&self, _key: &str) -> Result<(), CloudHomeError> {
            Ok(())
        }
        async fn exists(&self, _key: &str) -> Result<bool, CloudHomeError> {
            Ok(false)
        }
        async fn set_access(
            &self,
            desired: CloudAccessState,
        ) -> Result<CloudAccessOutcome, CloudHomeError> {
            self.accesses.lock().unwrap().push(desired.clone());
            match desired {
                CloudAccessState::Present { .. } => {
                    if self
                        .fail_next_grant
                        .swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        return Err(CloudHomeError::Transport(
                            "forced provider regrant failure".to_string(),
                        ));
                    }
                    Ok(CloudAccessOutcome::Present(CloudHomeJoinInfo::S3 {
                        bucket: "test-bucket".to_string(),
                        region: "us-east-1".to_string(),
                        endpoint: None,
                        access_key: "test-access-key".to_string(),
                        secret_key: "test-secret-key".to_string(),
                        key_prefix: None,
                    }))
                }
                CloudAccessState::Absent { .. } => {
                    Ok(CloudAccessOutcome::Absent(RevokeOutcome::Revoked))
                }
            }
        }
    }

    fn gen_keypair() -> UserKeypair {
        UserKeypair::generate()
    }

    /// The store id every invite test wraps keys under. The wrapped-key
    /// signature binds it, so the same id must be passed at unwrap time.
    const LIB_ID: &str = "lib-test";

    /// Verify and unwrap a wrapped key against a single expected Owner, the way an
    /// existing member does when adopting a rotated key. The invite/revoke
    /// mechanics tests use this to assert a wrapped key landed and decrypts under
    /// the right signer, without standing up the sealed, founder-anchored chain
    /// the full joiner path ([`unwrap_store_keyring`]) reads — that path has its
    /// own `InMemoryCloudHome` tests below.
    async fn unwrap_bytes_for_owner(
        cloud_home: &dyn CloudHome,
        keypair: &UserKeypair,
        owner: &str,
    ) -> Result<[u8; 32], InviteError> {
        unwrap_store_keyring_for_owners_with_activation(
            cloud_home,
            keypair,
            LIB_ID,
            std::iter::once(owner),
            None,
        )
        .await
        .map(|keyring| keyring.key_bytes())
    }

    async fn stored_membership_entries(storage: &MockSyncStorage) -> Vec<MembershipEntry> {
        let entry_keys = storage.discover_membership_entries().await;
        download_entries(storage, storage.store_root_hash(), &entry_keys)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, entry)| entry)
            .collect()
    }

    async fn invite_member_for_test(
        storage: &MockSyncStorage,
        chain: &mut MembershipChain,
        owner: &UserKeypair,
        member: &UserKeypair,
        key: &[u8; 32],
        timestamp: &str,
    ) {
        create_invitation(
            storage,
            &MockCloudHome,
            chain,
            owner,
            &pubkey_hex(member),
            None,
            MemberRole::Member,
            key,
            LIB_ID,
            timestamp,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn create_and_unwrap_store_key() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let encryption_key: [u8; 32] = [42u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        // Owner invites the new member.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // Chain should now have 2 entries.
        assert_eq!(chain.entries().len(), 2);
        chain.validate().unwrap();

        // Invitee should be a current member.
        let members = chain.current_members();
        assert!(members
            .iter()
            .any(|(pk, r)| pk == &pubkey_hex(&invitee) && *r == MemberRole::Member));

        // Invitee accepts the invitation: the key is authenticated against the
        // owner that signed it, then adopted.
        let unwrapped = unwrap_bytes_for_owner(&storage, &invitee, &pubkey_hex(&owner))
            .await
            .unwrap();
        assert_eq!(unwrapped, encryption_key);
    }

    /// A recipient's wrap can live under any current owner's prefix, not only the
    /// founder's. The reader scans every current owner and resolves the wrap
    /// wherever it sits — here only owner B (not owner A) wrapped the recipient.
    #[tokio::test]
    async fn unwrap_resolves_wrapped_key_from_any_owner_prefix() {
        let owner_a = gen_keypair();
        let owner_b = gen_keypair();
        let recipient = gen_keypair();
        let key: [u8; 32] = [51u8; 32];

        let storage = MockSyncStorage::new();
        let r_x = recipient.to_x25519_public_key();
        let wrapped =
            signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&recipient), &r_x, &key, &owner_b);
        storage
            .put_wrapped_key(&pubkey_hex(&owner_b), &pubkey_hex(&recipient), wrapped)
            .await
            .unwrap();

        let owners = [pubkey_hex(&owner_a), pubkey_hex(&owner_b)];
        let unwrapped = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &recipient,
            LIB_ID,
            owners.iter().map(String::as_str),
            None,
        )
        .await
        .unwrap()
        .key_bytes();
        assert_eq!(unwrapped, key);
    }

    /// When more than one owner has wrapped the recipient, the reader adopts the
    /// highest-generation wrap — the most recent rotation wins regardless of which
    /// owner produced it.
    #[tokio::test]
    async fn unwrap_takes_highest_generation_across_owner_prefixes() {
        let owner_a = gen_keypair();
        let owner_b = gen_keypair();
        let recipient = gen_keypair();
        let gen1_key: [u8; 32] = [61u8; 32];
        let gen2_key: [u8; 32] = [62u8; 32];

        let storage = MockSyncStorage::new();
        let r_x = recipient.to_x25519_public_key();

        // Owner A holds the generation-1 wrap.
        let gen1 =
            signed_wrapped_key_for_test(LIB_ID, &pubkey_hex(&recipient), &r_x, &gen1_key, &owner_a);
        storage
            .put_wrapped_key(&pubkey_hex(&owner_a), &pubkey_hex(&recipient), gen1)
            .await
            .unwrap();

        // Owner B holds a generation-2 keyring from a later rotation.
        let gen2_keyring = EncryptionService::from_key(gen1_key)
            .with_appended_generation(2, gen2_key)
            .unwrap();
        let gen2 = signed_wrapped_keyring_for_test(
            LIB_ID,
            &pubkey_hex(&recipient),
            &r_x,
            &gen2_keyring,
            &owner_b,
            None,
        );
        storage
            .put_wrapped_key(&pubkey_hex(&owner_b), &pubkey_hex(&recipient), gen2)
            .await
            .unwrap();

        let owners = [pubkey_hex(&owner_a), pubkey_hex(&owner_b)];
        let keyring = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &recipient,
            LIB_ID,
            owners.iter().map(String::as_str),
            None,
        )
        .await
        .unwrap();
        assert_eq!(keyring.current_generation(), 2);
        assert_eq!(keyring.key_bytes(), gen2_key);
    }

    /// The grant identity carries both the cryptographic member pubkey and the
    /// provider account email from the join request.
    #[tokio::test]
    async fn grant_access_receives_pubkey_and_provider_email() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let invitee_pubkey = pubkey_hex(&invitee);
        let encryption_key: [u8; 32] = [5u8; 32];

        let cloud = RecordingCloudHome::new();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &invitee_pubkey,
            Some("a@b.com"),
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();
        assert_eq!(
            cloud.last_grant(),
            Some(CloudAccessState::Present {
                member_pubkey: invitee_pubkey,
                provider_account_email: Some("a@b.com".to_string()),
            })
        );
    }

    #[tokio::test]
    async fn grant_access_allows_absent_provider_email_for_s3_like_homes() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let invitee_pubkey = pubkey_hex(&invitee);
        let encryption_key: [u8; 32] = [5u8; 32];

        let cloud = RecordingCloudHome::new();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &invitee_pubkey,
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();
        assert_eq!(
            cloud.last_grant(),
            Some(CloudAccessState::Present {
                member_pubkey: invitee_pubkey,
                provider_account_email: None,
            })
        );
    }

    #[tokio::test]
    async fn durable_invite_reuses_exact_plan_after_restart_and_lost_head_result() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let invitee_pubkey = pubkey_hex(&invitee);
        let storage = MockSyncStorage::with_store_and_keypair(LIB_ID, owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        let cloud = RecordingCloudHome::new();
        let directory = tempfile::tempdir().expect("membership outbox temp directory");
        let path = directory.path().join("store.sqlite3");
        let open = || {
            Database::open(
                &path,
                test_synced_tables(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                "durable-invite-device".to_string(),
                &test_migrations(),
            )
            .expect("open membership outbox database")
            .0
        };
        let db = open();
        storage.lose_membership_head_append_result_on_call(1);
        let first = create_invitation_with_encryption_durable(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &invitee_pubkey,
            Some("invitee@example.com"),
            MemberRole::Member,
            &EncryptionService::from_key([91; 32]),
            LIB_ID,
            "0000000002000-0000-device",
            &db,
        )
        .await;
        assert!(first
            .expect_err("lost head result must retain the durable mutation")
            .to_string()
            .contains("result lost"),);
        assert!(db
            .outbound_membership_mutation()
            .await
            .expect("read pending invitation")
            .is_some());
        drop(db);

        let reopened = open();
        create_invitation_with_encryption_durable(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &invitee_pubkey,
            Some("invitee@example.com"),
            MemberRole::Member,
            &EncryptionService::from_key([92; 32]),
            LIB_ID,
            "0000000009000-0000-different-retry-time",
            &reopened,
        )
        .await
        .expect("resume exact durable invitation");

        assert!(reopened
            .outbound_membership_mutation()
            .await
            .expect("read completed invitation")
            .is_none());
        assert!(chain
            .current_members()
            .iter()
            .any(|(pubkey, role)| pubkey == &invitee_pubkey && *role == MemberRole::Member));
        assert_eq!(
            cloud
                .accesses
                .lock()
                .unwrap()
                .iter()
                .filter(|access| matches!(access, CloudAccessState::Present { .. }))
                .count(),
            2,
            "restart must reassert and verify the same absolute provider access state",
        );
    }

    #[tokio::test]
    async fn durable_remove_reuses_rotation_after_restart_and_lost_head_result() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let member_pubkey = pubkey_hex(&member);
        let storage = MockSyncStorage::with_store_and_keypair(LIB_ID, owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        let cloud = RecordingCloudHome::new();
        let current = EncryptionService::from_key([93; 32]);
        create_invitation_with_encryption(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &member_pubkey,
            Some("member@example.com"),
            MemberRole::Member,
            &current,
            LIB_ID,
            "0000000002000-0000-device",
        )
        .await
        .expect("establish member before durable removal");

        let directory = tempfile::tempdir().expect("membership outbox temp directory");
        let path = directory.path().join("store.sqlite3");
        let open = || {
            Database::open(
                &path,
                test_synced_tables(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                crate::WritePolicy::MergeConcurrent,
                "durable-remove-device".to_string(),
                &test_migrations(),
            )
            .expect("open membership outbox database")
            .0
        };
        let db = open();
        storage.lose_membership_head_append_result_on_call(1);
        let first = revoke_member_durable(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &member_pubkey,
            LIB_ID,
            "0000000003000-0000-device",
            &current,
            &db,
        )
        .await;
        assert!(first
            .expect_err("lost head result must retain the durable removal")
            .to_string()
            .contains("result lost"),);
        assert!(db
            .outbound_membership_mutation()
            .await
            .expect("read pending removal")
            .is_some());
        drop(db);

        let reopened = open();
        let rotated = revoke_member_durable(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &member_pubkey,
            LIB_ID,
            "0000000009000-0000-different-retry-time",
            &EncryptionService::from_key([94; 32]),
            &reopened,
        )
        .await
        .expect("resume exact durable removal");
        assert_eq!(rotated.current_generation(), 2);
        assert!(!chain
            .current_members()
            .iter()
            .any(|(pubkey, _)| pubkey == &member_pubkey));
        assert!(reopened
            .outbound_membership_mutation()
            .await
            .expect("read completed removal")
            .is_none());
        assert_eq!(
            cloud
                .accesses
                .lock()
                .unwrap()
                .iter()
                .filter(|access| matches!(access, CloudAccessState::Absent { .. }))
                .count(),
            2,
            "unknown provider outcome is retried as the same absolute absent state",
        );
    }

    /// A joiner adopts only a store key the owner it pins signed; a key signed
    /// by anyone else is refused. A bucket writer who is not the owner can seal a
    /// key of their choosing to the joiner's public key (which is public), sign it
    /// with their own identity, and overwrite the invite's wrapped-key object — a
    /// sealed box authenticates only its recipient, not its author. Without
    /// verifying the owner's signature the joiner would take that attacker's key
    /// and the attacker could read everything it encrypts; this test enforces that
    /// it does not.
    #[tokio::test]
    async fn unwrap_refuses_key_not_signed_by_owner() {
        let owner = gen_keypair();
        let attacker = gen_keypair();
        let joiner = gen_keypair();

        let storage = MockSyncStorage::new();

        // The attacker forges a wrapped key: a key they chose (`[0xAA; 32]`),
        // sealed to the joiner's real public key, signed by the attacker (not the
        // owner), written to the joiner's slot.
        let attacker_key: [u8; 32] = [0xAAu8; 32];
        let joiner_x25519 = joiner.to_x25519_public_key();
        let forged = signed_wrapped_key(
            LIB_ID,
            &pubkey_hex(&joiner),
            &joiner_x25519,
            &EncryptionService::from_key(attacker_key),
            &attacker,
        )
        .unwrap();
        storage
            .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&joiner), forged)
            .await
            .unwrap();

        // The joiner adopts only keys signed by an authorized owner.
        let result = unwrap_bytes_for_owner(&storage, &joiner, &pubkey_hex(&owner)).await;
        assert!(
            matches!(result, Err(InviteError::Crypto(_))),
            "a key signed by a non-owner must be refused, got {result:?}",
        );
    }

    /// A wrapped key the owner legitimately signed for one member must not be
    /// adoptable from a *different* member's slot, even though both are real
    /// members. The signature binds the recipient pubkey (the slot), so a bucket
    /// writer can't relocate one member's wrapped key into another's slot.
    #[tokio::test]
    async fn unwrap_refuses_key_relocated_to_another_slot() {
        let owner = gen_keypair();
        let member_a = gen_keypair();
        let member_b = gen_keypair();
        let key: [u8; 32] = [9u8; 32];

        let storage = MockSyncStorage::new();

        // The owner seals the key to member A and signs it for A's slot, but the
        // bytes are written under member B's slot (a relocation a bucket writer
        // can perform).
        let a_x25519 = member_a.to_x25519_public_key();
        let for_a = signed_wrapped_key(
            LIB_ID,
            &pubkey_hex(&member_a),
            &a_x25519,
            &EncryptionService::from_key(key),
            &owner,
        )
        .unwrap();
        storage
            .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member_b), for_a)
            .await
            .unwrap();

        // Member B reads its slot; the signature is over A's pubkey, so it fails.
        let result = unwrap_bytes_for_owner(&storage, &member_b, &pubkey_hex(&owner)).await;
        assert!(
            matches!(result, Err(InviteError::Crypto(_))),
            "a key bound to another member's slot must be refused, got {result:?}",
        );
    }

    #[tokio::test]
    async fn unwrap_store_key_wrong_key_fails() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let wrong_keypair = gen_keypair();
        let encryption_key: [u8; 32] = [7u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // Someone else tries to accept -- should fail (no wrapped key in their
        // slot to even parse).
        let result = unwrap_bytes_for_owner(&storage, &wrong_keypair, &pubkey_hex(&owner)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_invitation_invalid_pubkey_hex() {
        let owner = gen_keypair();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        let encryption_key: [u8; 32] = [0u8; 32];

        let result = create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            "not-valid-hex",
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await;

        assert!(matches!(result, Err(InviteError::Crypto(_))));
    }

    #[tokio::test]
    async fn create_invitation_off_curve_pubkey_errors() {
        let owner = gen_keypair();
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        let encryption_key: [u8; 32] = [0u8; 32];
        let off_curve_pubkey = "0200000000000000000000000000000000000000000000000000000000000000";

        let result = create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            off_curve_pubkey,
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await;

        assert!(matches!(result, Err(InviteError::Crypto(_))));
    }

    #[tokio::test]
    async fn create_invitation_non_owner_fails() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let invitee = gen_keypair();
        let encryption_key: [u8; 32] = [0u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        // Add member first.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // Member (not owner) tries to invite someone.
        let result = create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &member,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await;

        assert!(matches!(result, Err(InviteError::Membership(_))));
    }

    #[tokio::test]
    async fn membership_entry_uploaded_to_bucket() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let encryption_key: [u8; 32] = [1u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // Verify the membership entry was uploaded.
        let entries = storage.discover_membership_entries().await;
        let owner_entries: Vec<_> = entries
            .iter()
            .filter(|coord| coord.author_pubkey == pubkey_hex(&owner))
            .collect();
        assert_eq!(owner_entries.len(), 1);

        // Verify the wrapped key was uploaded.
        let wrapped = storage
            .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&invitee))
            .await
            .unwrap();
        assert!(!wrapped.is_empty());
    }

    #[tokio::test]
    async fn revoke_member_roundtrip() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [42u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        // Owner invites the member.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // Member can unwrap the key.
        let unwrapped = unwrap_bytes_for_owner(&storage, &member, &pubkey_hex(&owner))
            .await
            .unwrap();
        assert_eq!(unwrapped, old_key);

        // Owner revokes the member.
        let new_key = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            LIB_ID,
            "0000000003000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        // New key should be different from old key.
        assert_ne!(new_key.key_bytes(), old_key);

        // Member is no longer in the chain.
        let members = chain.current_members();
        assert!(!members.iter().any(|(pk, _)| pk == &pubkey_hex(&member)));
        assert!(members.iter().any(|(pk, _)| pk == &pubkey_hex(&owner)));

        // Chain should still validate.
        chain.validate().unwrap();

        // Revoked member's wrapped key was deleted from the storage.
        let result = storage
            .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member))
            .await;
        assert!(result.is_err());

        // Owner can still unwrap the new key.
        let visible_entries =
            membership_coords(&storage, &storage.discover_membership_entries().await).await;
        let owner_pk = pubkey_hex(&owner);
        let owner_unwrapped = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &owner,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&merge_activations(&visible_entries)),
        )
        .await
        .unwrap()
        .key_bytes();
        assert_eq!(owner_unwrapped, new_key.key_bytes());

        // The Remove entry was uploaded to the storage.
        let entries = storage.discover_membership_entries().await;
        let owner_entries: Vec<_> = entries
            .iter()
            .filter(|coord| coord.author_pubkey == pubkey_hex(&owner))
            .collect();
        // 1 for invite + 1 for revoke = 2
        assert_eq!(owner_entries.len(), 2);
    }

    /// Crash-retry converges on one seal key, and every interim changeset stays
    /// decryptable. A first removal attempt mints a rotation and durably wraps it
    /// to every remaining member — including this owner's own `keys/{self}/{self}`
    /// slot — then crashes before publishing the head, so the revokee still reads
    /// as current. A member whose cycle already ran adopted that first key. The
    /// owner's retry must re-adopt its own durable prior wrap rather than minting a
    /// second key at the same generation: minting fresh would fork the fleet.
    #[tokio::test]
    async fn revoke_retry_readopts_prior_attempt_instead_of_minting_a_fork() {
        let owner = gen_keypair();
        let member = gen_keypair(); // stays
        let victim = gen_keypair(); // removed
        let owner_pk = pubkey_hex(&owner);
        let old_key: [u8; 32] = [30u8; 32];
        let attempt1_key: [u8; 32] = [31u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &member,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &victim,
            &old_key,
            "0000000003000-0000-dev1",
        )
        .await;

        // Reconstruct the durable state a crashed first attempt would leave: the
        // Remove entry uploaded (so it is listed) but the head NOT advanced, plus
        // the first attempt's generation-2 keyring wrapped to the owner's own slot
        // and to the remaining member's, each activated by that Remove entry.
        let remove_entry = chain
            .signed_remove_member(
                &owner,
                pubkey_hex(&victim),
                "0000000004000-0000-dev1".to_string(),
            )
            .expect("owner removes victim");
        let remove_coord = remove_entry.coord();
        upload_membership_entry(
            &storage,
            storage.store_root_hash(),
            &remove_coord,
            &remove_entry,
        )
        .await
        .unwrap();

        let attempt1 = EncryptionService::from_key(old_key)
            .with_appended_generation(2, attempt1_key)
            .unwrap();
        for recipient in [&owner, &member] {
            let recipient_x = recipient.to_x25519_public_key();
            let wrapped = signed_wrapped_keyring_for_test(
                LIB_ID,
                &pubkey_hex(recipient),
                &recipient_x,
                &attempt1,
                &owner,
                Some(remove_coord.clone()),
            );
            storage
                .put_wrapped_key(&owner_pk, &pubkey_hex(recipient), wrapped)
                .await
                .unwrap();
        }

        // The member's cycle already adopted the first attempt's key.
        let visible =
            membership_coords(&storage, &storage.discover_membership_entries().await).await;
        let member_adopted = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &member,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&merge_activations(&visible)),
        )
        .await
        .unwrap();
        assert_eq!(member_adopted.key_bytes(), attempt1_key);

        // The owner retries the removal from its pre-rotation cipher (its head
        // never advanced, so the victim still reads as current).
        // The storage and the cloud home are one bucket in production (the
        // CloudSyncStorage delegates to the same CloudHome), so the retry's read of
        // its own prior wrap hits the same objects it wrote — pass the mock as both.
        let retried = revoke_member(
            &storage,
            &storage,
            &mut chain,
            &owner,
            &pubkey_hex(&victim),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        assert_eq!(
            retried.key_bytes(),
            attempt1_key,
            "the retry re-adopts its own durable prior wrap, not a fresh fork",
        );
        // The interim changeset the member sealed under the first attempt's key is
        // still decryptable to the owner's retried keyring.
        let interim = attempt1.seal_app_data(b"interim changeset", b"ctx");
        assert_eq!(
            retried.open_app_data(&interim, b"ctx").unwrap(),
            b"interim changeset",
        );
    }

    /// Two owners rotating at once each wrap a distinct key at the same generation
    /// number, under their own prefix. The reader merges both wraps rather than
    /// picking one, so it holds both keys — content sealed by either owner stays
    /// decryptable, and deterministic seal selection converges the fleet.
    #[tokio::test]
    async fn unwrap_merges_concurrent_same_generation_wraps_from_two_owners() {
        let owner_a = gen_keypair();
        let owner_b = gen_keypair();
        let recipient = gen_keypair();
        let base_key: [u8; 32] = [70u8; 32];
        let key_a: [u8; 32] = [0xA1u8; 32];
        let key_b: [u8; 32] = [0xB2u8; 32];

        let storage = MockSyncStorage::new();
        let r_x = recipient.to_x25519_public_key();

        let keyring_a = EncryptionService::from_key(base_key)
            .with_appended_generation(2, key_a)
            .unwrap();
        let keyring_b = EncryptionService::from_key(base_key)
            .with_appended_generation(2, key_b)
            .unwrap();
        let wrapped_a = signed_wrapped_keyring_for_test(
            LIB_ID,
            &pubkey_hex(&recipient),
            &r_x,
            &keyring_a,
            &owner_a,
            None,
        );
        let wrapped_b = signed_wrapped_keyring_for_test(
            LIB_ID,
            &pubkey_hex(&recipient),
            &r_x,
            &keyring_b,
            &owner_b,
            None,
        );
        storage
            .put_wrapped_key(&pubkey_hex(&owner_a), &pubkey_hex(&recipient), wrapped_a)
            .await
            .unwrap();
        storage
            .put_wrapped_key(&pubkey_hex(&owner_b), &pubkey_hex(&recipient), wrapped_b)
            .await
            .unwrap();

        let owners = [pubkey_hex(&owner_a), pubkey_hex(&owner_b)];
        let merged = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &recipient,
            LIB_ID,
            owners.iter().map(String::as_str),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            merged.key_count(),
            3,
            "base key plus both owners' rotations"
        );
        assert_eq!(merged.current_generation(), 2);
        let sealed_a = keyring_a.seal_app_data(b"A", b"ctx");
        let sealed_b = keyring_b.seal_app_data(b"B", b"ctx");
        assert_eq!(merged.open_app_data(&sealed_a, b"ctx").unwrap(), b"A");
        assert_eq!(merged.open_app_data(&sealed_b, b"ctx").unwrap(), b"B");
    }

    #[tokio::test]
    async fn revoke_member_with_multiple_remaining() {
        let owner = gen_keypair();
        let member1 = gen_keypair();
        let member2 = gen_keypair();
        let old_key: [u8; 32] = [10u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        // Invite two members.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member1),
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member2),
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // Revoke member1.
        let new_key = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member1),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        // Both remaining members (owner + member2) can unwrap the new key.
        let visible_entries =
            membership_coords(&storage, &storage.discover_membership_entries().await).await;
        let owner_pk = pubkey_hex(&owner);
        let owner_key = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &owner,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&merge_activations(&visible_entries)),
        )
        .await
        .unwrap()
        .key_bytes();
        assert_eq!(owner_key, new_key.key_bytes());

        let member2_key = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &member2,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&merge_activations(&visible_entries)),
        )
        .await
        .unwrap()
        .key_bytes();
        assert_eq!(member2_key, new_key.key_bytes());

        // member1 cannot get a wrapped key.
        let result = storage
            .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member1))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn unwrap_refuses_removal_key_before_activation_entry_is_visible() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [14u8; 32];
        let new_key: [u8; 32] = [15u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &member,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;

        let owner_pk = pubkey_hex(&owner);
        let activation = MembershipCoord {
            author_pubkey: owner_pk.clone(),
            author_owner_grant: chain
                .active_owner_grant(&owner_pk)
                .expect("founder Owner grant"),
            stream_id: chain
                .preferred_author_stream(
                    &owner_pk,
                    &chain
                        .active_owner_grant(&owner_pk)
                        .expect("founder Owner grant"),
                )
                .expect("founder author stream"),
            seq: 3,
            entry_hash: crate::sync::store_commit::ObjectHash::digest(b"uncommitted removal"),
        };
        let keyring = EncryptionService::from_key(old_key)
            .with_appended_generation(2, new_key)
            .unwrap();
        let member_x25519 = member.to_x25519_public_key();
        let wrapped = signed_wrapped_keyring_for_test(
            LIB_ID,
            &pubkey_hex(&member),
            &member_x25519,
            &keyring,
            &owner,
            Some(activation.clone()),
        );
        storage
            .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member), wrapped)
            .await
            .unwrap();

        let visible_entries =
            membership_coords(&storage, &storage.discover_membership_entries().await).await;
        let result = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &member,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&merge_activations(&visible_entries)),
        )
        .await;

        assert!(matches!(
            result,
            Err(InviteError::InactiveWrappedKey { activation: seen, generation: 2 })
                if seen == WrappedKeyActivation::MergeConcurrent(activation)
        ));
    }

    /// A realistic opaque-home storage: membership entries and wrapped keys are
    /// keyed under the `.enc` suffix a joining device reads them by, which the
    /// suffixless [`MockSyncStorage`] does not reproduce. The join path lists
    /// these keys straight off the home, so the suffix has to match production.
    fn opaque_storage(
        home: Arc<InMemoryCloudHome>,
        key: [u8; 32],
        signer: &UserKeypair,
    ) -> CloudSyncStorage {
        CloudSyncStorage::new(
            home,
            CloudCipher::Encrypted(EncryptionService::from_key(key)),
            BlobPathScheme::for_storage(HomeStorage::Opaque),
            LIB_ID,
            signer.clone(),
        )
        .with_copy_ids(Arc::new(crate::storage::cloud::RandomCopyIdGenerator))
    }

    /// A member invited but not yet joined is a current member, so revoking a
    /// third member re-wraps the pending invitee's slot with an activation naming
    /// the Remove entry. When the invitee finally joins, `unwrap_store_keyring`
    /// lists the now-visible membership entries and the activation resolves, so
    /// the join succeeds and the invitee adopts the post-rotation key generation.
    #[tokio::test]
    async fn pending_invitee_joins_after_a_third_member_is_revoked() {
        let owner = gen_keypair();
        let pending = gen_keypair();
        let third = gen_keypair();
        let old_key: [u8; 32] = [20u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), old_key, &owner);
        // The founder entry lives on the home in production (written at store
        // creation); the joiner reads and anchors the chain to it, so found there.
        let (store_protocol_root, mut chain) =
            publish_test_protocol_roots(&storage, "test-store", &owner, "0000000001000-0000-dev1")
                .await;

        super::create_invitation(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &owner,
            &pubkey_hex(&pending),
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();
        super::create_invitation(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &owner,
            &pubkey_hex(&third),
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // Revoke the third member. `pending` is still a current member, so its
        // slot is re-wrapped with the new key and an activation for the Remove.
        let new_keyring = super::revoke_member(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &owner,
            &pubkey_hex(&third),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        // The pending invitee joins now, resolving the activation against the
        // listed Remove entry rather than being refused for lack of one.
        let joined = unwrap_store_keyring(
            home.clone(),
            &pending,
            store_protocol_root.object_hash(),
            LIB_ID,
            &pubkey_hex(&owner),
            &chain.author_heads(),
        )
        .await
        .unwrap();

        assert_eq!(
            joined.key_bytes(),
            new_keyring.key_bytes(),
            "the joiner adopts the post-rotation store key"
        );
        assert_eq!(
            joined.current_generation(),
            new_keyring.current_generation(),
            "the joiner is at the rotated generation"
        );
        assert_eq!(
            joined.current_generation(),
            2,
            "the rotation advanced the generation past the pre-revoke one"
        );
    }

    /// Resolving the activation by listing the home preserves the security
    /// property: an activation naming an entry the joiner cannot list is still
    /// refused, so a slot cannot be adopted before its Remove is durably visible.
    #[tokio::test]
    async fn join_refuses_wrapped_key_whose_activation_is_not_visible() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [21u8; 32];
        let new_key: [u8; 32] = [22u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), old_key, &owner);
        let (store_protocol_root, mut chain) =
            publish_test_protocol_roots(&storage, "test-store", &owner, "0000000001000-0000-dev1")
                .await;
        super::create_invitation(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // Overwrite the member's slot with a key whose activation names a Remove
        // entry that was never published — nothing sits at membership/{owner}/99.
        let owner_pk = pubkey_hex(&owner);
        let activation = MembershipCoord {
            author_pubkey: owner_pk.clone(),
            author_owner_grant: chain
                .active_owner_grant(&owner_pk)
                .expect("founder Owner grant"),
            stream_id: chain
                .preferred_author_stream(
                    &owner_pk,
                    &chain
                        .active_owner_grant(&owner_pk)
                        .expect("founder Owner grant"),
                )
                .expect("founder author stream"),
            seq: 99,
            entry_hash: crate::sync::store_commit::ObjectHash::digest(b"missing removal"),
        };
        let keyring = EncryptionService::from_key(old_key)
            .with_appended_generation(2, new_key)
            .unwrap();
        let member_x25519 = member.to_x25519_public_key();
        let wrapped = signed_wrapped_keyring_for_test(
            LIB_ID,
            &pubkey_hex(&member),
            &member_x25519,
            &keyring,
            &owner,
            Some(activation.clone()),
        );
        storage
            .put_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member), wrapped)
            .await
            .unwrap();

        let result = unwrap_store_keyring(
            home.clone(),
            &member,
            store_protocol_root.object_hash(),
            LIB_ID,
            &owner_pk,
            &chain.author_heads(),
        )
        .await;
        assert!(
            matches!(
                &result,
                Err(InviteError::InactiveWrappedKey { activation: seen, .. })
                    if *seen == WrappedKeyActivation::MergeConcurrent(activation)
            ),
            "an activation with no visible entry must be refused, got {result:?}"
        );
    }

    /// A non-founder Owner's invite is joinable: the founder promotes B to Owner,
    /// B invites C, and C joins by authenticating B's wrapped key against the
    /// Owner set derived from the founder-anchored chain — B is an Owner as of the
    /// entry that added C, so the key is adopted even though the founder never
    /// signed it.
    #[tokio::test]
    async fn non_founder_owner_invite_is_joinable() {
        let founder = gen_keypair();
        let second_owner = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [7u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let founder_storage = opaque_storage(home.clone(), key, &founder);
        let (store_protocol_root, mut chain) = publish_test_protocol_roots(
            &founder_storage,
            LIB_ID,
            &founder,
            "0000000001000-0000-dev1",
        )
        .await;

        // The founder promotes B to Owner.
        super::create_invitation(
            &founder_storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &founder,
            &pubkey_hex(&second_owner),
            None,
            MemberRole::Owner,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // B, signing under its own identity, invites C.
        let second_owner_storage = opaque_storage(home.clone(), key, &second_owner);
        let second_owner_db = crate::sync::test_helpers::open_test_db();
        super::create_invitation_with_encryption_durable(
            &second_owner_storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &second_owner,
            &pubkey_hex(&joiner),
            None,
            MemberRole::Member,
            &EncryptionService::from_key(key),
            LIB_ID,
            "0000000003000-0000-dev1",
            &second_owner_db,
        )
        .await
        .unwrap();

        // C joins, pinning the founder. B is still a current Owner, so B's wrapped
        // key verifies against the current Owner set ({founder, B}) and the join
        // succeeds.
        let joined = unwrap_store_keyring(
            home.clone(),
            &joiner,
            store_protocol_root.object_hash(),
            LIB_ID,
            &pubkey_hex(&founder),
            &chain.author_heads(),
        )
        .await
        .expect("a non-founder owner's invite is joinable");
        assert_eq!(joined.key_bytes(), key);
    }

    /// An invite from an Owner since removed is not joinable: the joiner scans the
    /// *current* Owner set for its wrap, and once B is removed its prefix is no
    /// longer scanned, so C's wrap — which only ever lived under B — is unreachable
    /// and the join finds no key to adopt. (The removal also rotated the key, so
    /// B's wrap holds a stale generation regardless.)
    #[tokio::test]
    async fn invite_from_a_removed_owner_is_not_joinable() {
        let founder = gen_keypair();
        let second_owner = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [10u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let founder_storage = opaque_storage(home.clone(), key, &founder);
        let (store_protocol_root, mut chain) = publish_test_protocol_roots(
            &founder_storage,
            LIB_ID,
            &founder,
            "0000000001000-0000-dev1",
        )
        .await;

        // The founder promotes B to Owner; B invites C, signing C's slot.
        super::create_invitation(
            &founder_storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &founder,
            &pubkey_hex(&second_owner),
            None,
            MemberRole::Owner,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();
        let second_owner_storage = opaque_storage(home.clone(), key, &second_owner);
        let second_owner_db = crate::sync::test_helpers::open_test_db();
        super::create_invitation_with_encryption_durable(
            &second_owner_storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &second_owner,
            &pubkey_hex(&joiner),
            None,
            MemberRole::Member,
            &EncryptionService::from_key(key),
            LIB_ID,
            "0000000003000-0000-dev1",
            &second_owner_db,
        )
        .await
        .unwrap();

        // The founder removes B before C joins. C's slot still holds B's signature
        // (the removal here does not re-wrap it).
        let founder_pk = pubkey_hex(&founder);
        let remove_b = chain
            .signed_remove_member(
                &founder,
                pubkey_hex(&second_owner),
                "0000000004000-0000-dev1".to_string(),
            )
            .expect("founder removes second owner");
        let remove_coord = remove_b.coord();
        let remove_seq = remove_coord.seq;
        chain.add_entry_at(remove_coord, remove_b.clone()).unwrap();
        crate::sync::test_helpers::append_membership_entry_bytes(
            &founder_storage,
            store_protocol_root.object_hash(),
            &founder_pk,
            remove_seq,
            serde_json::to_vec(&remove_b).unwrap(),
        )
        .await
        .unwrap();
        crate::sync::membership_ops::publish_membership_stream_head(
            &founder_storage,
            store_protocol_root.object_hash(),
            &chain,
            &founder,
            remove_b.stream_id,
        )
        .await
        .unwrap();

        // C joins: B is no longer a current Owner, so its prefix is not scanned and
        // C's wrap (only ever under B) is unreachable — no current owner vouches for
        // C, so the join finds no adoptable key.
        let result = unwrap_store_keyring(
            home.clone(),
            &joiner,
            store_protocol_root.object_hash(),
            LIB_ID,
            &founder_pk,
            &chain.author_heads(),
        )
        .await;
        assert!(
            matches!(
                result,
                Err(InviteError::CloudHome(CloudHomeError::NotFound(_)))
            ),
            "an invite from a since-removed Owner must not be joinable, got {result:?}",
        );
    }

    /// A wrapped key signed by a member who is not an Owner is refused: the member
    /// holds the store key, so it can seal the real key to the joiner, but it is
    /// not in the Owner set derived from the founder-anchored chain, so the joiner
    /// will not adopt what it wrapped.
    #[tokio::test]
    async fn join_refuses_wrapped_key_signed_by_non_owner_member() {
        let founder = gen_keypair();
        let member = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [8u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), key, &founder);
        let (store_protocol_root, mut chain) = publish_test_protocol_roots(
            &storage,
            "test-store",
            &founder,
            "0000000001000-0000-dev1",
        )
        .await;

        // The founder adds a plain Member and the joiner.
        super::create_invitation(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &founder,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();
        super::create_invitation(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &founder,
            &pubkey_hex(&joiner),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // The member (not an Owner) seals the real store key to the joiner and
        // signs it, overwriting the founder-signed slot.
        let joiner_x25519 = joiner.to_x25519_public_key();
        let forged = signed_wrapped_key(
            LIB_ID,
            &pubkey_hex(&joiner),
            &joiner_x25519,
            &EncryptionService::from_key(key),
            &member,
        )
        .unwrap();
        storage
            .put_wrapped_key(&pubkey_hex(&founder), &pubkey_hex(&joiner), forged)
            .await
            .unwrap();

        let result = unwrap_store_keyring(
            home.clone(),
            &joiner,
            store_protocol_root.object_hash(),
            LIB_ID,
            &pubkey_hex(&founder),
            &chain.author_heads(),
        )
        .await;
        assert!(
            matches!(result, Err(InviteError::Crypto(_))),
            "a key signed by a non-Owner member must be refused, got {result:?}",
        );
    }

    /// A wrapped key signed by a member who self-published an Owner add that is not
    /// committed in the founder-anchored chain is refused: the Owner set is derived
    /// only from the anchored, committed chain, so the uncommitted self-promotion
    /// grants no authority.
    #[tokio::test]
    async fn join_refuses_wrapped_key_signed_by_uncommitted_owner() {
        let founder = gen_keypair();
        let rogue = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [9u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), key, &founder);
        let (store_protocol_root, mut chain) = publish_test_protocol_roots(
            &storage,
            "test-store",
            &founder,
            "0000000001000-0000-dev1",
        )
        .await;

        // The founder adds `rogue` as a plain Member (so it holds the store key)
        // and the joiner.
        super::create_invitation(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &founder,
            &pubkey_hex(&rogue),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();
        super::create_invitation(
            &storage,
            &MockCloudHome,
            store_protocol_root.object_hash(),
            &mut chain,
            &founder,
            &pubkey_hex(&joiner),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // `rogue` self-publishes a founder-style Owner add at membership/{rogue}/1
        // but never publishes its head, so the entry stays uncommitted and never
        // enters the anchored chain.
        let rogue_self_add =
            crate::sync::membership::founder_entry("test-store", &rogue, "0000000002500-0000-dev1");
        crate::sync::test_helpers::append_membership_entry_bytes(
            &storage,
            store_protocol_root.object_hash(),
            &pubkey_hex(&rogue),
            1,
            serde_json::to_vec(&rogue_self_add).unwrap(),
        )
        .await
        .unwrap();

        // `rogue` seals the real key to the joiner and signs it as if it were an
        // Owner, overwriting the founder-signed slot.
        let joiner_x25519 = joiner.to_x25519_public_key();
        let forged = signed_wrapped_key(
            LIB_ID,
            &pubkey_hex(&joiner),
            &joiner_x25519,
            &EncryptionService::from_key(key),
            &rogue,
        )
        .unwrap();
        storage
            .put_wrapped_key(&pubkey_hex(&founder), &pubkey_hex(&joiner), forged)
            .await
            .unwrap();

        let result = unwrap_store_keyring(
            home.clone(),
            &joiner,
            store_protocol_root.object_hash(),
            LIB_ID,
            &pubkey_hex(&founder),
            &chain.author_heads(),
        )
        .await;
        assert!(
            matches!(result, Err(InviteError::Crypto(_))),
            "a key signed by an uncommitted self-promoted Owner must be refused, got {result:?}",
        );
    }

    #[tokio::test]
    async fn revoke_member_does_not_publish_remove_before_rewrap() {
        let owner = gen_keypair();
        let revokee = gen_keypair();
        let remaining = gen_keypair();
        let old_key: [u8; 32] = [11u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &revokee,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &remaining,
            &old_key,
            "0000000003000-0000-dev1",
        )
        .await;

        storage.fail_wrapped_key_put_on_call(1);
        let result = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&revokee),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await;

        assert!(result.is_err(), "injected re-wrap failure must surface");
        assert!(
            chain
                .current_members()
                .iter()
                .any(|(pk, _)| pk == &pubkey_hex(&revokee)),
            "the caller's chain must not advance before the Remove commit point",
        );
        let stored_entries = stored_membership_entries(&storage).await;
        assert!(
            !stored_entries.iter().any(|entry| {
                matches!(
                    &entry.change,
                    crate::sync::membership::MembershipChange::RemoveMember {
                        user_pubkey,
                        ..
                    } if user_pubkey == &pubkey_hex(&revokee)
                )
            }),
            "the Remove entry must not be published before all re-wraps land",
        );
        assert!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&revokee))
                .await
                .is_ok(),
            "the revokee key is deleted only after remaining members are re-wrapped",
        );
    }

    #[tokio::test]
    async fn revoke_member_validates_remaining_pubkeys_before_rewrap() {
        let owner = gen_keypair();
        let revokee = gen_keypair();
        let remaining = gen_keypair();
        let old_key: [u8; 32] = [16u8; 32];
        let off_curve_pubkey = "0200000000000000000000000000000000000000000000000000000000000000";

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &revokee,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &remaining,
            &old_key,
            "0000000003000-0000-dev1",
        )
        .await;
        let remaining_key_before = storage
            .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&remaining))
            .await
            .unwrap();

        let invalid_entry = chain
            .signed_set_member(
                &owner,
                off_curve_pubkey.to_string(),
                None,
                MemberRole::Member,
                "0000000004000-0000-dev1".to_string(),
            )
            .expect("owner adds syntactically invalid member key");
        chain.add_entry(invalid_entry).unwrap();

        let result = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&revokee),
            LIB_ID,
            "0000000005000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await;

        assert!(matches!(result, Err(InviteError::Crypto(_))));
        assert_eq!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&remaining))
                .await
                .unwrap(),
            remaining_key_before,
            "valid remaining member key is not overwritten before corrupt member key fails"
        );
        assert!(
            chain
                .current_members()
                .iter()
                .any(|(pk, _)| pk == &pubkey_hex(&revokee)),
            "the caller's chain must not advance before all remaining pubkeys validate",
        );
    }

    #[tokio::test]
    async fn revoke_member_completes_on_retry_after_partial_rewrap() {
        let owner = gen_keypair();
        let revokee = gen_keypair();
        let remaining_a = gen_keypair();
        let remaining_b = gen_keypair();
        let old_key: [u8; 32] = [12u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &revokee,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &remaining_a,
            &old_key,
            "0000000003000-0000-dev1",
        )
        .await;
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &remaining_b,
            &old_key,
            "0000000004000-0000-dev1",
        )
        .await;

        storage.fail_wrapped_key_put_on_call(3);
        let first = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&revokee),
            LIB_ID,
            "0000000005000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await;
        assert!(first.is_err(), "injected partial re-wrap must fail loud");

        let new_key = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&revokee),
            LIB_ID,
            "0000000005000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .expect("retry completes the removal");

        assert!(
            !chain
                .current_members()
                .iter()
                .any(|(pk, _)| pk == &pubkey_hex(&revokee)),
            "retry commits the Remove to the caller's chain",
        );
        assert!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&revokee))
                .await
                .is_err(),
            "retry deletes the revokee's wrapped key",
        );

        let visible_entries =
            membership_coords(&storage, &storage.discover_membership_entries().await).await;
        let owner_pk = pubkey_hex(&owner);
        for member in [&owner, &remaining_a, &remaining_b] {
            let unwrapped = unwrap_store_keyring_for_owners_with_activation(
                &storage as &dyn CloudHome,
                member,
                LIB_ID,
                std::iter::once(owner_pk.as_str()),
                Some(&merge_activations(&visible_entries)),
            )
            .await
            .unwrap()
            .key_bytes();
            assert_eq!(unwrapped, new_key.key_bytes());
        }
    }

    #[tokio::test]
    async fn revoke_member_retry_after_visible_remove_keeps_existing_rotation() {
        let owner = gen_keypair();
        let revokee = gen_keypair();
        let remaining = gen_keypair();
        let old_key: [u8; 32] = [13u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &revokee,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;
        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &remaining,
            &old_key,
            "0000000003000-0000-dev1",
        )
        .await;

        let committed_key = revoke_member(
            &storage,
            &storage,
            &mut chain,
            &owner,
            &pubkey_hex(&revokee),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        let retry_key = revoke_member(
            &storage,
            &storage,
            &mut chain,
            &owner,
            &pubkey_hex(&revokee),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        assert_eq!(retry_key.key_bytes(), committed_key.key_bytes());

        let visible_entries =
            membership_coords(&storage, &storage.discover_membership_entries().await).await;
        let owner_pk = pubkey_hex(&owner);
        let remaining_key = unwrap_store_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &remaining,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&merge_activations(&visible_entries)),
        )
        .await
        .unwrap()
        .key_bytes();
        assert_eq!(remaining_key, committed_key.key_bytes());
    }

    #[tokio::test]
    async fn revoke_member_uses_latest_active_provider_email() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let member_pubkey = pubkey_hex(&member);
        let old_key: [u8; 32] = [42u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &member_pubkey,
            Some("first@example.com"),
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &member_pubkey,
            Some("second@example.com"),
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        revoke_member(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &member_pubkey,
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        assert_eq!(
            cloud.last_revoke(),
            Some(CloudAccessState::Absent {
                member_pubkey,
                provider_account_email: Some("second@example.com".to_string()),
            })
        );
    }

    #[tokio::test]
    async fn failed_removal_head_restores_every_wrap_and_provider_grant() {
        let owner = gen_keypair();
        let revokee = gen_keypair();
        let remaining = gen_keypair();
        let old_key = [73_u8; 32];
        let owner_pubkey = pubkey_hex(&owner);
        let revokee_pubkey = pubkey_hex(&revokee);
        let remaining_pubkey = pubkey_hex(&remaining);
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &revokee_pubkey,
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000002000-0000-owner",
        )
        .await
        .unwrap();
        create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &remaining_pubkey,
            None,
            MemberRole::Member,
            &old_key,
            LIB_ID,
            "0000000003000-0000-owner",
        )
        .await
        .unwrap();
        let revokee_wrap = storage
            .get_wrapped_key(&owner_pubkey, &revokee_pubkey)
            .await
            .unwrap();
        let remaining_wrap = storage
            .get_wrapped_key(&owner_pubkey, &remaining_pubkey)
            .await
            .unwrap();
        assert!(storage
            .get_wrapped_key(&owner_pubkey, &owner_pubkey)
            .await
            .is_err());

        storage.fail_membership_head_append_on_call(1);
        let error = revoke_member(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &revokee_pubkey,
            LIB_ID,
            "0000000004000-0000-owner",
            &EncryptionService::from_key(old_key),
        )
        .await
        .expect_err("the removal head write is the commit point");

        assert!(error.to_string().contains("publish membership head"));
        assert!(chain
            .current_members()
            .iter()
            .any(|(member, _)| member == &revokee_pubkey));
        assert_eq!(
            storage
                .get_wrapped_key(&owner_pubkey, &revokee_pubkey)
                .await
                .unwrap(),
            revokee_wrap
        );
        assert_eq!(
            storage
                .get_wrapped_key(&owner_pubkey, &remaining_pubkey)
                .await
                .unwrap(),
            remaining_wrap
        );
        assert!(storage
            .get_wrapped_key(&owner_pubkey, &owner_pubkey)
            .await
            .is_err());
        assert_eq!(
            cloud.last_grant(),
            Some(CloudAccessState::Present {
                member_pubkey: revokee_pubkey.clone(),
                provider_account_email: None,
            })
        );
        assert_eq!(
            cloud.last_revoke(),
            Some(CloudAccessState::Absent {
                member_pubkey: revokee_pubkey,
                provider_account_email: None,
            })
        );
    }

    #[tokio::test]
    async fn failed_provider_restore_reports_original_and_rollback_state() {
        let owner = gen_keypair();
        let revokee = gen_keypair();
        let remaining = gen_keypair();
        let old_key = [74_u8; 32];
        let owner_pubkey = pubkey_hex(&owner);
        let revokee_pubkey = pubkey_hex(&revokee);
        let remaining_pubkey = pubkey_hex(&remaining);
        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        for (member, timestamp) in [
            (&revokee, "0000000002000-0000-owner"),
            (&remaining, "0000000003000-0000-owner"),
        ] {
            create_invitation(
                &storage,
                &cloud,
                &mut chain,
                &owner,
                &pubkey_hex(member),
                None,
                MemberRole::Member,
                &old_key,
                LIB_ID,
                timestamp,
            )
            .await
            .unwrap();
        }
        let revokee_wrap = storage
            .get_wrapped_key(&owner_pubkey, &revokee_pubkey)
            .await
            .unwrap();
        let remaining_wrap = storage
            .get_wrapped_key(&owner_pubkey, &remaining_pubkey)
            .await
            .unwrap();

        storage.fail_membership_head_append_on_call(1);
        cloud.fail_next_grant();
        let error = revoke_member(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &revokee_pubkey,
            LIB_ID,
            "0000000004000-0000-owner",
            &EncryptionService::from_key(old_key),
        )
        .await
        .expect_err("a failed rollback must surface both failures");

        let InviteError::Rollback {
            operation,
            original,
            rollback,
        } = error
        else {
            panic!("expected rollback error");
        };
        assert_eq!(operation, "publish membership head");
        assert!(
            original.contains("forced membership-head append failure"),
            "{original}"
        );
        assert!(
            rollback.contains("forced provider regrant failure"),
            "{rollback}"
        );
        assert!(chain
            .current_members()
            .iter()
            .any(|(member, _)| member == &revokee_pubkey));
        assert_eq!(
            storage
                .get_wrapped_key(&owner_pubkey, &revokee_pubkey)
                .await
                .unwrap(),
            revokee_wrap
        );
        assert_eq!(
            storage
                .get_wrapped_key(&owner_pubkey, &remaining_pubkey)
                .await
                .unwrap(),
            remaining_wrap
        );
        assert_eq!(
            cloud
                .accesses
                .lock()
                .unwrap()
                .iter()
                .filter(|access| matches!(access, CloudAccessState::Absent { .. }))
                .count(),
            1
        );
        assert_eq!(
            cloud.last_grant(),
            Some(CloudAccessState::Present {
                member_pubkey: revokee_pubkey,
                provider_account_email: None,
            })
        );
    }

    #[tokio::test]
    async fn revoke_non_member_fails() {
        let owner = gen_keypair();
        let outsider = gen_keypair();

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        let result = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&outsider),
            LIB_ID,
            "0000000002000-0000-dev1",
            &EncryptionService::from_key([42u8; 32]),
        )
        .await;

        assert!(matches!(result, Err(InviteError::NotAMember(_))));
    }

    #[tokio::test]
    async fn revoke_last_owner_fails() {
        let owner = gen_keypair();
        let member = gen_keypair();

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        // Add a regular member.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &[42u8; 32],
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        // Owner tries to revoke themselves (the only owner).
        let result = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&owner),
            LIB_ID,
            "0000000003000-0000-dev1",
            &EncryptionService::from_key([42u8; 32]),
        )
        .await;

        assert!(matches!(result, Err(InviteError::LastOwner)));
    }

    #[tokio::test]
    async fn non_owner_revoke_fails() {
        let owner = gen_keypair();
        let member1 = gen_keypair();
        let member2 = gen_keypair();

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        // Add two members.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member1),
            None,
            MemberRole::Member,
            &[42u8; 32],
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .unwrap();

        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member2),
            None,
            MemberRole::Member,
            &[42u8; 32],
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // Member (not owner) tries to revoke another member.
        let result = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            &member1,
            &pubkey_hex(&member2),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key([42u8; 32]),
        )
        .await;

        assert!(matches!(result, Err(InviteError::Membership(_))));
    }

    /// A failed head publish leaves the invite entry uploaded but uncommitted; the
    /// retry recomputes the same seq from the committed head, overwrites the orphan,
    /// and converges. Every reader loads a well-formed chain throughout — the orphan
    /// never re-admits itself above the head, so no author link ever breaks.
    #[tokio::test]
    async fn invite_converges_after_failed_head_publish() {
        use crate::sync::membership_ops::load_anchored_chain;

        let owner = gen_keypair();
        let invitee = gen_keypair();
        let owner_pk = pubkey_hex(&owner);
        let key: [u8; 32] = [21u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        storage.publish_protocol_founder_membership().await;

        let load = |visible: Vec<MembershipCoord>| {
            let owner_pk = owner_pk.clone();
            let storage = &storage;
            async move {
                load_anchored_chain(
                    storage,
                    storage.store_root_hash(),
                    &visible,
                    Some(&owner_pk),
                    None,
                )
                .await
                .unwrap()
            }
        };

        // Attempt 1: the entry uploads, then the head publish fails.
        let mut chain = load(storage.discover_membership_entries().await).await;
        storage.fail_membership_head_append_on_call(1);
        let first = create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await;
        assert!(first.is_err(), "the head-publish failure must surface");

        // The orphan entry is uncommitted: the committed head is still the founder's.
        let committed = load(storage.discover_membership_entries().await).await;
        assert!(
            !committed
                .current_members()
                .iter()
                .any(|(pk, _)| pk == &pubkey_hex(&invitee)),
            "the entry stays uncommitted until its author's head covers it"
        );

        // Retry from the committed chain: same seq, orphan overwritten, head published.
        let mut retry_chain = committed;
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut retry_chain,
            &owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .expect("the retry converges");

        let loaded = load(storage.discover_membership_entries().await).await;
        assert!(loaded
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&invitee)));
        let unwrapped = unwrap_bytes_for_owner(&storage, &invitee, &owner_pk)
            .await
            .unwrap();
        assert_eq!(unwrapped, key);
    }

    /// Same author, two devices, sequenced (not concurrently interleaved): the
    /// second device acts from a head that the first has already advanced, so its
    /// write at that seq would collide with the first's committed entry. The
    /// monotonic guard fails it loud (rather than clobbering the committed entry and
    /// breaking every reader's chain); the retry rebuilds on the observed head and
    /// both invites end committed. The simultaneous-read race is the residual window
    /// documented on `guard_extends_committed_head`, not covered here.
    #[tokio::test]
    async fn same_author_two_devices_second_fails_loud_then_retry_converges() {
        use crate::sync::membership_ops::load_anchored_chain;

        let owner = gen_keypair();
        let first_invitee = gen_keypair();
        let second_invitee = gen_keypair();
        let owner_pk = pubkey_hex(&owner);
        let key: [u8; 32] = [22u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        storage.publish_protocol_founder_membership().await;

        let load = |visible: Vec<MembershipCoord>| {
            let owner_pk = owner_pk.clone();
            let storage = &storage;
            async move {
                load_anchored_chain(
                    storage,
                    storage.store_root_hash(),
                    &visible,
                    Some(&owner_pk),
                    None,
                )
                .await
                .unwrap()
            }
        };

        // Both devices observe the founder head; device two keeps that stale view.
        let mut device_one = load(storage.discover_membership_entries().await).await;
        let mut device_two_stale = device_one.clone();

        // Device one invites and commits at seq 2.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut device_one,
            &owner,
            &pubkey_hex(&first_invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .expect("device one commits");

        // Device two, still at the founder head, computes the same seq 2 — but the
        // committed head already advanced, so the guard fails it loud.
        let stale = create_invitation(
            &storage,
            &MockCloudHome,
            &mut device_two_stale,
            &owner,
            &pubkey_hex(&second_invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await;
        assert!(
            matches!(
                stale,
                Err(InviteError::StaleMembershipHead { attempted: 2, .. })
            ),
            "the stale-seq publish must fail loud, got {stale:?}"
        );

        // The first device's committed entry was not clobbered.
        let after_stale = load(storage.discover_membership_entries().await).await;
        assert!(after_stale
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&first_invitee)));

        // Device two retries on top of the observed head at seq 3.
        let mut device_two_fresh = after_stale;
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut device_two_fresh,
            &owner,
            &pubkey_hex(&second_invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000004000-0000-dev1",
        )
        .await
        .expect("the retry converges");

        let loaded = load(storage.discover_membership_entries().await).await;
        let members = loaded.current_members();
        assert!(members
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&first_invitee)));
        assert!(members
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&second_invitee)));
    }

    /// The stale-head guard rolls back like the entry-upload path that follows it.
    /// A second device of the same owner, acting from a head the first has already
    /// advanced, grants access and writes the wrapped-key slot before the guard
    /// observes the committed head at its computed seq. The invite fails loud with
    /// `StaleMembershipHead`, and — because the Add never committed — the grant is
    /// revoked and the slot deleted, so no durable state lets a never-added invitee
    /// read the store while peers reject their writes.
    #[tokio::test]
    async fn stale_head_guard_failure_rolls_back_grant_and_slot() {
        use crate::sync::membership_ops::load_anchored_chain;

        let owner = gen_keypair();
        let committed_invitee = gen_keypair();
        let guard_victim = gen_keypair();
        let owner_pk = pubkey_hex(&owner);
        let key: [u8; 32] = [44u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        storage.publish_protocol_founder_membership().await;

        let load = |visible: Vec<MembershipCoord>| {
            let owner_pk = owner_pk.clone();
            let storage = &storage;
            async move {
                load_anchored_chain(
                    storage,
                    storage.store_root_hash(),
                    &visible,
                    Some(&owner_pk),
                    None,
                )
                .await
                .unwrap()
            }
        };

        // Both devices observe the founder head; device two keeps that stale view.
        let mut device_one = load(storage.discover_membership_entries().await).await;
        let mut device_two_stale = device_one.clone();

        // Device one invites and commits at seq 2, advancing the owner's head.
        create_invitation(
            &storage,
            &cloud,
            &mut device_one,
            &owner,
            &pubkey_hex(&committed_invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await
        .expect("device one commits");

        // Device two, still at the founder head, computes the same seq 2. Its grant
        // and wrapped-key slot are written before the guard sees the advanced head.
        let stale = create_invitation(
            &storage,
            &cloud,
            &mut device_two_stale,
            &owner,
            &pubkey_hex(&guard_victim),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await;
        assert!(
            matches!(
                stale,
                Err(InviteError::StaleMembershipHead { attempted: 2, .. })
            ),
            "the stale-seq invite must fail loud, got {stale:?}",
        );

        // The never-committed invitee's wrapped-key slot was deleted on rollback.
        assert!(
            storage
                .get_wrapped_key(&owner_pk, &pubkey_hex(&guard_victim))
                .await
                .is_err(),
            "a stale-head failure must delete the slot it wrote for a non-member",
        );

        // The access this invite granted was revoked, so no dangling grant remains.
        assert_eq!(
            cloud.last_revoke(),
            Some(CloudAccessState::Absent {
                member_pubkey: pubkey_hex(&guard_victim),
                provider_account_email: None,
            }),
            "a stale-head failure must revoke the access it granted a non-member",
        );

        // The committed invitee is untouched; the guard victim never joined.
        let loaded = load(storage.discover_membership_entries().await).await;
        assert!(loaded
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&committed_invitee)));
        assert!(!loaded
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&guard_victim)));
    }

    /// Re-inviting a current member overwrites their wrapped-key slot before the
    /// entry upload. If that upload fails, the rollback must restore the member's
    /// original wrapped key byte-for-byte — not delete the slot, which would leave
    /// the member one rotation from losing access — and must leave the member's
    /// cloud access alone, since they held it before this invitation.
    #[tokio::test]
    async fn reinvite_entry_failure_restores_prior_wrapped_key_and_keeps_access() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [30u8; 32];
        let new_key: [u8; 32] = [31u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &member,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;
        let prior_wrapped = storage
            .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member))
            .await
            .unwrap();

        // Re-invite the same member with a different key, but the entry upload fails
        // after the wrapped-key slot has already been overwritten.
        storage.fail_membership_entry_append_on_call(1);
        let result = create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &new_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await;
        assert!(
            result.is_err(),
            "the injected entry-upload failure must surface"
        );

        // The slot holds the exact prior object, not the failed invite's key.
        assert_eq!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member))
                .await
                .unwrap(),
            prior_wrapped,
            "rollback restores the member's prior wrapped key byte-for-byte",
        );

        // The member kept their cloud access: the rollback did not revoke it.
        assert!(
            cloud.last_revoke().is_none(),
            "re-inviting a current member must not revoke the access they already held",
        );

        // The member's refresh still unwraps their original key.
        let unwrapped = unwrap_bytes_for_owner(&storage, &member, &pubkey_hex(&owner))
            .await
            .unwrap();
        assert_eq!(unwrapped, old_key);
    }

    /// Inviting someone who is not yet a member and whose entry upload fails: the
    /// rollback deletes the wrapped key it just wrote — leaving no stale object
    /// behind — and revokes the cloud access this invitation granted.
    #[tokio::test]
    async fn non_member_entry_failure_deletes_slot_and_revokes_access() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let key: [u8; 32] = [32u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        assert!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&invitee))
                .await
                .is_err(),
            "the invitee has no wrapped key before the invite",
        );

        storage.fail_membership_entry_append_on_call(1);
        let result = create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await;
        assert!(
            result.is_err(),
            "the injected entry-upload failure must surface"
        );

        assert!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&invitee))
                .await
                .is_err(),
            "an invitee with no prior slot has the one just written deleted on rollback",
        );

        // The access this invitation created is revoked: the invitee is not a
        // member, so no dangling cloud grant is left behind.
        assert_eq!(
            cloud.last_revoke(),
            Some(CloudAccessState::Absent {
                member_pubkey: pubkey_hex(&invitee),
                provider_account_email: None,
            }),
            "a failed invite of a non-member must revoke the access it granted",
        );
    }

    /// A current member can have an absent wrapped-key slot — an interrupted earlier
    /// invite could leave it so. Re-inviting them writes a new slot; if the entry
    /// upload then fails, the rollback dispatches on chain membership, not slot
    /// presence: it deletes the wrap it just wrote (returning the slot to absent,
    /// which refresh re-wraps) and must not revoke the member's cloud access.
    #[tokio::test]
    async fn member_with_absent_slot_entry_failure_keeps_access() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [35u8; 32];
        let new_key: [u8; 32] = [36u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &member,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;

        // Drop the member's wrapped-key slot, modeling the absent-slot state an
        // interrupted earlier invite could have left; the member stays current in
        // the committed chain.
        storage
            .delete_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member))
            .await
            .unwrap();
        assert!(chain
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&member)));

        storage.fail_membership_entry_append_on_call(1);
        let result = create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &new_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await;
        assert!(
            result.is_err(),
            "the injected entry-upload failure must surface"
        );

        // The slot returns to absent — the wrap this invite wrote is deleted.
        assert!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member))
                .await
                .is_err(),
            "the wrap written by the failed invite is deleted, returning the slot to absent",
        );

        // The member kept their cloud access, dispatched on their chain membership.
        assert!(
            cloud.last_revoke().is_none(),
            "a current member with an absent slot must not have their access revoked",
        );
    }

    /// A wrapped-key slot present for someone who is not a member is anomalous — it
    /// belongs to no authorized member. On rollback of a failed invite of such a
    /// non-member, the slot is deleted rather than restored (restoring would rewrite
    /// an unauthorized object) and their access is revoked.
    #[tokio::test]
    async fn non_member_with_present_slot_entry_failure_deletes_not_restores() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let key: [u8; 32] = [37u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        // Seed a stray slot for a non-member (anomalous leftover state).
        storage
            .put_wrapped_key(
                &pubkey_hex(&owner),
                &pubkey_hex(&invitee),
                b"stray-slot".to_vec(),
            )
            .await
            .unwrap();
        assert!(!chain
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&invitee)));

        storage.fail_membership_entry_append_on_call(1);
        let result = create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000002000-0000-dev1",
        )
        .await;
        assert!(
            result.is_err(),
            "the injected entry-upload failure must surface"
        );

        // The stray slot is deleted, not restored: an unauthorized slot is never
        // rewritten.
        assert!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&invitee))
                .await
                .is_err(),
            "a non-member's slot is deleted on rollback, never restored",
        );
        assert_eq!(
            cloud.last_revoke(),
            Some(CloudAccessState::Absent {
                member_pubkey: pubkey_hex(&invitee),
                provider_account_email: None,
            }),
            "a failed invite of a non-member must revoke the access it granted",
        );
    }

    /// The wrapped-key upload itself can fail on a re-invite. The write never
    /// landed, so the member keeps their existing key; and since they held their
    /// cloud access before this invitation, the rollback must not revoke it.
    #[tokio::test]
    async fn reinvite_wrapped_key_put_failure_keeps_access_and_key() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [33u8; 32];
        let new_key: [u8; 32] = [34u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &member,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;
        let prior_wrapped = storage
            .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member))
            .await
            .unwrap();

        // Re-invite the same member, but the wrapped-key upload fails outright.
        storage.fail_wrapped_key_put_on_call(1);
        let result = create_invitation(
            &storage,
            &cloud,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &new_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await;
        assert!(
            result.is_err(),
            "the injected wrapped-key upload failure must surface"
        );

        // The failed write left the member's existing key in place.
        assert_eq!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&member))
                .await
                .unwrap(),
            prior_wrapped,
            "a failed overwrite leaves the member's prior wrapped key untouched",
        );

        // The member kept their cloud access.
        assert!(
            cloud.last_revoke().is_none(),
            "a failed re-invite must not revoke a current member's access",
        );
    }

    /// When restoring a re-invited member's prior wrapped key itself fails, the slot
    /// is left holding this invitation's key. The invite must surface that loudly,
    /// naming the slot, so the whole (idempotent) invitation is retried.
    #[tokio::test]
    async fn reinvite_restore_failure_surfaces_loud_naming_slot() {
        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [38u8; 32];
        let new_key: [u8; 32] = [39u8; 32];

        let storage = MockSyncStorage::with_keypair(owner.clone());
        let mut chain = bootstrap_chain(storage.store_protocol_root().founder.clone());

        invite_member_for_test(
            &storage,
            &mut chain,
            &owner,
            &member,
            &old_key,
            "0000000002000-0000-dev1",
        )
        .await;

        // The overwrite (wrapped-key put #1) lands; the entry upload fails; the
        // restore (wrapped-key put #2) also fails.
        storage.fail_wrapped_key_put_on_call(2);
        storage.fail_membership_entry_append_on_call(1);
        let result = create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            &owner,
            &pubkey_hex(&member),
            None,
            MemberRole::Member,
            &new_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await;

        match result {
            Err(InviteError::Rollback {
                operation,
                rollback,
                ..
            }) => {
                assert_eq!(operation, "upload membership entry");
                assert!(
                    rollback.contains(&pubkey_hex(&member)),
                    "the rollback failure must name the overwritten slot, got {rollback:?}",
                );
            }
            other => panic!("expected a loud Rollback error naming the slot, got {other:?}"),
        }
    }
}
