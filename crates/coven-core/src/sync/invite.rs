use std::sync::Arc;

use crate::config::HomeStorage;
use crate::encryption::{self, EncryptionService};
use crate::keys::{self, KeyError, UserKeypair};
/// Invitation and revocation flow for shared library membership.
///
/// `create_invitation()` is called by the library owner to invite a new member.
/// `unwrap_library_keyring()` is called by the invitee to join and unwrap the library key.
/// `revoke_member()` is called by the library owner to remove a member and rotate the key.
use crate::storage::cloud::{
    CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    RevokeOutcome,
};

use super::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use super::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipChain, MembershipCoord,
    MembershipEntry, MembershipError,
};
use super::membership_ops::{load_anchored_chain, publish_membership_head};
use super::signed_control::WrappedLibraryKey;
use super::storage::{StorageError, SyncStorage};

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
    #[error("wrapped library key activation is not visible: {activation:?}")]
    InactiveWrappedKey { activation: MembershipCoord },
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
    #[error("Cannot revoke the last owner of a library")]
    LastOwner,
}

/// The next seq for `author`'s membership entries: one past the tip its own
/// committed head certifies. Computing it from the committed prefix (not the object
/// listing) is what lets a retry after a failed head publish reuse the same seq —
/// the uncommitted object from the prior attempt is overwritten rather than skipped
/// over, so no orphan is left dangling above the head.
fn next_membership_seq(chain: &MembershipChain, author_pubkey_hex: &str) -> u64 {
    chain
        .author_tip(author_pubkey_hex)
        .map_or(1, |(seq, _)| seq + 1)
}

/// Refuse to write over `author`'s committed prefix. The intended `seq` must sit
/// past the head already in storage; if another device (or another of this owner's
/// devices) advanced the head first, the committed entry at that seq must not be
/// clobbered, so fail loud and let the caller retry on top of the head it now
/// observes.
///
/// This is a read-then-write guard, not a conditional put — two devices holding the
/// same restored keypair that read the head at the same instant can both pass and
/// race the entry object. The design accepts that residual window (a shared keypair
/// is one identity, not coordinated writers); the losing device's next load fails
/// the head's tip-hash check and it republishes on the observed head.
async fn guard_extends_committed_head(
    storage: &dyn SyncStorage,
    author: &str,
    seq: u64,
) -> Result<(), InviteError> {
    if let Some(committed) = super::membership_ops::committed_head_seq(storage, author)
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

/// Seal the library key to one member and wrap it in an owner-signed
/// [`WrappedLibraryKey`], serialized to the bytes stored at
/// `keys/{owner_pubkey}/{recipient_pubkey}` (the owner writes into its own
/// prefix). The signature binds `(library_id, recipient_pubkey, author_pubkey,
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
    library_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
) -> Result<Vec<u8>, InviteError> {
    signed_wrapped_key_with_activation(
        library_id,
        recipient_ed25519_pubkey,
        recipient_x25519_pk,
        encryption,
        owner_keypair,
        None,
    )
}

fn signed_wrapped_key_with_activation(
    library_id: &str,
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
    let wrapped = WrappedLibraryKey::signed(
        library_id,
        recipient_ed25519_pubkey,
        activation,
        sealed,
        owner_keypair,
    );
    serde_json::to_vec(&wrapped)
        .map_err(|e| InviteError::Crypto(format!("serialize wrapped key: {e}")))
}

#[cfg(test)]
pub(crate) fn signed_wrapped_key_for_test(
    library_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption_key: &[u8; 32],
    owner_keypair: &UserKeypair,
) -> Vec<u8> {
    signed_wrapped_keyring_for_test(
        library_id,
        recipient_ed25519_pubkey,
        recipient_x25519_pk,
        &EncryptionService::from_key(*encryption_key),
        owner_keypair,
        None,
    )
}

#[cfg(test)]
pub(crate) fn signed_wrapped_keyring_for_test(
    library_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
    activation: Option<MembershipCoord>,
) -> Vec<u8> {
    signed_wrapped_key_with_activation(
        library_id,
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
    coord: &MembershipCoord,
    entry: &MembershipEntry,
) -> Result<(), InviteError> {
    let entry_bytes =
        serde_json::to_vec(entry).map_err(|e| InviteError::Crypto(format!("serialize: {e}")))?;
    storage
        .put_membership_entry(&coord.author_pubkey, coord.seq, entry_bytes)
        .await?;

    Ok(())
}

/// Create an invitation for a new member.
///
/// This grants access on the cloud home, wraps the library encryption key
/// to the invitee's X25519 public key, creates and signs a membership entry
/// (Add), validates it against the local chain, and uploads both to the storage.
/// Returns the JoinInfo so the caller can share connection details with the invitee.
pub async fn create_invitation(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption_key: &[u8; 32],
    library_id: &str,
    timestamp: &str,
) -> Result<CloudHomeJoinInfo, InviteError> {
    let encryption = EncryptionService::from_key(*encryption_key);
    create_invitation_with_encryption(
        storage,
        cloud_home,
        chain,
        owner_keypair,
        invitee_ed25519_pubkey,
        invitee_email,
        role,
        &encryption,
        library_id,
        timestamp,
    )
    .await
}

pub async fn create_invitation_with_encryption(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    chain: &mut MembershipChain,
    owner_keypair: &UserKeypair,
    invitee_ed25519_pubkey: &str,
    invitee_email: Option<&str>,
    role: MemberRole,
    encryption: &EncryptionService,
    library_id: &str,
    timestamp: &str,
) -> Result<CloudHomeJoinInfo, InviteError> {
    // Convert Ed25519 -> X25519 for sealed box encryption.
    let invitee_x25519_pk = ed25519_hex_to_x25519(invitee_ed25519_pubkey)?;

    // Seal the library key to the invitee and sign the binding so the joiner can
    // authenticate it on adoption. The joiner verifies this signature against the
    // founder the invite pins, so for the invitee to actually adopt the key
    // `owner_keypair` must be the founder's for a fresh joiner; existing members
    // authorize rotated keys against the current Owner set during refresh.
    let wrapped_key = signed_wrapped_key(
        library_id,
        invitee_ed25519_pubkey,
        &invitee_x25519_pk,
        encryption,
        owner_keypair,
    )?;

    // Create and sign a membership entry.
    let author_pubkey_hex = hex::encode(owner_keypair.public_key());
    let entry_coord = MembershipCoord {
        author_pubkey: author_pubkey_hex.clone(),
        seq: next_membership_seq(chain, &author_pubkey_hex),
    };
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: invitee_ed25519_pubkey.to_string(),
        provider_account_email: invitee_email.map(str::to_string),
        prev_hash: chain.latest_author_hash(&author_pubkey_hex),
        role,
        timestamp: timestamp.to_string(),
        author_pubkey: String::new(),
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner_keypair);

    // Validate against the local chain before any provider or storage mutation.
    let mut validated_chain = chain.clone();
    validated_chain.add_entry_at(entry_coord.clone(), entry.clone())?;

    let grant = CloudAccessGrant {
        member_pubkey: invitee_ed25519_pubkey.to_string(),
        provider_account_email: invitee_email.map(str::to_string),
    };
    let revoke = CloudAccessRevoke {
        member_pubkey: grant.member_pubkey.clone(),
        provider_account_email: grant.provider_account_email.clone(),
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

    let join_info = cloud_home.grant_access(grant).await?;

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
            if let Err(rollback) = cloud_home.revoke_access(revoke).await {
                return Err(InviteError::Rollback {
                    operation: "upload wrapped key",
                    original: original.to_string(),
                    rollback: rollback.to_string(),
                });
            }
        }
        return Err(original.into());
    }

    // The head governs commitment, so re-check right before writing the entry: if
    // another owner device advanced this author's head to our seq while we were
    // preparing, its committed entry must not be overwritten. Fail loud; the retry
    // recomputes the seq from the now-newer head.
    guard_extends_committed_head(storage, &author_pubkey_hex, entry_coord.seq).await?;

    if let Err(original) = upload_membership_entry(storage, &entry_coord, &entry).await {
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
                        .put_wrapped_key(&author_pubkey_hex, invitee_ed25519_pubkey, prior)
                        .await
                    {
                        rollback_errors.push(format!(
                            "restore wrapped key for {invitee_ed25519_pubkey}: {restore}"
                        ));
                    }
                }
                // The member had no slot before (an interrupted earlier invite could
                // leave it absent); delete the wrap just written so the slot returns
                // to absent, which the member's next refresh re-wraps.
                None => {
                    if let Err(rollback) = storage
                        .delete_wrapped_key(&author_pubkey_hex, invitee_ed25519_pubkey)
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
                .delete_wrapped_key(&author_pubkey_hex, invitee_ed25519_pubkey)
                .await
            {
                rollback_errors.push(rollback.to_string());
            }
            if let Err(rollback) = cloud_home.revoke_access(revoke).await {
                rollback_errors.push(rollback.to_string());
            }
        }
        if !rollback_errors.is_empty() {
            return Err(InviteError::Rollback {
                operation: "upload membership entry",
                original: original.to_string(),
                rollback: rollback_errors.join("; "),
            });
        }
        return Err(original);
    }
    publish_membership_head(storage, &validated_chain, owner_keypair)
        .await
        .map_err(|e| InviteError::Crypto(format!("publish membership head: {e}")))?;

    *chain = validated_chain;

    Ok(join_info)
}

/// Join a shared library: authenticate and unwrap its encryption key from the
/// invitee's own wrapped-key slot.
///
/// `founder` is the owner the invite pins (the chain founder). The joiner holds
/// no membership chain yet — and every chain entry is sealed under the library
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
pub async fn unwrap_library_keyring(
    cloud_home: Arc<dyn CloudHome>,
    keypair: &UserKeypair,
    library_id: &str,
    founder: &str,
) -> Result<EncryptionService, InviteError> {
    // The candidate keyring: enough to read the sealed membership chain, but not
    // yet trusted to be the real, owner-authorized library key.
    let candidate = decrypt_wrapped_library_key_unverified(cloud_home.as_ref(), keypair).await?;

    // Read + anchor the chain to the pinned founder using the candidate key. A
    // candidate that is not the real library key cannot decrypt the sealed chain,
    // and a chain not founded by `founder` is a takeover — both fail closed here,
    // before the key is trusted. The joiner reads only, so `watermark_db` is None.
    let storage = CloudSyncStorage::new(
        cloud_home.clone(),
        CloudCipher::Encrypted(candidate),
        BlobPathScheme::for_storage(HomeStorage::Opaque),
        library_id.to_string(),
        keypair.clone(),
    );
    let entry_keys = storage.list_membership_entries().await?;
    let chain = load_anchored_chain(&storage, &entry_keys, Some(founder), None)
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
    let visible = membership_coords(&entry_keys);
    unwrap_library_keyring_for_owners_with_activation(
        cloud_home.as_ref(),
        keypair,
        library_id,
        owners.iter().map(String::as_str),
        Some(&visible),
    )
    .await
}

/// Fetch and parse the wrapped-key object `owner_hex` sealed for `recipient_hex`,
/// off the cloud home. The `.enc` suffix is hardcoded because reading a wrapped
/// library key is an encrypted-home-only path — wrapping a key is meaningful only
/// for a shared (encrypted) home — so `CloudSyncStorage::put_wrapped_key` always
/// wrote the slot at `keys/{owner}/{recipient}.enc`. Read straight off the home,
/// not through `CloudSyncStorage`, which the joiner has not built yet.
async fn fetch_wrapped_key(
    cloud_home: &dyn CloudHome,
    owner_hex: &str,
    recipient_hex: &str,
) -> Result<WrappedLibraryKey, InviteError> {
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

/// Open a sealed box carrying a library keyring to `keypair` and reconstruct the
/// [`EncryptionService`]. `sealed` is the raw sealed-box bytes — the unverified
/// candidate takes them straight off the wrapped key, the authenticated path
/// takes them from [`WrappedLibraryKey::verify_and_unwrap`].
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
/// the membership chain, every entry of which is sealed under the library key.
/// Never adopt the returned keyring without the founder-anchored Owner-set check
/// [`unwrap_library_keyring`] runs; it exists solely to bootstrap that check.
async fn decrypt_wrapped_library_key_unverified(
    cloud_home: &dyn CloudHome,
    keypair: &UserKeypair,
) -> Result<EncryptionService, InviteError> {
    let recipient_hex = hex::encode(keypair.public_key());
    let owners = wrap_owners_for_recipient(cloud_home, &recipient_hex).await?;

    // The candidate is untrusted — it only has to decrypt the sealed membership
    // chain, whose authenticated Owner set the caller then re-derives the real key
    // against. A rotation before the join may have re-wrapped this slot under a
    // non-founder owner, leaving the founder's original invite wrap a stale, older
    // generation, so take the highest-generation wrap that opens: it carries the
    // most key generations and so reads the furthest into the sealed chain.
    let mut best: Option<EncryptionService> = None;
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
        if best
            .as_ref()
            .is_none_or(|best| candidate.current_generation() > best.current_generation())
        {
            best = Some(candidate);
        }
    }
    best.ok_or_else(|| {
        InviteError::CloudHome(CloudHomeError::NotFound(format!(
            "keys/*/{recipient_hex}.enc"
        )))
    })
}

pub(crate) async fn unwrap_library_keyring_for_owners_with_activation<'a>(
    cloud_home: &dyn CloudHome,
    keypair: &UserKeypair,
    library_id: &str,
    expected_owners: impl IntoIterator<Item = &'a str>,
    visible_membership_entries: Option<&[MembershipCoord]>,
) -> Result<EncryptionService, InviteError> {
    let recipient_hex = hex::encode(keypair.public_key());

    // Scan each current owner's prefix for this recipient's wrap and adopt the
    // highest-generation one an owner's signature authenticates. An owner writes
    // only into its own prefix, so `keys/{owner}/{recipient}` is authenticated
    // against THAT owner; a wrap another owner rotated more recently supersedes an
    // older one by its keyring generation.
    let mut best: Option<EncryptionService> = None;
    // Remember why non-adoptable wraps were rejected, so "no wrap adopted" reports
    // the real reason rather than a bare not-found (which the caller treats as
    // "this device has no wrapped key").
    let mut saw_inactive: Option<MembershipCoord> = None;
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
            let visible = visible_membership_entries
                .is_some_and(|entries| entries.iter().any(|entry| entry == activation));
            if !visible {
                saw_inactive = Some(activation.clone());
                continue;
            }
        }

        // Authenticate against the owner whose prefix this wrap lives under. Any
        // bucket writer can drop a forged or relocated object into an owner's
        // prefix while no ACL enforces the layout; it fails to verify against that
        // owner and is skipped, so it can neither be adopted nor block a valid wrap
        // under another owner.
        let sealed =
            match wrapped.verify_and_unwrap(library_id, &recipient_hex, std::iter::once(owner)) {
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
        if best
            .as_ref()
            .is_none_or(|best| keyring.current_generation() > best.current_generation())
        {
            best = Some(keyring);
        }
    }

    if let Some(keyring) = best {
        return Ok(keyring);
    }
    if let Some(activation) = saw_inactive {
        return Err(InviteError::InactiveWrappedKey { activation });
    }
    if saw_unauthentic {
        return Err(InviteError::Crypto(format!(
            "no authentic wrapped library key for {recipient_hex} under any current owner"
        )));
    }
    Err(InviteError::CloudHome(CloudHomeError::NotFound(format!(
        "keys/*/{recipient_hex}.enc (no wrap under any of {owners_tried} owner prefixes)"
    ))))
}

/// Revoke a member from the library. This:
/// 1. Revokes access on the cloud home
/// 2. Re-wraps a new library key to all remaining members
/// 3. Deletes the revoked member's wrapped key
/// 4. Publishes the signed Remove membership entry as the visible commit point
///
/// Returns the new encryption key (caller must persist it and start using it).
pub async fn revoke_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn CloudHome,
    chain: &mut MembershipChain,
    entry_keys: Vec<(String, u64)>,
    owner_keypair: &UserKeypair,
    revokee_pubkey: &str,
    library_id: &str,
    timestamp: &str,
    current_encryption: &EncryptionService,
) -> Result<EncryptionService, InviteError> {
    let members = chain.current_members();
    let revokee_is_current = members.iter().any(|(pk, _)| pk == revokee_pubkey);
    let revokee_was_removed = chain.entries().iter().any(|entry| {
        entry.action == MembershipAction::Remove && entry.user_pubkey == revokee_pubkey
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

    // Withdraw the removed member's storage credential where the provider
    // supports it. Removal does not depend on it: the chain Remove and key
    // rotation below are the protection, so a backend that cannot revoke reports
    // Unsupported and removal proceeds.
    let provider_account_email = chain
        .current_member_provider_email(revokee_pubkey)
        .map(str::to_string);
    match cloud_home
        .revoke_access(CloudAccessRevoke {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email,
        })
        .await?
    {
        RevokeOutcome::Revoked => {}
        RevokeOutcome::Unsupported => {
            tracing::info!(
                member = %revokee_pubkey,
                "cloud provider offers no per-member credential revocation; removal proceeds — chain revocation and library key rotation are the protection",
            );
        }
    }

    // This owner deletes only its own wrap for the revokee, from its own prefix.
    // Any wrap another owner sealed for the revokee is a pre-rotation generation —
    // it wraps a key the revokee already held — so leaving it is harmless; that
    // owner reclaims it when it next rotates.
    let author_pubkey_hex = hex::encode(owner_keypair.public_key());

    if !revokee_is_current {
        let visible_coords = membership_coords(&entry_keys);
        let keyring = unwrap_library_keyring_for_owners_with_activation(
            cloud_home,
            owner_keypair,
            library_id,
            current_owners.iter().map(String::as_str),
            Some(&visible_coords),
        )
        .await?;
        storage
            .delete_wrapped_key(&author_pubkey_hex, revokee_pubkey)
            .await?;
        return Ok(keyring);
    }

    let remove_coord = MembershipCoord {
        author_pubkey: author_pubkey_hex.clone(),
        seq: next_membership_seq(chain, &author_pubkey_hex),
    };
    // Create and sign a Remove entry.
    let mut entry = MembershipEntry {
        action: MembershipAction::Remove,
        user_pubkey: revokee_pubkey.to_string(),
        provider_account_email: None,
        prev_hash: chain.latest_author_hash(&author_pubkey_hex),
        role: MemberRole::Member, // role field is not meaningful for Remove, but required
        timestamp: timestamp.to_string(),
        author_pubkey: String::new(),
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner_keypair);

    // Validate against a clone before any storage writes. The caller's chain
    // advances only after the Remove entry is uploaded as the commit point.
    let mut validated_chain = chain.clone();
    validated_chain.add_entry_at(remove_coord.clone(), entry.clone())?;

    // Generate a new random encryption key.
    let new_key = encryption::generate_random_key();
    let new_generation = current_encryption.current_generation() + 1;
    let new_keyring = current_encryption
        .with_appended_generation(new_generation, new_key)
        .map_err(|e| InviteError::Crypto(format!("append key generation: {e}")))?;

    // Re-wrap the new key to all remaining members, each signed so a joiner that
    // later adopts it can authenticate the signer against the current Owner set.
    let remaining_members = validated_chain.current_members();
    let remaining_member_keys = remaining_members
        .iter()
        .map(|(member_pubkey, _)| {
            ed25519_hex_to_x25519(member_pubkey).map(|x25519_pk| (member_pubkey, x25519_pk))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (member_pubkey, x25519_pk) in remaining_member_keys {
        let wrapped = signed_wrapped_key_with_activation(
            library_id,
            member_pubkey,
            &x25519_pk,
            &new_keyring,
            owner_keypair,
            Some(remove_coord.clone()),
        )?;
        storage
            .put_wrapped_key(&author_pubkey_hex, member_pubkey, wrapped)
            .await?;
    }

    // Delete this owner's wrap for the revoked member, from its own prefix.
    storage
        .delete_wrapped_key(&author_pubkey_hex, revokee_pubkey)
        .await?;

    // Don't overwrite a committed Remove another owner device already published at
    // this seq; fail loud and let the retry rebuild on top of the observed head.
    guard_extends_committed_head(storage, &author_pubkey_hex, remove_coord.seq).await?;
    upload_membership_entry(storage, &remove_coord, &entry).await?;
    publish_membership_head(storage, &validated_chain, owner_keypair)
        .await
        .map_err(|e| InviteError::Crypto(format!("publish membership head: {e}")))?;
    *chain = validated_chain;

    Ok(new_keyring)
}

fn membership_coords(entry_keys: &[(String, u64)]) -> Vec<MembershipCoord> {
    entry_keys
        .iter()
        .map(|(author_pubkey, seq)| MembershipCoord {
            author_pubkey: author_pubkey.clone(),
            seq: *seq,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HomeStorage;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::MemberRole;
    use crate::sync::membership_ops::download_entries;
    use crate::sync::test_helpers::{bootstrap_chain, pubkey_hex, MockSyncStorage};
    use async_trait::async_trait;
    use std::sync::Arc;

    /// Minimal CloudHome mock that returns a dummy S3 JoinInfo.
    struct MockCloudHome;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
        async fn grant_access(
            &self,
            _grant: CloudAccessGrant,
        ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
            Ok(CloudHomeJoinInfo::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key: "test-access-key".to_string(),
                secret_key: "test-secret-key".to_string(),
                key_prefix: None,
            })
        }
        async fn revoke_access(
            &self,
            _revoke: CloudAccessRevoke,
        ) -> Result<RevokeOutcome, CloudHomeError> {
            Ok(RevokeOutcome::Unsupported)
        }
    }

    /// CloudHome mock that records grant/revoke identities.
    struct RecordingCloudHome {
        grants: std::sync::Mutex<Vec<CloudAccessGrant>>,
        revokes: std::sync::Mutex<Vec<CloudAccessRevoke>>,
    }

    impl RecordingCloudHome {
        fn new() -> Self {
            Self {
                grants: std::sync::Mutex::new(Vec::new()),
                revokes: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn last_grant(&self) -> Option<CloudAccessGrant> {
            self.grants.lock().unwrap().last().cloned()
        }
        fn last_revoke(&self) -> Option<CloudAccessRevoke> {
            self.revokes.lock().unwrap().last().cloned()
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
        async fn grant_access(
            &self,
            grant: CloudAccessGrant,
        ) -> Result<CloudHomeJoinInfo, CloudHomeError> {
            self.grants.lock().unwrap().push(grant);
            Ok(CloudHomeJoinInfo::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key: "test-access-key".to_string(),
                secret_key: "test-secret-key".to_string(),
                key_prefix: None,
            })
        }
        async fn revoke_access(
            &self,
            revoke: CloudAccessRevoke,
        ) -> Result<RevokeOutcome, CloudHomeError> {
            self.revokes.lock().unwrap().push(revoke);
            Ok(RevokeOutcome::Unsupported)
        }
    }

    fn gen_keypair() -> UserKeypair {
        UserKeypair::generate()
    }

    /// The library id every invite test wraps keys under. The wrapped-key
    /// signature binds it, so the same id must be passed at unwrap time.
    const LIB_ID: &str = "lib-test";

    /// Verify and unwrap a wrapped key against a single expected Owner, the way an
    /// existing member does when adopting a rotated key. The invite/revoke
    /// mechanics tests use this to assert a wrapped key landed and decrypts under
    /// the right signer, without standing up the sealed, founder-anchored chain
    /// the full joiner path ([`unwrap_library_keyring`]) reads — that path has its
    /// own `InMemoryCloudHome` tests below.
    async fn unwrap_bytes_for_owner(
        cloud_home: &dyn CloudHome,
        keypair: &UserKeypair,
        owner: &str,
    ) -> Result<[u8; 32], InviteError> {
        unwrap_library_keyring_for_owners_with_activation(
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
        let entry_keys = storage.list_membership_entries().await.unwrap();
        download_entries(storage, &entry_keys)
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
    async fn create_and_unwrap_library_key() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let encryption_key: [u8; 32] = [42u8; 32];

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
        let unwrapped = unwrap_library_keyring_for_owners_with_activation(
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
        let keyring = unwrap_library_keyring_for_owners_with_activation(
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
        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);
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
            Some(CloudAccessGrant {
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
        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);
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
            Some(CloudAccessGrant {
                member_pubkey: invitee_pubkey,
                provider_account_email: None,
            })
        );
    }

    /// A joiner adopts only a library key the owner it pins signed; a key signed
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
    async fn unwrap_library_key_wrong_key_fails() {
        let owner = gen_keypair();
        let invitee = gen_keypair();
        let wrong_keypair = gen_keypair();
        let encryption_key: [u8; 32] = [7u8; 32];

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);
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
        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
        let entries = storage.list_membership_entries().await.unwrap();
        let owner_entries: Vec<_> = entries
            .iter()
            .filter(|(author, _)| author == &pubkey_hex(&owner))
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
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
        let visible_entries = membership_coords(&storage.list_membership_entries().await.unwrap());
        let owner_pk = pubkey_hex(&owner);
        let owner_unwrapped = unwrap_library_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &owner,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&visible_entries),
        )
        .await
        .unwrap()
        .key_bytes();
        assert_eq!(owner_unwrapped, new_key.key_bytes());

        // The Remove entry was uploaded to the storage.
        let entries = storage.list_membership_entries().await.unwrap();
        let owner_entries: Vec<_> = entries
            .iter()
            .filter(|(author, _)| author == &pubkey_hex(&owner))
            .collect();
        // 1 for invite + 1 for revoke = 2
        assert_eq!(owner_entries.len(), 2);
    }

    #[tokio::test]
    async fn revoke_member_with_multiple_remaining() {
        let owner = gen_keypair();
        let member1 = gen_keypair();
        let member2 = gen_keypair();
        let old_key: [u8; 32] = [10u8; 32];

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
            &owner,
            &pubkey_hex(&member1),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        // Both remaining members (owner + member2) can unwrap the new key.
        let visible_entries = membership_coords(&storage.list_membership_entries().await.unwrap());
        let owner_pk = pubkey_hex(&owner);
        let owner_key = unwrap_library_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &owner,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&visible_entries),
        )
        .await
        .unwrap()
        .key_bytes();
        assert_eq!(owner_key, new_key.key_bytes());

        let member2_key = unwrap_library_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &member2,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&visible_entries),
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);
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
            seq: 3,
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

        let visible_entries = membership_coords(&storage.list_membership_entries().await.unwrap());
        let result = unwrap_library_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &member,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&visible_entries),
        )
        .await;

        assert!(matches!(
            result,
            Err(InviteError::InactiveWrappedKey { activation: seen }) if seen == activation
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
    }

    /// A member invited but not yet joined is a current member, so revoking a
    /// third member re-wraps the pending invitee's slot with an activation naming
    /// the Remove entry. When the invitee finally joins, `unwrap_library_keyring`
    /// lists the now-visible membership entries and the activation resolves, so
    /// the join succeeds and the invitee adopts the post-rotation key generation.
    #[tokio::test]
    async fn pending_invitee_joins_after_a_third_member_is_revoked() {
        use crate::sync::membership_ops::write_founder_entry;

        let owner = gen_keypair();
        let pending = gen_keypair();
        let third = gen_keypair();
        let old_key: [u8; 32] = [20u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), old_key, &owner);
        // The founder entry lives on the home in production (written at library
        // creation); the joiner reads and anchors the chain to it, so found there.
        write_founder_entry(&storage, &owner, "0000000001000-0000-dev1")
            .await
            .unwrap();
        let mut chain = bootstrap_chain(&owner);

        create_invitation(
            &storage,
            &MockCloudHome,
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
        create_invitation(
            &storage,
            &MockCloudHome,
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
        let new_keyring = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            storage.list_membership_entries().await.unwrap(),
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
        let joined = unwrap_library_keyring(home.clone(), &pending, LIB_ID, &pubkey_hex(&owner))
            .await
            .unwrap();

        assert_eq!(
            joined.key_bytes(),
            new_keyring.key_bytes(),
            "the joiner adopts the post-rotation library key"
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
        use crate::sync::membership_ops::write_founder_entry;

        let owner = gen_keypair();
        let member = gen_keypair();
        let old_key: [u8; 32] = [21u8; 32];
        let new_key: [u8; 32] = [22u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), old_key, &owner);
        write_founder_entry(&storage, &owner, "0000000001000-0000-dev1")
            .await
            .unwrap();
        let mut chain = bootstrap_chain(&owner);
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

        // Overwrite the member's slot with a key whose activation names a Remove
        // entry that was never published — nothing sits at membership/{owner}/99.
        let owner_pk = pubkey_hex(&owner);
        let activation = MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: 99,
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

        let result = unwrap_library_keyring(home.clone(), &member, LIB_ID, &owner_pk).await;
        assert!(
            matches!(
                &result,
                Err(InviteError::InactiveWrappedKey { activation: seen }) if *seen == activation
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
        use crate::sync::membership_ops::write_founder_entry;

        let founder = gen_keypair();
        let second_owner = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [7u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let founder_storage = opaque_storage(home.clone(), key, &founder);
        write_founder_entry(&founder_storage, &founder, "0000000001000-0000-dev1")
            .await
            .unwrap();
        let mut chain = bootstrap_chain(&founder);

        // The founder promotes B to Owner.
        create_invitation(
            &founder_storage,
            &MockCloudHome,
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
        create_invitation(
            &second_owner_storage,
            &MockCloudHome,
            &mut chain,
            &second_owner,
            &pubkey_hex(&joiner),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // C joins, pinning the founder. B is still a current Owner, so B's wrapped
        // key verifies against the current Owner set ({founder, B}) and the join
        // succeeds.
        let joined = unwrap_library_keyring(home.clone(), &joiner, LIB_ID, &pubkey_hex(&founder))
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
        use crate::sync::membership::MembershipAction;
        use crate::sync::membership_ops::write_founder_entry;
        use crate::sync::test_helpers::make_linked_entry;

        let founder = gen_keypair();
        let second_owner = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [10u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let founder_storage = opaque_storage(home.clone(), key, &founder);
        write_founder_entry(&founder_storage, &founder, "0000000001000-0000-dev1")
            .await
            .unwrap();
        let mut chain = bootstrap_chain(&founder);

        // The founder promotes B to Owner; B invites C, signing C's slot.
        create_invitation(
            &founder_storage,
            &MockCloudHome,
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
        create_invitation(
            &second_owner_storage,
            &MockCloudHome,
            &mut chain,
            &second_owner,
            &pubkey_hex(&joiner),
            None,
            MemberRole::Member,
            &key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // The founder removes B before C joins. C's slot still holds B's signature
        // (the removal here does not re-wrap it).
        let founder_pk = pubkey_hex(&founder);
        let remove_b = make_linked_entry(
            &chain,
            &founder,
            MembershipAction::Remove,
            &second_owner,
            MemberRole::Member,
            "0000000004000-0000-dev1",
        );
        let remove_seq = next_membership_seq(&chain, &founder_pk);
        chain
            .add_entry_at(
                MembershipCoord {
                    author_pubkey: founder_pk.clone(),
                    seq: remove_seq,
                },
                remove_b.clone(),
            )
            .unwrap();
        founder_storage
            .put_membership_entry(
                &founder_pk,
                remove_seq,
                serde_json::to_vec(&remove_b).unwrap(),
            )
            .await
            .unwrap();
        publish_membership_head(&founder_storage, &chain, &founder)
            .await
            .unwrap();

        // C joins: B is no longer a current Owner, so its prefix is not scanned and
        // C's wrap (only ever under B) is unreachable — no current owner vouches for
        // C, so the join finds no adoptable key.
        let result = unwrap_library_keyring(home.clone(), &joiner, LIB_ID, &founder_pk).await;
        assert!(
            matches!(
                result,
                Err(InviteError::CloudHome(CloudHomeError::NotFound(_)))
            ),
            "an invite from a since-removed Owner must not be joinable, got {result:?}",
        );
    }

    /// A wrapped key signed by a member who is not an Owner is refused: the member
    /// holds the library key, so it can seal the real key to the joiner, but it is
    /// not in the Owner set derived from the founder-anchored chain, so the joiner
    /// will not adopt what it wrapped.
    #[tokio::test]
    async fn join_refuses_wrapped_key_signed_by_non_owner_member() {
        use crate::sync::membership_ops::write_founder_entry;

        let founder = gen_keypair();
        let member = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [8u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), key, &founder);
        write_founder_entry(&storage, &founder, "0000000001000-0000-dev1")
            .await
            .unwrap();
        let mut chain = bootstrap_chain(&founder);

        // The founder adds a plain Member and the joiner.
        create_invitation(
            &storage,
            &MockCloudHome,
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
        create_invitation(
            &storage,
            &MockCloudHome,
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

        // The member (not an Owner) seals the real library key to the joiner and
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

        let result =
            unwrap_library_keyring(home.clone(), &joiner, LIB_ID, &pubkey_hex(&founder)).await;
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
        use crate::sync::membership::founder_entry;
        use crate::sync::membership_ops::write_founder_entry;

        let founder = gen_keypair();
        let rogue = gen_keypair();
        let joiner = gen_keypair();
        let key: [u8; 32] = [9u8; 32];

        let home = Arc::new(InMemoryCloudHome::new());
        let storage = opaque_storage(home.clone(), key, &founder);
        write_founder_entry(&storage, &founder, "0000000001000-0000-dev1")
            .await
            .unwrap();
        let mut chain = bootstrap_chain(&founder);

        // The founder adds `rogue` as a plain Member (so it holds the library key)
        // and the joiner.
        create_invitation(
            &storage,
            &MockCloudHome,
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
        create_invitation(
            &storage,
            &MockCloudHome,
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
        let rogue_self_add = founder_entry(&rogue, "0000000002500-0000-dev1");
        storage
            .put_membership_entry(
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

        let result =
            unwrap_library_keyring(home.clone(), &joiner, LIB_ID, &pubkey_hex(&founder)).await;
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
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
                entry.action == MembershipAction::Remove
                    && entry.user_pubkey == pubkey_hex(&revokee)
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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

        let owner_pk = pubkey_hex(&owner);
        let coord = MembershipCoord {
            author_pubkey: owner_pk.clone(),
            seq: next_membership_seq(&chain, &owner_pk),
        };
        let mut invalid_entry = MembershipEntry {
            action: MembershipAction::Add,
            user_pubkey: off_curve_pubkey.to_string(),
            provider_account_email: None,
            prev_hash: chain.latest_author_hash(&owner_pk),
            role: MemberRole::Member,
            timestamp: "0000000004000-0000-dev1".to_string(),
            author_pubkey: String::new(),
            signature: String::new(),
        };
        sign_membership_entry(&mut invalid_entry, &owner);
        chain.add_entry_at(coord, invalid_entry).unwrap();

        let result = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            storage.list_membership_entries().await.unwrap(),
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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

        let visible_entries = membership_coords(&storage.list_membership_entries().await.unwrap());
        let owner_pk = pubkey_hex(&owner);
        for member in [&owner, &remaining_a, &remaining_b] {
            let unwrapped = unwrap_library_keyring_for_owners_with_activation(
                &storage as &dyn CloudHome,
                member,
                LIB_ID,
                std::iter::once(owner_pk.as_str()),
                Some(&visible_entries),
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
            &owner,
            &pubkey_hex(&revokee),
            LIB_ID,
            "0000000004000-0000-dev1",
            &EncryptionService::from_key(old_key),
        )
        .await
        .unwrap();

        assert_eq!(retry_key.key_bytes(), committed_key.key_bytes());

        let visible_entries = membership_coords(&storage.list_membership_entries().await.unwrap());
        let owner_pk = pubkey_hex(&owner);
        let remaining_key = unwrap_library_keyring_for_owners_with_activation(
            &storage as &dyn CloudHome,
            &remaining,
            LIB_ID,
            std::iter::once(owner_pk.as_str()),
            Some(&visible_entries),
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

        let storage = MockSyncStorage::new();
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
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
            Some(CloudAccessRevoke {
                member_pubkey,
                provider_account_email: Some("second@example.com".to_string()),
            })
        );
    }

    #[tokio::test]
    async fn revoke_non_member_fails() {
        let owner = gen_keypair();
        let outsider = gen_keypair();

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

        let result = revoke_member(
            &storage,
            &MockCloudHome,
            &mut chain,
            storage.list_membership_entries().await.unwrap(),
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
            storage.list_membership_entries().await.unwrap(),
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
        use crate::sync::membership_ops::{load_anchored_chain, write_founder_entry};

        let owner = gen_keypair();
        let invitee = gen_keypair();
        let owner_pk = pubkey_hex(&owner);
        let key: [u8; 32] = [21u8; 32];

        let storage = MockSyncStorage::new();
        write_founder_entry(&storage, &owner, "0000000001000-0000-dev1")
            .await
            .unwrap();

        let load = |visible: Vec<(String, u64)>| {
            let owner_pk = owner_pk.clone();
            let storage = &storage;
            async move {
                load_anchored_chain(storage, &visible, Some(&owner_pk), None)
                    .await
                    .unwrap()
            }
        };

        // Attempt 1: the entry uploads, then the head publish fails.
        let mut chain = load(storage.list_membership_entries().await.unwrap()).await;
        storage.fail_membership_head_put_on_call(1);
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
        let committed = load(storage.list_membership_entries().await.unwrap()).await;
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

        let loaded = load(storage.list_membership_entries().await.unwrap()).await;
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
        use crate::sync::membership_ops::{load_anchored_chain, write_founder_entry};

        let owner = gen_keypair();
        let first_invitee = gen_keypair();
        let second_invitee = gen_keypair();
        let owner_pk = pubkey_hex(&owner);
        let key: [u8; 32] = [22u8; 32];

        let storage = MockSyncStorage::new();
        write_founder_entry(&storage, &owner, "0000000001000-0000-dev1")
            .await
            .unwrap();

        let load = |visible: Vec<(String, u64)>| {
            let owner_pk = owner_pk.clone();
            let storage = &storage;
            async move {
                load_anchored_chain(storage, &visible, Some(&owner_pk), None)
                    .await
                    .unwrap()
            }
        };

        // Both devices observe the founder head; device two keeps that stale view.
        let mut device_one = load(storage.list_membership_entries().await.unwrap()).await;
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
        let after_stale = load(storage.list_membership_entries().await.unwrap()).await;
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

        let loaded = load(storage.list_membership_entries().await.unwrap()).await;
        let members = loaded.current_members();
        assert!(members
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&first_invitee)));
        assert!(members
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&second_invitee)));
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

        let storage = MockSyncStorage::new();
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(&owner);

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
        storage.fail_membership_entry_put_on_call(1);
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

        let storage = MockSyncStorage::new();
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(&owner);

        assert!(
            storage
                .get_wrapped_key(&pubkey_hex(&owner), &pubkey_hex(&invitee))
                .await
                .is_err(),
            "the invitee has no wrapped key before the invite",
        );

        storage.fail_membership_entry_put_on_call(1);
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
            Some(CloudAccessRevoke {
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

        let storage = MockSyncStorage::new();
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(&owner);

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

        storage.fail_membership_entry_put_on_call(1);
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

        let storage = MockSyncStorage::new();
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(&owner);

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

        storage.fail_membership_entry_put_on_call(1);
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
            Some(CloudAccessRevoke {
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

        let storage = MockSyncStorage::new();
        let cloud = RecordingCloudHome::new();
        let mut chain = bootstrap_chain(&owner);

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

        let storage = MockSyncStorage::new();
        let mut chain = bootstrap_chain(&owner);

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
        storage.fail_membership_entry_put_on_call(1);
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
