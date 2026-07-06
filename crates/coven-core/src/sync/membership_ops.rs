//! Membership operations: get members, invite, and revoke.
//!
//! These are the high-level orchestration functions that download the membership
//! chain from the storage, perform the operation, and upload the results.

use tracing::{debug, info};

use crate::encryption::EncryptionService;
use crate::keys::{KeyPersistence, UserKeypair};
#[cfg(test)]
use crate::storage::cloud::CloudHome;

use super::cloud_storage::CloudCipher;
use super::hlc::Hlc;
use super::membership::{
    founder_entry, MemberInfo, MemberRole, MembershipChain, MembershipChainHead, MembershipCoord,
    MembershipEntry,
};
use super::storage::{StorageError, SyncStorage};

/// `sync_state` key holding the hex Ed25519 pubkey of the library's established
/// owner — pinned at create (the creator), join (the invite's owner), or restore.
/// The membership chain is anchored to it: a chain whose founder differs is a
/// takeover attempt and is rejected (issue #95).
pub const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";

/// Read the membership chain from the sync storage and return the current members.
pub async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
) -> Result<Vec<MemberInfo>, String> {
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| format!("Failed to list membership entries: {e}"))?;

    if entry_keys.is_empty() {
        return Ok(Vec::new());
    }

    let chain = load_anchored_chain(storage, &entry_keys, None)
        .await
        .map_err(|e| e.to_string())?;
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

/// Invite a member to the shared library.
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
    library_id: &str,
    library_name: &str,
) -> Result<crate::join_code::InviteCode, String> {
    let user_pubkey_hex = hex::encode(user_keypair.public_key());

    if public_key_hex == user_pubkey_hex {
        return Err("Cannot invite yourself".to_string());
    }

    // Download existing membership entries
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| format!("Failed to list membership entries: {e}"))?;

    // The founder is written once, when a library is created and first connects
    // its cloud (issue #102) — never lazily here. An empty listing at invite time
    // means the chain is missing (a fresh library that never founded, or a wiped
    // `membership/*`); bootstrapping a new founder on the spot is the takeover
    // primitive #104 describes, so refuse instead.
    if entry_keys.is_empty() {
        return Err(
            "no membership chain to invite into: the library's founder entry is \
             missing (it is established at library creation, not on invite)"
                .to_string(),
        );
    }
    let mut chain = load_anchored_chain(storage, &entry_keys, None)
        .await
        .map_err(|e| e.to_string())?;

    // Create the invitation
    let invite_ts = hlc.now().to_string();
    let join_info = super::invite::create_invitation_with_encryption(
        storage,
        cloud_home,
        &mut chain,
        entry_keys,
        user_keypair,
        public_key_hex,
        invitee_email,
        role,
        encryption,
        library_id,
        &invite_ts,
    )
    .await
    .map_err(|e| format!("Failed to create invitation: {e}"))?;

    info!(
        "Invited member {}...",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // The invite anchors the joiner to the library's OWNER — the chain founder,
    // not whichever Owner happens to send the invite. A second Owner inviting must
    // still hand the joiner the founder's pubkey, or the joiner would pin the wrong
    // owner and reject the (correctly founder-anchored) chain forever (issue #102).
    let owner_pubkey = chain
        .founder_pubkey()
        .ok_or_else(|| "membership chain has no founder".to_string())?
        .to_string();

    // Build the invite code
    Ok(crate::join_code::InviteCode {
        library_id: library_id.to_string(),
        library_name: library_name.to_string(),
        join_info,
        owner_pubkey,
    })
}

/// Remove a member from the shared library.
///
/// Downloads the membership chain, creates a signed Remove entry, rotates
/// the encryption key, re-wraps it for remaining members, and returns the
/// new encryption key bytes. The caller is responsible for persisting the
/// new key to the keyring and updating config.
pub async fn remove_member(
    storage: &dyn SyncStorage,
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    user_keypair: &UserKeypair,
    hlc: &Hlc,
    public_key_hex: &str,
    library_id: &str,
    current_encryption: &EncryptionService,
) -> Result<EncryptionService, String> {
    // Download existing membership entries and build the chain.
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| format!("Failed to list membership entries: {e}"))?;

    if entry_keys.is_empty() {
        return Err("No membership chain exists".to_string());
    }

    let mut chain = load_anchored_chain(storage, &entry_keys, None)
        .await
        .map_err(|e| e.to_string())?;

    // Revoke the member
    let revoke_ts = hlc.now().to_string();
    let new_key = super::invite::revoke_member(
        storage,
        cloud_home,
        &mut chain,
        entry_keys,
        user_keypair,
        public_key_hex,
        library_id,
        &revoke_ts,
        current_encryption,
    )
    .await
    .map_err(|e| format!("Failed to revoke member: {e}"))?;

    info!(
        "Revoked member {}... and rotated encryption key",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    Ok(new_key)
}

/// Rotate the in-use encryption key after a member removal: persist it to the
/// keyring and swap the live cipher in place. Returns the new key's fingerprint
/// for the host to record in its own config — coven never writes the host's
/// config.
///
/// A plaintext home has no library key to rotate, so this is an error there:
/// sharing (and hence member removal) requires an encrypted home.
pub fn apply_key_rotation(
    new_encryption: EncryptionService,
    key_persistence: &dyn KeyPersistence,
    cipher_lock: &std::sync::RwLock<CloudCipher>,
) -> Result<String, String> {
    let new_fingerprint = {
        let mut cipher = cipher_lock.write().unwrap();
        match &mut *cipher {
            CloudCipher::Encrypted(enc) => {
                let new_key_hex = new_encryption
                    .to_keyring_string()
                    .map_err(|e| format!("Failed to serialize encryption keyring: {e}"))?;
                key_persistence
                    .set_encryption_key(&new_key_hex)
                    .map_err(|e| format!("Failed to persist new encryption key: {e}"))?;
                *enc = new_encryption;
                enc.fingerprint()
            }
            CloudCipher::Plaintext => {
                return Err("sharing requires an encrypted cloud home".to_string());
            }
        }
    };
    Ok(new_fingerprint)
}

/// Write a library's founder entry: chain entry #1, a self-signed Owner `Add` of
/// `owner`, uploaded to `membership/{owner_pubkey}/1`. Called once, when a created
/// library first connects its cloud, so every opaque library has an owner-anchored
/// chain from the start (issue #102). The caller is responsible for only invoking
/// this when no chain exists yet; this unconditionally writes seq 1.
pub(crate) async fn write_founder_entry(
    storage: &dyn SyncStorage,
    owner: &UserKeypair,
    timestamp: &str,
) -> Result<(), String> {
    let entry = founder_entry(owner, timestamp);
    let coord = MembershipCoord {
        author_pubkey: hex::encode(owner.public_key()),
        seq: 1,
    };
    let mut chain = MembershipChain::new();
    chain
        .add_entry_at(coord.clone(), entry.clone())
        .map_err(|e| format!("Failed to validate founder entry: {e}"))?;
    let bytes = serde_json::to_vec(&entry)
        .map_err(|e| format!("Failed to serialize founder entry: {e}"))?;
    storage
        .put_membership_entry(&coord.author_pubkey, coord.seq, bytes)
        .await
        .map_err(|e| format!("Failed to upload founder entry: {e}"))?;
    publish_membership_head(storage, &chain, owner)
        .await
        .map_err(|e| format!("Failed to upload founder membership head: {e}"))?;
    info!("Wrote founder Owner entry for the library's membership chain");
    Ok(())
}

/// Download each membership entry, paired with the storage coordinate it was
/// loaded from. The pairing lets a caller name the exact entry that authorizes a
/// given pubkey (see [`super::membership::write_grant_coord`]);
/// [`download_chain`] drops the coordinates and builds the validated chain.
pub async fn download_entries(
    storage: &dyn SyncStorage,
    entry_keys: &[(String, u64)],
) -> Result<Vec<(MembershipCoord, MembershipEntry)>, String> {
    let mut entries = Vec::with_capacity(entry_keys.len());
    for (author, seq) in entry_keys {
        let data = storage
            .get_membership_entry(author, *seq)
            .await
            .map_err(|e| format!("Failed to get membership entry {author}/{seq}: {e}"))?;

        let entry: MembershipEntry = serde_json::from_slice(&data)
            .map_err(|e| format!("Failed to parse membership entry {author}/{seq}: {e}"))?;
        entries.push((
            MembershipCoord {
                author_pubkey: author.clone(),
                seq: *seq,
            },
            entry,
        ));
    }
    Ok(entries)
}

/// Download and build a membership chain from the storage.
pub async fn download_chain(
    storage: &dyn SyncStorage,
    entry_keys: &[(String, u64)],
) -> Result<MembershipChain, String> {
    let raw_entries = download_entries(storage, entry_keys).await?;

    MembershipChain::from_entries_with_coords(raw_entries)
        .map_err(|e| format!("Invalid membership chain: {e}"))
}

pub async fn publish_membership_head(
    storage: &dyn SyncStorage,
    chain: &MembershipChain,
    signer: &UserKeypair,
) -> Result<MembershipChainHead, String> {
    let head = chain.signed_head(signer);
    let bytes = serde_json::to_vec(&head)
        .map_err(|e| format!("Failed to serialize membership head: {e}"))?;
    storage
        .put_membership_head(bytes)
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
pub(crate) enum AnchoredChainError {
    /// The entries didn't download, parse, or validate into a well-formed chain.
    #[error("membership chain failed to load/validate: {0}")]
    LoadFailed(String),
    /// The chain is well-formed but its founder is not the library's pinned owner.
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

/// Authorize an already-loaded membership chain. `None` is the chain-less open
/// library case.
pub(crate) fn authorize_loaded_membership_author(
    chain: Option<&MembershipChain>,
    author_pubkey: &str,
    requirement: MembershipAuthorRequirement,
) -> Result<(), String> {
    let Some(chain) = chain else {
        debug!(
            author = %author_pubkey,
            required_role = ?requirement,
            "membership author authorization skipped: library is chain-less (no membership, \
             no pinned owner), so authorization is not applicable"
        );
        return Ok(());
    };

    if !requirement.permits(chain, author_pubkey) {
        return Err(requirement.denial_message(author_pubkey));
    }

    Ok(())
}

/// Why a signed control object's author failed membership authorization.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MembershipAuthorAuthorizationError {
    /// Membership entries could not be listed from storage.
    #[error("failed to list membership entries: {0}")]
    ListMembershipEntries(StorageError),
    /// The author or membership chain does not satisfy the requested role.
    #[error("{0}")]
    Unauthorized(String),
}

/// Authorize a signed control object's author against the library's membership
/// chain, anchored to the pinned owner when one is known.
pub(crate) async fn authorize_membership_author(
    storage: &dyn SyncStorage,
    author_pubkey: &str,
    pinned_owner: Option<&str>,
    requirement: MembershipAuthorRequirement,
) -> Result<(), MembershipAuthorAuthorizationError> {
    let chain = load_membership_chain(storage, pinned_owner).await?;
    authorize_loaded_membership_author(chain.as_ref(), author_pubkey, requirement)
        .map_err(MembershipAuthorAuthorizationError::Unauthorized)
}

/// Load the library membership chain when one exists, anchored to the pinned owner
/// when known.
pub(crate) async fn load_membership_chain(
    storage: &dyn SyncStorage,
    pinned_owner: Option<&str>,
) -> Result<Option<MembershipChain>, MembershipAuthorAuthorizationError> {
    let entries = storage
        .list_membership_entries()
        .await
        .map_err(MembershipAuthorAuthorizationError::ListMembershipEntries)?;

    if entries.is_empty() {
        if let Some(owner) = pinned_owner {
            return Err(MembershipAuthorAuthorizationError::Unauthorized(format!(
                "membership chain is empty but owner {owner} is pinned (wiped membership/*)"
            )));
        }

        return Ok(None);
    }

    let chain = load_anchored_chain(storage, &entries, pinned_owner)
        .await
        .map_err(|e| MembershipAuthorAuthorizationError::Unauthorized(e.to_string()))?;
    Ok(Some(chain))
}

/// Download and validate the membership chain from `entry_keys`, then confirm it
/// is anchored to `owner_pubkey` when one is pinned. Returns the validated,
/// owner-anchored chain.
///
/// The shared load+anchor step the pull cycle and the snapshot authorization both
/// run: validation proves the chain is well-formed (signatures, owner-only
/// authorship), and the anchor proves it descends from the library's established
/// owner rather than a wiped-and-refounded chain under an attacker's key. A
/// library with no pinned owner (browsable) skips the anchor check — it is open by
/// design and has no owner to anchor to.
///
/// `entry_keys` must be non-empty (an empty listing is each caller's own
/// short-circuit: pull falls open, snapshot accepts on signature alone), so an
/// empty set here surfaces as a `LoadFailed` (the chain won't validate) rather
/// than a silent success.
pub(crate) async fn load_anchored_chain(
    storage: &dyn SyncStorage,
    entry_keys: &[(String, u64)],
    owner_pubkey: Option<&str>,
) -> Result<MembershipChain, AnchoredChainError> {
    let head = match storage.get_membership_head().await {
        Ok(bytes) => serde_json::from_slice::<MembershipChainHead>(&bytes)
            .map_err(|e| AnchoredChainError::LoadFailed(format!("parse membership head: {e}")))?,
        Err(StorageError::NotFound(e)) => {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head is missing: {e}"
            )))
        }
        Err(e) => {
            return Err(AnchoredChainError::LoadFailed(format!(
                "Failed to get membership head: {e}"
            )))
        }
    };
    let committed_entry_keys = entry_keys_committed_by_head(entry_keys, &head);
    let chain = download_chain(storage, &committed_entry_keys)
        .await
        .map_err(AnchoredChainError::LoadFailed)?;
    chain
        .verify_head(&head)
        .map_err(|e| AnchoredChainError::LoadFailed(e.to_string()))?;
    if let Some(owner) = owner_pubkey {
        if !chain.is_founded_by(owner) {
            return Err(AnchoredChainError::FounderMismatch {
                founder: chain.founder_pubkey().map(str::to_string),
                owner: owner.to_string(),
            });
        }
    }
    Ok(chain)
}

fn entry_keys_committed_by_head(
    entry_keys: &[(String, u64)],
    head: &MembershipChainHead,
) -> Vec<(String, u64)> {
    let mut keys = Vec::new();
    for (listed_author, listed_seq) in entry_keys {
        if head.entries.iter().any(|entry| {
            entry.coord.author_pubkey == *listed_author && *listed_seq <= entry.coord.seq
        }) {
            keys.push((listed_author.clone(), *listed_seq));
        }
    }
    for entry in &head.entries {
        if !keys.iter().any(|(listed_author, seq)| {
            listed_author == &entry.coord.author_pubkey && *seq == entry.coord.seq
        }) {
            keys.push((entry.coord.author_pubkey.clone(), entry.coord.seq));
        }
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::membership::MembershipAction;
    use crate::sync::test_helpers::{
        append_membership_entry, founder_entry, make_linked_entry, pubkey_hex, MockSyncStorage,
    };

    /// The invite anchors the joiner to the library FOUNDER, regardless of which
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
        let storage = MockSyncStorage::new();
        let mut chain = MembershipChain::new();
        let founder_entry = founder_entry(&founder, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &founder_pk, 1, founder_entry).await;
        let add_owner = make_linked_entry(
            &chain,
            &founder,
            MembershipAction::Add,
            &second_owner,
            MemberRole::Owner,
            "0000000002000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &founder_pk, 2, add_owner).await;
        publish_membership_head(&storage, &chain, &founder)
            .await
            .unwrap();

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
    async fn invite_and_remove_reuse_their_loaded_membership_listing() {
        let owner = UserKeypair::generate();
        let invitee = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let owner_pk = pubkey_hex(&owner);
        let invitee_pk = pubkey_hex(&invitee);
        let mut chain = MembershipChain::new();
        let founder = founder_entry(&owner, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

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
        )
        .await
        .expect("invite");

        assert_eq!(storage.membership_list_count(), 1);

        remove_member(
            &storage,
            &storage,
            &owner,
            &hlc,
            &invitee_pk,
            "lib-1",
            &EncryptionService::from_key([7u8; 32]),
        )
        .await
        .expect("remove");

        assert_eq!(storage.membership_list_count(), 2);
    }

    #[tokio::test]
    async fn suppressed_remove_is_detected() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let owner_pk = pubkey_hex(&owner);
        let member_pk = pubkey_hex(&member);
        let mut chain = MembershipChain::new();

        let founder = founder_entry(&owner, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_member = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
        let remove_member = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Remove,
            &member,
            MemberRole::Member,
            "0000000003000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        storage
            .delete(&format!("membership/{owner_pk}/3"))
            .await
            .unwrap();
        let visible = storage.list_membership_entries().await.unwrap();

        let result = load_anchored_chain(&storage, &visible, Some(&owner_pk)).await;
        assert!(
            result.is_err(),
            "a listed chain missing a signed Remove must be rejected, not read {member_pk} as current"
        );
    }

    #[tokio::test]
    async fn suppressed_middle_entry_breaks_the_link() {
        let owner = UserKeypair::generate();
        let member_a = UserKeypair::generate();
        let member_b = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = founder_entry(&owner, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_a = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &member_a,
            MemberRole::Member,
            "0000000002000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_a).await;
        let add_b = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &member_b,
            MemberRole::Member,
            "0000000003000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, add_b).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        storage.hide_membership_from_listing(&owner_pk, 2);
        let visible = storage.list_membership_entries().await.unwrap();

        let result = load_anchored_chain(&storage, &visible, Some(&owner_pk)).await;
        assert!(
            result.is_err(),
            "a successor that names a hidden predecessor must reject the listed chain"
        );
    }

    #[tokio::test]
    async fn complete_chain_still_validates() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = founder_entry(&owner, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_member = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        let visible = storage.list_membership_entries().await.unwrap();
        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk))
            .await
            .expect("complete chain validates");

        assert!(loaded.can_write_now(&pubkey_hex(&member)));
    }

    #[tokio::test]
    async fn missing_membership_head_is_rejected() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = founder_entry(&owner, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let visible = storage.list_membership_entries().await.unwrap();

        let result = load_anchored_chain(&storage, &visible, Some(&owner_pk)).await;
        assert!(
            result.is_err(),
            "a non-empty membership chain without a signed head is not committed"
        );
    }

    #[tokio::test]
    async fn entry_beyond_membership_head_is_not_committed() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();

        let founder = founder_entry(&owner, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, founder).await;
        let add_member = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add_member).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        let remove_member = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Remove,
            &member,
            MemberRole::Member,
            "0000000003000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove_member).await;
        let visible = storage.list_membership_entries().await.unwrap();

        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk))
            .await
            .expect("unheaded entry is ignored");
        assert!(
            loaded.can_write_now(&pubkey_hex(&member)),
            "membership state follows the signed head, not listed entries past it"
        );
    }
}
