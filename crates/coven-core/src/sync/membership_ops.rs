//! Membership operations: get members, invite, and revoke.
//!
//! These are the high-level orchestration functions that download the membership
//! chain from the storage, perform the operation, and upload the results.

use tracing::info;

use crate::encryption::EncryptionService;
use crate::keys::{KeyPersistence, UserKeypair};

use super::cloud_storage::CloudCipher;
use super::hlc::Hlc;
use super::membership::{
    founder_entry, MemberInfo, MemberRole, MembershipChain, MembershipCoord, MembershipEntry,
};
use super::storage::SyncStorage;

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

    let chain = download_chain(storage, &entry_keys).await?;
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
    role: MemberRole,
    encryption_key: &[u8; 32],
    library_id: &str,
    library_name: &str,
) -> Result<crate::join_code::InviteCode, String> {
    let user_pubkey_hex = hex::encode(user_keypair.public_key);

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
    let mut chain = download_chain(storage, &entry_keys).await?;

    // Create the invitation
    let invite_ts = hlc.now().to_string();
    let join_info = super::invite::create_invitation(
        storage,
        cloud_home,
        &mut chain,
        user_keypair,
        public_key_hex,
        role,
        encryption_key,
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
) -> Result<[u8; 32], String> {
    // Download existing membership entries and build the chain.
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| format!("Failed to list membership entries: {e}"))?;

    if entry_keys.is_empty() {
        return Err("No membership chain exists".to_string());
    }

    let mut chain = download_chain(storage, &entry_keys).await?;

    // Revoke the member
    let revoke_ts = hlc.now().to_string();
    let new_key = super::invite::revoke_member(
        storage,
        cloud_home,
        &mut chain,
        user_keypair,
        public_key_hex,
        library_id,
        &revoke_ts,
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
    new_key: [u8; 32],
    key_persistence: &dyn KeyPersistence,
    cipher_lock: &std::sync::RwLock<CloudCipher>,
) -> Result<String, String> {
    let new_fingerprint = {
        let mut cipher = cipher_lock.write().unwrap();
        match &mut *cipher {
            CloudCipher::Encrypted(enc) => {
                let new_key_hex = hex::encode(new_key);
                key_persistence
                    .set_encryption_key(&new_key_hex)
                    .map_err(|e| format!("Failed to persist new encryption key: {e}"))?;
                *enc = EncryptionService::from_key(new_key);
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
    let bytes = serde_json::to_vec(&entry)
        .map_err(|e| format!("Failed to serialize founder entry: {e}"))?;
    storage
        .put_membership_entry(&hex::encode(owner.public_key), 1, bytes)
        .await
        .map_err(|e| format!("Failed to upload founder entry: {e}"))?;
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
    let raw_entries = download_entries(storage, entry_keys)
        .await?
        .into_iter()
        .map(|(_, entry)| entry)
        .collect();

    MembershipChain::from_entries(raw_entries).map_err(|e| format!("Invalid membership chain: {e}"))
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
    let chain = download_chain(storage, entry_keys)
        .await
        .map_err(AnchoredChainError::LoadFailed)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::membership::MembershipAction;
    use crate::sync::test_helpers::{founder_entry, make_entry, pubkey_hex, MockSyncStorage};

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
        storage
            .put_membership_entry(
                &founder_pk,
                1,
                serde_json::to_vec(&founder_entry(&founder, "0000000001000-0000-f")).unwrap(),
            )
            .await
            .unwrap();
        let add_owner = make_entry(
            &founder,
            MembershipAction::Add,
            &second_owner,
            MemberRole::Owner,
            "0000000002000-0000-f",
        );
        storage
            .put_membership_entry(&founder_pk, 2, serde_json::to_vec(&add_owner).unwrap())
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
            MemberRole::Member,
            &[7u8; 32],
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
}
