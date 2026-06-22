//! Membership operations: get members, invite, and revoke.
//!
//! These are the high-level orchestration functions that download the membership
//! chain from the storage, perform the operation, and upload the results.

use tracing::{info, warn};

use crate::encryption::EncryptionService;
use crate::keys::{KeyService, UserKeypair};

use super::cloud_storage::CloudCipher;
use super::hlc::Hlc;
use super::membership::{founder_entry, MemberRole, MembershipChain, MembershipEntry};
use super::storage::SyncStorage;

/// `sync_state` key holding the hex Ed25519 pubkey of the library's established
/// owner — pinned at create (the creator), join (the invite's owner), or restore.
/// The membership chain is anchored to it: a chain whose founder differs is a
/// takeover attempt and is rejected (issue #95).
pub(crate) const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";

/// A member as seen by the caller.
pub struct MemberInfo {
    pub pubkey: String,
    pub role: MemberRole,
    pub is_self: bool,
}

/// Read the membership chain from the sync storage and return the current members.
pub async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| MembershipOpsError(format!("Failed to list membership entries: {e}")))?;

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
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    let user_pubkey_hex = hex::encode(user_keypair.public_key);

    if public_key_hex == user_pubkey_hex {
        return Err(MembershipOpsError("Cannot invite yourself".to_string()));
    }

    // Download existing membership entries
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| MembershipOpsError(format!("Failed to list membership entries: {e}")))?;

    // The founder is written once, when a library is created and first connects
    // its cloud (issue #102) — never lazily here. An empty listing at invite time
    // means the chain is missing (a fresh library that never founded, or a wiped
    // `membership/*`); bootstrapping a new founder on the spot is the takeover
    // primitive #104 describes, so refuse instead.
    if entry_keys.is_empty() {
        return Err(MembershipOpsError(
            "no membership chain to invite into: the library's founder entry is \
             missing (it is established at library creation, not on invite)"
                .to_string(),
        ));
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
        &invite_ts,
    )
    .await
    .map_err(|e| MembershipOpsError(format!("Failed to create invitation: {e}")))?;

    info!(
        "Invited member {}...",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // Sync authorized keys files for proxy auth
    if let Err(e) = sync_authorized_keys(cloud_home, &chain).await {
        warn!("Failed to sync authorized keys: {e}");
    }

    // Build the invite code
    Ok(crate::join_code::InviteCode {
        library_id: library_id.to_string(),
        library_name: library_name.to_string(),
        join_info,
        owner_pubkey: user_pubkey_hex,
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
) -> Result<[u8; 32], MembershipOpsError> {
    // Download existing membership entries and build the chain.
    let entry_keys = storage
        .list_membership_entries()
        .await
        .map_err(|e| MembershipOpsError(format!("Failed to list membership entries: {e}")))?;

    if entry_keys.is_empty() {
        return Err(MembershipOpsError("No membership chain exists".to_string()));
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
        &revoke_ts,
    )
    .await
    .map_err(|e| MembershipOpsError(format!("Failed to revoke member: {e}")))?;

    info!(
        "Revoked member {}... and rotated encryption key",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // Sync authorized keys files for proxy auth
    if let Err(e) = sync_authorized_keys(cloud_home, &chain).await {
        warn!("Failed to sync authorized keys: {e}");
    }

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
    key_service: &KeyService,
    cipher_lock: &std::sync::RwLock<CloudCipher>,
) -> Result<String, MembershipOpsError> {
    let new_fingerprint = {
        let mut cipher = cipher_lock.write().unwrap();
        match &mut *cipher {
            CloudCipher::Encrypted(enc) => {
                let new_key_hex = hex::encode(new_key);
                key_service.set_encryption_key(&new_key_hex).map_err(|e| {
                    MembershipOpsError(format!("Failed to persist new encryption key: {e}"))
                })?;
                *enc = EncryptionService::from_key(new_key);
                enc.fingerprint()
            }
            CloudCipher::Plaintext => {
                return Err(MembershipOpsError(
                    "sharing requires an encrypted cloud home".to_string(),
                ));
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
) -> Result<(), MembershipOpsError> {
    let entry = founder_entry(owner, timestamp);
    let bytes = serde_json::to_vec(&entry)
        .map_err(|e| MembershipOpsError(format!("Failed to serialize founder entry: {e}")))?;
    storage
        .put_membership_entry(&hex::encode(owner.public_key), 1, bytes)
        .await
        .map_err(|e| MembershipOpsError(format!("Failed to upload founder entry: {e}")))?;
    info!("Wrote founder Owner entry for the library's membership chain");
    Ok(())
}

/// Download and build a membership chain from the storage.
pub(crate) async fn download_chain(
    storage: &dyn SyncStorage,
    entry_keys: &[(String, u64)],
) -> Result<MembershipChain, MembershipOpsError> {
    let mut raw_entries = Vec::new();
    for (author, seq) in entry_keys {
        let data = storage
            .get_membership_entry(author, *seq)
            .await
            .map_err(|e| {
                MembershipOpsError(format!(
                    "Failed to get membership entry {author}/{seq}: {e}"
                ))
            })?;

        let entry: MembershipEntry = serde_json::from_slice(&data).map_err(|e| {
            MembershipOpsError(format!(
                "Failed to parse membership entry {author}/{seq}: {e}"
            ))
        })?;
        raw_entries.push(entry);
    }

    MembershipChain::from_entries(raw_entries)
        .map_err(|e| MembershipOpsError(format!("Invalid membership chain: {e}")))
}

/// Write individual `auth/keys/{pubkey}` files for each current member.
///
/// This materializes the membership chain into per-key files that the proxy
/// can read without understanding the (encrypted) chain format. The file's
/// *content* is the member's role wire string (`owner`/`member`/`follower`), so
/// the proxy can gate writes by role: a Follower's signed requests authenticate
/// but may only read. Keys are written for every current member (so a role
/// change is reflected) and deleted for anyone no longer in the chain.
pub async fn sync_authorized_keys(
    cloud_home: &dyn crate::storage::cloud::CloudHome,
    chain: &MembershipChain,
) -> Result<(), MembershipOpsError> {
    use std::collections::HashSet;

    let current = chain.current_members();
    let current_keys: HashSet<&String> = current.iter().map(|(pk, _)| pk).collect();

    let existing = cloud_home
        .list("auth/keys/")
        .await
        .map_err(|e| MembershipOpsError(format!("list auth keys: {e}")))?;
    let existing_keys: HashSet<String> = existing
        .iter()
        .filter_map(|k| k.strip_prefix("auth/keys/"))
        .map(|s| s.to_string())
        .collect();

    // Write (or overwrite) every current member's key file with its current
    // role, so role changes (e.g. Member -> Follower) propagate to the proxy.
    for (pk, role) in &current {
        cloud_home
            .write(
                &format!("auth/keys/{pk}"),
                role.as_str().as_bytes().to_vec(),
                &crate::storage::cloud::no_progress(),
            )
            .await
            .map_err(|e| MembershipOpsError(format!("write auth key: {e}")))?;
    }

    for pk in &existing_keys {
        if !current_keys.contains(pk) {
            cloud_home
                .delete(&format!("auth/keys/{pk}"))
                .await
                .map_err(|e| MembershipOpsError(format!("delete auth key: {e}")))?;
        }
    }

    Ok(())
}

/// Membership operations error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct MembershipOpsError(pub String);
