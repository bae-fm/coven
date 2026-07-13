//! Membership operations: get members, invite, and revoke.
//!
//! These are the high-level orchestration functions that download the membership
//! chain from the storage, perform the operation, and upload the results.

use tracing::{debug, info};

use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{KeyError, MasterKeyCustody, UserKeypair};
#[cfg(test)]
use crate::storage::cloud::CloudHome;

use super::cloud_storage::{CloudCipher, PendingRotation};
use super::hlc::Hlc;
use super::invite::InviteError;
use super::membership::{
    founder_entry, AuthorHead, MemberInfo, MemberRole, MembershipChain, MembershipCoord,
    MembershipEntry,
};
use super::storage::{StorageError, SyncStorage};
use crate::database::Database;
use std::collections::BTreeSet;

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
}

/// `sync_state` key holding the hex Ed25519 pubkey of the store's established
/// owner — pinned at create (the creator), join (the invite's owner), or restore.
/// The membership chain is anchored to it: a chain whose founder differs is a
/// takeover attempt and is rejected (issue #95).
pub const OWNER_PUBKEY_STATE_KEY: &str = "owner_pubkey";

/// The per-author membership-head floor at the chain's current committed state:
/// empty for a chain-less (browsable) store, otherwise every author's highest
/// committed seq ([`MembershipChain::author_heads`]). Minted into an invite or
/// restore code so the joiner or restorer can seed its watermark
/// ([`seed_head_watermark`]) before its first sync cycle.
///
/// `watermark_db`, when present, makes this read monotonic the same way every
/// other chain load is: the minting device's own view of the chain never
/// regresses either. Shares [`load_anchored_chain`]'s fail-closed stance — for a
/// `pinned_owner` a chain that won't validate or anchor is a takeover attempt,
/// not silently treated as absent.
pub async fn current_membership_floor(
    storage: &dyn SyncStorage,
    pinned_owner: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<Vec<super::membership::MembershipCoord>, MembershipOpsError> {
    let entries = storage.list_membership_entries().await?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let chain = load_anchored_chain(storage, &entries, pinned_owner, watermark_db).await?;
    Ok(chain.author_heads())
}

/// Read the membership chain from the sync storage and return the current members.
pub async fn get_members(
    storage: &dyn SyncStorage,
    user_pubkey: Option<&[u8]>,
) -> Result<Vec<MemberInfo>, MembershipOpsError> {
    let entry_keys = storage.list_membership_entries().await?;

    if entry_keys.is_empty() {
        return Ok(Vec::new());
    }

    let chain = load_anchored_chain(storage, &entry_keys, None, None).await?;
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
) -> Result<crate::join_code::InviteCode, MembershipOpsError> {
    let user_pubkey_hex = hex::encode(user_keypair.public_key());

    if public_key_hex == user_pubkey_hex {
        return Err(MembershipOpsError::SelfInvite);
    }

    // Download existing membership entries
    let entry_keys = storage.list_membership_entries().await?;

    // The founder is written once, when a store is created and first connects
    // its cloud (issue #102) — never lazily here. An empty listing at invite time
    // means the chain is missing (a fresh store that never founded, or a wiped
    // `membership/*`); bootstrapping a new founder on the spot is the takeover
    // primitive #104 describes, so refuse instead.
    if entry_keys.is_empty() {
        return Err(MembershipOpsError::NoFounderChain);
    }
    let mut chain = load_anchored_chain(storage, &entry_keys, None, None).await?;

    // Create the invitation
    let invite_ts = hlc.now().to_string();
    let join_info = super::invite::create_invitation_with_encryption(
        storage,
        cloud_home,
        &mut chain,
        user_keypair,
        public_key_hex,
        invitee_email,
        role,
        encryption,
        store_id,
        &invite_ts,
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

    // Build the invite code
    Ok(crate::join_code::InviteCode {
        v: crate::join_code::INVITE_CODE_VERSION,
        store_id: store_id.to_string(),
        store_name: store_name.to_string(),
        join_info,
        owner_pubkey,
        membership_floor,
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
    cipher_lock: &std::sync::RwLock<CloudCipher>,
    pending_rotation: &PendingRotation,
) -> Result<String, MembershipOpsError> {
    // Download existing membership entries and build the chain.
    let entry_keys = storage.list_membership_entries().await?;

    if entry_keys.is_empty() {
        return Err(MembershipOpsError::NoMembershipChain);
    }

    let mut chain = load_anchored_chain(storage, &entry_keys, None, None).await?;

    // Revoke the member and rotate the cloud key. On return the rotation is
    // committed for every remaining member.
    let revoke_ts = hlc.now().to_string();
    let new_key = super::invite::revoke_member(
        storage,
        cloud_home,
        &mut chain,
        entry_keys,
        user_keypair,
        public_key_hex,
        store_id,
        &revoke_ts,
        current_encryption,
    )
    .await?;

    info!(
        "Revoked member {}... and rotated encryption key",
        &public_key_hex[..public_key_hex.len().min(16)]
    );

    // Adopt the rotated key into this device's live cipher and custody. The cloud
    // rotation is already committed, so a failure here is not a generic membership
    // error but the specific half-applied state its own variant names.
    apply_key_rotation(new_key, custody, cipher_lock, pending_rotation)
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
    cipher_lock: &std::sync::RwLock<CloudCipher>,
    pending_rotation: &PendingRotation,
) -> Result<String, KeyError> {
    // Mark first, so a seal that clones the live cipher during the persist+swap
    // below refuses under the superseded generation until the swap lands.
    pending_rotation.mark_committed(new_encryption.current_generation());
    let new_fingerprint = {
        let mut cipher = cipher_lock.write().unwrap();
        match &mut *cipher {
            CloudCipher::Encrypted(live) => {
                // Merge, never replace: fold the incoming keyring into the live
                // one so a key once adopted is never dropped. Re-check, under this
                // same write lock, that the merge genuinely extends what is live —
                // a concurrent member op may have committed and adopted a newer
                // rotation between the caller's is-it-newer check (an earlier,
                // separate read lock, with a network scan in between) and here.
                // Adopting a keyring that adds nothing would regress the seal key
                // and custody, so a stale apply fails loud instead. A persistence
                // failure returns early with the mark left set — the rotation is
                // committed on the cloud and this device is still on the old key.
                let merged = live.merged_with(&new_encryption);
                if merged.key_count() == live.key_count() {
                    None
                } else {
                    custody.persist(&MasterKeyring::from(merged.clone()))?;
                    *live = merged;
                    Some(live.fingerprint())
                }
            }
            // Both callers confirm an encrypted home before rotating: the sync
            // manager's `require_encrypted_home` gate (surfacing NotEncryptedHome)
            // and the refresh cycle's own plaintext check. A plaintext home here is
            // a broken invariant, not a user-facing condition.
            CloudCipher::Plaintext => {
                unreachable!(
                    "apply_key_rotation runs only after the caller has confirmed an \
                     encrypted home"
                )
            }
        }
    };
    // Re-derive the pause from the live cipher: a merge (or an already-covered
    // stale apply) that now covers everything committed clears it; a strictly
    // newer generation still pending stays paused.
    pending_rotation.resolve(&cipher_lock.read().unwrap());
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
    info!("Wrote founder Owner entry for the store's membership chain");
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

/// The seq of `author`'s committed head in storage, or `None` when the author has
/// never published one (a legitimate absence — membership seqs start at 1). A head
/// present but whose signature does not verify is tamper, not absence: it fails
/// loud rather than reading as `None` and being silently overwritten.
pub(crate) async fn committed_head_seq(
    storage: &dyn SyncStorage,
    author: &str,
) -> Result<Option<u64>, String> {
    match storage.get_membership_head(author).await {
        Ok(bytes) => {
            let head: AuthorHead = serde_json::from_slice(&bytes)
                .map_err(|e| format!("Failed to parse membership head {author}: {e}"))?;
            if !head.verify() {
                return Err(format!("membership head {author} has an invalid signature"));
            }
            Ok(Some(head.seq))
        }
        Err(StorageError::NotFound(_)) => Ok(None),
        Err(e) => Err(format!("Failed to read membership head {author}: {e}")),
    }
}

/// Publish `signer`'s membership head, certifying its own committed prefix in
/// `chain`. The write is monotonic: it refuses to publish a head whose seq does
/// not advance the one already stored, so a device working from a stale view fails
/// loud (and retries on top of the observed head) instead of rolling the head back
/// over a peer's newer commit.
///
/// This read-then-write leaves one residual window: two devices holding the same
/// restored keypair that pass the precondition at the same instant can each write
/// an entry object at the same seq, and the later write wins. The design accepts
/// this — a shared keypair is one identity, not coordinated writers — and it does
/// not corrupt readers: whichever entry loses, the surviving head's tip-hash check
/// fails for the other device on its next load, so it republishes on top of the
/// observed head. No provider gives a conditional put that would close the window
/// outright.
pub async fn publish_membership_head(
    storage: &dyn SyncStorage,
    chain: &MembershipChain,
    signer: &UserKeypair,
) -> Result<AuthorHead, String> {
    let head = chain
        .signed_head(signer)
        .ok_or_else(|| "cannot publish a head for an author with no entries".to_string())?;
    if let Some(stored_seq) = committed_head_seq(storage, &head.author_pubkey).await? {
        if stored_seq >= head.seq {
            return Err(format!(
                "stale membership head: {} already committed through seq {stored_seq}, \
                 refusing to publish seq {}",
                head.author_pubkey, head.seq
            ));
        }
    }
    let bytes = serde_json::to_vec(&head)
        .map_err(|e| format!("Failed to serialize membership head: {e}"))?;
    storage
        .put_membership_head(&head.author_pubkey, bytes)
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

/// Authorize an already-loaded membership chain. `None` is the chain-less open
/// store case.
pub(crate) fn authorize_loaded_membership_author(
    chain: Option<&MembershipChain>,
    author_pubkey: &str,
    requirement: MembershipAuthorRequirement,
) -> Result<(), String> {
    let Some(chain) = chain else {
        debug!(
            author = %author_pubkey,
            required_role = ?requirement,
            "membership author authorization skipped: store is chain-less (no membership, \
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

/// Load the store membership chain when one exists, anchored to the pinned owner
/// when known. `watermark_db` threads through to [`load_anchored_chain`]'s
/// monotonic head guard for readers that re-evaluate authorization each cycle.
pub(crate) async fn load_membership_chain(
    storage: &dyn SyncStorage,
    pinned_owner: Option<&str>,
    watermark_db: Option<&Database>,
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

    let chain = load_anchored_chain(storage, &entries, pinned_owner, watermark_db)
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
/// authorship), and the anchor proves it descends from the store's established
/// owner rather than a wiped-and-refounded chain under an attacker's key. A
/// store with no pinned owner (browsable) skips the anchor check — it is open by
/// design and has no owner to anchor to.
///
/// `entry_keys` must be non-empty (an empty listing is each caller's own
/// short-circuit: pull falls open, snapshot accepts on signature alone), so an
/// empty set here surfaces as a `LoadFailed` (the chain won't validate) rather
/// than a silent success.
///
/// The committed set is the union of every author's committed prefix: for each
/// author that has a signed head, entries `1..=head.seq` are admitted (fetched by
/// keyed GET, so a listing that lags a fresh write can't hide a committed entry).
/// `validate` then re-derives owner authorship across the merged, timestamp-ordered
/// set, so an author who was never an owner has its entries rejected — the same
/// fold authorization already performs. An author whose head is missing contributes
/// nothing: its entries stay uncommitted until it publishes a head covering them.
///
/// `watermark_db`, when present, makes the read monotonic per author: a head whose
/// seq regresses the last one this reader accepted (persisted in `sync_state`) is
/// refused, and each accepted head advances the watermark. This closes the window
/// where a stale head replica (or a same-author two-device overwrite) would rewind
/// a reader's committed view.
pub(crate) async fn load_anchored_chain(
    storage: &dyn SyncStorage,
    entry_keys: &[(String, u64)],
    owner_pubkey: Option<&str>,
    watermark_db: Option<&Database>,
) -> Result<MembershipChain, AnchoredChainError> {
    let authors: BTreeSet<String> = entry_keys
        .iter()
        .map(|(author, _)| author.clone())
        .collect();

    let mut heads: Vec<AuthorHead> = Vec::new();
    for author in &authors {
        let bytes = match storage.get_membership_head(author).await {
            Ok(bytes) => bytes,
            Err(StorageError::NotFound(_)) => {
                // Entries exist but no head: this author has committed nothing yet,
                // so its prefix contributes nothing to the committed set.
                debug!(%author, "membership head absent; author's entries are uncommitted");
                continue;
            }
            Err(e) => {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "Failed to get membership head {author}: {e}"
                )))
            }
        };
        let head: AuthorHead = serde_json::from_slice(&bytes).map_err(|e| {
            AnchoredChainError::LoadFailed(format!("parse membership head {author}: {e}"))
        })?;
        if !head.verify() {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head {author} has an invalid signature"
            )));
        }
        if let Some(db) = watermark_db {
            if let Some(accepted) = read_head_watermark(db, author)
                .await
                .map_err(AnchoredChainError::LoadFailed)?
            {
                if head.seq < accepted {
                    return Err(AnchoredChainError::LoadFailed(format!(
                        "membership head {author} regressed to seq {} below the accepted {accepted}",
                        head.seq
                    )));
                }
            }
        }
        heads.push(head);
    }

    let committed_entry_keys: Vec<(String, u64)> = heads
        .iter()
        .flat_map(|head| (1..=head.seq).map(|seq| (head.author_pubkey.clone(), seq)))
        .collect();
    let chain = download_chain(storage, &committed_entry_keys)
        .await
        .map_err(AnchoredChainError::LoadFailed)?;

    // Each head must match the prefix it certifies: same tip seq, same tip hash.
    for head in &heads {
        match chain.author_tip(&head.author_pubkey) {
            Some((seq, tip_hash)) if seq == head.seq && tip_hash == head.tip_hash => {}
            other => {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {} claims seq {}/{} but the chain tip is {other:?}",
                    head.author_pubkey, head.seq, head.tip_hash
                )))
            }
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

    if let Some(db) = watermark_db {
        for head in &heads {
            persist_head_watermark(db, &head.author_pubkey, head.seq)
                .await
                .map_err(AnchoredChainError::LoadFailed)?;
        }
    }

    Ok(chain)
}

/// `sync_state` key holding the greatest membership-head seq this reader has
/// accepted from `author`. The read path refuses any later head that regresses it.
fn head_watermark_key(author: &str) -> String {
    format!("membership_head_seq/{author}")
}

async fn read_head_watermark(db: &Database, author: &str) -> Result<Option<u64>, String> {
    match db.get_sync_state(&head_watermark_key(author)).await {
        Ok(Some(value)) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|e| format!("membership head watermark for {author} is not a seq: {e}")),
        Ok(None) => Ok(None),
        Err(e) => Err(format!("read membership head watermark for {author}: {e}")),
    }
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
async fn persist_head_watermark(db: &Database, author: &str, seq: u64) -> Result<(), String> {
    let key = head_watermark_key(author);
    db.call(move |conn| {
        conn.execute(
            "INSERT INTO sync_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value \
             WHERE CAST(excluded.value AS INTEGER) > CAST(sync_state.value AS INTEGER)",
            rusqlite::params![key, seq.to_string()],
        )
        .map(|_| ())
        .map_err(crate::database::DbError::from)
    })
    .await
    .map_err(|e| format!("persist membership head watermark for {author}: {e}"))
}

/// Seed this reader's per-author membership-head watermark from `floor` — the
/// `(author_pubkey, seq)` pairs an invite or restore code carries from mint time
/// ([`current_membership_floor`]). Persists through the exact `sync_state`
/// entries [`load_anchored_chain`]'s monotonic guard reads, so from this
/// device's first sync cycle on, any head at or below the seeded floor is
/// refused as a regression — exactly as if this reader had already accepted it.
///
/// Called once, before a join or restore's first sync cycle, on a `db` whose
/// `sync_state` has no membership watermark yet; the persist is monotonic
/// regardless, so a seed can never lower a watermark either.
pub async fn seed_head_watermark(
    db: &Database,
    floor: &[super::membership::MembershipCoord],
) -> Result<(), String> {
    for coord in floor {
        persist_head_watermark(db, &coord.author_pubkey, coord.seq).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::cloud_storage::CloudCipher;
    use crate::sync::hlc::Hlc;
    use crate::sync::membership::MembershipAction;
    use crate::sync::test_helpers::{
        append_membership_entry, founder_entry, make_linked_entry, pubkey_hex, MockSyncStorage,
        TestCustody,
    };
    use std::sync::RwLock;

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
    async fn inviting_yourself_is_a_typed_self_invite_error() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let hlc = Hlc::new("f".to_string());

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
        )
        .await;
        assert!(matches!(result, Err(MembershipOpsError::NoFounderChain)));
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
        )
        .await
        .expect("remove");

        assert_eq!(storage.membership_list_count(), 2);
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
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

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
        )
        .await
        .expect("remove completes even though the home cannot revoke the credential");

        // The Remove entry was published, so the removed member is no longer a
        // current member of the reloaded chain.
        let visible = storage.list_membership_entries().await.unwrap();
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

        // The LIST omits seq 2, but the keyed GET still serves it.
        storage.hide_membership_from_listing(&owner_pk, 2);
        let visible = storage.list_membership_entries().await.unwrap();

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
        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk), None)
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
        let storage = MockSyncStorage::new();
        let founder_pk = pubkey_hex(&founder);
        let second_pk = pubkey_hex(&second_owner);

        // Founder's prefix: found, promote second_owner to Owner, add member.
        let mut chain = MembershipChain::new();
        let f = founder_entry(&founder, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &founder_pk, 1, f).await;
        let add_owner = make_linked_entry(
            &chain,
            &founder,
            MembershipAction::Add,
            &second_owner,
            MemberRole::Owner,
            "0000000002000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &founder_pk, 2, add_owner).await;
        let add_member = make_linked_entry(
            &chain,
            &founder,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000003000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &founder_pk, 3, add_member).await;

        // Concurrent, independent commits: the founder invites `invitee` at
        // founder/4 while second_owner removes `member` at second_owner/1, each
        // publishing only its own head.
        let add_invitee = make_linked_entry(
            &chain,
            &founder,
            MembershipAction::Add,
            &invitee,
            MemberRole::Member,
            "0000000004000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &founder_pk, 4, add_invitee).await;
        let remove_member = make_linked_entry(
            &chain,
            &second_owner,
            MembershipAction::Remove,
            &member,
            MemberRole::Member,
            "0000000005000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &second_pk, 1, remove_member).await;

        publish_membership_head(&storage, &chain, &founder)
            .await
            .unwrap();
        publish_membership_head(&storage, &chain, &second_owner)
            .await
            .unwrap();

        // A third reader loads the union: invitee added AND member removed.
        let visible = storage.list_membership_entries().await.unwrap();
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

        persist_head_watermark(&db, author, 10).await.unwrap();
        persist_head_watermark(&db, author, 9).await.unwrap();
        assert_eq!(
            read_head_watermark(&db, author).await.unwrap(),
            Some(10),
            "an out-of-order lower persist must not regress the stored watermark",
        );

        persist_head_watermark(&db, author, 11).await.unwrap();
        assert_eq!(
            read_head_watermark(&db, author).await.unwrap(),
            Some(11),
            "a higher persist still advances it",
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
        let storage = MockSyncStorage::new();
        let db = open_test_db();
        let owner_pk = pubkey_hex(&owner);

        let mut chain = MembershipChain::new();
        let f = founder_entry(&owner, "0000000001000-0000-f");
        append_membership_entry(&storage, &mut chain, &owner_pk, 1, f).await;
        let add = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 2, add.clone()).await;
        let remove = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Remove,
            &member,
            MemberRole::Member,
            "0000000003000-0000-f",
        );
        append_membership_entry(&storage, &mut chain, &owner_pk, 3, remove).await;
        publish_membership_head(&storage, &chain, &owner)
            .await
            .unwrap();

        // The reader accepts the head at seq 3: member removed, watermark now 3.
        let visible = storage.list_membership_entries().await.unwrap();
        let loaded = load_anchored_chain(&storage, &visible, Some(&owner_pk), Some(&db))
            .await
            .unwrap();
        assert!(!loaded
            .current_members()
            .iter()
            .any(|(pk, _)| pk == &pubkey_hex(&member)));

        // A stale read serves the head from before the Remove (seq 2). The reader
        // refuses to regress rather than re-admit the member.
        let stale = AuthorHead::signed(2, entry_hash(&add), &owner);
        storage
            .put_membership_head(&owner_pk, serde_json::to_vec(&stale).unwrap())
            .await
            .unwrap();
        let result = load_anchored_chain(&storage, &visible, Some(&owner_pk), Some(&db)).await;
        assert!(
            result.is_err(),
            "a head regressing below the accepted watermark must be refused"
        );
    }
}
