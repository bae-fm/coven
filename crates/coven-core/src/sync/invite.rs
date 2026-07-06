use crate::encryption::{self, EncryptionService};
use crate::keys::{self, KeyError, UserKeypair};
/// Invitation and revocation flow for shared library membership.
///
/// `create_invitation()` is called by the library owner to invite a new member.
/// `unwrap_library_key()` is called by the invitee to unwrap the library key.
/// `revoke_member()` is called by the library owner to remove a member and rotate the key.
use crate::storage::cloud::{
    CloudAccessGrant, CloudAccessRevoke, CloudHome, CloudHomeError, CloudHomeJoinInfo,
};

use super::membership::{
    sign_membership_entry, MemberRole, MembershipAction, MembershipChain, MembershipEntry,
    MembershipError,
};
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

/// Determine the next seq for an author's membership entries from a listed key set.
fn next_membership_seq(entry_keys: &[(String, u64)], author_pubkey_hex: &str) -> u64 {
    entry_keys
        .iter()
        .filter(|(author, _)| author == author_pubkey_hex)
        .map(|(_, seq)| seq)
        .max()
        .map_or(1, |max| max + 1)
}

/// Decode and convert an Ed25519 hex pubkey to X25519 for sealed box encryption.
fn ed25519_hex_to_x25519(
    ed25519_pubkey_hex: &str,
) -> Result<[u8; keys::CURVE25519_PUBLICKEYBYTES], InviteError> {
    let pk_bytes: [u8; keys::SIGN_PUBLICKEYBYTES] = hex::decode(ed25519_pubkey_hex)
        .map_err(|e| InviteError::Crypto(format!("invalid pubkey hex: {e}")))?
        .try_into()
        .map_err(|_| InviteError::Crypto("pubkey wrong length".to_string()))?;
    Ok(keys::ed25519_to_x25519_public_key(&pk_bytes))
}

/// Seal the library key to one member and wrap it in an owner-signed
/// [`WrappedLibraryKey`], serialized to the bytes stored at
/// `keys/{recipient_pubkey}`. The signature binds `(library_id,
/// recipient_pubkey, sealed)` so the joiner can prove the key came from the
/// owner and was meant for them, not substituted by a bucket writer.
///
/// `owner_keypair` is whatever Owner is performing the invite/revoke — NOT
/// necessarily the chain founder. The two callers below pass the local device's
/// own keypair, and the membership chain authorizes any current Owner to add or
/// remove members, so a second Owner can reach here and sign with their own key.
///
/// But a joining (or rotating) device pins exactly ONE clear-text authority: the
/// founder the invite carries (`InviteCode::owner_pubkey`, set from
/// `chain.founder_pubkey()`), because the joiner has no membership chain yet — it
/// is bootstrapping into the library and the chain itself is sealed under the very
/// key it is trying to adopt. [`WrappedLibraryKey::verify_and_unwrap`] therefore
/// checks the signature against that founder and nothing else. So a wrapped key an
/// adopting device accepts MUST be signed by the founder; one signed by a
/// non-founder Owner fails that check and the join/rotation fails closed (a loud
/// [`InviteError`], never a silent adoption of the wrong key). The practical
/// limitation: only the founder can issue or rotate library keys that a fresh
/// device will accept. A non-founder Owner can still author membership-chain
/// changes that existing members honor; what they cannot do is hand a brand-new
/// device a key it will adopt. Widening that would mean giving the joiner more
/// than one pinned authority — a multi-owner trust shape this code does not have.
fn signed_wrapped_key(
    library_id: &str,
    recipient_ed25519_pubkey: &str,
    recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
    encryption: &EncryptionService,
    owner_keypair: &UserKeypair,
) -> Result<Vec<u8>, InviteError> {
    let payload = encryption
        .to_keyring_payload()
        .map_err(|e| InviteError::Crypto(format!("serialize keyring payload: {e}")))?;
    let sealed = keys::seal_box_encrypt(&payload, recipient_x25519_pk);
    let wrapped =
        WrappedLibraryKey::signed(library_id, recipient_ed25519_pubkey, sealed, owner_keypair);
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
    signed_wrapped_key(
        library_id,
        recipient_ed25519_pubkey,
        recipient_x25519_pk,
        &EncryptionService::from_key(*encryption_key),
        owner_keypair,
    )
    .expect("signed wrapped key")
}

/// Upload a signed membership entry to the storage.
async fn upload_membership_entry(
    storage: &dyn SyncStorage,
    entry_keys: &[(String, u64)],
    entry: &MembershipEntry,
    author_pubkey_hex: &str,
) -> Result<(), InviteError> {
    let next_seq = next_membership_seq(entry_keys, author_pubkey_hex);

    let entry_bytes =
        serde_json::to_vec(entry).map_err(|e| InviteError::Crypto(format!("serialize: {e}")))?;
    storage
        .put_membership_entry(author_pubkey_hex, next_seq, entry_bytes)
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
    entry_keys: Vec<(String, u64)>,
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
        entry_keys,
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
    entry_keys: Vec<(String, u64)>,
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
    // `owner_keypair` must be the founder's (see `signed_wrapped_key`); a
    // non-founder Owner's invite fails closed at the joiner.
    let wrapped_key = signed_wrapped_key(
        library_id,
        invitee_ed25519_pubkey,
        &invitee_x25519_pk,
        encryption,
        owner_keypair,
    )?;

    // Create and sign a membership entry.
    let mut entry = MembershipEntry {
        action: MembershipAction::Add,
        user_pubkey: invitee_ed25519_pubkey.to_string(),
        provider_account_email: invitee_email.map(str::to_string),
        role,
        timestamp: timestamp.to_string(),
        author_pubkey: String::new(),
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner_keypair);

    // Validate against the local chain before any provider or storage mutation.
    let mut validated_chain = chain.clone();
    validated_chain.add_entry(entry.clone())?;

    let grant = CloudAccessGrant {
        member_pubkey: invitee_ed25519_pubkey.to_string(),
        provider_account_email: invitee_email.map(str::to_string),
    };
    let revoke = CloudAccessRevoke {
        member_pubkey: grant.member_pubkey.clone(),
        provider_account_email: grant.provider_account_email.clone(),
    };
    let join_info = cloud_home.grant_access(grant).await?;

    // Upload wrapped key and membership entry.
    if let Err(original) = storage
        .put_wrapped_key(invitee_ed25519_pubkey, wrapped_key)
        .await
    {
        if let Err(rollback) = cloud_home.revoke_access(revoke).await {
            return Err(InviteError::Rollback {
                operation: "upload wrapped key",
                original: original.to_string(),
                rollback: rollback.to_string(),
            });
        }
        return Err(original.into());
    }

    let author_pubkey_hex = hex::encode(owner_keypair.public_key());
    if let Err(original) =
        upload_membership_entry(storage, &entry_keys, &entry, &author_pubkey_hex).await
    {
        let mut rollback_errors = Vec::new();
        if let Err(rollback) = storage.delete_wrapped_key(invitee_ed25519_pubkey).await {
            rollback_errors.push(rollback.to_string());
        }
        if let Err(rollback) = cloud_home.revoke_access(revoke).await {
            rollback_errors.push(rollback.to_string());
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

    *chain = validated_chain;

    Ok(join_info)
}

/// Accept an invitation by downloading, authenticating, and unwrapping the
/// library encryption key.
///
/// The invitee calls this after receiving an invitation. It downloads the
/// wrapped key from cloud storage, verifies the owner signed it for this
/// recipient in this library, and only then decrypts it with the invitee's
/// X25519 keys.
///
/// `expected_owner` is the library owner the invite pins (the chain founder).
/// The bucket is writable by every member and anyone with the bucket
/// credential, and a sealed box authenticates only its recipient — so without
/// this check a bucket writer could overwrite the object with a box wrapping a
/// key of their choosing and the joiner would adopt it. Verifying the owner's
/// signature over `(library_id, recipient_pubkey, sealed)` rejects any such
/// substitution.
pub async fn unwrap_library_key(
    cloud_home: &dyn CloudHome,
    keypair: &UserKeypair,
    library_id: &str,
    expected_owner: &str,
) -> Result<[u8; 32], InviteError> {
    Ok(
        unwrap_library_keyring(cloud_home, keypair, library_id, expected_owner)
            .await?
            .key_bytes(),
    )
}

pub async fn unwrap_library_keyring(
    cloud_home: &dyn CloudHome,
    keypair: &UserKeypair,
    library_id: &str,
    expected_owner: &str,
) -> Result<EncryptionService, InviteError> {
    let pubkey_hex = hex::encode(keypair.public_key());

    // Download the wrapped key directly off the cloud home (not through
    // `CloudSyncStorage`, which the joiner hasn't built yet). The `.enc` suffix is
    // hardcoded because joining a shared library is an encrypted-home-only path —
    // the invite wraps the library key — so `CloudSyncStorage::put_wrapped_key`
    // always wrote it under the encrypted-home key (`keys/{pubkey}.enc`).
    let key_path = format!("keys/{pubkey_hex}.enc");
    let wrapped_bytes = cloud_home.read(&key_path).await?;

    let wrapped: WrappedLibraryKey = serde_json::from_slice(&wrapped_bytes)
        .map_err(|e| InviteError::Crypto(format!("malformed wrapped key: {e}")))?;

    // Authenticate the wrapped key against the owner the invite pins before
    // adopting anything. A failure here is a substituted, forged, or relocated
    // key, or a corrupt object — refuse it loudly, surfacing which, rather than
    // decrypt whatever bytes are present.
    let sealed = wrapped
        .verify_and_unwrap(library_id, &pubkey_hex, expected_owner)
        .map_err(|e| InviteError::Crypto(format!("wrapped library key: {e}")))?;

    // Decrypt with our X25519 secret key.
    let x25519_sk = keypair.to_x25519_secret_key();
    let plaintext = keys::seal_box_decrypt(&sealed, &x25519_sk)?;
    EncryptionService::from_keyring_payload(plaintext)
        .map_err(|e| InviteError::Crypto(format!("keyring payload: {e}")))
}

/// Revoke a member from the library. This:
/// 1. Revokes access on the cloud home
/// 2. Creates a Remove membership entry signed by the owner
/// 3. Generates a new library encryption key
/// 4. Re-wraps the new key to all remaining members
/// 5. Deletes the revoked member's wrapped key
/// 6. Uploads updated entries and keys
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

    // Verify the revokee is a current member.
    if !members.iter().any(|(pk, _)| pk == revokee_pubkey) {
        return Err(InviteError::NotAMember(revokee_pubkey.to_string()));
    }

    // Ensure at least one owner would remain after the removal.
    let remaining_owners = members
        .iter()
        .filter(|(pk, role)| pk != revokee_pubkey && *role == MemberRole::Owner)
        .count();
    if remaining_owners == 0 {
        return Err(InviteError::LastOwner);
    }

    // Revoke access on the cloud home (no-op for S3, removes share for consumer clouds).
    let provider_account_email = chain
        .current_member_provider_email(revokee_pubkey)
        .map(str::to_string);
    cloud_home
        .revoke_access(CloudAccessRevoke {
            member_pubkey: revokee_pubkey.to_string(),
            provider_account_email,
        })
        .await?;

    // Create and sign a Remove entry.
    let mut entry = MembershipEntry {
        action: MembershipAction::Remove,
        user_pubkey: revokee_pubkey.to_string(),
        provider_account_email: None,
        role: MemberRole::Member, // role field is not meaningful for Remove, but required
        timestamp: timestamp.to_string(),
        author_pubkey: String::new(),
        signature: String::new(),
    };
    sign_membership_entry(&mut entry, owner_keypair);

    // Validate against the local chain BEFORE any storage writes.
    chain.add_entry(entry.clone())?;

    // Upload the Remove entry.
    let author_pubkey_hex = hex::encode(owner_keypair.public_key());
    upload_membership_entry(storage, &entry_keys, &entry, &author_pubkey_hex).await?;

    // Generate a new random encryption key.
    let new_key = encryption::generate_random_key();
    let new_generation = current_encryption.current_generation() + 1;
    let new_keyring = current_encryption
        .with_appended_generation(new_generation, new_key)
        .map_err(|e| InviteError::Crypto(format!("append key generation: {e}")))?;

    // Re-wrap the new key to all remaining members, each signed so a joiner that
    // later adopts it can authenticate it the same way an invite's key is. As at
    // invite time, an adopting device verifies against the founder the invite
    // pinned, so `owner_keypair` must be the founder's for the rotated key to be
    // adoptable on a fresh device (see `signed_wrapped_key`); a non-founder Owner's
    // rotation re-wraps keys no joiner will accept (it fails closed, not silently).
    let remaining_members = chain.current_members();
    for (member_pubkey, _) in &remaining_members {
        let x25519_pk = ed25519_hex_to_x25519(member_pubkey)?;
        let wrapped = signed_wrapped_key(
            library_id,
            member_pubkey,
            &x25519_pk,
            &new_keyring,
            owner_keypair,
        )?;
        storage.put_wrapped_key(member_pubkey, wrapped).await?;
    }

    // Delete the revoked member's wrapped key.
    storage.delete_wrapped_key(revokee_pubkey).await?;

    Ok(new_keyring)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
    use crate::sync::membership::MemberRole;
    use crate::sync::test_helpers::{bootstrap_chain, pubkey_hex, MockSyncStorage};
    use async_trait::async_trait;

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
            Err(CloudHomeError::Storage("mock has no multipart".to_string()))
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
        async fn revoke_access(&self, _revoke: CloudAccessRevoke) -> Result<(), CloudHomeError> {
            Ok(())
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
            Err(CloudHomeError::Storage("mock has no multipart".to_string()))
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
        async fn revoke_access(&self, revoke: CloudAccessRevoke) -> Result<(), CloudHomeError> {
            self.revokes.lock().unwrap().push(revoke);
            Ok(())
        }
    }

    fn gen_keypair() -> UserKeypair {
        UserKeypair::generate()
    }

    /// The library id every invite test wraps keys under. The wrapped-key
    /// signature binds it, so the same id must be passed to `unwrap_library_key`.
    const LIB_ID: &str = "lib-test";

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
            storage.list_membership_entries().await.unwrap(),
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
        // owner the invite pins, then adopted.
        let unwrapped = unwrap_library_key(
            &storage as &dyn CloudHome,
            &invitee,
            LIB_ID,
            &pubkey_hex(&owner),
        )
        .await
        .unwrap();
        assert_eq!(unwrapped, encryption_key);
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
            .put_wrapped_key(&pubkey_hex(&joiner), forged)
            .await
            .unwrap();

        // The joiner adopts only keys signed by the owner the invite pins.
        let result = unwrap_library_key(
            &storage as &dyn CloudHome,
            &joiner,
            LIB_ID,
            &pubkey_hex(&owner),
        )
        .await;
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
            .put_wrapped_key(&pubkey_hex(&member_b), for_a)
            .await
            .unwrap();

        // Member B reads its slot; the signature is over A's pubkey, so it fails.
        let result = unwrap_library_key(
            &storage as &dyn CloudHome,
            &member_b,
            LIB_ID,
            &pubkey_hex(&owner),
        )
        .await;
        assert!(
            matches!(result, Err(InviteError::Crypto(_))),
            "a key bound to another member's slot must be refused, got {result:?}",
        );
    }

    /// A NON-founder Owner can legitimately invite (the membership chain
    /// authorizes any current Owner), but `create_invitation` signs the wrapped
    /// key with that inviting Owner's own key, while the invitee pins the founder.
    /// So the invitee cannot adopt a second Owner's key: verification is against
    /// the founder and fails closed. This is the founder-only key-issuance limit,
    /// asserted to fail loudly (an `InviteError`) rather than silently adopt a key
    /// signed by the wrong authority.
    #[tokio::test]
    async fn second_owner_invite_is_unadoptable_by_the_joiner() {
        use crate::sync::membership::MembershipAction;
        use crate::sync::test_helpers::make_entry;

        let founder = gen_keypair();
        let second_owner = gen_keypair();
        let invitee = gen_keypair();
        let encryption_key: [u8; 32] = [3u8; 32];

        let storage = MockSyncStorage::new();

        // Chain: founder, then the founder promotes `second_owner` to Owner.
        let mut chain = bootstrap_chain(&founder);
        chain
            .add_entry(make_entry(
                &founder,
                MembershipAction::Add,
                &second_owner,
                MemberRole::Owner,
                "0000000002000-0000-dev1",
            ))
            .unwrap();

        // The SECOND owner invites the new member. This succeeds — the chain
        // authorizes any Owner to add — and signs the wrapped key with the second
        // owner's key.
        create_invitation(
            &storage,
            &MockCloudHome,
            &mut chain,
            storage.list_membership_entries().await.unwrap(),
            &second_owner,
            &pubkey_hex(&invitee),
            None,
            MemberRole::Member,
            &encryption_key,
            LIB_ID,
            "0000000003000-0000-dev1",
        )
        .await
        .unwrap();

        // The invitee pins the founder (what the invite carries). The wrapped key
        // is signed by the second owner, so verification against the founder fails
        // and the join fails CLOSED — no silent adoption of the wrong key.
        let result = unwrap_library_key(
            &storage as &dyn CloudHome,
            &invitee,
            LIB_ID,
            &pubkey_hex(&founder),
        )
        .await;
        assert!(
            matches!(result, Err(InviteError::Crypto(_))),
            "a non-founder owner's wrapped key must not be adoptable by a joiner pinning the founder, got {result:?}",
        );

        // It is specifically the founder constraint, not a broken invite: had the
        // joiner instead pinned the second owner (the actual signer), the same
        // wrapped key would verify and adopt. (A device never does this — it only
        // ever pins the founder — but it isolates the cause to the pinned authority.)
        let adopted = unwrap_library_key(
            &storage as &dyn CloudHome,
            &invitee,
            LIB_ID,
            &pubkey_hex(&second_owner),
        )
        .await
        .unwrap();
        assert_eq!(adopted, encryption_key);
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
            storage.list_membership_entries().await.unwrap(),
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
        let result = unwrap_library_key(
            &storage as &dyn CloudHome,
            &wrong_keypair,
            LIB_ID,
            &pubkey_hex(&owner),
        )
        .await;
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
            .get_wrapped_key(&pubkey_hex(&invitee))
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
            storage.list_membership_entries().await.unwrap(),
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
        let unwrapped = unwrap_library_key(
            &storage as &dyn CloudHome,
            &member,
            LIB_ID,
            &pubkey_hex(&owner),
        )
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
        let result = storage.get_wrapped_key(&pubkey_hex(&member)).await;
        assert!(result.is_err());

        // Owner can still unwrap the new key.
        let owner_unwrapped = unwrap_library_key(
            &storage as &dyn CloudHome,
            &owner,
            LIB_ID,
            &pubkey_hex(&owner),
        )
        .await
        .unwrap();
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
        let owner_key = unwrap_library_key(
            &storage as &dyn CloudHome,
            &owner,
            LIB_ID,
            &pubkey_hex(&owner),
        )
        .await
        .unwrap();
        assert_eq!(owner_key, new_key.key_bytes());

        let member2_key = unwrap_library_key(
            &storage as &dyn CloudHome,
            &member2,
            LIB_ID,
            &pubkey_hex(&owner),
        )
        .await
        .unwrap();
        assert_eq!(member2_key, new_key.key_bytes());

        // member1 cannot get a wrapped key.
        let result = storage.get_wrapped_key(&pubkey_hex(&member1)).await;
        assert!(result.is_err());
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
            storage.list_membership_entries().await.unwrap(),
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
}
