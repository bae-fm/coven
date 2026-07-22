use crate::encryption::EncryptionService;
use crate::keys::{self, UserKeypair};
use crate::sync::membership::MembershipHeadRef;
use crate::sync::storage::{StorageError, SyncStorage};
use crate::sync::store::membership::{load_exact_anchored_chain, validate_membership_floor};
use crate::sync::store_commit::{ObjectHash, StoreRootRef};
use crate::sync::wrapped_store_key::{load_wrapped_store_key, WrappedStoreKey, WrappedStoreKeyRef};

use super::InviteError;

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

pub async fn unwrap_store_keyring(
    bootstrap_storage: &dyn SyncStorage,
    keypair: &UserKeypair,
    store_root: &StoreRootRef,
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
    validate_membership_floor(membership_floor).map_err(InviteError::Crypto)?;
    let chain = load_exact_anchored_chain(
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
    store_root_hash: ObjectHash,
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
    store_root_hash: ObjectHash,
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
