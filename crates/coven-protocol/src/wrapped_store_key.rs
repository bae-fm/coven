//! Owner-signed wrapped store keys.

use serde::{Deserialize, Serialize};

use crate::objects::{ExactObjectRef, PreparedExactObject, StorageError};
use crate::store_commit::ObjectHash;
use coven_keys::keys::{self, UserKeypair};

/// A Store encryption keyring sealed to one member and signed by an Owner.
///
/// Membership authority names the immutable exact object
/// through [`WrappedStoreKeyRef`]. The sealed box authenticates no sender, so
/// the Owner signature additionally binds the Store, recipient, generation,
/// author, and sealed bytes. The reader verifies both the exact reference and
/// that signature before opening the keyring.
///
/// `recipient_pubkey` is part of the signed payload and exact path rather than
/// duplicated in this value, so a wrap cannot be relocated to another member.
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WrappedStoreKey {
    /// Hex-encoded Ed25519 public key of the Owner that signed this wrapped key.
    pub author_pubkey: String,
    /// The keyring's current generation, covered by the Owner signature and the
    /// exact reference.
    pub generation: u64,
    /// Hex-encoded sealed box (`seal_box_encrypt` output) carrying the store key.
    pub sealed: String,
    /// Hex-encoded detached signature over `WrappedKeyFields`, produced by the owner.
    pub signature: String,
}

/// The wrapped-key fields the signature covers, in declaration order. Excludes
/// `signature` (the signature's own output). Includes `store_id` (so a key
/// can't be replayed into a different store) and `recipient_pubkey` (the slot,
/// so a key can't be relocated to another member).
#[derive(Serialize)]
struct WrappedKeyFields<'a> {
    store_id: &'a str,
    recipient_pubkey: &'a str,
    generation: u64,
    author_pubkey: &'a str,
    sealed: &'a str,
}

/// Why a [`WrappedStoreKey`] could not be authenticated and unwrapped. Named
/// per reason so the caller can surface *why* an adoption was refused — a
/// substituted/forged key (the signature does not verify against the pinned
/// owner) is distinct from a corrupt object (the sealed box is not valid hex) —
/// rather than collapsing both into one opaque failure.
#[derive(Debug, thiserror::Error)]
pub enum WrappedKeyError {
    /// The signature does not verify against an authorized Owner over
    /// `(store_id, recipient_pubkey, author_pubkey, sealed)`. Covers a box
    /// signed by anyone outside the authorized set, a payload tampered after
    /// signing (different store, slot, author, or sealed bytes), and a
    /// malformed signature or owner pubkey — all indistinguishable here and all
    /// meaning "not authentically signed by an authorized Owner".
    #[error("signature does not verify against an authorized store owner")]
    SignatureMismatch,
    /// The signature verified, but the sealed-box field is not valid hex, so
    /// there are no bytes to decrypt — a corrupt object, not an attack.
    #[error("sealed box is not valid hex")]
    MalformedSealed,
}

#[derive(Debug, thiserror::Error)]
pub enum WrappedKeyringError {
    #[error("{0}")]
    Authentication(#[from] WrappedKeyError),
    #[error("decrypt sealed Store keyring: {0}")]
    Decryption(#[from] coven_keys::keys::KeyError),
    #[error("decode Store keyring payload: {0}")]
    Payload(#[from] coven_keys::encryption::EncryptionError),
    #[error(
        "wrapped Store-key ref declares generation {reference}, but its keyring declares {payload}"
    )]
    GenerationMismatch { reference: u64, payload: u64 },
}

impl WrappedStoreKey {
    pub fn seal_keyring(
        store_id: &str,
        recipient_pubkey: &str,
        recipient_x25519_pk: &[u8; keys::CURVE25519_PUBLICKEYBYTES],
        encryption: &coven_keys::encryption::EncryptionService,
        owner: &UserKeypair,
    ) -> Result<Self, coven_keys::encryption::EncryptionError> {
        let payload = encryption.to_keyring_payload()?;
        Ok(Self::signed(
            store_id,
            recipient_pubkey,
            encryption.current_generation(),
            keys::seal_box_encrypt(&payload, recipient_x25519_pk),
            owner,
        ))
    }

    /// Wrap `sealed` (a sealed box of the store key, already encrypted to
    /// `recipient_pubkey`) and sign the binding with `owner`: fills `signature`
    /// with the owner's detached signature over the canonical payload.
    pub fn signed(
        store_id: &str,
        recipient_pubkey: &str,
        generation: u64,
        sealed: Vec<u8>,
        owner: &UserKeypair,
    ) -> Self {
        let author_pubkey = hex::encode(owner.public_key());
        let sealed_hex = hex::encode(sealed);
        let payload = wrapped_key_signing_payload(
            store_id,
            recipient_pubkey,
            generation,
            &author_pubkey,
            &sealed_hex,
        );
        let (_, signature) = keys::sign_hex(owner, &payload);
        WrappedStoreKey {
            author_pubkey,
            generation,
            sealed: sealed_hex,
            signature,
        }
    }

    /// Verify this wrapped key was authentically produced by one of
    /// `expected_owners` for `recipient_pubkey` in `store_id`, and return the
    /// sealed-box bytes to decrypt. Verifies the signature against the authorized
    /// Owner set for this context over the binding `(store_id,
    /// recipient_pubkey, author_pubkey, sealed)`. Fails closed, naming why, if the
    /// signature doesn't verify against that set (a substituted, forged,
    /// replayed, or relocated key) or the sealed box is malformed; neither must
    /// be adopted.
    pub fn verify_and_unwrap<'a>(
        &self,
        store_id: &str,
        recipient_pubkey: &str,
        expected_owners: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<u8>, WrappedKeyError> {
        let payload = wrapped_key_signing_payload(
            store_id,
            recipient_pubkey,
            self.generation,
            &self.author_pubkey,
            &self.sealed,
        );
        // Any way this fails to verify against the named authorized owner is one
        // outcome: not authentically an authorized key, refuse it.
        if !expected_owners
            .into_iter()
            .any(|owner| owner == self.author_pubkey)
            || !keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
        {
            return Err(WrappedKeyError::SignatureMismatch);
        }
        // Verified as the owner's bytes; an un-decodable sealed field is a corrupt
        // object, a distinct failure.
        hex::decode(&self.sealed).map_err(|_| WrappedKeyError::MalformedSealed)
    }

    pub fn verify_and_open_keyring<'a>(
        &self,
        store_id: &str,
        recipient_pubkey: &str,
        expected_owners: impl IntoIterator<Item = &'a str>,
        expected_generation: u64,
        recipient: &dyn coven_keys::keys::IdentityKeyAuthority,
    ) -> Result<coven_keys::encryption::EncryptionService, WrappedKeyringError> {
        let sealed = self.verify_and_unwrap(store_id, recipient_pubkey, expected_owners)?;
        let plaintext = keys::seal_box_decrypt(&sealed, &recipient.to_x25519_secret_key())?;
        let keyring = coven_keys::encryption::EncryptionService::from_keyring_payload(plaintext)?;
        if keyring.current_generation() != expected_generation {
            return Err(WrappedKeyringError::GenerationMismatch {
                reference: expected_generation,
                payload: keyring.current_generation(),
            });
        }
        Ok(keyring)
    }
}

fn wrapped_key_signing_payload(
    store_id: &str,
    recipient_pubkey: &str,
    generation: u64,
    author_pubkey: &str,
    sealed_hex: &str,
) -> Vec<u8> {
    let fields = WrappedKeyFields {
        store_id,
        recipient_pubkey,
        generation,
        author_pubkey,
        sealed: sealed_hex,
    };
    serde_json::to_vec(&fields).expect("wrapped key fields serialization cannot fail")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct WrappedStoreKeyRef {
    pub owner_pubkey: String,
    pub recipient_pubkey: String,
    pub generation: u64,
    pub wrap_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl WrappedStoreKeyRef {
    pub fn semantic_prefix(&self) -> String {
        format!(
            "keys/{}/{}/{}/{}",
            self.owner_pubkey, self.recipient_pubkey, self.generation, self.wrap_hash
        )
    }

    pub fn validate_identity(&self) -> Result<(), StorageError> {
        for pubkey in [&self.owner_pubkey, &self.recipient_pubkey] {
            coven_foundation::store_dir::validate_path_token(pubkey)?;
            let bytes = hex::decode(pubkey).map_err(|_| {
                StorageError::InvalidContent(
                    "wrapped Store-key ref contains an invalid public key".to_string(),
                )
            })?;
            if bytes.len() != keys::SIGN_PUBLICKEYBYTES || hex::encode(&bytes) != *pubkey {
                return Err(StorageError::InvalidContent(
                    "wrapped Store-key ref contains an invalid public key".to_string(),
                ));
            }
        }
        if self.generation == 0
            || self.object.slot().logical_key() != format!("{}.json", self.semantic_prefix())
        {
            return Err(StorageError::InvalidContent(
                "wrapped Store-key ref has an invalid semantic identity".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_value(
        &self,
        value: &WrappedStoreKey,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.validate_identity()?;
        if value.author_pubkey != self.owner_pubkey
            || value.generation != self.generation
            || ObjectHash::digest(bytes) != self.wrap_hash
        {
            return Err(StorageError::InvalidContent(
                "wrapped Store-key ref does not match its exact value".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedWrappedStoreKey {
    pub reference: WrappedStoreKeyRef,
    pub object: PreparedExactObject,
}

impl PreparedWrappedStoreKey {
    pub fn validate(&self) -> Result<WrappedStoreKey, StorageError> {
        if self.reference.object != *self.object.reference() {
            return Err(StorageError::InvalidContent(
                "prepared wrapped Store key carries a different exact reference".to_string(),
            ));
        }
        let value: WrappedStoreKey = serde_json::from_slice(self.object.stored_bytes())?;
        self.reference
            .validate_value(&value, self.object.stored_bytes())?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapped_key_round_trips_and_returns_sealed_bytes() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key());
        let sealed = vec![1u8, 2, 3, 4, 5];
        let wrapped = WrappedStoreKey::signed("lib", "recipient-pk", 1, sealed.clone(), &owner);

        // Round-trips through JSON and yields the sealed bytes back.
        let json = serde_json::to_vec(&wrapped).expect("serialize wrapped key");
        let parsed: WrappedStoreKey = serde_json::from_slice(&json).expect("parse wrapped key");
        assert_eq!(
            parsed
                .verify_and_unwrap("lib", "recipient-pk", std::iter::once(owner_hex.as_str()))
                .unwrap(),
            sealed,
        );
    }

    #[test]
    fn wrapped_key_signed_by_non_owner_is_refused() {
        // The object is signed by some key, but the joiner verifies against the
        // owner it pins (the chain founder). A box the owner did not sign — here
        // signed by a different key, the shape of a bucket writer substituting an
        // attacker-chosen key — fails to verify against that owner and is refused.
        let signer = UserKeypair::generate();
        let pinned_owner = UserKeypair::generate();
        let pinned_owner_hex = hex::encode(pinned_owner.public_key());
        let sealed = vec![9u8; 32];
        let wrapped = WrappedStoreKey::signed("lib", "recipient-pk", 1, sealed, &signer);

        assert!(
            matches!(
                wrapped.verify_and_unwrap(
                    "lib",
                    "recipient-pk",
                    std::iter::once(pinned_owner_hex.as_str())
                ),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "a key not signed by the pinned owner must be refused",
        );
    }

    #[test]
    fn wrapped_key_rejects_rebinding() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key());
        let sealed = vec![9u8; 32];
        let wrapped = WrappedStoreKey::signed("lib", "recipient-pk", 1, sealed, &owner);

        // The signature binds the store and the recipient slot: changing either
        // at verify time fails, so a key can't be replayed cross-store or
        // relocated to another member's slot.
        assert!(
            matches!(
                wrapped.verify_and_unwrap(
                    "other-lib",
                    "recipient-pk",
                    std::iter::once(owner_hex.as_str())
                ),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "must reject a key replayed into a different store",
        );
        assert!(
            matches!(
                wrapped.verify_and_unwrap(
                    "lib",
                    "other-recipient",
                    std::iter::once(owner_hex.as_str())
                ),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "must reject a key relocated to another recipient's slot",
        );
    }

    #[test]
    fn wrapped_key_rejects_author_tamper() {
        let owner = UserKeypair::generate();
        let other_owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key());
        let other_owner_hex = hex::encode(other_owner.public_key());
        let sealed = vec![9u8; 32];
        let mut wrapped = WrappedStoreKey::signed("lib", "recipient-pk", 1, sealed, &owner);
        assert!(
            wrapped
                .verify_and_unwrap("lib", "recipient-pk", std::iter::once(owner_hex.as_str()))
                .is_ok(),
            "freshly signed author verifies",
        );

        wrapped.author_pubkey = other_owner_hex.clone();
        assert!(
            matches!(
                wrapped.verify_and_unwrap(
                    "lib",
                    "recipient-pk",
                    [owner_hex.as_str(), other_owner_hex.as_str()]
                ),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "tampering with the named author invalidates the signature",
        );
    }

    #[test]
    fn wrapped_key_rejects_generation_tamper() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key());
        let mut wrapped = WrappedStoreKey::signed("lib", "recipient-pk", 3, vec![9u8; 32], &owner);

        // The generation is covered by the signature, so a bucket writer cannot
        // raise it to wedge a member into pausing under a generation the owner
        // never committed.
        wrapped.generation = 99;
        assert!(
            matches!(
                wrapped.verify_and_unwrap(
                    "lib",
                    "recipient-pk",
                    std::iter::once(owner_hex.as_str())
                ),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "tampering with the claimed generation invalidates the signature",
        );
    }

    #[test]
    fn wrapped_key_malformed_signature_fails_closed() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key());
        let mut wrapped = WrappedStoreKey::signed("lib", "recipient-pk", 1, vec![1u8; 4], &owner);

        // A signature that isn't valid hex can't verify against the owner.
        wrapped.signature = "not-hex!!".to_string();
        assert!(matches!(
            wrapped.verify_and_unwrap("lib", "recipient-pk", std::iter::once(owner_hex.as_str())),
            Err(WrappedKeyError::SignatureMismatch),
        ));
    }

    #[test]
    fn wrapped_key_malformed_sealed_is_distinguished() {
        // A correctly owner-signed object whose sealed field is not valid hex:
        // the signature verifies (it is taken over the malformed bytes), but
        // there is nothing to decrypt. This is a corrupt object, surfaced as a
        // reason distinct from a signature mismatch.
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key());

        let mut wrapped = WrappedStoreKey {
            author_pubkey: owner_hex.clone(),
            generation: 1,
            sealed: "not-hex!!".to_string(),
            signature: String::new(),
        };
        let payload = wrapped_key_signing_payload(
            "lib",
            "recipient-pk",
            wrapped.generation,
            &wrapped.author_pubkey,
            &wrapped.sealed,
        );
        let (_, signature) = keys::sign_hex(&owner, &payload);
        wrapped.signature = signature;

        assert!(matches!(
            wrapped.verify_and_unwrap("lib", "recipient-pk", std::iter::once(owner_hex.as_str())),
            Err(WrappedKeyError::MalformedSealed),
        ));
    }

    #[test]
    fn wrapped_ref_generation_must_match_its_decrypted_keyring() {
        let owner = UserKeypair::generate();
        let recipient = UserKeypair::generate();
        let recipient_pubkey = keys::public_key_hex(&recipient);
        let keyring = coven_keys::encryption::EncryptionService::from_key([7; 32]);
        let sealed = keys::seal_box_encrypt(
            &keyring.to_keyring_payload().expect("serialize keyring"),
            &recipient.to_x25519_public_key(),
        );
        let wrapped = WrappedStoreKey::signed(
            "wrapped-generation-store",
            &recipient_pubkey,
            2,
            sealed,
            &owner,
        );

        assert!(matches!(
            wrapped.verify_and_open_keyring(
                "wrapped-generation-store",
                &recipient_pubkey,
                std::iter::once(keys::public_key_hex(&owner).as_str()),
                2,
                &recipient,
            ),
            Err(WrappedKeyringError::GenerationMismatch {
                reference: 2,
                payload: 1,
            }),
        ));
    }
}
