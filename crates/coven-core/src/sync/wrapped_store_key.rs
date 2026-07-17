//! Owner-signed wrapped store keys.

use serde::{Deserialize, Serialize};

use crate::keys::{self, UserKeypair};
use crate::sync::membership::MembershipCoord;
use crate::sync::store_commit::StoreBatchCommitRef;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WrappedKeyActivation {
    MergeConcurrent(MembershipCoord),
    Serial(StoreBatchCommitRef),
}

/// Serialized form of `keys/{recipient_pubkey}{suffix}`: the store encryption
/// key sealed to one member, plus the owner signature that authenticates it.
///
/// The sealed box alone proves only that the named recipient can open it — not
/// who produced it (the sender is an ephemeral key). Anyone who can write the
/// bucket knows a member's public key and can overwrite this object with a box
/// wrapping a key of their choosing; the joiner would then adopt an
/// attacker-chosen store key. So the owner signs the binding
/// `(store_id, recipient_pubkey, author_pubkey, sealed)`. A fresh joiner
/// verifies that signature against the owner the invite pins (the chain founder);
/// an existing member verifies against the current Owner set from the anchored
/// membership chain before adopting a rotated key. A substituted box no longer
/// carries an authorized Owner's signature over these bytes and is refused.
///
/// `recipient_pubkey` is the slot the object lives under (the member's hex
/// Ed25519 pubkey). It is part of the signed payload — not stored in the JSON —
/// so a validly-signed key for one member cannot be relocated to another
/// member's slot.
///
#[derive(Serialize, Deserialize)]
pub struct WrappedStoreKey {
    /// Hex-encoded Ed25519 public key of the Owner that signed this wrapped key.
    pub author_pubkey: String,
    /// Membership entry that must be visible before an existing member adopts
    /// this keyring. Invitation keys have no activation coordinate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<WrappedKeyActivation>,
    /// The keyring's current generation — the generation this wrap would adopt
    /// this recipient to — carried in the clear so a member can learn which
    /// committed generation an inactive wrap (one whose activation entry is not
    /// yet visible) names WITHOUT opening the sealed box, and pause its own
    /// sealing at that generation. Covered by the signature, so a bucket writer
    /// cannot forge a higher generation to wedge a member's cycle.
    pub generation: u64,
    /// Hex-encoded sealed box (`seal_box_encrypt` output) carrying the store key.
    pub sealed: String,
    /// Hex-encoded detached signature over [`WrappedKeyFields`], produced by the owner.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    activation: Option<&'a WrappedKeyActivation>,
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

impl WrappedStoreKey {
    /// Wrap `sealed` (a sealed box of the store key, already encrypted to
    /// `recipient_pubkey`) and sign the binding with `owner`: fills `signature`
    /// with the owner's detached signature over the canonical payload.
    pub fn signed(
        store_id: &str,
        recipient_pubkey: &str,
        activation: Option<WrappedKeyActivation>,
        generation: u64,
        sealed: Vec<u8>,
        owner: &UserKeypair,
    ) -> Self {
        let author_pubkey = hex::encode(owner.public_key());
        let sealed_hex = hex::encode(sealed);
        let payload = wrapped_key_signing_payload(
            store_id,
            recipient_pubkey,
            activation.as_ref(),
            generation,
            &author_pubkey,
            &sealed_hex,
        );
        let (_, signature) = keys::sign_hex(owner, &payload);
        WrappedStoreKey {
            author_pubkey,
            activation,
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
            self.activation.as_ref(),
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
}

fn wrapped_key_signing_payload(
    store_id: &str,
    recipient_pubkey: &str,
    activation: Option<&WrappedKeyActivation>,
    generation: u64,
    author_pubkey: &str,
    sealed_hex: &str,
) -> Vec<u8> {
    let fields = WrappedKeyFields {
        store_id,
        recipient_pubkey,
        activation,
        generation,
        author_pubkey,
        sealed: sealed_hex,
    };
    serde_json::to_vec(&fields).expect("wrapped key fields serialization cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapped_key_round_trips_and_returns_sealed_bytes() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key());
        let sealed = vec![1u8, 2, 3, 4, 5];
        let wrapped =
            WrappedStoreKey::signed("lib", "recipient-pk", None, 1, sealed.clone(), &owner);

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
        let wrapped = WrappedStoreKey::signed("lib", "recipient-pk", None, 1, sealed, &signer);

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
        let wrapped = WrappedStoreKey::signed("lib", "recipient-pk", None, 1, sealed, &owner);

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
        let mut wrapped = WrappedStoreKey::signed("lib", "recipient-pk", None, 1, sealed, &owner);
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
        let mut wrapped =
            WrappedStoreKey::signed("lib", "recipient-pk", None, 3, vec![9u8; 32], &owner);

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
        let mut wrapped =
            WrappedStoreKey::signed("lib", "recipient-pk", None, 1, vec![1u8; 4], &owner);

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
            activation: None,
            generation: 1,
            sealed: "not-hex!!".to_string(),
            signature: String::new(),
        };
        let payload = wrapped_key_signing_payload(
            "lib",
            "recipient-pk",
            wrapped.activation.as_ref(),
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
}
