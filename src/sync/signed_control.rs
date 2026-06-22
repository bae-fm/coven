//! Signed control objects: the on-disk shapes of `heads/{device}.json` and
//! `min_schema_version.json`, each carrying its author's Ed25519 public key and a
//! detached signature over its canonical payload.
//!
//! The cloud bucket is untrusted — any member, or anyone holding the bucket
//! credential, can write any object, and the at-rest cipher proves only
//! confidentiality, not who authored an object. So a control object that
//! influences trust/ordering must be signed by its author and verified before it
//! is acted on. A forged head pollutes sync status and drives a per-seq fetch
//! loop; a forged `min_schema_version` freezes the fleet (`SchemaVersionTooOld`)
//! or forces a downgrade. The membership (authorization) check is the *caller's*
//! job, where the chain is — this module only proves the embedded `author_pubkey`
//! signed these bytes.
//!
//! The signing/verification core mirrors [`crate::sync::envelope`]: a canonical
//! serialization of the signed fields (everything except the signature's own
//! outputs), signed with the device's [`UserKeypair`]. Both the real
//! [`CloudSyncStorage`](crate::sync::cloud_storage::CloudSyncStorage) and the test
//! `MockSyncStorage` call these helpers, so tests exercise the production crypto
//! rather than a parallel reimplementation.
use serde::{Deserialize, Serialize};

use crate::keys::{self, UserKeypair};

/// Serialized form of a device head stored in `heads/{device_id}.json{suffix}`.
///
/// `author_pubkey`/`signature` cover the [`HeadFields`] canonical payload —
/// including the `device_id` slot the head lives under — so a head re-stamped
/// with a forged seq or snapshot coverage, OR copied verbatim into a different
/// device's slot, no longer verifies. The slot is not stored in the JSON (it is
/// the object key); the reader passes the key's device_id to [`Self::verify`].
#[derive(Serialize, Deserialize)]
pub struct HeadJson {
    pub seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<u64>,
    /// RFC 3339 timestamp of when this head was last written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sync: Option<String>,
    /// Hex-encoded Ed25519 public key of the device that wrote this head.
    pub author_pubkey: String,
    /// Hex-encoded detached signature over [`HeadFields`].
    pub signature: String,
}

/// The head fields the signature covers, in declaration order. Excludes
/// `author_pubkey`/`signature` (the signature's own outputs). Includes
/// `device_id` — the slot the head is stored under — so a valid head cannot be
/// relocated to another device's slot (mirrors `SignedEnvelopeFields`'
/// device_id binding).
#[derive(Serialize)]
struct HeadFields<'a> {
    device_id: &'a str,
    seq: u64,
    snapshot_seq: Option<u64>,
    last_sync: Option<&'a str>,
}

impl HeadJson {
    /// Build a head for slot `device_id` signed by `keypair`: fills
    /// `author_pubkey` with the device's public key and `signature` with the
    /// detached signature over the canonical payload (which binds `device_id`).
    pub fn signed(
        device_id: &str,
        seq: u64,
        snapshot_seq: Option<u64>,
        last_sync: Option<String>,
        keypair: &UserKeypair,
    ) -> Self {
        let payload = head_signing_payload(device_id, seq, snapshot_seq, last_sync.as_deref());
        let sig = keypair.sign(&payload);
        HeadJson {
            seq,
            snapshot_seq,
            last_sync,
            author_pubkey: hex::encode(keypair.public_key),
            signature: hex::encode(sig),
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`, bound
    /// to the slot `device_id` the head was read from. A head that fails this is
    /// forged, corrupt, or relocated to a foreign slot, and must be skipped.
    pub fn verify(&self, device_id: &str) -> bool {
        let payload = head_signing_payload(
            device_id,
            self.seq,
            self.snapshot_seq,
            self.last_sync.as_deref(),
        );
        keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
    }
}

fn head_signing_payload(
    device_id: &str,
    seq: u64,
    snapshot_seq: Option<u64>,
    last_sync: Option<&str>,
) -> Vec<u8> {
    let fields = HeadFields {
        device_id,
        seq,
        snapshot_seq,
        last_sync,
    };
    serde_json::to_vec(&fields).expect("head fields serialization cannot fail")
}

/// Serialized form of `min_schema_version.json{suffix}`.
///
/// `author_pubkey`/`signature` cover the version, so only a value the caller can
/// attribute to a current owner is honored.
#[derive(Serialize, Deserialize)]
pub struct MinSchemaVersionJson {
    pub min_schema_version: u32,
    /// Hex-encoded Ed25519 public key of the device that set this minimum.
    pub author_pubkey: String,
    /// Hex-encoded detached signature over the version.
    pub signature: String,
}

impl MinSchemaVersionJson {
    /// Build a `min_schema_version` signed by `keypair`.
    pub fn signed(min_schema_version: u32, keypair: &UserKeypair) -> Self {
        let sig = keypair.sign(&min_schema_signing_payload(min_schema_version));
        MinSchemaVersionJson {
            min_schema_version,
            author_pubkey: hex::encode(keypair.public_key),
            signature: hex::encode(sig),
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`.
    pub fn verify(&self) -> bool {
        keys::verify_signature_hex(
            &self.author_pubkey,
            &self.signature,
            &min_schema_signing_payload(self.min_schema_version),
        )
    }
}

fn min_schema_signing_payload(version: u32) -> Vec<u8> {
    // A single integer field; its big-endian bytes are a canonical payload.
    version.to_be_bytes().to_vec()
}

/// Serialized form of `keys/{recipient_pubkey}{suffix}`: the library encryption
/// key sealed to one member, plus the owner signature that authenticates it.
///
/// The sealed box alone proves only that the named recipient can open it — not
/// who produced it (the sender is an ephemeral key). Anyone who can write the
/// bucket knows a member's public key and can overwrite this object with a box
/// wrapping a key of their choosing; the joiner would then adopt an
/// attacker-chosen library key. So the owner signs the binding
/// `(library_id, recipient_pubkey, sealed)` and the joiner verifies that
/// signature against the owner the invite pins (the chain founder) before
/// adopting the key. A substituted box no longer carries the owner's signature
/// over these bytes and is refused.
///
/// `recipient_pubkey` is the slot the object lives under (the member's hex
/// Ed25519 pubkey). It is part of the signed payload — not stored in the JSON —
/// so a validly-signed key for one member cannot be relocated to another
/// member's slot, mirroring how [`HeadJson`] binds its `device_id`.
#[derive(Serialize, Deserialize)]
pub struct WrappedLibraryKey {
    /// Hex-encoded sealed box (`seal_box_encrypt` output) carrying the library key.
    pub sealed: String,
    /// Hex-encoded Ed25519 public key of the owner that wrapped and signed this key.
    pub author_pubkey: String,
    /// Hex-encoded detached signature over [`WrappedKeyFields`].
    pub signature: String,
}

/// The wrapped-key fields the signature covers, in declaration order. Excludes
/// `author_pubkey`/`signature` (the signature's own outputs). Includes
/// `library_id` (so a key can't be replayed into a different library) and
/// `recipient_pubkey` (the slot, so a key can't be relocated to another member).
#[derive(Serialize)]
struct WrappedKeyFields<'a> {
    library_id: &'a str,
    recipient_pubkey: &'a str,
    sealed: &'a str,
}

impl WrappedLibraryKey {
    /// Wrap `sealed` (a sealed box of the library key, already encrypted to
    /// `recipient_pubkey`) and sign the binding with `owner`: fills
    /// `author_pubkey` with the owner's public key and `signature` with the
    /// detached signature over the canonical payload.
    pub fn signed(
        library_id: &str,
        recipient_pubkey: &str,
        sealed: Vec<u8>,
        owner: &UserKeypair,
    ) -> Self {
        let sealed_hex = hex::encode(sealed);
        let payload = wrapped_key_signing_payload(library_id, recipient_pubkey, &sealed_hex);
        let sig = owner.sign(&payload);
        WrappedLibraryKey {
            sealed: sealed_hex,
            author_pubkey: hex::encode(owner.public_key),
            signature: hex::encode(sig),
        }
    }

    /// Verify this wrapped key was authentically produced by `expected_owner`
    /// (the chain founder the invite pins) for `recipient_pubkey` in
    /// `library_id`, and return the sealed-box bytes to decrypt. Fails closed if
    /// the embedded author isn't the expected owner, if the signature doesn't
    /// cover these exact bytes/slot/library, or if any field is malformed —
    /// every one of which is a substituted or relocated key that must not be
    /// adopted.
    pub fn verify_and_unwrap(
        &self,
        library_id: &str,
        recipient_pubkey: &str,
        expected_owner: &str,
    ) -> Option<Vec<u8>> {
        if self.author_pubkey != expected_owner {
            return None;
        }
        let payload = wrapped_key_signing_payload(library_id, recipient_pubkey, &self.sealed);
        if !keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload) {
            return None;
        }
        hex::decode(&self.sealed).ok()
    }
}

fn wrapped_key_signing_payload(
    library_id: &str,
    recipient_pubkey: &str,
    sealed_hex: &str,
) -> Vec<u8> {
    let fields = WrappedKeyFields {
        library_id,
        recipient_pubkey,
        sealed: sealed_hex,
    };
    serde_json::to_vec(&fields).expect("wrapped key fields serialization cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_round_trips_and_binds_its_fields() {
        let kp = UserKeypair::generate();
        let head = HeadJson::signed(
            "devA",
            7,
            Some(3),
            Some("2026-01-01T00:00:00Z".to_string()),
            &kp,
        );

        assert_eq!(head.author_pubkey, hex::encode(kp.public_key));
        assert!(head.verify("devA"), "a freshly signed head verifies");

        // Round-trips through JSON unchanged.
        let json = serde_json::to_vec(&head).expect("serialize head");
        let parsed: HeadJson = serde_json::from_slice(&json).expect("parse head");
        assert!(
            parsed.verify("devA"),
            "head verifies after a JSON round-trip"
        );

        // The signature binds the seq: bumping it after signing invalidates it,
        // so a forged head can't be re-stamped to a higher seq to drive a fetch
        // loop.
        let mut tampered = HeadJson::signed("devA", 7, Some(3), None, &kp);
        tampered.seq = 99;
        assert!(
            !tampered.verify("devA"),
            "a tampered seq fails verification"
        );

        // It also binds the snapshot coverage.
        let mut tampered_snap = HeadJson::signed("devA", 7, Some(3), None, &kp);
        tampered_snap.snapshot_seq = Some(999);
        assert!(!tampered_snap.verify("devA"), "tampered snapshot_seq fails");
    }

    #[test]
    fn head_does_not_verify_in_a_different_slot() {
        // A validly-signed head for slot devA, copied verbatim into slot devB
        // (the bucket is untrusted, so anyone can move objects), must NOT verify
        // under devB — otherwise an attacker relocates a member's head to a
        // fabricated slot and drives a per-seq fetch loop / pollutes status.
        let kp = UserKeypair::generate();
        let head = HeadJson::signed("devA", 5, None, None, &kp);
        assert!(head.verify("devA"));
        assert!(
            !head.verify("devB"),
            "a head must be bound to its slot, not relocatable",
        );
    }

    #[test]
    fn head_signed_by_one_key_does_not_verify_under_another() {
        let kp = UserKeypair::generate();
        let other = UserKeypair::generate();
        let mut head = HeadJson::signed("devA", 1, None, None, &kp);
        // Swap the claimed author to a different key: the signature no longer
        // matches, so the head is rejected (a forger can't claim someone else's
        // pubkey).
        head.author_pubkey = hex::encode(other.public_key);
        assert!(!head.verify("devA"));
    }

    #[test]
    fn min_schema_round_trips_and_binds_its_version() {
        let kp = UserKeypair::generate();
        let min = MinSchemaVersionJson::signed(5, &kp);

        assert_eq!(min.author_pubkey, hex::encode(kp.public_key));
        assert!(min.verify());

        let json = serde_json::to_vec(&min).expect("serialize min_schema");
        let parsed: MinSchemaVersionJson = serde_json::from_slice(&json).expect("parse min_schema");
        assert!(parsed.verify());

        // Bumping the version (the freeze-the-fleet attack) invalidates it.
        let mut tampered = MinSchemaVersionJson::signed(5, &kp);
        tampered.min_schema_version = 9999;
        assert!(!tampered.verify());
    }

    #[test]
    fn malformed_signature_fails_closed() {
        let kp = UserKeypair::generate();
        let mut head = HeadJson::signed("devA", 1, None, None, &kp);
        head.signature = "not-valid-hex!!".to_string();
        assert!(!head.verify("devA"));

        let mut bad_pk = HeadJson::signed("devA", 1, None, None, &kp);
        bad_pk.author_pubkey = hex::encode([0u8; 16]); // wrong length
        assert!(!bad_pk.verify("devA"));
    }

    #[test]
    fn wrapped_key_round_trips_and_returns_sealed_bytes() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key);
        let sealed = vec![1u8, 2, 3, 4, 5];
        let wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", sealed.clone(), &owner);

        assert_eq!(wrapped.author_pubkey, owner_hex);

        // Round-trips through JSON and yields the sealed bytes back.
        let json = serde_json::to_vec(&wrapped).expect("serialize wrapped key");
        let parsed: WrappedLibraryKey = serde_json::from_slice(&json).expect("parse wrapped key");
        assert_eq!(
            parsed.verify_and_unwrap("lib", "recipient-pk", &owner_hex),
            Some(sealed),
        );
    }

    #[test]
    fn wrapped_key_rejects_wrong_owner_and_rebinding() {
        let owner = UserKeypair::generate();
        let other = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key);
        let other_hex = hex::encode(other.public_key);
        let sealed = vec![9u8; 32];
        let wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", sealed, &owner);

        // A different expected owner: a substituted box signed by a non-owner
        // (or claiming the owner without their key) is refused.
        assert_eq!(
            wrapped.verify_and_unwrap("lib", "recipient-pk", &other_hex),
            None,
            "must reject when the expected owner is not the signer",
        );

        // The signature binds the library and the recipient slot: changing either
        // at verify time fails, so a key can't be replayed cross-library or
        // relocated to another member's slot.
        assert_eq!(
            wrapped.verify_and_unwrap("other-lib", "recipient-pk", &owner_hex),
            None,
            "must reject a key replayed into a different library",
        );
        assert_eq!(
            wrapped.verify_and_unwrap("lib", "other-recipient", &owner_hex),
            None,
            "must reject a key relocated to another recipient's slot",
        );
    }

    #[test]
    fn wrapped_key_rejects_a_forged_author_claim() {
        // A box sealed and signed by an attacker, then re-labeled to claim the
        // owner's pubkey, must fail: the signature no longer matches the claimed
        // author.
        let attacker = UserKeypair::generate();
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key);

        let mut wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", vec![7u8; 8], &attacker);
        wrapped.author_pubkey = owner_hex.clone();
        assert_eq!(
            wrapped.verify_and_unwrap("lib", "recipient-pk", &owner_hex),
            None,
            "a forged author claim with an attacker's signature must be refused",
        );
    }

    #[test]
    fn wrapped_key_malformed_fields_fail_closed() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key);
        let mut wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", vec![1u8; 4], &owner);

        wrapped.signature = "not-hex!!".to_string();
        assert_eq!(
            wrapped.verify_and_unwrap("lib", "recipient-pk", &owner_hex),
            None,
        );
    }
}
