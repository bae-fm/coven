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
/// `author_pubkey`/`signature` cover the [`HeadFields`] canonical payload, so a
/// head re-stamped with a forged seq or snapshot coverage no longer verifies.
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
/// `author_pubkey`/`signature` (the signature's own outputs).
#[derive(Serialize)]
struct HeadFields<'a> {
    seq: u64,
    snapshot_seq: Option<u64>,
    last_sync: Option<&'a str>,
}

impl HeadJson {
    /// Build a head signed by `keypair`: fills `author_pubkey` with the device's
    /// public key and `signature` with the detached signature over the canonical
    /// payload.
    pub fn signed(
        seq: u64,
        snapshot_seq: Option<u64>,
        last_sync: Option<String>,
        keypair: &UserKeypair,
    ) -> Self {
        let payload = head_signing_payload(seq, snapshot_seq, last_sync.as_deref());
        let sig = keypair.sign(&payload);
        HeadJson {
            seq,
            snapshot_seq,
            last_sync,
            author_pubkey: hex::encode(keypair.public_key),
            signature: hex::encode(sig),
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`. A head
    /// that fails this is forged (or corrupt) and must be skipped by the reader.
    pub fn verify(&self) -> bool {
        let payload = head_signing_payload(self.seq, self.snapshot_seq, self.last_sync.as_deref());
        verify_detached(&self.author_pubkey, &self.signature, &payload)
    }
}

fn head_signing_payload(seq: u64, snapshot_seq: Option<u64>, last_sync: Option<&str>) -> Vec<u8> {
    let fields = HeadFields {
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
        verify_detached(
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

/// Verify a hex-encoded detached Ed25519 signature (`sig_hex`) over `payload`
/// against a hex-encoded public key (`pk_hex`). Malformed hex, a wrong-length
/// key/signature, or a signature that doesn't match all fail closed (false).
/// This is the crypto half of
/// [`verify_changeset_signature`](crate::sync::envelope::verify_changeset_signature),
/// shared by both control-object shapes.
fn verify_detached(pk_hex: &str, sig_hex: &str, payload: &[u8]) -> bool {
    let Ok(pk_bytes) = hex::decode(pk_hex) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(sig_hex) else {
        return false;
    };
    let Ok(pk): Result<[u8; keys::SIGN_PUBLICKEYBYTES], _> = pk_bytes.try_into() else {
        return false;
    };
    let Ok(sig): Result<[u8; keys::SIGN_BYTES], _> = sig_bytes.try_into() else {
        return false;
    };
    keys::verify_signature(&sig, payload, &pk)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_round_trips_and_binds_its_fields() {
        let kp = UserKeypair::generate();
        let head = HeadJson::signed(7, Some(3), Some("2026-01-01T00:00:00Z".to_string()), &kp);

        assert_eq!(head.author_pubkey, hex::encode(kp.public_key));
        assert!(head.verify(), "a freshly signed head verifies");

        // Round-trips through JSON unchanged.
        let json = serde_json::to_vec(&head).expect("serialize head");
        let parsed: HeadJson = serde_json::from_slice(&json).expect("parse head");
        assert!(parsed.verify(), "head verifies after a JSON round-trip");

        // The signature binds the seq: bumping it after signing invalidates it,
        // so a forged head can't be re-stamped to a higher seq to drive a fetch
        // loop.
        let mut tampered = HeadJson::signed(7, Some(3), None, &kp);
        tampered.seq = 99;
        assert!(!tampered.verify(), "a tampered seq fails verification");

        // It also binds the snapshot coverage.
        let mut tampered_snap = HeadJson::signed(7, Some(3), None, &kp);
        tampered_snap.snapshot_seq = Some(999);
        assert!(!tampered_snap.verify(), "tampered snapshot_seq fails");
    }

    #[test]
    fn head_signed_by_one_key_does_not_verify_under_another() {
        let kp = UserKeypair::generate();
        let other = UserKeypair::generate();
        let mut head = HeadJson::signed(1, None, None, &kp);
        // Swap the claimed author to a different key: the signature no longer
        // matches, so the head is rejected (a forger can't claim someone else's
        // pubkey).
        head.author_pubkey = hex::encode(other.public_key);
        assert!(!head.verify());
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
        let mut head = HeadJson::signed(1, None, None, &kp);
        head.signature = "not-valid-hex!!".to_string();
        assert!(!head.verify());

        let mut bad_pk = HeadJson::signed(1, None, None, &kp);
        bad_pk.author_pubkey = hex::encode([0u8; 16]); // wrong length
        assert!(!bad_pk.verify());
    }
}
