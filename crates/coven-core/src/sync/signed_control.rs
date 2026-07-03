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
        let (author_pubkey, signature) = keys::sign_hex(keypair, &payload);
        HeadJson {
            seq,
            snapshot_seq,
            last_sync,
            author_pubkey,
            signature,
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

/// Serialized form of a device's pull-ack stored in `acks/{device_id}.json{suffix}`.
///
/// An ack reports how far this device has pulled every *other* device's log:
/// `cursors` is `peer_device_id -> last_seq pulled`. Changeset GC reads the acks
/// of the current devices to find a floor below which no member still needs a
/// changeset, so it reclaims only what everyone has already pulled. Nothing else
/// consumes an ack — a missing, stale, or low ack only narrows reclamation, it
/// never holds back a pull or a push.
///
/// Because an ack drives fleet-wide deletion, it is signed exactly like
/// [`HeadJson`]: `author_pubkey`/`signature` cover the [`AckFields`] canonical
/// payload — including the `device_id` slot the ack lives under — so an ack
/// re-stamped with forged cursors, or copied verbatim into another device's slot,
/// no longer verifies. The slot is not stored in the JSON (it is the object key);
/// the reader passes the key's device_id to [`Self::verify`].
#[derive(Serialize, Deserialize)]
pub struct AckJson {
    /// This device's pull cursor on every other device: `peer_device_id -> seq`.
    /// A `BTreeMap` so its serialization is order-deterministic — a canonical
    /// payload.
    pub cursors: std::collections::BTreeMap<String, u64>,
    /// Hex-encoded Ed25519 public key of the device that wrote this ack.
    pub author_pubkey: String,
    /// Hex-encoded detached signature over [`AckFields`].
    pub signature: String,
}

/// The ack fields the signature covers, in declaration order. Excludes
/// `author_pubkey`/`signature` (the signature's own outputs). Includes
/// `device_id` — the slot the ack is stored under — so a valid ack cannot be
/// relocated to another device's slot (mirrors `HeadFields`' device_id binding).
#[derive(Serialize)]
struct AckFields<'a> {
    device_id: &'a str,
    cursors: &'a std::collections::BTreeMap<String, u64>,
}

impl AckJson {
    /// Build an ack for slot `device_id` signed by `keypair`: fills
    /// `author_pubkey` with the device's public key and `signature` with the
    /// detached signature over the canonical payload (which binds `device_id`).
    pub fn signed(
        device_id: &str,
        cursors: std::collections::BTreeMap<String, u64>,
        keypair: &UserKeypair,
    ) -> Self {
        let payload = ack_signing_payload(device_id, &cursors);
        let (author_pubkey, signature) = keys::sign_hex(keypair, &payload);
        AckJson {
            cursors,
            author_pubkey,
            signature,
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`, bound
    /// to the slot `device_id` the ack was read from. An ack that fails this is
    /// forged, corrupt, or relocated to a foreign slot, and must be ignored.
    pub fn verify(&self, device_id: &str) -> bool {
        let payload = ack_signing_payload(device_id, &self.cursors);
        keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
    }
}

fn ack_signing_payload(
    device_id: &str,
    cursors: &std::collections::BTreeMap<String, u64>,
) -> Vec<u8> {
    let fields = AckFields { device_id, cursors };
    serde_json::to_vec(&fields).expect("ack fields serialization cannot fail")
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
    /// Test-only: the only writer of the schema floor is
    /// [`crate::sync::storage::SyncStorage::set_min_schema_version`], which has no
    /// production caller (the read/`verify` side is live).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed(min_schema_version: u32, keypair: &UserKeypair) -> Self {
        let (author_pubkey, signature) =
            keys::sign_hex(keypair, &min_schema_signing_payload(min_schema_version));
        MinSchemaVersionJson {
            min_schema_version,
            author_pubkey,
            signature,
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

#[derive(Serialize)]
struct MinSchemaFields {
    min_schema_version: u32,
}

fn min_schema_signing_payload(version: u32) -> Vec<u8> {
    serde_json::to_vec(&MinSchemaFields {
        min_schema_version: version,
    })
    .expect("min schema fields serialization cannot fail")
}

/// Serialized form of a snapshot generation's
/// `snapshot/{author}/{seq}_meta.json{suffix}`: the per-device cursors of a
/// snapshot plus the hash of the snapshot DB image, under one author signature.
///
/// A snapshot is a destructive primitive twice over. The DB image is the whole
/// catalog a joining device adopts wholesale; the `cursors` are control input —
/// a bootstrapping device starts pulling each peer *past* these, so a forged
/// `cursors = {device: u64::MAX}` makes it silently skip that device's real
/// changesets. The at-rest cipher proves only confidentiality (the library key is
/// shared by every member), not authorship, so a bucket writer can overwrite both
/// objects. So the author signs both halves together: `db_hash` binds the DB
/// image and `cursors` bind the metadata, under one signature. A tampered DB no
/// longer matches `db_hash`; swapped cursors no longer verify.
///
/// `db_hash` is the SHA-256 of the snapshot's **stored** (sealed) bytes — exactly
/// what the generation's db object holds — so a verifier hashes the bytes it
/// downloaded and compares, with no decryption needed to detect a substituted
/// image.
///
/// `library_id` binds the meta to its library: it is part of the signed payload
/// but not stored (the reader supplies its own library id to [`Self::verify`]),
/// mirroring [`WrappedLibraryKey`]. A member of two libraries cannot take one
/// library's catalog and re-sign its meta as the other's — re-verifying under the
/// second library's id fails, because the signature was taken over the first's.
///
/// Like [`HeadJson`] (and unlike [`WrappedLibraryKey`]), the `author_pubkey` is
/// stored: a snapshot's author varies device to device, so the verifier learns
/// who signed it and then checks that author against the membership chain (the
/// authorization step, the caller's job). It is also what lets the
/// superseded-generation sweep recognize which generations a device published, so
/// each device reclaims only its own (see [`crate::sync::snapshot`]).
#[derive(Serialize, Deserialize)]
pub struct SnapshotMetaJson {
    /// Per-device cursors at snapshot time: device_id -> head seq. A bootstrapping
    /// device uses these as its initial sync_cursors.
    pub cursors: std::collections::BTreeMap<String, u64>,
    /// Hex-encoded SHA-256 of the stored (sealed) snapshot DB bytes.
    pub db_hash: String,
    /// The writer's applied synced-schema version (`PRAGMA user_version`) at
    /// snapshot time. The DB image carries this version in its header anyway; the
    /// signed copy lets a too-old joiner refuse the generation *before* downloading
    /// the image, instead of relying solely on the at-open backstop.
    pub schema_version: u32,
    /// Hex-encoded Ed25519 public key of the device that wrote this snapshot.
    pub author_pubkey: String,
    /// Hex-encoded detached signature over [`SnapshotMetaFields`].
    pub signature: String,
}

/// The snapshot-meta fields the signature covers, in declaration order. Excludes
/// `author_pubkey`/`signature` (the signature's own outputs). Includes
/// `library_id` (so a meta can't be replayed into a different library, mirroring
/// [`WrappedKeyFields`]). `cursors` is a `BTreeMap` so its serialization is
/// order-deterministic — a canonical payload.
#[derive(Serialize)]
struct SnapshotMetaFields<'a> {
    library_id: &'a str,
    cursors: &'a std::collections::BTreeMap<String, u64>,
    db_hash: &'a str,
    schema_version: u32,
}

impl SnapshotMetaJson {
    /// Build a snapshot meta for `library_id` signed by `keypair`: fills
    /// `author_pubkey` with the device's public key and `signature` with the
    /// detached signature over the canonical payload (which binds `library_id`,
    /// the cursors, and the DB hash). `library_id` is bound but not stored — the
    /// reader passes its own to [`Self::verify`].
    pub fn signed(
        library_id: &str,
        cursors: std::collections::BTreeMap<String, u64>,
        db_hash: String,
        schema_version: u32,
        keypair: &UserKeypair,
    ) -> Self {
        let payload = snapshot_meta_signing_payload(library_id, &cursors, &db_hash, schema_version);
        let (author_pubkey, signature) = keys::sign_hex(keypair, &payload);
        SnapshotMetaJson {
            cursors,
            db_hash,
            schema_version,
            author_pubkey,
            signature,
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`, bound
    /// to `library_id`. A meta that fails this is forged, corrupt, tampered
    /// (cursors or DB hash changed after signing), or a different library's meta
    /// replayed here, and must not be adopted. Whether the author is *authorized*
    /// (a current member) is a separate check the caller runs.
    pub fn verify(&self, library_id: &str) -> bool {
        let payload = snapshot_meta_signing_payload(
            library_id,
            &self.cursors,
            &self.db_hash,
            self.schema_version,
        );
        keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
    }
}

fn snapshot_meta_signing_payload(
    library_id: &str,
    cursors: &std::collections::BTreeMap<String, u64>,
    db_hash: &str,
    schema_version: u32,
) -> Vec<u8> {
    let fields = SnapshotMetaFields {
        library_id,
        cursors,
        db_hash,
        schema_version,
    };
    serde_json::to_vec(&fields).expect("snapshot meta fields serialization cannot fail")
}

/// Serialized form of `snapshot/current.json{suffix}`: the pointer that names
/// which snapshot generation is live.
///
/// A snapshot is published as a set of objects under a per-generation key in the
/// publishing device's own keyspace (`snapshot/{author}/{seq}.db`,
/// `snapshot/{author}/{seq}_meta.json`), all written *before* the pointer. The
/// pointer is the commit: a reader resolves it first, then loads the db+meta pair
/// it names from `{author_pubkey, seq}` — always a complete, self-consistent
/// generation, never a half-written one. Writing the db+meta at fixed keys instead
/// would let a reader observe a new db paired with a stale meta (a torn read); the
/// pointer removes that window because nothing references a generation until it is
/// whole.
///
/// The bucket is untrusted, so the pointer is signed like every other control
/// object. It names a generation's `{author_pubkey, seq}` and repeats that
/// generation's `db_hash`, all under one author signature, so a non-member who can
/// write the bucket cannot repoint the live snapshot at a fabricated or stale
/// generation: a pointer they author fails the membership check, and one they copy
/// from an older generation no longer matches the `seq`/`db_hash` they would need
/// to forge. `db_hash` is repeated here (not only in the meta) so the pointer
/// commits to the *exact* generation, not merely its number — the verifier checks
/// the pointer's `db_hash` equals the meta's before adopting the db image.
///
/// `library_id` binds the pointer to its library: it is part of the signed
/// payload but not stored (the reader supplies its own library id to
/// [`Self::verify`]), mirroring [`WrappedLibraryKey`]. A member of two libraries
/// cannot repoint one library's snapshot under the other's — re-verifying under
/// the second library's id fails.
///
/// Like [`HeadJson`] and [`SnapshotMetaJson`], `author_pubkey` is stored: the
/// publishing device varies, so the verifier learns who signed and then checks
/// that author against the membership chain (the authorization step, the caller's
/// job).
#[derive(Serialize, Deserialize)]
pub struct SnapshotPointerJson {
    /// The sequence of the snapshot generation this pointer publishes. With the
    /// pointer's `author_pubkey`, names the
    /// `snapshot/{author_pubkey}/{seq}.db` / `snapshot/{author_pubkey}/{seq}_meta.json`
    /// objects to load.
    pub seq: u64,
    /// Hex-encoded SHA-256 of the named generation's stored (sealed) snapshot DB
    /// bytes — the same hash that generation's [`SnapshotMetaJson`] commits to.
    pub db_hash: String,
    /// Hex-encoded Ed25519 public key of the device that published this pointer.
    /// Doubles as the live generation's keyspace segment: the named generation's
    /// objects live under `snapshot/{author_pubkey}/{seq}`.
    pub author_pubkey: String,
    /// Hex-encoded detached signature over [`SnapshotPointerFields`].
    pub signature: String,
}

/// The pointer fields the signature covers, in declaration order. Excludes
/// `author_pubkey`/`signature` (the signature's own outputs). Includes
/// `library_id` (so a pointer can't be replayed into a different library,
/// mirroring [`WrappedKeyFields`]). Binding both `seq` and `db_hash` means a
/// pointer cannot be re-stamped to a different generation number, nor have its
/// number kept while pointed at a substituted image.
#[derive(Serialize)]
struct SnapshotPointerFields<'a> {
    library_id: &'a str,
    seq: u64,
    db_hash: &'a str,
}

impl SnapshotPointerJson {
    /// Build a pointer for `library_id` signed by `keypair`: fills `author_pubkey`
    /// with the device's public key and `signature` with the detached signature
    /// over the canonical payload (which binds `library_id`, the generation `seq`,
    /// and its DB hash). `library_id` is bound but not stored — the reader passes
    /// its own to [`Self::verify`].
    pub fn signed(library_id: &str, seq: u64, db_hash: String, keypair: &UserKeypair) -> Self {
        let payload = snapshot_pointer_signing_payload(library_id, seq, &db_hash);
        let (author_pubkey, signature) = keys::sign_hex(keypair, &payload);
        SnapshotPointerJson {
            seq,
            db_hash,
            author_pubkey,
            signature,
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`, bound
    /// to `library_id`. A pointer that fails this is forged, corrupt, tampered
    /// (`seq` or `db_hash` changed after signing), or a different library's pointer
    /// replayed here, and must not be followed. Whether the author is *authorized*
    /// (a current write-capable member) is a separate check the caller runs.
    pub fn verify(&self, library_id: &str) -> bool {
        let payload = snapshot_pointer_signing_payload(library_id, self.seq, &self.db_hash);
        keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
    }
}

fn snapshot_pointer_signing_payload(library_id: &str, seq: u64, db_hash: &str) -> Vec<u8> {
    let fields = SnapshotPointerFields {
        library_id,
        seq,
        db_hash,
    };
    serde_json::to_vec(&fields).expect("snapshot pointer fields serialization cannot fail")
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
///
/// Unlike [`HeadJson`], no author is stored: the only key whose signature this
/// object may bear is the owner the joiner already pins (the chain founder),
/// which the verifier is handed directly. There is no second valid signer to
/// distinguish, so the signature is checked straight against that pinned owner.
#[derive(Serialize, Deserialize)]
pub struct WrappedLibraryKey {
    /// Hex-encoded sealed box (`seal_box_encrypt` output) carrying the library key.
    pub sealed: String,
    /// Hex-encoded detached signature over [`WrappedKeyFields`], produced by the owner.
    pub signature: String,
}

/// The wrapped-key fields the signature covers, in declaration order. Excludes
/// `signature` (the signature's own output). Includes `library_id` (so a key
/// can't be replayed into a different library) and `recipient_pubkey` (the slot,
/// so a key can't be relocated to another member).
#[derive(Serialize)]
struct WrappedKeyFields<'a> {
    library_id: &'a str,
    recipient_pubkey: &'a str,
    sealed: &'a str,
}

/// Why a [`WrappedLibraryKey`] could not be authenticated and unwrapped. Named
/// per reason so the caller can surface *why* an adoption was refused — a
/// substituted/forged key (the signature does not verify against the pinned
/// owner) is distinct from a corrupt object (the sealed box is not valid hex) —
/// rather than collapsing both into one opaque failure.
#[derive(Debug, thiserror::Error)]
pub enum WrappedKeyError {
    /// The signature does not verify against the pinned owner over
    /// `(library_id, recipient_pubkey, sealed)`. Covers a box signed by anyone
    /// other than the owner, a payload tampered after signing (different library,
    /// slot, or sealed bytes), and a malformed signature or owner pubkey — all
    /// indistinguishable here and all meaning "not authentically the owner's".
    #[error("signature does not verify against the pinned library owner")]
    SignatureMismatch,
    /// The signature verified, but the sealed-box field is not valid hex, so
    /// there are no bytes to decrypt — a corrupt object, not an attack.
    #[error("sealed box is not valid hex")]
    MalformedSealed,
}

impl WrappedLibraryKey {
    /// Wrap `sealed` (a sealed box of the library key, already encrypted to
    /// `recipient_pubkey`) and sign the binding with `owner`: fills `signature`
    /// with the owner's detached signature over the canonical payload.
    pub fn signed(
        library_id: &str,
        recipient_pubkey: &str,
        sealed: Vec<u8>,
        owner: &UserKeypair,
    ) -> Self {
        let sealed_hex = hex::encode(sealed);
        let payload = wrapped_key_signing_payload(library_id, recipient_pubkey, &sealed_hex);
        let (_, signature) = keys::sign_hex(owner, &payload);
        WrappedLibraryKey {
            sealed: sealed_hex,
            signature,
        }
    }

    /// Verify this wrapped key was authentically produced by `expected_owner`
    /// (the chain founder the invite pins) for `recipient_pubkey` in
    /// `library_id`, and return the sealed-box bytes to decrypt. Verifies the
    /// signature directly against `expected_owner` — the only key whose
    /// signature this object may bear — over the binding `(library_id,
    /// recipient_pubkey, sealed)`. Fails closed, naming why, if the signature
    /// doesn't verify against that owner (a substituted, forged, or relocated
    /// key) or the sealed box is malformed; neither must be adopted.
    pub fn verify_and_unwrap(
        &self,
        library_id: &str,
        recipient_pubkey: &str,
        expected_owner: &str,
    ) -> Result<Vec<u8>, WrappedKeyError> {
        let payload = wrapped_key_signing_payload(library_id, recipient_pubkey, &self.sealed);
        // Any way this fails to verify against the pinned owner is one outcome:
        // not authentically the owner's key, refuse it.
        if !keys::verify_signature_hex(expected_owner, &self.signature, &payload) {
            return Err(WrappedKeyError::SignatureMismatch);
        }
        // Verified as the owner's bytes; an un-decodable sealed field is a corrupt
        // object, a distinct failure.
        hex::decode(&self.sealed).map_err(|_| WrappedKeyError::MalformedSealed)
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
    fn ack_round_trips_and_binds_its_slot_and_cursors() {
        use std::collections::BTreeMap;
        let kp = UserKeypair::generate();
        let cursors = BTreeMap::from([("devB".to_string(), 4u64), ("devC".to_string(), 9)]);
        let ack = AckJson::signed("devA", cursors.clone(), &kp);

        assert_eq!(ack.author_pubkey, hex::encode(kp.public_key));
        assert!(ack.verify("devA"), "a freshly signed ack verifies");

        // Round-trips through JSON unchanged.
        let json = serde_json::to_vec(&ack).expect("serialize ack");
        let parsed: AckJson = serde_json::from_slice(&json).expect("parse ack");
        assert!(
            parsed.verify("devA"),
            "ack verifies after a JSON round-trip"
        );

        // The signature binds the slot: an ack for devA copied into devB's slot
        // does not verify, so it cannot be relocated to forge another device's
        // pulled-cursor claim.
        assert!(
            !ack.verify("devB"),
            "an ack must be bound to its slot, not relocatable",
        );

        // It binds the cursors: editing a pulled-cursor after signing (the
        // forge-a-high-ack-to-trigger-deletion primitive) invalidates it.
        let mut tampered = AckJson::signed("devA", cursors, &kp);
        tampered.cursors.insert("devB".to_string(), u64::MAX);
        assert!(
            !tampered.verify("devA"),
            "tampered cursors fail verification"
        );
    }

    #[test]
    fn ack_signed_by_one_key_does_not_verify_under_another() {
        use std::collections::BTreeMap;
        let kp = UserKeypair::generate();
        let other = UserKeypair::generate();
        let mut ack = AckJson::signed("devA", BTreeMap::from([("devB".to_string(), 1u64)]), &kp);
        // Swap the claimed author: the signature no longer matches, so the ack is
        // rejected (a forger can't claim a member's pubkey).
        ack.author_pubkey = hex::encode(other.public_key);
        assert!(!ack.verify("devA"));
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
    fn min_schema_signing_payload_is_canonical_json() {
        assert_eq!(
            min_schema_signing_payload(5),
            br#"{"min_schema_version":5}"#
        );
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
    fn snapshot_meta_round_trips_and_binds_its_fields() {
        use std::collections::BTreeMap;
        let kp = UserKeypair::generate();
        let cursors = BTreeMap::from([("devA".to_string(), 5u64), ("devB".to_string(), 9)]);
        let meta = SnapshotMetaJson::signed("lib", cursors.clone(), "abc123".to_string(), 3, &kp);

        assert_eq!(meta.author_pubkey, hex::encode(kp.public_key));
        assert_eq!(meta.schema_version, 3);
        assert!(
            meta.verify("lib"),
            "a freshly signed snapshot meta verifies"
        );

        // Round-trips through JSON unchanged.
        let json = serde_json::to_vec(&meta).expect("serialize meta");
        let parsed: SnapshotMetaJson = serde_json::from_slice(&json).expect("parse meta");
        assert!(
            parsed.verify("lib"),
            "meta verifies after a JSON round-trip"
        );

        // The signature binds the library: a meta re-verified under a different
        // library id is refused (the cross-library replay defense).
        assert!(
            !meta.verify("other-lib"),
            "a meta must not verify under a different library id"
        );

        // The signature binds the DB hash: a meta swapped onto a different DB image
        // (a substituted snapshot) no longer verifies.
        let mut tampered_db =
            SnapshotMetaJson::signed("lib", cursors.clone(), "abc123".to_string(), 3, &kp);
        tampered_db.db_hash = "deadbeef".to_string();
        assert!(
            !tampered_db.verify("lib"),
            "a tampered db_hash fails verification"
        );

        // It binds the schema_version too: bumping it after signing (so a too-old
        // joiner is told the image is at a version it can apply) invalidates it.
        let mut tampered_version =
            SnapshotMetaJson::signed("lib", cursors.clone(), "abc123".to_string(), 3, &kp);
        tampered_version.schema_version = 99;
        assert!(
            !tampered_version.verify("lib"),
            "a tampered schema_version fails verification"
        );

        // It also binds the cursors: poisoning a cursor (the bootstrap-skip attack)
        // invalidates the signature.
        let mut tampered_cursors =
            SnapshotMetaJson::signed("lib", cursors, "abc123".to_string(), 3, &kp);
        tampered_cursors
            .cursors
            .insert("devA".to_string(), u64::MAX);
        assert!(
            !tampered_cursors.verify("lib"),
            "tampered cursors fail verification"
        );
    }

    #[test]
    fn snapshot_meta_signed_by_one_key_does_not_verify_under_another() {
        use std::collections::BTreeMap;
        let kp = UserKeypair::generate();
        let other = UserKeypair::generate();
        let mut meta = SnapshotMetaJson::signed(
            "lib",
            BTreeMap::from([("devA".to_string(), 1u64)]),
            "hash".to_string(),
            1,
            &kp,
        );
        // Swap the claimed author: the signature no longer matches, so the meta is
        // rejected (a forger can't claim a member's pubkey).
        meta.author_pubkey = hex::encode(other.public_key);
        assert!(!meta.verify("lib"));
    }

    #[test]
    fn snapshot_pointer_round_trips_and_binds_its_fields() {
        let kp = UserKeypair::generate();
        let pointer = SnapshotPointerJson::signed("lib", 7, "abc123".to_string(), &kp);

        assert_eq!(pointer.author_pubkey, hex::encode(kp.public_key));
        assert!(pointer.verify("lib"), "a freshly signed pointer verifies");

        // Round-trips through JSON unchanged.
        let json = serde_json::to_vec(&pointer).expect("serialize pointer");
        let parsed: SnapshotPointerJson = serde_json::from_slice(&json).expect("parse pointer");
        assert!(
            parsed.verify("lib"),
            "pointer verifies after a JSON round-trip"
        );

        // The signature binds the library: a pointer re-verified under a different
        // library id is refused (the cross-library replay defense).
        assert!(
            !pointer.verify("other-lib"),
            "a pointer must not verify under a different library id"
        );

        // The signature binds the seq: repointing to a different generation number
        // after signing invalidates it, so a forged pointer can't name an arbitrary
        // generation.
        let mut tampered_seq = SnapshotPointerJson::signed("lib", 7, "abc123".to_string(), &kp);
        tampered_seq.seq = 99;
        assert!(
            !tampered_seq.verify("lib"),
            "a tampered seq fails verification"
        );

        // It also binds the DB hash: pointing the same seq at a substituted image
        // (a different generation's db) fails.
        let mut tampered_hash = SnapshotPointerJson::signed("lib", 7, "abc123".to_string(), &kp);
        tampered_hash.db_hash = "deadbeef".to_string();
        assert!(
            !tampered_hash.verify("lib"),
            "a tampered db_hash fails verification"
        );
    }

    #[test]
    fn snapshot_pointer_signed_by_one_key_does_not_verify_under_another() {
        let kp = UserKeypair::generate();
        let other = UserKeypair::generate();
        let mut pointer = SnapshotPointerJson::signed("lib", 1, "hash".to_string(), &kp);
        // Swap the claimed author: the signature no longer matches, so the pointer
        // is rejected (a forger can't claim a member's pubkey to repoint).
        pointer.author_pubkey = hex::encode(other.public_key);
        assert!(!pointer.verify("lib"));
    }

    #[test]
    fn wrapped_key_round_trips_and_returns_sealed_bytes() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key);
        let sealed = vec![1u8, 2, 3, 4, 5];
        let wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", sealed.clone(), &owner);

        // Round-trips through JSON and yields the sealed bytes back.
        let json = serde_json::to_vec(&wrapped).expect("serialize wrapped key");
        let parsed: WrappedLibraryKey = serde_json::from_slice(&json).expect("parse wrapped key");
        assert_eq!(
            parsed
                .verify_and_unwrap("lib", "recipient-pk", &owner_hex)
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
        let pinned_owner_hex = hex::encode(pinned_owner.public_key);
        let sealed = vec![9u8; 32];
        let wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", sealed, &signer);

        assert!(
            matches!(
                wrapped.verify_and_unwrap("lib", "recipient-pk", &pinned_owner_hex),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "a key not signed by the pinned owner must be refused",
        );
    }

    #[test]
    fn wrapped_key_rejects_rebinding() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key);
        let sealed = vec![9u8; 32];
        let wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", sealed, &owner);

        // The signature binds the library and the recipient slot: changing either
        // at verify time fails, so a key can't be replayed cross-library or
        // relocated to another member's slot.
        assert!(
            matches!(
                wrapped.verify_and_unwrap("other-lib", "recipient-pk", &owner_hex),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "must reject a key replayed into a different library",
        );
        assert!(
            matches!(
                wrapped.verify_and_unwrap("lib", "other-recipient", &owner_hex),
                Err(WrappedKeyError::SignatureMismatch),
            ),
            "must reject a key relocated to another recipient's slot",
        );
    }

    #[test]
    fn wrapped_key_malformed_signature_fails_closed() {
        let owner = UserKeypair::generate();
        let owner_hex = hex::encode(owner.public_key);
        let mut wrapped = WrappedLibraryKey::signed("lib", "recipient-pk", vec![1u8; 4], &owner);

        // A signature that isn't valid hex can't verify against the owner.
        wrapped.signature = "not-hex!!".to_string();
        assert!(matches!(
            wrapped.verify_and_unwrap("lib", "recipient-pk", &owner_hex),
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
        let owner_hex = hex::encode(owner.public_key);

        let mut wrapped = WrappedLibraryKey {
            sealed: "not-hex!!".to_string(),
            signature: String::new(),
        };
        let payload = wrapped_key_signing_payload("lib", "recipient-pk", &wrapped.sealed);
        let (_, signature) = keys::sign_hex(&owner, &payload);
        wrapped.signature = signature;

        assert!(matches!(
            wrapped.verify_and_unwrap("lib", "recipient-pk", &owner_hex),
            Err(WrappedKeyError::MalformedSealed),
        ));
    }
}
