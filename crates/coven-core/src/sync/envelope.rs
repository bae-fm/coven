/// Changeset envelope: metadata + binary changeset packed into a single blob.
///
/// Wire format: `JSON bytes + \0 + changeset bytes`
///
/// The envelope carries enough context to understand the changeset without
/// unpacking the binary portion (schema version, author, description).
use serde::{Deserialize, Serialize};

use super::membership::MembershipCoord;
use crate::keys::{self, UserKeypair};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobLocationRecord {
    pub namespace: String,
    pub blob_id: String,
    pub location: crate::blob::CloudBlobLocation,
    /// The plaintext byte length signed by the changeset author. An UPDATE omits
    /// an unchanged size column, and a pulling device may not have the row yet if
    /// another device stream creates it later in the same pull.
    pub plaintext_size: u64,
    /// The content hash paired with `plaintext_size`. This is carried for the
    /// same ordering case and pins the fetched bytes without consulting local
    /// state that may describe a concurrent version of the row.
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangesetEnvelope {
    pub device_id: String,
    pub seq: u64,
    pub schema_version: u32,
    pub message: String,
    pub timestamp: String,
    pub changeset_size: usize,
    /// Hex-encoded Ed25519 public key of the author.
    pub author_pubkey: String,
    /// The membership entry that authorizes this author to write, as its storage
    /// coordinate. A puller that does not yet see that entry fetches it by this
    /// coordinate to resolve a membership-propagation gap instead of skipping the
    /// changeset as non-member. Covered by the signature, so it can't be forged.
    /// None for a solo (no membership chain) store.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub membership_grant: Option<MembershipCoord>,
    pub blob_locations: Vec<BlobLocationRecord>,
    /// Hex-encoded detached Ed25519 signature over the envelope metadata and
    /// changeset bytes (see `signing_payload`).
    pub signature: String,
}

/// The envelope fields the signature covers, in declaration order. Excludes
/// `author_pubkey`/`signature` (the signature's own outputs); the changeset
/// bytes are appended in [`signing_payload`]. `membership_grant` is included so
/// the authorizing position can't be re-stamped after signing.
#[derive(Serialize)]
struct SignedEnvelopeFields<'a> {
    device_id: &'a str,
    seq: u64,
    schema_version: u32,
    message: &'a str,
    timestamp: &'a str,
    changeset_size: usize,
    membership_grant: Option<&'a MembershipCoord>,
    blob_locations: &'a [BlobLocationRecord],
}

/// Canonical bytes a changeset signature covers: the authorization-relevant
/// envelope metadata followed by the changeset payload. Binding the metadata --
/// not just the payload -- means a signed changeset can't be re-stamped with a
/// forged timestamp or position to slip past pull-side membership validation.
fn signing_fields(env: &ChangesetEnvelope) -> SignedEnvelopeFields<'_> {
    SignedEnvelopeFields {
        device_id: &env.device_id,
        seq: env.seq,
        schema_version: env.schema_version,
        message: &env.message,
        timestamp: &env.timestamp,
        changeset_size: env.changeset_size,
        membership_grant: env.membership_grant.as_ref(),
        blob_locations: &env.blob_locations,
    }
}

fn signing_payload(fields: &SignedEnvelopeFields<'_>, changeset_bytes: &[u8]) -> Vec<u8> {
    let mut payload = serde_json::to_vec(&fields).expect("signed fields serialization cannot fail");
    payload.push(0);
    payload.extend_from_slice(changeset_bytes);
    payload
}

impl ChangesetEnvelope {
    /// Build the complete current envelope shape and sign its canonical fields.
    #[allow(clippy::too_many_arguments)]
    pub fn signed(
        device_id: &str,
        seq: u64,
        schema_version: u32,
        message: &str,
        timestamp: &str,
        membership_grant: Option<MembershipCoord>,
        blob_locations: Vec<BlobLocationRecord>,
        keypair: &UserKeypair,
        changeset_bytes: &[u8],
    ) -> Self {
        let fields = SignedEnvelopeFields {
            device_id,
            seq,
            schema_version,
            message,
            timestamp,
            changeset_size: changeset_bytes.len(),
            membership_grant: membership_grant.as_ref(),
            blob_locations: &blob_locations,
        };
        let (author_pubkey, signature) =
            keys::sign_hex(keypair, &signing_payload(&fields, changeset_bytes));
        Self {
            device_id: device_id.to_string(),
            seq,
            schema_version,
            message: message.to_string(),
            timestamp: timestamp.to_string(),
            changeset_size: changeset_bytes.len(),
            author_pubkey,
            membership_grant,
            blob_locations,
            signature,
        }
    }
}

/// Verify the signature on a changeset envelope.
///
/// Returns true only for a valid signature matching the author's public key.
pub fn verify_changeset_signature(env: &ChangesetEnvelope, changeset_bytes: &[u8]) -> bool {
    keys::verify_signature_hex(
        &env.author_pubkey,
        &env.signature,
        &signing_payload(&signing_fields(env), changeset_bytes),
    )
}

/// Build a signed envelope over `changeset` and pack it into the wire format.
/// Both the cycle's outgoing push and the deferred-changeset merge go through
/// this constructor so signing and packing cannot drift.
#[allow(clippy::too_many_arguments)]
pub fn pack_signed(
    device_id: &str,
    seq: u64,
    schema_version: u32,
    message: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    membership_grant: Option<MembershipCoord>,
    changeset: &[u8],
) -> Vec<u8> {
    pack_signed_with_blob_locations(
        device_id,
        seq,
        schema_version,
        message,
        timestamp,
        keypair,
        membership_grant,
        Vec::new(),
        changeset,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn pack_signed_with_blob_locations(
    device_id: &str,
    seq: u64,
    schema_version: u32,
    message: &str,
    timestamp: &str,
    keypair: &UserKeypair,
    membership_grant: Option<MembershipCoord>,
    blob_locations: Vec<BlobLocationRecord>,
    changeset: &[u8],
) -> Vec<u8> {
    let env = ChangesetEnvelope::signed(
        device_id,
        seq,
        schema_version,
        message,
        timestamp,
        membership_grant,
        blob_locations,
        keypair,
        changeset,
    );
    pack(&env, changeset)
}

/// Pack an envelope and changeset into the wire format.
///
/// Layout: `[envelope JSON] \0 [changeset bytes]`
pub fn pack(envelope: &ChangesetEnvelope, changeset: &[u8]) -> Vec<u8> {
    let json = serde_json::to_vec(envelope).expect("envelope serialization cannot fail");
    let mut buf = Vec::with_capacity(json.len() + 1 + changeset.len());
    buf.extend_from_slice(&json);
    buf.push(0);
    buf.extend_from_slice(changeset);
    buf
}

/// Error from unpacking a changeset envelope.
#[derive(Debug)]
pub enum UnpackError {
    /// No null separator found between envelope JSON and changeset bytes.
    NoSeparator,
    /// The envelope portion is not valid JSON.
    InvalidJson(serde_json::Error),
}

impl std::fmt::Display for UnpackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnpackError::NoSeparator => write!(f, "no null separator in envelope"),
            UnpackError::InvalidJson(e) => write!(f, "invalid envelope JSON: {e}"),
        }
    }
}

impl std::error::Error for UnpackError {}

/// Unpack the wire format into envelope + changeset bytes.
pub fn unpack(data: &[u8]) -> Result<(ChangesetEnvelope, Vec<u8>), UnpackError> {
    // Splitting on the first null byte is safe because the envelope is valid
    // JSON, and JSON cannot contain raw 0x00 bytes -- any null characters in
    // JSON strings must be escaped as \u0000. So the first 0x00 in the packed
    // blob is always our separator, not part of the envelope JSON.
    let separator = data
        .iter()
        .position(|&b| b == 0)
        .ok_or(UnpackError::NoSeparator)?;
    let json_bytes = &data[..separator];
    let changeset_bytes = &data[separator + 1..];

    let envelope: ChangesetEnvelope =
        serde_json::from_slice(json_bytes).map_err(UnpackError::InvalidJson)?;
    Ok((envelope, changeset_bytes.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::UserKeypair;

    fn test_envelope(changeset: &[u8]) -> ChangesetEnvelope {
        ChangesetEnvelope::signed(
            "dev-abc123",
            42,
            2,
            "Imported release placeholder",
            "2026-02-10T14:30:00Z",
            None,
            Vec::new(),
            &UserKeypair::generate(),
            changeset,
        )
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let changeset = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02];
        let envelope = test_envelope(&changeset);

        let packed = pack(&envelope, &changeset);
        let (unpacked_env, unpacked_cs) = unpack(&packed).expect("unpack");

        assert_eq!(unpacked_env, envelope);
        assert_eq!(unpacked_cs, changeset);
    }

    #[test]
    fn pack_unpack_empty_changeset() {
        let changeset: Vec<u8> = vec![];
        let envelope = test_envelope(&changeset);

        let packed = pack(&envelope, &changeset);
        let (unpacked_env, unpacked_cs) = unpack(&packed).expect("unpack");

        assert_eq!(unpacked_env, envelope);
        assert!(unpacked_cs.is_empty());
    }

    #[test]
    fn pack_contains_null_separator() {
        let changeset = vec![0xFF];
        let envelope = test_envelope(&changeset);

        let packed = pack(&envelope, &changeset);

        // Find the null byte -- it should exist exactly once between JSON and changeset.
        let null_positions: Vec<usize> = packed
            .iter()
            .enumerate()
            .filter(|&(_, &b)| b == 0)
            .map(|(i, _)| i)
            .collect();

        // The changeset doesn't contain 0x00 in this case, so exactly one null.
        assert_eq!(null_positions.len(), 1);
    }

    #[test]
    fn changeset_with_embedded_nulls() {
        // Changeset bytes that contain null bytes -- unpack should handle this
        // because we split on the FIRST null (after JSON).
        let changeset = vec![0x00, 0x00, 0xFF, 0x00];
        let envelope = test_envelope(&changeset);

        let packed = pack(&envelope, &changeset);
        let (unpacked_env, unpacked_cs) = unpack(&packed).expect("unpack");

        assert_eq!(unpacked_env, envelope);
        assert_eq!(unpacked_cs, changeset);
    }

    #[test]
    fn unpack_invalid_no_separator() {
        let data = b"hello world";
        assert!(matches!(unpack(data), Err(UnpackError::NoSeparator)));
    }

    #[test]
    fn unpack_invalid_bad_json() {
        // Null separator present but JSON is invalid
        let mut data = b"not json".to_vec();
        data.push(0);
        data.extend_from_slice(b"changeset");

        assert!(matches!(unpack(&data), Err(UnpackError::InvalidJson(_))));
    }

    #[test]
    fn unpack_rejects_missing_required_signing_fields() {
        let changeset = b"signed changeset";
        let packed = pack_signed(
            "dev-abc123",
            42,
            2,
            "message",
            "2026-02-10T14:30:00Z",
            &UserKeypair::generate(),
            None,
            changeset,
        );
        let separator = packed.iter().position(|byte| *byte == 0).unwrap();

        for field in ["author_pubkey", "signature"] {
            let mut json: serde_json::Value = serde_json::from_slice(&packed[..separator]).unwrap();
            json.as_object_mut().unwrap().remove(field);
            let mut missing = serde_json::to_vec(&json).unwrap();
            missing.push(0);
            missing.extend_from_slice(changeset);

            assert!(
                matches!(unpack(&missing), Err(UnpackError::InvalidJson(_))),
                "missing {field} must be rejected",
            );
        }
    }

    #[test]
    fn unpack_empty_input() {
        assert!(matches!(unpack(&[]), Err(UnpackError::NoSeparator)));
    }

    // ---- Signing tests ----

    #[test]
    fn changeset_signing() {
        let keypair = UserKeypair::generate();

        let changeset_bytes = b"some changeset payload";

        let env = ChangesetEnvelope::signed(
            "dev-abc123",
            42,
            2,
            "Imported release placeholder",
            "2026-02-10T14:30:00Z",
            None,
            Vec::new(),
            &keypair,
            changeset_bytes,
        );

        assert_eq!(env.author_pubkey, hex::encode(keypair.public_key()));
        assert!(verify_changeset_signature(&env, changeset_bytes));

        // Signed envelope round-trips through pack/unpack.
        let packed = pack(&env, changeset_bytes);
        let (unpacked_env, unpacked_cs) = unpack(&packed).expect("unpack");
        assert_eq!(unpacked_env.author_pubkey, env.author_pubkey);
        assert_eq!(unpacked_env.signature, env.signature);
        assert!(verify_changeset_signature(&unpacked_env, &unpacked_cs));

        // Tampered changeset bytes fail verification.
        assert!(!verify_changeset_signature(&env, b"tampered payload"));

        // The signature binds envelope metadata, not just the payload. Mutating
        // an authorization-relevant field (timestamp) after signing must
        // invalidate it -- otherwise a revoked member could backdate a signed
        // changeset to a time they were still a member and slip past pull-side
        // membership validation.
        let mut backdated = env.clone();
        backdated.timestamp = "2000-01-01T00:00:00Z".to_string();
        assert!(!verify_changeset_signature(&backdated, changeset_bytes));

        // The changeset's identity (device_id, seq) is bound too, so a signed
        // changeset can't be replayed under a different position.
        let mut moved = env.clone();
        moved.seq += 1;
        assert!(!verify_changeset_signature(&moved, changeset_bytes));
        let mut rehomed = env.clone();
        rehomed.device_id = "other-device".to_string();
        assert!(!verify_changeset_signature(&rehomed, changeset_bytes));

        let location = BlobLocationRecord {
            namespace: "photos".to_string(),
            blob_id: "p1".to_string(),
            location: crate::blob::CloudBlobLocation::generated("aa11", "stamp"),
            plaintext_size: 42,
            content_hash: "abcd".to_string(),
        };
        let mut with_location = ChangesetEnvelope::signed(
            "dev-abc123",
            42,
            2,
            "Imported release placeholder",
            "2026-02-10T14:30:00Z",
            None,
            vec![location],
            &keypair,
            changeset_bytes,
        );
        assert!(verify_changeset_signature(&with_location, changeset_bytes));
        let mut changed_generation = with_location.clone();
        changed_generation.blob_locations[0].location.generation = "deadbeef".to_string();
        assert!(!verify_changeset_signature(
            &changed_generation,
            changeset_bytes
        ));
        with_location.blob_locations[0].content_hash = "dcba".to_string();
        assert!(!verify_changeset_signature(&with_location, changeset_bytes));

        // The authorizing membership position is bound too: re-stamping the grant
        // after signing (to claim a different, e.g. higher-privilege, entry)
        // invalidates the signature.
        let signed_with_grant = ChangesetEnvelope::signed(
            "dev-abc123",
            42,
            2,
            "Imported release placeholder",
            "2026-02-10T14:30:00Z",
            Some(crate::sync::membership::MembershipCoord {
                author_pubkey: "owner-pk".to_string(),
                seq: 2,
            }),
            Vec::new(),
            &keypair,
            changeset_bytes,
        );
        assert!(verify_changeset_signature(
            &signed_with_grant,
            changeset_bytes
        ));
        let mut regranted = signed_with_grant.clone();
        regranted.membership_grant = Some(crate::sync::membership::MembershipCoord {
            author_pubkey: "owner-pk".to_string(),
            seq: 9,
        });
        assert!(!verify_changeset_signature(&regranted, changeset_bytes));

        // Malformed hex in signature fails.
        let mut bad_sig_env = env.clone();
        bad_sig_env.signature = "not-valid-hex!!".to_string();
        assert!(!verify_changeset_signature(&bad_sig_env, changeset_bytes));

        // Wrong-length public key fails.
        let mut bad_pk_env = env.clone();
        bad_pk_env.author_pubkey = hex::encode([0u8; 16]); // 16 bytes, not 32
        assert!(!verify_changeset_signature(&bad_pk_env, changeset_bytes));
    }

    #[test]
    fn signed_envelope_without_blob_locations_is_rejected() {
        let keypair = UserKeypair::generate();
        let changeset = b"current changeset";
        let env = ChangesetEnvelope::signed(
            "dev-abc123",
            42,
            2,
            "Imported release placeholder",
            "2026-02-10T14:30:00Z",
            None,
            Vec::new(),
            &keypair,
            changeset,
        );

        let mut json = serde_json::to_value(&env).unwrap();
        json.as_object_mut().unwrap().remove("blob_locations");
        let mut packed = serde_json::to_vec(&json).unwrap();
        packed.push(0);
        packed.extend_from_slice(changeset);
        assert!(unpack(&packed).is_err());
    }
}
