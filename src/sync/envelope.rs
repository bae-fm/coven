/// Changeset envelope: metadata + binary changeset packed into a single blob.
///
/// Wire format: `JSON bytes + \0 + changeset bytes`
///
/// The envelope carries enough context to understand the changeset without
/// unpacking the binary portion (schema version, author, description).
use serde::{Deserialize, Serialize};

use crate::keys::{self, UserKeypair};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangesetEnvelope {
    pub device_id: String,
    pub seq: u64,
    pub schema_version: u32,
    pub message: String,
    pub timestamp: String,
    pub changeset_size: usize,
    /// Hex-encoded Ed25519 public key of the author. None for unsigned changesets.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub author_pubkey: Option<String>,
    /// Hex-encoded detached Ed25519 signature over the changeset bytes. None for unsigned.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub signature: Option<String>,
}

/// Sign a changeset envelope with the user's Ed25519 keypair.
///
/// Sets `author_pubkey` to the hex-encoded public key and `signature` to the
/// hex-encoded detached signature over the raw changeset bytes.
pub fn sign_envelope(env: &mut ChangesetEnvelope, keypair: &UserKeypair, changeset_bytes: &[u8]) {
    let sig = keypair.sign(changeset_bytes);
    env.author_pubkey = Some(hex::encode(keypair.public_key));
    env.signature = Some(hex::encode(sig));
}

/// Verify the signature on a changeset envelope.
///
/// Returns true if:
/// - No signature is present (unsigned changesets are accepted).
/// - A valid signature is present that matches the author's public key.
///
/// Returns false if a signature is present but invalid (wrong key, tampered data,
/// or malformed hex).
pub fn verify_changeset_signature(env: &ChangesetEnvelope, changeset_bytes: &[u8]) -> bool {
    match (&env.author_pubkey, &env.signature) {
        (None, None) => return true, // Unsigned envelope -- accepted.
        (Some(_), None) | (None, Some(_)) => return false, // Half-signed is invalid.
        _ => {}
    }
    let (Some(pk_hex), Some(sig_hex)) = (&env.author_pubkey, &env.signature) else {
        unreachable!()
    };

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

    keys::verify_signature(&sig, changeset_bytes, &pk)
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
    use crate::keys::KeyService;

    fn test_envelope() -> ChangesetEnvelope {
        ChangesetEnvelope {
            device_id: "dev-abc123".into(),
            seq: 42,
            schema_version: 2,
            message: "Imported Album One".into(),
            timestamp: "2026-02-10T14:30:00Z".into(),
            changeset_size: 4096,
            author_pubkey: None,
            signature: None,
        }
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let envelope = test_envelope();
        let changeset = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02];

        let packed = pack(&envelope, &changeset);
        let (unpacked_env, unpacked_cs) = unpack(&packed).expect("unpack");

        assert_eq!(unpacked_env, envelope);
        assert_eq!(unpacked_cs, changeset);
    }

    #[test]
    fn pack_unpack_empty_changeset() {
        let envelope = test_envelope();
        let changeset: Vec<u8> = vec![];

        let packed = pack(&envelope, &changeset);
        let (unpacked_env, unpacked_cs) = unpack(&packed).expect("unpack");

        assert_eq!(unpacked_env, envelope);
        assert!(unpacked_cs.is_empty());
    }

    #[test]
    fn pack_contains_null_separator() {
        let envelope = test_envelope();
        let changeset = vec![0xFF];

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
        let envelope = test_envelope();
        // Changeset bytes that contain null bytes -- unpack should handle this
        // because we split on the FIRST null (after JSON).
        let changeset = vec![0x00, 0x00, 0xFF, 0x00];

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
    fn unpack_empty_input() {
        assert!(matches!(unpack(&[]), Err(UnpackError::NoSeparator)));
    }

    // ---- Signing tests ----

    /// Combined test for signing operations. Uses a single KeyService call
    /// because env vars are process-global and parallel tests race.
    #[test]
    fn changeset_signing() {
        // Clear env to avoid interference from other tests.
        std::env::remove_var("BAE_USER_SIGNING_KEY");
        std::env::remove_var("BAE_USER_PUBLIC_KEY");

        let ks = KeyService::new(true, "test-signing".to_string());
        let keypair = ks.get_or_create_user_keypair().unwrap();

        let changeset_bytes = b"some changeset payload";

        // sign_envelope produces a valid signature.
        let mut env = test_envelope();
        sign_envelope(&mut env, &keypair, changeset_bytes);

        assert!(env.author_pubkey.is_some());
        assert!(env.signature.is_some());
        assert_eq!(
            env.author_pubkey.as_ref().unwrap(),
            &hex::encode(keypair.public_key)
        );
        assert!(verify_changeset_signature(&env, changeset_bytes));

        // Signed envelope round-trips through pack/unpack.
        let packed = pack(&env, changeset_bytes);
        let (unpacked_env, unpacked_cs) = unpack(&packed).expect("unpack");
        assert_eq!(unpacked_env.author_pubkey, env.author_pubkey);
        assert_eq!(unpacked_env.signature, env.signature);
        assert!(verify_changeset_signature(&unpacked_env, &unpacked_cs));

        // Tampered changeset bytes fail verification.
        assert!(!verify_changeset_signature(&env, b"tampered payload"));

        // Unsigned envelope passes verification.
        let unsigned_env = test_envelope();
        assert!(verify_changeset_signature(&unsigned_env, changeset_bytes));

        // Malformed hex in signature fails.
        let mut bad_sig_env = env.clone();
        bad_sig_env.signature = Some("not-valid-hex!!".to_string());
        assert!(!verify_changeset_signature(&bad_sig_env, changeset_bytes));

        // Wrong-length public key fails.
        let mut bad_pk_env = env.clone();
        bad_pk_env.author_pubkey = Some(hex::encode([0u8; 16])); // 16 bytes, not 32
        assert!(!verify_changeset_signature(&bad_pk_env, changeset_bytes));

        // Clean up
        std::env::remove_var("BAE_USER_SIGNING_KEY");
        std::env::remove_var("BAE_USER_PUBLIC_KEY");
    }
}
