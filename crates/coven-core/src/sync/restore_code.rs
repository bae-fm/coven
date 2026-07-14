//! Restore codes: single-string encoding of everything needed to restore a store from cloud.
//!
//! A restore code encodes the store ID, encryption key, cloud provider details, and
//! credentials into a single base64url string prefixed with "coven:".
//!
//! The code contains secrets (encryption key, S3 credentials). OAuth tokens are NOT included
//! because they expire -- the user re-authenticates on restore.
//!
//! The encryption keyring (`ek`) is present for an opaque home and absent for a
//! browsable one, so `ek`'s presence *is* the home's storage mode: `ek` present
//! ⇒ opaque (encrypted, obfuscated blob paths), `ek` absent ⇒ browsable
//! (plaintext, readable blob paths). The restorer rebuilds both the cipher and
//! the blob-path scheme from that one signal.

use serde::{Deserialize, Serialize};

use crate::code_envelope::{self, EnvelopeError};
use crate::storage::cloud::CloudHomeJoinInfo;
use crate::sync::membership::MembershipCoord;
#[cfg(test)]
use crate::sync::membership::OwnerGrantId;
#[cfg(test)]
use crate::sync::store_commit::ObjectHash;

pub const RESTORE_CODE_VERSION: u8 = 3;

/// Everything needed to restore a store from cloud storage.
///
/// `Debug` is hand-written: `ek` (encryption keyring) and `sk` (Ed25519 signing
/// key) are secrets and print as `<redacted>` so `{:?}` in an error path
/// cannot leak key material.
#[derive(Clone, Serialize, Deserialize)]
pub struct RestoreCode {
    /// Version (currently 2).
    pub v: u8,
    /// Store ID (UUID).
    pub sid: String,
    /// Encryption keyring, present only for an opaque home.
    /// Its presence is the home's storage mode: present ⇒ opaque (the restorer
    /// builds `CloudCipher::Encrypted` + `BlobPathScheme::Hashed`); absent ⇒
    /// browsable (`CloudCipher::Plaintext` + `BlobPathScheme::Plain`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ek: Option<String>,
    /// Store display name.
    pub name: String,
    /// Cloud provider and its connection details. Shared with the invite code
    /// (`InviteCode::join_info`) — one wire representation for both. A
    /// [`CloudHomeJoinInfo::CloudKitShare`] is never valid here: restore
    /// recovers your own zone, not one shared to you, so
    /// [`decode_restore_code`] rejects it.
    pub provider: CloudHomeJoinInfo,
    /// Ed25519 signing key, hex-encoded, 64 bytes. Required.
    pub sk: String,
    pub genesis_hash: super::store_commit::ObjectHash,
    pub founder_pubkey: String,
    /// Every author's membership-head coordinate at mint time
    /// ([`MembershipChain::author_heads`](crate::sync::membership::MembershipChain::author_heads)),
    /// non-empty for every initialized store, including a browsable one. The
    /// restorer seeds its per-author head watermark from this before its first sync
    /// cycle, so a storage provider serving an older, otherwise validly signed
    /// membership state is refused as a regression rather than accepted for
    /// having no watermark yet. Decode requires a non-empty, well-formed floor.
    pub membership_floor: Vec<MembershipCoord>,
}

impl std::fmt::Debug for RestoreCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestoreCode")
            .field("v", &self.v)
            .field("sid", &self.sid)
            // Presence is the storage mode (opaque vs browsable), so show
            // Some/None; the key bytes themselves are redacted.
            .field("ek", &self.ek.as_ref().map(|_| "<redacted>"))
            .field("name", &self.name)
            .field("provider", &self.provider)
            .field("sk", &"<redacted>")
            .field("genesis_hash", &self.genesis_hash)
            .field("founder_pubkey", &self.founder_pubkey)
            .field("membership_floor", &self.membership_floor)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreCodeError {
    #[error("That doesn't look like a coven restore code — it should start with \"coven:\".")]
    MissingPrefix,
    #[error(
        "The restore code is incomplete or has a typo. Check that you copied the entire code."
    )]
    InvalidBase64,
    #[error("The restore code is corrupted. Regenerate it on the source device. ({0})")]
    InvalidJson(String),
    #[error(
        "This restore code was made with an older version of the app (v{0}). Generate a new restore code on the source device."
    )]
    ObsoleteVersion(u8),
    #[error(
        "This restore code was made with a newer version of the app (v{0}). Update the app to use it."
    )]
    NewerVersion(u8),
    /// The restore code's `sid` is not a safe path component, so it cannot name a
    /// store directory under `stores/`. The code is unsigned and anyone can
    /// craft one, so the id is refused here at decode rather than reaching a path
    /// operation.
    #[error(
        "The store id in this restore code is invalid. Regenerate it on the source device. ({0})"
    )]
    InvalidStoreId(crate::store_dir::PathTokenError),
    #[error("The encryption key in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidEncryptionKey(String),
    #[error("The signing key in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidSigningKey(String),
    #[error("The founder key in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidFounderKey(String),
    #[error("The restore code has no membership floor. Regenerate it on the source device.")]
    EmptyMembershipFloor,
    #[error("The membership floor in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidMembershipFloor(String),
    /// A CloudKit share is a zone shared *to* this device by another owner;
    /// restore recovers *your own* zone, so a restore code can never carry
    /// one. Rejected at decode rather than reaching provider setup.
    #[error(
        "This restore code names a shared CloudKit zone, which restore can't use. Restore recovers your own store, not one shared to you — generate a restore code from the device that owns it."
    )]
    CloudKitShareNotRestorable,
}

impl From<EnvelopeError> for RestoreCodeError {
    fn from(e: EnvelopeError) -> Self {
        match e {
            EnvelopeError::MissingPrefix => RestoreCodeError::MissingPrefix,
            EnvelopeError::InvalidBase64 => RestoreCodeError::InvalidBase64,
            EnvelopeError::InvalidJson(s) => RestoreCodeError::InvalidJson(s),
        }
    }
}

/// Encode a `RestoreCode` into a prefixed base64url string.
pub fn encode_restore_code(code: &RestoreCode) -> String {
    code_envelope::encode_code(code_envelope::PREFIX, code)
}

/// Decode a restore code string back into a `RestoreCode`.
pub fn decode_restore_code(s: &str) -> Result<RestoreCode, RestoreCodeError> {
    let code: RestoreCode = code_envelope::decode_code(code_envelope::PREFIX, s)?;
    if code.v < RESTORE_CODE_VERSION {
        return Err(RestoreCodeError::ObsoleteVersion(code.v));
    }
    if code.v > RESTORE_CODE_VERSION {
        return Err(RestoreCodeError::NewerVersion(code.v));
    }
    // A restore code is unsigned, so `sid` is attacker-controlled. It becomes the
    // name of a directory the restorer creates under `stores/` and recursively
    // deletes on a bootstrap failure, so a value carrying `..`, a separator, or an
    // absolute path would put that create/delete outside the stores root. Reject
    // it the moment the code is parsed: a decoded `RestoreCode` always carries a
    // `sid` that is a single safe path component.
    crate::store_dir::validate_path_token(&code.sid).map_err(RestoreCodeError::InvalidStoreId)?;
    // A restore code is unsigned, so a crafted one could name a share the
    // decoder holds no rights to. Restore recovers your own zone, never a
    // shared one, so reject the case structurally at decode.
    if matches!(code.provider, CloudHomeJoinInfo::CloudKitShare { .. }) {
        return Err(RestoreCodeError::CloudKitShareNotRestorable);
    }
    if let Some(key_hex) = &code.ek {
        crate::encryption::EncryptionService::new(key_hex)
            .map_err(|e| RestoreCodeError::InvalidEncryptionKey(e.to_string()))?;
    }
    decode_hex_bytes("signing key", &code.sk, 64).map_err(RestoreCodeError::InvalidSigningKey)?;
    decode_hex_bytes("founder public key", &code.founder_pubkey, 32)
        .map_err(RestoreCodeError::InvalidFounderKey)?;
    if code.membership_floor.is_empty() {
        return Err(RestoreCodeError::EmptyMembershipFloor);
    }
    super::membership_ops::membership_floor_by_grant(&code.membership_floor)
        .map_err(RestoreCodeError::InvalidMembershipFloor)?;
    Ok(code)
}

/// Decode `hex_value` and check it is exactly `expected_len` bytes. Shared
/// with `join_code`'s `owner_pubkey` check, since both are fixed-length
/// hex-encoded key material that must be validated at decode time.
pub(crate) fn decode_hex_bytes(
    label: &str,
    hex_value: &str,
    expected_len: usize,
) -> Result<Vec<u8>, String> {
    let bytes = hex::decode(hex_value).map_err(|e| format!("{label} is not hex: {e}"))?;
    if bytes.len() != expected_len {
        return Err(format!(
            "{label} must be {expected_len} bytes, got {}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

/// Returns true if this provider requires an OAuth flow before restore.
pub fn provider_needs_oauth(provider: &CloudHomeJoinInfo) -> bool {
    matches!(
        provider,
        CloudHomeJoinInfo::GoogleDrive { .. }
            | CloudHomeJoinInfo::Dropbox { .. }
            | CloudHomeJoinInfo::OneDrive { .. }
    )
}

/// UI-ready info from a decoded restore code.
pub struct RestoreCodeInfo {
    pub store_id: String,
    pub store_name: String,
    pub cloud_provider: crate::config::CloudProvider,
    pub needs_oauth: bool,
    /// Ed25519 signing key bytes (always 64 bytes).
    pub signing_key: Vec<u8>,
}

/// Decode a restore code and return UI-ready info.
pub fn decode_restore_code_info(code: &str) -> Result<RestoreCodeInfo, RestoreCodeError> {
    let parsed = decode_restore_code(code)?;

    let cloud_provider = parsed.provider.cloud_provider();

    let signing_key = decode_hex_bytes("signing key", &parsed.sk, 64)
        .map_err(RestoreCodeError::InvalidSigningKey)?;

    Ok(RestoreCodeInfo {
        store_id: parsed.sid,
        store_name: parsed.name,
        cloud_provider,
        needs_oauth: provider_needs_oauth(&parsed.provider),
        signing_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    fn test_sk() -> String {
        hex::encode([0xAB_u8; 64])
    }

    fn test_membership_floor() -> Vec<MembershipCoord> {
        vec![MembershipCoord {
            author_pubkey: hex::encode([0xCDu8; 32]),
            author_owner_grant: OwnerGrantId(ObjectHash::digest(b"test owner grant")),
            seq: 1,
            entry_hash: ObjectHash::digest(b"test membership entry"),
        }]
    }

    fn sample_s3_code() -> RestoreCode {
        RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            ek: Some("aa".repeat(32)),
            name: "Test Store".to_string(),
            provider: CloudHomeJoinInfo::S3 {
                bucket: "my-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some("https://s3.example.com".to_string()),
                key_prefix: None,
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            },
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        }
    }

    #[test]
    fn roundtrip_s3() {
        let code = sample_s3_code();
        let encoded = encode_restore_code(&code);
        assert!(encoded.starts_with("coven:"));

        let decoded = decode_restore_code(&encoded).unwrap();
        assert_eq!(decoded.v, RESTORE_CODE_VERSION);
        assert_eq!(decoded.sid, code.sid);
        assert_eq!(decoded.ek, code.ek);
        assert_eq!(decoded.sk, code.sk);
        assert_eq!(decoded.name, "Test Store");
        assert_eq!(decoded.membership_floor, code.membership_floor);
        match &decoded.provider {
            CloudHomeJoinInfo::S3 {
                bucket,
                region,
                endpoint,
                key_prefix,
                access_key,
                secret_key,
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, "us-east-1");
                assert_eq!(endpoint.as_deref(), Some("https://s3.example.com"));
                assert!(key_prefix.is_none());
                assert_eq!(access_key, "AKIAIOSFODNN7EXAMPLE");
                assert_eq!(secret_key, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
            }
            _ => panic!("expected S3 provider"),
        }
    }

    #[test]
    fn roundtrip_cloudkit() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "lib-123".to_string(),
            ek: Some("bb".repeat(32)),
            name: "CloudKit Store".to_string(),
            provider: CloudHomeJoinInfo::CloudKit,
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        };
        let encoded = encode_restore_code(&code);
        let decoded = decode_restore_code(&encoded).unwrap();
        assert_eq!(decoded.name, "CloudKit Store");
        assert!(matches!(decoded.provider, CloudHomeJoinInfo::CloudKit));
    }

    #[test]
    fn roundtrip_google_drive() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "lib-456".to_string(),
            ek: Some("cc".repeat(32)),
            name: "GDrive Store".to_string(),
            provider: CloudHomeJoinInfo::GoogleDrive {
                folder_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs".to_string(),
            },
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        };
        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        match &decoded.provider {
            CloudHomeJoinInfo::GoogleDrive { folder_id } => {
                assert_eq!(folder_id, "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs");
            }
            _ => panic!("expected GoogleDrive provider"),
        }
    }

    #[test]
    fn roundtrip_dropbox() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "lib-789".to_string(),
            ek: Some("dd".repeat(32)),
            name: "Dropbox Store".to_string(),
            provider: CloudHomeJoinInfo::Dropbox {
                folder_path: "/Apps/your-app/My Store".to_string(),
            },
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        };
        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        match &decoded.provider {
            CloudHomeJoinInfo::Dropbox { folder_path } => {
                assert_eq!(folder_path, "/Apps/your-app/My Store");
            }
            _ => panic!("expected Dropbox provider"),
        }
    }

    #[test]
    fn roundtrip_onedrive() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "lib-abc".to_string(),
            ek: Some("ee".repeat(32)),
            name: "OneDrive Store".to_string(),
            provider: CloudHomeJoinInfo::OneDrive {
                drive_id: "drive-id-123".to_string(),
                folder_id: "folder-id-456".to_string(),
            },
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        };
        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        match &decoded.provider {
            CloudHomeJoinInfo::OneDrive {
                drive_id,
                folder_id,
            } => {
                assert_eq!(drive_id, "drive-id-123");
                assert_eq!(folder_id, "folder-id-456");
            }
            _ => panic!("expected OneDrive provider"),
        }
    }

    /// A restore code naming a CloudKit share is rejected at decode: restore
    /// recovers your own zone, never one shared to you.
    #[test]
    fn decode_rejects_cloudkit_share() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "lib-ck-share".to_string(),
            ek: Some("ff".repeat(32)),
            name: "CloudKit Share Store".to_string(),
            provider: CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://share.example/abc".to_string(),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            },
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        };
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::CloudKitShareNotRestorable)
        ));
    }

    #[test]
    fn missing_prefix() {
        let code = sample_s3_code();
        let encoded = encode_restore_code(&code);
        // Strip the "coven:" prefix
        let without_prefix = &encoded[code_envelope::PREFIX.len()..];
        assert!(matches!(
            decode_restore_code(without_prefix),
            Err(RestoreCodeError::MissingPrefix)
        ));
    }

    #[test]
    fn invalid_base64() {
        assert!(matches!(
            decode_restore_code("coven:not-valid!!!"),
            Err(RestoreCodeError::InvalidBase64)
        ));
    }

    #[test]
    fn invalid_json() {
        let b64 = URL_SAFE_NO_PAD.encode(b"not json");
        let code = format!("coven:{b64}");
        assert!(matches!(
            decode_restore_code(&code),
            Err(RestoreCodeError::InvalidJson(_))
        ));
    }

    #[test]
    fn unsupported_version() {
        let mut code = sample_s3_code();
        code.v = 99;
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::NewerVersion(99))
        ));
    }

    #[test]
    fn v1_base64_signing_key_is_unsupported_version() {
        let mut code = sample_s3_code();
        code.v = 1;
        code.sk = URL_SAFE_NO_PAD.encode([0xAB_u8; 64]);
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::ObsoleteVersion(1))
        ));
    }

    #[test]
    fn whitespace_trimmed() {
        let code = sample_s3_code();
        let encoded = encode_restore_code(&code);
        let padded = format!("  {encoded}  \n");
        let decoded = decode_restore_code(&padded).unwrap();
        assert_eq!(decoded.sid, code.sid);
    }

    #[test]
    fn optional_fields_omitted_in_json() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "lib-1".to_string(),
            ek: Some("aa".repeat(32)),
            name: "Test Store".to_string(),
            provider: CloudHomeJoinInfo::S3 {
                bucket: "b".to_string(),
                region: "r".to_string(),
                endpoint: None,
                key_prefix: None,
                access_key: "ak".to_string(),
                secret_key: "sk-cred".to_string(),
            },
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        };
        let json = serde_json::to_string(&code).unwrap();
        // None fields should not appear in the JSON
        assert!(!json.contains("endpoint"));
        assert!(!json.contains("key_prefix"));
        // Required fields should be present
        assert!(json.contains("name"));
    }

    /// A browsable home's restore code carries no encryption key: `ek` is `None`,
    /// so the field is omitted from the JSON, and it round-trips back to `None`.
    #[test]
    fn browsable_code_omits_ek() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "lib-plain".to_string(),
            ek: None,
            name: "Plaintext Store".to_string(),
            provider: CloudHomeJoinInfo::S3 {
                bucket: "b".to_string(),
                region: "r".to_string(),
                endpoint: None,
                key_prefix: None,
                access_key: "ak".to_string(),
                secret_key: "sk-cred".to_string(),
            },
            sk: test_sk(),
            genesis_hash: crate::sync::store_commit::ObjectHash::digest(b"restore genesis"),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
        };
        let json = serde_json::to_string(&code).unwrap();
        assert!(
            !json.contains("\"ek\""),
            "a browsable home's code must omit ek: {json}"
        );

        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        assert_eq!(decoded.ek, None, "ek round-trips back to None");
    }

    /// An opaque home's restore code carries the key: `ek` is `Some`, present in
    /// the JSON, and round-trips intact.
    #[test]
    fn opaque_code_includes_ek() {
        let code = sample_s3_code();
        assert!(code.ek.is_some());
        let json = serde_json::to_string(&code).unwrap();
        assert!(
            json.contains("\"ek\""),
            "an opaque home's code must include ek: {json}"
        );

        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        assert_eq!(decoded.ek, code.ek, "ek round-trips intact");
    }

    #[test]
    fn needs_oauth() {
        assert!(!provider_needs_oauth(&CloudHomeJoinInfo::S3 {
            bucket: String::new(),
            region: String::new(),
            endpoint: None,
            key_prefix: None,
            access_key: String::new(),
            secret_key: String::new(),
        }));
        assert!(!provider_needs_oauth(&CloudHomeJoinInfo::CloudKit));
        assert!(provider_needs_oauth(&CloudHomeJoinInfo::GoogleDrive {
            folder_id: String::new(),
        }));
        assert!(provider_needs_oauth(&CloudHomeJoinInfo::Dropbox {
            folder_path: String::new(),
        }));
        assert!(provider_needs_oauth(&CloudHomeJoinInfo::OneDrive {
            drive_id: String::new(),
            folder_id: String::new(),
        }));
    }

    #[test]
    fn display_messages_name_cause_and_recovery() {
        let missing = RestoreCodeError::MissingPrefix.to_string();
        assert!(missing.contains("coven:"), "{missing}");
        assert!(missing.contains("coven restore code"), "{missing}");

        let invalid_b64 = RestoreCodeError::InvalidBase64.to_string();
        assert!(
            invalid_b64.contains("incomplete") || invalid_b64.contains("typo"),
            "{invalid_b64}",
        );

        let invalid_json = RestoreCodeError::InvalidJson("trailing comma".to_string()).to_string();
        assert!(invalid_json.contains("Regenerate"), "{invalid_json}");
        assert!(invalid_json.contains("trailing comma"), "{invalid_json}");

        let old_version = RestoreCodeError::ObsoleteVersion(1).to_string();
        assert!(old_version.contains("v1"), "{old_version}");
        assert!(
            old_version.contains("Generate a new restore code"),
            "{old_version}"
        );

        let newer_version = RestoreCodeError::NewerVersion(99).to_string();
        assert!(newer_version.contains("v99"), "{newer_version}");
        assert!(newer_version.contains("Update the app"), "{newer_version}");
    }

    #[test]
    fn invalid_encryption_key_rejected_at_decode() {
        let mut code = sample_s3_code();
        code.ek = Some("not hex".to_string());
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidEncryptionKey(_))
        ));

        let mut code = sample_s3_code();
        code.ek = Some(hex::encode([0u8; 31]));
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidEncryptionKey(_))
        ));
    }

    #[test]
    fn invalid_signing_key_rejected_at_decode() {
        let mut code = sample_s3_code();
        code.sk = "not hex".to_string();
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidSigningKey(_))
        ));

        let mut code = sample_s3_code();
        code.sk = hex::encode([0u8; 63]);
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidSigningKey(_))
        ));
    }

    /// `membership_floor` is required, not merely present-when-known: a code
    /// serialized without it (an older minter, or a hand-crafted attack code)
    /// must be refused at decode rather than silently read as "no floor" — the
    /// exact masking this field exists to remove.
    #[test]
    fn missing_membership_floor_is_refused_at_decode() {
        let mut json = serde_json::to_value(sample_s3_code()).unwrap();
        json.as_object_mut().unwrap().remove("membership_floor");
        let bytes = serde_json::to_vec(&json).unwrap();
        let encoded = format!("coven:{}", URL_SAFE_NO_PAD.encode(bytes));
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidJson(_))
        ));
    }

    #[test]
    fn empty_membership_floor_is_refused_at_decode() {
        let mut code = sample_s3_code();
        code.membership_floor.clear();
        assert!(matches!(
            decode_restore_code(&encode_restore_code(&code)),
            Err(RestoreCodeError::EmptyMembershipFloor)
        ));
    }

    #[test]
    fn debug_redacts_key_material() {
        let code = sample_s3_code();
        let debug = format!("{code:?}");

        assert!(debug.contains("<redacted>"), "{debug}");
        // Non-secret fields are still visible.
        assert!(debug.contains("Test Store"), "{debug}");
        assert!(debug.contains("my-bucket"), "{debug}");
        // The encryption keyring and signing key never appear.
        let ek_hex = code.ek.as_deref().expect("sample has ek");
        assert!(!debug.contains(ek_hex), "encryption key leaked: {debug}");
        assert!(!debug.contains(&code.sk), "signing key leaked: {debug}");
        // ek presence (the storage mode) is still observable.
        assert!(debug.contains("ek: Some"), "{debug}");
    }
}
