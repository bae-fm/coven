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

use crate::protocol::membership::MembershipFloor;
#[cfg(test)]
use crate::protocol::membership::{MembershipCoord, MembershipGrantId, MembershipHeadRef};
#[cfg(test)]
use crate::protocol::store_commit::ObjectHash;
use crate::storage::cloud::CloudHomeJoinInfo;
use coven_foundation::code_envelope::{self, EnvelopeError};

pub(crate) const RESTORE_CODE_VERSION: u8 = 4;

use crate::protocol::recovery::RestoreAuthority;

/// Everything needed to restore a store from cloud storage.
///
/// `Debug` is hand-written: the encryption keyring and signing keys are
/// secrets and print as `<redacted>` so `{:?}` in an error path
/// cannot leak key material.
#[derive(Clone, Serialize, Deserialize)]
pub struct RestoreCode {
    /// Wire-format version.
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
    pub store_root: crate::protocol::store_commit::StoreRootRef,
    pub founder_pubkey: String,
    /// The exact causal membership heads the restorer must observe.
    pub membership_floor: MembershipFloor,
    pub authority: RestoreAuthority,
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
            .field("store_root", &self.store_root)
            .field("founder_pubkey", &self.founder_pubkey)
            .field("membership_floor", &self.membership_floor)
            .field("authority", &self.authority)
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
    #[error("This restore code uses unsupported format version v{0}. Generate a new restore code on the source device.")]
    UnsupportedVersion(u8),
    /// The restore code's `sid` is not a safe path component, so it cannot name a
    /// store directory under `stores/`. The code is unsigned and anyone can
    /// craft one, so the id is refused here at decode rather than reaching a path
    /// operation.
    #[error(
        "The store id in this restore code is invalid. Regenerate it on the source device. ({0})"
    )]
    InvalidStoreId(coven_foundation::store_dir::PathTokenError),
    #[error("The encryption key in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidEncryptionKey(String),
    #[error(
        "A signing key in this restore code is invalid. Regenerate it on the source device. ({0})"
    )]
    InvalidSigningKey(String),
    #[error("The Owner recovery authority in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidRecoveryAuthority(String),
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
pub(crate) fn encode_restore_code(code: &RestoreCode) -> String {
    code_envelope::encode_code(code_envelope::PREFIX, code)
}

/// Decode a restore code string back into a `RestoreCode`.
pub(crate) fn decode_restore_code(s: &str) -> Result<RestoreCode, RestoreCodeError> {
    let code: RestoreCode = code_envelope::decode_code(code_envelope::PREFIX, s)?;
    if code.v != RESTORE_CODE_VERSION {
        return Err(RestoreCodeError::UnsupportedVersion(code.v));
    }
    // A restore code is unsigned, so `sid` is attacker-controlled. It becomes the
    // name of a directory the restorer creates under `stores/` and recursively
    // deletes on a bootstrap failure, so a value carrying `..`, a separator, or an
    // absolute path would put that create/delete outside the stores root. Reject
    // it the moment the code is parsed: a decoded `RestoreCode` always carries a
    // `sid` that is a single safe path component.
    coven_foundation::store_dir::validate_path_token(&code.sid)
        .map_err(RestoreCodeError::InvalidStoreId)?;
    // A restore code is unsigned, so a crafted one could name a share the
    // decoder holds no rights to. Restore recovers your own zone, never a
    // shared one, so reject the case structurally at decode.
    if matches!(code.provider, CloudHomeJoinInfo::CloudKitShare { .. }) {
        return Err(RestoreCodeError::CloudKitShareNotRestorable);
    }
    if let Some(serialized_keyring) = &code.ek {
        coven_keys::encryption::EncryptionService::new(serialized_keyring)
            .map_err(|e| RestoreCodeError::InvalidEncryptionKey(e.to_string()))?;
    }
    match &code.authority {
        RestoreAuthority::ActivatedContinuation(continuation) => {
            coven_foundation::code_envelope::decode_fixed_hex(
                "identity signing key",
                &continuation.identity_signing_secret,
                64,
            )
            .map_err(RestoreCodeError::InvalidSigningKey)?;
            coven_foundation::code_envelope::decode_fixed_hex(
                "device signing key",
                &continuation.device_signing_secret,
                64,
            )
            .map_err(RestoreCodeError::InvalidSigningKey)?;
        }
        RestoreAuthority::OwnerRecovery(recovery) => {
            coven_foundation::code_envelope::decode_fixed_hex(
                "Owner identity signing key",
                &recovery.owner_identity_secret,
                64,
            )
            .map_err(RestoreCodeError::InvalidSigningKey)?;
            if recovery.recovery.owner_grant != recovery.owner_grant {
                return Err(RestoreCodeError::InvalidRecoveryAuthority(
                    "Owner recovery cursor belongs to another grant".to_string(),
                ));
            }
        }
    }
    coven_foundation::code_envelope::decode_fixed_hex(
        "founder public key",
        &code.founder_pubkey,
        32,
    )
    .map_err(RestoreCodeError::InvalidFounderKey)?;
    if code.membership_floor.0.is_empty() {
        return Err(RestoreCodeError::EmptyMembershipFloor);
    }
    code.membership_floor
        .validate()
        .map_err(RestoreCodeError::InvalidMembershipFloor)?;
    Ok(code)
}

/// UI-ready info from a decoded restore code.
pub struct RestoreCodeInfo {
    pub store_id: String,
    pub store_name: String,
    pub cloud_provider: coven_foundation::config::CloudProvider,
    pub needs_oauth: bool,
}

/// Decode a restore code and return UI-ready info.
pub fn decode_restore_code_info(code: &str) -> Result<RestoreCodeInfo, RestoreCodeError> {
    let parsed = decode_restore_code(code)?;

    let cloud_provider = parsed.provider.cloud_provider();

    Ok(RestoreCodeInfo {
        store_id: parsed.sid,
        store_name: parsed.name,
        needs_oauth: cloud_provider.needs_oauth(),
        cloud_provider,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::recovery::OwnerRecoveryAuthority;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    fn test_sk() -> String {
        hex::encode([0xAB_u8; 64])
    }

    fn test_keyring(byte: u8) -> String {
        coven_keys::encryption::MasterKeyring::from(
            coven_keys::encryption::EncryptionService::from_key([byte; 32]),
        )
        .to_serialized()
    }

    fn test_store_root() -> crate::protocol::store_commit::StoreRootRef {
        let stored = b"restore protocol root object";
        crate::protocol::store_commit::StoreRootRef {
            store_root_id: ObjectHash::digest(b"restore protocol root identity"),
            store_root_hash: ObjectHash::digest(stored),
            object: crate::protocol::objects::ExactObjectRef::new(
                crate::protocol::objects::ObjectSlot::logical(
                    "store-v1/protocol/root/restore-code-test.json".to_string(),
                )
                .expect("valid test Store-root slot"),
                stored.len() as u64,
                ObjectHash::digest(stored),
            ),
        }
    }

    fn test_membership_floor() -> MembershipFloor {
        let coord = MembershipCoord {
            author_pubkey: hex::encode([0xCDu8; 32]),
            author_owner_grant: MembershipGrantId(ObjectHash::digest(b"test owner grant")),
            stream_id: crate::protocol::membership::AuthorStreamId::from_bytes([1; 32]),
            seq: 1,
            entry_hash: ObjectHash::digest(b"test membership entry"),
        };
        let stored = b"test restore membership head";
        MembershipFloor(vec![MembershipHeadRef {
            coord,
            head_hash: ObjectHash::digest(b"test restore membership head semantic bytes"),
            object: crate::protocol::objects::ExactObjectRef::new(
                crate::protocol::objects::ObjectSlot::logical(
                    "store-v1/membership/heads/test-restore-owner/1.json".to_string(),
                )
                .expect("valid restore membership-head slot"),
                stored.len() as u64,
                ObjectHash::digest(stored),
            ),
        }])
    }

    fn test_authority() -> RestoreAuthority {
        let owner_grant = MembershipGrantId(ObjectHash::digest(b"test owner grant"));
        let first_slot = crate::protocol::objects::ObjectSlot::logical(
            "store-v1/recovery/test-owner/first.json".to_string(),
        )
        .expect("valid recovery slot");
        let anchor = crate::protocol::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot };
        let owner_pubkey = hex::encode([0xCDu8; 32]);
        let activation = crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
            &test_store_root(),
            &owner_pubkey,
            &owner_grant,
            &anchor,
        )
        .expect("valid recovery activation");
        RestoreAuthority::OwnerRecovery(OwnerRecoveryAuthority {
            owner_identity_secret: test_sk(),
            owner_grant: owner_grant.clone(),
            recovery: crate::protocol::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: crate::protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                    activation,
                },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        })
    }

    fn sample_s3_code() -> RestoreCode {
        RestoreCode {
            v: RESTORE_CODE_VERSION,
            sid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            ek: Some(test_keyring(0xaa)),
            name: "Test Store".to_string(),
            provider: CloudHomeJoinInfo::S3 {
                bucket: "my-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some("https://s3.example.com".to_string()),
                key_prefix: None,
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            },
            store_root: test_store_root(),
            founder_pubkey: hex::encode([0xCDu8; 32]),
            membership_floor: test_membership_floor(),
            authority: test_authority(),
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
        assert_eq!(
            serde_json::to_value(&decoded.authority).unwrap(),
            serde_json::to_value(&code.authority).unwrap()
        );
        assert_eq!(decoded.name, "Test Store");
        assert_eq!(decoded.store_root, code.store_root);
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
            ek: Some(test_keyring(0xbb)),
            name: "CloudKit Store".to_string(),
            provider: CloudHomeJoinInfo::CloudKit,
            authority: test_authority(),
            store_root: test_store_root(),
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
            ek: Some(test_keyring(0xcc)),
            name: "GDrive Store".to_string(),
            provider: CloudHomeJoinInfo::GoogleDrive {
                folder_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs".to_string(),
            },
            authority: test_authority(),
            store_root: test_store_root(),
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
            ek: Some(test_keyring(0xdd)),
            name: "Dropbox Store".to_string(),
            provider: CloudHomeJoinInfo::Dropbox {
                folder_path: "/Apps/your-app/My Store".to_string(),
            },
            authority: test_authority(),
            store_root: test_store_root(),
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
            ek: Some(test_keyring(0xee)),
            name: "OneDrive Store".to_string(),
            provider: CloudHomeJoinInfo::OneDrive {
                drive_id: "drive-id-123".to_string(),
                folder_id: "folder-id-456".to_string(),
            },
            authority: test_authority(),
            store_root: test_store_root(),
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
            ek: Some(test_keyring(0xff)),
            name: "CloudKit Share Store".to_string(),
            provider: CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://share.example/abc".to_string(),
                owner_name: "owner".to_string(),
                zone_name: "zone".to_string(),
            },
            authority: test_authority(),
            store_root: test_store_root(),
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
            Err(RestoreCodeError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn lower_unsupported_version_is_rejected_before_field_validation() {
        let mut code = sample_s3_code();
        code.v = 0;
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::UnsupportedVersion(0))
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
            ek: Some(test_keyring(0xaa)),
            name: "Test Store".to_string(),
            provider: CloudHomeJoinInfo::S3 {
                bucket: "b".to_string(),
                region: "r".to_string(),
                endpoint: None,
                key_prefix: None,
                access_key: "ak".to_string(),
                secret_key: "sk-cred".to_string(),
            },
            authority: test_authority(),
            store_root: test_store_root(),
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
            authority: test_authority(),
            store_root: test_store_root(),
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
    fn decoded_info_uses_the_provider_oauth_requirement() {
        let s3 = sample_s3_code();
        let s3_info = decode_restore_code_info(&encode_restore_code(&s3)).unwrap();
        assert!(!s3_info.needs_oauth);

        let mut google_drive = sample_s3_code();
        google_drive.provider = CloudHomeJoinInfo::GoogleDrive {
            folder_id: "folder".to_string(),
        };
        let google_drive_info =
            decode_restore_code_info(&encode_restore_code(&google_drive)).unwrap();
        assert!(google_drive_info.needs_oauth);
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

        let lower_version = RestoreCodeError::UnsupportedVersion(0).to_string();
        assert!(lower_version.contains("v0"), "{lower_version}");
        assert!(
            lower_version.contains("Generate a new restore code"),
            "{lower_version}"
        );

        let higher_version = RestoreCodeError::UnsupportedVersion(99).to_string();
        assert!(higher_version.contains("v99"), "{higher_version}");
        assert!(
            higher_version.contains("Generate a new restore code"),
            "{higher_version}"
        );
    }

    #[test]
    fn invalid_encryption_key_rejected_at_decode() {
        let mut code = sample_s3_code();
        code.ek = Some("not keyring JSON".to_string());
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidEncryptionKey(_))
        ));

        let mut code = sample_s3_code();
        code.ek = Some(hex::encode([0u8; 32]));
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidEncryptionKey(_))
        ));
    }

    #[test]
    fn invalid_signing_key_rejected_at_decode() {
        let mut code = sample_s3_code();
        let RestoreAuthority::OwnerRecovery(authority) = &mut code.authority else {
            panic!("test authority is Owner recovery")
        };
        authority.owner_identity_secret = "not hex".to_string();
        let encoded = encode_restore_code(&code);
        assert!(matches!(
            decode_restore_code(&encoded),
            Err(RestoreCodeError::InvalidSigningKey(_))
        ));

        let mut code = sample_s3_code();
        let RestoreAuthority::OwnerRecovery(authority) = &mut code.authority else {
            panic!("test authority is Owner recovery")
        };
        authority.owner_identity_secret = hex::encode([0u8; 63]);
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
        code.membership_floor = MembershipFloor(Vec::new());
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
        let RestoreAuthority::OwnerRecovery(authority) = &code.authority else {
            panic!("test authority is Owner recovery")
        };
        assert!(
            !debug.contains(&authority.owner_identity_secret),
            "signing key leaked: {debug}"
        );
        // ek presence (the storage mode) is still observable.
        assert!(debug.contains("ek: Some"), "{debug}");
    }
}
