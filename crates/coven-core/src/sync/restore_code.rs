//! Restore codes: single-string encoding of everything needed to restore a library from cloud.
//!
//! A restore code encodes the library ID, encryption key, cloud provider details, and
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

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

const PREFIX: &str = "coven:";
pub const RESTORE_CODE_VERSION: u8 = 2;

/// Everything needed to restore a library from cloud storage.
///
/// `Debug` is hand-written: `ek` (encryption keyring) and `sk` (Ed25519 signing
/// key) are secrets and print as `<redacted>` so `{:?}` in an error path
/// cannot leak key material.
#[derive(Clone, Serialize, Deserialize)]
pub struct RestoreCode {
    /// Version (currently 2).
    pub v: u8,
    /// Library ID (UUID).
    pub lid: String,
    /// Encryption keyring, present only for an opaque home.
    /// Its presence is the home's storage mode: present ⇒ opaque (the restorer
    /// builds `CloudCipher::Encrypted` + `BlobPathScheme::Hashed`); absent ⇒
    /// browsable (`CloudCipher::Plaintext` + `BlobPathScheme::Plain`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ek: Option<String>,
    /// Library display name.
    pub name: String,
    /// Cloud provider and its connection details.
    pub provider: RestoreProvider,
    /// Ed25519 signing key, hex-encoded, 64 bytes. Required.
    pub sk: String,
}

impl std::fmt::Debug for RestoreCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestoreCode")
            .field("v", &self.v)
            .field("lid", &self.lid)
            // Presence is the storage mode (opaque vs browsable), so show
            // Some/None; the key bytes themselves are redacted.
            .field("ek", &self.ek.as_ref().map(|_| "<redacted>"))
            .field("name", &self.name)
            .field("provider", &self.provider)
            .field("sk", &"<redacted>")
            .finish()
    }
}

/// Cloud provider details. Each variant carries only the fields needed for that provider.
/// OAuth tokens are NOT stored (they expire); the user re-authenticates during restore.
///
/// `Debug` is hand-written so the S3 `secret_key` prints as `<redacted>`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum RestoreProvider {
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        region: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
        access_key: String,
        secret_key: String,
    },
    #[serde(rename = "ck")]
    CloudKit,
    #[serde(rename = "gd")]
    GoogleDrive { folder_id: String },
    #[serde(rename = "db")]
    Dropbox { folder_path: String },
    #[serde(rename = "od")]
    OneDrive { drive_id: String, folder_id: String },
}

impl std::fmt::Debug for RestoreProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestoreProvider::S3 {
                bucket,
                region,
                endpoint,
                key_prefix,
                access_key,
                secret_key: _,
            } => f
                .debug_struct("S3")
                .field("bucket", bucket)
                .field("region", region)
                .field("endpoint", endpoint)
                .field("key_prefix", key_prefix)
                .field("access_key", access_key)
                .field("secret_key", &"<redacted>")
                .finish(),
            RestoreProvider::CloudKit => f.write_str("CloudKit"),
            RestoreProvider::GoogleDrive { folder_id } => f
                .debug_struct("GoogleDrive")
                .field("folder_id", folder_id)
                .finish(),
            RestoreProvider::Dropbox { folder_path } => f
                .debug_struct("Dropbox")
                .field("folder_path", folder_path)
                .finish(),
            RestoreProvider::OneDrive {
                drive_id,
                folder_id,
            } => f
                .debug_struct("OneDrive")
                .field("drive_id", drive_id)
                .field("folder_id", folder_id)
                .finish(),
        }
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
    /// The restore code's `lid` is not a safe path component, so it cannot name a
    /// library directory under `libraries/`. The code is unsigned and anyone can
    /// craft one, so the id is refused here at decode rather than reaching a path
    /// operation.
    #[error(
        "The library id in this restore code is invalid. Regenerate it on the source device. ({0})"
    )]
    InvalidLibraryId(crate::library_dir::PathTokenError),
    #[error("The encryption key in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidEncryptionKey(String),
    #[error("The signing key in this restore code is invalid. Regenerate it on the source device. ({0})")]
    InvalidSigningKey(String),
}

/// Encode a `RestoreCode` into a prefixed base64url string.
pub fn encode_restore_code(code: &RestoreCode) -> String {
    let json = serde_json::to_string(code).expect("RestoreCode serialization cannot fail");
    let b64 = URL_SAFE_NO_PAD.encode(json.as_bytes());
    format!("{PREFIX}{b64}")
}

/// Decode a restore code string back into a `RestoreCode`.
pub fn decode_restore_code(s: &str) -> Result<RestoreCode, RestoreCodeError> {
    let trimmed = s.trim();
    let payload = trimmed
        .strip_prefix(PREFIX)
        .ok_or(RestoreCodeError::MissingPrefix)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| RestoreCodeError::InvalidBase64)?;
    let code: RestoreCode =
        serde_json::from_slice(&bytes).map_err(|e| RestoreCodeError::InvalidJson(e.to_string()))?;
    if code.v < RESTORE_CODE_VERSION {
        return Err(RestoreCodeError::ObsoleteVersion(code.v));
    }
    if code.v > RESTORE_CODE_VERSION {
        return Err(RestoreCodeError::NewerVersion(code.v));
    }
    // A restore code is unsigned, so `lid` is attacker-controlled. It becomes the
    // name of a directory the restorer creates under `libraries/` and recursively
    // deletes on a bootstrap failure, so a value carrying `..`, a separator, or an
    // absolute path would put that create/delete outside the libraries root. Reject
    // it the moment the code is parsed: a decoded `RestoreCode` always carries a
    // `lid` that is a single safe path component.
    crate::library_dir::validate_path_token(&code.lid)
        .map_err(RestoreCodeError::InvalidLibraryId)?;
    if let Some(key_hex) = &code.ek {
        crate::encryption::EncryptionService::new(key_hex)
            .map_err(|e| RestoreCodeError::InvalidEncryptionKey(e.to_string()))?;
    }
    decode_hex_bytes("signing key", &code.sk, 64).map_err(RestoreCodeError::InvalidSigningKey)?;
    Ok(code)
}

fn decode_hex_bytes(label: &str, hex_value: &str, expected_len: usize) -> Result<Vec<u8>, String> {
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
pub fn provider_needs_oauth(provider: &RestoreProvider) -> bool {
    matches!(
        provider,
        RestoreProvider::GoogleDrive { .. }
            | RestoreProvider::Dropbox { .. }
            | RestoreProvider::OneDrive { .. }
    )
}

/// UI-ready info from a decoded restore code.
pub struct RestoreCodeInfo {
    pub library_id: String,
    pub library_name: String,
    pub cloud_provider: crate::config::CloudProvider,
    pub needs_oauth: bool,
    /// Ed25519 signing key bytes (always 64 bytes).
    pub signing_key: Vec<u8>,
}

/// Decode a restore code and return UI-ready info.
pub fn decode_restore_code_info(code: &str) -> Result<RestoreCodeInfo, RestoreCodeError> {
    let parsed = decode_restore_code(code)?;

    let cloud_provider = match &parsed.provider {
        RestoreProvider::S3 { .. } => crate::config::CloudProvider::S3,
        RestoreProvider::CloudKit => crate::config::CloudProvider::CloudKit,
        RestoreProvider::GoogleDrive { .. } => crate::config::CloudProvider::GoogleDrive,
        RestoreProvider::Dropbox { .. } => crate::config::CloudProvider::Dropbox,
        RestoreProvider::OneDrive { .. } => crate::config::CloudProvider::OneDrive,
    };

    let signing_key = decode_hex_bytes("signing key", &parsed.sk, 64)
        .map_err(RestoreCodeError::InvalidSigningKey)?;

    Ok(RestoreCodeInfo {
        library_id: parsed.lid,
        library_name: parsed.name,
        cloud_provider,
        needs_oauth: provider_needs_oauth(&parsed.provider),
        signing_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sk() -> String {
        hex::encode([0xAB_u8; 64])
    }

    fn sample_s3_code() -> RestoreCode {
        RestoreCode {
            v: RESTORE_CODE_VERSION,
            lid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            ek: Some("aa".repeat(32)),
            name: "Test Library".to_string(),
            provider: RestoreProvider::S3 {
                bucket: "my-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some("https://s3.example.com".to_string()),
                key_prefix: None,
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            },
            sk: test_sk(),
        }
    }

    #[test]
    fn roundtrip_s3() {
        let code = sample_s3_code();
        let encoded = encode_restore_code(&code);
        assert!(encoded.starts_with("coven:"));

        let decoded = decode_restore_code(&encoded).unwrap();
        assert_eq!(decoded.v, RESTORE_CODE_VERSION);
        assert_eq!(decoded.lid, code.lid);
        assert_eq!(decoded.ek, code.ek);
        assert_eq!(decoded.sk, code.sk);
        assert_eq!(decoded.name, "Test Library");
        match &decoded.provider {
            RestoreProvider::S3 {
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
            lid: "lib-123".to_string(),
            ek: Some("bb".repeat(32)),
            name: "CloudKit Library".to_string(),
            provider: RestoreProvider::CloudKit,
            sk: test_sk(),
        };
        let encoded = encode_restore_code(&code);
        let decoded = decode_restore_code(&encoded).unwrap();
        assert_eq!(decoded.name, "CloudKit Library");
        assert!(matches!(decoded.provider, RestoreProvider::CloudKit));
    }

    #[test]
    fn roundtrip_google_drive() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            lid: "lib-456".to_string(),
            ek: Some("cc".repeat(32)),
            name: "GDrive Library".to_string(),
            provider: RestoreProvider::GoogleDrive {
                folder_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs".to_string(),
            },
            sk: test_sk(),
        };
        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        match &decoded.provider {
            RestoreProvider::GoogleDrive { folder_id } => {
                assert_eq!(folder_id, "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs");
            }
            _ => panic!("expected GoogleDrive provider"),
        }
    }

    #[test]
    fn roundtrip_dropbox() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            lid: "lib-789".to_string(),
            ek: Some("dd".repeat(32)),
            name: "Dropbox Library".to_string(),
            provider: RestoreProvider::Dropbox {
                folder_path: "/Apps/your-app/My Library".to_string(),
            },
            sk: test_sk(),
        };
        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        match &decoded.provider {
            RestoreProvider::Dropbox { folder_path } => {
                assert_eq!(folder_path, "/Apps/your-app/My Library");
            }
            _ => panic!("expected Dropbox provider"),
        }
    }

    #[test]
    fn roundtrip_onedrive() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            lid: "lib-abc".to_string(),
            ek: Some("ee".repeat(32)),
            name: "OneDrive Library".to_string(),
            provider: RestoreProvider::OneDrive {
                drive_id: "drive-id-123".to_string(),
                folder_id: "folder-id-456".to_string(),
            },
            sk: test_sk(),
        };
        let decoded = decode_restore_code(&encode_restore_code(&code)).unwrap();
        match &decoded.provider {
            RestoreProvider::OneDrive {
                drive_id,
                folder_id,
            } => {
                assert_eq!(drive_id, "drive-id-123");
                assert_eq!(folder_id, "folder-id-456");
            }
            _ => panic!("expected OneDrive provider"),
        }
    }

    #[test]
    fn missing_prefix() {
        let code = sample_s3_code();
        let encoded = encode_restore_code(&code);
        // Strip the "coven:" prefix
        let without_prefix = &encoded[PREFIX.len()..];
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
        assert_eq!(decoded.lid, code.lid);
    }

    #[test]
    fn optional_fields_omitted_in_json() {
        let code = RestoreCode {
            v: RESTORE_CODE_VERSION,
            lid: "lib-1".to_string(),
            ek: Some("aa".repeat(32)),
            name: "Test Library".to_string(),
            provider: RestoreProvider::S3 {
                bucket: "b".to_string(),
                region: "r".to_string(),
                endpoint: None,
                key_prefix: None,
                access_key: "ak".to_string(),
                secret_key: "sk-cred".to_string(),
            },
            sk: test_sk(),
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
            lid: "lib-plain".to_string(),
            ek: None,
            name: "Plaintext Library".to_string(),
            provider: RestoreProvider::S3 {
                bucket: "b".to_string(),
                region: "r".to_string(),
                endpoint: None,
                key_prefix: None,
                access_key: "ak".to_string(),
                secret_key: "sk-cred".to_string(),
            },
            sk: test_sk(),
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
        assert!(!provider_needs_oauth(&RestoreProvider::S3 {
            bucket: String::new(),
            region: String::new(),
            endpoint: None,
            key_prefix: None,
            access_key: String::new(),
            secret_key: String::new(),
        }));
        assert!(!provider_needs_oauth(&RestoreProvider::CloudKit));
        assert!(provider_needs_oauth(&RestoreProvider::GoogleDrive {
            folder_id: String::new(),
        }));
        assert!(provider_needs_oauth(&RestoreProvider::Dropbox {
            folder_path: String::new(),
        }));
        assert!(provider_needs_oauth(&RestoreProvider::OneDrive {
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

    #[test]
    fn debug_redacts_key_material() {
        let code = sample_s3_code();
        let debug = format!("{code:?}");

        assert!(debug.contains("<redacted>"), "{debug}");
        // Non-secret fields are still visible.
        assert!(debug.contains("Test Library"), "{debug}");
        assert!(debug.contains("my-bucket"), "{debug}");
        // The encryption keyring and signing key never appear.
        let ek_hex = code.ek.as_deref().expect("sample has ek");
        assert!(!debug.contains(ek_hex), "encryption key leaked: {debug}");
        assert!(!debug.contains(&code.sk), "signing key leaked: {debug}");
        // ek presence (the storage mode) is still observable.
        assert!(debug.contains("ek: Some"), "{debug}");
    }

    #[test]
    fn provider_debug_redacts_s3_secret_key() {
        let provider = RestoreProvider::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            key_prefix: None,
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI-secret-value".to_string(),
        };
        let debug = format!("{provider:?}");

        assert!(debug.contains("<redacted>"), "{debug}");
        assert!(debug.contains("AKIAIOSFODNN7EXAMPLE"), "{debug}");
        assert!(
            !debug.contains("wJalrXUtnFEMI-secret-value"),
            "S3 secret key leaked: {debug}"
        );
    }
}
