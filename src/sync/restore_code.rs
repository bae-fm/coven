//! Restore codes: single-string encoding of everything needed to restore a library from cloud.
//!
//! A restore code encodes the library ID, encryption key, cloud provider details, and
//! credentials into a single base64url string prefixed with "coven:".
//!
//! The code contains secrets (encryption key, S3 credentials). OAuth tokens are NOT included
//! because they expire -- the user re-authenticates on restore.
//!
//! The encryption key (`ek`) is present for an opaque home and absent for a
//! browsable one, so `ek`'s presence *is* the home's storage mode: `ek` present
//! ⇒ opaque (encrypted, obfuscated blob paths), `ek` absent ⇒ browsable
//! (plaintext, readable blob paths). The restorer rebuilds both the cipher and
//! the blob-path scheme from that one signal.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

const PREFIX: &str = "coven:";

/// Everything needed to restore a library from cloud storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreCode {
    /// Version (currently 1).
    pub v: u8,
    /// Library ID (UUID).
    pub lid: String,
    /// Encryption key (hex-encoded, 64 chars), present only for an opaque home.
    /// Its presence is the home's storage mode: present ⇒ opaque (the restorer
    /// builds `CloudCipher::Encrypted` + `BlobPathScheme::Hashed`); absent ⇒
    /// browsable (`CloudCipher::Plaintext` + `BlobPathScheme::Plain`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ek: Option<String>,
    /// Library display name.
    pub name: String,
    /// Cloud provider and its connection details.
    pub provider: RestoreProvider,
    /// Ed25519 signing key, base64url-encoded, 64 bytes. Required.
    pub sk: String,
}

/// Cloud provider details. Each variant carries only the fields needed for that provider.
/// OAuth tokens are NOT stored (they expire); the user re-authenticates during restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        "This restore code was made with a newer version of the app (v{0}). Update the app to use it."
    )]
    UnsupportedVersion(u8),
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
    if code.v != 1 {
        return Err(RestoreCodeError::UnsupportedVersion(code.v));
    }
    Ok(code)
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

    let signing_key = URL_SAFE_NO_PAD
        .decode(&parsed.sk)
        .map_err(|e| RestoreCodeError::InvalidJson(format!("Invalid signing key encoding: {e}")))?;

    if signing_key.len() != 64 {
        return Err(RestoreCodeError::InvalidJson(format!(
            "Signing key must be 64 bytes, got {}",
            signing_key.len()
        )));
    }

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
        URL_SAFE_NO_PAD.encode([0xAB_u8; 64])
    }

    fn sample_s3_code() -> RestoreCode {
        RestoreCode {
            v: 1,
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
        assert_eq!(decoded.v, 1);
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
            v: 1,
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
            v: 1,
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
            v: 1,
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
            v: 1,
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
            Err(RestoreCodeError::UnsupportedVersion(99))
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
            v: 1,
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
            v: 1,
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

        let bad_version = RestoreCodeError::UnsupportedVersion(99).to_string();
        assert!(bad_version.contains("v99"), "{bad_version}");
        assert!(bad_version.contains("Update the app"), "{bad_version}");
    }
}
