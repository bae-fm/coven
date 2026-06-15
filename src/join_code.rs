use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::storage::cloud::CloudHomeJoinInfo;

#[derive(Serialize, Deserialize)]
pub struct InviteCode {
    pub library_id: String,
    pub library_name: String,
    pub join_info: CloudHomeJoinInfo,
    pub owner_pubkey: String,
    /// Whether the home obfuscates blob paths (the default content-addressed
    /// layout) vs. the consumer's readable path. A joining device must know the
    /// scheme to compute the same blob keys the owner writes. Omitted (and decoded
    /// back to `true`) for an obfuscated home, matching every code written before
    /// this field.
    #[serde(
        default = "default_obfuscate_blob_paths",
        skip_serializing_if = "is_default_obfuscate_blob_paths"
    )]
    pub obfuscate_blob_paths: bool,
}

/// The default for [`InviteCode::obfuscate_blob_paths`]: an absent value means the
/// obfuscated (content-addressed) layout, matching every code written before this
/// field existed.
fn default_obfuscate_blob_paths() -> bool {
    true
}

/// Skip serializing [`InviteCode::obfuscate_blob_paths`] when it holds the default
/// (obfuscated), so an ordinary invite code stays compact.
fn is_default_obfuscate_blob_paths(obfuscate: &bool) -> bool {
    *obfuscate
}

pub fn encode(code: &InviteCode) -> String {
    let json = serde_json::to_vec(code).expect("InviteCode is always serializable");
    URL_SAFE_NO_PAD.encode(&json)
}

pub fn decode(s: &str) -> Result<InviteCode, JoinCodeError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(s.trim())
        .map_err(|_| JoinCodeError::InvalidBase64)?;
    serde_json::from_slice(&bytes).map_err(|e| JoinCodeError::InvalidJson(e.to_string()))
}

#[derive(Serialize, Deserialize)]
pub struct JoinRequestCode {
    pub public_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// Generate a join request code containing this device's Ed25519 public key.
/// Creates a keypair if one doesn't exist yet.
pub fn generate_join_request(
    needs_email: bool,
    email: String,
) -> Result<String, crate::keys::KeyError> {
    let global_ks = crate::keys::KeyService::new("global".to_string());
    let keypair = global_ks.get_or_create_user_keypair()?;

    let code = JoinRequestCode {
        public_key: hex::encode(keypair.public_key),
        email: if needs_email { Some(email) } else { None },
    };

    Ok(encode_join_request(&code))
}

pub fn encode_join_request(code: &JoinRequestCode) -> String {
    let json = serde_json::to_vec(code).expect("JoinRequestCode is always serializable");
    URL_SAFE_NO_PAD.encode(&json)
}

pub fn decode_join_request(s: &str) -> Result<JoinRequestCode, JoinCodeError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(s.trim())
        .map_err(|_| JoinCodeError::InvalidBase64)?;
    serde_json::from_slice(&bytes).map_err(|e| JoinCodeError::InvalidJson(e.to_string()))
}

/// UI-ready info from a decoded invite code.
pub struct InviteCodeInfo {
    pub library_id: String,
    pub library_name: String,
    pub owner_pubkey: String,
    pub cloud_provider: crate::config::CloudProvider,
}

/// Decode an invite code and return UI-ready info.
pub fn decode_invite_code_info(code: &str) -> Result<InviteCodeInfo, JoinCodeError> {
    let invite = decode(code)?;
    let cloud_provider = invite.join_info.cloud_provider();
    Ok(InviteCodeInfo {
        library_id: invite.library_id,
        library_name: invite.library_name,
        owner_pubkey: invite.owner_pubkey,
        cloud_provider,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum JoinCodeError {
    #[error("invalid base64url encoding")]
    InvalidBase64,
    #[error("invalid invite code payload: {0}")]
    InvalidJson(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_s3() {
        let code = InviteCode {
            library_id: "lib-123".into(),
            library_name: "My Library".into(),
            join_info: CloudHomeJoinInfo::S3 {
                bucket: "my-bucket".into(),
                region: "us-east-1".into(),
                endpoint: None,
                access_key: "AKIAEXAMPLE".into(),
                secret_key: "secret123".into(),
                key_prefix: None,
            },
            owner_pubkey: "deadbeef".into(),
            obfuscate_blob_paths: true,
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.library_id, "lib-123");
        assert_eq!(decoded.library_name, "My Library");
        assert_eq!(decoded.owner_pubkey, "deadbeef");
        match decoded.join_info {
            CloudHomeJoinInfo::S3 {
                bucket,
                region,
                endpoint,
                access_key,
                secret_key,
                key_prefix,
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, "us-east-1");
                assert_eq!(endpoint, None);
                assert_eq!(access_key, "AKIAEXAMPLE");
                assert_eq!(secret_key, "secret123");
                assert_eq!(key_prefix, None);
            }
            _ => panic!("expected S3 variant"),
        }
    }

    #[test]
    fn round_trip_s3_with_endpoint() {
        let code = InviteCode {
            library_id: "lib-456".into(),
            library_name: "Shared".into(),
            join_info: CloudHomeJoinInfo::S3 {
                bucket: "bucket".into(),
                region: "eu-west-1".into(),
                endpoint: Some("https://s3.example.com".into()),
                access_key: "ak".into(),
                secret_key: "sk".into(),
                key_prefix: None,
            },
            owner_pubkey: "cafebabe".into(),
            obfuscate_blob_paths: true,
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.library_id, "lib-456");
        match decoded.join_info {
            CloudHomeJoinInfo::S3 { endpoint, .. } => {
                assert_eq!(endpoint, Some("https://s3.example.com".to_string()));
            }
            _ => panic!("expected S3 variant"),
        }
    }

    #[test]
    fn round_trip_google_drive() {
        let code = InviteCode {
            library_id: "lib-789".into(),
            library_name: "Cloud Shared".into(),
            join_info: CloudHomeJoinInfo::GoogleDrive {
                folder_id: "abc123".into(),
            },
            owner_pubkey: "cafebabe".into(),
            obfuscate_blob_paths: true,
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.library_id, "lib-789");
        match decoded.join_info {
            CloudHomeJoinInfo::GoogleDrive { folder_id } => assert_eq!(folder_id, "abc123"),
            _ => panic!("expected GoogleDrive variant"),
        }
    }

    #[test]
    fn decode_invalid_base64() {
        assert!(matches!(
            decode("not-valid!!!"),
            Err(JoinCodeError::InvalidBase64)
        ));
    }

    #[test]
    fn decode_invalid_json() {
        let encoded = URL_SAFE_NO_PAD.encode(b"not json");
        assert!(matches!(
            decode(&encoded),
            Err(JoinCodeError::InvalidJson(_))
        ));
    }

    #[test]
    fn round_trip_cloudkit() {
        let code = InviteCode {
            library_id: "lib-ck".into(),
            library_name: "CloudKit Library".into(),
            join_info: CloudHomeJoinInfo::CloudKit {
                share_url: "https://www.icloud.com/share/abc123".into(),
            },
            owner_pubkey: "aabbccdd".into(),
            obfuscate_blob_paths: true,
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.library_id, "lib-ck");
        match decoded.join_info {
            CloudHomeJoinInfo::CloudKit { share_url } => {
                assert_eq!(share_url, "https://www.icloud.com/share/abc123")
            }
            _ => panic!("expected CloudKit variant"),
        }
    }

    /// An invite for an unobfuscated home carries `obfuscate_blob_paths = false`
    /// so the joiner computes the owner's readable blob keys; a default invite
    /// omits the field and decodes back to `true` (obfuscated).
    #[test]
    fn round_trip_obfuscate_blob_paths() {
        let plain = InviteCode {
            library_id: "lib-plain".into(),
            library_name: "Browsable".into(),
            join_info: CloudHomeJoinInfo::S3 {
                bucket: "b".into(),
                region: "r".into(),
                endpoint: None,
                access_key: "ak".into(),
                secret_key: "sk".into(),
                key_prefix: None,
            },
            owner_pubkey: "ff00".into(),
            obfuscate_blob_paths: false,
        };
        let json = serde_json::to_string(&plain).unwrap();
        assert!(
            json.contains("obfuscate_blob_paths"),
            "an unobfuscated invite must carry the flag: {json}"
        );
        let decoded = decode(&encode(&plain)).unwrap();
        assert!(!decoded.obfuscate_blob_paths, "false round-trips");

        let mut obfuscated = plain;
        obfuscated.obfuscate_blob_paths = true;
        let json = serde_json::to_string(&obfuscated).unwrap();
        assert!(
            !json.contains("obfuscate_blob_paths"),
            "an obfuscated (default) invite omits the flag: {json}"
        );
        let decoded = decode(&encode(&obfuscated)).unwrap();
        assert!(decoded.obfuscate_blob_paths, "absent decodes to true");
    }

    #[test]
    fn decode_trims_whitespace() {
        let code = InviteCode {
            library_id: "lib-ws".into(),
            library_name: "Trimmed".into(),
            join_info: CloudHomeJoinInfo::Dropbox {
                shared_folder_id: "sf1".into(),
            },
            owner_pubkey: "aabb".into(),
            obfuscate_blob_paths: true,
        };
        let encoded = format!("  {} \n", encode(&code));
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.library_id, "lib-ws");
    }

    #[test]
    fn join_request_round_trip_with_email() {
        let code = JoinRequestCode {
            public_key: "abcdef1234567890".into(),
            email: Some("user@example.com".into()),
        };
        let encoded = encode_join_request(&code);
        let decoded = decode_join_request(&encoded).unwrap();
        assert_eq!(decoded.public_key, "abcdef1234567890");
        assert_eq!(decoded.email, Some("user@example.com".to_string()));
    }

    #[test]
    fn join_request_round_trip_without_email() {
        let code = JoinRequestCode {
            public_key: "deadbeef".into(),
            email: None,
        };
        let encoded = encode_join_request(&code);
        let decoded = decode_join_request(&encoded).unwrap();
        assert_eq!(decoded.public_key, "deadbeef");
        assert_eq!(decoded.email, None);
    }

    #[test]
    fn join_request_trims_whitespace() {
        let code = JoinRequestCode {
            public_key: "aabbccdd".into(),
            email: None,
        };
        let encoded = format!("  {} \n", encode_join_request(&code));
        let decoded = decode_join_request(&encoded).unwrap();
        assert_eq!(decoded.public_key, "aabbccdd");
    }
}
