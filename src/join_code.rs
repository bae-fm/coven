use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::storage::cloud::CloudHomeJoinInfo;

/// An invite is always for a private home: sharing wraps and rotates the library
/// key, which a public (plaintext) home has none of, so the joiner always builds
/// an encrypted, obfuscated home. The invite therefore carries no visibility
/// flag.
#[derive(Serialize, Deserialize)]
pub struct InviteCode {
    pub library_id: String,
    pub library_name: String,
    pub join_info: CloudHomeJoinInfo,
    pub owner_pubkey: String,
}

pub fn encode(code: &InviteCode) -> String {
    let json = serde_json::to_vec(code).expect("InviteCode is always serializable");
    URL_SAFE_NO_PAD.encode(&json)
}

pub fn decode(s: &str) -> Result<InviteCode, JoinCodeError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(s.trim())
        .map_err(|_| JoinCodeError::InvalidBase64)?;
    let code: InviteCode =
        serde_json::from_slice(&bytes).map_err(|e| JoinCodeError::InvalidJson(e.to_string()))?;
    // An invite is unsigned, so `library_id` is attacker-controlled. It becomes the
    // name of a directory the joiner creates under `libraries/` and recursively
    // deletes on a bootstrap failure, so a value carrying `..`, a separator, or an
    // absolute path would put that create/delete outside the libraries root. Reject
    // it the moment the code is parsed: a decoded `InviteCode` always carries a
    // `library_id` that is a single safe path component.
    crate::library_dir::validate_path_token(&code.library_id)
        .map_err(JoinCodeError::InvalidLibraryId)?;
    Ok(code)
}

#[derive(Serialize, Deserialize)]
pub struct JoinRequestCode {
    pub public_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// Generate a join request code containing this device's Ed25519 public key,
/// and optionally a contact email the inviter can use to recognize the device.
/// Creates a keypair if one doesn't exist yet.
pub fn generate_join_request(email: Option<String>) -> Result<String, crate::keys::KeyError> {
    let global_ks = crate::keys::KeyService::new("global".to_string());
    let keypair = global_ks.get_or_create_user_keypair()?;

    let code = JoinRequestCode {
        public_key: hex::encode(keypair.public_key),
        email,
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
    /// Whether the joining device must run an OAuth flow before joining, so the
    /// host fetches the token first — mirrors `RestoreCodeInfo::needs_oauth`.
    pub needs_oauth: bool,
}

/// Decode an invite code and return UI-ready info.
pub fn decode_invite_code_info(code: &str) -> Result<InviteCodeInfo, JoinCodeError> {
    let invite = decode(code)?;
    let cloud_provider = invite.join_info.cloud_provider();
    Ok(InviteCodeInfo {
        library_id: invite.library_id,
        library_name: invite.library_name,
        owner_pubkey: invite.owner_pubkey,
        needs_oauth: cloud_provider.needs_oauth(),
        cloud_provider,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum JoinCodeError {
    #[error("invalid base64url encoding")]
    InvalidBase64,
    #[error("invalid invite code payload: {0}")]
    InvalidJson(String),
    /// The invite's `library_id` is not a safe path component, so it cannot name a
    /// library directory under `libraries/`. The invite is unsigned and anyone can
    /// craft one, so the id is refused here at decode rather than reaching a path
    /// operation.
    #[error("invalid library id in invite code: {0}")]
    InvalidLibraryId(crate::library_dir::PathTokenError),
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

    #[test]
    fn decode_trims_whitespace() {
        let code = InviteCode {
            library_id: "lib-ws".into(),
            library_name: "Trimmed".into(),
            join_info: CloudHomeJoinInfo::Dropbox {
                shared_folder_id: "sf1".into(),
            },
            owner_pubkey: "aabb".into(),
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
