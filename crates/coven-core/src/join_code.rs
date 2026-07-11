use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::storage::cloud::CloudHomeJoinInfo;

/// An invite is always for a private home: sharing wraps and rotates the store
/// key, which a public (plaintext) home has none of, so the joiner always builds
/// an encrypted, obfuscated home. The invite therefore carries no visibility
/// flag.
#[derive(Serialize, Deserialize)]
pub struct InviteCode {
    pub store_id: String,
    pub store_name: String,
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
    // An invite is unsigned, so `store_id` is attacker-controlled. It becomes the
    // name of a directory the joiner creates under `stores/` and recursively
    // deletes on a bootstrap failure, so a value carrying `..`, a separator, or an
    // absolute path would put that create/delete outside the stores root. Reject
    // it the moment the code is parsed: a decoded `InviteCode` always carries a
    // `store_id` that is a single safe path component.
    crate::store_dir::validate_path_token(&code.store_id).map_err(JoinCodeError::InvalidStoreId)?;
    Ok(code)
}

#[derive(Serialize, Deserialize)]
pub struct JoinRequestCode {
    pub public_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// Generate a join request code containing this device's Ed25519 public key and
/// optionally a contact email the inviter can use to recognize the device.
pub fn generate_join_request_for_keypair(
    keypair: &crate::keys::UserKeypair,
    email: Option<String>,
) -> String {
    let code = JoinRequestCode {
        public_key: hex::encode(keypair.public_key()),
        email,
    };

    encode_join_request(&code)
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
    pub store_id: String,
    pub store_name: String,
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
        store_id: invite.store_id,
        store_name: invite.store_name,
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
    /// The invite's `store_id` is not a safe path component, so it cannot name a
    /// store directory under `stores/`. The invite is unsigned and anyone can
    /// craft one, so the id is refused here at decode rather than reaching a path
    /// operation.
    #[error("invalid store id in invite code: {0}")]
    InvalidStoreId(crate::store_dir::PathTokenError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_s3() {
        let code = InviteCode {
            store_id: "lib-123".into(),
            store_name: "My Store".into(),
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
        assert_eq!(decoded.store_id, "lib-123");
        assert_eq!(decoded.store_name, "My Store");
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
            store_id: "lib-456".into(),
            store_name: "Shared".into(),
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
        assert_eq!(decoded.store_id, "lib-456");
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
            store_id: "lib-789".into(),
            store_name: "Cloud Shared".into(),
            join_info: CloudHomeJoinInfo::GoogleDrive {
                folder_id: "abc123".into(),
            },
            owner_pubkey: "cafebabe".into(),
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.store_id, "lib-789");
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
            store_id: "lib-ck".into(),
            store_name: "CloudKit Store".into(),
            join_info: CloudHomeJoinInfo::CloudKit,
            owner_pubkey: "aabbccdd".into(),
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.store_id, "lib-ck");
        assert!(matches!(decoded.join_info, CloudHomeJoinInfo::CloudKit));
    }

    #[test]
    fn round_trip_cloudkit_share() {
        let code = InviteCode {
            store_id: "lib-ck-share".into(),
            store_name: "CloudKit Store".into(),
            join_info: CloudHomeJoinInfo::CloudKitShare {
                share_url: "https://www.icloud.com/share/example".into(),
                owner_name: "_owner".into(),
                zone_name: "bae-store".into(),
            },
            owner_pubkey: "aabbccdd".into(),
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.store_id, "lib-ck-share");
        assert!(matches!(
            decoded.join_info,
            CloudHomeJoinInfo::CloudKitShare {
                share_url,
                owner_name,
                zone_name
            } if share_url == "https://www.icloud.com/share/example"
                && owner_name == "_owner"
                && zone_name == "bae-store"
        ));
    }

    #[test]
    fn decode_trims_whitespace() {
        let code = InviteCode {
            store_id: "lib-ws".into(),
            store_name: "Trimmed".into(),
            join_info: CloudHomeJoinInfo::Dropbox {
                shared_folder_id: "sf1".into(),
            },
            owner_pubkey: "aabb".into(),
        };
        let encoded = format!("  {} \n", encode(&code));
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.store_id, "lib-ws");
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
