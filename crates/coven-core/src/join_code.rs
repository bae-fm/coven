use serde::{Deserialize, Serialize};

use crate::code_envelope::{self, EnvelopeError};
use crate::storage::cloud::CloudHomeJoinInfo;
use crate::sync::membership::MembershipHeadRef;
#[cfg(test)]
use crate::sync::membership::{MembershipCoord, MembershipGrantId};
#[cfg(test)]
use crate::sync::store_commit::ObjectHash;

pub const INVITE_CODE_VERSION: u8 = 4;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct MembershipFloor(pub Vec<MembershipHeadRef>);

/// An invite is always for a private home: sharing wraps and rotates the store
/// key, which a public (plaintext) home has none of, so the joiner always builds
/// an encrypted, obfuscated home. The invite therefore carries no visibility
/// flag.
///
/// `Debug` is derived: `join_info` (`CloudHomeJoinInfo`) already hand-writes
/// its own redacting `Debug`, and no other field here carries a secret.
#[derive(Serialize, Deserialize, Debug)]
pub struct InviteCode {
    /// Wire-format version.
    pub v: u8,
    pub store_id: String,
    pub store_name: String,
    pub join_info: CloudHomeJoinInfo,
    pub owner_pubkey: String,
    pub wrapped_key: crate::sync::wrapped_store_key::WrappedStoreKeyRef,
    pub store_root: crate::sync::store_commit::StoreRootRef,
    /// The exact causal membership heads the joiner must observe.
    pub membership_floor: MembershipFloor,
}

/// Encode an `InviteCode` into a prefixed base64url string.
pub fn encode(code: &InviteCode) -> String {
    code_envelope::encode_code(code_envelope::PREFIX, code)
}

/// Decode an invite code string back into an `InviteCode`.
pub fn decode(s: &str) -> Result<InviteCode, JoinCodeError> {
    let code: InviteCode = code_envelope::decode_code(code_envelope::PREFIX, s)?;
    if code.v != INVITE_CODE_VERSION {
        return Err(JoinCodeError::UnsupportedVersion(code.v));
    }
    // An invite is unsigned, so `store_id` is attacker-controlled. It becomes the
    // name of a directory the joiner creates under `stores/` and recursively
    // deletes on a bootstrap failure, so a value carrying `..`, a separator, or an
    // absolute path would put that create/delete outside the stores root. Reject
    // it the moment the code is parsed: a decoded `InviteCode` always carries a
    // `store_id` that is a single safe path component.
    crate::store_dir::validate_path_token(&code.store_id).map_err(JoinCodeError::InvalidStoreId)?;
    // `owner_pubkey` pins the membership-chain founder the joiner authenticates
    // the invite against (`join.rs`): a malformed value can't name a real chain,
    // so it is refused here rather than surfacing as an opaque chain-lookup
    // failure deep in the join.
    crate::sync::restore_code::decode_hex_bytes("owner public key", &code.owner_pubkey, 32)
        .map_err(JoinCodeError::InvalidOwnerPubkey)?;
    crate::sync::restore_code::decode_hex_bytes(
        "wrapped-key author public key",
        &code.wrapped_key.owner_pubkey,
        32,
    )
    .map_err(|error| JoinCodeError::InvalidWrappedKey(error.to_string()))?;
    crate::sync::restore_code::decode_hex_bytes(
        "wrapped-key recipient public key",
        &code.wrapped_key.recipient_pubkey,
        32,
    )
    .map_err(|error| JoinCodeError::InvalidWrappedKey(error.to_string()))?;
    code.wrapped_key
        .validate_identity()
        .map_err(|error| JoinCodeError::InvalidWrappedKey(error.to_string()))?;
    if code.membership_floor.0.is_empty() {
        return Err(JoinCodeError::EmptyMembershipFloor);
    }
    crate::sync::store::membership::validate_membership_floor(&code.membership_floor.0)
        .map_err(JoinCodeError::InvalidMembershipFloor)?;
    Ok(code)
}

/// `Debug` is derived: neither field carries a secret.
#[derive(Serialize, Deserialize, Debug)]
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

/// A join request carries no prefix or version: it is a short-lived exchange
/// between the joiner and inviter, not a durable code a user stores and pastes
/// back in later, so it reuses the envelope with an empty prefix.
pub fn encode_join_request(code: &JoinRequestCode) -> String {
    code_envelope::encode_code("", code)
}

pub fn decode_join_request(s: &str) -> Result<JoinRequestCode, JoinCodeError> {
    Ok(code_envelope::decode_code("", s)?)
}

/// UI-ready info from a decoded invite code.
pub struct InviteCodeInfo {
    pub store_id: String,
    pub store_name: String,
    pub owner_pubkey: String,
    pub store_root_hash: crate::sync::store_commit::ObjectHash,
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
        store_root_hash: invite.store_root.store_root_hash,
        needs_oauth: cloud_provider.needs_oauth(),
        cloud_provider,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum JoinCodeError {
    #[error("That doesn't look like a coven invite code — it should start with \"coven:\".")]
    MissingPrefix,
    #[error("The invite code is incomplete or has a typo. Check that you copied the entire code.")]
    InvalidBase64,
    #[error("The invite code is corrupted. Ask the inviter to generate a new one. ({0})")]
    InvalidJson(String),
    #[error("This invite code uses unsupported format version v{0}. Ask the inviter to generate a new one.")]
    UnsupportedVersion(u8),
    /// The invite's `store_id` is not a safe path component, so it cannot name a
    /// store directory under `stores/`. The invite is unsigned and anyone can
    /// craft one, so the id is refused here at decode rather than reaching a path
    /// operation.
    #[error(
        "The store id in this invite code is invalid. Ask the inviter to generate a new one. ({0})"
    )]
    InvalidStoreId(crate::store_dir::PathTokenError),
    /// `owner_pubkey` pins the membership-chain founder (`join.rs`); a value
    /// that isn't 32 bytes of hex can't name one, so it is refused at decode.
    #[error(
        "The owner key in this invite code is invalid. Ask the inviter to generate a new one. ({0})"
    )]
    InvalidOwnerPubkey(String),
    #[error("The wrapped key in this invite code is invalid. Ask the inviter to generate a new one. ({0})")]
    InvalidWrappedKey(String),
    #[error("The invite code has no membership floor. Ask the inviter to generate a new one.")]
    EmptyMembershipFloor,
    #[error("The membership floor in this invite code is invalid. Ask the inviter to generate a new one. ({0})")]
    InvalidMembershipFloor(String),
}

impl From<EnvelopeError> for JoinCodeError {
    fn from(e: EnvelopeError) -> Self {
        match e {
            EnvelopeError::MissingPrefix => JoinCodeError::MissingPrefix,
            EnvelopeError::InvalidBase64 => JoinCodeError::InvalidBase64,
            EnvelopeError::InvalidJson(s) => JoinCodeError::InvalidJson(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    fn test_owner_pubkey() -> String {
        hex::encode([0xAB_u8; 32])
    }

    fn test_membership_floor() -> Vec<MembershipHeadRef> {
        let coord = MembershipCoord {
            author_pubkey: test_owner_pubkey(),
            author_owner_grant: MembershipGrantId(ObjectHash::digest(b"test owner grant")),
            stream_id: crate::sync::membership::AuthorStreamId::from_bytes([1; 32]),
            seq: 1,
            entry_hash: ObjectHash::digest(b"test membership entry"),
        };
        let stored = b"test membership head";
        vec![MembershipHeadRef {
            coord,
            head_hash: ObjectHash::digest(b"test membership head semantic bytes"),
            object: crate::sync::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(
                    "store-v1/membership/heads/test-owner/1.json".to_string(),
                )
                .expect("valid test membership-head slot"),
                stored.len() as u64,
                ObjectHash::digest(stored),
            ),
        }]
    }

    fn test_wrapped_key() -> crate::sync::wrapped_store_key::WrappedStoreKeyRef {
        let owner = test_owner_pubkey();
        let recipient = hex::encode([0xCD_u8; 32]);
        let wrap_hash = ObjectHash::digest(b"invite wrapped key");
        crate::sync::wrapped_store_key::WrappedStoreKeyRef {
            owner_pubkey: owner.clone(),
            recipient_pubkey: recipient.clone(),
            generation: 1,
            wrap_hash,
            object: crate::sync::storage::ExactObjectRef::new(
                crate::storage::cloud::ObjectSlot::logical(format!(
                    "keys/{owner}/{recipient}/1/{wrap_hash}.json"
                ))
                .expect("valid test wrapped-key slot"),
                4,
                ObjectHash::digest(b"wrap"),
            ),
        }
    }

    fn sample_s3_code(store_id: &str) -> InviteCode {
        InviteCode {
            v: INVITE_CODE_VERSION,
            store_id: store_id.to_string(),
            store_name: "My Store".into(),
            join_info: CloudHomeJoinInfo::S3 {
                bucket: "my-bucket".into(),
                region: "us-east-1".into(),
                endpoint: None,
                access_key: "AKIAEXAMPLE".into(),
                secret_key: "secret123".into(),
                key_prefix: None,
            },
            owner_pubkey: test_owner_pubkey(),
            wrapped_key: test_wrapped_key(),
            store_root: crate::sync::store_commit::StoreRootRef {
                store_root_id: crate::sync::store_commit::ObjectHash::digest(
                    b"invite store protocol root",
                ),
                store_root_hash: ObjectHash::digest(b"root"),
                object: crate::sync::storage::ExactObjectRef::new(
                    crate::storage::cloud::ObjectSlot::logical(
                        "store-v1/protocol/root/test.json".to_string(),
                    )
                    .expect("valid test Store-root slot"),
                    4,
                    ObjectHash::digest(b"root"),
                ),
            },
            membership_floor: MembershipFloor(test_membership_floor()),
        }
    }

    #[test]
    fn round_trip_s3() {
        let code = sample_s3_code("lib-123");
        let encoded = encode(&code);
        assert!(encoded.starts_with(code_envelope::PREFIX));
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.v, INVITE_CODE_VERSION);
        assert_eq!(decoded.store_id, "lib-123");
        assert_eq!(decoded.store_name, "My Store");
        assert_eq!(decoded.owner_pubkey, test_owner_pubkey());
        assert_eq!(decoded.store_root, code.store_root);
        assert_eq!(
            decoded.membership_floor,
            MembershipFloor(test_membership_floor())
        );
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
    fn round_trip_s3_with_endpoint_and_key_prefix() {
        let mut code = sample_s3_code("lib-456");
        code.join_info = CloudHomeJoinInfo::S3 {
            bucket: "bucket".into(),
            region: "eu-west-1".into(),
            endpoint: Some("https://s3.example.com".into()),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            key_prefix: Some("prefix/".into()),
        };
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.store_id, "lib-456");
        match decoded.join_info {
            CloudHomeJoinInfo::S3 {
                endpoint,
                key_prefix,
                ..
            } => {
                assert_eq!(endpoint, Some("https://s3.example.com".to_string()));
                assert_eq!(key_prefix, Some("prefix/".to_string()));
            }
            _ => panic!("expected S3 variant"),
        }
    }

    /// Absent optional S3 fields (`endpoint`, `key_prefix`) must not appear in
    /// the encoded JSON at all — a smaller invite code, and a `None` that isn't
    /// confusable with an explicit `null`.
    #[test]
    fn absent_s3_optionals_omitted_from_json() {
        let code = sample_s3_code("lib-omit");
        let encoded = encode(&code);
        let payload = encoded.strip_prefix(code_envelope::PREFIX).unwrap();
        let bytes = URL_SAFE_NO_PAD.decode(payload).unwrap();
        let json = String::from_utf8(bytes).unwrap();
        assert!(!json.contains("endpoint"), "{json}");
        assert!(!json.contains("key_prefix"), "{json}");
    }

    #[test]
    fn round_trip_google_drive() {
        let mut code = sample_s3_code("lib-789");
        code.store_name = "Cloud Shared".into();
        code.join_info = CloudHomeJoinInfo::GoogleDrive {
            folder_id: "abc123".into(),
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
    fn decode_missing_prefix() {
        let code = sample_s3_code("lib-noprefix");
        let encoded = encode(&code);
        let without_prefix = &encoded[code_envelope::PREFIX.len()..];
        assert!(matches!(
            decode(without_prefix),
            Err(JoinCodeError::MissingPrefix)
        ));
    }

    #[test]
    fn decode_invalid_base64() {
        assert!(matches!(
            decode("coven:not-valid!!!"),
            Err(JoinCodeError::InvalidBase64)
        ));
    }

    #[test]
    fn decode_invalid_json() {
        let b64 = URL_SAFE_NO_PAD.encode(b"not json");
        let encoded = format!("coven:{b64}");
        assert!(matches!(
            decode(&encoded),
            Err(JoinCodeError::InvalidJson(_))
        ));
    }

    #[test]
    fn decode_obsolete_version() {
        let mut code = sample_s3_code("lib-old");
        code.v = 0;
        let encoded = encode(&code);
        assert!(matches!(
            decode(&encoded),
            Err(JoinCodeError::UnsupportedVersion(0))
        ));
    }

    #[test]
    fn decode_newer_version() {
        let mut code = sample_s3_code("lib-new");
        code.v = 99;
        let encoded = encode(&code);
        assert!(matches!(
            decode(&encoded),
            Err(JoinCodeError::UnsupportedVersion(99))
        ));
    }

    /// `membership_floor` is required, not merely present-when-known: a code
    /// serialized without it by a hand-crafted attack code
    /// must be refused at decode rather than silently read as "no floor" — the
    /// exact masking this field exists to remove.
    #[test]
    fn decode_missing_membership_floor_is_refused() {
        let mut json = serde_json::to_value(sample_s3_code("lib-no-floor")).unwrap();
        json.as_object_mut().unwrap().remove("membership_floor");
        let bytes = serde_json::to_vec(&json).unwrap();
        let encoded = format!("{}{}", code_envelope::PREFIX, URL_SAFE_NO_PAD.encode(bytes));
        assert!(matches!(
            decode(&encoded),
            Err(JoinCodeError::InvalidJson(_))
        ));
    }

    #[test]
    fn decode_empty_membership_floor_is_refused() {
        let mut code = sample_s3_code("lib-empty-floor");
        code.membership_floor = MembershipFloor(Vec::new());
        assert!(matches!(
            decode(&encode(&code)),
            Err(JoinCodeError::EmptyMembershipFloor)
        ));
    }

    #[test]
    fn decode_invalid_owner_pubkey_wrong_length() {
        let mut code = sample_s3_code("lib-short-key");
        code.owner_pubkey = hex::encode([0xABu8; 16]);
        let encoded = encode(&code);
        assert!(matches!(
            decode(&encoded),
            Err(JoinCodeError::InvalidOwnerPubkey(_))
        ));
    }

    #[test]
    fn decode_invalid_owner_pubkey_non_hex() {
        let mut code = sample_s3_code("lib-bad-hex-key");
        code.owner_pubkey = "not hex".to_string();
        let encoded = encode(&code);
        assert!(matches!(
            decode(&encoded),
            Err(JoinCodeError::InvalidOwnerPubkey(_))
        ));
    }

    #[test]
    fn decode_rejects_a_relocated_wrapped_key_reference() {
        let mut code = sample_s3_code("lib-relocated-wrap");
        code.wrapped_key.object = crate::sync::storage::ExactObjectRef::new(
            crate::storage::cloud::ObjectSlot::logical(
                "keys/attacker/recipient/1/different.json".to_string(),
            )
            .expect("syntactically valid but semantically relocated slot"),
            4,
            ObjectHash::digest(b"wrap"),
        );
        assert!(matches!(
            decode(&encode(&code)),
            Err(JoinCodeError::InvalidWrappedKey(_))
        ));
    }

    #[test]
    fn decode_reports_an_invalid_wrapped_key_recipient_as_a_wrapped_key_error() {
        let mut code = sample_s3_code("lib-invalid-wrap-recipient");
        code.wrapped_key.recipient_pubkey = "not-a-public-key".to_string();

        assert!(matches!(
            decode(&encode(&code)),
            Err(JoinCodeError::InvalidWrappedKey(_))
        ));
    }

    #[test]
    fn round_trip_cloudkit() {
        let mut code = sample_s3_code("lib-ck");
        code.store_name = "CloudKit Store".into();
        code.join_info = CloudHomeJoinInfo::CloudKit;
        let encoded = encode(&code);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.store_id, "lib-ck");
        assert!(matches!(decoded.join_info, CloudHomeJoinInfo::CloudKit));
    }

    #[test]
    fn round_trip_cloudkit_share() {
        let mut code = sample_s3_code("lib-ck-share");
        code.store_name = "CloudKit Store".into();
        code.join_info = CloudHomeJoinInfo::CloudKitShare {
            share_url: "https://www.icloud.com/share/example".into(),
            owner_name: "_owner".into(),
            zone_name: "bae-store".into(),
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
        let mut code = sample_s3_code("lib-ws");
        code.store_name = "Trimmed".into();
        code.join_info = CloudHomeJoinInfo::Dropbox {
            folder_path: "/Apps/your-app/sf1".into(),
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
