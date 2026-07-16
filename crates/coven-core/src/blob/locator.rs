use serde::{Deserialize, Serialize};

use crate::blob::BlobScope;
use crate::encryption::KeyFingerprint;
use crate::store_dir::{validate_cloud_path, validate_path_token, StoreDir};
use crate::sync::circle::{Audience, CircleId};
use crate::sync::store_commit::ObjectHash;

const RESERVED_READABLE_VERSION_SEGMENT: &str = ".coven-versions";

/// A cloud-backed audience. Local rows have no blob locator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteAudience {
    Store,
    Circle(CircleId),
}

impl TryFrom<Audience> for RemoteAudience {
    type Error = BlobLocatorError;

    fn try_from(value: Audience) -> Result<Self, Self::Error> {
        match value {
            Audience::Store => Ok(Self::Store),
            Audience::Circle(circle_id) => Ok(Self::Circle(circle_id)),
            Audience::Local => Err(BlobLocatorError::LocalAudience),
        }
    }
}

/// The exact semantic cloud slot and verification facts committed with one row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "protection", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlobLocator {
    Opaque {
        namespace: String,
        blob_id: String,
        uploader: String,
        audience: RemoteAudience,
        scope: BlobScope,
        key_fingerprint: KeyFingerprint,
        semantic_key: String,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    },
    Browsable {
        namespace: String,
        blob_id: String,
        uploader: String,
        cloud_path: String,
        semantic_key: String,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    },
}

impl BlobLocator {
    #[allow(clippy::too_many_arguments)]
    pub fn opaque(
        namespace: impl Into<String>,
        blob_id: impl Into<String>,
        uploader: impl Into<String>,
        audience: RemoteAudience,
        scope: BlobScope,
        key_fingerprint: KeyFingerprint,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    ) -> Result<Self, BlobLocatorError> {
        let namespace = namespace.into();
        let blob_id = blob_id.into();
        let uploader = uploader.into();
        let semantic_key = opaque_semantic_key(&namespace, &uploader, key_fingerprint, &blob_id)?;
        Ok(Self::Opaque {
            namespace,
            blob_id,
            uploader,
            audience,
            scope,
            key_fingerprint,
            semantic_key,
            plaintext_size,
            plaintext_hash,
        })
    }

    pub fn browsable(
        namespace: impl Into<String>,
        blob_id: impl Into<String>,
        uploader: impl Into<String>,
        cloud_path: impl Into<String>,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    ) -> Result<Self, BlobLocatorError> {
        let namespace = namespace.into();
        let blob_id = blob_id.into();
        let uploader = uploader.into();
        let cloud_path = cloud_path.into();
        let semantic_key =
            browsable_semantic_key(&namespace, &cloud_path, &uploader, &blob_id, plaintext_hash)?;
        Ok(Self::Browsable {
            namespace,
            blob_id,
            uploader,
            cloud_path,
            semantic_key,
            plaintext_size,
            plaintext_hash,
        })
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, BlobLocatorError> {
        let locator: Self = serde_json::from_slice(bytes)
            .map_err(|error| BlobLocatorError::Malformed(error.to_string()))?;
        locator.validate()?;
        if locator.to_bytes() != bytes {
            return Err(BlobLocatorError::NonCanonicalEncoding);
        }
        Ok(locator)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("BlobLocator serialization cannot fail")
    }

    pub fn locator_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.to_bytes())
    }

    pub fn semantic_key(&self) -> &str {
        match self {
            Self::Opaque { semantic_key, .. } | Self::Browsable { semantic_key, .. } => {
                semantic_key
            }
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            Self::Opaque { namespace, .. } | Self::Browsable { namespace, .. } => namespace,
        }
    }

    pub fn blob_id(&self) -> &str {
        match self {
            Self::Opaque { blob_id, .. } | Self::Browsable { blob_id, .. } => blob_id,
        }
    }

    pub fn uploader(&self) -> &str {
        match self {
            Self::Opaque { uploader, .. } | Self::Browsable { uploader, .. } => uploader,
        }
    }

    pub fn audience(&self) -> RemoteAudience {
        match self {
            Self::Opaque { audience, .. } => audience.clone(),
            Self::Browsable { .. } => RemoteAudience::Store,
        }
    }

    pub fn plaintext_size(&self) -> u64 {
        match self {
            Self::Opaque { plaintext_size, .. } | Self::Browsable { plaintext_size, .. } => {
                *plaintext_size
            }
        }
    }

    pub fn plaintext_hash(&self) -> ObjectHash {
        match self {
            Self::Opaque { plaintext_hash, .. } | Self::Browsable { plaintext_hash, .. } => {
                *plaintext_hash
            }
        }
    }

    pub fn scope(&self) -> Option<&BlobScope> {
        match self {
            Self::Opaque { scope, .. } => Some(scope),
            Self::Browsable { .. } => None,
        }
    }

    pub fn key_fingerprint(&self) -> Option<KeyFingerprint> {
        match self {
            Self::Opaque {
                key_fingerprint, ..
            } => Some(*key_fingerprint),
            Self::Browsable { .. } => None,
        }
    }

    pub fn cloud_path(&self) -> Option<&str> {
        match self {
            Self::Opaque { .. } => None,
            Self::Browsable { cloud_path, .. } => Some(cloud_path),
        }
    }

    pub(crate) fn storage_suffix(&self) -> &'static str {
        match self {
            Self::Opaque { .. } => ".enc",
            Self::Browsable { .. } => "",
        }
    }

    pub(crate) fn validate(&self) -> Result<(), BlobLocatorError> {
        let expected = match self {
            Self::Opaque {
                namespace,
                blob_id,
                uploader,
                key_fingerprint,
                ..
            } => opaque_semantic_key(namespace, uploader, *key_fingerprint, blob_id)?,
            Self::Browsable {
                namespace,
                blob_id,
                uploader,
                cloud_path,
                plaintext_hash,
                ..
            } => browsable_semantic_key(namespace, cloud_path, uploader, blob_id, *plaintext_hash)?,
        };
        if self.semantic_key() != expected {
            return Err(BlobLocatorError::SemanticKeyMismatch {
                expected,
                actual: self.semantic_key().to_string(),
            });
        }
        Ok(())
    }
}

fn opaque_semantic_key(
    namespace: &str,
    uploader: &str,
    key_fingerprint: KeyFingerprint,
    blob_id: &str,
) -> Result<String, BlobLocatorError> {
    validate_namespace(namespace)?;
    validate_uploader(uploader)?;
    let shard = StoreDir::id_shard(blob_id).map_err(|error| BlobLocatorError::UnsafeBlobId {
        value: blob_id.to_string(),
        reason: error.to_string(),
    })?;
    Ok(format!(
        "{namespace}/opaque/{uploader}/{key_fingerprint}/{shard}"
    ))
}

fn browsable_semantic_key(
    namespace: &str,
    cloud_path: &str,
    uploader: &str,
    blob_id: &str,
    plaintext_hash: ObjectHash,
) -> Result<String, BlobLocatorError> {
    validate_namespace(namespace)?;
    validate_uploader(uploader)?;
    validate_path_token(blob_id).map_err(|error| BlobLocatorError::UnsafeBlobId {
        value: blob_id.to_string(),
        reason: error.to_string(),
    })?;
    validate_cloud_path(cloud_path).map_err(|error| BlobLocatorError::UnsafeCloudPath {
        value: cloud_path.to_string(),
        reason: error.to_string(),
    })?;
    if cloud_path
        .split('/')
        .any(|segment| segment == RESERVED_READABLE_VERSION_SEGMENT)
    {
        return Err(BlobLocatorError::ReservedCloudPath {
            value: cloud_path.to_string(),
        });
    }
    Ok(format!(
        "{namespace}/readable/{cloud_path}/{RESERVED_READABLE_VERSION_SEGMENT}/{uploader}/{blob_id}/{plaintext_hash}"
    ))
}

fn validate_namespace(namespace: &str) -> Result<(), BlobLocatorError> {
    validate_path_token(namespace).map_err(|error| BlobLocatorError::UnsafeNamespace {
        value: namespace.to_string(),
        reason: error.to_string(),
    })
}

fn validate_uploader(uploader: &str) -> Result<(), BlobLocatorError> {
    validate_path_token(uploader).map_err(|error| BlobLocatorError::UnsafeUploader {
        value: uploader.to_string(),
        reason: error.to_string(),
    })?;
    if uploader.len() != 64
        || uploader
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(BlobLocatorError::UnsafeUploader {
            value: uploader.to_string(),
            reason: "uploader must be a 32-byte lowercase hexadecimal public key".to_string(),
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobLocatorError {
    #[error("Local audience has no blob locator")]
    LocalAudience,
    #[error("unsafe blob namespace {value:?}: {reason}")]
    UnsafeNamespace { value: String, reason: String },
    #[error("unsafe blob id {value:?}: {reason}")]
    UnsafeBlobId { value: String, reason: String },
    #[error("unsafe blob uploader {value:?}: {reason}")]
    UnsafeUploader { value: String, reason: String },
    #[error("unsafe readable blob path {value:?}: {reason}")]
    UnsafeCloudPath { value: String, reason: String },
    #[error("readable blob path uses reserved segment: {value:?}")]
    ReservedCloudPath { value: String },
    #[error("blob semantic key mismatch: expected {expected:?}, found {actual:?}")]
    SemanticKeyMismatch { expected: String, actual: String },
    #[error("malformed blob locator: {0}")]
    Malformed(String),
    #[error("blob locator bytes are not canonical")]
    NonCanonicalEncoding,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::circle::{Audience, CircleId};
    use crate::sync::store_commit::ObjectHash;
    use crate::{BlobScope, KeyFingerprint};

    fn hash(bytes: &[u8]) -> ObjectHash {
        ObjectHash::digest(bytes)
    }

    #[test]
    fn opaque_locator_round_trips_canonical_bytes_and_path() {
        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            "11".repeat(32),
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            BlobScope::Derived("release-a".to_string()),
            KeyFingerprint::from_bytes([4; 8]),
            27,
            hash(b"cover"),
        )
        .expect("build locator");

        assert_eq!(
            locator.semantic_key(),
            format!(
                "covers/opaque/{}/0404040404040404/a1/b2/a1b2-blob",
                "11".repeat(32)
            )
        );
        assert_eq!(BlobLocator::parse(&locator.to_bytes()).unwrap(), locator);
        assert_eq!(
            String::from_utf8(locator.to_bytes()).unwrap(),
            format!(
                "{{\"protection\":\"opaque\",\"namespace\":\"covers\",\"blob_id\":\"a1b2-blob\",\"uploader\":\"{}\",\"audience\":{{\"circle\":\"{}\"}},\"scope\":{{\"Derived\":\"release-a\"}},\"key_fingerprint\":\"0404040404040404\",\"semantic_key\":\"covers/opaque/{}/0404040404040404/a1/b2/a1b2-blob\",\"plaintext_size\":27,\"plaintext_hash\":\"{}\"}}",
                "11".repeat(32),
                CircleId::from_bytes([3; 16]),
                "11".repeat(32),
                hash(b"cover"),
            )
        );
    }

    #[test]
    fn browsable_locator_round_trips_canonical_bytes_and_version_path() {
        let locator = BlobLocator::browsable(
            "audio",
            "abcd-track",
            "22".repeat(32),
            "Artist/Album/01 Track.flac",
            91,
            hash(b"track"),
        )
        .expect("build locator");

        assert_eq!(
            locator.semantic_key(),
            format!(
                "audio/readable/Artist/Album/01 Track.flac/.coven-versions/{}/abcd-track/{}",
                "22".repeat(32),
                hash(b"track")
            )
        );
        assert_eq!(BlobLocator::parse(&locator.to_bytes()).unwrap(), locator);
    }

    #[test]
    fn locator_rejects_unsafe_reserved_and_noncanonical_paths() {
        assert!(matches!(
            BlobLocator::opaque(
                "../covers",
                "a1b2-blob",
                "11".repeat(32),
                RemoteAudience::Store,
                BlobScope::Master,
                KeyFingerprint::from_bytes([4; 8]),
                1,
                hash(b"x"),
            ),
            Err(BlobLocatorError::UnsafeNamespace { .. })
        ));
        assert!(matches!(
            BlobLocator::browsable(
                "audio",
                "abcd-track",
                "22".repeat(32),
                "Artist/.coven-versions/track.flac",
                1,
                hash(b"x"),
            ),
            Err(BlobLocatorError::ReservedCloudPath { .. })
        ));

        for cloud_path in [
            "C:/Music/track.flac",
            "Artist/Album/C:track.flac",
            "Artist/../track.flac",
            "Artist/./track.flac",
            "Artist//track.flac",
            "Artist/track.flac/",
            "Artist\\track.flac",
            "Artist/track\0.flac",
        ] {
            assert!(matches!(
                BlobLocator::browsable(
                    "audio",
                    "abcd-track",
                    "22".repeat(32),
                    cloud_path,
                    1,
                    hash(b"x"),
                ),
                Err(BlobLocatorError::UnsafeCloudPath { .. })
            ));
        }

        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            "11".repeat(32),
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 8]),
            1,
            hash(b"x"),
        )
        .unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&locator.to_bytes()).unwrap();
        value["semantic_key"] = serde_json::json!("covers/opaque/relocated");
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&value).unwrap()),
            Err(BlobLocatorError::SemanticKeyMismatch { .. })
                | Err(BlobLocatorError::NonCanonicalEncoding)
        ));
    }

    #[test]
    fn locator_rejects_unknown_shape_and_noncanonical_bytes() {
        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            "11".repeat(32),
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 8]),
            1,
            hash(b"x"),
        )
        .unwrap();
        let bytes = locator.to_bytes();

        let mut unknown_field: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown_field["unknown"] = serde_json::json!(true);
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&unknown_field).unwrap()),
            Err(BlobLocatorError::Malformed(_))
        ));

        let mut unknown_variant: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown_variant["protection"] = serde_json::json!("unknown");
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&unknown_variant).unwrap()),
            Err(BlobLocatorError::Malformed(_))
        ));

        let mut noncanonical = bytes.clone();
        noncanonical.push(b'\n');
        assert_eq!(
            BlobLocator::parse(&noncanonical),
            Err(BlobLocatorError::NonCanonicalEncoding)
        );
    }

    #[test]
    fn locator_hash_is_the_digest_of_canonical_bytes() {
        let locator = BlobLocator::browsable(
            "audio",
            "abcd-track",
            "22".repeat(32),
            "Artist/Album/track.flac",
            91,
            hash(b"track"),
        )
        .unwrap();

        assert_eq!(locator.locator_hash(), hash(&locator.to_bytes()));
    }

    #[test]
    fn locator_rejects_uppercase_uploader_keys() {
        assert!(matches!(
            BlobLocator::opaque(
                "covers",
                "a1b2-blob",
                "AA".repeat(32),
                RemoteAudience::Store,
                BlobScope::Master,
                KeyFingerprint::from_bytes([4; 8]),
                1,
                hash(b"x"),
            ),
            Err(BlobLocatorError::UnsafeUploader { .. })
        ));

        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            "11".repeat(32),
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 8]),
            1,
            hash(b"x"),
        )
        .unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&locator.to_bytes()).unwrap();
        value["uploader"] = serde_json::json!("AA".repeat(32));
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&value).unwrap()),
            Err(BlobLocatorError::UnsafeUploader { .. })
        ));
    }

    #[test]
    fn local_audience_has_no_locator_variant() {
        assert!(RemoteAudience::try_from(Audience::Local).is_err());
        assert_eq!(
            RemoteAudience::try_from(Audience::Store).unwrap(),
            RemoteAudience::Store
        );
    }
}
