use serde::{Deserialize, Serialize};

use super::BlobScope;
use crate::circle::{Audience, CircleId};
use crate::objects::ExactObjectRef;
use crate::store_commit::{ObjectHash, StoreDeviceRegistrationRef};
use coven_foundation::store_dir::{validate_cloud_path, validate_path_token};
use coven_keys::encryption::KeyFingerprint;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "protection", rename_all = "snake_case", deny_unknown_fields)]
pub enum BlobLocator {
    Opaque {
        namespace: String,
        blob_id: String,
        uploader: StoreDeviceRegistrationRef,
        audience: RemoteAudience,
        scope: BlobScope,
        key_fingerprint: KeyFingerprint,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    },
    Browsable {
        namespace: String,
        blob_id: String,
        uploader: StoreDeviceRegistrationRef,
        cloud_path: String,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoredBlobRef {
    locator: BlobLocator,
    object: ExactObjectRef,
}

impl StoredBlobRef {
    pub fn new(locator: BlobLocator, object: ExactObjectRef) -> Result<Self, BlobLocatorError> {
        let stored = Self { locator, object };
        stored.validate()?;
        Ok(stored)
    }

    pub fn locator(&self) -> &BlobLocator {
        &self.locator
    }

    pub fn object(&self) -> &ExactObjectRef {
        &self.object
    }

    fn validate(&self) -> Result<(), BlobLocatorError> {
        self.locator.validate()?;
        let expected = self.locator.semantic_key();
        if self.object.slot().logical_key() != expected {
            return Err(BlobLocatorError::ObjectKeyMismatch {
                expected,
                actual: self.object.slot().logical_key().to_string(),
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for StoredBlobRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            locator: BlobLocator,
            object: ExactObjectRef,
        }

        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.locator, fields.object).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for BlobLocator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "protection", rename_all = "snake_case", deny_unknown_fields)]
        enum Fields {
            Opaque {
                namespace: String,
                blob_id: String,
                uploader: StoreDeviceRegistrationRef,
                audience: RemoteAudience,
                scope: BlobScope,
                key_fingerprint: KeyFingerprint,
                plaintext_size: u64,
                plaintext_hash: ObjectHash,
            },
            Browsable {
                namespace: String,
                blob_id: String,
                uploader: StoreDeviceRegistrationRef,
                cloud_path: String,
                plaintext_size: u64,
                plaintext_hash: ObjectHash,
            },
        }

        let locator = match Fields::deserialize(deserializer)? {
            Fields::Opaque {
                namespace,
                blob_id,
                uploader,
                audience,
                scope,
                key_fingerprint,
                plaintext_size,
                plaintext_hash,
            } => Self::Opaque {
                namespace,
                blob_id,
                uploader,
                audience,
                scope,
                key_fingerprint,
                plaintext_size,
                plaintext_hash,
            },
            Fields::Browsable {
                namespace,
                blob_id,
                uploader,
                cloud_path,
                plaintext_size,
                plaintext_hash,
            } => Self::Browsable {
                namespace,
                blob_id,
                uploader,
                cloud_path,
                plaintext_size,
                plaintext_hash,
            },
        };
        locator.validate().map_err(serde::de::Error::custom)?;
        Ok(locator)
    }
}

impl BlobLocator {
    #[allow(clippy::too_many_arguments)]
    pub fn opaque(
        namespace: impl Into<String>,
        blob_id: impl Into<String>,
        uploader: StoreDeviceRegistrationRef,
        audience: RemoteAudience,
        scope: BlobScope,
        key_fingerprint: KeyFingerprint,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    ) -> Result<Self, BlobLocatorError> {
        let namespace = namespace.into();
        let blob_id = blob_id.into();
        let locator = Self::Opaque {
            namespace,
            blob_id,
            uploader,
            audience,
            scope,
            key_fingerprint,
            plaintext_size,
            plaintext_hash,
        };
        locator.validate()?;
        Ok(locator)
    }

    pub fn browsable(
        namespace: impl Into<String>,
        blob_id: impl Into<String>,
        uploader: StoreDeviceRegistrationRef,
        cloud_path: impl Into<String>,
        plaintext_size: u64,
        plaintext_hash: ObjectHash,
    ) -> Result<Self, BlobLocatorError> {
        let namespace = namespace.into();
        let blob_id = blob_id.into();
        let cloud_path = cloud_path.into();
        let locator = Self::Browsable {
            namespace,
            blob_id,
            uploader,
            cloud_path,
            plaintext_size,
            plaintext_hash,
        };
        locator.validate()?;
        Ok(locator)
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, BlobLocatorError> {
        let locator: Self = serde_json::from_slice(bytes).map_err(BlobLocatorError::Json)?;
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

    pub fn semantic_key(&self) -> String {
        match self {
            Self::Opaque { namespace, .. } => {
                format!("{namespace}/opaque/{}", self.locator_hash())
            }
            Self::Browsable {
                namespace,
                cloud_path,
                ..
            } => format!(
                "{namespace}/readable/{cloud_path}/{RESERVED_READABLE_VERSION_SEGMENT}/{}",
                self.locator_hash()
            ),
        }
    }

    /// Whether the stored object is sealed — an opaque home's blob, carrying a
    /// per-chunk authenticator. A browsable home stores the plaintext verbatim,
    /// so its object can only be verified as a whole against the row's hash.
    pub fn is_sealed(&self) -> bool {
        matches!(self, Self::Opaque { .. })
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

    pub fn uploader(&self) -> &StoreDeviceRegistrationRef {
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

    pub fn validate(&self) -> Result<(), BlobLocatorError> {
        match self {
            Self::Opaque {
                namespace, blob_id, ..
            } => {
                validate_namespace(namespace)?;
                validate_blob_id(blob_id)?;
            }
            Self::Browsable {
                namespace,
                blob_id,
                cloud_path,
                ..
            } => {
                validate_namespace(namespace)?;
                validate_blob_id(blob_id)?;
                validate_readable_path(cloud_path)?;
            }
        }
        Ok(())
    }
}

fn validate_blob_id(blob_id: &str) -> Result<(), BlobLocatorError> {
    validate_path_token(blob_id).map_err(|source| BlobLocatorError::UnsafeBlobId {
        value: blob_id.to_string(),
        source,
    })
}

fn validate_readable_path(cloud_path: &str) -> Result<(), BlobLocatorError> {
    validate_cloud_path(cloud_path).map_err(|source| BlobLocatorError::UnsafeCloudPath {
        value: cloud_path.to_string(),
        source,
    })?;
    if cloud_path
        .split('/')
        .any(|segment| segment == RESERVED_READABLE_VERSION_SEGMENT)
    {
        return Err(BlobLocatorError::ReservedCloudPath {
            value: cloud_path.to_string(),
        });
    }
    Ok(())
}

fn validate_namespace(namespace: &str) -> Result<(), BlobLocatorError> {
    validate_path_token(namespace).map_err(|source| BlobLocatorError::UnsafeNamespace {
        value: namespace.to_string(),
        source,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum BlobLocatorError {
    #[error("Local audience has no blob locator")]
    LocalAudience,
    #[error("unsafe blob namespace {value:?}: {source}")]
    UnsafeNamespace {
        value: String,
        #[source]
        source: coven_foundation::store_dir::PathTokenError,
    },
    #[error("unsafe blob id {value:?}: {source}")]
    UnsafeBlobId {
        value: String,
        #[source]
        source: coven_foundation::store_dir::PathTokenError,
    },
    #[error("unsafe readable blob path {value:?}: {source}")]
    UnsafeCloudPath {
        value: String,
        #[source]
        source: coven_foundation::store_dir::PathTokenError,
    },
    #[error("readable blob path uses reserved segment: {value:?}")]
    ReservedCloudPath { value: String },
    #[error("stored blob object key mismatch: expected {expected:?}, found {actual:?}")]
    ObjectKeyMismatch { expected: String, actual: String },
    #[error("malformed blob locator: {0}")]
    Json(#[source] serde_json::Error),
    #[error("blob locator bytes are not canonical")]
    NonCanonicalEncoding,
}

#[cfg(test)]
mod tests {
    use super::BlobScope;
    use super::*;
    use crate::circle::{Audience, CircleId};
    use crate::objects::ExactObjectRef;
    use crate::objects::ObjectSlot;
    use crate::store_commit::{ObjectHash, StoreDeviceRegistrationRef};
    use coven_keys::encryption::KeyFingerprint;

    fn hash(bytes: &[u8]) -> ObjectHash {
        ObjectHash::digest(bytes)
    }

    fn uploader() -> StoreDeviceRegistrationRef {
        let bytes = b"blob-locator uploader registration";
        StoreDeviceRegistrationRef {
            device_id: "11".repeat(32).parse().unwrap(),
            registration_hash: hash(bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/devices/blob-locator-uploader.json".to_string())
                    .unwrap(),
                bytes.len() as u64,
                hash(bytes),
            ),
        }
    }

    fn stored(locator: BlobLocator) -> StoredBlobRef {
        let key = locator.semantic_key();
        StoredBlobRef::new(
            locator,
            ExactObjectRef::new(ObjectSlot::logical(key).unwrap(), 4, hash(b"body")),
        )
        .unwrap()
    }

    #[test]
    fn opaque_locator_round_trips_canonical_bytes_and_path() {
        let uploader = uploader();
        let uploader_json = serde_json::to_string(&uploader).unwrap();
        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            uploader,
            RemoteAudience::Circle(CircleId::from_bytes([3; 16])),
            BlobScope::Derived("release-a".to_string()),
            KeyFingerprint::from_bytes([4; 32]),
            27,
            hash(b"cover"),
        )
        .expect("build locator");

        assert_eq!(
            locator.semantic_key(),
            format!("covers/opaque/{}", locator.locator_hash())
        );
        assert_eq!(BlobLocator::parse(&locator.to_bytes()).unwrap(), locator);
        assert_eq!(
            String::from_utf8(locator.to_bytes()).unwrap(),
            format!(
                "{{\"protection\":\"opaque\",\"namespace\":\"covers\",\"blob_id\":\"a1b2-blob\",\"uploader\":{uploader_json},\"audience\":{{\"circle\":\"{}\"}},\"scope\":{{\"Derived\":\"release-a\"}},\"key_fingerprint\":\"0404040404040404040404040404040404040404040404040404040404040404\",\"plaintext_size\":27,\"plaintext_hash\":\"{}\"}}",
                CircleId::from_bytes([3; 16]),
                hash(b"cover"),
            )
        );
    }

    #[test]
    fn browsable_locator_round_trips_canonical_bytes_and_version_path() {
        let locator = BlobLocator::browsable(
            "audio",
            "abcd-track",
            uploader(),
            "Artist/Album/01 Track.flac",
            91,
            hash(b"track"),
        )
        .expect("build locator");

        assert_eq!(
            locator.semantic_key(),
            format!(
                "audio/readable/Artist/Album/01 Track.flac/.coven-versions/{}",
                locator.locator_hash()
            )
        );
        assert_eq!(BlobLocator::parse(&locator.to_bytes()).unwrap(), locator);
    }

    #[test]
    fn stored_blob_ref_rejects_an_object_from_another_locator() {
        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            uploader(),
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 32]),
            4,
            hash(b"body"),
        )
        .unwrap();
        let wrong = ExactObjectRef::new(
            ObjectSlot::logical("covers/opaque/wrong.enc".to_string()).unwrap(),
            4,
            hash(b"body"),
        );

        assert!(matches!(
            StoredBlobRef::new(locator.clone(), wrong),
            Err(BlobLocatorError::ObjectKeyMismatch { .. })
        ));
        let valid = stored(locator);
        let bytes = serde_json::to_vec(&valid).unwrap();
        assert_eq!(
            serde_json::from_slice::<StoredBlobRef>(&bytes).unwrap(),
            valid
        );
    }

    #[test]
    fn locator_rejects_unsafe_reserved_and_noncanonical_paths() {
        assert!(matches!(
            BlobLocator::opaque(
                "../covers",
                "a1b2-blob",
                uploader(),
                RemoteAudience::Store,
                BlobScope::Master,
                KeyFingerprint::from_bytes([4; 32]),
                1,
                hash(b"x"),
            ),
            Err(BlobLocatorError::UnsafeNamespace { .. })
        ));
        assert!(matches!(
            BlobLocator::browsable(
                "audio",
                "abcd-track",
                uploader(),
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
                    uploader(),
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
            uploader(),
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 32]),
            1,
            hash(b"x"),
        )
        .unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&locator.to_bytes()).unwrap();
        value["semantic_key"] = serde_json::json!("covers/opaque/relocated");
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&value).unwrap()),
            Err(BlobLocatorError::Json(_))
        ));
    }

    #[test]
    fn locator_rejects_unknown_shape_and_noncanonical_bytes() {
        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            uploader(),
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 32]),
            1,
            hash(b"x"),
        )
        .unwrap();
        let bytes = locator.to_bytes();

        let mut unknown_field: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown_field["unknown"] = serde_json::json!(true);
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&unknown_field).unwrap()),
            Err(BlobLocatorError::Json(_))
        ));

        let mut unknown_variant: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown_variant["protection"] = serde_json::json!("unknown");
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&unknown_variant).unwrap()),
            Err(BlobLocatorError::Json(_))
        ));

        let mut noncanonical = bytes.clone();
        noncanonical.push(b'\n');
        assert!(matches!(
            BlobLocator::parse(&noncanonical),
            Err(BlobLocatorError::NonCanonicalEncoding)
        ));
    }

    #[test]
    fn locator_hash_is_the_digest_of_canonical_bytes() {
        let locator = BlobLocator::browsable(
            "audio",
            "abcd-track",
            uploader(),
            "Artist/Album/track.flac",
            91,
            hash(b"track"),
        )
        .unwrap();

        assert_eq!(locator.locator_hash(), hash(&locator.to_bytes()));
    }

    #[test]
    fn locator_parse_rejects_malformed_uploader_registration_reference() {
        let locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            uploader(),
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 32]),
            1,
            hash(b"x"),
        )
        .unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&locator.to_bytes()).unwrap();
        value["uploader"]["device_id"] = serde_json::json!("AA".repeat(32));
        assert!(matches!(
            BlobLocator::parse(&serde_json::to_vec(&value).unwrap()),
            Err(BlobLocatorError::Json(_))
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

#[cfg(test)]
mod row_upload_identity_tests {
    use super::*;
    use crate::blob::{locator_is_this_rows_upload, BlobRef, CacheFill, Provenance};
    use crate::objects::{ExactObjectRef, ObjectSlot};
    use crate::store_commit::StoreDeviceRegistrationRef;

    fn hash(bytes: &[u8]) -> ObjectHash {
        ObjectHash::digest(bytes)
    }

    fn uploader() -> StoreDeviceRegistrationRef {
        let bytes = b"row upload identity uploader registration";
        StoreDeviceRegistrationRef {
            device_id: "11".repeat(32).parse().unwrap(),
            registration_hash: hash(bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/devices/row-upload-identity.json".to_string())
                    .unwrap(),
                bytes.len() as u64,
                hash(bytes),
            ),
        }
    }

    fn row_blob() -> BlobRef {
        BlobRef {
            namespace: "release_files".to_string(),
            id: "03b1d792".to_string(),
            scope: BlobScope::Master,
            cloud_path: None,
            fill: CacheFill::CacheLazy,
            provenance: Provenance::UserProvided,
        }
    }

    fn sealed_under(fingerprint: [u8; 32], audience: RemoteAudience) -> BlobLocator {
        BlobLocator::opaque(
            "release_files",
            "03b1d792",
            uploader(),
            audience,
            BlobScope::Master,
            KeyFingerprint::from_bytes(fingerprint),
            13_205_924,
            hash(b"the row's plaintext"),
        )
        .unwrap()
    }

    /// A blob's stored locator names the key generation that sealed its bytes.
    /// Rotating the Store key changes what a *new* upload would be sealed
    /// under; it does not re-identify bytes already at the provider.
    ///
    /// Comparing a stored locator against one minted under today's key says the
    /// opposite, and that answer wedges the Store: a snapshot after any
    /// rotation asks the publisher to re-upload every pre-rotation blob, and a
    /// user-provided blob has no local file left to re-upload from.
    #[test]
    fn a_key_rotation_does_not_re_identify_a_blob_already_uploaded() {
        let blob = row_blob();
        let before_rotation = sealed_under([1; 32], RemoteAudience::Store);
        let after_rotation = sealed_under([2; 32], RemoteAudience::Store);

        assert_ne!(
            before_rotation, after_rotation,
            "the two locators differ only in the key that sealed them",
        );
        for locator in [&before_rotation, &after_rotation] {
            assert!(
                locator_is_this_rows_upload(
                    locator,
                    &blob,
                    13_205_924,
                    hash(b"the row's plaintext"),
                    &RemoteAudience::Store,
                ),
                "a generation change is not a different blob",
            );
        }
    }

    /// The rule still refuses what a re-seal really is: an audience move, or
    /// bytes that are not this row's.
    #[test]
    fn a_moved_audience_or_changed_content_is_not_this_rows_upload() {
        let blob = row_blob();
        let circle = crate::circle::CircleId::from_bytes([7; 16]);

        assert!(
            !locator_is_this_rows_upload(
                &sealed_under([1; 32], RemoteAudience::Circle(circle)),
                &blob,
                13_205_924,
                hash(b"the row's plaintext"),
                &RemoteAudience::Store,
            ),
            "a Circle blob is not the Store audience's upload",
        );
        assert!(
            !locator_is_this_rows_upload(
                &sealed_under([1; 32], RemoteAudience::Store),
                &blob,
                13_205_924,
                hash(b"other bytes"),
                &RemoteAudience::Store,
            ),
            "different plaintext is a different blob",
        );
    }
}
