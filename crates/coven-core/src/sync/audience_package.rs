use serde::{Deserialize, Serialize};

use crate::blob::locator::{RemoteAudience, StoredBlobRef};
use crate::encryption::KeyFingerprint;
use crate::sync::circle::CircleId;
use crate::sync::circle_control::CircleControlCoord;
use crate::sync::store_commit::{
    CandidateFamilyId, ObjectHash, StoreCommitCoord, StoreDeviceRegistrationRef,
    STORE_PROTOCOL_VERSION,
};
use crate::WriteId;

/// The Store or Circle whose exact package bytes carry a changeset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PackageAudience {
    Store,
    Circle {
        circle_id: CircleId,
        control: CircleControlCoord,
        key_fingerprint: KeyFingerprint,
    },
}

impl PackageAudience {
    pub fn remote_audience(&self) -> RemoteAudience {
        match self {
            Self::Store => RemoteAudience::Store,
            Self::Circle { circle_id, .. } => RemoteAudience::Circle(*circle_id),
        }
    }
}

/// One declared row blob and the exact immutable locator committed beside it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowBlobLocatorBinding {
    table: String,
    row_id: String,
    row_stamp: String,
    column: String,
    blob: StoredBlobRef,
}

impl RowBlobLocatorBinding {
    pub fn new(
        table: impl Into<String>,
        row_id: impl Into<String>,
        row_stamp: impl Into<String>,
        column: impl Into<String>,
        blob: StoredBlobRef,
    ) -> Result<Self, AudiencePackageError> {
        let binding = Self {
            table: table.into(),
            row_id: row_id.into(),
            row_stamp: row_stamp.into(),
            column: column.into(),
            blob,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn row_id(&self) -> &str {
        &self.row_id
    }

    pub fn row_stamp(&self) -> &str {
        &self.row_stamp
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn blob(&self) -> &StoredBlobRef {
        &self.blob
    }

    fn sort_key(&self) -> (&str, &str, &str, &str) {
        (&self.table, &self.row_id, &self.column, &self.row_stamp)
    }

    fn identity(&self) -> (&str, &str, &str) {
        (&self.table, &self.row_id, &self.column)
    }

    fn validate(&self) -> Result<(), AudiencePackageError> {
        for (field, value) in [
            ("table", self.table.as_str()),
            ("row_id", self.row_id.as_str()),
            ("row_stamp", self.row_stamp.as_str()),
            ("column", self.column.as_str()),
        ] {
            if value.is_empty() {
                return Err(AudiencePackageError::EmptyBindingField(field));
            }
        }
        Ok(())
    }
}

/// Canonical bytes for one audience partition of a Store write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudiencePackage {
    version: u32,
    store_root_hash: ObjectHash,
    candidate_family: CandidateFamilyId,
    write_id: WriteId,
    commit_coord: StoreCommitCoord,
    schema_version: u32,
    audience: PackageAudience,
    changeset: Vec<u8>,
    blob_bindings: Vec<RowBlobLocatorBinding>,
}

impl AudiencePackage {
    #[allow(clippy::too_many_arguments)]
    pub fn store(
        store_root_hash: ObjectHash,
        candidate_family: CandidateFamilyId,
        write_id: WriteId,
        commit_coord: StoreCommitCoord,
        schema_version: u32,
        changeset: Vec<u8>,
        blob_bindings: Vec<RowBlobLocatorBinding>,
    ) -> Result<Self, AudiencePackageError> {
        Self::new(
            store_root_hash,
            candidate_family,
            write_id,
            commit_coord,
            schema_version,
            PackageAudience::Store,
            changeset,
            blob_bindings,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn circle(
        store_root_hash: ObjectHash,
        candidate_family: CandidateFamilyId,
        write_id: WriteId,
        commit_coord: StoreCommitCoord,
        schema_version: u32,
        circle_id: CircleId,
        control: CircleControlCoord,
        key_fingerprint: KeyFingerprint,
        changeset: Vec<u8>,
        blob_bindings: Vec<RowBlobLocatorBinding>,
    ) -> Result<Self, AudiencePackageError> {
        Self::new(
            store_root_hash,
            candidate_family,
            write_id,
            commit_coord,
            schema_version,
            PackageAudience::Circle {
                circle_id,
                control,
                key_fingerprint,
            },
            changeset,
            blob_bindings,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        store_root_hash: ObjectHash,
        candidate_family: CandidateFamilyId,
        write_id: WriteId,
        commit_coord: StoreCommitCoord,
        schema_version: u32,
        audience: PackageAudience,
        changeset: Vec<u8>,
        mut blob_bindings: Vec<RowBlobLocatorBinding>,
    ) -> Result<Self, AudiencePackageError> {
        blob_bindings.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        let package = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            candidate_family,
            write_id,
            commit_coord,
            schema_version,
            audience,
            changeset,
            blob_bindings,
        };
        package.validate()?;
        Ok(package)
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, AudiencePackageError> {
        let package: Self = serde_json::from_slice(bytes)
            .map_err(|error| AudiencePackageError::Malformed(error.to_string()))?;
        package.validate()?;
        if package.to_bytes() != bytes {
            return Err(AudiencePackageError::NonCanonicalEncoding);
        }
        Ok(package)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("AudiencePackage serialization cannot fail")
    }

    pub fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub fn write_id(&self) -> &WriteId {
        &self.write_id
    }

    pub fn candidate_family(&self) -> CandidateFamilyId {
        self.candidate_family
    }

    pub fn commit_coord(&self) -> &StoreCommitCoord {
        &self.commit_coord
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn audience(&self) -> &PackageAudience {
        &self.audience
    }

    pub fn changeset(&self) -> &[u8] {
        &self.changeset
    }

    pub fn blob_bindings(&self) -> &[RowBlobLocatorBinding] {
        &self.blob_bindings
    }

    /// Require every exact blob locator in this package to name the registration
    /// that authored the enclosing Store commit.
    pub fn validate_blob_uploader(
        &self,
        author: &StoreDeviceRegistrationRef,
    ) -> Result<(), AudiencePackageError> {
        for binding in &self.blob_bindings {
            let actual = binding.blob().locator().uploader();
            if actual != author {
                return Err(AudiencePackageError::LocatorUploaderMismatch {
                    table: binding.table.clone(),
                    row_id: binding.row_id.clone(),
                    expected: Box::new(author.clone()),
                    actual: Box::new(actual.clone()),
                });
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), AudiencePackageError> {
        if self.version != STORE_PROTOCOL_VERSION {
            return Err(AudiencePackageError::UnsupportedVersion(self.version));
        }
        self.commit_coord
            .validate()
            .map_err(|error| AudiencePackageError::InvalidCommitCoord(error.to_string()))?;
        if let PackageAudience::Circle { control, .. } = &self.audience {
            control
                .validate()
                .map_err(|error| AudiencePackageError::InvalidCircleControl(error.to_string()))?;
        }
        let expected_audience = self.audience.remote_audience();
        let mut previous_sort_key = None;
        let mut identities = std::collections::BTreeSet::new();
        for binding in &self.blob_bindings {
            binding.validate()?;
            if binding.blob.locator().audience() != expected_audience {
                return Err(AudiencePackageError::LocatorAudienceMismatch {
                    table: binding.table.clone(),
                    row_id: binding.row_id.clone(),
                    expected: expected_audience.clone(),
                    actual: binding.blob.locator().audience(),
                });
            }
            if let PackageAudience::Circle {
                key_fingerprint, ..
            } = &self.audience
            {
                if let Some(actual) = binding
                    .blob
                    .locator()
                    .key_fingerprint()
                    .filter(|actual| *actual != *key_fingerprint)
                {
                    return Err(AudiencePackageError::LocatorKeyFingerprintMismatch {
                        table: binding.table.clone(),
                        row_id: binding.row_id.clone(),
                        expected: *key_fingerprint,
                        actual,
                    });
                }
            }
            if !identities.insert(binding.identity()) {
                return Err(AudiencePackageError::DuplicateBinding {
                    table: binding.table.clone(),
                    row_id: binding.row_id.clone(),
                    row_stamp: binding.row_stamp.clone(),
                    column: binding.column.clone(),
                });
            }
            let sort_key = binding.sort_key();
            if let Some(previous) = previous_sort_key {
                if previous > sort_key {
                    return Err(AudiencePackageError::UnsortedBindings);
                }
            }
            previous_sort_key = Some(sort_key);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AudiencePackageError {
    #[error("unsupported audience package version {0}")]
    UnsupportedVersion(u32),
    #[error("invalid audience package Store commit coordinate: {0}")]
    InvalidCommitCoord(String),
    #[error("invalid Circle control coordinate: {0}")]
    InvalidCircleControl(String),
    #[error("row blob binding has empty {0}")]
    EmptyBindingField(&'static str),
    #[error(
        "row blob locator audience mismatch for {table:?}/{row_id:?}: expected {expected:?}, found {actual:?}"
    )]
    LocatorAudienceMismatch {
        table: String,
        row_id: String,
        expected: RemoteAudience,
        actual: RemoteAudience,
    },
    #[error(
        "row blob locator key fingerprint mismatch for {table:?}/{row_id:?}: expected {expected}, found {actual:?}"
    )]
    LocatorKeyFingerprintMismatch {
        table: String,
        row_id: String,
        expected: KeyFingerprint,
        actual: KeyFingerprint,
    },
    #[error(
        "row blob locator uploader mismatch for {table:?}/{row_id:?}: expected {expected:?}, found {actual:?}"
    )]
    LocatorUploaderMismatch {
        table: String,
        row_id: String,
        expected: Box<StoreDeviceRegistrationRef>,
        actual: Box<StoreDeviceRegistrationRef>,
    },
    #[error(
        "duplicate row blob locator binding for {table:?}/{row_id:?}/{column:?} at {row_stamp:?}"
    )]
    DuplicateBinding {
        table: String,
        row_id: String,
        row_stamp: String,
        column: String,
    },
    #[error("row blob locator bindings are not canonically sorted")]
    UnsortedBindings,
    #[error("malformed audience package: {0}")]
    Malformed(String),
    #[error("audience package bytes are not canonical")]
    NonCanonicalEncoding,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::locator::{BlobLocator, RemoteAudience, StoredBlobRef};
    use crate::storage::cloud::ObjectSlot;
    use crate::sync::circle::CircleId;
    use crate::sync::circle_control::CircleControlCoord;
    use crate::sync::membership::AuthorStreamId;
    use crate::sync::storage::ExactObjectRef;
    use crate::sync::store_commit::{
        CandidateFamilyId, ObjectHash, StoreCommitCoord, StoreDeviceRegistrationRef,
    };
    use crate::{BlobScope, KeyFingerprint, WriteId};

    fn uploader() -> StoreDeviceRegistrationRef {
        let bytes = b"audience-package uploader registration";
        StoreDeviceRegistrationRef {
            device_id: "11".repeat(32).parse().unwrap(),
            registration_hash: ObjectHash::digest(bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/devices/audience-package-uploader.json".to_string())
                    .unwrap(),
                bytes.len() as u64,
                ObjectHash::digest(bytes),
            ),
        }
    }

    fn candidate_family() -> CandidateFamilyId {
        CandidateFamilyId::from_hash(ObjectHash::digest(b"audience-package candidate family"))
    }

    fn merge_coord() -> StoreCommitCoord {
        StoreCommitCoord {
            stream_id: AuthorStreamId::from_bytes([3; 32]),
            sequence: 3,
        }
    }

    fn circle_control_coord() -> CircleControlCoord {
        CircleControlCoord {
            device_id: "device-a".to_string(),
            stream_id: AuthorStreamId::from_bytes([4; 32]),
            author_pubkey: "22".repeat(32),
            author_owner_grant: crate::sync::test_helpers::test_membership_grant_id(
                "audience-package owner",
            ),
            seq: 4,
            control_hash: ObjectHash::digest(b"control"),
        }
    }

    fn locator(id: &str, audience: RemoteAudience) -> BlobLocator {
        BlobLocator::opaque(
            "covers",
            id,
            uploader(),
            audience,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 8]),
            7,
            ObjectHash::digest(id.as_bytes()),
        )
        .unwrap()
    }

    fn stored(locator: BlobLocator) -> StoredBlobRef {
        let key = locator.semantic_key();
        StoredBlobRef::new(
            locator,
            ExactObjectRef::new(
                ObjectSlot::logical(key).unwrap(),
                6,
                ObjectHash::digest(b"stored"),
            ),
        )
        .unwrap()
    }

    fn binding(row: &str, locator: BlobLocator) -> RowBlobLocatorBinding {
        RowBlobLocatorBinding::new(
            "covers",
            row,
            "0000000001000-0000-device",
            "blob_id",
            stored(locator),
        )
        .unwrap()
    }

    #[test]
    fn store_package_sorts_bindings_and_round_trips_canonical_bytes() {
        let package = AudiencePackage::store(
            ObjectHash::digest(b"root"),
            candidate_family(),
            WriteId::from_generated("write-a".to_string()),
            merge_coord(),
            8,
            b"changeset".to_vec(),
            vec![
                binding("row-b", locator("b2c3-blob", RemoteAudience::Store)),
                binding("row-a", locator("a1b2-blob", RemoteAudience::Store)),
            ],
        )
        .unwrap();

        assert_eq!(package.blob_bindings()[0].row_id(), "row-a");
        let bytes = package.to_bytes();
        assert_eq!(AudiencePackage::parse(&bytes).unwrap(), package);
        assert_eq!(bytes, package.to_bytes());
    }

    #[test]
    fn store_package_has_literal_canonical_bytes() {
        let root = ObjectHash::digest(b"root");
        let package = AudiencePackage::store(
            root,
            candidate_family(),
            WriteId::from_generated("write-a".to_string()),
            merge_coord(),
            8,
            b"cs".to_vec(),
            Vec::new(),
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(package.to_bytes()).unwrap(),
            format!(
                "{{\"version\":1,\"store_root_hash\":\"{root}\",\"candidate_family\":\"{}\",\"write_id\":\"write-a\",\"commit_coord\":{{\"stream_id\":\"{}\",\"sequence\":3}},\"schema_version\":8,\"audience\":\"store\",\"changeset\":[99,115],\"blob_bindings\":[]}}",
                candidate_family().as_hash(),
                AuthorStreamId::from_bytes([3; 32]),
            )
        );
    }

    #[test]
    fn package_refuses_duplicate_row_binding() {
        let one = binding("row-a", locator("a1b2-blob", RemoteAudience::Store));
        let two = binding("row-a", locator("b2c3-blob", RemoteAudience::Store));
        assert!(matches!(
            AudiencePackage::store(
                ObjectHash::digest(b"root"),
                candidate_family(),
                WriteId::from_generated("write-a".to_string()),
                merge_coord(),
                8,
                Vec::new(),
                vec![one, two],
            ),
            Err(AudiencePackageError::DuplicateBinding { .. })
        ));
    }

    #[test]
    fn package_refuses_locator_from_another_audience() {
        let circle = CircleId::from_bytes([8; 16]);
        assert!(matches!(
            AudiencePackage::store(
                ObjectHash::digest(b"root"),
                candidate_family(),
                WriteId::from_generated("write-a".to_string()),
                merge_coord(),
                8,
                Vec::new(),
                vec![binding(
                    "row-a",
                    locator("a1b2-blob", RemoteAudience::Circle(circle))
                )],
            ),
            Err(AudiencePackageError::LocatorAudienceMismatch { .. })
        ));
    }

    #[test]
    fn circle_package_refuses_browsable_locator() {
        let circle = CircleId::from_bytes([8; 16]);
        let browsable = BlobLocator::browsable(
            "audio",
            "abcd-track",
            uploader(),
            "Artist/Album/track.flac",
            7,
            ObjectHash::digest(b"track"),
        )
        .unwrap();

        assert!(matches!(
            AudiencePackage::circle(
                ObjectHash::digest(b"root"),
                candidate_family(),
                WriteId::from_generated("write-a".to_string()),
                merge_coord(),
                8,
                circle,
                circle_control_coord(),
                KeyFingerprint::from_bytes([4; 8]),
                Vec::new(),
                vec![binding("row-a", browsable)],
            ),
            Err(AudiencePackageError::LocatorAudienceMismatch { .. })
        ));
    }

    #[test]
    fn circle_package_refuses_locator_from_another_key() {
        let circle = CircleId::from_bytes([8; 16]);
        let wrong_key_locator = BlobLocator::opaque(
            "covers",
            "a1b2-blob",
            uploader(),
            RemoteAudience::Circle(circle),
            BlobScope::Master,
            KeyFingerprint::from_bytes([5; 8]),
            7,
            ObjectHash::digest(b"cover"),
        )
        .unwrap();

        assert!(matches!(
            AudiencePackage::circle(
                ObjectHash::digest(b"root"),
                candidate_family(),
                WriteId::from_generated("write-a".to_string()),
                merge_coord(),
                8,
                circle,
                circle_control_coord(),
                KeyFingerprint::from_bytes([4; 8]),
                Vec::new(),
                vec![binding("row-a", wrong_key_locator)],
            ),
            Err(AudiencePackageError::LocatorKeyFingerprintMismatch { .. })
        ));
    }

    #[test]
    fn package_rejects_unknown_shape_and_noncanonical_bytes() {
        let package = AudiencePackage::store(
            ObjectHash::digest(b"root"),
            candidate_family(),
            WriteId::from_generated("write-a".to_string()),
            merge_coord(),
            8,
            b"changeset".to_vec(),
            Vec::new(),
        )
        .unwrap();
        let bytes = package.to_bytes();

        let mut unknown_field: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown_field["unknown"] = serde_json::json!(true);
        assert!(matches!(
            AudiencePackage::parse(&serde_json::to_vec(&unknown_field).unwrap()),
            Err(AudiencePackageError::Malformed(_))
        ));

        let mut unknown_variant: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown_variant["audience"] = serde_json::json!({ "unknown": {} });
        assert!(matches!(
            AudiencePackage::parse(&serde_json::to_vec(&unknown_variant).unwrap()),
            Err(AudiencePackageError::Malformed(_))
        ));

        let mut noncanonical = bytes;
        noncanonical.push(b'\n');
        assert_eq!(
            AudiencePackage::parse(&noncanonical),
            Err(AudiencePackageError::NonCanonicalEncoding)
        );
    }

    #[test]
    fn circle_package_binds_control_fingerprint_and_locator_audience() {
        let circle = CircleId::from_bytes([8; 16]);
        let package = AudiencePackage::circle(
            ObjectHash::digest(b"root"),
            candidate_family(),
            WriteId::from_generated("write-a".to_string()),
            merge_coord(),
            8,
            circle,
            circle_control_coord(),
            KeyFingerprint::from_bytes([4; 8]),
            b"circle changeset".to_vec(),
            vec![binding(
                "row-a",
                locator("a1b2-blob", RemoteAudience::Circle(circle)),
            )],
        )
        .unwrap();

        assert_eq!(
            AudiencePackage::parse(&package.to_bytes()).unwrap(),
            package
        );
    }
}
