use super::*;

/// The exact commit coordinate that first made a blob locator authoritative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobActivation {
    pub coord: StoreCommitCoord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedAudiencePackage {
    remote_object_id: ObjectHash,
    package: AudiencePackage,
    semantic_bytes: Vec<u8>,
    stored_bytes: Vec<u8>,
    object: ExactObjectRef,
}

impl PreparedAudiencePackage {
    pub(super) fn from_remote(remote: RemoteObjectRecord) -> Result<Self, DbError> {
        remote
            .validate()
            .map_err(|error| DbError::Message(format!("prepared remote package: {error}")))?;
        let is_package = match &remote {
            RemoteObjectRecord::CandidateCommit(_) | RemoteObjectRecord::RetainedAuthority(_) => {
                false
            }
            RemoteObjectRecord::CandidateExclusive(record) => matches!(
                record.identity.domain,
                CandidateExclusiveObjectDomain::StorePackage { .. }
                    | CandidateExclusiveObjectDomain::CirclePackage { .. }
            ),
            RemoteObjectRecord::SharedLiveSet(record) => matches!(
                record.identity.domain,
                SharedLiveSetObjectDomain::StorePackage { .. }
                    | SharedLiveSetObjectDomain::CirclePackage { .. }
            ),
        };
        if !is_package {
            return Err(DbError::Message(
                "prepared package index references a non-package remote object".to_string(),
            ));
        }
        let remote_object_id = remote.object_id();
        let semantic_bytes = remote.bytes().canonical_semantic_bytes().to_vec();
        let object = remote.object().clone();
        let stored_bytes = remote
            .bytes()
            .stored()
            .inline_bytes()
            .ok_or_else(|| {
                DbError::Message("prepared package remote object is not inline".to_string())
            })?
            .to_vec();
        Self::new(remote_object_id, semantic_bytes, stored_bytes, object)
    }

    pub(crate) fn new(
        remote_object_id: ObjectHash,
        semantic_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        object: ExactObjectRef,
    ) -> Result<Self, DbError> {
        let package = AudiencePackage::parse(&semantic_bytes)
            .map_err(|error| DbError::Message(format!("prepared audience package: {error}")))?;
        object.verify(&stored_bytes).map_err(|error| {
            DbError::Message(format!("prepared audience package stored bytes: {error}"))
        })?;
        Ok(Self {
            remote_object_id,
            package,
            semantic_bytes,
            stored_bytes,
            object,
        })
    }

    pub(crate) fn remote_object_id(&self) -> ObjectHash {
        self.remote_object_id
    }

    pub(crate) fn package(&self) -> &AudiencePackage {
        &self.package
    }

    pub(crate) fn semantic_bytes(&self) -> &[u8] {
        &self.semantic_bytes
    }

    pub(crate) fn stored_bytes(&self) -> &[u8] {
        &self.stored_bytes
    }

    pub(crate) fn object(&self) -> &ExactObjectRef {
        &self.object
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedAudienceBlob {
    remote_object_id: ObjectHash,
    audience: RemoteAudience,
    blob: StoredBlobRef,
    spool_path: Option<PathBuf>,
}

impl PreparedAudienceBlob {
    pub(crate) fn from_remote(
        audience: RemoteAudience,
        expected_locator_hash: &str,
        remote: RemoteObjectRecord,
        spool_path: Option<PathBuf>,
    ) -> Result<Self, DbError> {
        remote
            .validate()
            .map_err(|error| DbError::Message(format!("prepared remote blob: {error}")))?;
        if !matches!(
            &remote,
            RemoteObjectRecord::SharedLiveSet(record)
                if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob
        ) {
            return Err(DbError::Message(
                "prepared blob index references a non-blob remote object".to_string(),
            ));
        }
        let locator = BlobLocator::parse(remote.bytes().canonical_semantic_bytes())
            .map_err(|error| DbError::Message(format!("prepared blob locator: {error}")))?;
        if locator.locator_hash().to_string() != expected_locator_hash {
            return Err(DbError::Message(format!(
                "prepared blob locator hashes to {}, indexed as {expected_locator_hash}",
                locator.locator_hash()
            )));
        }
        if locator.audience() != audience {
            return Err(DbError::Message(format!(
                "prepared blob index audience {audience:?} differs from locator audience {:?}",
                locator.audience()
            )));
        }
        let requires_upload = matches!(
            &remote,
            RemoteObjectRecord::SharedLiveSet(record)
                if matches!(record.state, crate::sync::remote_object::OwnedObjectState::Prepared { .. })
        );
        if requires_upload && spool_path.is_none() {
            return Err(DbError::Message(
                "prepared blob awaiting upload has no local spool".to_string(),
            ));
        }
        if spool_path.as_ref().is_some_and(|path| !path.is_absolute()) {
            return Err(DbError::Message(
                "prepared blob local spool path is not absolute".to_string(),
            ));
        }
        let blob = StoredBlobRef::new(locator, remote.object().clone())
            .map_err(|error| DbError::Message(format!("prepared blob reference: {error}")))?;
        Ok(Self {
            remote_object_id: remote.object_id(),
            audience,
            blob,
            spool_path,
        })
    }

    pub(crate) fn remote_object_id(&self) -> ObjectHash {
        self.remote_object_id
    }

    pub(crate) fn audience(&self) -> &RemoteAudience {
        &self.audience
    }

    pub(crate) fn blob(&self) -> &StoredBlobRef {
        &self.blob
    }

    pub(crate) fn spool_path(&self) -> Option<&Path> {
        self.spool_path.as_deref()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedAudienceObjects {
    pub packages: Vec<PreparedAudiencePackage>,
    pub blobs: Vec<PreparedAudienceBlob>,
}

pub(crate) struct PreparedRemoteObject {
    pub record: RemoteObjectRecord,
    pub spool_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MakeRemoteIntentState {
    Uploading,
    Cancelling,
    Publishing(WriteId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoredBlobReferenceState {
    NotLiveRemote,
    LiveRemote,
    Unresolved,
}

pub(crate) fn validate_prepared_audience_blob_graph(
    object_ids: &std::collections::BTreeSet<ObjectHash>,
    audiences: &PreparedAudienceObjects,
) -> Result<(), DbError> {
    let mut indexed = std::collections::BTreeSet::new();
    for package in &audiences.packages {
        if !indexed.insert(package.remote_object_id()) {
            return Err(DbError::Message(
                "prepared audience objects contain a duplicate package body".to_string(),
            ));
        }
    }
    for blob in &audiences.blobs {
        if !indexed.insert(blob.remote_object_id()) {
            return Err(DbError::Message(
                "prepared audience objects contain a duplicate blob body".to_string(),
            ));
        }
    }
    if &indexed != object_ids {
        return Err(DbError::Message(
            "closed remote objects differ from package/blob indexes".to_string(),
        ));
    }
    validate_prepared_audience_blob_bindings(audiences)
}

pub(super) fn validate_prepared_audience_blob_bindings(
    audiences: &PreparedAudienceObjects,
) -> Result<(), DbError> {
    for package in &audiences.packages {
        let audience = package.package().audience().remote_audience();
        for binding in package.package().blob_bindings() {
            if !audiences
                .blobs
                .iter()
                .any(|blob| blob.audience() == &audience && blob.blob() == binding.blob())
            {
                return Err(DbError::Message(
                    "prepared package blob binding has no exact blob index".to_string(),
                ));
            }
        }
    }
    for blob in &audiences.blobs {
        if !audiences.packages.iter().any(|package| {
            package.package().audience().remote_audience() == *blob.audience()
                && package
                    .package()
                    .blob_bindings()
                    .iter()
                    .any(|binding| binding.blob() == blob.blob())
        }) {
            return Err(DbError::Message(
                "prepared blob index has no exact package binding".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "invariant-tests")]
#[doc(hidden)]
pub fn exercise_exact_outbound_blob_graph(
    circle: bool,
    include_body: bool,
    include_locator: bool,
    include_binding: bool,
) -> Result<(), String> {
    use crate::blob::BlobScope;
    use crate::storage::cloud::ObjectSlot;
    use crate::sync::audience_package::RowBlobLocatorBinding;
    use crate::sync::causal_grants::AuthorStreamId;
    use crate::sync::circle::CircleId;
    use crate::sync::circle_control::CircleControlCoord;
    use crate::sync::store_commit::{CandidateFamilyId, StoreCommitCoord};

    let store_root_hash = ObjectHash::digest(b"outbound-graph-store");
    let write_id = WriteId::from_generated("outbound-graph-write".to_string());
    let coord = StoreCommitCoord {
        stream_id: AuthorStreamId::from_bytes([4; 32]),
        sequence: 1,
    };
    let candidate_family = CandidateFamilyId::from_hash(ObjectHash::digest(b"outbound-family"));
    let remote_audience = if circle {
        RemoteAudience::Circle(CircleId::from_bytes([7; 16]))
    } else {
        RemoteAudience::Store
    };
    let uploader_bytes = b"outbound graph uploader registration";
    let uploader = StoreDeviceRegistrationRef {
        device_id: "01"
            .repeat(32)
            .parse::<crate::sync::store_commit::StoreDeviceId>()
            .map_err(|error| error.to_string())?,
        registration_hash: ObjectHash::digest(uploader_bytes),
        object: ExactObjectRef::new(
            ObjectSlot::logical("store-v1/registrations/outbound-graph.json".to_string())
                .map_err(|error| error.to_string())?,
            uploader_bytes.len() as u64,
            ObjectHash::digest(uploader_bytes),
        ),
    };
    let locator = BlobLocator::opaque(
        "media".to_string(),
        "blob-a".to_string(),
        uploader,
        remote_audience.clone(),
        BlobScope::Master,
        crate::KeyFingerprint::from_bytes([3; 32]),
        7,
        ObjectHash::digest(b"content"),
    )
    .map_err(|error| error.to_string())?;
    let stored_bytes = b"sealed-content".to_vec();
    let object = ExactObjectRef::new(
        ObjectSlot::logical(locator.semantic_key()).map_err(|error| error.to_string())?,
        stored_bytes.len() as u64,
        ObjectHash::digest(&stored_bytes),
    );
    let stored = StoredBlobRef::new(locator, object).map_err(|error| error.to_string())?;
    let bindings = if include_binding {
        vec![
            RowBlobLocatorBinding::new("items", "row-a", "stamp-a", "media_blob", stored.clone())
                .map_err(|error| error.to_string())?,
        ]
    } else {
        Vec::new()
    };
    let package = if let RemoteAudience::Circle(circle_id) = remote_audience {
        AudiencePackage::circle(
            store_root_hash,
            candidate_family,
            write_id.clone(),
            coord,
            1,
            circle_id,
            CircleControlCoord {
                device_id: "01".repeat(32),
                stream_id: AuthorStreamId::from_bytes([5; 32]),
                author_pubkey: "author-a".to_string(),
                author_owner_grant: crate::sync::test_helpers::test_membership_grant_id(
                    "outbound-graph owner grant",
                ),
                seq: 1,
                control_hash: ObjectHash::digest(b"circle-control"),
            },
            crate::KeyFingerprint::from_bytes([3; 32]),
            b"changeset".to_vec(),
            bindings,
        )
    } else {
        AudiencePackage::store(
            store_root_hash,
            candidate_family,
            write_id,
            coord,
            1,
            b"changeset".to_vec(),
            bindings,
        )
    }
    .map_err(|error| error.to_string())?;
    let package_bytes = package.to_bytes();
    let package_object = ExactObjectRef::new(
        ObjectSlot::logical("test/package".to_string()).map_err(|error| error.to_string())?,
        package_bytes.len() as u64,
        ObjectHash::digest(&package_bytes),
    );
    let package_id = ObjectHash::digest(b"package-record");
    let blob_id = ObjectHash::digest(b"blob-record");
    let packages = vec![PreparedAudiencePackage::new(
        package_id,
        package_bytes.clone(),
        package_bytes,
        package_object,
    )
    .map_err(|error| error.to_string())?];
    let blobs = if include_locator {
        vec![PreparedAudienceBlob {
            remote_object_id: blob_id,
            audience: remote_audience,
            blob: stored,
            spool_path: Some(PathBuf::from("/outbound-blob.spool")),
        }]
    } else {
        Vec::new()
    };
    let mut object_ids = std::collections::BTreeSet::from([package_id]);
    if include_body {
        object_ids.insert(blob_id);
    }
    validate_prepared_audience_blob_graph(&object_ids, &PreparedAudienceObjects { packages, blobs })
        .map_err(|error| error.to_string())
}
