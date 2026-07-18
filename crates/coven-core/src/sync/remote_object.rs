//! Closed local publication and ownership state for remote package and blob objects.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::circle::CircleId;
use super::storage::ExactObjectRef;
use super::store_commit::{CandidateFamilyId, ObjectHash, StoreBatchCommitRef, StreamActivationId};

const REMOTE_OBJECT_ID_DOMAIN: &[u8] = b"coven.remote-object-id.v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RemoteObjectRecord {
    CandidateExclusive(CandidateObjectRecord),
    SharedLiveSet(SharedObjectRecord),
}

impl RemoteObjectRecord {
    pub(crate) fn snapshot_activated_blob(
        stored: &crate::blob::locator::StoredBlobRef,
        owner: SnapshotObjectOwner,
    ) -> Result<Self, RemoteObjectRecordError> {
        let locator_bytes = stored.locator().to_bytes();
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: stored.object().clone(),
            },
            bytes: RemoteObjectBytes::blob(locator_bytes, stored.object().clone())?,
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::Snapshot(owner)]),
                },
            },
        });
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn activated_blob(
        stored: &crate::blob::locator::StoredBlobRef,
        owner: StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let locator_bytes = stored.locator().to_bytes();
        let record = Self::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&locator_bytes),
                object: stored.object().clone(),
            },
            bytes: RemoteObjectBytes::blob(locator_bytes, stored.object().clone())?,
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner)]),
                },
            },
        });
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn merge_blob_activation(
        &mut self,
        stored: &crate::blob::locator::StoredBlobRef,
        owner: &StoreBatchCommitRef,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.bytes.canonical_semantic_bytes() != locator_bytes
            || record.bytes.stored().object() != stored.object()
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        match &mut record.state {
            OwnedObjectState::Prepared { ownership } => {
                let mut pending = ownership.pending.clone();
                pending.remove(owner);
                record.state = OwnedObjectState::UploadedVerified {
                    ownership: SharedObjectOwnership {
                        pending,
                        activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner.clone())]),
                    },
                };
            }
            OwnedObjectState::UploadedVerified { ownership } => {
                ownership.pending.remove(owner);
                ownership
                    .activated
                    .insert(SharedObjectOwner::StoreCommit(owner.clone()));
            }
        }
        self.validate()
    }

    pub(crate) fn merge_snapshot_owner(
        &mut self,
        stored: &crate::blob::locator::StoredBlobRef,
        owner: SnapshotObjectOwner,
    ) -> Result<(), RemoteObjectRecordError> {
        let Self::SharedLiveSet(record) = self else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        let locator_bytes = stored.locator().to_bytes();
        if record.identity.domain != SharedLiveSetObjectDomain::StoredBlob
            || record.identity.semantic_hash != ObjectHash::digest(&locator_bytes)
            || record.identity.object != *stored.object()
            || record.bytes.canonical_semantic_bytes() != locator_bytes
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        let OwnedObjectState::UploadedVerified { ownership } = &mut record.state else {
            return Err(RemoteObjectRecordError::InvalidActivation);
        };
        ownership
            .activated
            .insert(SharedObjectOwner::Snapshot(owner));
        self.validate()
    }

    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::CandidateExclusive(record) => &record.identity.object,
            Self::SharedLiveSet(record) => &record.identity.object,
        }
    }

    pub(crate) fn bytes(&self) -> &RemoteObjectBytes {
        match self {
            Self::CandidateExclusive(record) => &record.bytes,
            Self::SharedLiveSet(record) => &record.bytes,
        }
    }

    pub(crate) fn object_id(&self) -> ObjectHash {
        remote_object_id(self.object())
    }

    pub(crate) fn is_activated_stored_blob(&self) -> bool {
        matches!(
            self,
            Self::SharedLiveSet(record)
                if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob
                    && matches!(
                        &record.state,
                        OwnedObjectState::UploadedVerified { ownership }
                            if !ownership.activated.is_empty()
                    )
        )
    }

    pub(crate) fn snapshot_owners(&self) -> impl Iterator<Item = &SnapshotObjectOwner> {
        let owners = match self {
            Self::SharedLiveSet(record)
                if record.identity.domain == SharedLiveSetObjectDomain::StoredBlob =>
            {
                match &record.state {
                    OwnedObjectState::UploadedVerified { ownership } => Some(&ownership.activated),
                    OwnedObjectState::Prepared { .. } => None,
                }
            }
            _ => None,
        };
        owners.into_iter().flat_map(|owners| {
            owners.iter().filter_map(|owner| match owner {
                SharedObjectOwner::Snapshot(owner) => Some(owner),
                SharedObjectOwner::StoreCommit(_) => None,
            })
        })
    }

    pub(crate) fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        self.bytes().validate()?;
        match self {
            Self::CandidateExclusive(record) => {
                record
                    .identity
                    .validate_semantic(record.bytes.canonical_semantic_bytes())?;
                let package = super::audience_package::AudiencePackage::parse(
                    record.bytes.canonical_semantic_bytes(),
                )
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
                match (&record.identity.domain, package.audience()) {
                    (
                        CandidateExclusiveObjectDomain::StorePackage,
                        super::audience_package::PackageAudience::Store,
                    ) => {}
                    (
                        CandidateExclusiveObjectDomain::CirclePackage { circle_id },
                        super::audience_package::PackageAudience::Circle {
                            circle_id: package_circle,
                            ..
                        },
                    ) if circle_id == package_circle => {}
                    _ => return Err(RemoteObjectRecordError::DomainMismatch),
                }
                record.state.validate()?;
            }
            Self::SharedLiveSet(record) => {
                record
                    .identity
                    .validate_semantic(record.bytes.canonical_semantic_bytes())?;
                match record.identity.domain {
                    SharedLiveSetObjectDomain::StoredBlob => {
                        let locator = crate::blob::locator::BlobLocator::parse(
                            record.bytes.canonical_semantic_bytes(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                        crate::blob::locator::StoredBlobRef::new(
                            locator,
                            record.identity.object.clone(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                    }
                    SharedLiveSetObjectDomain::StorePackage => {
                        let package = super::audience_package::AudiencePackage::parse(
                            record.bytes.canonical_semantic_bytes(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                        if !matches!(
                            package.audience(),
                            super::audience_package::PackageAudience::Store
                        ) {
                            return Err(RemoteObjectRecordError::DomainMismatch);
                        }
                    }
                    SharedLiveSetObjectDomain::CirclePackage => {
                        let package = super::audience_package::AudiencePackage::parse(
                            record.bytes.canonical_semantic_bytes(),
                        )
                        .map_err(|error| {
                            RemoteObjectRecordError::InvalidDomain(error.to_string())
                        })?;
                        if !matches!(
                            package.audience(),
                            super::audience_package::PackageAudience::Circle { .. }
                        ) {
                            return Err(RemoteObjectRecordError::DomainMismatch);
                        }
                    }
                }
                record.state.validate()?;
            }
        }
        if self.object() != self.bytes().stored().object() {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        Ok(())
    }

    pub(crate) fn into_activated(
        self,
        commit: &StoreBatchCommitRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let activated = match self {
            Self::CandidateExclusive(record) => {
                let CandidateObjectState::UploadedVerified { ownership } = &record.state else {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                };
                if ownership.pending.len() != 1 || !ownership.pending.contains(commit) {
                    return Err(RemoteObjectRecordError::InvalidActivation);
                }
                let domain = match record.identity.domain {
                    CandidateExclusiveObjectDomain::StorePackage => {
                        SharedLiveSetObjectDomain::StorePackage
                    }
                    CandidateExclusiveObjectDomain::CirclePackage { .. } => {
                        SharedLiveSetObjectDomain::CirclePackage
                    }
                };
                Self::SharedLiveSet(SharedObjectRecord {
                    identity: SharedLiveSetObjectRef {
                        domain,
                        semantic_hash: record.identity.semantic_hash,
                        object: record.identity.object,
                    },
                    bytes: record.bytes,
                    state: OwnedObjectState::UploadedVerified {
                        ownership: SharedObjectOwnership {
                            pending: BTreeSet::new(),
                            activated: BTreeSet::from([SharedObjectOwner::StoreCommit(
                                commit.clone(),
                            )]),
                        },
                    },
                })
            }
            Self::SharedLiveSet(mut record) => {
                match &mut record.state {
                    OwnedObjectState::UploadedVerified { ownership } => {
                        if ownership.pending.remove(commit) {
                            ownership
                                .activated
                                .insert(SharedObjectOwner::StoreCommit(commit.clone()));
                        } else if !ownership
                            .activated
                            .contains(&SharedObjectOwner::StoreCommit(commit.clone()))
                        {
                            return Err(RemoteObjectRecordError::InvalidActivation);
                        }
                    }
                    OwnedObjectState::Prepared { .. } => {
                        return Err(RemoteObjectRecordError::InvalidActivation);
                    }
                }
                Self::SharedLiveSet(record)
            }
        };
        activated.validate()?;
        Ok(activated)
    }

    pub(crate) fn mark_uploaded_verified(&mut self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::CandidateExclusive(record) => match &record.state {
                CandidateObjectState::Prepared { ownership } => {
                    record.state = CandidateObjectState::UploadedVerified {
                        ownership: ownership.clone(),
                    };
                }
                CandidateObjectState::UploadedVerified { .. } => {}
            },
            Self::SharedLiveSet(record) => match &record.state {
                OwnedObjectState::Prepared { ownership } => {
                    record.state = OwnedObjectState::UploadedVerified {
                        ownership: SharedObjectOwnership {
                            pending: ownership.pending.clone(),
                            activated: BTreeSet::new(),
                        },
                    };
                }
                OwnedObjectState::UploadedVerified { .. } => {}
            },
        }
        self.validate()
    }
}

pub(crate) fn remote_object_id(object: &ExactObjectRef) -> ObjectHash {
    let mut material = REMOTE_OBJECT_ID_DOMAIN.to_vec();
    material.extend(serde_json::to_vec(object).expect("ExactObjectRef serialization cannot fail"));
    ObjectHash::digest(&material)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateObjectRecord {
    pub(crate) identity: CandidateExclusiveTarget,
    pub(crate) bytes: RemoteObjectBytes,
    pub(crate) state: CandidateObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedObjectRecord {
    pub(crate) identity: SharedLiveSetObjectRef,
    pub(crate) bytes: RemoteObjectBytes,
    pub(crate) state: OwnedObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteObjectBytes {
    canonical_semantic_bytes: Vec<u8>,
    stored: RemoteStoredRepresentation,
}

impl RemoteObjectBytes {
    pub(crate) fn inline(
        canonical_semantic_bytes: Vec<u8>,
        stored_bytes: Vec<u8>,
        object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            canonical_semantic_bytes,
            stored: RemoteStoredRepresentation::Inline {
                bytes: stored_bytes,
                object,
            },
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn blob(
        canonical_semantic_bytes: Vec<u8>,
        object: ExactObjectRef,
    ) -> Result<Self, RemoteObjectRecordError> {
        let value = Self {
            canonical_semantic_bytes,
            stored: RemoteStoredRepresentation::Blob { object },
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn canonical_semantic_bytes(&self) -> &[u8] {
        &self.canonical_semantic_bytes
    }

    pub(crate) fn stored(&self) -> &RemoteStoredRepresentation {
        &self.stored
    }

    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match &self.stored {
            RemoteStoredRepresentation::Inline { bytes, object } => object
                .verify(bytes)
                .map_err(|error| RemoteObjectRecordError::StoredBytes(error.to_string())),
            RemoteStoredRepresentation::Blob { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RemoteStoredRepresentation {
    Inline {
        bytes: Vec<u8>,
        object: ExactObjectRef,
    },
    Blob {
        object: ExactObjectRef,
    },
}

impl RemoteStoredRepresentation {
    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Inline { object, .. } | Self::Blob { object } => object,
        }
    }

    pub(crate) fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline { bytes, .. } => Some(bytes),
            Self::Blob { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CandidateObjectState {
    Prepared {
        ownership: PendingCandidateOwnership,
    },
    UploadedVerified {
        ownership: PendingCandidateOwnership,
    },
}

impl CandidateObjectState {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::Prepared { ownership } | Self::UploadedVerified { ownership } => {
                ownership.validate()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OwnedObjectState {
    Prepared {
        ownership: PendingCandidateOwnership,
    },
    UploadedVerified {
        ownership: SharedObjectOwnership,
    },
}

impl OwnedObjectState {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        match self {
            Self::Prepared { ownership } => ownership.validate(),
            Self::UploadedVerified { ownership } => ownership.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingCandidateOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
}

impl PendingCandidateOwnership {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() {
            return Err(RemoteObjectRecordError::EmptyPendingOwnership);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedObjectOwnership {
    pub(crate) pending: BTreeSet<StoreBatchCommitRef>,
    pub(crate) activated: BTreeSet<SharedObjectOwner>,
}

impl SharedObjectOwnership {
    fn validate(&self) -> Result<(), RemoteObjectRecordError> {
        if self.pending.is_empty() && self.activated.is_empty() {
            Err(RemoteObjectRecordError::EmptyOwnership)
        } else {
            let activated_commits = self.activated.iter().filter_map(|owner| match owner {
                SharedObjectOwner::StoreCommit(commit) => Some(commit),
                SharedObjectOwner::Snapshot(_) => None,
            });
            if activated_commits
                .into_iter()
                .any(|commit| self.pending.contains(commit))
            {
                Err(RemoteObjectRecordError::OverlappingOwnership)
            } else {
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SharedObjectOwner {
    StoreCommit(StoreBatchCommitRef),
    Snapshot(SnapshotObjectOwner),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotObjectOwner {
    pub(crate) activation: StreamActivationId,
    pub(crate) sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateExclusiveTarget {
    pub(crate) family: CandidateFamilyId,
    pub(crate) domain: CandidateExclusiveObjectDomain,
    pub(crate) semantic_hash: ObjectHash,
    pub(crate) object: ExactObjectRef,
}

impl CandidateExclusiveTarget {
    fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
        validate_semantic_hash(self.semantic_hash, bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CandidateExclusiveObjectDomain {
    StorePackage,
    CirclePackage { circle_id: CircleId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SharedLiveSetObjectRef {
    pub(crate) domain: SharedLiveSetObjectDomain,
    pub(crate) semantic_hash: ObjectHash,
    pub(crate) object: ExactObjectRef,
}

impl SharedLiveSetObjectRef {
    fn validate_semantic(&self, bytes: &[u8]) -> Result<(), RemoteObjectRecordError> {
        validate_semantic_hash(self.semantic_hash, bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SharedLiveSetObjectDomain {
    StoredBlob,
    StorePackage,
    CirclePackage,
}

fn validate_semantic_hash(
    expected: ObjectHash,
    bytes: &[u8],
) -> Result<(), RemoteObjectRecordError> {
    let actual = ObjectHash::digest(bytes);
    if actual != expected {
        return Err(RemoteObjectRecordError::SemanticHashMismatch { expected, actual });
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RemoteObjectRecordError {
    #[error("prepared stored bytes do not match their exact reference: {0}")]
    StoredBytes(String),
    #[error("prepared stored reference differs from the closed identity reference")]
    StoredReferenceMismatch,
    #[error("prepared semantic hash is {actual}, expected {expected}")]
    SemanticHashMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("pending candidate ownership has no pending candidate")]
    EmptyPendingOwnership,
    #[error("candidate ownership sets overlap")]
    OverlappingOwnership,
    #[error("candidate ownership has no pending or activated owner")]
    EmptyOwnership,
    #[error("prepared canonical bytes do not parse as their claimed domain: {0}")]
    InvalidDomain(String),
    #[error("prepared canonical bytes disagree with their claimed domain")]
    DomainMismatch,
    #[error("remote object is not uploaded for the exact activating commit")]
    InvalidActivation,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::locator::{BlobLocator, RemoteAudience};
    use crate::storage::cloud::ObjectSlot;
    use crate::{BlobScope, KeyFingerprint};

    #[test]
    fn stored_blob_record_rejects_object_outside_locator_semantic_slot() {
        let uploader_bytes = b"uploader registration";
        let uploader = super::super::store_commit::StoreDeviceRegistrationRef {
            device_id: "11".repeat(32).parse().expect("valid test device id"),
            registration_hash: ObjectHash::digest(b"uploader registration semantic bytes"),
            object: ExactObjectRef::new(
                ObjectSlot::logical("store-v1/registrations/uploader.json".to_string())
                    .expect("valid uploader registration slot"),
                uploader_bytes.len() as u64,
                ObjectHash::digest(uploader_bytes),
            ),
        };
        let locator = BlobLocator::opaque(
            "covers",
            "cover-a",
            uploader,
            RemoteAudience::Store,
            BlobScope::Master,
            KeyFingerprint::from_bytes([4; 8]),
            7,
            ObjectHash::digest(b"cover-a"),
        )
        .expect("valid locator");
        let canonical_semantic_bytes = locator.to_bytes();
        let stored_bytes = b"stored cover".to_vec();
        let object = ExactObjectRef::new(
            ObjectSlot::logical("covers/opaque/wrong-slot".to_string()).expect("valid slot"),
            stored_bytes.len() as u64,
            ObjectHash::digest(&stored_bytes),
        );
        let owner = StoreBatchCommitRef {
            coord: super::super::store_commit::StoreCommitCoord::Serial { sequence: 1 },
            commit_hash: ObjectHash::digest(b"commit semantic bytes"),
            object: ExactObjectRef::new(
                ObjectSlot::logical(format!(
                    "{}.json",
                    super::super::store_commit::commit_semantic_prefix(
                        super::super::store_commit::CandidateFamilyId::from_hash(
                            ObjectHash::digest(b"remote object test candidate family"),
                        ),
                        super::super::store_commit::SERIAL_STREAM_ID,
                        1,
                        ObjectHash::digest(b"commit semantic bytes"),
                    )
                ))
                .expect("valid slot"),
                1,
                ObjectHash::digest(b"commit"),
            ),
        };
        let record = RemoteObjectRecord::SharedLiveSet(SharedObjectRecord {
            identity: SharedLiveSetObjectRef {
                domain: SharedLiveSetObjectDomain::StoredBlob,
                semantic_hash: ObjectHash::digest(&canonical_semantic_bytes),
                object: object.clone(),
            },
            bytes: RemoteObjectBytes::inline(canonical_semantic_bytes, stored_bytes, object)
                .expect("valid stored bytes"),
            state: OwnedObjectState::UploadedVerified {
                ownership: SharedObjectOwnership {
                    pending: BTreeSet::new(),
                    activated: BTreeSet::from([SharedObjectOwner::StoreCommit(owner)]),
                },
            },
        });

        assert!(matches!(
            record.validate(),
            Err(RemoteObjectRecordError::InvalidDomain(_))
        ));
    }
}
