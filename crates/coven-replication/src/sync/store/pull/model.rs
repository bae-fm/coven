use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStoreCoordinate {
    Head {
        device_id: String,
        seq: u64,
        head_hash: ObjectHash,
    },
    Commit {
        device_id: String,
        commit: StoreBatchCommitRef,
    },
    Package {
        device_id: String,
        seq: u64,
        package_hash: ObjectHash,
    },
    Dependency {
        dependent_device_id: String,
        dependent_commit: StoreBatchCommitRef,
        required_device_id: String,
        required_commit: StoreBatchCommitRef,
    },
}

impl HeldStoreCoordinate {
    pub fn device_id(&self) -> &str {
        match self {
            Self::Head { device_id, .. }
            | Self::Commit { device_id, .. }
            | Self::Package { device_id, .. } => device_id,
            Self::Dependency {
                dependent_device_id,
                ..
            } => dependent_device_id,
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            Self::Head { seq, .. } | Self::Package { seq, .. } => *seq,
            Self::Commit { commit, .. } => commit.coord.sequence(),
            Self::Dependency {
                dependent_commit, ..
            } => dependent_commit.coord.sequence(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldStorePosition {
    pub coordinate: HeldStoreCoordinate,
    pub reason: HeldStorePositionReason,
}

impl HeldStorePosition {
    pub(crate) fn commit(reference: &StoreBatchCommitRef, reason: HeldStorePositionReason) -> Self {
        Self {
            coordinate: HeldStoreCoordinate::Commit {
                device_id: commit_stream_id(&reference.coord),
                commit: reference.clone(),
            },
            reason,
        }
    }

    pub(crate) fn package(
        reference: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        reason: HeldStorePositionReason,
    ) -> Self {
        let package = commit
            .store_package()
            .expect("held Store package is named by the commit");
        Self {
            coordinate: HeldStoreCoordinate::Package {
                device_id: commit_stream_id(&reference.coord),
                seq: commit.seq(),
                package_hash: package.content_hash,
            },
            reason,
        }
    }

    pub(crate) fn dependency(
        dependent: &StoreBatchCommitRef,
        required_device_id: &str,
        required: &StoreBatchCommitRef,
        reason: HeldStorePositionReason,
    ) -> Self {
        Self {
            coordinate: HeldStoreCoordinate::Dependency {
                dependent_device_id: commit_stream_id(&dependent.coord),
                dependent_commit: dependent.clone(),
                required_device_id: required_device_id.to_string(),
                required_commit: required.clone(),
            },
            reason,
        }
    }
}

#[derive(Debug)]
pub struct StorePullResult {
    pub changesets_applied: u64,
    pub held_positions: Vec<HeldStorePosition>,
    pub visible_heads: Vec<VerifiedStoreDeviceHead>,
    pub row_changes: Vec<RowChange>,
    pub asset_downloads_failed: bool,
    pub local_blob_cleanup_pending: bool,
    #[cfg(any(test, feature = "test-utils"))]
    pub frontier: BTreeMap<String, StoreBatchCommitRef>,
}

#[derive(Debug, Clone)]
pub struct VerifiedStoreDeviceHead {
    pub head: StoreDeviceHead,
    pub author: StoreDeviceRegistration,
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullError {
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("database: {0}")]
    Database(#[from] DbError),
    #[error("Store protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("Store protocol root: {0}")]
    ProtocolRoot(#[from] crate::sync::store::protocol_root::StoreProtocolRootError),
    #[error("remote object record: {0}")]
    RemoteObject(#[from] coven_protocol::remote_object::RemoteObjectRecordError),
    #[error("membership chain: {0}")]
    MembershipChain(#[from] crate::sync::store::membership::AnchoredChainError),
    #[error("device join exchange: {0}")]
    DeviceJoinExchange(
        #[from] coven_protocol::store_commit::device_join_exchange::DeviceJoinExchangeError,
    ),
    #[error("Store operation: {0}")]
    Store(#[source] Box<crate::sync::store::StoreError>),
    #[error("serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("row routing key: {0}")]
    RowRoutingKey(#[from] coven_protocol::circle::RowRoutingKeyError),
    /// Pulled Store evidence contradicts itself — a commit outside its own
    /// verified history, a reference that differs from the state it names, a
    /// precondition the pull requires. Invariant text with no source error:
    /// nothing underneath failed, the evidence is inconsistent.
    #[error("Store pull state is invalid: {0}")]
    InvalidState(String),
    /// A [`StorePullError`] with the operation that produced it named in front
    /// of it, the same shape [`DbError::context`] gives database failures.
    #[error("{context}: {source}")]
    Context {
        context: String,
        source: Box<StorePullError>,
    },
    #[error("active Store device {device_id} for member {member:?} has no activated acknowledgement for the selected snapshot")]
    SnapshotNotStable { member: String, device_id: String },
    #[error("Store snapshot author is inactive in its exact covered device state")]
    SnapshotAuthorInactive,
    #[error("Store snapshot author is not an Owner in its exact membership state")]
    SnapshotAuthorNotOwner,
    #[error("membership: {0}")]
    Membership(#[source] StorePullMembershipError),
    #[error("{0}")]
    BlobDownloads(#[source] crate::sync::store::pull::BlobDownloadFailures),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

impl StorePullError {
    /// Name the operation `source` failed in without flattening it.
    pub(crate) fn context(
        context: impl Into<String>,
        source: impl Into<StorePullError>,
    ) -> StorePullError {
        StorePullError::Context {
            context: context.into(),
            source: Box::new(source.into()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullMembershipError {
    #[error("{0}")]
    State(#[source] coven_protocol::membership::MembershipError),
    #[error("{0}")]
    Message(String),
}

#[derive(Clone)]
pub(crate) struct Candidate {
    pub(crate) verified: VerifiedStoreBatchCommit,
    pub(crate) package: Option<Vec<u8>>,
    pub(crate) registrations: Vec<ActivatedStoreDeviceRegistration>,
}

impl Candidate {
    pub(crate) fn commit_ref(&self) -> &StoreBatchCommitRef {
        self.verified.reference()
    }

    pub(crate) fn commit(&self) -> &StoreBatchCommit {
        self.verified.value()
    }

    pub(crate) fn author(&self) -> &StoreDeviceRegistration {
        self.verified.author()
    }

    pub(crate) fn parse_store_package(&self, bytes: &[u8]) -> Result<AudiencePackage, String> {
        let commit = self.commit();
        let package = AudiencePackage::parse(bytes)
            .map_err(|error| format!("invalid Store audience package: {error}"))?;
        if !matches!(package.audience(), PackageAudience::Store)
            || package.store_root_hash() != commit.store_root_hash
            || package.write_id() != &commit.write_id
            || package.commit_coord() != &self.commit_ref().coord
            || package.candidate_family() != commit.candidate_family()
            || commit
                .store_package()
                .as_ref()
                .is_none_or(|reference| package.schema_version() != reference.schema_version)
        {
            return Err("Store audience package differs from its exact commit".to_string());
        }
        Ok(package)
    }

    pub(crate) fn parse_circle_package(
        &self,
        loaded: &LoadedCirclePackage,
    ) -> Result<AudiencePackage, String> {
        let commit = self.commit();
        let package = AudiencePackage::parse(&loaded.bytes)
            .map_err(|error| format!("invalid Circle audience package: {error}"))?;
        let expected = &loaded.reference;
        if !matches!(
            package.audience(),
            PackageAudience::Circle {
                circle_id,
                control,
                key_fingerprint,
            } if *circle_id == expected.circle_id
                && control == &expected.control
                && *key_fingerprint == expected.key_fingerprint
        ) || package.store_root_hash() != commit.store_root_hash
            || package.write_id() != &commit.write_id
            || package.commit_coord() != &self.commit_ref().coord
            || package.candidate_family() != commit.candidate_family()
            || package.schema_version() != expected.package.schema_version
        {
            return Err("Circle audience package differs from its exact commit".to_string());
        }
        package
            .validate_blob_uploader(&commit.author_registration)
            .map_err(|error| format!("invalid Circle blob authority: {error}"))?;
        Ok(package)
    }
}

#[derive(Clone)]
pub struct LoadedCirclePackage {
    pub(crate) reference: CirclePackageRef,
    pub(crate) bytes: Vec<u8>,
}

pub(crate) fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    coord.stream_id.to_string()
}
