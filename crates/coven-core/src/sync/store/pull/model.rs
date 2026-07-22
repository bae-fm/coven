use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStorePositionReason {
    MissingCommit,
    MissingPackage,
    MissingDeviceRegistration {
        device_id: String,
        revision: u64,
        registration_hash: ObjectHash,
    },
    MissingPredecessor(StoreBatchCommitRef),
    MissingDependency {
        device_id: String,
        commit: StoreBatchCommitRef,
    },
    NewerSchema {
        local: u32,
        required: u32,
    },
    Unauthorized,
    DeviceExclusionFreeze {
        proposal: super::store_commit::StoreDeviceExclusionProposalRef,
        target_cut: StoreHistoryCut,
    },
    InactiveDevice {
        terminals: Vec<super::store_commit::StoreDeviceExclusionRef>,
        accepted_cut: StoreHistoryCut,
    },
    InvalidChangeset(String),
    InvalidRowIdentity {
        table: String,
        reason: String,
    },
    BlobDownloadFailed,
    ForeignKeyDependency,
    ConstraintConflict(Vec<String>),
    HashMismatch {
        referenced_device_id: String,
        referenced_commit: StoreBatchCommitRef,
        materialized_hash: ObjectHash,
    },
    InvalidSignature,
    WrongSlot(String),
    ObjectCollision(String),
    ObjectUnreadable {
        key: String,
        detail: String,
    },
    InvalidObject(String),
}

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

#[derive(Debug)]
pub struct StorePullResult {
    pub changesets_applied: u64,
    pub devices_pulled: u64,
    pub held_positions: Vec<HeldStorePosition>,
    pub visible_heads: Vec<VerifiedStoreDeviceHead>,
    pub row_changes: Vec<RowChange>,
    pub asset_downloads_failed: bool,
    pub local_blob_cleanup_pending: bool,
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
    Database(String),
    #[error("active Store device {device_id} for member {member:?} has no activated acknowledgement for the selected snapshot")]
    SnapshotNotStable { member: String, device_id: String },
    #[error("Store snapshot author is inactive in its exact covered device state")]
    SnapshotAuthorInactive,
    #[error("Store snapshot author is not an Owner in its exact membership state")]
    SnapshotAuthorNotOwner,
    #[error("membership: {0}")]
    Membership(#[source] StorePullMembershipError),
    #[error("{0}")]
    BlobDownloads(#[source] crate::sync::pull::BlobDownloadFailures),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullMembershipError {
    #[error("{0}")]
    Object(#[source] StoreObjectError),
    #[error("{0}")]
    Chain(#[source] super::membership_ops::AnchoredChainError),
    #[error("{0}")]
    Message(String),
}

pub(crate) type StorePullFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, StorePullError>> + Send + 'a>>;

impl From<DbError> for StorePullError {
    fn from(error: DbError) -> Self {
        Self::Database(error.into_message())
    }
}

#[derive(Clone)]
pub(crate) struct Candidate {
    pub(crate) commit_ref: StoreBatchCommitRef,
    pub(crate) commit: StoreBatchCommit,
    pub(crate) author: StoreDeviceRegistration,
    pub(crate) package: Option<Vec<u8>>,
    pub(crate) registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
}

#[derive(Clone)]
pub(crate) struct LoadedCirclePackage {
    pub(crate) reference: CirclePackageRef,
    pub(crate) bytes: Vec<u8>,
    pub(crate) blob_protection: BlobSpoolProtection,
}

#[derive(Clone)]
pub(crate) struct CirclePackageAccess {
    pub(crate) encryption: EncryptionService,
    pub(crate) key_fingerprint: KeyFingerprint,
    pub(crate) writers: BTreeSet<String>,
}

pub(crate) type CirclePackageAccesses =
    BTreeMap<(super::circle::CircleId, super::circle::CircleControlCoord), CirclePackageAccess>;

pub(crate) fn parse_candidate_store_package(
    candidate: &Candidate,
    bytes: &[u8],
) -> Result<AudiencePackage, String> {
    let package = AudiencePackage::parse(bytes)
        .map_err(|error| format!("invalid Store audience package: {error}"))?;
    if !matches!(package.audience(), PackageAudience::Store)
        || package.store_root_hash() != candidate.commit.store_root_hash
        || package.write_id() != &candidate.commit.write_id
        || package.commit_coord() != &candidate.commit_ref.coord
        || package.candidate_family() != candidate.commit.candidate_family()
        || candidate
            .commit
            .store_package()
            .as_ref()
            .is_none_or(|reference| package.schema_version() != reference.schema_version)
    {
        return Err("Store audience package differs from its exact commit".to_string());
    }
    Ok(package)
}

pub(crate) fn parse_candidate_circle_package(
    candidate: &Candidate,
    loaded: &LoadedCirclePackage,
) -> Result<AudiencePackage, String> {
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
    ) || package.store_root_hash() != candidate.commit.store_root_hash
        || package.write_id() != &candidate.commit.write_id
        || package.commit_coord() != &candidate.commit_ref.coord
        || package.candidate_family() != candidate.commit.candidate_family()
        || package.schema_version() != expected.package.schema_version
    {
        return Err("Circle audience package differs from its exact commit".to_string());
    }
    package
        .validate_blob_uploader(&candidate.commit.author_registration)
        .map_err(|error| format!("invalid Circle blob authority: {error}"))?;
    Ok(package)
}

pub(crate) fn held_commit(
    reference: &StoreBatchCommitRef,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Commit {
            device_id: commit_stream_id(&reference.coord),
            commit: reference.clone(),
        },
        reason,
    }
}

pub(crate) fn held_package(
    reference: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    let package = commit
        .store_package()
        .expect("held Store package is named by the commit");
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Package {
            device_id: commit_stream_id(&reference.coord),
            seq: commit.seq(),
            package_hash: package.content_hash,
        },
        reason,
    }
}

pub(crate) fn held_dependency(
    dependent: &StoreBatchCommitRef,
    required_device_id: &str,
    required: &StoreBatchCommitRef,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Dependency {
            dependent_device_id: commit_stream_id(&dependent.coord),
            dependent_commit: dependent.clone(),
            required_device_id: required_device_id.to_string(),
            required_commit: required.clone(),
        },
        reason,
    }
}

pub(crate) fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    coord.stream_id.to_string()
}
