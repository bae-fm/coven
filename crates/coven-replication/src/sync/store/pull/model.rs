use super::*;

#[derive(Debug, Clone)]
pub enum HeldStorePositionReason {
    MissingCommit,
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
        proposal: coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        target_cut: StoreHistoryCut,
    },
    InactiveDevice {
        terminals: Vec<coven_protocol::store_commit::StoreDeviceExclusionRef>,
        accepted_cut: StoreHistoryCut,
    },
    InvalidChangeset(String),
    InvalidChangesetIdentity(std::sync::Arc<coven_database::ChangesetIdentityError>),
    InvalidChangesetDatabase(std::sync::Arc<DbError>),
    InvalidChangesetBlobDecl(std::sync::Arc<coven_database::BlobDeclError>),
    InvalidStorePackage(std::sync::Arc<coven_protocol::audience_package::AudiencePackageError>),
    StorePackageMismatch,
    InvalidCirclePackage(std::sync::Arc<coven_protocol::audience_package::AudiencePackageError>),
    CirclePackageMismatch,
    InvalidCircleBlobAuthority(
        std::sync::Arc<coven_protocol::audience_package::AudiencePackageError>,
    ),
    CirclePackageRead(std::sync::Arc<crate::sync::store::CirclePackageReadError>),
    ChangesetUnreadable(std::sync::Arc<coven_database::ChangesetError>),
    InvalidRowIdentity(std::sync::Arc<coven_protocol::synced_schema::RowIdentityError>),
    ForeignKeyDependency,
    ConstraintConflict(Vec<String>),
    PrivateSharedConflict {
        table: String,
        row_id: String,
        commit: StoreBatchCommitRef,
    },
    InvalidLocalCircleContext {
        circle_id: coven_protocol::circle::CircleId,
    },
    HashMismatch {
        referenced_device_id: String,
        referenced_commit: StoreBatchCommitRef,
        materialized_hash: ObjectHash,
    },
    InvalidSignature,
    WrongSlot(String),
    WrongSlotProtocol(std::sync::Arc<StoreProtocolError>),
    ObjectUnreadableStorage {
        key: String,
        source: std::sync::Arc<StorageError>,
    },
    ObjectUnreadableProtocol {
        key: String,
        source: std::sync::Arc<StoreProtocolError>,
    },
    ObjectUnreadablePull {
        key: String,
        source: std::sync::Arc<StorePullError>,
    },
    InvalidObject(String),
    InvalidObjectJson(std::sync::Arc<serde_json::Error>),
    InvalidObjectProtocol(std::sync::Arc<StoreProtocolError>),
    InvalidObjectPull(std::sync::Arc<StorePullError>),
}

impl PartialEq for HeldStorePositionReason {
    fn eq(&self, other: &Self) -> bool {
        use HeldStorePositionReason as Reason;
        match (self, other) {
            (Reason::MissingCommit, Reason::MissingCommit)
            | (Reason::Unauthorized, Reason::Unauthorized)
            | (Reason::StorePackageMismatch, Reason::StorePackageMismatch)
            | (Reason::CirclePackageMismatch, Reason::CirclePackageMismatch)
            | (Reason::ForeignKeyDependency, Reason::ForeignKeyDependency)
            | (Reason::InvalidSignature, Reason::InvalidSignature) => true,
            (
                Reason::InvalidLocalCircleContext { circle_id: left },
                Reason::InvalidLocalCircleContext { circle_id: right },
            ) => left == right,
            (Reason::MissingPredecessor(left), Reason::MissingPredecessor(right)) => left == right,
            (
                Reason::MissingDependency {
                    device_id: ld,
                    commit: lc,
                },
                Reason::MissingDependency {
                    device_id: rd,
                    commit: rc,
                },
            ) => ld == rd && lc == rc,
            (
                Reason::NewerSchema {
                    local: ll,
                    required: lr,
                },
                Reason::NewerSchema {
                    local: rl,
                    required: rr,
                },
            ) => ll == rl && lr == rr,
            (
                Reason::DeviceExclusionFreeze {
                    proposal: lp,
                    target_cut: lc,
                },
                Reason::DeviceExclusionFreeze {
                    proposal: rp,
                    target_cut: rc,
                },
            ) => lp == rp && lc == rc,
            (
                Reason::InactiveDevice {
                    terminals: lt,
                    accepted_cut: lc,
                },
                Reason::InactiveDevice {
                    terminals: rt,
                    accepted_cut: rc,
                },
            ) => lt == rt && lc == rc,
            (Reason::InvalidChangeset(left), Reason::InvalidChangeset(right))
            | (Reason::WrongSlot(left), Reason::WrongSlot(right))
            | (Reason::InvalidObject(left), Reason::InvalidObject(right)) => left == right,
            (Reason::InvalidStorePackage(left), Reason::InvalidStorePackage(right))
            | (Reason::InvalidCirclePackage(left), Reason::InvalidCirclePackage(right))
            | (
                Reason::InvalidCircleBlobAuthority(left),
                Reason::InvalidCircleBlobAuthority(right),
            ) => left.to_string() == right.to_string(),
            (Reason::CirclePackageRead(left), Reason::CirclePackageRead(right)) => {
                left.to_string() == right.to_string()
            }
            (Reason::InvalidRowIdentity(left), Reason::InvalidRowIdentity(right)) => left == right,
            (Reason::ConstraintConflict(left), Reason::ConstraintConflict(right)) => left == right,
            (
                Reason::PrivateSharedConflict {
                    table: lt,
                    row_id: lr,
                    commit: lc,
                },
                Reason::PrivateSharedConflict {
                    table: rt,
                    row_id: rr,
                    commit: rc,
                },
            ) => lt == rt && lr == rr && lc == rc,
            (
                Reason::HashMismatch {
                    referenced_device_id: ld,
                    referenced_commit: lc,
                    materialized_hash: lh,
                },
                Reason::HashMismatch {
                    referenced_device_id: rd,
                    referenced_commit: rc,
                    materialized_hash: rh,
                },
            ) => ld == rd && lc == rc && lh == rh,
            (
                Reason::ObjectUnreadableStorage {
                    key: lk,
                    source: ls,
                },
                Reason::ObjectUnreadableStorage {
                    key: rk,
                    source: rs,
                },
            ) => lk == rk && ls.to_string() == rs.to_string(),
            (
                Reason::ObjectUnreadableProtocol {
                    key: lk,
                    source: ls,
                },
                Reason::ObjectUnreadableProtocol {
                    key: rk,
                    source: rs,
                },
            ) => lk == rk && ls.to_string() == rs.to_string(),
            (
                Reason::ObjectUnreadablePull {
                    key: lk,
                    source: ls,
                },
                Reason::ObjectUnreadablePull {
                    key: rk,
                    source: rs,
                },
            ) => lk == rk && ls.to_string() == rs.to_string(),
            (Reason::InvalidChangesetIdentity(left), Reason::InvalidChangesetIdentity(right)) => {
                left.to_string() == right.to_string()
            }
            (Reason::InvalidChangesetDatabase(left), Reason::InvalidChangesetDatabase(right)) => {
                left.to_string() == right.to_string()
            }
            (Reason::InvalidChangesetBlobDecl(left), Reason::InvalidChangesetBlobDecl(right)) => {
                left.to_string() == right.to_string()
            }
            (Reason::ChangesetUnreadable(left), Reason::ChangesetUnreadable(right)) => {
                left.to_string() == right.to_string()
            }
            (Reason::InvalidObjectJson(left), Reason::InvalidObjectJson(right)) => {
                left.to_string() == right.to_string()
            }
            (Reason::InvalidObjectPull(left), Reason::InvalidObjectPull(right)) => {
                left.to_string() == right.to_string()
            }
            (Reason::WrongSlotProtocol(left), Reason::WrongSlotProtocol(right))
            | (Reason::InvalidObjectProtocol(left), Reason::InvalidObjectProtocol(right)) => {
                left.to_string() == right.to_string()
            }
            _ => false,
        }
    }
}

impl Eq for HeldStorePositionReason {}

pub(crate) type ApplyOutcome = coven_protocol::membership::ApplyOutcome<HeldStorePositionReason>;

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
    #[error("membership protocol: {0}")]
    MembershipProtocol(#[from] coven_protocol::membership::MembershipError),
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
    /// The snapshot's coverage does not reach this device's installed replay
    /// baseline, so the device stands past it. Verifying a snapshot means
    /// recomposing its history summary, and the history behind this one was
    /// retired when the baseline moved over it — there is nothing left to
    /// recompose from and nothing to gain: whatever this snapshot restates, the
    /// baseline already restates at least as much.
    #[error("Store snapshot is behind this device's installed replay baseline")]
    SnapshotBehindReplayBaseline,
    #[error("current membership is not named by accepted Store history")]
    ReplayRetirementMembershipUnwitnessed,
    #[error("membership: {0}")]
    Membership(#[source] StorePullMembershipError),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("Circle package: {0}")]
    CirclePackage(#[source] Box<crate::sync::store::CirclePackageReadError>),
}

impl From<crate::sync::store::CirclePackageReadError> for StorePullError {
    fn from(error: crate::sync::store::CirclePackageReadError) -> Self {
        Self::CirclePackage(Box::new(error))
    }
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

    pub(crate) fn parse_store_package(
        &self,
        bytes: &[u8],
    ) -> Result<AudiencePackage, HeldStorePositionReason> {
        let commit = self.commit();
        let package = AudiencePackage::parse(bytes)
            .map_err(|error| HeldStorePositionReason::InvalidStorePackage(error.into()))?;
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
            return Err(HeldStorePositionReason::StorePackageMismatch);
        }
        Ok(package)
    }

    pub(crate) fn parse_circle_package(
        &self,
        loaded: &LoadedCirclePackage,
    ) -> Result<AudiencePackage, HeldStorePositionReason> {
        let commit = self.commit();
        let package = AudiencePackage::parse(&loaded.bytes)
            .map_err(|error| HeldStorePositionReason::InvalidCirclePackage(error.into()))?;
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
            return Err(HeldStorePositionReason::CirclePackageMismatch);
        }
        package
            .validate_blob_uploader(&commit.author_registration)
            .map_err(|error| HeldStorePositionReason::InvalidCircleBlobAuthority(error.into()))?;
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
