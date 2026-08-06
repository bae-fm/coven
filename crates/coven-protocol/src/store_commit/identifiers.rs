use super::device_state::merge_history_cuts;
use super::*;

pub use coven_foundation::object_hash::ObjectHash;

/// Closed coordinate of one Store commit in its author stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCommitCoord {
    pub stream_id: AuthorStreamId,
    pub sequence: u64,
}

impl StoreCommitCoord {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn validate(&self) -> Result<(), StoreProtocolError> {
        if self.sequence() == 0 {
            return Err(StoreProtocolError::Malformed(
                "Store commit coordinate uses sequence zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Domain-separated family shared by replacements at one competition point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CandidateFamilyId(ObjectHash);

impl CandidateFamilyId {
    pub fn from_hash(hash: ObjectHash) -> Self {
        Self(hash)
    }

    pub fn as_hash(self) -> ObjectHash {
        self.0
    }

    pub fn derive(
        store_root_hash: ObjectHash,
        author_registration: &StoreDeviceRegistrationRef,
        write_id: &WriteId,
        order: &StoreCommitOrder,
    ) -> Self {
        #[derive(Serialize)]
        struct Fields<'a> {
            store_root_hash: ObjectHash,
            author_registration: &'a StoreDeviceRegistrationRef,
            write_id: &'a WriteId,
            sequence: u64,
            predecessor: Option<&'a StoreBatchCommitRef>,
        }
        let fields = Fields {
            store_root_hash,
            author_registration,
            write_id,
            sequence: order.seq(),
            predecessor: order.predecessor.as_ref(),
        };
        Self(ObjectHash::digest(&domain_json(
            CANDIDATE_FAMILY_DOMAIN,
            &fields,
        )))
    }
}

/// Exact identity of one signed Store commit candidate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommitRef {
    pub coord: StoreCommitCoord,
    pub commit_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// Exact stored candidate commit retained as cleanup authority after abandonment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBatchCommitDeletionTarget {
    pub coord: StoreCommitCoord,
    pub object: ExactObjectRef,
    pub canonical_signed_bytes: Vec<u8>,
}

impl StoreBatchCommitDeletionTarget {
    pub(crate) fn verify_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<VerifiedStoreBatchCommit, StoreProtocolError> {
        let commit = self.verify_exact_candidate(expected_store_root_hash, author)?;
        if matches!(&commit.body, StoreCommitBody::AbandonCandidates { .. }) {
            return Err(StoreProtocolError::Malformed(
                "retained authority cannot be a candidate cleanup target".to_string(),
            ));
        }
        Ok(commit)
    }

    pub fn verify_nonactivation_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<VerifiedStoreBatchCommit, StoreProtocolError> {
        self.verify_exact_candidate(expected_store_root_hash, author)
    }

    fn verify_exact_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<VerifiedStoreBatchCommit, StoreProtocolError> {
        self.object
            .verify(&self.canonical_signed_bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let commit = VerifiedStoreBatchCommit::parse_prepared(
            &self.canonical_signed_bytes,
            expected_store_root_hash,
            self.coord.clone(),
            self.object.clone(),
            author,
        )?;
        if commit.to_bytes() != self.canonical_signed_bytes {
            return Err(StoreProtocolError::Malformed(
                "candidate commit bytes are not canonical".to_string(),
            ));
        }
        Ok(commit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateCleanupManifest {
    pub candidate: StoreBatchCommitDeletionTarget,
}

impl StoreBatchCommitRef {
    pub fn from_commit(
        commit: &StoreBatchCommit,
        coord: StoreCommitCoord,
        object: ExactObjectRef,
    ) -> Result<Self, StoreProtocolError> {
        if coord.sequence() != commit.seq() {
            return Err(StoreProtocolError::Malformed(
                "Store commit reference coordinate differs from the signed commit".to_string(),
            ));
        }
        let reference = Self {
            coord,
            commit_hash: commit.commit_hash(),
            object,
        };
        reference.verify_commit(commit)?;
        Ok(reference)
    }

    pub fn verify_commit(&self, commit: &StoreBatchCommit) -> Result<(), StoreProtocolError> {
        if self.coord.sequence() != commit.seq() || self.commit_hash != commit.commit_hash() {
            return Err(StoreProtocolError::Malformed(
                "exact Store commit reference differs from the signed commit".to_string(),
            ));
        }
        let stream_id = commit_stream_id(&self.coord);
        let expected = format!(
            "{}.json",
            commit_semantic_prefix(
                commit.candidate_family(),
                &stream_id,
                self.coord.sequence(),
                self.commit_hash,
            )
        );
        if self.object.slot().logical_key() != expected {
            return Err(StoreProtocolError::RelocatedSlot {
                expected,
                actual: self.object.slot().logical_key().to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreRootRef {
    pub store_root_id: ObjectHash,
    pub store_root_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamActivationId(ObjectHash);

impl StreamActivationId {
    pub fn from_digest(hash: ObjectHash) -> Self {
        Self(hash)
    }

    pub fn as_hash(self) -> ObjectHash {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamActivation {
    GrantAuthorized {
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        grant_id: MembershipGrantId,
        anchor: GrantStreamAnchor,
    },
    DeviceAuthorized {
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        anchor: DeviceStreamAnchor,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredStreamActivation {
    activation: StreamActivation,
    activating_commit: StoreBatchCommitRef,
}

impl RegisteredStreamActivation {
    pub fn from_stored(
        stored_activation_id: StreamActivationId,
        stored_author_stream_id: AuthorStreamId,
        activation: StreamActivation,
        activating_commit: StoreBatchCommitRef,
    ) -> Result<Self, StoreProtocolError> {
        if activation.activation_id() != stored_activation_id {
            return Err(StoreProtocolError::Malformed(
                "stored stream activation id differs from its canonical descriptor".to_string(),
            ));
        }
        if activation.author_stream_id() != stored_author_stream_id {
            return Err(StoreProtocolError::Malformed(
                "stored author stream id differs from its canonical descriptor".to_string(),
            ));
        }
        Ok(Self {
            activation,
            activating_commit,
        })
    }

    pub fn activation(&self) -> &StreamActivation {
        &self.activation
    }

    pub fn activating_commit(&self) -> &StoreBatchCommitRef {
        &self.activating_commit
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamAnchorDomain {
    StoreMembership,
    OwnerRecovery,
    CircleControl { circle_id: CircleId },
    CircleRoster { circle_id: CircleId },
    CircleMetadata { circle_id: CircleId },
    CircleAcknowledgements { circle_id: CircleId },
    CircleSnapshots { circle_id: CircleId },
    StoreAnnouncements,
    StoreAcknowledgements,
    StoreSnapshots,
}

impl GrantStreamAnchor {
    fn domain(&self) -> StreamAnchorDomain {
        match self {
            Self::StoreMembership { .. } => StreamAnchorDomain::StoreMembership,
            Self::OwnerRecovery { .. } => StreamAnchorDomain::OwnerRecovery,
            Self::CircleControl { circle_id, .. } => StreamAnchorDomain::CircleControl {
                circle_id: *circle_id,
            },
            Self::CircleRoster { circle_id, .. } => StreamAnchorDomain::CircleRoster {
                circle_id: *circle_id,
            },
            Self::CircleMetadata { circle_id, .. } => StreamAnchorDomain::CircleMetadata {
                circle_id: *circle_id,
            },
        }
    }
}

impl DeviceStreamAnchor {
    fn domain(&self) -> StreamAnchorDomain {
        match self {
            Self::StoreAnnouncements { .. } => StreamAnchorDomain::StoreAnnouncements,
            Self::StoreAcknowledgements { .. } => StreamAnchorDomain::StoreAcknowledgements,
            Self::StoreSnapshots { .. } => StreamAnchorDomain::StoreSnapshots,
            Self::CircleAcknowledgements { circle_id, .. } => {
                StreamAnchorDomain::CircleAcknowledgements {
                    circle_id: *circle_id,
                }
            }
            Self::CircleSnapshots { circle_id, .. } => StreamAnchorDomain::CircleSnapshots {
                circle_id: *circle_id,
            },
        }
    }
}

impl StreamActivation {
    pub fn grant_authorized(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        grant_id: MembershipGrantId,
        anchor: GrantStreamAnchor,
    ) -> Self {
        Self::GrantAuthorized {
            store_root_hash,
            author_registration,
            grant_id,
            anchor,
        }
    }

    pub fn device_authorized(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        anchor: DeviceStreamAnchor,
    ) -> Self {
        Self::DeviceAuthorized {
            store_root_hash,
            author_registration,
            anchor,
        }
    }

    pub fn activation_id(&self) -> StreamActivationId {
        StreamActivationId(ObjectHash::digest(&domain_json(
            STREAM_ACTIVATION_ID_DOMAIN,
            self,
        )))
    }

    pub fn author_stream_id(&self) -> AuthorStreamId {
        match self {
            Self::GrantAuthorized {
                store_root_hash,
                author_registration,
                grant_id,
                anchor,
            } => derive_grant_author_stream_id(
                *store_root_hash,
                author_registration,
                grant_id,
                anchor.domain(),
            ),
            Self::DeviceAuthorized {
                store_root_hash,
                author_registration,
                anchor,
            } => derive_device_author_stream_id(
                *store_root_hash,
                author_registration,
                anchor.domain(),
            ),
        }
    }

    pub fn device_authorized_stream_id(
        store_root_hash: ObjectHash,
        author_registration: &StoreDeviceRegistrationRef,
        domain: StreamAnchorDomain,
    ) -> AuthorStreamId {
        derive_device_author_stream_id(store_root_hash, author_registration, domain)
    }

    pub fn grant_authorized_stream_id(
        store_root_hash: ObjectHash,
        author_registration: &StoreDeviceRegistrationRef,
        grant_id: &MembershipGrantId,
        domain: StreamAnchorDomain,
    ) -> AuthorStreamId {
        derive_grant_author_stream_id(store_root_hash, author_registration, grant_id, domain)
    }

    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::GrantAuthorized { anchor, .. } => anchor.first_slot(),
            Self::DeviceAuthorized { anchor, .. } => anchor.first_slot(),
        }
    }

    pub fn author_registration(&self) -> &StoreDeviceRegistrationRef {
        match self {
            Self::GrantAuthorized {
                author_registration,
                ..
            }
            | Self::DeviceAuthorized {
                author_registration,
                ..
            } => author_registration,
        }
    }

    pub fn store_root_hash(&self) -> ObjectHash {
        match self {
            Self::GrantAuthorized {
                store_root_hash, ..
            }
            | Self::DeviceAuthorized {
                store_root_hash, ..
            } => *store_root_hash,
        }
    }
}

#[derive(Serialize)]
struct GrantAuthorStreamFields<'a> {
    store_root_hash: ObjectHash,
    domain: StreamAnchorDomain,
    author_registration: &'a StoreDeviceRegistrationRef,
    grant_id: &'a MembershipGrantId,
}

#[derive(Serialize)]
struct DeviceAuthorStreamFields<'a> {
    store_root_hash: ObjectHash,
    domain: StreamAnchorDomain,
    author_registration: &'a StoreDeviceRegistrationRef,
}

fn derive_grant_author_stream_id(
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    grant_id: &MembershipGrantId,
    domain: StreamAnchorDomain,
) -> AuthorStreamId {
    derive_author_stream_id(&GrantAuthorStreamFields {
        store_root_hash,
        domain,
        author_registration,
        grant_id,
    })
}

fn derive_device_author_stream_id(
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    domain: StreamAnchorDomain,
) -> AuthorStreamId {
    derive_author_stream_id(&DeviceAuthorStreamFields {
        store_root_hash,
        domain,
        author_registration,
    })
}

fn derive_author_stream_id(fields: &impl Serialize) -> AuthorStreamId {
    AuthorStreamId::from_digest(ObjectHash::digest(&domain_json(
        AUTHOR_STREAM_ID_DOMAIN,
        fields,
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorLink {
    pub activation: StreamActivationId,
    pub predecessor: Option<ExactObjectRef>,
    pub next_slot: ObjectSlot,
}

/// Exact materialized cut across author streams.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CommitFrontier(pub BTreeMap<AuthorStreamId, StoreBatchCommitRef>);

/// Exact Store history cut across author streams.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreHistoryCut(pub BTreeMap<AuthorStreamId, StoreBatchCommitRef>);

impl StoreHistoryCut {
    pub fn from_commits(commits: BTreeMap<AuthorStreamId, StoreBatchCommitRef>) -> Self {
        Self(commits)
    }

    pub fn position_count(&self) -> usize {
        self.0.len()
    }

    pub fn commits(&self) -> &BTreeMap<AuthorStreamId, StoreBatchCommitRef> {
        &self.0
    }

    pub fn frontier(&self) -> CommitFrontier {
        CommitFrontier(self.0.clone())
    }

    pub fn join(self, other: Self) -> Result<Self, StoreProtocolError> {
        merge_history_cuts(self, other)
    }
}

impl CommitFrontier {
    pub fn from_refs(
        commits: BTreeMap<String, StoreBatchCommitRef>,
    ) -> Result<Self, StoreProtocolError> {
        commits
            .into_iter()
            .map(|(stream_id, commit)| {
                let stream_id = stream_id.parse().map_err(StoreProtocolError::Malformed)?;
                Ok((stream_id, commit))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(Self)
    }

    pub fn into_refs(self) -> BTreeMap<String, StoreBatchCommitRef> {
        self.0
            .into_iter()
            .map(|(stream_id, commit)| (stream_id.to_string(), commit))
            .collect()
    }

    pub fn position_count(&self) -> usize {
        self.0.len()
    }

    pub fn covers(&self, covered: &Self) -> bool {
        covered
            .0
            .iter()
            .all(|(stream, covered_ref)| self.covers_commit_on_stream(stream, covered_ref))
    }

    pub fn commits(&self) -> &BTreeMap<AuthorStreamId, StoreBatchCommitRef> {
        &self.0
    }

    pub fn covers_commit(&self, commit: &StoreBatchCommitRef) -> bool {
        self.covers_commit_on_stream(&commit.coord.stream_id, commit)
    }

    fn covers_commit_on_stream(
        &self,
        stream: &AuthorStreamId,
        covered: &StoreBatchCommitRef,
    ) -> bool {
        self.0.get(stream).is_some_and(|current| {
            current.coord.sequence() > covered.coord.sequence()
                || current.coord.sequence() == covered.coord.sequence() && current == covered
        })
    }

    pub fn join(self, other: Self) -> Result<Self, StoreProtocolError> {
        StoreHistoryCut::from_commits(self.0)
            .join(StoreHistoryCut::from_commits(other.0))
            .map(|cut| cut.frontier())
    }
}

/// Predecessor and dependency order authenticated by one Store commit.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreCommitOrder {
    pub seq: u64,
    pub predecessor: Option<StoreBatchCommitRef>,
    pub dependencies: BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
}

impl StoreCommitOrder {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn predecessor(&self) -> Option<&StoreBatchCommitRef> {
        self.predecessor.as_ref()
    }

    pub fn dependencies(&self) -> &BTreeMap<AuthorStreamId, StoreBatchCommitRef> {
        &self.dependencies
    }

    pub fn stream_id<'a>(&self, device_id: &'a str) -> &'a str {
        device_id
    }

    pub fn predecessor_cut(&self) -> Result<StoreHistoryCut, StoreProtocolError> {
        let mut cut = self.dependencies.clone();
        if let Some(predecessor) = &self.predecessor {
            if cut
                .insert(predecessor.coord.stream_id, predecessor.clone())
                .is_some_and(|existing| existing != *predecessor)
            {
                return Err(StoreProtocolError::JoinAttemptMismatch);
            }
        }
        Ok(StoreHistoryCut(cut))
    }
}

pub(super) fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    coord.stream_id.to_string()
}
