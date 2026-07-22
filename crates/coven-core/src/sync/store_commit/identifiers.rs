use super::device_state::merge_history_cuts;
use super::*;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectHash([u8; 32]);

impl ObjectHash {
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub(crate) fn from_digest(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ObjectHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for ObjectHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for ObjectHash {
    type Err = StoreProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(StoreProtocolError::InvalidObjectHash(value.to_string()));
        }
        let decoded = hex::decode(value)
            .map_err(|_| StoreProtocolError::InvalidObjectHash(value.to_string()))?;
        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|_| StoreProtocolError::InvalidObjectHash(value.to_string()))?;
        Ok(Self(bytes))
    }
}

impl Serialize for ObjectHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ObjectHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Closed coordinate of one Store commit under the Store's signed policy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitCoord {
    MergeConcurrent {
        stream_id: AuthorStreamId,
        sequence: u64,
    },
    Serial {
        sequence: u64,
    },
}

impl StoreCommitCoord {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::MergeConcurrent { sequence, .. } | Self::Serial { sequence } => *sequence,
        }
    }

    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial { .. } => WritePolicy::Serial,
        }
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
            policy: WritePolicy,
            sequence: u64,
            predecessor: CandidateFamilyPredecessor<'a>,
        }

        #[derive(Serialize)]
        #[serde(rename_all = "snake_case")]
        enum CandidateFamilyPredecessor<'a> {
            Merge(Option<&'a StoreBatchCommitRef>),
            SerialGenesis {
                root: &'a StoreRootRef,
                founder_registration: &'a StoreDeviceRegistrationRef,
            },
            SerialCommit(&'a StoreBatchCommitRef),
        }

        let predecessor = match order {
            StoreCommitOrder::MergeConcurrent { predecessor, .. } => {
                CandidateFamilyPredecessor::Merge(predecessor.as_ref())
            }
            StoreCommitOrder::Serial {
                predecessor:
                    StoreSerialPredecessor::Genesis {
                        root,
                        founder_registration,
                    },
                ..
            } => CandidateFamilyPredecessor::SerialGenesis {
                root,
                founder_registration,
            },
            StoreCommitOrder::Serial {
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => CandidateFamilyPredecessor::SerialCommit(predecessor),
        };
        let fields = Fields {
            store_root_hash,
            author_registration,
            write_id,
            policy: order.policy(),
            sequence: order.seq(),
            predecessor,
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
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        let commit = self.verify_exact_candidate(expected_store_root_hash, author)?;
        if matches!(
            &commit.body,
            StoreCommitBody::SerialRecoveryActivation { .. }
                | StoreCommitBody::AbandonCandidates { .. }
        ) {
            return Err(StoreProtocolError::Malformed(
                "retained authority cannot be a candidate cleanup target".to_string(),
            ));
        }
        Ok(commit)
    }

    pub(crate) fn verify_nonactivation_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        self.verify_exact_candidate(expected_store_root_hash, author)
    }

    fn verify_exact_candidate(
        &self,
        expected_store_root_hash: ObjectHash,
        author: &StoreDeviceRegistration,
    ) -> Result<StoreBatchCommit, StoreProtocolError> {
        self.object
            .verify(&self.canonical_signed_bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        let commit: StoreBatchCommit = serde_json::from_slice(&self.canonical_signed_bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        if commit.to_bytes() != self.canonical_signed_bytes {
            return Err(StoreProtocolError::Malformed(
                "candidate commit bytes are not canonical".to_string(),
            ));
        }
        commit.verify_at(expected_store_root_hash, &self.coord, author)?;
        StoreBatchCommitRef::from_commit(&commit, self.coord.clone(), self.object.clone())?;
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
        if coord.policy() != commit.policy() || coord.sequence() != commit.seq() {
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
        if self.coord.policy() != commit.policy()
            || self.coord.sequence() != commit.seq()
            || self.commit_hash != commit.commit_hash()
        {
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
    pub(crate) fn from_digest(hash: ObjectHash) -> Self {
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
pub(crate) struct RegisteredStreamActivation {
    activation: StreamActivation,
    activating_commit: StoreBatchCommitRef,
}

impl RegisteredStreamActivation {
    pub(crate) fn from_stored(
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

    pub(crate) fn activation(&self) -> &StreamActivation {
        &self.activation
    }

    pub(crate) fn activating_commit(&self) -> &StoreBatchCommitRef {
        &self.activating_commit
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StreamAnchorDomain {
    StoreMembership,
    OwnerRecovery,
    CircleControl { circle_id: CircleId },
    CircleRoster { circle_id: CircleId },
    CircleMetadata { circle_id: CircleId },
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

    pub(crate) fn device_authorized_stream_id(
        store_root_hash: ObjectHash,
        author_registration: &StoreDeviceRegistrationRef,
        domain: StreamAnchorDomain,
    ) -> AuthorStreamId {
        derive_device_author_stream_id(store_root_hash, author_registration, domain)
    }

    pub(crate) fn grant_authorized_stream_id(
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

/// Exact materialized cut, shaped by the Store's signed write policy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitFrontier {
    MergeConcurrent(BTreeMap<AuthorStreamId, StoreBatchCommitRef>),
    Serial(Option<StoreBatchCommitRef>),
}

/// Exact Store history cut, including the signed Serial genesis authority.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreHistoryCut {
    MergeConcurrent(BTreeMap<AuthorStreamId, StoreBatchCommitRef>),
    Serial(StoreSerialPredecessor),
}

impl StoreHistoryCut {
    pub fn merge_concurrent(commits: BTreeMap<AuthorStreamId, StoreBatchCommitRef>) -> Self {
        Self::MergeConcurrent(commits)
    }

    pub fn serial(predecessor: StoreSerialPredecessor) -> Self {
        Self::Serial(predecessor)
    }

    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent(_) => WritePolicy::MergeConcurrent,
            Self::Serial(_) => WritePolicy::Serial,
        }
    }

    pub fn position_count(&self) -> usize {
        match self {
            Self::MergeConcurrent(commits) => commits.len(),
            Self::Serial(_) => 1,
        }
    }

    pub fn serial_predecessor(&self) -> Option<&StoreSerialPredecessor> {
        match self {
            Self::Serial(predecessor) => Some(predecessor),
            Self::MergeConcurrent(_) => None,
        }
    }

    pub fn frontier(&self) -> CommitFrontier {
        match self {
            Self::MergeConcurrent(commits) => CommitFrontier::MergeConcurrent(commits.clone()),
            Self::Serial(StoreSerialPredecessor::Genesis { .. }) => CommitFrontier::Serial(None),
            Self::Serial(StoreSerialPredecessor::Commit(commit)) => {
                CommitFrontier::Serial(Some(commit.clone()))
            }
        }
    }

    pub(crate) fn join(self, other: Self) -> Result<Self, StoreProtocolError> {
        merge_history_cuts(self, other)
    }
}

impl CommitFrontier {
    pub fn from_refs(
        policy: WritePolicy,
        mut commits: BTreeMap<String, StoreBatchCommitRef>,
    ) -> Result<Self, StoreProtocolError> {
        match policy {
            WritePolicy::MergeConcurrent => commits
                .into_iter()
                .map(|(stream_id, commit)| {
                    let stream_id = stream_id.parse().map_err(StoreProtocolError::Malformed)?;
                    Ok((stream_id, commit))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map(Self::MergeConcurrent),
            WritePolicy::Serial => {
                let commit = commits.remove(SERIAL_STREAM_ID);
                if !commits.is_empty() {
                    return Err(StoreProtocolError::Malformed(format!(
                        "Serial frontier contains non-serial streams: {:?}",
                        commits.keys().collect::<Vec<_>>()
                    )));
                }
                if commit.as_ref().is_some_and(|reference| {
                    !matches!(reference.coord, StoreCommitCoord::Serial { .. })
                }) {
                    return Err(StoreProtocolError::Malformed(
                        "Serial frontier contains a Merge commit".to_string(),
                    ));
                }
                Ok(Self::Serial(commit))
            }
        }
    }

    pub fn into_refs(self) -> BTreeMap<String, StoreBatchCommitRef> {
        match self {
            Self::MergeConcurrent(commits) => commits
                .into_iter()
                .map(|(stream_id, commit)| (stream_id.to_string(), commit))
                .collect(),
            Self::Serial(Some(commit)) => BTreeMap::from([(SERIAL_STREAM_ID.to_string(), commit)]),
            Self::Serial(None) => BTreeMap::new(),
        }
    }

    pub fn position_count(&self) -> usize {
        match self {
            Self::MergeConcurrent(positions) => positions.len(),
            Self::Serial(Some(_)) => 1,
            Self::Serial(None) => 0,
        }
    }

    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent(_) => WritePolicy::MergeConcurrent,
            Self::Serial(_) => WritePolicy::Serial,
        }
    }

    pub fn covers(&self, covered: &Self) -> bool {
        match (self, covered) {
            (Self::MergeConcurrent(current), Self::MergeConcurrent(covered)) => {
                covered.iter().all(|(stream, covered_ref)| {
                    current.get(stream).is_some_and(|current_ref| {
                        current_ref.coord.sequence() > covered_ref.coord.sequence()
                            || current_ref.coord.sequence() == covered_ref.coord.sequence()
                                && current_ref == covered_ref
                    })
                })
            }
            (Self::Serial(current), Self::Serial(covered)) => match (current, covered) {
                (_, None) => true,
                (Some(current), Some(covered)) => {
                    current.coord.sequence() > covered.coord.sequence()
                        || current.coord.sequence() == covered.coord.sequence()
                            && current == covered
                }
                (None, Some(_)) => false,
            },
            _ => false,
        }
    }

    pub fn merge_commits(
        &self,
    ) -> Result<&BTreeMap<AuthorStreamId, StoreBatchCommitRef>, StoreProtocolError> {
        match self {
            Self::MergeConcurrent(commits) => Ok(commits),
            Self::Serial(_) => Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::MergeConcurrent,
                actual: WritePolicy::Serial,
            }),
        }
    }

    pub fn serial_commit(&self) -> Result<Option<&StoreBatchCommitRef>, StoreProtocolError> {
        match self {
            Self::Serial(position) => Ok(position.as_ref()),
            Self::MergeConcurrent(_) => Err(StoreProtocolError::WritePolicyMismatch {
                expected: WritePolicy::Serial,
                actual: WritePolicy::MergeConcurrent,
            }),
        }
    }
}

/// Predecessor and dependency order authenticated by one Store commit.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreCommitOrder {
    MergeConcurrent {
        seq: u64,
        predecessor: Option<StoreBatchCommitRef>,
        dependencies: BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
    },
    Serial {
        seq: u64,
        predecessor: SerialStorePosition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SerialStorePosition {
    Genesis {
        root: StoreRootRef,
        founder_registration: StoreDeviceRegistrationRef,
    },
    Commit(StoreBatchCommitRef),
}

pub type StoreSerialPredecessor = SerialStorePosition;

impl StoreCommitOrder {
    pub fn policy(&self) -> WritePolicy {
        match self {
            Self::MergeConcurrent { .. } => WritePolicy::MergeConcurrent,
            Self::Serial { .. } => WritePolicy::Serial,
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            Self::MergeConcurrent { seq, .. } | Self::Serial { seq, .. } => *seq,
        }
    }

    pub fn predecessor(&self) -> Option<&StoreBatchCommitRef> {
        match self {
            Self::MergeConcurrent { predecessor, .. } => predecessor.as_ref(),
            Self::Serial {
                predecessor: StoreSerialPredecessor::Commit(predecessor),
                ..
            } => Some(predecessor),
            Self::Serial {
                predecessor: StoreSerialPredecessor::Genesis { .. },
                ..
            } => None,
        }
    }

    pub fn dependencies(&self) -> Option<&BTreeMap<AuthorStreamId, StoreBatchCommitRef>> {
        match self {
            Self::MergeConcurrent { dependencies, .. } => Some(dependencies),
            Self::Serial { .. } => None,
        }
    }

    pub fn stream_id<'a>(&self, device_id: &'a str) -> &'a str {
        match self {
            Self::MergeConcurrent { .. } => device_id,
            Self::Serial { .. } => SERIAL_STREAM_ID,
        }
    }

    pub fn predecessor_cut(&self) -> Result<StoreHistoryCut, StoreProtocolError> {
        match self {
            Self::MergeConcurrent {
                predecessor,
                dependencies,
                ..
            } => {
                let mut cut = dependencies.clone();
                if let Some(predecessor) = predecessor {
                    let StoreCommitCoord::MergeConcurrent { stream_id, .. } = predecessor.coord
                    else {
                        return Err(StoreProtocolError::JoinAttemptMismatch);
                    };
                    if cut
                        .insert(stream_id, predecessor.clone())
                        .is_some_and(|existing| existing != *predecessor)
                    {
                        return Err(StoreProtocolError::JoinAttemptMismatch);
                    }
                }
                Ok(StoreHistoryCut::MergeConcurrent(cut))
            }
            Self::Serial { predecessor, .. } => Ok(StoreHistoryCut::Serial(predecessor.clone())),
        }
    }
}

pub const SERIAL_STREAM_ID: &str = "serial";

pub(super) fn commit_stream_id(coord: &StoreCommitCoord) -> String {
    match coord {
        StoreCommitCoord::MergeConcurrent { stream_id, .. } => stream_id.to_string(),
        StoreCommitCoord::Serial { .. } => SERIAL_STREAM_ID.to_string(),
    }
}
