use crate::*;
use coven_protocol::audience_package::AudiencePackage;
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
    StoreMembershipConflictResolutionRef,
};
use coven_protocol::objects::{ExactObjectRef, PreparedExactObject};
use coven_protocol::remote_object::{remote_object_id, SharedLiveSetObjectDomain};
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, CirclePackageRef, ObjectHash, RetainedStoreDeviceOperations,
    RetainedStoreDeviceRegistrationActivations, StoreBatchCommit, StoreBatchCommitRef,
    StorePackageRef, VerifiedStoreDeviceOperations,
};
use coven_protocol::store_commit::{
    RetainedMergeCommitEvidence, RetainedReplaySnapshotAuthority, StoreRootRef,
    VerifiedStoreBatchCommit,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMergeMaterializationInput {
    pub commit: PreparedExactObject,
    pub history_evidence: RetainedMergeCommitEvidence,
    pub membership_objects: Option<VerifiedMergeMembershipObjects>,
    pub packages: Vec<RetainedAudiencePackage>,
    pub activation: RetainedCommitActivationInput,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedMergeMembershipObjects {
    entry: MembershipEntryRef,
    head: MembershipHeadRef,
    resolution: Option<StoreMembershipConflictResolutionRef>,
}

impl VerifiedMergeMembershipObjects {
    pub fn entry(&self) -> &MembershipEntryRef {
        &self.entry
    }

    pub fn head(&self) -> &MembershipHeadRef {
        &self.head
    }

    pub fn resolution(&self) -> Option<&StoreMembershipConflictResolutionRef> {
        self.resolution.as_ref()
    }

    pub fn verify(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        entry: &MembershipEntry,
        head_value: &AuthorHead,
        head: MembershipHeadRef,
    ) -> Result<Self, DbError> {
        let Some(coven_protocol::store_commit::StoreControl { transition }) = commit.control()
        else {
            return Err(DbError::Message(
                "Merge membership object closure accompanies another Store control".to_string(),
            ));
        };
        if transition.body.entry.coord != entry.coord()
            || !transition.matches_head(head_value, &head)
            || !matches!(
                &head_value.activation,
                coven_protocol::membership::MembershipHeadActivation::StoreCommit { commit, .. }
                    if commit == commit_ref
            )
        {
            return Err(DbError::Message(
                "Merge membership object closure differs from its exact Store transition"
                    .to_string(),
            ));
        }
        let resolution = match &entry.change {
            coven_protocol::membership::StoreAuthorityChange::ResolutionActivation {
                resolution,
            } => Some(resolution.clone()),
            _ => None,
        };
        Ok(Self {
            entry: transition.body.entry.clone(),
            head,
            resolution,
        })
    }

    pub fn object_ids(&self) -> impl Iterator<Item = ObjectHash> + '_ {
        [
            Some(remote_object_id(&self.entry.object)),
            Some(remote_object_id(&self.head.object)),
            self.resolution
                .as_ref()
                .map(|resolution| remote_object_id(&resolution.object)),
        ]
        .into_iter()
        .flatten()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedAudiencePackage {
    Store {
        reference: StorePackageRef,
        package: AudiencePackage,
    },
    Circle {
        reference: CirclePackageRef,
        package: AudiencePackage,
    },
}

impl RetainedAudiencePackage {
    pub fn verify(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        package: AudiencePackage,
    ) -> Result<Self, DbError> {
        if package.store_root_hash() != commit.store_root_hash
            || package.write_id() != &commit.write_id
            || package.commit_coord() != &commit_ref.coord
            || package.candidate_family() != commit.candidate_family()
        {
            return Err(DbError::Message(
                "retained audience package differs from its exact Store commit".to_string(),
            ));
        }
        package
            .validate_blob_uploader(&commit.author_registration)
            .map_err(DbError::from)?;
        match package.audience() {
            coven_protocol::audience_package::PackageAudience::Store => {
                let reference = commit.store_package().ok_or_else(|| {
                    DbError::Message(
                        "retained Store package is absent from its exact commit".to_string(),
                    )
                })?;
                if package.schema_version() != reference.schema_version {
                    return Err(DbError::Message(
                        "retained Store package schema version differs from its exact commit"
                            .to_string(),
                    ));
                }
                commit
                    .verify_store_package(&package.to_bytes())
                    .map_err(DbError::from)?;
                Ok(Self::Store {
                    reference: reference.clone(),
                    package,
                })
            }
            coven_protocol::audience_package::PackageAudience::Circle {
                circle_id,
                control,
                key_fingerprint,
            } => {
                let reference = commit
                    .circle_packages()
                    .iter()
                    .find(|reference| reference.circle_id == *circle_id)
                    .ok_or_else(|| {
                        DbError::Message(format!(
                            "retained Circle package {circle_id} is absent from its exact commit"
                        ))
                    })?;
                if reference.control != *control
                    || reference.key_fingerprint != *key_fingerprint
                    || package.schema_version() != reference.package.schema_version
                {
                    return Err(DbError::Message(format!(
                        "retained Circle package {circle_id} differs from its exact commit"
                    )));
                }
                commit
                    .verify_circle_package(*circle_id, &package.to_bytes())
                    .map_err(DbError::from)?;
                Ok(Self::Circle {
                    reference: reference.clone(),
                    package,
                })
            }
        }
    }

    pub fn package(&self) -> &AudiencePackage {
        match self {
            Self::Store { package, .. } | Self::Circle { package, .. } => package,
        }
    }

    pub fn domain(&self) -> SharedLiveSetObjectDomain {
        match self {
            Self::Store { reference, .. } => SharedLiveSetObjectDomain::StorePackage {
                reference: reference.clone(),
            },
            Self::Circle { reference, .. } => SharedLiveSetObjectDomain::CirclePackage {
                reference: reference.clone(),
            },
        }
    }

    pub fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Store { reference, .. } => &reference.object,
            Self::Circle { reference, .. } => &reference.package.object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedCommitActivationInput {
    pub registrations: RetainedStoreDeviceRegistrationActivations,
    pub device_operations: RetainedStoreDeviceOperations,
    pub circle_activations: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_application: Option<RetainedPackageApplication>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RetainedPackageApplication {
    LocallyAuthored,
    Received { receiver_wall_ms: u64 },
}

pub struct RetainedMergeMaterializationKey {
    pub commit_ref: String,
    pub input_hash: ObjectHash,
}

pub struct VerifiedMergeMaterialization<'a> {
    root: &'a coven_protocol::store_commit::StoreRootRef,
    verified_commit: &'a coven_protocol::store_commit::VerifiedStoreBatchCommit,
    device_operations: &'a VerifiedStoreDeviceOperations,
    circle_activations: &'a VerifiedCircleActivations,
    acceptance: &'a AcceptedStoreCommitEvidence,
    history_evidence: &'a RetainedMergeCommitEvidence,
    membership_objects: Option<&'a VerifiedMergeMembershipObjects>,
    packages: &'a [AudiencePackage],
    package_application: Option<RetainedPackageApplication>,
    registrations: &'a [ActivatedStoreDeviceRegistration],
}

#[derive(Clone)]
pub struct OwnedVerifiedMergeMaterialization {
    root: coven_protocol::store_commit::StoreRootRef,
    verified_commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
    registrations: Vec<ActivatedStoreDeviceRegistration>,
    device_operations: VerifiedStoreDeviceOperations,
    circle_activations: VerifiedCircleActivations,
    acceptance: AcceptedStoreCommitEvidence,
    history_evidence: RetainedMergeCommitEvidence,
    membership_objects: Option<VerifiedMergeMembershipObjects>,
    packages: Vec<AudiencePackage>,
    package_application: Option<RetainedPackageApplication>,
    input_hash: ObjectHash,
}

/// The replay baseline a device stands on, as the history verifier needs it.
///
/// A device that installed or advanced a baseline holds one signed image in
/// place of the commits under `coverage`, and the rows those commits produced
/// are gone. So a walk that reaches a covered position has nothing left to walk
/// to — and nothing to prove, because the baseline already restates the result.
/// It stops there instead, and reads the position's device state out of
/// `covered_states`, which is what `retain_snapshot_device_states` keeps alive
/// for exactly the positions commits above the coverage still name.
///
/// A device on a genesis baseline has an empty coverage, so every walk runs to
/// genesis as it always did.
#[derive(Debug, Clone)]
pub struct InstalledReplayBaseline {
    coverage: coven_protocol::store_commit::CommitFrontier,
    covered_states: std::collections::BTreeMap<
        StoreBatchCommitRef,
        std::sync::Arc<coven_protocol::store_commit::ResolvedStoreDeviceState>,
    >,
    summary: Option<coven_protocol::store_commit::OpenedRetainedMergeHistorySummary>,
    /// The snapshot this baseline was installed or advanced from, when it came
    /// from one. Keeping the exact published snapshot lets pre-activation join
    /// verification read its installed starting point without requiring a
    /// local author registration.
    snapshot: Option<PublishedStoreSnapshot>,
}

impl Default for InstalledReplayBaseline {
    /// The genesis baseline: nothing is covered, so every walk runs to the
    /// bottom of the history exactly as it does on a device that never
    /// installed a snapshot.
    fn default() -> Self {
        Self {
            coverage: coven_protocol::store_commit::CommitFrontier(
                std::collections::BTreeMap::new(),
            ),
            covered_states: std::collections::BTreeMap::new(),
            summary: None,
            snapshot: None,
        }
    }
}

impl InstalledReplayBaseline {
    pub fn new(
        coverage: coven_protocol::store_commit::CommitFrontier,
        covered_states: std::collections::BTreeMap<
            StoreBatchCommitRef,
            std::sync::Arc<coven_protocol::store_commit::ResolvedStoreDeviceState>,
        >,
        summary: Option<coven_protocol::store_commit::OpenedRetainedMergeHistorySummary>,
        snapshot: Option<PublishedStoreSnapshot>,
    ) -> Self {
        Self {
            coverage,
            covered_states,
            summary,
            snapshot,
        }
    }

    /// Whether this baseline was installed from `snapshot` itself.
    pub fn stands_on(&self, snapshot: &coven_protocol::store_commit::StoreSnapshotRef) -> bool {
        self.snapshot
            .as_ref()
            .is_some_and(|installed| &installed.reference == snapshot)
    }

    /// The exact snapshot installed as this replay's starting point.
    pub fn snapshot(&self) -> Option<&PublishedStoreSnapshot> {
        self.snapshot.as_ref()
    }

    /// The signed history summary standing for everything under the coverage.
    ///
    /// A composition that walked to genesis produced this from the commits
    /// themselves; one that stops at the baseline starts from it instead. Both
    /// arrive at the same summary, which is what makes a summary a summary.
    pub fn history_summary(
        &self,
    ) -> Option<&coven_protocol::store_commit::OpenedRetainedMergeHistorySummary> {
        self.summary.as_ref()
    }

    pub fn coverage(&self) -> &coven_protocol::store_commit::CommitFrontier {
        &self.coverage
    }

    /// Whether the baseline restates `reference`, so no walk need pass it.
    pub fn covers(&self, reference: &StoreBatchCommitRef) -> bool {
        self.coverage.covers_commit(reference)
    }

    /// The device state that stood at a covered position, or `None` when this
    /// device never recorded one there — which is a commit naming a position
    /// outside its own history, not a baseline that lost something.
    pub fn covered_state(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&coven_protocol::store_commit::ResolvedStoreDeviceState> {
        self.covered_states.get(reference).map(AsRef::as_ref)
    }

    pub fn covered_states(
        &self,
    ) -> impl Iterator<
        Item = (
            &StoreBatchCommitRef,
            &coven_protocol::store_commit::ResolvedStoreDeviceState,
        ),
    > {
        self.covered_states
            .iter()
            .map(|(reference, state)| (reference, state.as_ref()))
    }
}

pub enum RetainedMergeHistoryCheckpoint {
    Snapshot(coven_protocol::store_commit::OpenedRetainedMergeHistorySummary),
    Commit(Box<OwnedVerifiedMergeMaterialization>),
}

impl OwnedVerifiedMergeMaterialization {
    pub fn verify(
        root: coven_protocol::store_commit::StoreRootRef,
        verified_commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
        registrations: Vec<ActivatedStoreDeviceRegistration>,
        device_operations: VerifiedStoreDeviceOperations,
        circle_activations: VerifiedCircleActivations,
        acceptance: AcceptedStoreCommitEvidence,
        history_evidence: RetainedMergeCommitEvidence,
        membership_objects: Option<VerifiedMergeMembershipObjects>,
        packages: Vec<AudiencePackage>,
        package_application: Option<RetainedPackageApplication>,
        input_hash: ObjectHash,
    ) -> Result<Self, DbError> {
        VerifiedMergeMaterialization::verify(
            &root,
            &verified_commit,
            &registrations,
            &device_operations,
            &circle_activations,
            &acceptance,
            &history_evidence,
            membership_objects.as_ref(),
            &packages,
            package_application,
        )?;
        Ok(Self {
            root,
            verified_commit,
            registrations,
            device_operations,
            circle_activations,
            acceptance,
            history_evidence,
            membership_objects,
            packages,
            package_application,
            input_hash,
        })
    }

    pub fn input_hash(&self) -> ObjectHash {
        self.input_hash
    }

    pub fn root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        &self.root
    }

    pub fn commit(&self) -> &StoreBatchCommit {
        self.verified_commit.value()
    }

    pub fn commit_ref(&self) -> &StoreBatchCommitRef {
        self.verified_commit.reference()
    }

    pub fn verified_commit(&self) -> &coven_protocol::store_commit::VerifiedStoreBatchCommit {
        &self.verified_commit
    }

    pub fn registrations(&self) -> &[ActivatedStoreDeviceRegistration] {
        &self.registrations
    }

    pub fn device_operations(&self) -> &VerifiedStoreDeviceOperations {
        &self.device_operations
    }

    pub fn circle_activations(&self) -> &VerifiedCircleActivations {
        &self.circle_activations
    }

    pub fn circle_activation(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<coven_protocol::circle_activation::VerifiedCircleReference, DbError> {
        let mut matches = self
            .circle_activations
            .circles()
            .iter()
            .filter(|activation| {
                activation.circle_id == circle_id && activation.control.coord == *control
            });
        let activation = matches.next().cloned().ok_or_else(|| {
            DbError::Message(format!(
                "Circle {circle_id} retained activation omits control {control:?}"
            ))
        })?;
        if matches.next().is_some() {
            return Err(DbError::Message(format!(
                "Circle {circle_id} retained activation duplicates control {control:?}"
            )));
        }
        Ok(activation)
    }

    pub fn acceptance(&self) -> &AcceptedStoreCommitEvidence {
        &self.acceptance
    }

    pub fn history_evidence(&self) -> &RetainedMergeCommitEvidence {
        &self.history_evidence
    }

    pub fn membership_objects(&self) -> Option<&VerifiedMergeMembershipObjects> {
        self.membership_objects.as_ref()
    }

    pub(crate) fn membership_remote_objects(
        &self,
    ) -> Result<Vec<coven_protocol::remote_object::ClosedRemoteObject>, DbError> {
        let Some(objects) = self.membership_objects() else {
            return Ok(Vec::new());
        };
        let proof = self
            .history_evidence
            .membership_proof
            .as_ref()
            .expect("verified membership objects have their exact retained proof");
        // Verification binds these canonical plaintext bytes to the exact
        // objects. A retained image need not carry a second remote-record copy.
        let entry = serde_json::to_vec(&proof.entry_value)?;
        let head = serde_json::to_vec(&proof.head_value)?;
        let resolution = proof
            .resolution_value
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .map(|bytes| MembershipAuthorityBytes::new(bytes.clone(), bytes));
        activated_merge_membership_remote_objects(
            self.commit().candidate_family(),
            objects,
            MembershipAuthorityBytes::new(entry.clone(), entry),
            MembershipAuthorityBytes::new(head.clone(), head),
            resolution,
            self.commit_ref(),
        )
        .map_err(DbError::from)
    }

    pub fn packages(&self) -> &[AudiencePackage] {
        &self.packages
    }

    pub fn package_application(&self) -> Option<RetainedPackageApplication> {
        self.package_application
    }
}

impl<'a> VerifiedMergeMaterialization<'a> {
    pub fn root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        self.root
    }

    pub fn commit(&self) -> &StoreBatchCommit {
        self.verified_commit.value()
    }

    pub fn commit_ref(&self) -> &StoreBatchCommitRef {
        self.verified_commit.reference()
    }

    pub fn verified_commit(&self) -> &coven_protocol::store_commit::VerifiedStoreBatchCommit {
        self.verified_commit
    }

    pub fn registrations(&self) -> &[ActivatedStoreDeviceRegistration] {
        self.registrations
    }

    pub fn device_operations(&self) -> &VerifiedStoreDeviceOperations {
        self.device_operations
    }

    pub fn circle_activations(&self) -> &VerifiedCircleActivations {
        self.circle_activations
    }

    pub fn acceptance(&self) -> &AcceptedStoreCommitEvidence {
        self.acceptance
    }

    pub fn history_evidence(&self) -> &RetainedMergeCommitEvidence {
        self.history_evidence
    }

    pub fn membership_objects(&self) -> Option<&VerifiedMergeMembershipObjects> {
        self.membership_objects
    }

    pub fn packages(&self) -> &[AudiencePackage] {
        self.packages
    }

    pub fn package_application(&self) -> Option<RetainedPackageApplication> {
        self.package_application
    }

    pub fn verify(
        root: &'a coven_protocol::store_commit::StoreRootRef,
        verified_commit: &'a coven_protocol::store_commit::VerifiedStoreBatchCommit,
        registrations: &'a [ActivatedStoreDeviceRegistration],
        device_operations: &'a VerifiedStoreDeviceOperations,
        circle_activations: &'a VerifiedCircleActivations,
        acceptance: &'a AcceptedStoreCommitEvidence,
        history_evidence: &'a RetainedMergeCommitEvidence,
        membership_objects: Option<&'a VerifiedMergeMembershipObjects>,
        packages: &'a [AudiencePackage],
        package_application: Option<RetainedPackageApplication>,
    ) -> Result<Self, DbError> {
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        history_evidence
            .validate_for(commit_ref, commit)
            .map_err(DbError::from)?;
        if verified_commit.store_root_hash() != root.store_root_hash
            || commit.store_root_hash != root.store_root_hash
            || acceptance.commit_ref() != commit_ref
            || circle_activations.stream_activations().activating_commit() != commit_ref
            || circle_activations.stream_activations().as_slice() != commit.stream_activations()
            || circle_activations.circles().len() != commit.circle_controls().len()
            || circle_activations
                .circles()
                .iter()
                .zip(commit.circle_controls())
                .any(|(activation, reference)| activation.reference != *reference)
            || packages.is_empty() != package_application.is_none()
            || commit.control().is_some() != membership_objects.is_some()
        {
            return Err(DbError::Message(
                "verified Merge materialization differs from its exact Store commit".to_string(),
            ));
        }
        let retained_objects = history_evidence
            .membership_proof
            .as_ref()
            .map(|proof| {
                VerifiedMergeMembershipObjects::verify(
                    commit,
                    commit_ref,
                    &proof.entry_value,
                    &proof.head_value,
                    proof.head.clone(),
                )
            })
            .transpose()?;
        if membership_objects != retained_objects.as_ref() {
            return Err(DbError::Message(
                "Merge membership objects differ from their retained exact proof".into(),
            ));
        }
        RetainedStoreDeviceRegistrationActivations::from_verified(root, commit, registrations)
            .map_err(DbError::from)?;
        Ok(Self {
            root,
            verified_commit,
            device_operations,
            circle_activations,
            acceptance,
            history_evidence,
            membership_objects,
            packages,
            package_application,
            registrations,
        })
    }
}

pub struct PreparedMergeMaterializationPackage {
    pub package: AudiencePackage,
    pub changeset: ValidatedChangeset<Vec<u8>>,
}

pub struct PreparedMergeMaterialization {
    pub root: StoreRootRef,
    pub verified_commit: VerifiedStoreBatchCommit,
    pub acceptance: AcceptedStoreCommitEvidence,
    pub history_evidence: RetainedMergeCommitEvidence,
    pub membership_objects: Option<VerifiedMergeMembershipObjects>,
    pub membership_remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    pub registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub packages: Vec<PreparedMergeMaterializationPackage>,
    pub device_operations: VerifiedStoreDeviceOperations,
    pub circle_activations: VerifiedCircleActivations,
    pub package_application: Option<crate::RetainedPackageApplication>,
}

pub struct MembershipAuthorityBytes {
    canonical: Vec<u8>,
    stored: Vec<u8>,
}

impl MembershipAuthorityBytes {
    pub fn new(canonical: Vec<u8>, stored: Vec<u8>) -> Self {
        Self { canonical, stored }
    }
}

pub fn activated_merge_membership_remote_objects(
    family: coven_protocol::store_commit::CandidateFamilyId,
    objects: &VerifiedMergeMembershipObjects,
    entry_bytes: MembershipAuthorityBytes,
    head_bytes: MembershipAuthorityBytes,
    resolution_bytes: Option<MembershipAuthorityBytes>,
    commit_ref: &StoreBatchCommitRef,
) -> Result<
    Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    coven_protocol::remote_object::RemoteObjectRecordError,
> {
    let mut remotes = vec![
        coven_protocol::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
            family,
            objects.entry().clone(),
            &entry_bytes.canonical,
            &entry_bytes.stored,
            commit_ref.clone(),
        )?
        .map_record(|record| record.into_observed_activated(commit_ref))?,
        coven_protocol::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
            family,
            objects.head().clone(),
            &head_bytes.canonical,
            &head_bytes.stored,
            commit_ref.clone(),
        )?
        .map_record(|record| record.into_observed_activated(commit_ref))?,
    ];
    if let Some(resolution) = objects.resolution() {
        let bytes = resolution_bytes.ok_or(
            coven_protocol::remote_object::RemoteObjectRecordError::StoredReferenceMismatch,
        )?;
        remotes.push(
            coven_protocol::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                resolution.clone(),
                &bytes.canonical,
                &bytes.stored,
                commit_ref.clone(),
            )?
            .map_record(|record| record.into_observed_activated(commit_ref))?,
        );
    } else if resolution_bytes.is_some() {
        return Err(
            coven_protocol::remote_object::RemoteObjectRecordError::StoredReferenceMismatch,
        );
    }
    Ok(remotes)
}

/// One snapshot verified as installable: its signed metadata, the cut it
/// covers, and the devices and registrations active there. A device installs
/// its baseline from this and verifies everything that arrives afterwards
/// against it.
#[derive(Debug)]
pub struct VerifiedStoreSnapshotAuthority {
    authority: RetainedReplaySnapshotAuthority,
}

pub(crate) struct PreparedSnapshotReplayBaselineAdvance {
    pub(crate) changes_publication_base: bool,
    pub(crate) expected_current_cut: coven_protocol::store_commit::CommitFrontier,
    pub(crate) image: Vec<u8>,
    pub(crate) folded: Vec<crate::SettledStoreWrite>,
}

impl VerifiedStoreSnapshotAuthority {
    pub fn from_authority(
        authority: RetainedReplaySnapshotAuthority,
    ) -> Result<Self, crate::DbError> {
        authority.validate()?;
        Ok(Self { authority })
    }

    pub fn into_authority(self) -> RetainedReplaySnapshotAuthority {
        self.authority
    }
}

#[derive(Clone)]
pub struct DeviceJoinBootstrapCommit {
    pub reference: StoreBatchCommitRef,
    pub commit: VerifiedStoreBatchCommit,
    pub registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub device_operations: VerifiedStoreDeviceOperations,
    pub history_evidence: RetainedMergeCommitEvidence,
}

pub struct DeviceJoinBootstrapPlan {
    pub founder_reference: StoreDeviceRegistrationRef,
    pub founder: StoreDeviceRegistration,
    pub founder_bytes: Vec<u8>,
    pub genesis: ResolvedStoreDeviceState,
    pub membership: InitialStoreMembershipAuthority,
    pub publication: AcceptedStorePublicationInterval,
    pub commits: Vec<DeviceJoinBootstrapCommit>,
}

/// Everything one bootstrap commit needs to materialize its rows, for a commit
/// the joining database does not already cover through an installed snapshot.
///
/// Installation runs inside a single database transaction and cannot read the
/// cloud, so the joining device resolves this beforehand — reading, decrypting
/// and verifying each package exactly the way an ordinary pull does.
pub struct DeviceJoinBootstrapRowData {
    pub circle_activations: VerifiedCircleActivations,
    pub membership_objects: Option<VerifiedMergeMembershipObjects>,
    pub membership_remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    pub packages: Vec<PreparedMergeMaterializationPackage>,
}

/// A bootstrap plan together with the row data for every commit in it the
/// joining database does not already materialize. Installation only accepts
/// this shape, so a bootstrap can never advance its position over commits
/// whose rows were never resolved.
pub struct ResolvedDeviceJoinBootstrap {
    pub plan: DeviceJoinBootstrapPlan,
    pub snapshot_circles: crate::StagedCircleRestore,
    pub row_data: std::collections::BTreeMap<StoreBatchCommitRef, DeviceJoinBootstrapRowData>,
    pub local_store_membership: coven_protocol::membership::LocalStoreMembership,
    pub routing_key: Option<coven_protocol::circle::RowRoutingKey>,
    pub receiver_wall_ms: u64,
}

#[path = "device_join_bootstrap.rs"]
mod device_join_bootstrap;
