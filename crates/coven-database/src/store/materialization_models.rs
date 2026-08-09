use crate::RetainedReplaySnapshotAuthority;
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
    StoreDeviceHead, StorePackageRef, VerifiedStoreDeviceOperations,
};
use coven_protocol::store_commit::{
    RetainedMergeCommitEvidence, StoreRootRef, VerifiedStoreBatchCommit,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedMergeMaterializationInput {
    pub commit: PreparedExactObject,
    pub activation_head: PreparedExactObject,
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
                coven_protocol::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == commit_ref
            )
        {
            return Err(DbError::Message(
                "Merge membership object closure differs from its exact Store transition"
                    .to_string(),
            ));
        }
        let resolution = match &entry.change {
            coven_protocol::membership::MembershipChange::ResolutionActivation { resolution } => {
                Some(resolution.clone())
            }
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
#[serde(deny_unknown_fields)]
pub struct MergeRetractionCleanupInput {
    pub commit: PreparedExactObject,
    pub activation_head: PreparedExactObject,
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
            .map_err(|error| DbError::Message(error.to_string()))?;
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
                    .map_err(|error| DbError::Message(error.to_string()))?;
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
                    .map_err(|error| DbError::Message(error.to_string()))?;
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
    verified_commit: &'a coven_protocol::store_commit::VerifiedStoreBatchCommit,
    device_operations: &'a VerifiedStoreDeviceOperations,
    circle_activations: &'a VerifiedCircleActivations,
    activation_head: &'a StoreDeviceHead,
    activation_head_object: &'a ExactObjectRef,
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
    activation_head: StoreDeviceHead,
    activation_head_object: ExactObjectRef,
    history_evidence: RetainedMergeCommitEvidence,
    membership_objects: Option<VerifiedMergeMembershipObjects>,
    packages: Vec<AudiencePackage>,
    package_application: Option<RetainedPackageApplication>,
    input_hash: ObjectHash,
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
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
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
            &activation_head,
            &activation_head_object,
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
            activation_head,
            activation_head_object,
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

    pub fn activation_head(&self) -> &StoreDeviceHead {
        &self.activation_head
    }

    pub fn activation_head_object(&self) -> &ExactObjectRef {
        &self.activation_head_object
    }

    pub fn history_evidence(&self) -> &RetainedMergeCommitEvidence {
        &self.history_evidence
    }

    pub fn membership_objects(&self) -> Option<&VerifiedMergeMembershipObjects> {
        self.membership_objects.as_ref()
    }

    pub fn packages(&self) -> &[AudiencePackage] {
        &self.packages
    }

    pub fn package_application(&self) -> Option<RetainedPackageApplication> {
        self.package_application
    }
}

impl<'a> VerifiedMergeMaterialization<'a> {
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

    pub fn activation_head(&self) -> &StoreDeviceHead {
        self.activation_head
    }

    pub fn activation_head_object(&self) -> &ExactObjectRef {
        self.activation_head_object
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
        root: &coven_protocol::store_commit::StoreRootRef,
        verified_commit: &'a coven_protocol::store_commit::VerifiedStoreBatchCommit,
        registrations: &'a [ActivatedStoreDeviceRegistration],
        device_operations: &'a VerifiedStoreDeviceOperations,
        circle_activations: &'a VerifiedCircleActivations,
        activation_head: &'a StoreDeviceHead,
        activation_head_object: &'a ExactObjectRef,
        history_evidence: &'a RetainedMergeCommitEvidence,
        membership_objects: Option<&'a VerifiedMergeMembershipObjects>,
        packages: &'a [AudiencePackage],
        package_application: Option<RetainedPackageApplication>,
    ) -> Result<Self, DbError> {
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        history_evidence
            .validate_for(commit_ref, commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        if verified_commit.store_root_hash() != root.store_root_hash
            || commit.store_root_hash != root.store_root_hash
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
        RetainedStoreDeviceRegistrationActivations::from_verified(root, commit, registrations)
            .map_err(|error| DbError::Message(error.to_string()))?;
        Ok(Self {
            verified_commit,
            device_operations,
            circle_activations,
            activation_head,
            activation_head_object,
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
    pub activation_head: StoreDeviceHead,
    pub activation_head_object: ExactObjectRef,
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

#[derive(Debug)]
pub struct VerifiedStoreSnapshotStability {
    authority: RetainedReplaySnapshotAuthority,
}

impl VerifiedStoreSnapshotStability {
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

pub struct DeviceJoinBootstrapCommit {
    pub reference: StoreBatchCommitRef,
    pub commit: VerifiedStoreBatchCommit,
    pub registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub device_operations: VerifiedStoreDeviceOperations,
    pub activation: DeviceJoinBootstrapActivation,
}

pub struct DeviceJoinBootstrapActivation {
    pub head: StoreDeviceHead,
    pub object: ExactObjectRef,
    pub history_evidence: RetainedMergeCommitEvidence,
}

pub struct DeviceJoinBootstrapPlan {
    pub founder_reference: StoreDeviceRegistrationRef,
    pub founder: StoreDeviceRegistration,
    pub founder_bytes: Vec<u8>,
    pub genesis: ResolvedStoreDeviceState,
    pub membership: InitialStoreMembershipAuthority,
    pub commits: Vec<DeviceJoinBootstrapCommit>,
}

impl DeviceJoinBootstrapPlan {
    pub fn verified_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&VerifiedStoreBatchCommit> {
        self.commits
            .iter()
            .find(|commit| &commit.reference == reference)
            .map(|commit| &commit.commit)
    }
}
