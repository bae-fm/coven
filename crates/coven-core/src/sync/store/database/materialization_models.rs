use crate::database::*;
use crate::sync::audience_package::AudiencePackage;
use crate::sync::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
    StoreMembershipConflictResolutionRef,
};
use crate::sync::remote_object::{remote_object_id, SharedLiveSetObjectDomain};
use crate::sync::storage::{ExactObjectRef, PreparedExactObject};
use crate::sync::store::circle_controls::activation::VerifiedCircleActivations;
use crate::sync::store_commit::{
    CirclePackageRef, ObjectHash, RetainedStoreDeviceOperations,
    RetainedStoreDeviceRegistrationActivations, StoreBatchCommit, StoreBatchCommitRef,
    StoreDeviceHead, StoreDeviceRegistration, StorePackageRef, VerifiedStoreDeviceOperations,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedMergeMaterializationInput {
    pub(crate) commit: PreparedExactObject,
    pub(crate) activation_head: PreparedExactObject,
    pub(crate) history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    pub(crate) membership_objects: Option<VerifiedMergeMembershipObjects>,
    pub(crate) packages: Vec<RetainedAudiencePackage>,
    pub(crate) activation: RetainedCommitActivationInput,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerifiedMergeMembershipObjects {
    entry: MembershipEntryRef,
    head: MembershipHeadRef,
    resolution: Option<StoreMembershipConflictResolutionRef>,
}

impl VerifiedMergeMembershipObjects {
    pub(crate) fn entry(&self) -> &MembershipEntryRef {
        &self.entry
    }

    pub(crate) fn head(&self) -> &MembershipHeadRef {
        &self.head
    }

    pub(crate) fn resolution(&self) -> Option<&StoreMembershipConflictResolutionRef> {
        self.resolution.as_ref()
    }

    pub(crate) fn verify(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        entry: &MembershipEntry,
        head_value: &AuthorHead,
        head: MembershipHeadRef,
    ) -> Result<Self, DbError> {
        let Some(crate::sync::store_commit::StoreControl { transition }) = commit.control() else {
            return Err(DbError::Message(
                "Merge membership object closure accompanies another Store control".to_string(),
            ));
        };
        if transition.body.entry.coord != entry.coord()
            || !transition.matches_head(head_value, &head)
            || !matches!(
                &head_value.activation,
                crate::sync::membership::MembershipHeadActivation::StoreCommit { commit }
                    if commit == commit_ref
            )
        {
            return Err(DbError::Message(
                "Merge membership object closure differs from its exact Store transition"
                    .to_string(),
            ));
        }
        let resolution = match &entry.change {
            crate::sync::membership::MembershipChange::ResolutionActivation { resolution } => {
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

    pub(crate) fn object_ids(&self) -> impl Iterator<Item = ObjectHash> + '_ {
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
pub(crate) struct MergeRetractionCleanupInput {
    pub(crate) commit: PreparedExactObject,
    pub(crate) activation_head: PreparedExactObject,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedAudiencePackage {
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
    pub(crate) fn package(&self) -> &AudiencePackage {
        match self {
            Self::Store { package, .. } | Self::Circle { package, .. } => package,
        }
    }

    pub(crate) fn domain(&self) -> SharedLiveSetObjectDomain {
        match self {
            Self::Store { reference, .. } => SharedLiveSetObjectDomain::StorePackage {
                reference: reference.clone(),
            },
            Self::Circle { reference, .. } => SharedLiveSetObjectDomain::CirclePackage {
                reference: reference.clone(),
            },
        }
    }

    pub(crate) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Store { reference, .. } => &reference.object,
            Self::Circle { reference, .. } => &reference.package.object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedCommitActivationInput {
    pub(crate) registrations: RetainedStoreDeviceRegistrationActivations,
    pub(crate) device_operations: RetainedStoreDeviceOperations,
    pub(crate) circle_activations: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) package_application: Option<RetainedPackageApplication>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedPackageApplication {
    LocallyAuthored,
    Received { receiver_wall_ms: u64 },
}

pub(crate) struct RetainedMergeMaterializationKey {
    pub(crate) commit_ref: String,
    pub(crate) input_hash: ObjectHash,
}

pub(crate) struct VerifiedMergeMaterialization<'a> {
    verified_commit: &'a crate::sync::store_commit::VerifiedStoreBatchCommit,
    device_operations: &'a VerifiedStoreDeviceOperations,
    circle_activations: &'a VerifiedCircleActivations,
    activation_head: &'a StoreDeviceHead,
    activation_head_object: &'a ExactObjectRef,
    history_summary: &'a crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    membership_objects: Option<&'a VerifiedMergeMembershipObjects>,
    packages: &'a [AudiencePackage],
    package_application: Option<RetainedPackageApplication>,
    registrations: &'a [(
        StoreDeviceRegistration,
        crate::sync::store_commit::StoreDeviceRegistrationActivation,
    )],
}

#[derive(Clone)]
pub(crate) struct OwnedVerifiedMergeMaterialization {
    root: crate::sync::store_commit::StoreRootRef,
    verified_commit: crate::sync::store_commit::VerifiedStoreBatchCommit,
    registrations: Vec<(
        StoreDeviceRegistration,
        crate::sync::store_commit::StoreDeviceRegistrationActivation,
    )>,
    device_operations: VerifiedStoreDeviceOperations,
    circle_activations: VerifiedCircleActivations,
    activation_head: StoreDeviceHead,
    activation_head_object: ExactObjectRef,
    history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    membership_objects: Option<VerifiedMergeMembershipObjects>,
    packages: Vec<AudiencePackage>,
    package_application: Option<RetainedPackageApplication>,
    input_hash: ObjectHash,
}

impl OwnedVerifiedMergeMaterialization {
    pub(crate) fn verify(
        root: crate::sync::store_commit::StoreRootRef,
        verified_commit: crate::sync::store_commit::VerifiedStoreBatchCommit,
        registrations: Vec<(
            StoreDeviceRegistration,
            crate::sync::store_commit::StoreDeviceRegistrationActivation,
        )>,
        device_operations: VerifiedStoreDeviceOperations,
        circle_activations: VerifiedCircleActivations,
        activation_head: StoreDeviceHead,
        activation_head_object: ExactObjectRef,
        history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
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
            &history_summary,
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
            history_summary,
            membership_objects,
            packages,
            package_application,
            input_hash,
        })
    }

    pub(crate) fn input_hash(&self) -> ObjectHash {
        self.input_hash
    }

    pub(crate) fn root(&self) -> &crate::sync::store_commit::StoreRootRef {
        &self.root
    }

    pub(crate) fn commit(&self) -> &StoreBatchCommit {
        self.verified_commit.value()
    }

    pub(crate) fn commit_ref(&self) -> &StoreBatchCommitRef {
        self.verified_commit.reference()
    }

    pub(crate) fn verified_commit(&self) -> &crate::sync::store_commit::VerifiedStoreBatchCommit {
        &self.verified_commit
    }

    pub(crate) fn registrations(
        &self,
    ) -> &[(
        StoreDeviceRegistration,
        crate::sync::store_commit::StoreDeviceRegistrationActivation,
    )] {
        &self.registrations
    }

    pub(crate) fn device_operations(&self) -> &VerifiedStoreDeviceOperations {
        &self.device_operations
    }

    pub(crate) fn circle_activations(&self) -> &VerifiedCircleActivations {
        &self.circle_activations
    }

    pub(crate) fn activation_head(&self) -> &StoreDeviceHead {
        &self.activation_head
    }

    pub(crate) fn activation_head_object(&self) -> &ExactObjectRef {
        &self.activation_head_object
    }

    pub(crate) fn history_summary(
        &self,
    ) -> &crate::sync::store_commit::RetainedVerifiedMergeHistorySummary {
        &self.history_summary
    }

    pub(crate) fn membership_objects(&self) -> Option<&VerifiedMergeMembershipObjects> {
        self.membership_objects.as_ref()
    }

    pub(crate) fn packages(&self) -> &[AudiencePackage] {
        &self.packages
    }

    pub(crate) fn package_application(&self) -> Option<RetainedPackageApplication> {
        self.package_application
    }
}

impl<'a> VerifiedMergeMaterialization<'a> {
    pub(crate) fn commit(&self) -> &StoreBatchCommit {
        self.verified_commit.value()
    }

    pub(crate) fn commit_ref(&self) -> &StoreBatchCommitRef {
        self.verified_commit.reference()
    }

    pub(crate) fn verified_commit(&self) -> &crate::sync::store_commit::VerifiedStoreBatchCommit {
        self.verified_commit
    }

    pub(crate) fn registrations(
        &self,
    ) -> &[(
        StoreDeviceRegistration,
        crate::sync::store_commit::StoreDeviceRegistrationActivation,
    )] {
        self.registrations
    }

    pub(crate) fn device_operations(&self) -> &VerifiedStoreDeviceOperations {
        self.device_operations
    }

    pub(crate) fn circle_activations(&self) -> &VerifiedCircleActivations {
        self.circle_activations
    }

    pub(crate) fn activation_head(&self) -> &StoreDeviceHead {
        self.activation_head
    }

    pub(crate) fn activation_head_object(&self) -> &ExactObjectRef {
        self.activation_head_object
    }

    pub(crate) fn history_summary(
        &self,
    ) -> &crate::sync::store_commit::RetainedVerifiedMergeHistorySummary {
        self.history_summary
    }

    pub(crate) fn membership_objects(&self) -> Option<&VerifiedMergeMembershipObjects> {
        self.membership_objects
    }

    pub(crate) fn packages(&self) -> &[AudiencePackage] {
        self.packages
    }

    pub(crate) fn package_application(&self) -> Option<RetainedPackageApplication> {
        self.package_application
    }

    pub(crate) fn verify(
        root: &crate::sync::store_commit::StoreRootRef,
        verified_commit: &'a crate::sync::store_commit::VerifiedStoreBatchCommit,
        registrations: &'a [(
            StoreDeviceRegistration,
            crate::sync::store_commit::StoreDeviceRegistrationActivation,
        )],
        device_operations: &'a VerifiedStoreDeviceOperations,
        circle_activations: &'a VerifiedCircleActivations,
        activation_head: &'a StoreDeviceHead,
        activation_head_object: &'a ExactObjectRef,
        history_summary: &'a crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
        membership_objects: Option<&'a VerifiedMergeMembershipObjects>,
        packages: &'a [AudiencePackage],
        package_application: Option<RetainedPackageApplication>,
    ) -> Result<Self, DbError> {
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        history_summary
            .validate_shape()
            .map_err(|error| DbError::Message(error.to_string()))?;
        if verified_commit.store_root_hash() != root.store_root_hash
            || commit.store_root_hash != root.store_root_hash
            || history_summary.store_root_hash != root.store_root_hash
            || history_summary.digest() != activation_head.history_summary
            || history_summary.causal_cut.get(&commit_ref.coord) != Some(commit_ref)
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
            history_summary,
            membership_objects,
            packages,
            package_application,
            registrations,
        })
    }
}
