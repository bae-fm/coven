use super::*;

pub(crate) struct StoreWritePreparation {
    pub write_id: WriteId,
    pub remote_objects: Vec<RemoteObjectRecord>,
    pub audiences: PreparedAudienceObjects,
    pub commit: PreparedProtocolObject<StoreBatchCommit>,
    pub head: PreparedProtocolObject<StoreDeviceHead>,
    pub history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}

pub(crate) struct MergeCandidateAbandonmentPreparation {
    pub write_id: WriteId,
    pub commit: PreparedProtocolObject<StoreBatchCommit>,
    pub head: PreparedProtocolObject<StoreDeviceHead>,
    pub history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
}

pub(crate) struct SerialCandidateAbandonmentPreparation {
    pub branch_id: PendingBranchId,
    pub candidate: StoreBatchCommitRef,
    pub commit: PreparedProtocolObject<StoreBatchCommit>,
    pub head: StoreSerialHead,
    pub original_head_bytes: Vec<u8>,
}

pub(crate) struct SerialStoreWritePreparation {
    pub branch_id: PendingBranchId,
    pub base: Option<StoreBatchCommitRef>,
    pub base_head: VersionedObject,
    pub writes: Vec<SerialStoreWritePreparationEntry>,
    pub head: StoreSerialHead,
}

pub(crate) struct SerialStoreWritePreparationEntry {
    pub write_id: WriteId,
    pub remote_objects: Vec<RemoteObjectRecord>,
    pub audiences: PreparedAudienceObjects,
    pub commit: PreparedProtocolObject<StoreBatchCommit>,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateCleanupObject {
    pub(crate) object: ExactObjectRef,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum PreparedStoreWriteState {
    MergeConcurrent {
        commit: DurablePreparedProtocolObject,
        head: DurablePreparedProtocolObject,
        history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
    MergeAbandonment {
        candidate_commit: DurablePreparedProtocolObject,
        candidate_head: DurablePreparedProtocolObject,
        candidate_history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
        authority_commit: DurablePreparedProtocolObject,
        authority_head: DurablePreparedProtocolObject,
        authority_history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
        outcome: MergeAbandonmentOutcome,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
    SerialPreparing,
    Serial {
        base_head: VersionedObject,
        commit: DurablePreparedProtocolObject,
        tip_head_bytes: Option<Vec<u8>>,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
}

pub(super) enum PreparedWriteMaterialization<'a> {
    MergeConcurrent {
        head: &'a StoreDeviceHead,
        head_object: &'a ExactObjectRef,
        history_summary: &'a crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    },
    Serial,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedMergeMaterializationInput {
    pub(super) commit: PreparedExactObject,
    pub(super) activation_head: PreparedExactObject,
    pub(super) history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    pub(super) membership_objects: Option<VerifiedMergeMembershipObjects>,
    pub(super) packages: Vec<RetainedAudiencePackage>,
    pub(super) activation: RetainedCommitActivationInput,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerifiedMergeMembershipObjects {
    entry: MembershipEntryRef,
    head: MembershipHeadRef,
    resolution: Option<StoreMembershipConflictResolutionRef>,
}

impl VerifiedMergeMembershipObjects {
    pub(super) fn entry(&self) -> &MembershipEntryRef {
        &self.entry
    }

    pub(super) fn head(&self) -> &MembershipHeadRef {
        &self.head
    }

    pub(super) fn resolution(&self) -> Option<&StoreMembershipConflictResolutionRef> {
        self.resolution.as_ref()
    }

    pub(crate) fn verify(
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
        entry: &MembershipEntry,
        head_value: &AuthorHead,
        head: MembershipHeadRef,
    ) -> Result<Self, DbError> {
        let Some(crate::sync::store_commit::StoreControl::MergeMembership { transition }) =
            commit.control()
        else {
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

    pub(super) fn object_ids(&self) -> impl Iterator<Item = ObjectHash> + '_ {
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
pub(super) struct MergeRetractionCleanupInput {
    pub(super) commit: PreparedExactObject,
    pub(super) activation_head: PreparedExactObject,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum RetainedAudiencePackage {
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
    pub(super) fn package(&self) -> &AudiencePackage {
        match self {
            Self::Store { package, .. } | Self::Circle { package, .. } => package,
        }
    }

    pub(super) fn domain(&self) -> SharedLiveSetObjectDomain {
        match self {
            Self::Store { reference, .. } => SharedLiveSetObjectDomain::StorePackage {
                reference: reference.clone(),
            },
            Self::Circle { reference, .. } => SharedLiveSetObjectDomain::CirclePackage {
                reference: reference.clone(),
            },
        }
    }

    pub(super) fn object(&self) -> &ExactObjectRef {
        match self {
            Self::Store { reference, .. } => &reference.object,
            Self::Circle { reference, .. } => &reference.package.object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedCommitActivationInput {
    pub(super) registrations: RetainedStoreDeviceRegistrationActivations,
    pub(super) device_operations: RetainedStoreDeviceOperations,
    pub(super) circle_activations: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) package_application: Option<RetainedPackageApplication>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RetainedPackageApplication {
    LocallyAuthored,
    Received { receiver_wall_ms: u64 },
}

pub(super) struct RetainedMergeMaterializationKey {
    pub(super) commit_ref: String,
    pub(super) input_hash: ObjectHash,
}

pub(super) enum MaterializedCommitRetention<'a> {
    MergeConcurrent(&'a RetainedMergeMaterializationKey),
    Serial,
}

pub(crate) struct VerifiedMergeMaterialization<'a> {
    commit: &'a StoreBatchCommit,
    commit_ref: &'a StoreBatchCommitRef,
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
    commit: StoreBatchCommit,
    commit_ref: StoreBatchCommitRef,
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
    pub(super) fn verify(
        root: crate::sync::store_commit::StoreRootRef,
        commit: StoreBatchCommit,
        commit_ref: StoreBatchCommitRef,
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
            &commit,
            &commit_ref,
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
            commit,
            commit_ref,
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

    pub(crate) fn as_verified(&self) -> Result<VerifiedMergeMaterialization<'_>, DbError> {
        VerifiedMergeMaterialization::verify(
            &self.root,
            &self.commit,
            &self.commit_ref,
            &self.registrations,
            &self.device_operations,
            &self.circle_activations,
            &self.activation_head,
            &self.activation_head_object,
            &self.history_summary,
            self.membership_objects.as_ref(),
            &self.packages,
            self.package_application,
        )
    }

    pub(crate) fn input_hash(&self) -> ObjectHash {
        self.input_hash
    }

    pub(crate) fn root(&self) -> &crate::sync::store_commit::StoreRootRef {
        &self.root
    }

    pub(crate) fn commit(&self) -> &StoreBatchCommit {
        &self.commit
    }

    pub(crate) fn commit_ref(&self) -> &StoreBatchCommitRef {
        &self.commit_ref
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
        self.commit
    }

    pub(crate) fn commit_ref(&self) -> &StoreBatchCommitRef {
        self.commit_ref
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
        commit: &'a StoreBatchCommit,
        commit_ref: &'a StoreBatchCommitRef,
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
        commit_ref
            .verify_commit(commit)
            .map_err(|error| DbError::Message(error.to_string()))?;
        history_summary
            .validate_shape()
            .map_err(|error| DbError::Message(error.to_string()))?;
        if commit.store_root_hash != root.store_root_hash
            || commit.policy() != WritePolicy::MergeConcurrent
            || commit_ref.coord.policy() != WritePolicy::MergeConcurrent
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
            || matches!(
                commit.control(),
                Some(crate::sync::store_commit::StoreControl::MergeMembership { .. })
            ) != membership_objects.is_some()
        {
            return Err(DbError::Message(
                "verified Merge materialization differs from its exact Store commit".to_string(),
            ));
        }
        RetainedStoreDeviceRegistrationActivations::from_verified(root, commit, registrations)
            .map_err(|error| DbError::Message(error.to_string()))?;
        Ok(Self {
            commit,
            commit_ref,
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

pub(crate) enum LocalRetirementMaterialization {
    MergeConcurrent {
        head: StoreDeviceHead,
        head_object: ExactObjectRef,
        history_summary: crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
    },
    Serial {
        authorization: SerialAuthorizationState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum MergeAbandonmentOutcome {
    Prepared,
    Accepted {
        authority: StoreBatchCommitRef,
    },
    Lost {
        winner_commit: StoreBatchCommitRef,
        winner_head: crate::sync::store_commit::StoreDeviceHeadRef,
    },
    AuthorExcluded,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DurableSerialCandidateAbandonment {
    pub(super) branch_id: PendingBranchId,
    pub(super) base: Option<StoreBatchCommitRef>,
    pub(super) base_head: VersionedObject,
    pub(super) candidate: StoreBatchCommitRef,
    pub(super) commit: DurablePreparedProtocolObject,
    pub(super) head_bytes: Vec<u8>,
    pub(super) original_head_bytes: Vec<u8>,
}

pub(super) struct PreparedSerialCandidate {
    pub(super) commit: StoreBatchCommit,
    pub(super) reference: StoreBatchCommitRef,
    pub(super) canonical_signed_bytes: Vec<u8>,
}

pub(super) struct PreparedMergeCandidate {
    pub(super) commit: StoreBatchCommit,
    pub(super) reference: StoreBatchCommitRef,
    pub(super) canonical_signed_bytes: Vec<u8>,
    pub(super) commit_prepared: PreparedExactObject,
    pub(super) head: StoreDeviceHead,
    pub(super) head_prepared: PreparedExactObject,
}
