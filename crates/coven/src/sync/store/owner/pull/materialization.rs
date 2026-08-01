use super::*;

pub(crate) enum Readiness {
    Ready,
    AlreadyMaterialized,
    Held(HeldStorePosition),
}

pub(crate) fn held_object_error(error: StoreObjectError) -> HeldStorePositionReason {
    match error {
        StoreObjectError::Storage(source) => HeldStorePositionReason::ObjectUnreadable {
            key: "exact Store object".to_string(),
            detail: source.to_string(),
        },
        StoreObjectError::InvalidObject { key, source, .. } => match *source {
            StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
            StoreProtocolError::RelocatedSlot { .. }
            | StoreProtocolError::RelocatedPackage { .. }
            | StoreProtocolError::StoreRootMismatch { .. }
            | StoreProtocolError::StoreMismatch { .. }
            | StoreProtocolError::FounderMismatch { .. } => {
                HeldStorePositionReason::WrongSlot(source.to_string())
            }
            source => HeldStorePositionReason::ObjectUnreadable {
                key,
                detail: source.to_string(),
            },
        },
    }
}

pub(super) fn historical_local_store_membership(
    latest: LocalStoreMembership,
    candidate: LocalStoreMembership,
) -> LocalStoreMembership {
    if matches!(latest, LocalStoreMembership::Removed)
        || matches!(candidate, LocalStoreMembership::Removed)
    {
        LocalStoreMembership::Removed
    } else if matches!(latest, LocalStoreMembership::Current)
        && matches!(candidate, LocalStoreMembership::Current)
    {
        LocalStoreMembership::Current
    } else if matches!(latest, LocalStoreMembership::IdentityNotSupplied)
        || matches!(candidate, LocalStoreMembership::IdentityNotSupplied)
    {
        LocalStoreMembership::IdentityNotSupplied
    } else {
        LocalStoreMembership::NotYetMember
    }
}

pub(crate) struct PreparedMergeMaterializationPackage {
    pub(crate) package: AudiencePackage,
    pub(crate) changeset: ValidatedChangeset<Vec<u8>>,
}

pub(crate) struct PreparedMergeMaterialization {
    pub(crate) root: StoreRootRef,
    pub(crate) verified_commit: VerifiedStoreBatchCommit,
    pub(crate) activation_head: StoreDeviceHead,
    pub(crate) activation_head_object: ExactObjectRef,
    pub(crate) history_summary: RetainedVerifiedMergeHistorySummary,
    pub(crate) membership_objects: Option<crate::database::VerifiedMergeMembershipObjects>,
    pub(crate) membership_remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    pub(crate) registrations: Vec<(StoreDeviceRegistration, StoreDeviceRegistrationActivation)>,
    pub(crate) packages: Vec<PreparedMergeMaterializationPackage>,
    pub(crate) device_operations: VerifiedStoreDeviceOperations,
    pub(crate) circle_activations: VerifiedCircleActivations,
    pub(crate) package_application: Option<crate::database::RetainedPackageApplication>,
}

pub(crate) struct MembershipAuthorityBytes {
    canonical: Vec<u8>,
    stored: Vec<u8>,
}

impl MembershipAuthorityBytes {
    pub(crate) fn new(canonical: Vec<u8>, stored: Vec<u8>) -> Self {
        Self { canonical, stored }
    }
}

pub(crate) fn activated_merge_membership_remote_objects(
    family: super::store_commit::CandidateFamilyId,
    objects: &crate::database::VerifiedMergeMembershipObjects,
    entry_bytes: MembershipAuthorityBytes,
    head_bytes: MembershipAuthorityBytes,
    resolution_bytes: Option<MembershipAuthorityBytes>,
    commit_ref: &StoreBatchCommitRef,
) -> Result<
    Vec<super::remote_object::RemoteObjectRecord>,
    super::remote_object::RemoteObjectRecordError,
> {
    let mut remotes = vec![
        super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_entry(
            family,
            objects.entry().clone(),
            entry_bytes.canonical,
            entry_bytes.stored,
            commit_ref.clone(),
        )?
        .into_observed_activated(commit_ref)?,
        super::remote_object::RemoteObjectRecord::candidate_exclusive_merge_membership_head(
            family,
            objects.head().clone(),
            head_bytes.canonical,
            head_bytes.stored,
            commit_ref.clone(),
        )?
        .into_observed_activated(commit_ref)?,
    ];
    if let Some(resolution) = objects.resolution() {
        let bytes = resolution_bytes
            .ok_or(super::remote_object::RemoteObjectRecordError::StoredReferenceMismatch)?;
        remotes.push(
            super::remote_object::RemoteObjectRecord::candidate_activated_store_membership_resolution(
                resolution.clone(),
                bytes.canonical,
                bytes.stored,
                commit_ref.clone(),
            )?
            .into_observed_activated(commit_ref)?,
        );
    } else if resolution_bytes.is_some() {
        return Err(super::remote_object::RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(remotes)
}
